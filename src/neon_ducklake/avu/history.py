"""Record applied AVU journal rows into the MESA DuckLake history, in few large snapshots.

``record_scope`` runs under the global ``mesa-tag`` file lock, only when no other process holds
``~/.mesa/ducklake/catalog.duckdb``, with the one ``DuckLakeClient`` of this process. Recovery runs
first. Applied rows are grouped (ordered by path, id; a path is never split) into batches of at most
``rows_per_batch`` rows; each batch becomes ONE ``record_changes`` snapshot whose note carries
``batch=<uuid>`` so a crash can be matched back to the journal.

Recovery (the only private mesa-ducklake API use lives in :func:`_catalog_snapshots` and
:func:`_pending_pushes`):

1. List our project's snapshots including pending/failed, plus the pending-push WAL.
2. A batch is recorded iff its ``batch=`` snapshot exists with ``parquet_file`` not in
   {'pending', 'failed'}.
3. Local Parquet of failed, orphan-pending (no WAL row), unknown or invalid (unreadable / wrong row
   count) snapshots is moved from ``work/mesa-cache/<project>/`` to ``work/quarantine/`` BEFORE
   ``recover_pending_pushes`` — LakeStore reads glob every cached ``snapshot_*.parquet``.
4. Batches whose snapshot committed are marked recorded; dead batches release their rows for
   re-recording; a batch still pending after recovery blocks its rows (never recorded twice).

Backends (docs/STREAMING.md "History backends"): ``NEON_DUCKLAKE_HISTORY_MODE`` (or ``mode=``) picks

- ``direct`` (default): record into the MESA catalog as above; needs the catalog's single-writer lock.
  This is also the path for DuckDB 2.0 multi-writer catalogs once they are released.
- ``spool`` (workers on DE VMs): each batch becomes ``<batch_id>.parquet`` (the ``AvuChange`` columns plus
  ``batch_id``, ``scope``, ``vm``, ``journal_id``) and a ``<batch_id>.json`` manifest (rows, sha256, md5,
  createdAt), written under ``work/spool/<vm>/`` and uploaded, checksum-verified, to
  ``<IRODS_ROOT>/.mesa/spool/<vm>/`` (Parquet first, manifest last). The journal rows become ``spooled`` and
  the batch ``spooled``; the local files are then deleted. ``batches.vm`` records the VM that spooled it. A
  batch interrupted before its upload finished stays ``spooling`` and is completed by the next call (on
  this VM, or on another VM after a journal restore): from its local files when this host has them, else it
  is trusted when ``.mesa/spool/<vm>/`` holds its Parquet with a manifest whose md5 matches, or when
  ``.mesa/spool-done/<vm>/`` (the recorder already took it, ``.dup-*`` copies included) holds its Parquet.
  Only when neither holds it are its rows released for re-spooling; a Parquet without a (matching) manifest
  is first moved to ``.mesa/spool-aside/<vm>/`` so the recorder never sees it.
  :mod:`neon_ducklake.avu.recorder` ingests the spool into MESA on the catalog host, merging several
  batches per snapshot with the note ``neon-ducklake spool batch=<id>,<id>…``; :func:`batch_ids_of` and
  :func:`recover` understand such multi-batch notes.

``mesa-tag`` (the host lock of every MESA writer) is waited for up to ``NEON_DUCKLAKE_MESA_LOCK_TIMEOUT_S``
(default 1800 s) in the ``direct`` path, so a recorder pass never fails a chunk's history stage.

Scale (docs/SCALING.md "Hard limits"):

- ``rows_per_batch`` defaults to 250,000, about 3 min of staging at ~1,380 rows/s. That is about 4-5
  snapshots per site. ``NEON_DUCKLAKE_ROWS_PER_BATCH`` overrides it (``tag`` has no flag for it).
- **Snapshot budget guard.** mesa-ducklake's ``_ENSURE_CACHED_LIMIT`` is 1000 snapshots per project,
  with no compaction. Before recording, the project's snapshots are counted, including pending ones.
  If that count plus the planned batches would pass 900, recording is refused with a clear message
  (``SystemExit``) and nothing is written.
- Each batch's staging time and rows/s are logged, returned in ``batch_stats``, and appended to
  ``batches.note`` locally. The MESA snapshot note itself is unchanged.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import posixpath
import re
import shutil
import socket
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import env, state

log = logging.getLogger("neon_ducklake.avu.history")

_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
BATCH_RE = re.compile(rf"\bbatch=({_UUID}(?:,{_UUID})*)\b")  # one id, or several (recorder merges)
DEAD_PARQUET = ("pending", "failed")
HISTORY_MODE_ENV = "NEON_DUCKLAKE_HISTORY_MODE"
HISTORY_MODES = ("direct", "spool")
SPOOL_STATUSES = ("spooling", "spooled")  # batches owned by the spool path: never released by recover()
SPOOL_FORMAT = "neon-ducklake-spool/1"
DEFAULT_ROWS_PER_BATCH = 250_000
ROWS_PER_BATCH_ENV = "NEON_DUCKLAKE_ROWS_PER_BATCH"
SNAPSHOT_BUDGET = 900  # refuse to record past this many project snapshots (MESA caches at most 1000)
SNAPSHOT_LIST_LIMIT = 1000
NOTE_VM_RE = re.compile(r"\bvm=([A-Za-z0-9._-]+)")


class SnapshotBudgetExceeded(SystemExit):
    """Recording would push the MESA project past SNAPSHOT_BUDGET snapshots."""


def rows_per_batch_default() -> int:
    """``NEON_DUCKLAKE_ROWS_PER_BATCH`` (read at call time) or DEFAULT_ROWS_PER_BATCH."""
    raw = os.environ.get(ROWS_PER_BATCH_ENV, "").strip()
    if not raw:
        return DEFAULT_ROWS_PER_BATCH
    try:
        n = int(raw.replace("_", ""))
    except ValueError as exc:
        raise SystemExit(f"{ROWS_PER_BATCH_ENV}={raw!r} is not an integer") from exc
    if n < 1:
        raise SystemExit(f"{ROWS_PER_BATCH_ENV} must be >= 1 (got {n})")
    if n > DEFAULT_ROWS_PER_BATCH:
        log.warning("%s=%d exceeds the designed %d rows per batch (longer MESA lock windows)", ROWS_PER_BATCH_ENV,
                    n, DEFAULT_ROWS_PER_BATCH)
    return n


def parse_ts(value: str) -> datetime:
    """Journal ``applied_at`` -> tz-aware UTC datetime (naive values are taken as UTC, never local)."""
    ts = datetime.fromisoformat(value)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def batch_ids_of(note: str | None) -> list[str]:
    """Every batch id of a snapshot note: ``batch=<id>`` or the recorder's ``batch=<id>,<id>,…``."""
    m = BATCH_RE.search(note or "")
    return m.group(1).split(",") if m else []


def batch_id_of(note: str | None) -> str | None:
    """The first batch id of a note (None when it has none)."""
    ids = batch_ids_of(note)
    return ids[0] if ids else None


def history_mode(mode: str | None = None) -> str:
    """``mode`` or ``$NEON_DUCKLAKE_HISTORY_MODE`` (read at call time): ``direct`` (default) or ``spool``."""
    raw = (mode or os.environ.get(HISTORY_MODE_ENV) or "direct").strip().lower()
    if raw not in HISTORY_MODES:
        raise SystemExit(f"{HISTORY_MODE_ENV}={raw!r}: choose one of {', '.join(HISTORY_MODES)}")
    return raw


def snapshot_batches(snaps: list[Any]) -> tuple[dict[str, int], dict[str, int]]:
    """(committed, pending): batch id -> snapshot id over snapshot notes (multi-batch notes included)."""
    committed: dict[str, int] = {}
    pending: dict[str, int] = {}
    for s in snaps:
        for bid in batch_ids_of(s.note):
            if s.parquet_file not in DEAD_PARQUET:
                committed[bid] = s.snapshot_id
            elif s.parquet_file == "pending":
                pending[bid] = s.snapshot_id
    return committed, pending


# ---------------------------------------------------------------- private-API helpers (one place)

def _catalog_snapshots(client: Any, project_id: Any) -> list[Any]:
    return list(client._get_catalog().list_snapshots(project_id, limit=SNAPSHOT_LIST_LIMIT, include_pending=True))


def _pending_pushes(client: Any) -> list[Any]:
    return list(client._get_catalog().list_pending_pushes(limit=1000))


# ---------------------------------------------------------------- batching

def plan_batches(con: sqlite3.Connection, scope: str, rows_per_batch: int | None = None
                 ) -> list[list[sqlite3.Row]]:
    """Applied, un-batched rows of ``scope`` in (path, id) order, cut into batches without splitting a path."""
    rows_per_batch = rows_per_batch or rows_per_batch_default()
    rows = con.execute("SELECT * FROM avu_journal WHERE scope=? AND status='applied' AND batch_id IS NULL"
                       " ORDER BY path, id", (scope,)).fetchall()
    by_path: dict[str, list[sqlite3.Row]] = defaultdict(list)
    order: list[str] = []
    for r in rows:
        if r["path"] not in by_path:
            order.append(r["path"])
        by_path[r["path"]].append(r)
    batches: list[list[sqlite3.Row]] = []
    cur: list[sqlite3.Row] = []
    for path in order:
        group = by_path[path]
        if cur and len(cur) + len(group) > rows_per_batch:
            batches.append(cur)
            cur = []
        if len(group) > rows_per_batch:
            log.warning("path %s has %d rows (> %d); it gets a batch of its own", path, len(group), rows_per_batch)
        cur.extend(group)
    if cur:
        batches.append(cur)
    return batches


def to_changes(rows: list[sqlite3.Row], scope: str) -> list[Any]:
    """Journal rows -> ``AvuChange``. An adopted row (``avu_journal.adopted``: an AVU found in iRODS that this
    journal did not own, see ``bulk``) gets the source ``neon-ducklake:<generator>+adopted``."""
    from mesa_ducklake import AvuChange

    out = []
    for r in rows:
        if not r["applied_at"]:
            raise ValueError(f"journal row {r['id']} is applied without applied_at")
        adopted = "adopted" in r.keys() and bool(r["adopted"])  # noqa: SIM118 - sqlite3.Row: `in` tests values
        out.append(AvuChange(irods_path=r["path"], target_type=r["target_type"], attribute=r["attribute"],
                             value=r["value"], unit=r["unit"] or "", op="add" if r["op"] == "add" else "delete",
                             actor=env.ACTOR, ts=parse_ts(r["applied_at"]),
                             source=f"neon-ducklake:{r['generator'] or scope}" + ("+adopted" if adopted else "")))
    return out


def check_snapshot_budget(client: Any, project_id: Any, planned: int, budget: int = SNAPSHOT_BUDGET) -> int:
    """Existing snapshots of the project (pending included); refuses when ``existing + planned > budget``."""
    existing = len(_catalog_snapshots(client, project_id))
    if existing + planned > budget:
        capped = " (listing capped)" if existing >= SNAPSHOT_LIST_LIMIT else ""
        raise SnapshotBudgetExceeded(
            f"MESA snapshot budget: project {project_id} already has {existing}{capped} snapshot(s) and this "
            f"record needs {planned} more; {existing} + {planned} > {budget} (mesa-ducklake caches at most "
            f"{SNAPSHOT_LIST_LIMIT}). Nothing was recorded. Use fewer, larger batches "
            f"({ROWS_PER_BATCH_ENV}, default {DEFAULT_ROWS_PER_BATCH}) or compact the project's history first.")
    return existing


def record_batches(client: Any, project_id: Any, session: Any, con: sqlite3.Connection, scope: str,
                   rows_per_batch: int | None = None, *, snapshot_budget: int = SNAPSHOT_BUDGET) -> dict[str, Any]:
    """Record every applied, un-batched row of ``scope``; one snapshot per batch. Stops at the first failure.

    Refuses (``SnapshotBudgetExceeded``) before writing anything when the project's snapshots plus the
    planned batches would exceed ``snapshot_budget``.
    """
    rows_per_batch = rows_per_batch or rows_per_batch_default()
    batches = plan_batches(con, scope, rows_per_batch)
    total = len(batches)
    out: dict[str, Any] = {"batches_planned": total, "batches_recorded": 0, "rows_recorded": 0, "snapshots": [],
                           "rows_per_batch": rows_per_batch, "batch_stats": []}
    if total:
        out["snapshots_before"] = check_snapshot_budget(client, project_id, total, snapshot_budget)
        log.info("recording %s: %d batch(es) of <= %d rows; project has %d snapshot(s), budget %d", scope, total,
                 rows_per_batch, out["snapshots_before"], snapshot_budget)
    for k, rows in enumerate(batches, 1):
        batch_id = str(uuid.uuid4())
        n_paths = len({r["path"] for r in rows})
        note = f"neon-ducklake {scope} batch={batch_id} {k}/{total} paths={n_paths} avus={len(rows)}"
        changes = to_changes(rows, scope)
        ids = [(batch_id, r["id"]) for r in rows]
        con.execute("BEGIN IMMEDIATE")
        try:
            con.execute("INSERT INTO batches(batch_id, scope, seq, total, note, n_rows, status, created_at)"
                        " VALUES (?,?,?,?,?,?, 'recording', ?)",
                        (batch_id, scope, k, total, note, len(rows), env.utcnow().isoformat()))
            con.executemany("UPDATE avu_journal SET batch_id=? WHERE id=?", ids)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        t0 = time.monotonic()
        try:
            snap = client.record_changes(project_id, env.ACTOR, changes, note=note, session=session)
        except Exception as exc:  # noqa: BLE001 - any failure leaves the batch for `recover`
            con.execute("UPDATE batches SET status='failed', error=? WHERE batch_id=?",
                        (f"{type(exc).__name__}: {exc}"[:500], batch_id))
            log.error("batch %d/%d (%s) failed: %s — run `neon-ducklake recover`", k, total, batch_id, exc)
            out["failed_batch"] = batch_id
            out["error"] = f"{type(exc).__name__}: {exc}"[:500]
            return out
        secs = max(time.monotonic() - t0, 1e-6)
        rps = len(rows) / secs
        _mark_recorded(con, batch_id, snap.snapshot_id,
                       note_suffix=f" | staged {len(rows)} rows in {secs:.1f}s ({rps:.0f} rows/s)")
        out["batches_recorded"] += 1
        out["rows_recorded"] += len(rows)
        out["snapshots"].append(snap.snapshot_id)
        out["batch_stats"].append({"seq": k, "snapshot_id": snap.snapshot_id, "rows": len(rows),
                                   "seconds": round(secs, 2), "rows_per_s": round(rps, 1)})
        log.info("recorded batch %d/%d: snapshot %s, %d rows over %d paths in %.1fs (%.0f rows/s)", k, total,
                 snap.snapshot_id, len(rows), n_paths, secs, rps)
    return out


def _mark_recorded(con: sqlite3.Connection, batch_id: str, snapshot_id: int, note_suffix: str = "") -> int:
    con.execute("BEGIN IMMEDIATE")
    try:
        n = con.execute("UPDATE avu_journal SET status='recorded', snapshot_id=? WHERE batch_id=?"
                        " AND status IN ('applied', 'spooled')", (snapshot_id, batch_id)).rowcount
        con.execute("UPDATE batches SET status='recorded', snapshot_id=?, recorded_at=?, error=NULL,"
                    " note=COALESCE(note, '') || ? WHERE batch_id=?",
                    (snapshot_id, env.utcnow().isoformat(), note_suffix, batch_id))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return n


def _release(con: sqlite3.Connection, batch_id: str, reason: str) -> int:
    con.execute("BEGIN IMMEDIATE")
    try:
        n = con.execute("UPDATE avu_journal SET batch_id=NULL WHERE batch_id=? AND status='applied'",
                        (batch_id,)).rowcount
        con.execute("UPDATE batches SET status='abandoned', error=? WHERE batch_id=?", (reason[:500], batch_id))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return n


# ---------------------------------------------------------------- recovery

def parquet_rows(path: Path) -> int | None:
    """Row count of a local Parquet file, or None when it is missing/unreadable."""
    import duckdb

    if not path.exists():
        return None
    try:
        con = duckdb.connect()
        try:
            return int(con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001 - duckdb raises various IO/format errors
        log.warning("unreadable parquet %s: %s", path, exc)
        return None


def quarantine(path: Path, project_id: Any, reason: str) -> Path | None:
    if not path.exists():
        return None
    dest_dir = env.QUARANTINE_DIR / str(project_id)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{env.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{path.name}"
    shutil.move(str(path), dest)
    log.warning("quarantined %s -> %s (%s)", path, dest, reason)
    return dest


def recover(client: Any, project: Any, session: Any, con: sqlite3.Connection) -> dict[str, Any]:
    """Quarantine bad local Parquet, drain the pending-push WAL, reconcile our batches. Returns a summary."""
    pid = project.project_id
    cache = env.MESA_CACHE_DIR / str(pid)
    snaps = _catalog_snapshots(client, pid)
    by_id = {s.snapshot_id: s for s in snaps}
    ours = {s.snapshot_id for s in snaps}
    wal = [p for p in _pending_pushes(client) if p.snapshot_id in ours]
    wal_ids = {p.snapshot_id for p in wal}
    batch_rows = {r["batch_id"]: r["n_rows"] for r in con.execute("SELECT batch_id, n_rows FROM batches")}
    summary: dict[str, Any] = {"snapshots": len(snaps), "pending_pushes": len(wal), "quarantined": [],
                               "recovered": None, "recorded": 0, "released": 0, "blocked": []}

    # 1) local cache files that must not be globbed by LakeStore reads
    for f in sorted(cache.glob("snapshot_*.parquet")) if cache.exists() else []:
        m = re.match(r"snapshot_(\d+)\.parquet$", f.name)
        sid = int(m.group(1)) if m else None
        snap = by_id.get(sid) if sid is not None else None
        reason = None
        if snap is None:
            reason = "no catalog snapshot"
        elif snap.parquet_file == "failed":
            reason = "snapshot failed"
        elif snap.parquet_file == "pending" and sid not in wal_ids:
            reason = "orphan pending snapshot (no WAL row)"
        if reason and quarantine(f, pid, reason):
            summary["quarantined"].append({"file": f.name, "reason": reason})

    # 2) validate every pending local parquet before recovery pushes it
    for p in wal:
        if by_id[p.snapshot_id].parquet_file != "pending":
            continue  # already committed; recovery only drops the WAL row
        local = Path(p.local_path)
        n = parquet_rows(local)
        bids = batch_ids_of(by_id[p.snapshot_id].note)
        expected = sum(batch_rows[b] or 0 for b in bids) \
            if bids and all(b in batch_rows and batch_rows[b] is not None for b in bids) else None
        bad = None
        if n is None:
            bad = "missing or unreadable"
        elif expected is not None and n != expected:
            bad = f"row count {n} != batch {expected}"
        if bad and local.exists() and quarantine(local, pid, f"pending snapshot {p.snapshot_id}: {bad}"):
            summary["quarantined"].append({"file": local.name, "reason": bad})

    # 3) drain the WAL (pushes valid pending parquet to iRODS and commits the catalog rows)
    wal_local = {p.snapshot_id: Path(p.local_path) for p in wal}
    if wal:
        summary["recovered"] = client.recover_pending_pushes(session=session, limit=1000)
        snaps = _catalog_snapshots(client, pid)
        wal_local = {p.snapshot_id: Path(p.local_path) for p in _pending_pushes(client) if p.snapshot_id in ours}

    # 4) reconcile our batches
    committed, maybe_pending = snapshot_batches(snaps)
    # still in flight with a valid local file: must not be re-recorded
    pending = {bid: sid for bid, sid in maybe_pending.items()
               if bid not in committed and sid in wal_local and wal_local[sid].exists()}
    open_batches = con.execute("SELECT batch_id, status FROM batches WHERE status != 'recorded'").fetchall()
    for b in open_batches:
        bid = b["batch_id"]
        if bid in committed:
            summary["recorded"] += _mark_recorded(con, bid, committed[bid])
        elif bid in pending:
            con.execute("UPDATE batches SET status='pending', snapshot_id=? WHERE batch_id=?", (pending[bid], bid))
            summary["blocked"].append({"batch_id": bid, "snapshot_id": pending[bid]})
        elif b["status"] in SPOOL_STATUSES:
            continue  # the spool path and the recorder own these; releasing them could record rows twice
        elif b["status"] != "abandoned":
            summary["released"] += _release(con, bid, "no committed snapshot after recovery")
    # rows attached to an abandoned batch (e.g. an interrupted release) go back to the pool
    con.execute("UPDATE avu_journal SET batch_id=NULL WHERE status='applied' AND batch_id IN"
                " (SELECT batch_id FROM batches WHERE status='abandoned')")
    if summary["blocked"]:
        log.warning("%d batch(es) still pending in MESA after recovery; their rows are held back",
                    len(summary["blocked"]))
    log.info("recover: %s", {k: (len(v) if isinstance(v, list) else v) for k, v in summary.items()})
    return summary


# ---------------------------------------------------------------- spool backend (workers)

def spool_schema() -> Any:
    import pyarrow as pa

    return pa.schema([("irods_path", pa.string()), ("target_type", pa.string()), ("attribute", pa.string()),
                      ("value", pa.string()), ("unit", pa.string()), ("op", pa.string()), ("actor", pa.string()),
                      ("ts", pa.timestamp("us", tz="UTC")), ("source", pa.string()), ("batch_id", pa.string()),
                      ("scope", pa.string()), ("vm", pa.string()), ("journal_id", pa.int64())])


def spool_local_dir(vm: str) -> Path:
    return env.WORK_DIR / "spool" / vm


def spool_aside_dir(vm: str) -> str:
    return f"{env.IRODS_ROOT}/.mesa/spool-aside/{vm}"


def ensure_batches_vm(con: sqlite3.Connection) -> None:
    """Add ``batches.vm`` (the VM that spooled a batch) when missing (safe when two processes race)."""
    if any(r[1] == "vm" for r in con.execute("PRAGMA table_info(batches)")):
        return
    try:
        con.execute("ALTER TABLE batches ADD COLUMN vm TEXT")
    except sqlite3.OperationalError as exc:
        if "duplicate column" not in str(exc).lower():
            raise


def batch_vm(row: Any, default: str) -> str:
    """The VM of a spool batch row: ``batches.vm``, else the note's ``vm=``, else ``default``."""
    keys = row.keys() if hasattr(row, "keys") else ()
    if "vm" in keys and row["vm"]:
        return str(row["vm"])
    m = NOTE_VM_RE.search(row["note"] or "") if "note" in keys else None
    return m.group(1) if m else default


def _file_digests(path: Path) -> tuple[str, str]:
    sha, md5 = hashlib.sha256(), hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        while chunk := fh.read(8 << 20):
            sha.update(chunk)
            md5.update(chunk)
    return sha.hexdigest(), md5.hexdigest()


def write_spool_batch(rows: list[sqlite3.Row], scope: str, batch_id: str, vm: str, out_dir: Path
                      ) -> tuple[Path, Path, dict[str, Any]]:
    """Write one batch as ``<batch_id>.parquet`` + ``<batch_id>.json`` in ``out_dir``; returns (parquet, manifest, doc)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    changes = to_changes(rows, scope)
    cols: dict[str, list[Any]] = {f.name: [] for f in spool_schema()}
    for r, c in zip(rows, changes, strict=True):
        for k in ("irods_path", "target_type", "attribute", "value", "unit", "op", "actor", "ts", "source"):
            cols[k].append(getattr(c, k))
        cols["batch_id"].append(batch_id)
        cols["scope"].append(scope)
        cols["vm"].append(vm)
        cols["journal_id"].append(int(r["id"]))
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet = out_dir / f"{batch_id}.parquet"
    tmp = parquet.with_name(parquet.name + ".part")
    pq.write_table(pa.Table.from_pydict(cols, schema=spool_schema()), tmp, compression="zstd")
    os.replace(tmp, parquet)
    sha, md5 = _file_digests(parquet)
    doc = {"format": SPOOL_FORMAT, "batchId": batch_id, "scope": scope, "vm": vm, "host": socket.gethostname(),
           "rows": len(rows), "paths": len({r["path"] for r in rows}), "sha256": sha, "md5": md5,
           "bytes": parquet.stat().st_size, "createdAt": env.utcnow().isoformat(), "actor": env.ACTOR,
           "journalIds": [min(cols["journal_id"]), max(cols["journal_id"])] if rows else []}
    manifest = out_dir / f"{batch_id}.json"
    env.write_json(manifest, doc)
    return parquet, manifest, doc


def _upload_spool(session: Any, con: sqlite3.Connection, parquet: Path, manifest: Path, doc: dict[str, Any],
                  vm: str) -> str:
    """Parquet first, then the manifest, each verified against its iRODS checksum. Returns the remote dir."""
    from .. import coord, mirror

    remote = coord.spool_dir(vm)
    for local, md5 in ((parquet, doc["md5"]), (manifest, None)):
        stats = mirror.push_tree([(local, f"{remote}/{local.name}", md5)], threads=1, session=session, con=con)
        if stats.get("errors"):
            raise RuntimeError(f"spool upload of {local.name} failed: {stats.get('error_samples')}")
    return remote


def _mark_spooled(con: sqlite3.Connection, batch_id: str, note_suffix: str) -> int:
    con.execute("BEGIN IMMEDIATE")
    try:
        n = con.execute("UPDATE avu_journal SET status='spooled' WHERE batch_id=? AND status='applied'",
                        (batch_id,)).rowcount
        con.execute("UPDATE batches SET status='spooled', error=NULL, note=COALESCE(note, '') || ? WHERE batch_id=?",
                    (note_suffix, batch_id))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return n


def _in_spool_done(session: Any, vm: str, batch_id: str) -> str | None:
    """The path of the batch's Parquet in ``.mesa/spool-done/<vm>/`` (a ``.dup-*`` copy included), or None."""
    from .. import coord

    done = coord.spool_done_dir(vm)
    exact = coord.stat(session, f"{done}/{batch_id}.parquet")
    if exact is not None:
        return exact.path
    try:
        objs = coord.list_objects(session, done)
    except Exception as exc:  # noqa: BLE001 - a missing collection or a query error: not there
        log.debug("listing %s failed: %s", done, exc)
        return None
    return next((p for p in sorted(objs) if p.rsplit("/", 1)[-1].startswith(f"{batch_id}.parquet")), None)


def _finish_spooling(con: sqlite3.Connection, scope: str, session: Any, vm: str) -> dict[str, int]:
    """Complete batches left ``spooling`` by an interrupted call (this VM's, or another VM's after a journal
    restore): upload them from local files, trust a verified copy in the spool or a recorded copy in
    ``spool-done``, or release their rows when the batch is nowhere (see the module docstring)."""
    from .. import coord

    ensure_batches_vm(con)
    out = {"resumed": 0, "released": 0, "set_aside": 0}
    for b in con.execute("SELECT batch_id, note, vm FROM batches WHERE scope=? AND status='spooling'"
                         " ORDER BY created_at", (scope,)).fetchall():
        bid = b["batch_id"]
        bvm = batch_vm(b, vm)
        local_dir = spool_local_dir(bvm)
        remote = coord.spool_dir(bvm)
        parquet, manifest = local_dir / f"{bid}.parquet", local_dir / f"{bid}.json"
        if parquet.exists() and manifest.exists():
            doc = json.loads(manifest.read_text())
            _upload_spool(session, con, parquet, manifest, doc, bvm)
            _mark_spooled(con, bid, f" | spooled to {remote} (resumed)")
            parquet.unlink(missing_ok=True)
            manifest.unlink(missing_ok=True)
            out["resumed"] += 1
            continue
        obj = coord.stat(session, f"{remote}/{bid}.parquet")
        doc = coord.read_json(session, f"{remote}/{bid}.json")
        if obj is not None and isinstance(doc, dict) and obj.checksum and \
                obj.checksum.lower().removeprefix("md5:") == str(doc.get("md5", "")).lower():
            _mark_spooled(con, bid, f" | spooled to {remote} (verified remote copy)")
            out["resumed"] += 1
            continue
        done = _in_spool_done(session, bvm, bid)
        if done is not None:
            _mark_spooled(con, bid, f" | already taken by the recorder ({done})")
            out["resumed"] += 1
            continue
        if obj is not None:  # a Parquet without a matching manifest: never let the recorder see it
            aside = spool_aside_dir(bvm)
            for name in (f"{bid}.parquet", f"{bid}.json"):
                if coord.stat(session, f"{remote}/{name}") is not None:
                    moved = coord.move(session, f"{remote}/{name}", f"{aside}/{name}")
                    log.warning("spool batch %s: %s moved aside to %s (no matching manifest)", bid, name, moved)
            out["set_aside"] += 1
        parquet.unlink(missing_ok=True)
        manifest.unlink(missing_ok=True)
        _release(con, bid, "spool files lost before a verified upload; rows re-spooled")
        out["released"] += 1
    return out


SpoolKey = tuple[str, str, str, str]  # (irods path, attribute, value, unit)
_SPOOL_NAME_RE = re.compile(rf"^({_UUID})\.(parquet|json)(?:\.dup-.*)?$")


def spool_vms(con: sqlite3.Connection, scope: str) -> set[str]:
    """VMs whose spool may hold batches of ``scope``: this VM, the VMs of the scope's batches in this journal,
    and the VMs this host restored the site's journal from (``journal_sync_marks``)."""
    ensure_batches_vm(con)
    vms = {env.vm_id()}
    for r in con.execute("SELECT note, vm FROM batches WHERE scope=?", (scope,)):
        vms.add(batch_vm(r, env.vm_id()))
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='journal_sync_marks'").fetchone():
        vms |= {r[0] for r in con.execute("SELECT vm FROM journal_sync_marks WHERE site=? AND vm IS NOT NULL",
                                          (scope.upper(),))}
    return {v for v in vms if v and re.fullmatch(r"[A-Za-z0-9._-]+", v)}


def spooled_times(con: sqlite3.Connection, scope: str, *, session: Any, vms: set[str] | None = None
                  ) -> dict[SpoolKey, tuple[str, str]]:
    """The last spooled change of every AVU of ``scope`` in spool batches this journal does not know:
    ``(path, attribute, value, unit) -> (op, ts)`` (op ``add``/``remove`` as in the journal, ts ISO UTC).

    Such batches were spooled by another VM (or by this VM before its journal was restored from an older
    backup) and never reached this journal. ``bulk`` gives an adopted AVU or a reconciled intent the time of
    that change instead of now, so the recorder's content dedup (path, attribute, value, unit, op, ts) records
    the change once. Looks in ``.mesa/spool/<vm>/`` and ``.mesa/spool-done/<vm>/`` of :func:`spool_vms`;
    batches of other scopes are skipped by their manifest. A directory or batch that cannot be read is skipped
    (the caller then uses now)."""
    import pyarrow.parquet as pq

    from .. import coord

    known = {r[0] for r in con.execute("SELECT batch_id FROM batches")}
    out: dict[SpoolKey, tuple[datetime, str]] = {}
    tmp = env.WORK_DIR / "spool-scan" / f"{scope}-{os.getpid()}-{uuid.uuid4().hex[:8]}"  # private to this call
    for vm in sorted(vms if vms is not None else spool_vms(con, scope)):
        seen: set[str] = set()
        for d in (coord.spool_dir(vm), coord.spool_done_dir(vm)):
            try:
                objs = coord.list_objects(session, d)
            except Exception as exc:  # noqa: BLE001 - not there, or a query error: nothing known
                log.debug("listing %s failed: %s", d, exc)
                continue
            parts: dict[str, dict[str, coord.RemoteObject]] = {}
            for path, obj in sorted(objs.items()):
                m = _SPOOL_NAME_RE.match(posixpath.basename(path))
                if m and m.group(1) not in known:
                    parts.setdefault(m.group(1), {}).setdefault(m.group(2), obj)
            for bid, got in sorted(parts.items()):
                if bid in seen or "parquet" not in got:
                    continue
                doc = coord.read_json(session, got["json"].path) if "json" in got else None
                if isinstance(doc, dict) and doc.get("scope") not in (None, scope):
                    continue
                local = tmp / vm / f"{bid}.parquet"
                try:
                    coord.fetch(session, got["parquet"].path, local, obj=got["parquet"])
                    table = pq.read_table(local, columns=["irods_path", "attribute", "value", "unit", "op", "ts",
                                                          "scope"])
                except Exception as exc:  # noqa: BLE001 - unreadable: skipped, the caller uses now
                    log.warning("spool batch %s of %s not read: %s", bid, vm, exc)
                    continue
                finally:
                    local.unlink(missing_ok=True)
                seen.add(bid)
                for r in table.to_pylist():
                    if r["scope"] != scope or r["ts"] is None:
                        continue
                    ts = r["ts"] if r["ts"].tzinfo else r["ts"].replace(tzinfo=UTC)
                    key = (r["irods_path"], r["attribute"], r["value"], r["unit"] or "")
                    op = "add" if r["op"] == "add" else "remove"
                    if key not in out or ts >= out[key][0]:
                        out[key] = (ts, op)
    shutil.rmtree(tmp, ignore_errors=True)
    return {k: (op, ts.astimezone(UTC).isoformat()) for k, (ts, op) in out.items()}


def spool_batches(con: sqlite3.Connection, scope: str, *, session: Any | None = None,
                  rows_per_batch: int | None = None, vm: str | None = None) -> dict[str, Any]:
    """The ``spool`` backend of :func:`record_scope` (see the module docstring). Stops at the first failure."""
    from .. import coord

    rows_per_batch = rows_per_batch or rows_per_batch_default()
    vm = vm or env.vm_id()
    ensure_batches_vm(con)
    out: dict[str, Any] = {"mode": "spool", "vm": vm, "spool_dir": coord.spool_dir(vm), "batches_planned": 0,
                           "batches_spooled": 0, "rows_spooled": 0, "rows_per_batch": rows_per_batch,
                           "resumed": 0, "released": 0, "set_aside": 0, "batch_ids": []}
    with env.file_lock("mesa-spool", blocking=True):
        s = session if session is not None else env.mesa().session
        try:
            out.update(_finish_spooling(con, scope, s, vm))
        except Exception as exc:  # noqa: BLE001 - reported; the batch stays 'spooling' for the next call
            out["error"] = f"{type(exc).__name__}: {exc}"[:500]
            out["failed_batch"] = "resume"
            log.error("spool %s: completing an interrupted batch failed: %s", scope, exc)
            return out
        batches = plan_batches(con, scope, rows_per_batch)
        total = out["batches_planned"] = len(batches)
        for k, rows in enumerate(batches, 1):
            batch_id = str(uuid.uuid4())
            n_paths = len({r["path"] for r in rows})
            note = f"neon-ducklake {scope} spool batch={batch_id} {k}/{total} paths={n_paths} avus={len(rows)} vm={vm}"
            con.execute("BEGIN IMMEDIATE")
            try:
                con.execute("INSERT INTO batches(batch_id, scope, seq, total, note, n_rows, status, created_at, vm)"
                            " VALUES (?,?,?,?,?,?, 'spooling', ?, ?)",
                            (batch_id, scope, k, total, note, len(rows), env.utcnow().isoformat(), vm))
                con.executemany("UPDATE avu_journal SET batch_id=? WHERE id=?", [(batch_id, r["id"]) for r in rows])
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
            t0 = time.monotonic()
            try:
                parquet, manifest, doc = write_spool_batch(rows, scope, batch_id, vm, spool_local_dir(vm))
                remote = _upload_spool(s, con, parquet, manifest, doc, vm)
            except Exception as exc:  # noqa: BLE001 - the batch stays 'spooling'; the next call completes it
                err = f"{type(exc).__name__}: {exc}"[:500]
                con.execute("UPDATE batches SET error=? WHERE batch_id=?", (err, batch_id))
                log.error("spool batch %d/%d (%s) failed: %s", k, total, batch_id, err)
                out["failed_batch"], out["error"] = batch_id, err
                return out
            secs = max(time.monotonic() - t0, 1e-6)
            _mark_spooled(con, batch_id, f" | spooled {len(rows)} rows ({doc['bytes']} B) to {remote} in {secs:.1f}s")
            parquet.unlink(missing_ok=True)
            manifest.unlink(missing_ok=True)
            out["batches_spooled"] += 1
            out["rows_spooled"] += len(rows)
            out["batch_ids"].append(batch_id)
            log.info("spooled batch %d/%d: %s, %d rows over %d paths in %.1fs", k, total, batch_id, len(rows),
                     n_paths, secs)
    return out


# ---------------------------------------------------------------- entry points

def _guard_catalog() -> None:
    holders = env.catalog_lock_holders()
    if holders:
        pids = ", ".join(f"{h['pid']} ({h['cmd'][:60]})" for h in holders)
        raise SystemExit(f"MESA catalog is held by PID(s) {pids}; close that session and re-run")


def _open() -> tuple[Any, Any, Any]:
    m = env.mesa()
    client = env.mesa_ducklake_client(m)
    project = client.find_project_by_path(env.IRODS_ROOT)
    if project is None:
        raise SystemExit(f"{env.IRODS_ROOT} is not a MESA project yet: run `neon-ducklake enroll` first")
    return m, client, project


def record_scope(scope: str, *, rows_per_batch: int | None = None, con: sqlite3.Connection | None = None,
                 mode: str | None = None, session: Any | None = None) -> dict[str, Any]:
    """Record every applied journal row of ``scope`` with the history backend ``mode``.

    ``mode`` defaults to ``$NEON_DUCKLAKE_HISTORY_MODE`` or ``direct``:

    - ``direct``: recover, then record into MESA history (the snapshot budget guard runs first);
    - ``spool``: write the batches to the Data Store spool (:func:`spool_batches`; ``session`` is the iRODS
      session, default ``env.mesa().session``); the recorder records them later.

    ``rows_per_batch`` defaults to ``NEON_DUCKLAKE_ROWS_PER_BATCH`` or 250,000.
    """
    rows_per_batch = rows_per_batch or rows_per_batch_default()
    con = con or state.connect()
    if history_mode(mode) == "spool":
        out = spool_batches(con, scope, session=session, rows_per_batch=rows_per_batch)
        out["blocked_rows"] = 0
        return out
    with env.file_lock("mesa-tag", blocking=True, timeout_s=env.mesa_lock_timeout()):
        _guard_catalog()
        try:
            m, client, project = _open()
            rec = recover(client, project, m.session, con)
            out = record_batches(client, project.project_id, m.session, con, scope, rows_per_batch)
        finally:
            env.close_mesa_ducklake_client()
    out["recover"] = rec
    out["mode"] = "direct"
    out["blocked_rows"] = con.execute(
        "SELECT count(*) FROM avu_journal j JOIN batches b ON b.batch_id=j.batch_id"
        " WHERE j.scope=? AND j.status='applied' AND b.status='pending'", (scope,)).fetchone()[0]
    return out


def recover_main() -> int:
    """``neon-ducklake recover``: drain MESA pending pushes and reconcile batches."""
    con = state.connect()
    with env.file_lock("mesa-tag"):
        _guard_catalog()
        try:
            m, client, project = _open()
            rec = recover(client, project, m.session, con)
        finally:
            env.close_mesa_ducklake_client()
    print(f"project {project.project_id} ({project.irods_path})")
    for k, v in rec.items():
        print(f"  {k:<15} {v}")
    return 1 if rec["blocked"] else 0
