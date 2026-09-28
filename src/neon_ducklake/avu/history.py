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

import logging
import os
import re
import shutil
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import env, state

log = logging.getLogger("neon_ducklake.avu.history")

BATCH_RE = re.compile(r"\bbatch=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b")
DEAD_PARQUET = ("pending", "failed")
DEFAULT_ROWS_PER_BATCH = 250_000
ROWS_PER_BATCH_ENV = "NEON_DUCKLAKE_ROWS_PER_BATCH"
SNAPSHOT_BUDGET = 900  # refuse to record past this many project snapshots (MESA caches at most 1000)
SNAPSHOT_LIST_LIMIT = 1000


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


def batch_id_of(note: str | None) -> str | None:
    m = BATCH_RE.search(note or "")
    return m.group(1) if m else None


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
    from mesa_ducklake import AvuChange

    out = []
    for r in rows:
        if not r["applied_at"]:
            raise ValueError(f"journal row {r['id']} is applied without applied_at")
        out.append(AvuChange(irods_path=r["path"], target_type=r["target_type"], attribute=r["attribute"],
                             value=r["value"], unit=r["unit"] or "", op="add" if r["op"] == "add" else "delete",
                             actor=env.ACTOR, ts=parse_ts(r["applied_at"]),
                             source=f"neon-ducklake:{r['generator'] or scope}"))
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
                        " AND status='applied'", (snapshot_id, batch_id)).rowcount
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
        bid = batch_id_of(by_id[p.snapshot_id].note)
        expected = batch_rows.get(bid) if bid else None
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
    committed: dict[str, int] = {}
    pending: dict[str, int] = {}
    for s in snaps:
        bid = batch_id_of(s.note)
        if not bid:
            continue
        if s.parquet_file not in DEAD_PARQUET:
            committed[bid] = s.snapshot_id
        elif s.parquet_file == "pending" and s.snapshot_id in wal_local and wal_local[s.snapshot_id].exists():
            pending[bid] = s.snapshot_id  # still in flight with a valid local file: must not be re-recorded
    open_batches = con.execute("SELECT batch_id, status FROM batches WHERE status != 'recorded'").fetchall()
    for b in open_batches:
        bid = b["batch_id"]
        if bid in committed:
            summary["recorded"] += _mark_recorded(con, bid, committed[bid])
        elif bid in pending:
            con.execute("UPDATE batches SET status='pending', snapshot_id=? WHERE batch_id=?", (pending[bid], bid))
            summary["blocked"].append({"batch_id": bid, "snapshot_id": pending[bid]})
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


def record_scope(scope: str, *, rows_per_batch: int | None = None,
                 con: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Recover, then record every applied journal row of ``scope`` into MESA history.

    ``rows_per_batch`` defaults to ``NEON_DUCKLAKE_ROWS_PER_BATCH`` or 250,000. The snapshot budget
    guard (see module docstring) runs before any batch is written.
    """
    rows_per_batch = rows_per_batch or rows_per_batch_default()
    con = con or state.connect()
    with env.file_lock("mesa-tag"):
        _guard_catalog()
        try:
            m, client, project = _open()
            rec = recover(client, project, m.session, con)
            out = record_batches(client, project.project_id, m.session, con, scope, rows_per_batch)
        finally:
            env.close_mesa_ducklake_client()
    out["recover"] = rec
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
