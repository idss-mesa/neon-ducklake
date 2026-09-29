"""``neon-ducklake recorder [--watch --interval 60] [--max-rows N] [--min-rows N] [--max-age-h H] [--flush]
[--dry-run]``: spool -> MESA history.

Runs on the one host that owns the MESA catalog (``$MESA_HOME/ducklake/catalog.duckdb``). Workers write
their AVU history as spool batches (``avu.history`` ``spool`` mode) to ``<IRODS_ROOT>/.mesa/spool/<vm>/``:
``<batch_id>.parquet`` plus a ``<batch_id>.json`` manifest (rows, sha256, md5, createdAt). One pass:

1. Lists the spool (one GenQuery). A batch needs both files; a Parquet without its manifest is still being
   uploaded and is left alone. When nothing is spooled the MESA catalog is never opened.
2. Waits for the global ``mesa-tag`` file lock (up to ``NEON_DUCKLAKE_MESA_LOCK_TIMEOUT_S``, default
   1800 s), refuses while another process holds the MESA catalog (``env.catalog_lock_holders()``), empties
   ``work/spool-in/``, opens the one ``DuckLakeClient`` and runs ``history.recover``.
3. Skips (idempotency) every batch id already in a committed snapshot note, including the multi-batch
   notes this recorder writes, and holds back batches whose snapshot is still pending.
4. **Coalescing** (the MESA project has a 900-snapshot budget for ~81 sites): the rest, in order
   (createdAt, vm, batch id), is cut into groups of at most ``max_rows`` rows (default
   ``NEON_DUCKLAKE_ROWS_PER_BATCH`` or 250,000; a batch is never split). A group is recorded only when it
   holds at least ``min_rows`` rows (default 200,000), its oldest batch is older than ``max_age_h`` hours
   (default 12), or ``flush`` is given; every older group is recorded with it, so the order holds. The rest
   waits for the next pass. The snapshot budget guard runs first.
5. Each batch is downloaded to ``work/spool-in/<vm>/``, checked against its manifest (sha256 and row count)
   and read as ``AvuChange`` rows. A batch that cannot be fetched or checked is reported and left in the
   spool, and the later batches of the same VM wait for the next pass (a VM's batches are recorded in
   order).
6. **Content dedup**: every recorded row's key ``sha1(irods_path, attribute, value, unit, op, ts)`` (the
   first 16 bytes, as a UUID) is kept in ``work/recorder_seen.duckdb``. Rows whose key is already there
   (the same change spooled again under another batch id, e.g. after a VM resumed from an older journal
   backup) are skipped and counted (``duplicates_skipped``); a group with nothing new records no snapshot.
7. The snapshot note is ``neon-ducklake spool batch=<id>[,<id>...] n=<batches> rows=<rows>``. After a commit
   the recorder indexes the keys, marks the batches ``recorded`` in the local ``batches`` table (and any
   local journal rows of those batches, when a worker shares this host) and moves both files to
   ``<IRODS_ROOT>/.mesa/spool-done/<vm>/``. Spool files are never deleted. A crash between the commit and
   the move is repaired by the next pass (step 3, which also indexes the batch's keys when they are missing).

``--watch`` repeats the pass every ``--interval`` seconds. A pass that fails (the MESA catalog held by another
process, the lock wait timed out, an iRODS error) is logged and retried at the next interval; an error after
a group was committed is reported and the next group still runs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import posixpath
import shutil
import signal
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import coord, env, state
from . import history

log = logging.getLogger("neon_ducklake.avu.recorder")

DEFAULT_MIN_ROWS = 200_000
DEFAULT_MAX_AGE_H = 12.0
MIN_ROWS_ENV = "NEON_DUCKLAKE_RECORDER_MIN_ROWS"
MAX_AGE_ENV = "NEON_DUCKLAKE_RECORDER_MAX_AGE_H"


@dataclass
class SpoolBatch:
    vm: str
    batch_id: str
    parquet: str  # remote paths
    manifest: str
    doc: dict[str, Any]
    obj: coord.RemoteObject  # the remote Parquet

    @property
    def rows(self) -> int:
        return int(self.doc.get("rows") or 0)

    @property
    def scope(self) -> str:
        return str(self.doc.get("scope") or "spool")

    @property
    def created_at(self) -> str:
        return str(self.doc.get("createdAt") or "")

    def age_h(self, now: datetime) -> float:
        try:
            ts = datetime.fromisoformat(self.created_at)
        except ValueError:
            return float("inf")  # unknown age: never let it wait forever
        ts = ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
        return (now - ts).total_seconds() / 3600.0


def spool_root() -> str:
    return f"{env.IRODS_ROOT}/.mesa/spool"


def list_spool(session: Any) -> tuple[list[SpoolBatch], dict[str, list[str]]]:
    """(complete batches in ingest order, {"incomplete": [...], "orphans": [...], "invalid": [...]})."""
    objs = coord.list_objects(session, spool_root(), recursive=True)
    by_key: dict[tuple[str, str], dict[str, coord.RemoteObject]] = {}
    for path, obj in objs.items():
        stem, ext = posixpath.splitext(posixpath.basename(path))
        if ext not in (".parquet", ".json"):
            continue
        vm = posixpath.basename(posixpath.dirname(path))
        by_key.setdefault((vm, stem), {})[ext] = obj
    batches: list[SpoolBatch] = []
    issues: dict[str, list[str]] = {"incomplete": [], "orphans": [], "invalid": []}
    for (vm, stem), parts in sorted(by_key.items()):
        if ".json" not in parts:
            issues["incomplete"].append(parts[".parquet"].path)
            continue
        if ".parquet" not in parts:
            issues["orphans"].append(parts[".json"].path)
            continue
        doc = coord.read_json(session, parts[".json"].path)
        if not isinstance(doc, dict) or doc.get("batchId") != stem or not doc.get("sha256"):
            issues["invalid"].append(parts[".json"].path)
            continue
        batches.append(SpoolBatch(vm, stem, parts[".parquet"].path, parts[".json"].path, doc, parts[".parquet"]))
    batches.sort(key=lambda b: (b.created_at, b.vm, b.batch_id))
    return batches, issues


def plan_groups(batches: list[SpoolBatch], max_rows: int) -> list[list[SpoolBatch]]:
    """Consecutive groups of at most ``max_rows`` rows; a larger batch is a group of its own."""
    groups: list[list[SpoolBatch]] = []
    cur: list[SpoolBatch] = []
    n = 0
    for b in batches:
        if cur and n + b.rows > max_rows:
            groups.append(cur)
            cur, n = [], 0
        cur.append(b)
        n += b.rows
    if cur:
        groups.append(cur)
    return groups


def eligible_groups(groups: list[list[SpoolBatch]], *, min_rows: int, max_age_h: float, flush: bool,
                    now: datetime) -> tuple[list[list[SpoolBatch]], list[list[SpoolBatch]]]:
    """(groups to record now, groups that wait). A group is due with ``min_rows`` rows, a batch older than
    ``max_age_h`` hours, or ``flush``; every group before the last due one is recorded too (order)."""
    last = -1
    for i, g in enumerate(groups):
        if flush or sum(b.rows for b in g) >= min_rows or max(b.age_h(now) for b in g) >= max_age_h:
            last = i
    return groups[: last + 1], groups[last + 1:]


def _env_number(var: str, default: float) -> float:
    raw = (os.environ.get(var) or "").strip().replace("_", "")
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SystemExit(f"{var}={raw!r} is not a number") from exc


def min_rows_default() -> int:
    return int(_env_number(MIN_ROWS_ENV, DEFAULT_MIN_ROWS))


def max_age_h_default() -> float:
    return _env_number(MAX_AGE_ENV, DEFAULT_MAX_AGE_H)


def inbox_root() -> Path:
    return env.WORK_DIR / "spool-in"


def inbox_dir(vm: str) -> Path:
    return inbox_root() / vm


# ---------------------------------------------------------------- content dedup index

def change_key(c: Any) -> str:
    """``sha1(irods_path, attribute, value, unit, op, ts)`` (first 16 bytes, as a UUID string)."""
    ts = c.ts if isinstance(c.ts, datetime) else datetime.fromisoformat(str(c.ts))
    ts = ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)
    raw = "\x1f".join((c.irods_path, c.attribute, c.value, c.unit or "", c.op, ts.isoformat(timespec="microseconds")))
    return str(uuid.UUID(bytes=hashlib.sha1(raw.encode("utf-8"), usedforsecurity=False).digest()[:16]))


def seen_path() -> Path:
    return env.WORK_DIR / "recorder_seen.duckdb"


class SeenIndex:
    """``work/recorder_seen.duckdb``: the content key of every row this recorder host has recorded."""

    def __init__(self, path: Path | None = None) -> None:
        import duckdb

        self.path = path or seen_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path))
        self.con.execute("CREATE TABLE IF NOT EXISTS seen (key UUID NOT NULL, batch_id VARCHAR, snapshot_id BIGINT,"
                         " recorded_at TIMESTAMPTZ)")

    def known(self, keys: list[str]) -> set[str]:
        if not keys:
            return set()
        import pyarrow as pa

        self.con.register("probe", pa.table({"key": pa.array(keys, pa.string())}))
        try:
            return {str(r[0]) for r in self.con.execute(
                "SELECT DISTINCT s.key FROM seen s JOIN (SELECT DISTINCT key::UUID AS key FROM probe) p"
                " ON s.key = p.key").fetchall()}
        finally:
            self.con.unregister("probe")

    def has_batch(self, batch_id: str) -> bool:
        return self.con.execute("SELECT 1 FROM seen WHERE batch_id=? LIMIT 1", [batch_id]).fetchone() is not None

    def add(self, keys_by_batch: dict[str, list[str]], snapshot_id: int | None) -> int:
        import pyarrow as pa

        ks = [k for ks in keys_by_batch.values() for k in ks]
        if not ks:
            return 0
        bids = [b for b, ks_ in keys_by_batch.items() for _ in ks_]
        self.con.register("incoming", pa.table({"key": pa.array(ks, pa.string()), "batch_id": pa.array(bids)}))
        try:
            self.con.execute("INSERT INTO seen SELECT key::UUID, batch_id, ?, now() FROM incoming", [snapshot_id])
        finally:
            self.con.unregister("incoming")
        return len(ks)

    def count(self) -> int:
        return int(self.con.execute("SELECT count(*) FROM seen").fetchone()[0])

    def close(self) -> None:
        self.con.close()


def read_changes(path: Path) -> list[Any]:
    import pyarrow.parquet as pq
    from mesa_ducklake import AvuChange

    out = []
    for r in pq.read_table(path).to_pylist():
        out.append(AvuChange(irods_path=r["irods_path"], target_type=r["target_type"], attribute=r["attribute"],
                             value=r["value"], unit=r["unit"] or "", op=r["op"], actor=r["actor"], ts=r["ts"],
                             source=r["source"]))
    return out


def fetch_batch(session: Any, b: SpoolBatch) -> tuple[Path, list[Any]]:
    """Download and check one batch against its manifest; returns (local file, changes)."""
    local = inbox_dir(b.vm) / f"{b.batch_id}.parquet"
    coord.fetch(session, b.parquet, local, obj=b.obj)
    sha, _md5 = history._file_digests(local)
    if sha != b.doc.get("sha256"):
        local.unlink(missing_ok=True)
        raise ValueError(f"sha256 {sha} != manifest {b.doc.get('sha256')}")
    changes = read_changes(local)
    if len(changes) != b.rows:
        local.unlink(missing_ok=True)
        raise ValueError(f"{len(changes)} rows != manifest {b.rows}")
    return local, changes


def move_done(session: Any, b: SpoolBatch) -> list[str]:
    done = coord.spool_done_dir(b.vm)
    return [coord.move(session, src, f"{done}/{posixpath.basename(src)}") for src in (b.parquet, b.manifest)]


def _register(con: sqlite3.Connection, group: list[SpoolBatch], note: str) -> None:
    now = env.utcnow().isoformat()
    con.execute("BEGIN IMMEDIATE")
    try:
        for b in group:
            con.execute(
                "INSERT INTO batches(batch_id, scope, note, n_rows, status, created_at) VALUES (?,?,?,?, 'recording', ?)"
                " ON CONFLICT(batch_id) DO UPDATE SET status='recording', note=excluded.note, n_rows=excluded.n_rows,"
                " snapshot_id=NULL, error=NULL WHERE batches.status != 'recorded'",
                (b.batch_id, b.scope, note, b.rows, now))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def _mark(con: sqlite3.Connection, ids: list[str], snapshot_id: int | None, suffix: str = "") -> None:
    now = env.utcnow().isoformat()
    con.execute("BEGIN IMMEDIATE")
    try:
        for bid in ids:
            con.execute("UPDATE avu_journal SET status='recorded', snapshot_id=? WHERE batch_id=?"
                        " AND status IN ('spooled', 'applied')", (snapshot_id, bid))
            con.execute("INSERT INTO batches(batch_id, scope, status, snapshot_id, created_at, recorded_at, note)"
                        " VALUES (?, 'spool', 'recorded', ?, ?, ?, ?) ON CONFLICT(batch_id) DO UPDATE SET"
                        " status='recorded', snapshot_id=excluded.snapshot_id, recorded_at=excluded.recorded_at,"
                        " error=NULL, note=COALESCE(batches.note, '') || ?",
                        (bid, snapshot_id, now, now, suffix.strip(" |"), suffix))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def _dedup(index: SeenIndex, fetched: list[tuple[SpoolBatch, Path, list[Any]]]
           ) -> tuple[list[Any], dict[str, list[str]], int]:
    """(new changes, their keys by batch id, duplicates skipped) over the group's fetched batches."""
    keys = {b.batch_id: [change_key(c) for c in ch] for b, _, ch in fetched}
    known = index.known([k for ks in keys.values() for k in ks])
    new: list[Any] = []
    new_keys: dict[str, list[str]] = {}
    dup = 0
    for b, _, ch in fetched:
        for c, k in zip(ch, keys[b.batch_id], strict=True):
            if k in known:
                dup += 1
                continue
            known.add(k)
            new.append(c)
            new_keys.setdefault(b.batch_id, []).append(k)
    return new, new_keys, dup


def ingest(*, session: Any | None = None, con: sqlite3.Connection | None = None, max_rows: int | None = None,
           min_rows: int | None = None, max_age_h: float | None = None, flush: bool = False,
           dry_run: bool = False, client: Any | None = None, project: Any | None = None,
           mesa_session: Any | None = None, snapshot_budget: int = history.SNAPSHOT_BUDGET,
           now: datetime | None = None) -> dict[str, Any]:
    """One recorder pass (see the module docstring). ``client``/``project``/``now`` are injected by tests."""
    s = coord.get_session(session)
    con = con or state.connect()
    max_rows = max_rows or history.rows_per_batch_default()
    min_rows = min_rows_default() if min_rows is None else min_rows
    max_age_h = max_age_h_default() if max_age_h is None else max_age_h
    now = now or env.utcnow()
    batches, issues = list_spool(s)
    out: dict[str, Any] = {"spooled": len(batches), "spooled_rows": sum(b.rows for b in batches),
                           "max_rows": max_rows, "min_rows": min_rows, "max_age_h": max_age_h, "flush": flush,
                           "snapshots": [], "batches_recorded": 0, "rows_recorded": 0, "duplicates_skipped": 0,
                           "already_committed": 0, "blocked": [], "errors": [], "moved": 0, "waiting_batches": 0,
                           "waiting_rows": 0, "deferred": [], **{k: v for k, v in issues.items() if v}}
    if batches:
        out["oldest_age_h"] = round(max(b.age_h(now) for b in batches), 2)
    if not batches and not issues["orphans"]:
        return out
    if dry_run:
        due, wait = eligible_groups(plan_groups(batches, max_rows), min_rows=min_rows, max_age_h=max_age_h,
                                    flush=flush, now=now)
        out["groups"] = [[b.batch_id for b in g] for g in due]
        out["waiting_batches"] = sum(len(g) for g in wait)
        out["waiting_rows"] = sum(b.rows for g in wait for b in g)
        return out
    with env.file_lock("mesa-tag", blocking=True, timeout_s=env.mesa_lock_timeout()):
        history._guard_catalog()
        shutil.rmtree(inbox_root(), ignore_errors=True)
        own = client is None
        index: SeenIndex | None = None
        try:
            if own:
                m, client, project = history._open()
                mesa_session = m.session
            index = SeenIndex()
            out["recover"] = history.recover(client, project, mesa_session, con)
            committed, pending = history.snapshot_batches(history._catalog_snapshots(client, project.project_id))
            todo: list[SpoolBatch] = []
            for b in batches:
                if b.batch_id in committed:
                    try:
                        if not index.has_batch(b.batch_id):  # a crash between commit and indexing
                            local, ch = fetch_batch(s, b)
                            index.add({b.batch_id: [change_key(c) for c in ch]}, committed[b.batch_id])
                            local.unlink(missing_ok=True)
                        _mark(con, [b.batch_id], committed[b.batch_id])
                        out["moved"] += len(move_done(s, b))
                        out["already_committed"] += 1
                    except Exception as exc:  # noqa: BLE001 - retried by the next pass
                        log.error("committed spool batch %s (%s) not finished: %s", b.batch_id, b.vm, exc)
                        out["errors"].append({"batch_id": b.batch_id, "vm": b.vm, "error": str(exc)[:300]})
                elif b.batch_id in pending:
                    out["blocked"].append({"batch_id": b.batch_id, "snapshot_id": pending[b.batch_id]})
                else:
                    todo.append(b)
            for path in issues["orphans"]:  # a manifest whose Parquet was already moved by a crashed pass
                bid = posixpath.splitext(posixpath.basename(path))[0]
                if bid in committed:
                    vm = posixpath.basename(posixpath.dirname(path))
                    try:
                        coord.move(s, path, f"{coord.spool_done_dir(vm)}/{posixpath.basename(path)}")
                        out["moved"] += 1
                    except Exception as exc:  # noqa: BLE001
                        out["errors"].append({"batch_id": bid, "vm": vm, "error": str(exc)[:300]})
            groups, waiting = eligible_groups(plan_groups(todo, max_rows), min_rows=min_rows, max_age_h=max_age_h,
                                              flush=flush, now=now)
            out["waiting_batches"] = sum(len(g) for g in waiting)
            out["waiting_rows"] = sum(b.rows for g in waiting for b in g)
            if groups:
                out["snapshots_before"] = history.check_snapshot_budget(client, project.project_id, len(groups),
                                                                        snapshot_budget)
            blocked_vms: set[str] = set()
            for g in groups:
                fetched: list[tuple[SpoolBatch, Path, list[Any]]] = []
                try:
                    for b in g:
                        if b.vm in blocked_vms:
                            out["deferred"].append(b.batch_id)
                            continue
                        try:
                            local, ch = fetch_batch(s, b)
                        except Exception as exc:  # noqa: BLE001 - this VM's later batches wait
                            log.error("spool batch %s (%s) not fetched: %s; later batches of %s wait", b.batch_id,
                                      b.vm, exc, b.vm)
                            out["errors"].append({"batch_id": b.batch_id, "vm": b.vm, "error": str(exc)[:300]})
                            blocked_vms.add(b.vm)
                            continue
                        fetched.append((b, local, ch))
                    if not fetched:
                        continue
                    ids = [b.batch_id for b, _, _ in fetched]
                    changes, keys, dup = _dedup(index, fetched)
                    out["duplicates_skipped"] += dup
                    if not changes:  # everything was recorded before under other batch ids
                        _mark(con, ids, None, f" | all {dup} row(s) already recorded (content dedup)")
                        for b, _, _ in fetched:
                            out["moved"] += len(move_done(s, b))
                        out["batches_recorded"] += len(ids)
                        log.warning("spool batch(es) %s: every row was already recorded; no snapshot", ids)
                        continue
                    note = f"neon-ducklake spool batch={','.join(ids)} n={len(ids)} rows={len(changes)}"
                    _register(con, [b for b, _, _ in fetched], note)
                    t0 = time.monotonic()
                    try:
                        snap = client.record_changes(project.project_id, env.ACTOR, changes, note=note,
                                                     session=mesa_session)
                    except Exception as exc:  # noqa: BLE001 - left for `recover` and the next pass
                        err = f"{type(exc).__name__}: {exc}"[:500]
                        con.executemany("UPDATE batches SET status='failed', error=? WHERE batch_id=?",
                                        [(err, i) for i in ids])
                        log.error("recording %d spool batch(es) failed: %s; run `neon-ducklake recover`", len(ids),
                                  err)
                        out["failed_batches"], out["error"] = ids, err
                        break
                    secs = max(time.monotonic() - t0, 1e-6)
                    out["snapshots"].append(snap.snapshot_id)
                    out["batches_recorded"] += len(ids)
                    out["rows_recorded"] += len(changes)
                    log.info("snapshot %s: %d spool batch(es), %d rows (%d duplicate(s) skipped) in %.1fs",
                             snap.snapshot_id, len(ids), len(changes), dup, secs)
                    index.add(keys, snap.snapshot_id)
                    _mark(con, ids, snap.snapshot_id,
                          f" | recorded by the recorder: {len(changes)} rows in {secs:.1f}s "
                          f"({len(changes) / secs:.0f} rows/s)" + (f", {dup} duplicate(s) skipped" if dup else ""))
                    for b, _, _ in fetched:
                        out["moved"] += len(move_done(s, b))
                except Exception as exc:  # after a commit: the next pass repairs it (step 3)
                    log.exception("recorder group %s failed", [b.batch_id for b in g])
                    out["errors"].append({"batch_id": ",".join(b.batch_id for b in g), "vm": "",
                                          "error": f"{type(exc).__name__}: {exc}"[:300]})
                finally:
                    for _, local, _ in fetched:
                        local.unlink(missing_ok=True)
        finally:
            if index is not None:
                index.close()
            if own:
                env.close_mesa_ducklake_client()
            shutil.rmtree(inbox_root(), ignore_errors=True)
    return out


def _show(out: dict[str, Any]) -> None:
    keys = ("spooled", "spooled_rows", "oldest_age_h", "already_committed", "batches_recorded", "rows_recorded",
            "duplicates_skipped", "snapshots", "waiting_batches", "waiting_rows", "deferred", "moved", "blocked",
            "errors", "incomplete", "orphans", "invalid", "groups", "error")
    for k in keys:
        if k in out and out[k] not in (None, [], 0) or k in ("spooled", "batches_recorded"):
            v = out.get(k)
            print(f"  {k:<18} {json.dumps(v, default=str)[:300] if isinstance(v, list | dict) else v}")


def main(*, watch: bool = False, interval: float = 60.0, max_rows: int | None = None, dry_run: bool = False,
         min_rows: int | None = None, max_age_h: float | None = None, flush: bool = False,
         session: Any | None = None, passes: int | None = None, sleep: Callable[[float], None] = time.sleep,
         ingest_fn: Callable[..., dict[str, Any]] | None = None) -> int:
    """``neon-ducklake recorder``: one pass, or ``--watch`` every ``interval`` seconds until SIGINT/SIGTERM.

    With ``--watch`` no pass error stops the loop (it is logged and the next pass retries)."""
    stop = {"flag": False}

    def _stop(signum: int, _frame: Any) -> None:
        stop["flag"] = True
        log.warning("recorder: signal %d, stopping after this pass", signum)

    old = {}
    if watch:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                old[sig] = signal.signal(sig, _stop)
            except ValueError:  # not the main thread (tests)
                pass
    rc = 0
    n = 0
    try:
        while True:
            n += 1
            try:
                out = (ingest_fn or ingest)(session=session, max_rows=max_rows, min_rows=min_rows,
                                            max_age_h=max_age_h, flush=flush, dry_run=dry_run)
            except (SystemExit, Exception) as exc:  # the MESA catalog is held, the budget, an iRODS error, ...
                if not watch:
                    raise
                log.warning("recorder pass %d failed: %s: %s", n, type(exc).__name__, exc)
                out = {"error": f"{type(exc).__name__}: {exc}"[:500]}
            print(f"== recorder pass {n} at {env.utcnow().isoformat(timespec='seconds')}"
                  f"{' (dry run)' if dry_run else ''}")
            _show(out)
            failed = bool(out.get("error") or out.get("errors") or out.get("invalid"))
            rc = 1 if failed else 0
            if not watch or stop["flag"] or (passes is not None and n >= passes):
                break
            waited = 0.0
            while waited < interval and not stop["flag"]:
                step = min(1.0, interval - waited)
                sleep(step)
                waited += step
            if stop["flag"]:
                break
    finally:
        for sig, h in old.items():
            signal.signal(sig, h)
    return rc


def cli(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="neon-ducklake recorder", description=__doc__.split("\n\n")[0])
    ap.add_argument("--watch", action="store_true")
    ap.add_argument("--interval", type=float, default=60.0)
    ap.add_argument("--max-rows", type=int, help="rows per snapshot at most (default 250,000)")
    ap.add_argument("--min-rows", type=int, help=f"record only with this many rows pending (default "
                                                 f"{DEFAULT_MIN_ROWS:,}, ${MIN_ROWS_ENV})")
    ap.add_argument("--max-age-h", type=float, help=f"... or when a batch is older than this (default "
                                                    f"{DEFAULT_MAX_AGE_H:g} h, ${MAX_AGE_ENV})")
    ap.add_argument("--flush", action="store_true", help="record everything pending now")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    env.setup_logging("warning")
    return main(watch=a.watch, interval=a.interval, max_rows=a.max_rows, dry_run=a.dry_run, min_rows=a.min_rows,
                max_age_h=a.max_age_h, flush=a.flush)


if __name__ == "__main__":
    import sys

    sys.exit(cli())
