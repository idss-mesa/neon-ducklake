"""Chunk planning and the ``stream_chunks`` journal (docs/STREAMING.md § Chunks).

A chunk is a bounded slice of one site's listed NEON files: ONE product, ascending months, at most
``max_bytes`` (``$NEON_DUCKLAKE_CHUNK_BYTES``, default 8 GB). Months are packed greedily in ascending
order; a single month larger than ``max_bytes`` is a chunk by itself. ``chunk_id`` is stable:
``<product>__<firstMonth>_<lastMonth>``.

Sources: the journal ``site_files`` rows of the site (after ``site list``), minus superseded files.

Stability rules of :func:`plan_chunks` (re-planning is safe at any time):

- a chunk that has started (its status is no longer ``planned``) is frozen: its months never move to
  another chunk, and months that appear later are packed around it;
- ``planned`` rows that the new plan no longer contains (and that overlap the months planned now) are
  removed, so the journal never holds two chunks covering the same month;
- a finished chunk (``deleted``) whose months gained files to download (a re-list after a new release)
  is reset to ``planned`` so its stages run again for the new files;
- a chunk reset to ``planned`` or a new chunk (new months after a re-list) resets the site's finalize steps.

Journal (``work/state.sqlite``, created here with ``CREATE TABLE IF NOT EXISTS``):

``stream_chunks(site, chunk_id, product, months JSON, n_files, bytes, status, attempt, started_at,
finished_at, error, PK(site, chunk_id))`` plus three module columns: ``done`` (the last status reached
successfully, so a ``failed`` chunk knows where to resume), ``info`` (JSON: per-stage summaries) and
``planned_at``. ``stream_sites(site, step, status, ...)`` records the finalize/cleanup steps.
``lake_pushes(site, catalog_md5, pushed_at, snapshot_id, kind, vm, snapshot_key)`` records every site-lake
catalog this host pushed to (``kind='push'``) or restored from (``kind='restore'``) the Data Store; ``snapshot_key``
(``id|time|message`` of that catalog's last snapshot) ties a row to a lake: a row counts for the local lake only
when the local catalog holds that snapshot (a journal copied from another VM proves nothing about this lake).
The catalog guard of ``upload.push_lake`` compares the Data Store copy with the last such row.

A chunk reset to ``planned`` (new files after it finished), or a new chunk of a site that has finalize steps
journaled (new months after a re-list), also resets the site's finalize steps, so the product/site AVUs, cards
and the finalize upload are rebuilt from the grown lake.

Statuses, in order: planned → downloaded → stacked → avus → uploaded → applied → recorded → verified →
deleted; any stage can instead end in ``failed``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .. import env, state

log = logging.getLogger("neon_ducklake.site.chunks")

CHUNK_BYTES_ENV = "NEON_DUCKLAKE_CHUNK_BYTES"
DEFAULT_CHUNK_BYTES = 8 * 10**9
STATUSES: tuple[str, ...] = ("planned", "downloaded", "stacked", "avus", "uploaded", "applied", "recorded",
                             "verified", "deleted")
FAILED = "failed"
RANK = {s: i for i, s in enumerate(STATUSES)}
# stage -> the status it reaches (stages run in this order; each needs the previous status)
STAGE_STATUS: dict[str, str] = {"download": "downloaded", "stack": "stacked", "avus": "avus", "upload": "uploaded",
                                "apply": "applied", "history": "recorded", "verify": "verified", "delete": "deleted"}
STAGES: tuple[str, ...] = tuple(STAGE_STATUS)
SITE_STEPS: tuple[str, ...] = ("terms", "cards", "curate", "validate", "avus", "upload", "apply", "history",
                               "verify", "cleanup")
FINALIZE_STEPS: tuple[str, ...] = SITE_STEPS[:-1]  # every step but cleanup (site/stream.py runs them in order)

SCHEMA = """
CREATE TABLE IF NOT EXISTS stream_chunks (
  site TEXT NOT NULL, chunk_id TEXT NOT NULL, product TEXT NOT NULL, months TEXT NOT NULL,
  n_files INTEGER, bytes INTEGER, status TEXT NOT NULL DEFAULT 'planned', attempt INTEGER NOT NULL DEFAULT 0,
  started_at TEXT, finished_at TEXT, error TEXT,
  done TEXT NOT NULL DEFAULT 'planned', info TEXT, planned_at TEXT,
  PRIMARY KEY (site, chunk_id)
);
CREATE INDEX IF NOT EXISTS stream_chunks_status ON stream_chunks(site, status);
CREATE TABLE IF NOT EXISTS stream_sites (
  site TEXT NOT NULL, step TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
  started_at TEXT, finished_at TEXT, error TEXT, info TEXT, PRIMARY KEY (site, step)
);
CREATE TABLE IF NOT EXISTS lake_pushes (
  id INTEGER PRIMARY KEY AUTOINCREMENT, site TEXT NOT NULL, catalog_md5 TEXT NOT NULL, pushed_at TEXT NOT NULL,
  snapshot_id INTEGER, kind TEXT NOT NULL DEFAULT 'push', vm TEXT, snapshot_key TEXT
);
CREATE INDEX IF NOT EXISTS lake_pushes_site ON lake_pushes(site, id);
"""
# module columns beyond the contract's list (added when another process created the table first)
EXTRA_COLUMNS: dict[str, str] = {"done": "TEXT NOT NULL DEFAULT 'planned'", "info": "TEXT", "planned_at": "TEXT"}
_UNITS = {"": 1, "b": 1, "k": 10**3, "kb": 10**3, "m": 10**6, "mb": 10**6, "g": 10**9, "gb": 10**9,
          "t": 10**12, "tb": 10**12, "kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30, "tib": 1 << 40}
_BYTES_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]*)?)\s*([a-zA-Z]*)\s*$")


# ---------------------------------------------------------------- sizes

def parse_bytes(value: str | float) -> int:
    """``8000000000``, ``8G``, ``8 GB`` (decimal) or ``8GiB`` (binary) -> bytes."""
    if isinstance(value, int | float):
        return int(value)
    m = _BYTES_RE.match(str(value))
    if not m or m.group(2).lower() not in _UNITS:
        raise ValueError(f"not a byte size: {value!r}")
    return int(float(m.group(1)) * _UNITS[m.group(2).lower()])


def env_bytes(var: str, default: int, environ: Mapping[str, str] | None = None) -> int:
    raw = (environ if environ is not None else os.environ).get(var)
    if not raw:
        return default
    try:
        n = parse_bytes(raw)
    except ValueError:
        log.warning("%s=%r is not a byte size; using %d", var, raw, default)
        return default
    return n if n > 0 else default


def chunk_bytes(environ: Mapping[str, str] | None = None) -> int:
    """``$NEON_DUCKLAKE_CHUNK_BYTES`` (read at call time), default 8 GB."""
    return env_bytes(CHUNK_BYTES_ENV, DEFAULT_CHUNK_BYTES, environ)


CHUNK_BYTES = chunk_bytes()


# ---------------------------------------------------------------- model

@dataclass
class Chunk:
    site: str
    chunk_id: str
    product: str
    months: list[str]
    n_files: int
    bytes: int
    status: str = "planned"
    done: str = "planned"
    attempt: int = 0
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    info: dict[str, Any] = field(default_factory=dict)


def make_chunk_id(product: str, months: Iterable[str]) -> str:
    ms = sorted(months)
    if not ms:
        raise ValueError("a chunk needs at least one month")
    return f"{product}__{ms[0]}_{ms[-1]}"


def _from_row(r: sqlite3.Row) -> Chunk:
    try:
        info = json.loads(r["info"] or "{}")
    except ValueError:
        info = {}
    return Chunk(site=r["site"], chunk_id=r["chunk_id"], product=r["product"], months=list(json.loads(r["months"])),
                 n_files=int(r["n_files"] or 0), bytes=int(r["bytes"] or 0), status=r["status"],
                 done=r["done"] or "planned", attempt=int(r["attempt"] or 0), error=r["error"],
                 started_at=r["started_at"], finished_at=r["finished_at"], info=info)


# ---------------------------------------------------------------- journal

def ensure_schema(con: sqlite3.Connection) -> None:
    """``stream_chunks`` + ``stream_sites`` + ``lake_pushes`` (and the module columns when another process made
    the table)."""
    con.executescript(SCHEMA)
    if "snapshot_key" not in {r[1] for r in con.execute("PRAGMA table_info(lake_pushes)")}:
        try:
            con.execute("ALTER TABLE lake_pushes ADD COLUMN snapshot_key TEXT")
        except sqlite3.OperationalError as exc:  # another process added it first
            if "duplicate column" not in str(exc).lower():
                raise
    have = {r[1] for r in con.execute("PRAGMA table_info(stream_chunks)")}
    for col, typ in EXTRA_COLUMNS.items():
        if col in have:
            continue
        try:
            con.execute(f"ALTER TABLE stream_chunks ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError as exc:  # another process added it first
            if "duplicate column" not in str(exc).lower():
                raise


def _connect(con: sqlite3.Connection | None) -> tuple[sqlite3.Connection, bool]:
    if con is not None:
        ensure_schema(con)
        return con, False
    c = state.connect(state.DB_PATH)
    ensure_schema(c)
    return c, True


def list_chunks(site: str, *, con: sqlite3.Connection | None = None, product: str | None = None) -> list[Chunk]:
    c, own = _connect(con)
    try:
        where, args = "site=?", [site.upper()]
        if product:
            where += " AND product=?"
            args.append(product)
        rows = c.execute(f"SELECT * FROM stream_chunks WHERE {where} ORDER BY product, chunk_id", args).fetchall()
        return [_from_row(r) for r in rows]
    finally:
        if own:
            c.close()


def get_chunk(site: str, chunk_id: str, *, con: sqlite3.Connection | None = None) -> Chunk | None:
    c, own = _connect(con)
    try:
        r = c.execute("SELECT * FROM stream_chunks WHERE site=? AND chunk_id=?", (site.upper(), chunk_id)).fetchone()
        return _from_row(r) if r else None
    finally:
        if own:
            c.close()


def set_status(con: sqlite3.Connection, site: str, chunk_id: str, status: str, *, done: str | None = None,
               error: str | None = None, info: dict[str, Any] | None = None, started: bool = False,
               finished: bool = False, attempt: int | None = None) -> None:
    """Update one chunk row (``info`` keys are merged into the stored JSON)."""
    if status not in STATUSES and status != FAILED:
        raise ValueError(f"unknown chunk status {status!r}")
    sets, args = ["status=?", "error=?"], [status, error]
    if done is not None:
        sets.append("done=?")
        args.append(done)
    now = env.utcnow().isoformat()
    if started:
        sets.append("started_at=?")
        args.append(now)
    if finished:
        sets.append("finished_at=?")
        args.append(now)
    if attempt is not None:
        sets.append("attempt=?")
        args.append(attempt)
    if info:
        row = con.execute("SELECT info FROM stream_chunks WHERE site=? AND chunk_id=?", (site, chunk_id)).fetchone()
        try:
            cur = json.loads(row["info"] or "{}") if row else {}
        except ValueError:
            cur = {}
        cur.update(info)
        sets.append("info=?")
        args.append(json.dumps(cur, default=str))
    con.execute(f"UPDATE stream_chunks SET {', '.join(sets)} WHERE site=? AND chunk_id=?", (*args, site, chunk_id))


def month_inventory(con: sqlite3.Connection, site: str, products: Iterable[str] | None = None,
                    months: Iterable[str] | None = None) -> dict[str, list[tuple[str, int, int]]]:
    """product -> [(month, files, bytes)] ascending, from the journal (superseded files excluded)."""
    from .fetch import SUPERSEDED_SCHEMA

    con.executescript(SUPERSEDED_SCHEMA)
    where, args = ["f.site=?", "s.name IS NULL", "coalesce(f.dl_status, '') != 'superseded'"], [site]
    ps, ms = sorted(set(products or [])), sorted(set(months or []))
    if ps:
        where.append(f"f.product IN ({','.join('?' * len(ps))})")
        args += ps
    if ms:
        where.append(f"f.month IN ({','.join('?' * len(ms))})")
        args += ms
    rows = con.execute(
        "SELECT f.product, f.month, count(*) AS n, coalesce(sum(coalesce(f.size, f.dl_bytes, 0)), 0) AS b"
        " FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND"
        f" s.month=f.month AND s.name=f.name WHERE {' AND '.join(where)} GROUP BY f.product, f.month"
        " ORDER BY f.product, f.month", args).fetchall()
    out: dict[str, list[tuple[str, int, int]]] = defaultdict(list)
    for r in rows:
        out[r["product"]].append((r["month"], int(r["n"]), int(r["b"])))
    return dict(out)


def _pending_months(con: sqlite3.Connection, site: str, product: str, months: list[str]) -> int:
    """Files of these months still to download (new, stale or failed; superseded excluded)."""
    if not months:
        return 0
    return int(con.execute(
        "SELECT count(*) FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND"
        " s.month=f.month AND s.name=f.name WHERE f.site=? AND f.product=? AND s.name IS NULL AND"
        f" f.month IN ({','.join('?' * len(months))}) AND coalesce(f.dl_status, '') NOT IN ('ok', 'superseded')",
        (site, product, *months)).fetchone()[0])


def pack_months(months: list[tuple[str, int, int]], max_bytes: int, frozen: set[str] = frozenset()
                ) -> list[list[tuple[str, int, int]]]:
    """Greedy ascending packing; a frozen month ends the current group (it belongs to another chunk)."""
    groups: list[list[tuple[str, int, int]]] = []
    cur: list[tuple[str, int, int]] = []
    size = 0
    for m in sorted(months):
        if m[0] in frozen:
            if cur:
                groups.append(cur)
            cur, size = [], 0
            continue
        if cur and size + m[2] > max_bytes:
            groups.append(cur)
            cur, size = [], 0
        cur.append(m)
        size += m[2]
    if cur:
        groups.append(cur)
    return groups


def plan_chunks(site: str, *, max_bytes: int | None = None, products: Iterable[str] | None = None,
                months: Iterable[str] | None = None, con: sqlite3.Connection | None = None,
                record: bool = True) -> list[Chunk]:
    """Plan (and, with ``record``, journal) the chunks of one site; see the module docstring.

    ``max_bytes`` defaults to :func:`chunk_bytes` (``$NEON_DUCKLAKE_CHUNK_BYTES`` at call time).
    Returns every chunk of the selected products (started ones included), ordered by product and months.
    """
    site = site.upper()
    limit = parse_bytes(max_bytes) if max_bytes else chunk_bytes()
    if limit <= 0:
        raise ValueError("max_bytes must be > 0")
    c, own = _connect(con)
    try:
        inv = month_inventory(c, site, products, months)
        existing = [_from_row(r) for r in c.execute("SELECT * FROM stream_chunks WHERE site=?", (site,)).fetchall()]
        by_product: dict[str, list[Chunk]] = defaultdict(list)
        for ch in existing:
            by_product[ch.product].append(ch)
        plan: list[Chunk] = []
        stale: list[str] = []
        resets: list[str] = []
        for product in sorted(inv):
            month_rows = {m: (m, n, b) for m, n, b in inv[product]}
            started = [ch for ch in by_product.get(product, []) if ch.status != "planned"]
            frozen = {m for ch in started for m in ch.months}
            chunks: list[Chunk] = []
            for ch in started:
                n = sum(month_rows[m][1] for m in ch.months if m in month_rows)
                b = sum(month_rows[m][2] for m in ch.months if m in month_rows)
                if not any(m in month_rows for m in ch.months):
                    n, b = ch.n_files, ch.bytes  # outside this (filtered) inventory: unchanged
                chunks.append(Chunk(site, ch.chunk_id, product, list(ch.months), n, b, status=ch.status,
                                    done=ch.done, attempt=ch.attempt, error=ch.error, started_at=ch.started_at,
                                    finished_at=ch.finished_at, info=ch.info))
                if ch.status == "deleted" and _pending_months(c, site, product, ch.months):
                    resets.append(ch.chunk_id)
            for group in pack_months(list(month_rows.values()), limit, frozen):
                ms = [m for m, _n, _b in group]
                chunks.append(Chunk(site, make_chunk_id(product, ms), product, ms, sum(n for _m, n, _b in group),
                                    sum(b for _m, _n, b in group)))
            chunks.sort(key=lambda ch: (ch.months[0], ch.chunk_id))
            plan += chunks
            planned_now = {ch.chunk_id for ch in chunks}
            considered = set(month_rows)
            stale += [ch.chunk_id for ch in by_product.get(product, []) if ch.status == "planned"
                      and ch.chunk_id not in planned_now and considered & set(ch.months)]
        if record:
            _record(c, site, plan, stale, resets)
            if resets:
                for ch in plan:
                    if ch.chunk_id in resets:
                        ch.status, ch.done, ch.error = "planned", "planned", None
        return plan
    finally:
        if own:
            c.close()


def _record(con: sqlite3.Connection, site: str, plan: list[Chunk], stale: list[str], resets: list[str]) -> None:
    now = env.utcnow().isoformat()
    con.execute("BEGIN IMMEDIATE")
    try:
        existing = {r[0] for r in con.execute("SELECT chunk_id FROM stream_chunks WHERE site=?", (site,))}
        new = [ch.chunk_id for ch in plan if ch.chunk_id not in existing]
        for cid in stale:
            con.execute("DELETE FROM stream_chunks WHERE site=? AND chunk_id=? AND status='planned'", (site, cid))
        for ch in plan:
            con.execute(
                "INSERT INTO stream_chunks(site, chunk_id, product, months, n_files, bytes, status, done, planned_at)"
                " VALUES (?,?,?,?,?,?, 'planned', 'planned', ?) ON CONFLICT(site, chunk_id) DO UPDATE SET"
                " n_files=excluded.n_files, bytes=excluded.bytes,"
                " months=CASE WHEN stream_chunks.status='planned' THEN excluded.months ELSE stream_chunks.months END",
                (site, ch.chunk_id, ch.product, json.dumps(ch.months), ch.n_files, ch.bytes, now))
        for cid in resets:
            log.warning("chunk %s %s has new files to download after it finished: re-planned", site, cid)
            con.execute("UPDATE stream_chunks SET status='planned', done='planned', error=NULL, attempt=0"
                        " WHERE site=? AND chunk_id=?", (site, cid))
        if resets or new:
            reset_finalize(con, site)  # no-op on a site that never finalized
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    if stale:
        log.info("plan %s: %d stale planned chunk(s) removed", site, len(stale))


# ---------------------------------------------------------------- site steps (finalize / cleanup)

def step_status(site: str, step: str, *, con: sqlite3.Connection | None = None) -> str | None:
    c, own = _connect(con)
    try:
        r = c.execute("SELECT status FROM stream_sites WHERE site=? AND step=?", (site.upper(), step)).fetchone()
        return r["status"] if r else None
    finally:
        if own:
            c.close()


def set_step(con: sqlite3.Connection, site: str, step: str, status: str, *, error: str | None = None,
             info: dict[str, Any] | None = None, started: bool = False) -> None:
    now = env.utcnow().isoformat()
    con.execute(
        "INSERT INTO stream_sites(site, step, status, attempt, started_at, finished_at, error, info)"
        " VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(site, step) DO UPDATE SET status=excluded.status,"
        " attempt=stream_sites.attempt + excluded.attempt,"
        " started_at=coalesce(excluded.started_at, stream_sites.started_at),"
        " finished_at=excluded.finished_at, error=excluded.error, info=coalesce(excluded.info, stream_sites.info)",
        (site.upper(), step, status, 1 if started else 0, now if started else None, None if started else now, error,
         json.dumps(info, default=str) if info is not None else None))


def reset_finalize(con: sqlite3.Connection, site: str) -> int:
    """Forget the site's finalize steps (not ``cleanup``: its row says the local lake data may be gone, which only
    ``stream.restore_lake`` may clear). Returns the rows removed."""
    marks = ",".join("?" * len(FINALIZE_STEPS))
    n = con.execute(f"DELETE FROM stream_sites WHERE site=? AND step IN ({marks})",
                    (site.upper(), *FINALIZE_STEPS)).rowcount
    if n:
        log.warning("site %s: %d finalize step(s) reset (a chunk was re-planned or added)", site, n)
    return n


def clear_cleanup(con: sqlite3.Connection, site: str) -> bool:
    """Drop the ``cleanup`` row once the local lake data is back (``stream.restore_lake``)."""
    return con.execute("DELETE FROM stream_sites WHERE site=? AND step='cleanup'", (site.upper(),)).rowcount > 0


def site_cleaned(site: str, *, con: sqlite3.Connection | None = None) -> bool:
    """Whether ``cleanup_site`` ran (or started) for the site: its local lake data, cards and curation may be gone.

    Any cleanup attempt counts (a partial or crashed cleanup leaves a partial tree that must not be rebuilt
    from). Read-only; no journal -> False.
    """
    if con is None:
        if not state.DB_PATH.exists():
            return False
        try:
            c = sqlite3.connect(f"file:{state.DB_PATH}?mode=ro", uri=True, timeout=60)
        except sqlite3.Error:
            return False
        own = True
    else:
        c, own = con, False
    try:
        r = c.execute("SELECT status FROM stream_sites WHERE site=? AND step='cleanup'", (site.upper(),)).fetchone()
    except sqlite3.Error:  # no stream_sites table yet
        return False
    finally:
        if own:
            c.close()
    return r is not None


def unfinished(site: str, *, con: sqlite3.Connection | None = None) -> list[Chunk]:
    """Chunks of the site that are not ``deleted`` yet."""
    return [ch for ch in list_chunks(site, con=con) if ch.status != "deleted"]


# ---------------------------------------------------------------- lake pushes (site-lake catalog in the Data Store)

def record_lake_push(con: sqlite3.Connection, site: str, catalog_md5: str, snapshot_id: int | None, *,
                     kind: str = "push", snapshot_key: str | None = None) -> None:
    """Journal a catalog this host pushed (``push``) or restored from the Data Store (``restore``)."""
    if kind not in ("push", "restore"):
        raise ValueError(f"unknown lake push kind {kind!r}")
    ensure_schema(con)
    con.execute("INSERT INTO lake_pushes(site, catalog_md5, pushed_at, snapshot_id, kind, vm, snapshot_key) "
                "VALUES (?,?,?,?,?,?,?)", (site.upper(), catalog_md5.lower(), env.utcnow().isoformat(),
                                           None if snapshot_id is None else int(snapshot_id), kind, _vm(),
                                           snapshot_key))


def _vm() -> str | None:
    fn = getattr(env, "vm_id", None)
    try:
        return str(fn()) if callable(fn) else None
    except Exception:  # noqa: BLE001 - informational only
        return None


def lake_pushes(site: str, *, con: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """The site's ``lake_pushes`` rows, oldest first."""
    c, own = _connect(con)
    try:
        return [dict(r) for r in c.execute("SELECT * FROM lake_pushes WHERE site=? ORDER BY id", (site.upper(),))]
    finally:
        if own:
            c.close()


def last_lake_push(site: str, *, con: sqlite3.Connection | None = None,
                   local_keys: set[str] | None = None) -> dict[str, Any] | None:
    """The newest ``lake_pushes`` row; with ``local_keys`` (the local catalog's snapshot keys) the newest row
    whose snapshot the local lake holds."""
    rows = [r for r in lake_pushes(site, con=con) if local_keys is None or r.get("snapshot_key") in local_keys]
    return rows[-1] if rows else None


def stacked_required(site: str, *, con: sqlite3.Connection | None = None, exclude: Iterable[str] = ()
                     ) -> list[str]:
    """Chunk ids whose ``done`` is at least ``stacked`` and whose stack wrote something (not ``empty``): the site
    lake must hold ``stream stack <SITE> <chunk>`` snapshots of each of them."""
    skip = set(exclude)
    out = []
    for ch in list_chunks(site, con=con):
        if ch.chunk_id in skip or RANK.get(ch.done, 0) < RANK["stacked"]:
            continue
        st = ch.info.get("stack") if isinstance(ch.info, dict) else None
        if isinstance(st, dict) and (st.get("status") == "empty" or st.get("files") == 0
                                     or ("lastSnapshot" in st and st["lastSnapshot"] is None)):
            continue  # nothing was committed for it
        out.append(ch.chunk_id)
    return out
