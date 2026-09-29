"""Journaled bulk AVU writer: ``avu_desired`` -> iRODS, through paced atomic metadata calls.

Per work unit (``unit_id`` = a collection path; usually that collection plus its direct objects):

1. Read the current state: ONE GenQuery for the collection's own AVUs (``CollectionMeta``) and ONE
   for its direct data objects' AVUs (``DataObjectMeta`` with ``Collection.name == unit``). Replica
   rows collapse by ``DataObject.id``. Targets with no AVU rows at all get one existence query.
2. ``add = desired - current``; ``remove = journal-owned - desired``. Journal-owned means an
   ``op='add'`` row for that (scope, path, triple) in status applied/spooled/recorded that was not later
   removed. ``ipc*`` and ``mesa.enabled`` are never added or removed, nor is anything we don't own.
   **Adoption**: a desired AVU already present in iRODS but not owned (typically written by another VM
   that died mid-apply, whose journal rows never reached the Data Store backup this host restored) is
   journaled as ``op='add'``, status ``applied``, ``adopted=1``, so it is owned from now on and spooled
   into MESA history once. Its ``applied_at`` is the original time when known (an earlier journal row of
   the scope, or the last spooled change of that AVU in a spool batch this journal does not know,
   ``history.spooled_times``), else now; with the original time the recorder's content dedup drops the
   copy another VM already spooled. An AVU owned by another scope of this journal is never adopted.
3. Journal every op as ``intent`` -> ``apply_atomic_operations`` per object (removes first, <= 500
   ops per call) -> ``applied`` with a tz-aware UTC ``applied_at``.
4. On an atomic failure: re-read that object, recompute, retry once; then per-AVU add/remove where
   "already exists" / "not found" are ``noop`` (never recorded to MESA history).

A crash between the iRODS call and the journal update leaves ``intent`` rows; the next run
reconciles them against iRODS before computing ownership. An intent settled as ``applied`` keeps its
``applied_at`` when it has one, else gets the time of the same change in an unknown spool batch (the VM
that wrote it spooled it before this host restored an older journal), else now. Units whose rows vanished from
``avu_desired`` altogether are left untouched (a filtered build must never strip other units).

Pacing (docs/SCALING.md "Hard limits"): every iRODS call takes a token from a ``tuning.AdaptiveRate``.

- The rate starts at ``rate``, has a floor of 5 calls/s and a ceiling of 60 calls/s.
- It grows by 2 calls/s after 40 successful writes whose p95 is below 250 ms, and only while the bucket
  is binding.
- It halves on a p95 above 1 s or on a connection-limit, reset, timeout or retryable error; semantic
  catalog errors don't count.
- ``LatencyMonitor`` stays as the hard stop: ``LatencyAbort`` fires once a full 200-call window has a
  p95 above 5 s.
- Worker threads (at most 16) lease sessions from a ``tuning.SessionPool`` bound to the cross-process
  iRODS connection budget (``work/locks/irods-conn-NN.lock``, 32 slots shared with
  ``mirror.push_tree``).
"""

from __future__ import annotations

import logging
import posixpath
import sqlite3
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .. import state
from ..tuning import AdaptiveRate, ConnectionBudget, SessionPool, irods_budget, irods_error_kind
from .normalize import Triple, is_protected, normalize

log = logging.getLogger("neon_ducklake.avu.bulk")

MAX_THREADS = 16  # worker sessions of one apply (each holds a slot of the shared iRODS connection budget)
MAX_OPS_PER_CALL = 500
P95_WINDOW = 200
P95_LIMIT_S = 5.0  # hard stop (LatencyAbort); the adaptive rate backs off long before, at 1 s
RATE_MIN = 5.0
RATE_MAX = 60.0
RATE_STEP = 2.0
RATE_EVERY = 40
RATE_TARGET_S = 0.25
RATE_CEILING_S = 1.0
RATE_WINDOW = 100
RATE_MIN_SAMPLES = 20
RATE_COOLDOWN_S = 2.0
OWNED_STATUSES = ("applied", "spooled", "recorded")
COUNTERS = ("units_done", "targets", "missing", "adds", "removes", "absent_removes", "noops", "errors", "queries",
            "atomic_calls", "fallback_calls", "retries", "intents_reconciled", "objects_changed", "unit_failures",
            "adopted", "adopted_original_time")
ADOPTED_NOTE = "adopted: present in iRODS, not owned by this journal"

DESIRED_DDL = """
CREATE TABLE IF NOT EXISTS avu_desired (
  scope TEXT NOT NULL, unit_id TEXT NOT NULL, path TEXT NOT NULL,
  target_type TEXT NOT NULL CHECK (target_type IN ('collection','data_object')),
  attribute TEXT NOT NULL, value TEXT NOT NULL, unit TEXT NOT NULL DEFAULT '', level TEXT, generator TEXT,
  PRIMARY KEY (scope, path, attribute, value, unit)
);
CREATE INDEX IF NOT EXISTS avu_desired_unit ON avu_desired(scope, unit_id);
"""

Key = tuple[str, str]  # (path, target_type)


class LatencyAbort(RuntimeError):
    """Rolling p95 of metadata write calls exceeded the limit: stop before iCAT suffers."""


def utc_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------- avu_desired helpers (for builders)

def ensure_desired(con: sqlite3.Connection) -> None:
    con.executescript(DESIRED_DDL)


def ensure_journal_columns(con: sqlite3.Connection) -> None:
    """Add ``avu_journal.adopted`` (1: an adopted AVU, see the module docstring) when missing."""
    if any(r[1] == "adopted" for r in con.execute("PRAGMA table_info(avu_journal)")):
        return
    try:
        con.execute("ALTER TABLE avu_journal ADD COLUMN adopted INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError as exc:  # another process added it first
        if "duplicate column" not in str(exc).lower():
            raise


def replace_desired(con: sqlite3.Connection, scope: str, rows: Iterable[tuple[Any, ...]]) -> dict[str, int]:
    """Transactionally replace every ``avu_desired`` row of ``scope``.

    ``rows`` are ``(unit_id, path, target_type, attribute, value, unit, level, generator)``. Values are
    normalised (strip, drop empties, cap at 2,000 bytes); protected attributes are dropped.
    """
    ensure_desired(con)
    kept = dropped = protected = 0
    out: dict[tuple[str, str, str, str], tuple[Any, ...]] = {}
    for unit_id, path, ttype, a, v, u, level, generator in rows:
        if ttype not in ("collection", "data_object"):
            raise ValueError(f"bad target_type {ttype!r} for {path}")
        n = normalize(a, v, u)
        if n is None:
            dropped += 1
            continue
        if is_protected(n[0]):
            protected += 1
            continue
        out.setdefault((path, *n), (scope, unit_id, path, ttype, *n, level, generator))
    kept = len(out)
    con.execute("BEGIN IMMEDIATE")
    try:
        con.execute("DELETE FROM avu_desired WHERE scope=?", (scope,))
        con.executemany(
            "INSERT INTO avu_desired(scope, unit_id, path, target_type, attribute, value, unit, level, generator)"
            " VALUES (?,?,?,?,?,?,?,?,?)", list(out.values()))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    if protected:
        log.warning("replace_desired(%s): dropped %d protected AVUs (ipc*/mesa.enabled)", scope, protected)
    return {"rows": kept, "dropped_empty": dropped, "dropped_protected": protected}


@dataclass
class Unit:
    unit_id: str
    desired: dict[Key, set[Triple]] = field(default_factory=lambda: defaultdict(set))
    generators: dict[tuple[str, Triple], str | None] = field(default_factory=dict)


def load_desired(con: sqlite3.Connection, scope: str, only_units: Iterable[str] | None = None) -> dict[str, Unit]:
    ensure_desired(con)
    wanted = set(only_units) if only_units is not None else None
    units: dict[str, Unit] = {}
    for r in con.execute("SELECT unit_id, path, target_type, attribute, value, unit, generator FROM avu_desired"
                         " WHERE scope=? ORDER BY unit_id, path", (scope,)):
        if wanted is not None and r[0] not in wanted:
            continue
        u = units.setdefault(r[0], Unit(r[0]))
        t = (r[3], r[4], r[5] or "")
        u.desired[(r[1], r[2])].add(t)
        u.generators[(r[1], t)] = r[6]
    return units


# ---------------------------------------------------------------- pacing + latency guard

def adaptive_rate(rate: float, *, clock: Callable[[], float] = time.monotonic,
                  sleep: Callable[[float], None] = time.sleep) -> AdaptiveRate:
    """The AIMD calls/s bucket of one apply (start ``rate``, clamped to [5, 60])."""
    return AdaptiveRate("avu-apply", start=min(RATE_MAX, max(RATE_MIN, rate)), lo=RATE_MIN, hi=RATE_MAX,
                        step=RATE_STEP, every=RATE_EVERY, target=RATE_TARGET_S, ceiling=RATE_CEILING_S,
                        window=RATE_WINDOW, min_samples=RATE_MIN_SAMPLES, cooldown_s=RATE_COOLDOWN_S,
                        clock=clock, sleep=sleep)


class TokenBucket:
    """Thread-safe fixed-rate token bucket: ``rate`` tokens/s, at most ``burst`` banked (see AdaptiveRate)."""

    def __init__(self, rate: float, burst: float = 1.0, *, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate, self.burst = float(rate), max(1.0, float(burst))
        self._clock, self._sleep = clock, sleep
        self._tokens = self.burst
        self._last = clock()
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Take one token, sleeping as needed; returns the seconds waited."""
        with self._lock:
            now = self._clock()
            self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate)
            self._last = now
            self._tokens -= 1.0
            wait_s = 0.0 if self._tokens >= 0 else -self._tokens / self.rate
        if wait_s > 0:
            self._sleep(wait_s)
        return wait_s


class LatencyMonitor:
    """Rolling window of call latencies; raises LatencyAbort once a full window's p95 exceeds the limit."""

    def __init__(self, window: int = P95_WINDOW, limit_s: float = P95_LIMIT_S) -> None:
        self.window, self.limit_s = window, limit_s
        self._d: deque[float] = deque(maxlen=window)
        self._lock = threading.Lock()
        self.calls = 0

    def p95(self) -> float | None:
        with self._lock:
            if not self._d:
                return None
            s = sorted(self._d)
        return s[min(len(s) - 1, max(0, round(0.95 * len(s)) - 1))]

    def observe(self, seconds: float) -> None:
        with self._lock:
            self._d.append(seconds)
            self.calls += 1
            full = len(self._d) >= self.window
        if full:
            p = self.p95()
            if p is not None and p > self.limit_s:
                raise LatencyAbort(f"rolling p95 of metadata calls {p:.2f}s > {self.limit_s:.1f}s "
                                   f"over the last {self.window} calls; stopping (re-run resumes)")


# ---------------------------------------------------------------- iRODS reads

def _models() -> tuple[Any, Any, Any, Any]:
    from irods.models import Collection, CollectionMeta, DataObject, DataObjectMeta

    return Collection, CollectionMeta, DataObject, DataObjectMeta


def model_for(target_type: str) -> Any:
    from irods.models import Collection, DataObject

    return DataObject if target_type == "data_object" else Collection


def query_collection_avus(session: Any, coll: str) -> tuple[bool, set[Triple]]:
    """(has_rows, AVUs) of one collection: one GenQuery."""
    Collection, CollectionMeta, _, _ = _models()
    q = session.query(Collection.name, CollectionMeta.name, CollectionMeta.value, CollectionMeta.units)
    out: set[Triple] = set()
    seen = False
    for row in q.filter(Collection.name == coll):
        seen = True
        out.add((row[CollectionMeta.name], row[CollectionMeta.value], row[CollectionMeta.units] or ""))
    return seen, out


def query_object_avus(session: Any, coll: str) -> dict[str, set[Triple]]:
    """AVUs of every direct data object of ``coll`` (one GenQuery; replica rows collapse by id)."""
    Collection, _, DataObject, DataObjectMeta = _models()
    q = session.query(DataObject.id, DataObject.name, DataObjectMeta.name, DataObjectMeta.value,
                      DataObjectMeta.units).filter(Collection.name == coll)
    by_id: dict[Any, tuple[str, set[Triple]]] = {}
    for row in q:
        ent = by_id.setdefault(row[DataObject.id], (row[DataObject.name], set()))
        ent[1].add((row[DataObjectMeta.name], row[DataObjectMeta.value], row[DataObjectMeta.units] or ""))
    return {posixpath.join(coll, name): avus for name, avus in by_id.values()}


def collection_exists(session: Any, coll: str) -> bool:
    Collection = _models()[0]
    return any(True for _ in session.query(Collection.id).filter(Collection.name == coll))


def object_names(session: Any, coll: str) -> set[str]:
    Collection, _, DataObject, _ = _models()
    return {r[DataObject.name] for r in session.query(DataObject.name).filter(Collection.name == coll)}


def read_target_avus(session: Any, key: Key) -> set[Triple]:
    metas = session.metadata.get(model_for(key[1]), key[0])
    return {(m.name, m.value, m.units or "") for m in metas or []}


def _err_name(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:500]


def _is_already_exists(exc: BaseException) -> bool:
    s = f"{type(exc).__name__} {exc}"
    return "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME" in s or getattr(exc, "code", None) == -809000


def _is_not_found(exc: BaseException) -> bool:
    s = f"{type(exc).__name__} {exc}"
    return any(k in s for k in ("CAT_SUCCESS_BUT_WITH_NO_INFO", "CAT_NO_ROWS_FOUND", "DoesNotExist",
                                "CAT_UNKNOWN_FILE", "CAT_UNKNOWN_COLLECTION", "USER_FILE_DOES_NOT_EXIST"))


# ---------------------------------------------------------------- the writer

@dataclass
class Op:
    row_id: int | None
    op: str  # 'add' | 'remove'
    triple: Triple


class BulkWriter:
    def __init__(self, con: sqlite3.Connection, scope: str, session: Any, *, rate: float = 20.0,
                 dry_run: bool = False, bucket: TokenBucket | AdaptiveRate | None = None,
                 monitor: LatencyMonitor | None = None, budget: ConnectionBudget | None = None,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 spool_times: Callable[[], dict[tuple[str, str, str, str], tuple[str, str]]] | None = None) -> None:
        self.con, self.scope, self.root_session, self.dry_run = con, scope, session, dry_run
        ensure_journal_columns(con)
        self.clock = clock
        self.bucket = bucket if bucket is not None else adaptive_rate(rate, clock=clock, sleep=sleep)
        self.monitor = monitor or LatencyMonitor()
        self.pool = SessionPool(session, budget=budget if budget is not None else irods_budget())
        self._db = threading.Lock()
        self._slock = threading.Lock()
        self.stop = threading.Event()
        self.all_desired_paths: set[str] = set()
        self.stats: dict[str, Any] = defaultdict(int)
        self.stats.update({k: 0 for k in COUNTERS})
        self.stats.update({"scope": scope, "dry_run": dry_run, "missing_paths": [], "error_samples": [],
                           "unit_errors": []})
        self.spool_times = spool_times
        self._spool_index: dict[tuple[str, str, str, str], tuple[str, str]] | None = None
        self._spool_lock = threading.Lock()

    # ---------------------------------------------------------- plumbing
    def cleanup(self) -> None:
        self.pool.close()

    def _ok(self, seconds: float) -> None:
        """One successful write call: feed the adaptive rate, then the hard-stop monitor."""
        on_success = getattr(self.bucket, "on_success", None)
        if on_success is not None:
            on_success(seconds)
        self.monitor.observe(seconds)

    def _failed(self, exc: BaseException) -> None:
        """A failed write call: connection/timeout/retryable failures halve the adaptive rate."""
        kind = irods_error_kind(exc)
        on_error = getattr(self.bucket, "on_error", None)
        if kind is not None and on_error is not None:
            on_error(kind)

    def _bump(self, key: str, n: int = 1) -> None:
        with self._slock:
            self.stats[key] += n

    def _note(self, key: str, item: Any, cap: int = 50) -> None:
        with self._slock:
            if len(self.stats[key]) < cap:
                self.stats[key].append(item)

    def _db_exec(self, sql: str, params: tuple[Any, ...]) -> None:
        with self._db:
            self.con.execute(sql, params)

    def _executemany(self, sql: str, params: list[tuple[Any, ...]]) -> None:
        """One transaction per batch (the connection is in autocommit mode)."""
        with self._db:
            self.con.execute("BEGIN IMMEDIATE")
            try:
                self.con.executemany(sql, params)
                self.con.execute("COMMIT")
            except BaseException:
                self.con.execute("ROLLBACK")
                raise

    def _set_status(self, ops: list[Op], status: str, *, error: str | None = None, applied: bool = False) -> None:
        ids = [o.row_id for o in ops if o.row_id is not None]
        if not ids or self.dry_run:
            return
        at = utc_iso() if applied else None
        self._executemany("UPDATE avu_journal SET status=?, applied_at=COALESCE(?, applied_at), error=? WHERE id=?",
                          [(status, at, error, i) for i in ids])

    # ---------------------------------------------------------- ownership
    def _journal_rows(self, unit: Unit) -> list[sqlite3.Row]:
        paths = sorted({k[0] for k in unit.desired})
        rows: dict[int, sqlite3.Row] = {}
        with self._db:
            for r in self.con.execute("SELECT * FROM avu_journal WHERE scope=? AND unit_id=?",
                                      (self.scope, unit.unit_id)):
                rows[r["id"]] = r
            for i in range(0, len(paths), 500):
                chunk = paths[i:i + 500]
                q = f"SELECT * FROM avu_journal WHERE scope=? AND path IN ({','.join('?' * len(chunk))})"
                for r in self.con.execute(q, (self.scope, *chunk)):
                    rows[r["id"]] = r
        return [rows[i] for i in sorted(rows)]

    def _reconcile_intents(self, intents: list[sqlite3.Row], current: dict[Key, set[Triple]],
                           missing: set[Key]) -> dict[int, str]:
        """Stale 'intent' rows (crash mid-write): settle them against what iRODS shows now."""
        out: dict[int, str] = {}
        for r in intents:
            key = (r["path"], r["target_type"])
            t = (r["attribute"], r["value"], r["unit"] or "")
            if key in missing:
                out[r["id"]] = "abandoned"
                continue
            present = t in current.get(key, set())
            done = present if r["op"] == "add" else not present
            out[r["id"]] = "applied" if done else "abandoned"
        if out and not self.dry_run:
            now = utc_iso()
            params = []
            for r in intents:
                s = out.get(r["id"])
                if s is None:
                    continue
                at = r["applied_at"]
                if s == "applied" and not at:  # keep the original time: the spooled copy's, when there is one
                    at = self._spooled_at(r["path"], (r["attribute"], r["value"], r["unit"] or ""), r["op"]) or now
                params.append((s, at, "reconciled from stale intent", r["id"]))
            self._executemany("UPDATE avu_journal SET status=?, applied_at=?, error=? WHERE id=?", params)
        self._bump("intents_reconciled", len(out))
        return out

    # ---------------------------------------------------------- adoption
    def _spooled_at(self, path: str, t: Triple, op: str) -> str | None:
        """The time of the last spooled change of this AVU in a spool batch this journal does not know, when that
        change is ``op`` (``history.spooled_times``, loaded once per apply)."""
        if self.spool_times is None:
            return None
        with self._spool_lock:
            if self._spool_index is None:
                try:
                    self._spool_index = self.spool_times()
                except Exception as exc:  # noqa: BLE001 - unknown: the caller uses now
                    log.warning("apply %s: spool batches not read (%s); adopted rows get the current time",
                                self.scope, _err_name(exc))
                    self._spool_index = {}
        hit = self._spool_index.get((path, *t))
        return hit[1] if hit is not None and hit[0] == op else None

    def _owned_elsewhere(self, cands: dict[Key, list[Triple]]) -> set[tuple[str, str, Triple]]:
        """(path, target_type, triple) of ``cands`` owned by another scope of this journal (never adopted)."""
        paths = sorted({k[0] for k in cands})
        rows: list[sqlite3.Row] = []
        with self._db:
            for i in range(0, len(paths), 500):
                chunk = paths[i:i + 500]
                marks = ",".join("?" * len(chunk))
                rows += self.con.execute(
                    f"SELECT * FROM avu_journal WHERE scope != ? AND path IN ({marks}) ORDER BY id",
                    (self.scope, *chunk)).fetchall()
        by_scope: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for r in rows:
            by_scope[r["scope"]].append(r)
        out: set[tuple[str, str, Triple]] = set()
        for scope_rows in by_scope.values():
            owned, _ = self._owned(scope_rows, {})
            out |= {(k[0], k[1], t) for k, ts in owned.items() for t in ts}
        return out

    @staticmethod
    def _journal_time(rows: list[sqlite3.Row], key: Key, t: Triple) -> str | None:
        """``applied_at`` of the scope's last journal row of this AVU when that row is an ``add`` with a time (a
        later remove means the present AVU is a newer write: no known time)."""
        mine = [r for r in rows if r["path"] == key[0] and r["target_type"] == key[1]
                and (r["attribute"], r["value"], r["unit"] or "") == t and r["status"] not in ("intent", "abandoned")]
        last = max(mine, key=lambda r: r["id"]) if mine else None
        return last["applied_at"] if last is not None and last["op"] == "add" and last["applied_at"] else None

    @staticmethod
    def _owned(rows: list[sqlite3.Row], settled: dict[int, str]) -> tuple[dict[Key, set[Triple]], dict]:
        owned: dict[Key, set[Triple]] = defaultdict(set)
        gens: dict[tuple[str, Triple], str | None] = {}
        for r in rows:
            status = settled.get(r["id"], r["status"])
            if status not in OWNED_STATUSES:
                continue
            key = (r["path"], r["target_type"])
            t = (r["attribute"], r["value"], r["unit"] or "")
            if r["op"] == "add":
                owned[key].add(t)
                gens[(r["path"], t)] = r["generator"]
            else:
                owned[key].discard(t)
        return owned, gens

    # ---------------------------------------------------------- one unit
    def read_current(self, session: Any, keys: set[Key], unit_id: str) -> tuple[dict[Key, set[Triple]], set[Key]]:
        current: dict[Key, set[Triple]] = {}
        colls = sorted({p for p, t in keys if t == "collection"} | {unit_id})
        parents = sorted({posixpath.dirname(p) for p, t in keys if t == "data_object"})
        seen_colls: set[str] = set()
        for c in colls:
            self.bucket.acquire()
            has, avus = query_collection_avus(session, c)
            self._bump("queries")
            if has:
                seen_colls.add(c)
                current[(c, "collection")] = avus
        objs: dict[str, set[Triple]] = {}
        for parent in parents:
            self.bucket.acquire()
            objs.update(query_object_avus(session, parent))
            self._bump("queries")
        for p, t in keys:
            if t == "data_object" and p in objs:
                current[(p, t)] = objs[p]
        # existence checks only for targets that returned no AVU rows at all
        missing: set[Key] = set()
        for c in colls:
            if c not in seen_colls:
                self.bucket.acquire()
                self._bump("queries")
                if collection_exists(session, c):
                    seen_colls.add(c)
                    current[(c, "collection")] = set()
        unseen_by_parent: dict[str, list[str]] = defaultdict(list)
        for p, t in keys:
            if t == "data_object" and (p, t) not in current:
                unseen_by_parent[posixpath.dirname(p)].append(p)
        for parent, paths in unseen_by_parent.items():
            if parent in colls and parent not in seen_colls:
                names: set[str] = set()
            else:
                self.bucket.acquire()
                self._bump("queries")
                names = object_names(session, parent)
            for p in paths:
                if posixpath.basename(p) in names:
                    current[(p, "data_object")] = set()
        for k in keys:
            if k not in current:
                missing.add(k)
        return current, missing

    def process_unit(self, unit: Unit) -> None:
        if self.stop.is_set():
            return
        with self.pool.lease() as session:
            self._process_unit(session, unit)

    def _process_unit(self, session: Any, unit: Unit) -> None:
        rows = self._journal_rows(unit)
        # journal-only paths (desired no more) stay with this unit unless another unit now wants them
        journal_keys = {(r["path"], r["target_type"]) for r in rows
                        if (r["path"], r["target_type"]) in unit.desired or r["path"] not in self.all_desired_paths}
        rows = [r for r in rows if (r["path"], r["target_type"]) in unit.desired or
                (r["path"], r["target_type"]) in journal_keys]
        keys = set(unit.desired) | journal_keys
        current, missing = self.read_current(session, keys, unit.unit_id)
        for k in sorted(missing):
            if k in unit.desired:
                self._bump("missing")
                self._note("missing_paths", k[0])
        intents = [r for r in rows if r["status"] == "intent"]
        settled = self._reconcile_intents(intents, current, missing) if intents else {}
        owned, owned_gens = self._owned(rows, settled)

        plan: list[tuple[Key, list[tuple[str, Triple]], list[Triple]]] = []
        adopt: dict[Key, list[Triple]] = {}
        for key in sorted(keys):
            if key in missing:
                continue
            cur = current.get(key, set())
            want = {t for t in unit.desired.get(key, set()) if not is_protected(t[0])}
            mine = {t for t in owned.get(key, set()) if not is_protected(t[0])}
            stale = mine - want
            removes = sorted(stale & cur)
            absent = sorted(stale - cur)  # owned but already gone from iRODS: record the delete only
            adds = sorted(want - cur)
            if removes or adds or absent:
                plan.append((key, [("remove", t) for t in removes] + [("add", t) for t in adds], absent))
            present = sorted((want & cur) - mine)  # desired, already there, not ours: adopt (module docstring)
            if present:
                adopt[key] = present
        if adopt:
            elsewhere = self._owned_elsewhere(adopt)
            adopt = {k: kept for k, ts in adopt.items() if (kept := [t for t in ts if (*k, t) not in elsewhere])}
        self._bump("targets", len(keys) - len(missing))
        if self.dry_run:
            for _key, ops, absent in plan:
                self._bump("adds", sum(1 for o, _ in ops if o == "add"))
                self._bump("removes", sum(1 for o, _ in ops if o == "remove"))
                self._bump("absent_removes", len(absent))
                self._bump("objects_to_change")
            self._bump("adopted", sum(len(ts) for ts in adopt.values()))
            self._bump("units_done")
            return
        adopted: list[tuple[Any, ...]] = []
        for key, ts in sorted(adopt.items()):
            for t in ts:
                at = self._journal_time(rows, key, t) or self._spooled_at(key[0], t, "add")
                if at:
                    self._bump("adopted_original_time")
                adopted.append((self.scope, unit.unit_id, key[0], key[1], *t, unit.generators.get((key[0], t)),
                                at or utc_iso(), ADOPTED_NOTE))

        # journal intents (one transaction per unit)
        staged: list[tuple[Key, list[Op], list[Op]]] = []
        with self._db:
            self.con.execute("BEGIN IMMEDIATE")
            try:
                for key, ops, absent in plan:
                    jops: list[Op] = []
                    for op, t in ops:
                        gen = unit.generators.get((key[0], t)) if op == "add" else owned_gens.get((key[0], t))
                        cur = self.con.execute(
                            "INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op,"
                            " generator, status) VALUES (?,?,?,?,?,?,?,?,?, 'intent')",
                            (self.scope, unit.unit_id, key[0], key[1], *t, op, gen))
                        jops.append(Op(cur.lastrowid, op, t))
                    aops: list[Op] = []
                    for t in absent:
                        cur = self.con.execute(
                            "INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op,"
                            " generator, status, applied_at, error) VALUES (?,?,?,?,?,?,?, 'remove', ?, 'applied', ?,"
                            " 'already absent in iRODS')",
                            (self.scope, unit.unit_id, key[0], key[1], *t, owned_gens.get((key[0], t)), utc_iso()))
                        aops.append(Op(cur.lastrowid, "remove", t))
                    staged.append((key, jops, aops))
                if adopted:
                    self.con.executemany(
                        "INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op,"
                        " generator, status, applied_at, error, adopted) VALUES (?,?,?,?,?,?,?, 'add', ?, 'applied',"
                        " ?, ?, 1)", adopted)
                self.con.execute("COMMIT")
            except BaseException:
                self.con.execute("ROLLBACK")
                raise
        self._bump("adopted", len(adopted))
        for key, jops, aops in staged:
            self._bump("absent_removes", len(aops))
            if self.stop.is_set():
                break  # remaining rows stay 'intent'; the next run reconciles them against iRODS
            if jops:
                self._bump("objects_changed")
                self.apply_target(session, key, jops)
        self._bump("units_done")

    # ---------------------------------------------------------- writes
    def _atomic(self, session: Any, key: Key, ops: list[Op]) -> None:
        from irods.meta import AVUOperation, iRODSMeta

        avu_ops = [AVUOperation(operation=o.op, avu=iRODSMeta(o.triple[0], o.triple[1], o.triple[2] or None))
                   for o in ops]
        self.bucket.acquire()
        t0 = self.clock()
        try:
            session.metadata.apply_atomic_operations(model_for(key[1]), key[0], *avu_ops)
        except Exception as exc:
            self._failed(exc)
            raise
        finally:
            dt = self.clock() - t0
            self._bump("atomic_calls")
        self._set_status(ops, "applied", applied=True)
        self._bump("adds", sum(1 for o in ops if o.op == "add"))
        self._bump("removes", sum(1 for o in ops if o.op == "remove"))
        self._ok(dt)

    def apply_target(self, session: Any, key: Key, ops: list[Op]) -> None:
        ordered = [o for o in ops if o.op == "remove"] + [o for o in ops if o.op == "add"]
        for i in range(0, len(ordered), MAX_OPS_PER_CALL):
            chunk = ordered[i:i + MAX_OPS_PER_CALL]
            try:
                self._atomic(session, key, chunk)
                continue
            except LatencyAbort:
                raise
            except Exception as exc:  # noqa: BLE001 - PRC raises many types
                log.warning("atomic metadata call failed on %s (%d ops): %s; re-reading", key[0], len(chunk),
                            _err_name(exc))
                self._bump("retries")
            chunk = self._recompute(session, key, chunk)
            if not chunk:
                continue
            try:
                self._atomic(session, key, chunk)
                continue
            except LatencyAbort:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("atomic retry failed on %s: %s; falling back to per-AVU calls", key[0], _err_name(exc))
            self._per_avu(session, key, chunk)

    def _recompute(self, session: Any, key: Key, chunk: list[Op]) -> list[Op]:
        """After a failed atomic call: drop ops iRODS already reflects.

        If *every* op is reflected the failed call most likely did apply (e.g. a connection error after
        the server committed), so those rows become 'applied'; otherwise reflected ones are 'noop'.
        """
        self.bucket.acquire()
        try:
            cur = read_target_avus(session, key)
        except Exception as exc:
            if _is_not_found(exc):
                self._set_status(chunk, "error", error="target missing: " + _err_name(exc))
                self._bump("errors", len(chunk))
                self._note("error_samples", f"{key[0]}: target missing")
                return []
            raise
        reflected = [o for o in chunk if (o.triple in cur) == (o.op == "add")]
        rest = [o for o in chunk if o not in reflected]
        if reflected and not rest:
            self._set_status(reflected, "applied", applied=True, error="applied (confirmed by re-read)")
            self._bump("adds", sum(1 for o in reflected if o.op == "add"))
            self._bump("removes", sum(1 for o in reflected if o.op == "remove"))
        elif reflected:
            self._set_status(reflected, "noop", error="already reflected in iRODS")
            self._bump("noops", len(reflected))
        return rest

    def _per_avu(self, session: Any, key: Key, chunk: list[Op]) -> None:
        from irods.meta import iRODSMeta

        model = model_for(key[1])
        for o in chunk:
            self.bucket.acquire()
            t0 = self.clock()
            try:
                meta = iRODSMeta(o.triple[0], o.triple[1], o.triple[2] or None)
                if o.op == "add":
                    session.metadata.add(model, key[0], meta)
                else:
                    session.metadata.remove(model, key[0], meta)
            except Exception as exc:  # noqa: BLE001
                if (o.op == "add" and _is_already_exists(exc)) or (o.op == "remove" and _is_not_found(exc)):
                    self._set_status([o], "noop", error=_err_name(exc))
                    self._bump("noops")
                else:
                    self._failed(exc)
                    self._set_status([o], "error", error=_err_name(exc))
                    self._bump("errors")
                    self._note("error_samples", f"{key[0]} {o.op} {o.triple[0]}: {_err_name(exc)}")
                continue
            finally:
                self._bump("fallback_calls")
            self._set_status([o], "applied", applied=True)
            self._bump("adds" if o.op == "add" else "removes")
            self._ok(self.clock() - t0)

    # ---------------------------------------------------------- driver
    def _guarded(self, unit: Unit) -> None:
        if self.stop.is_set():
            return
        try:
            self.process_unit(unit)
        except LatencyAbort:
            self.stop.set()
            raise
        except Exception as exc:  # noqa: BLE001 - one bad unit must not stop the rest
            self._failed(exc)  # e.g. a read query hit a connection reset: back off like a failed write
            log.error("unit %s failed: %s", unit.unit_id, _err_name(exc))
            self._bump("unit_failures")
            self._note("unit_errors", f"{unit.unit_id}: {_err_name(exc)}")

    def run(self, units: dict[str, Unit], threads: int) -> dict[str, Any]:
        t0 = time.time()
        self.stats["units"] = len(units)
        self.all_desired_paths = {k[0] for u in units.values() for k in u.desired}
        order = [units[k] for k in sorted(units)]
        try:
            if threads <= 1:
                for u in order:
                    self._guarded(u)
            else:
                with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="avu") as pool:
                    futs = [pool.submit(self._guarded, u) for u in order]
                    done, _ = wait(futs, return_when=FIRST_EXCEPTION)
                    failed = [f for f in done if f.exception() is not None]
                    if failed:
                        self.stop.set()
                        for f in futs:
                            f.cancel()
                        raise failed[0].exception()  # type: ignore[misc]
        finally:
            self.cleanup()
            p = self.monitor.p95()
            self.stats["p95_ms"] = round(p * 1000, 1) if p is not None else None
            self.stats["seconds"] = round(time.time() - t0, 1)
            snap = getattr(self.bucket, "snapshot", None)
            if snap is not None:
                r = snap()
                self.stats.update(rate_start=r["start"], rate_final=round(r["current"], 2),
                                  rate_min=round(r["min"], 2), rate_max=round(r["max"], 2),
                                  rate_changes=r["changes"])
            else:
                self.stats["rate_final"] = getattr(self.bucket, "rate", None)
            self.stats.update(self.pool.stats())
        return dict(self.stats)


def apply_scope(scope: str, *, threads: int = 8, rate: float = 20.0, dry_run: bool = False,
                only_units: Iterable[str] | None = None, con: sqlite3.Connection | None = None,
                session: Any | None = None, budget: ConnectionBudget | None = None,
                clock: Callable[[], float] = time.monotonic,
                sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Apply ``avu_desired`` rows of ``scope`` to iRODS (see module docstring). Returns stats.

    ``rate`` is the starting calls/s of the adaptive bucket (AIMD in [5, 60]); ``threads`` <= 16.
    ``con``/``session``/``budget``/``clock``/``sleep`` are injectable for tests; by default the journal
    at work/state.sqlite, the pooled MESA session (leased clones, one per concurrent unit) and the shared
    iRODS connection budget are used.
    """
    con = con or state.connect()
    ensure_journal_columns(con)
    units = load_desired(con, scope, only_units)
    if not units:
        log.warning("apply_scope(%s): no avu_desired rows%s", scope, " for the selected units" if only_units else "")
        return {"scope": scope, "dry_run": dry_run, "units": 0}
    if threads > MAX_THREADS:
        log.warning("threads=%d exceeds the iRODS connection budget; using %d", threads, MAX_THREADS)
    threads = max(1, min(threads, MAX_THREADS))
    if session is None:
        from ..env import mesa

        session = mesa().session
    root = session

    def spool_times() -> dict[tuple[str, str, str, str], tuple[str, str]]:
        from . import history

        return history.spooled_times(con, scope, session=root)

    writer = BulkWriter(con, scope, session, rate=rate, dry_run=dry_run, budget=budget, clock=clock, sleep=sleep,
                        spool_times=spool_times)
    stats = writer.run(units, threads)
    stats["threads"] = threads
    if stats.get("adopted"):
        log.warning("apply %s: adopted %d desired AVU(s) already present in iRODS but not in this journal (%d with "
                    "their original time)", scope, stats["adopted"], stats.get("adopted_original_time", 0))
    log.info("apply %s%s: units=%d targets=%d adds=%d removes=%d noops=%d errors=%d missing=%d calls=%d p95=%sms"
             " in %ss; rate %s -> %s calls/s (min %s, max %s)", scope, " (dry run)" if dry_run else "",
             stats.get("units", 0), stats.get("targets", 0), stats.get("adds", 0), stats.get("removes", 0),
             stats.get("noops", 0), stats.get("errors", 0), stats.get("missing", 0), stats.get("atomic_calls", 0),
             stats.get("p95_ms"), stats.get("seconds"), stats.get("rate_start"), stats.get("rate_final"),
             stats.get("rate_min"), stats.get("rate_max"))
    return stats
