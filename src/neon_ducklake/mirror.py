"""Idempotent iRODS mirror (contract § Uploads): replica-aware skip, checksum-registered put, verify.

``push_tree(items)`` takes ``(local Path, iRODS path, expected md5 | None)`` tuples:

- every target collection is pre-created (``collections.create(recurse=True)``; "exists" tolerated);
- one GenQuery per collection lists (DataObject.id, name, size, checksum, replica_number,
  replica_status), grouped by id; a file is skipped when a good replica (``replica_status == '1'``)
  has the local size and a checksum equal to the local MD5 (plain MD5 hex at CyVerse);
- otherwise ``put`` with ``REG_CHKSUM_KW`` (``num_threads=1`` below 512 MiB, else 4 on a 2-slot lane),
  then verify size + checksum (``data_objects.chksum`` when iRODS has none yet);
- LOCKED_DATA_OBJECT_ACCESS / SYS_REPLICA_INACCESSIBLE are retried after 60 s (3x);
  SYS_MAX_CONNECT_COUNT_EXCEEDED backs off and halves the concurrency;
- ``*.ducklake`` catalog files are uploaded LAST, and only when everything else succeeded.

Results go to the ``uploads`` table (and ``site_files`` when ``record_site`` is given). Local MD5s are
cached in ``mirror_md5`` by (path, size, mtime). Nothing is ever deleted remotely; orphans are reported.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import posixpath
import re
import sqlite3
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from . import env, state

log = logging.getLogger("neon_ducklake.mirror")

BIG_BYTES = 512 << 20
BIG_LANES = 2
BIG_THREADS = 4
MAX_THREADS = 16
RETRY_WAIT_S = 60.0
MAX_RETRIES = 3
MAX_CONN_RETRIES = 6
RETRYABLE = ("LOCKED_DATA_OBJECT_ACCESS", "SYS_REPLICA_INACCESSIBLE")
CONN_EXCEEDED = "SYS_MAX_CONNECT_COUNT_EXCEEDED"
CODE_FILES = ("README.md", "pyproject.toml", "uv.lock")
CODE_DIRS = ("scripts", "src", "sql", "docs", "tests")
SKIP_PARTS = {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}
MD5_DDL = """CREATE TABLE IF NOT EXISTS mirror_md5 (
  local_path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, md5 TEXT NOT NULL, computed_at TEXT
)"""

COUNTERS = ("files", "present", "uploaded", "bytes_uploaded", "errors", "blocked", "retries", "conn_backoffs",
            "md5_computed", "queries", "collections_created")

Item = tuple[Path, str, str | None]


class VerifyError(RuntimeError):
    """The uploaded object has no good replica with the local size and MD5."""


@dataclass
class Replica:
    number: int
    size: int
    checksum: str
    status: str


def norm_checksum(c: str | None) -> str:
    c = (c or "").strip()
    return c[4:].lower() if c.lower().startswith("md5:") else c.lower()


def good_replica(reps: Iterable[Replica], size: int, md5: str) -> bool:
    return any(r.status == "1" and r.size == size and norm_checksum(r.checksum) == md5.lower() for r in reps)


def md5_of(path: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        while chunk := fh.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def _err(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:500]


def _has(exc: BaseException, names: Iterable[str]) -> bool:
    s = f"{type(exc).__name__} {exc!r} {exc}"
    return any(n in s for n in names)


def remote_replicas(session: Any, coll: str) -> dict[str, list[Replica]]:
    """name -> replicas of every data object directly in ``coll`` (one GenQuery, grouped by id)."""
    from irods.models import Collection, DataObject

    q = session.query(DataObject.id, DataObject.name, DataObject.size, DataObject.checksum,
                      DataObject.replica_number, DataObject.replica_status).filter(Collection.name == coll)
    by_id: dict[Any, tuple[str, dict[int, Replica]]] = {}
    for row in q:
        ent = by_id.setdefault(row[DataObject.id], (row[DataObject.name], {}))
        n = int(row[DataObject.replica_number] or 0)
        ent[1][n] = Replica(n, int(row[DataObject.size] or 0), row[DataObject.checksum] or "",
                            str(row[DataObject.replica_status]))
    return {name: list(reps.values()) for name, reps in by_id.values()}


def existing_collections(session: Any, prefix: str) -> set[str]:
    from irods.column import Like
    from irods.models import Collection

    out = {r[Collection.name] for r in session.query(Collection.name).filter(Collection.name == prefix)}
    if out:
        out |= {r[Collection.name] for r in session.query(Collection.name).filter(Like(Collection.name, prefix + "/%"))}
    return out


class AdaptiveLimit:
    """A counting gate whose limit can be lowered at runtime."""

    def __init__(self, n: int) -> None:
        self.limit = max(1, n)
        self.active = 0
        self._cv = threading.Condition()

    def __enter__(self) -> Self:
        with self._cv:
            while self.active >= self.limit:
                self._cv.wait()
            self.active += 1
        return self

    def __exit__(self, *exc: object) -> None:
        with self._cv:
            self.active -= 1
            self._cv.notify_all()

    def shrink(self) -> int:
        with self._cv:
            self.limit = max(1, self.limit // 2)
            return self.limit


class Mirror:
    def __init__(self, session: Any, con: sqlite3.Connection, *, threads: int = 8, record_site: str | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.root_session, self.con, self.record_site, self.sleep = session, con, record_site, sleep
        self.threads = max(1, min(threads, MAX_THREADS))
        self.limit = AdaptiveLimit(self.threads)
        self.big = threading.Semaphore(BIG_LANES)
        self._db = threading.Lock()
        self._slock = threading.Lock()
        self._local = threading.local()
        self._clones: list[Any] = []
        self.known_colls: set[str] = set()
        self.stats: dict[str, Any] = defaultdict(int)
        self.stats.update({k: 0 for k in COUNTERS})
        self.stats["error_samples"] = []
        con.execute(MD5_DDL)

    # ---------------------------------------------------------- plumbing
    def session(self) -> Any:
        s = getattr(self._local, "session", None)
        if s is None:
            s = self.root_session.clone() if hasattr(self.root_session, "clone") else self.root_session
            self._local.session = s
            with self._slock:
                self._clones.append(s)
        return s

    def cleanup(self) -> None:
        for s in self._clones:
            if s is not self.root_session and hasattr(s, "cleanup"):
                try:
                    s.cleanup()
                except Exception as exc:  # noqa: BLE001
                    log.debug("session cleanup failed: %s", exc)
        self._clones.clear()

    def bump(self, key: str, n: int = 1) -> None:
        with self._slock:
            self.stats[key] += n

    # ---------------------------------------------------------- local md5 cache
    def local_md5(self, path: Path, size: int, mtime_ns: int) -> str:
        key = str(path)
        with self._db:
            row = self.con.execute("SELECT size, mtime_ns, md5 FROM mirror_md5 WHERE local_path=?", (key,)).fetchone()
        if row and row[0] == size and row[1] == mtime_ns:
            return row[2]
        md5 = md5_of(path)
        self.bump("md5_computed")
        with self._db:
            self.con.execute("INSERT OR REPLACE INTO mirror_md5(local_path, size, mtime_ns, md5, computed_at)"
                             " VALUES (?,?,?,?,?)", (key, size, mtime_ns, md5, env.utcnow().isoformat()))
        return md5

    # ---------------------------------------------------------- journal
    def record(self, local: Path, irods_path: str, size: int, md5: str | None, status: str, *,
               checksum: str | None = None, error: str | None = None, attempts: int = 0) -> None:
        now = env.utcnow().isoformat() if status in ("uploaded", "present") else None
        with self._db:
            self.con.execute(
                "INSERT INTO uploads(local_path, irods_path, size, md5, status, irods_checksum, uploaded_at, error,"
                " attempts) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(local_path) DO UPDATE SET"
                " irods_path=excluded.irods_path, size=excluded.size, md5=excluded.md5, status=excluded.status,"
                " irods_checksum=COALESCE(excluded.irods_checksum, uploads.irods_checksum),"
                " uploaded_at=COALESCE(excluded.uploaded_at, uploads.uploaded_at), error=excluded.error,"
                " attempts=uploads.attempts + excluded.attempts",
                (str(local), irods_path, size, md5, status, checksum, now, error, attempts))
            if self.record_site:
                rel = str(local.relative_to(env.ROOT)) if local.is_relative_to(env.ROOT) else str(local)
                self.con.execute(
                    "UPDATE site_files SET irods_path=?, up_status=?, up_attempts=COALESCE(up_attempts,0)+?,"
                    " up_error=?, irods_size=?, irods_checksum=COALESCE(?, irods_checksum),"
                    " uploaded_at=COALESCE(?, uploaded_at) WHERE site=? AND local_path IN (?, ?)",
                    (irods_path, status, attempts, error, size if status in ("uploaded", "present") else None,
                     checksum, now, self.record_site, str(local), rel))

    # ---------------------------------------------------------- collections
    def ensure_collections(self, colls: set[str]) -> None:
        todo = sorted(c for c in colls if c not in self.known_colls)
        if not todo:
            return
        s = self.root_session
        prefix = posixpath.commonpath(todo)
        existing = existing_collections(s, prefix)
        self.bump("queries")
        for c in sorted(todo, key=lambda p: (p.count("/"), p)):
            if c in existing or c in self.known_colls:
                self.known_colls.add(c)
                continue
            try:
                s.collections.create(c, recurse=True)
                self.bump("collections_created")
            except Exception as exc:
                if not _has(exc, ("CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME", "exist")):
                    raise
            parts = c.split("/")
            self.known_colls.update("/".join(parts[:i]) for i in range(2, len(parts) + 1))

    # ---------------------------------------------------------- one file
    def verify(self, session: Any, irods_path: str, size: int, md5: str) -> str:
        coll, name = posixpath.split(irods_path)
        reps = remote_replicas(session, coll).get(name, [])
        good = [r for r in reps if r.status == "1" and r.size == size]
        if not good:
            raise VerifyError(f"no good replica with size {size} for {irods_path} (replicas: "
                              f"{[(r.number, r.size, r.status) for r in reps]})")
        sums = {norm_checksum(r.checksum) for r in good if r.checksum}
        if not sums:
            sums = {norm_checksum(session.data_objects.chksum(irods_path))}
        if md5.lower() not in sums:
            raise VerifyError(f"checksum mismatch for {irods_path}: iRODS {sorted(sums)} != local {md5}")
        return md5.lower()

    def push_one(self, item: Item, reps: list[Replica]) -> str:
        import irods.keywords as kw

        local, irods_path, expected = item
        try:
            st = local.stat()
        except OSError as exc:
            self.record(local, irods_path, 0, None, "error", error=_err(exc))
            self.bump("errors")
            return "error"
        size = st.st_size
        md5 = (expected or self.local_md5(local, size, st.st_mtime_ns)).lower()
        if good_replica(reps, size, md5):
            self.record(local, irods_path, size, md5, "present", checksum=md5)
            self.bump("present")
            return "present"
        attempts = retries = conn_retries = 0
        exists = bool(reps)
        while True:
            attempts += 1
            session = self.session()
            try:
                opts = {kw.REG_CHKSUM_KW: ""}
                if exists:
                    opts[kw.FORCE_FLAG_KW] = ""
                with self.limit:
                    if size >= BIG_BYTES:
                        with self.big:
                            session.data_objects.put(str(local), irods_path, num_threads=BIG_THREADS, **opts)
                    else:
                        session.data_objects.put(str(local), irods_path, num_threads=1, **opts)
                    exists = True
                    checksum = self.verify(session, irods_path, size, md5)
            except Exception as exc:  # noqa: BLE001 - classify and retry below
                if _has(exc, (CONN_EXCEEDED,)) and conn_retries < MAX_CONN_RETRIES:
                    conn_retries += 1
                    new = self.limit.shrink()
                    log.warning("%s: connection budget exceeded; concurrency -> %d, backing off", irods_path, new)
                    self.bump("conn_backoffs")
                    self.sleep(min(300.0, 30.0 * conn_retries))
                    continue
                if _has(exc, RETRYABLE) and retries < MAX_RETRIES:
                    retries += 1
                    log.warning("%s: %s; retry %d/%d in %.0fs", irods_path, _err(exc), retries, MAX_RETRIES,
                                RETRY_WAIT_S)
                    self.bump("retries")
                    self.sleep(RETRY_WAIT_S)
                    continue
                if isinstance(exc, VerifyError) and retries < 1:
                    retries += 1
                    log.warning("%s; re-uploading once", exc)
                    self.bump("retries")
                    continue
                self.record(local, irods_path, size, md5, "error", error=_err(exc), attempts=attempts)
                self.bump("errors")
                with self._slock:
                    if len(self.stats["error_samples"]) < 20:
                        self.stats["error_samples"].append(f"{irods_path}: {_err(exc)}")
                log.error("upload failed %s: %s", irods_path, _err(exc))
                return "error"
            self.record(local, irods_path, size, md5, "uploaded", checksum=checksum, attempts=attempts)
            self.bump("uploaded")
            self.bump("bytes_uploaded", size)
            return "uploaded"

    # ---------------------------------------------------------- batches
    def _batch(self, pool: ThreadPoolExecutor, items: list[Item]) -> None:
        by_coll: dict[str, list[Item]] = defaultdict(list)
        for it in items:
            by_coll[posixpath.dirname(it[1])].append(it)

        def remote(coll: str) -> dict[str, list[Replica]]:
            with self.limit:
                self.bump("queries")
                return remote_replicas(self.session(), coll)

        remote_futs = {c: pool.submit(remote, c) for c in sorted(by_coll)}
        remote_state = {c: f.result() for c, f in remote_futs.items()}
        futs = [pool.submit(self.push_one, it, remote_state[posixpath.dirname(it[1])].get(posixpath.basename(it[1]), []))
                for c in sorted(by_coll) for it in by_coll[c]]
        for f in futs:
            f.result()

    def run(self, items: list[Item]) -> dict[str, Any]:
        t0 = time.time()
        seen: dict[str, Item] = {}
        for it in items:
            local, ipath, md5 = Path(it[0]), it[1].rstrip("/"), it[2]
            if ipath in seen and seen[ipath][0] != local:
                raise ValueError(f"two local files map to {ipath}: {seen[ipath][0]} and {local}")
            seen[ipath] = (local, ipath, md5)
        items = list(seen.values())
        catalogs = [it for it in items if it[1].endswith(".ducklake")]
        rest = [it for it in items if not it[1].endswith(".ducklake")]
        if any(it[0].exists() and it[0].stat().st_size >= BIG_BYTES for it in items):
            # 2 big lanes x 4 streams must still fit the 16-connection budget
            self.threads = min(self.threads, MAX_THREADS - BIG_LANES * (BIG_THREADS - 1))
            self.limit = AdaptiveLimit(self.threads)
        self.stats.update(files=len(items), threads_start=self.threads)
        try:
            self.ensure_collections({posixpath.dirname(it[1]) for it in items})
            with ThreadPoolExecutor(max_workers=self.threads, thread_name_prefix="put") as pool:
                if rest:
                    self._batch(pool, rest)
                if catalogs:
                    if self.stats["errors"]:
                        for local, ipath, md5 in catalogs:
                            size = local.stat().st_size if local.exists() else 0
                            self.record(local, ipath, size, md5, "blocked",
                                        error="not uploaded: other files in this push failed")
                        self.bump("blocked", len(catalogs))
                        log.error("catalog file(s) %s NOT uploaded: %d other upload(s) failed",
                                  [c[1] for c in catalogs], self.stats["errors"])
                    else:
                        self._batch(pool, catalogs)
        finally:
            self.cleanup()
        self.stats["threads_final"] = self.limit.limit
        self.stats["seconds"] = round(time.time() - t0, 1)
        return dict(self.stats)


def push_tree(items: list[Item], *, threads: int = 8, record_site: str | None = None, session: Any | None = None,
              con: sqlite3.Connection | None = None, sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Upload ``items`` idempotently (see module docstring). ``*.ducklake`` files go last. Returns stats."""
    con = con or state.connect()
    if session is None:
        session = env.mesa().session
    stats = Mirror(session, con, threads=threads, record_site=record_site, sleep=sleep).run(items)
    log.info("push_tree: %d files, %d present, %d uploaded (%.1f MB), %d errors, %d blocked in %ss",
             stats.get("files", 0), stats.get("present", 0), stats.get("uploaded", 0),
             stats.get("bytes_uploaded", 0) / 1e6, stats.get("errors", 0), stats.get("blocked", 0),
             stats.get("seconds"))
    return stats


# ---------------------------------------------------------------- orphans (report only)

def report_orphans(session: Any, items: list[Item], roots: list[tuple[str, bool]]) -> dict[str, Any]:
    """Remote objects under ``roots`` ((collection, recursive)) with no local counterpart. Never deletes."""
    from irods.column import Like
    from irods.models import Collection, DataObject

    local = {it[1] for it in items}
    orphans: set[str] = set()
    for root, recursive in roots:
        qs = [session.query(Collection.name, DataObject.name).filter(Collection.name == root)]
        if recursive:
            qs.append(session.query(Collection.name, DataObject.name).filter(Like(Collection.name, root + "/%")))
        for q in qs:
            for r in q:
                p = f"{r[Collection.name]}/{r[DataObject.name]}"
                if p not in local:
                    orphans.add(p)
    return {"count": len(orphans), "sample": sorted(orphans)[:20]}


# ---------------------------------------------------------------- what to mirror

def _rel_items(paths: Iterable[Path]) -> list[Item]:
    return [(p, f"{env.IRODS_ROOT}/{p.relative_to(env.ROOT).as_posix()}", None) for p in paths]


def _tree(d: Path) -> list[Path]:
    if not d.is_dir():
        return []
    return sorted(p for p in d.rglob("*") if p.is_file() and not SKIP_PARTS.intersection(p.parts)
                  and not p.name.endswith((".pyc", ".part", ".wal")))


def code_items() -> tuple[list[Item], list[tuple[str, bool]]]:
    files = [env.ROOT / f for f in CODE_FILES if (env.ROOT / f).is_file()]
    for d in CODE_DIRS:
        files += _tree(env.ROOT / d)
    cur = env.ROOT / "curation"
    if cur.is_dir():
        files += sorted(p for p in cur.iterdir() if p.is_file() and p.suffix in (".md", ".json"))
    roots = [(env.IRODS_ROOT, False), (f"{env.IRODS_ROOT}/curation", False)]
    roots += [(f"{env.IRODS_ROOT}/{d}", True) for d in CODE_DIRS if (env.ROOT / d).is_dir()]
    return _rel_items(files), roots


_SIGNED = re.compile(r"X-Goog-[A-Za-z-]+=[^&\"\s]*")


def export_ledger(run_id: str, con: sqlite3.Connection) -> Path:
    """api/runs/<runId>/fetch_log.jsonl: the request ledger of that run (no signed URLs)."""
    out = env.RUNS_DIR / run_id / "fetch_log.jsonl"
    tmp = out.with_name(out.name + ".part")
    cols = ("family", "key", "method", "path", "params", "http_status", "attempts", "elapsed_ms",
            "response_bytes", "landed_path", "landed_bytes", "sha256", "status", "error", "fetched_at")
    n = 0
    with tmp.open("w") as fh:
        for r in con.execute(f"SELECT {', '.join(cols)} FROM requests WHERE run_id=? ORDER BY family, key",
                             (run_id,)):
            line = json.dumps(dict(zip(cols, tuple(r), strict=True)), ensure_ascii=False, default=str)
            fh.write(_SIGNED.sub("X-Goog-REDACTED", line) + "\n")
            n += 1
    os.replace(tmp, out)
    log.info("exported %d ledger rows to %s", n, out)
    return out


def api_items(run_id: str | None, con: sqlite3.Connection) -> tuple[list[Item], list[tuple[str, bool]]]:
    from .avu.vocab import latest_run_id

    rid = run_id or latest_run_id()
    if not rid:
        raise SystemExit("no harvest run found under api/runs")
    run_dir = env.RUNS_DIR / rid
    row = con.execute("SELECT status FROM runs WHERE run_id=? AND stage='catalog-fetch'", (rid,)).fetchone()
    if row and row[0] == "running":
        raise SystemExit(f"harvest {rid} is still running; mirror 'api' after it finished and was sealed")
    sealed = run_dir / "sealed"
    if not sealed.is_dir():
        raise SystemExit(f"{sealed} does not exist: run `neon-ducklake catalog seal` first")
    manifest = run_dir / "fetch_manifest.json"
    if not manifest.exists():
        raise SystemExit(f"{manifest} missing")
    files = _tree(sealed) + [manifest, export_ledger(rid, con)]
    if (env.API_DIR / "LATEST").is_file():
        files.append(env.API_DIR / "LATEST")
    return _rel_items(files), [(f"{env.IRODS_ROOT}/api/runs/{rid}", True)]


def lake_items() -> tuple[list[Item], list[tuple[str, bool]]]:
    cat = env.LAKE_DIR / env.CATALOG_LAKE
    if not cat.exists():
        raise SystemExit(f"{cat} does not exist: run `neon-ducklake catalog build` first")
    wal = sorted(env.LAKE_DIR.glob("*.wal"))
    if wal:
        raise SystemExit(f"{[w.name for w in wal]} present: a writer is active or did not CHECKPOINT; not mirroring")
    holders = env.catalog_lock_holders(cat)
    if holders:
        raise SystemExit(f"{cat.name} is open in PID(s) {[h['pid'] for h in holders]}; not mirroring")
    files = _tree(env.LAKE_DIR / "data") + [cat]
    return _rel_items(files), [(f"{env.IRODS_ROOT}/ducklake", True)]


def main(what: set[str], run_id: str | None = None, threads: int = 8) -> int:
    """``neon-ducklake mirror --what code,api,lake``: in that order; the lake catalog file goes last."""
    what = {w.strip() for w in what if w.strip()}
    unknown = what - {"code", "api", "lake"}
    if unknown:
        raise SystemExit(f"unknown --what {sorted(unknown)} (choose from code, api, lake)")
    con = state.connect()
    m = env.mesa()
    rc = 0
    for w in ("code", "api", "lake"):
        if w not in what:
            continue
        if w == "code":
            items, roots = code_items()
        elif w == "api":
            items, roots = api_items(run_id, con)
        else:
            items, roots = lake_items()
        stats = push_tree(items, threads=threads, session=m.session, con=con)
        stats["orphans"] = report_orphans(m.session, items, roots)
        print(f"== mirror {w}: {stats.get('files', 0)} files, {stats.get('present', 0)} present, "
              f"{stats.get('uploaded', 0)} uploaded ({stats.get('bytes_uploaded', 0) / 1e6:.1f} MB), "
              f"{stats.get('errors', 0)} errors, {stats.get('blocked', 0)} blocked, "
              f"orphans {stats['orphans']['count']}, {stats.get('seconds')}s")
        for s in stats.get("error_samples", []):
            print(f"   error: {s}")
        for o in stats["orphans"]["sample"]:
            print(f"   orphan (kept): {o}")
        if stats.get("errors") or stats.get("blocked"):
            rc = 1
    return rc
