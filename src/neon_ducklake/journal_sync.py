"""Per-site journal backup on the Data Store (docs/STREAMING.md, "Journal backup").

A worker VM keeps its journal in ``work/state.sqlite``. When the VM dies, another VM that takes the site
over needs that journal (which files were listed, uploaded, which AVUs were applied and spooled, which chunks
and finalize steps are done).

**Layout** under ``J = <IRODS_ROOT>/sites/<SITE>/.mesa-journal/``: every push writes an immutable, versioned
set ``J/v/<pushId>/<table>.parquet`` (the site's rows of every table in :data:`TABLES`: ``site_files``,
``listings``, ``stream_chunks``, ``stream_sites``, ``fetch_superseded``, ``lake_pushes``, ``site_run_chunks``
by ``site``; ``avu_journal``, ``batches``, ``avu_desired`` by ``scope``; a table missing on this host is
skipped) plus ``J/v/<pushId>/manifest.json`` (rows, sha256, the table's DDL and column declarations, pushId,
vm, host, pushedAt), then publishes it by replacing the pointer ``J/CURRENT`` last. The pointer holds the
pushId, pushedAt, vm, host, the manifest's relative path and sha256, and ``prev``: the two sets before it.
Nothing is ever overwritten in place; the last :data:`KEEP_SETS` sets (the pointer's chain) are kept, and
older sets are deleted only once a newer pointer has landed (and after :data:`PRUNE_GRACE_S`). The flat
layout of earlier versions (``J/manifest.json`` + ``J/<table>.parquet``) is still read as a set.

**Push** (``push_site_journal(site)``), under the host lock ``journal-push-<SITE>``:

1. Read the pointer. Refuse (:class:`JournalConflict`) when the Data Store holds a journal this host has not
   seen (:func:`needs_restore`), so a VM that lost the site never overwrites the new owner's backup.
2. Export every table inside ONE read transaction of the SQLite connection, so the files are one consistent
   snapshot (a concurrent ``history`` stage cannot tear ``avu_journal`` from ``batches``).
3. Upload the set (checksum-verified, ``mirror.push_tree``), manifest last.
4. Write the new pointer as ``J/staged/<pushId>.json``, re-check ``coord.holds_site(site)`` (False: raise
   :class:`ClaimLost`, nothing published; a claim the heartbeat is rewriting at that moment is read again,
   and one unreadable for the whole window raises ``coord.ClaimUnreadable``, a transient error, also with
   nothing published), then swap it in with iRODS renames, which refuse an existing
   target (``coord.rename``): move ``CURRENT`` to ``J/retired/<pushId>.json``, check that it is the pointer
   this push was built on (else put it back and raise :class:`JournalConflict`), move the staged pointer to
   ``CURRENT``. Only one pusher can take ``CURRENT`` away, so the conflict check and the pointer write are
   one atomic step. A reader that finds no ``CURRENT`` while ``retired/X`` and ``staged/X`` both exist sees
   a swap in flight and uses the retired (still committed) pointer; a swap left like that for longer than
   :data:`SWAP_GRACE_S` (its pusher died) is rolled back by the next push.
5. Mark the push locally and prune sets outside the pointer's chain.

A failed push deletes its own set; the pointer is untouched.

**Restore** (``restore_site_journal(site, *, force=False) -> bool``; :func:`restore_detail` returns the
summary) reads the pointer and downloads its set, checking every file against the manifest and the manifest
against the pointer. A set that fails its checksums (or misses a file) is skipped for the previous one in
the chain, with an error in the log and ``fallback``/``skipped`` in the summary; the restore is then marked
with the pointer it saw, so this host may push over the broken set. Transient iRODS errors are raised, never
fallen back from. The restore creates every table with its owner module's schema (``chunks.ensure_schema``,
``evict.ensure_schema``, ``fetch.SUPERSEDED_SCHEMA``, ``bulk.ensure_desired``; other tables from the DDL in
the manifest), ADDS any column the backup has and this host lacks (never drops one) and replaces the site's
rows in one transaction. ``avu_journal.id`` is re-assigned locally (rows are linked to their batch by
``batch_id``, never by id). In the same transaction :func:`release_torn_batches` releases ``applied`` rows
whose batch is not in flight (a backup torn between ``avu_journal`` and ``batches`` by an older version): they
are spooled again, and the recorder's content dedup keeps MESA history single. Then, for a journal written on
another VM: chunks that never reached ``uploaded`` and whose raw files are not on this host go back to
``planned`` (``site download`` re-fetches their files), and when the finalize ``upload`` step is not ``ok``
the steps before it are forgotten (their cards and curation files were on the other VM's disk).

``needs_restore(site)``: the pointer names a push this host has not pushed or restored last (by pushId, or
by ``pushedAt`` for a flat-layout backup) that is newer or was pushed by another VM. Marks live in
``journal_sync_marks(site, direction, manifest_pushed_at, vm, marked_at, push_id)`` (``direction`` = push |
restore).

Column types follow the SQLite declarations (INTEGER -> int64, REAL -> float64, else string); a numeric
column that holds a non-numeric value in this site's rows is written as string instead. SQLite's type
affinity restores the original values either way.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import posixpath
import re
import shutil
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import coord, env, state

log = logging.getLogger("neon_ducklake.journal_sync")

# table -> the column that selects the site's rows (tables missing on the pushing host are skipped)
TABLES: dict[str, str] = {"site_files": "site", "listings": "site", "stream_chunks": "site", "stream_sites": "site",
                          "fetch_superseded": "site", "avu_journal": "scope", "batches": "scope",
                          "avu_desired": "scope", "lake_pushes": "site", "site_run_chunks": "site"}
# host-local tables that must never travel between VMs (documented; they are not in TABLES)
HOST_LOCAL: tuple[str, ...] = ("fetch_workset", "journal_sync_marks", "uploads", "requests", "runs", "evict_log")
# columns never restored: row ids local to each host (any INTEGER PRIMARY KEY, e.g. lake_pushes.id, is
# dropped as well, see _dropped); rows keep their order, so re-assigned ids keep it too
DROP_ON_RESTORE: dict[str, tuple[str, ...]] = {"avu_journal": ("id",), "lake_pushes": ("id",)}
MARKS_DDL = """CREATE TABLE IF NOT EXISTS journal_sync_marks (
  site TEXT NOT NULL, direction TEXT NOT NULL, manifest_pushed_at TEXT, vm TEXT, marked_at TEXT, push_id TEXT,
  PRIMARY KEY (site, direction)
)"""
FORMAT = "neon-ducklake-journal/2"
MANIFEST = "manifest.json"
POINTER = "CURRENT"
VERSIONS = "v"  # J/v/<pushId>/<table>.parquet + manifest.json: one immutable set per push
STAGED = "staged"  # J/staged/<pushId>.json: a new pointer, written before the swap
RETIRED = "retired"  # J/retired/<pushId>.json: the old pointer, moved aside by push <pushId> during its swap
KEEP_SETS = 3  # the pointer's set plus the two before it (restore falls back along this chain)
SWAP_GRACE_S = 300.0  # a swap whose staged pointer is older than this was interrupted: the next push rolls it back
PRUNE_GRACE_S = 600.0  # a set outside the pointer's chain is deleted only once it is older than this
FETCH_ROWS = 50_000
# finalize steps whose outputs (cards, curation traces, terms) live on the pushing VM's disk until `upload`
PRE_UPLOAD_STEPS: tuple[str, ...] = ("terms", "cards", "curate", "validate", "avus")
# batch statuses whose 'applied' rows a restore leaves attached: completed by history._finish_spooling
# (spool) or history.recover (direct: the MESA snapshot decides)
IN_FLIGHT_BATCHES: tuple[str, ...] = ("spooling", "recording", "pending", "failed")


class JournalConflict(RuntimeError):
    """The Data Store holds a newer journal of the site than this host has (restore it first), or another push
    replaced the pointer while this one ran."""


class ClaimLost(JournalConflict):
    """This process may no longer write the site's shared state (``coord.holds_site`` is False): nothing was
    published."""


class JournalSetBroken(RuntimeError):
    """A journal set on the Data Store fails its checksums, misses a file or has an unreadable manifest."""


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _exists(con: sqlite3.Connection, table: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(con: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    return [(r[1], (r[2] or "").upper()) for r in con.execute(f"PRAGMA table_info({_q(table)})")]


def _column_decls(con: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [{"name": r[1], "type": r[2] or "", "notnull": bool(r[3]), "default": r[4], "pk": int(r[5] or 0)}
            for r in con.execute(f"PRAGMA table_info({_q(table)})")]


def _ddl(con: sqlite3.Connection, table: str) -> str | None:
    r = con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return r[0] if r else None


def _schema(con: sqlite3.Connection, table: str, key: str, site: str) -> Any:
    import pyarrow as pa

    fields = []
    for name, decl in _columns(con, table):
        if "INT" in decl:
            typ, ok = pa.int64(), ("integer", "null")
        elif any(t in decl for t in ("REAL", "FLOA", "DOUB")):
            typ, ok = pa.float64(), ("real", "integer", "null")
        else:
            typ, ok = pa.string(), None
        if ok is not None:
            bad = con.execute(f'SELECT count(*) FROM {_q(table)} WHERE {_q(key)}=? AND typeof({_q(name)}) NOT IN '
                              f"({', '.join('?' * len(ok))})", (site, *ok)).fetchone()[0]
            if bad:
                typ = pa.string()
        fields.append(pa.field(name, typ))
    return pa.schema(fields)


def _cell(v: Any, typ: Any) -> Any:
    import pyarrow as pa

    if v is None:
        return None
    if typ == pa.string():
        return v if isinstance(v, str) else str(v)
    if typ == pa.float64():
        return float(v)
    return int(v)


def export_table(con: sqlite3.Connection, table: str, site: str, path: Path) -> dict[str, Any]:
    """The site's rows of ``table`` -> ``path`` (streamed in batches). Returns {rows, sha256, bytes, ddl, columns}."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    key = TABLES[table]
    schema = _schema(con, table, key, site)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    rows = 0
    cur = con.execute(f"SELECT * FROM {_q(table)} WHERE {_q(key)}=? ORDER BY rowid", (site,))
    with pq.ParquetWriter(tmp, schema, compression="zstd") as w:
        while batch := cur.fetchmany(FETCH_ROWS):
            cols = {f.name: [_cell(r[i], f.type) for r in batch] for i, f in enumerate(schema)}
            w.write_table(pa.Table.from_pydict(cols, schema=schema))
            rows += len(batch)
    tmp.replace(path)
    return {"rows": rows, "sha256": _sha256(path), "bytes": path.stat().st_size, "ddl": _ddl(con, table),
            "columns": _column_decls(con, table)}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def export_tables(con: sqlite3.Connection, site: str, work: Path) -> dict[str, dict[str, Any] | None]:
    """Export the site's rows of every table in :data:`TABLES` to ``work/<table>.parquet`` inside ONE read
    transaction, so all files come from one snapshot of the journal (WAL: concurrent writers go on, unseen).
    Returns {table: export_table info, or None when the table does not exist here}."""
    own = not con.in_transaction
    if own:
        con.execute("BEGIN")  # deferred: the first read below pins the snapshot for every table
    try:
        out: dict[str, dict[str, Any] | None] = {}
        for table in TABLES:
            out[table] = export_table(con, table, site, work / f"{table}.parquet") if _exists(con, table) else None
        return out
    finally:
        if own:
            con.execute("COMMIT")  # read-only: nothing to write


# ---------------------------------------------------------------- marks (last push / restore on this host)

def _ts(value: Any) -> datetime:
    try:
        ts = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return datetime.fromtimestamp(0, UTC)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts


def _ensure_marks(con: sqlite3.Connection) -> None:
    con.execute(MARKS_DDL)
    if not any(r[1] == "push_id" for r in con.execute("PRAGMA table_info(journal_sync_marks)")):
        try:
            con.execute("ALTER TABLE journal_sync_marks ADD COLUMN push_id TEXT")
        except sqlite3.OperationalError as exc:  # another process added it first
            if "duplicate column" not in str(exc).lower():
                raise


def _mark(con: sqlite3.Connection, site: str, direction: str, pushed_at: str | None, vm: str | None,
          push_id: str | None = None) -> None:
    _ensure_marks(con)
    con.execute("INSERT INTO journal_sync_marks(site, direction, manifest_pushed_at, vm, marked_at, push_id)"
                " VALUES (?,?,?,?,?,?) ON CONFLICT(site, direction) DO UPDATE SET"
                " manifest_pushed_at=excluded.manifest_pushed_at, vm=excluded.vm, marked_at=excluded.marked_at,"
                " push_id=excluded.push_id",
                (site, direction, pushed_at, vm, env.utcnow().isoformat(), push_id))


def last_mark(site: str, *, con: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """This host's latest push or restore of ``site``: {direction, manifest_pushed_at, vm, marked_at, push_id}."""
    con = con or state.connect()
    _ensure_marks(con)
    rows = [dict(r) for r in con.execute("SELECT * FROM journal_sync_marks WHERE site=?", (site.upper(),))]
    return max(rows, key=lambda r: _ts(r["marked_at"])) if rows else None


def needs_restore(site: str, *, session: Any | None = None, con: sqlite3.Connection | None = None,
                  doc: dict[str, Any] | None = None) -> bool:
    """Whether the Data Store holds a journal of ``site`` this host has not seen.

    ``doc`` is the pointer (:func:`read_pointer`; read when not given). True when a pointer exists and either
    this host never pushed or restored the site (and the pointer was pushed by another VM, or this host has
    no rows of the site), or it names another push than this host's last mark (pushId; ``pushedAt`` for a
    flat-layout backup) that is newer or was pushed by another VM.
    """
    site = site.upper()
    con = con or state.connect()
    doc = doc if doc is not None else read_pointer(site, session=session)
    if doc is None:
        return False
    pushed, by, pid = doc.get("pushedAt"), doc.get("vm"), doc.get("pushId")
    mark = last_mark(site, con=con)
    if mark is None:
        return by != coord.vm_id() or local_rows(site, con=con) == 0
    if pid and mark.get("push_id"):
        if mark["push_id"] == pid:
            return False
    elif mark["manifest_pushed_at"] == pushed:
        return False
    return _ts(pushed) > _ts(mark["manifest_pushed_at"]) or by != coord.vm_id()


# ---------------------------------------------------------------- the pointer

def _jdir(site: str) -> str:
    return coord.journal_dir(site)


def new_push_id(now: datetime | None = None) -> str:
    """``<UTC time>-<vm>-<random>``: unique, and sorts by time for one clock."""
    now = now or env.utcnow()
    vm = re.sub(r"[^A-Za-z0-9._-]", "_", coord.vm_id())[:40] or "vm"
    return f"{now.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')}-{vm}-{uuid.uuid4().hex[:8]}"


def _valid_pointer(doc: Any) -> bool:
    return isinstance(doc, dict) and bool(doc.get("manifest")) and bool(doc.get("pushedAt"))


def _entry(ptr: dict[str, Any]) -> dict[str, Any]:
    """The part of a pointer that names its set (kept in a later pointer's ``prev``)."""
    return {k: ptr.get(k) for k in ("pushId", "pushedAt", "vm", "host", "manifest", "manifestSha256")}


def _chain(cur: dict[str, Any] | None) -> list[dict[str, Any]]:
    """``prev`` of a pointer that replaces ``cur``: ``cur`` and the sets before it, KEEP_SETS - 1 at most."""
    if cur is None:
        return []
    return ([_entry(cur)] + [e for e in cur.get("prev") or [] if _valid_pointer(e)])[:KEEP_SETS - 1]


def _set_dir(manifest_rel: str) -> str:
    """The set of a manifest: ``v/<pushId>`` (or ``""`` for the flat layout's ``manifest.json``)."""
    return posixpath.dirname(manifest_rel)


def _age_s(obj: coord.RemoteObject | None, now: datetime) -> float:
    if obj is None or obj.modified is None:
        return float("inf")
    return (now - obj.modified).total_seconds()


def _listing(s: Any, site: str) -> dict[str, coord.RemoteObject]:
    """Every object under the site's journal collection, by path relative to it."""
    j = _jdir(site)
    return {p[len(j) + 1:]: o for p, o in coord.list_objects(s, j, recursive=True).items() if p.startswith(j + "/")}


def read_pointer(site: str, *, session: Any | None = None) -> dict[str, Any] | None:
    """The committed journal pointer of ``site`` (None: no backup), with ``state``:

    - ``current``: ``J/CURRENT``;
    - ``swapping``: no ``CURRENT`` because a swap is in flight (or its pusher died): the retired pointer,
      which is still the committed one, with ``swapId`` and ``swapAgeS`` (age of the staged pointer);
    - ``legacy``: the flat layout of earlier versions (``J/manifest.json``);
    - ``orphan``: no pointer at all, only sets: the newest set, the others as ``prev``.

    ``corrupt`` is True when a ``CURRENT`` object exists but holds no valid pointer.
    """
    s = coord.get_session(session)
    j = _jdir(site)
    doc = coord.read_json(s, f"{j}/{POINTER}")
    if _valid_pointer(doc):
        return {**doc, "state": "current", "corrupt": False}
    now = env.utcnow()
    rel = _listing(s, site)
    corrupt = POINTER in rel
    if corrupt:
        log.error("journal of %s: %s/%s holds no valid pointer", site.upper(), j, POINTER)
    for name in sorted(rel):
        if not (name.startswith(RETIRED + "/") and name.endswith(".json")):
            continue
        swap = name[len(RETIRED) + 1:-len(".json")]
        staged = rel.get(f"{STAGED}/{swap}.json")
        if staged is None:
            continue  # a finished swap whose leftover was not deleted
        old = coord.read_json(s, f"{j}/{name}")
        if _valid_pointer(old):
            new = coord.read_json(s, f"{j}/{STAGED}/{swap}.json")
            by = new if isinstance(new, dict) else {}
            return {**old, "state": "swapping", "swapId": swap, "swapAgeS": _age_s(staged, now),
                    "swapVm": by.get("vm"), "swapHost": by.get("host"), "corrupt": corrupt}
    if MANIFEST in rel:
        m = coord.read_json(s, f"{j}/{MANIFEST}")
        if isinstance(m, dict) and m.get("pushedAt"):
            return {"format": "legacy", "site": site.upper(), "pushId": None, "pushedAt": m.get("pushedAt"),
                    "vm": m.get("vm"), "host": m.get("host"), "manifest": MANIFEST, "manifestSha256": None,
                    "prev": [], "state": "legacy", "corrupt": corrupt}
    sets: list[dict[str, Any]] = []
    for name in rel:
        parts = name.split("/")
        if len(parts) == 3 and parts[0] == VERSIONS and parts[2] == MANIFEST:
            m = coord.read_json(s, f"{j}/{name}")
            if isinstance(m, dict) and m.get("pushedAt"):
                sets.append({"pushId": m.get("pushId") or parts[1], "pushedAt": m.get("pushedAt"),
                             "vm": m.get("vm"), "host": m.get("host"), "manifest": name, "manifestSha256": None})
    if not sets:
        return None
    sets.sort(key=lambda e: (_ts(e["pushedAt"]), str(e["pushId"])), reverse=True)
    log.warning("journal of %s: no pointer; using the newest set %s", site.upper(), sets[0]["pushId"])
    return {"format": FORMAT, "site": site.upper(), **sets[0], "prev": sets[1:KEEP_SETS], "state": "orphan",
            "corrupt": corrupt}


def remote_manifest(site: str, *, session: Any | None = None) -> dict[str, Any] | None:
    """The committed pointer of the site's journal (:func:`read_pointer`): pushId, pushedAt, vm, host, ..."""
    return read_pointer(site, session=session)


def _settle_swap(s: Any, site: str, cur: dict[str, Any]) -> dict[str, Any] | None:
    """A pointer swap without ``CURRENT``: in flight (raise :class:`JournalConflict`), or interrupted: roll it back
    (the retired pointer becomes ``CURRENT`` again) and re-read the pointer. A swap is interrupted when it is
    older than SWAP_GRACE_S, or when this VM and host staged it: the caller holds the host lock
    ``journal-push-<SITE>``, so no other push of this host can still be running it."""
    j = _jdir(site)
    swap = cur["swapId"]
    ours = cur.get("swapVm") == coord.vm_id() and cur.get("swapHost") == coord.host()
    if not ours and cur.get("swapAgeS", float("inf")) < SWAP_GRACE_S:
        raise JournalConflict(f"a journal push of {site} by another process is swapping its pointer ({swap}); "
                              "try again later")
    res = coord.rename(s, f"{j}/{RETIRED}/{swap}.json", f"{j}/{POINTER}")
    log.warning("journal of %s: interrupted pointer swap %s rolled back (%s)", site, swap, res)
    with contextlib.suppress(Exception):
        coord.unlink(s, f"{j}/{STAGED}/{swap}.json")
    return read_pointer(site, session=s)


def _put_back(s: Any, retired: str, current: str) -> None:
    res = coord.rename(s, retired, current)
    if res != "ok":
        log.error("journal pointer %s could not be put back to %s (%s); the next push repairs it", retired,
                  current, res)


def _publish(s: Any, site: str, cur: dict[str, Any] | None, pointer: dict[str, Any], *, force: bool) -> None:
    """Write ``pointer`` as staged, re-check the claim, then swap it in for ``cur`` (compare-and-swap through
    renames; see the module docstring). Raises :class:`ClaimLost` or :class:`JournalConflict` before anything
    is published."""
    j = _jdir(site)
    pid = pointer["pushId"]
    staged, retired, current = f"{j}/{STAGED}/{pid}.json", f"{j}/{RETIRED}/{pid}.json", f"{j}/{POINTER}"
    coord.put_json(s, staged, pointer)
    try:
        if not force and not coord.holds_site(site, session=s):
            raise ClaimLost(f"claim lost: {coord.vm_id()} may no longer write the journal of {site} (its site claim "
                            "is gone, stale or another VM's); the pointer was not replaced")
        replace = cur is not None and (cur.get("state") == "current" or cur.get("corrupt"))
        if not replace:
            res = coord.rename(s, staged, current)
            if res == "exists":
                raise JournalConflict(f"another push installed a journal pointer of {site} first; restore it")
            if res != "ok":
                raise RuntimeError(f"journal pointer {staged} vanished before it was installed")
            return
        res = coord.rename(s, current, retired)
        if res == "missing":
            raise JournalConflict(f"the journal pointer of {site} is being replaced by another push")
        if res != "ok":
            raise RuntimeError(f"journal pointer of {site}: {retired} already exists")
        if cur.get("state") == "current" and not cur.get("corrupt"):
            moved = coord.read_json(s, retired)
            if not isinstance(moved, dict) or moved.get("pushId") != cur.get("pushId"):
                _put_back(s, retired, current)
                who = moved.get("vm") if isinstance(moved, dict) else "?"
                raise JournalConflict(f"the journal pointer of {site} was replaced by {who} while this push ran "
                                      f"({cur.get('pushId')} -> {(moved or {}).get('pushId')}); restore it first")
        res = coord.rename(s, staged, current)
        if res != "ok":
            _put_back(s, retired, current)
            raise JournalConflict(f"the journal pointer of {site} could not be installed ({res})")
    except BaseException:
        with contextlib.suppress(Exception):
            coord.unlink(s, staged)
        raise
    with contextlib.suppress(Exception):
        coord.unlink(s, retired)


def _delete_objects(s: Any, objs: list[coord.RemoteObject]) -> int:
    n = 0
    for o in objs:
        with contextlib.suppress(Exception):
            n += bool(coord.unlink(s, o.path))
    return n


def _delete_set(s: Any, site: str, set_rel: str) -> int:
    """Delete one set (a failed push's own upload)."""
    j = _jdir(site)
    objs = list(coord.list_objects(s, f"{j}/{set_rel}").values())
    n = _delete_objects(s, objs)
    with contextlib.suppress(Exception):
        s.collections.remove(f"{j}/{set_rel}", recurse=False, force=True)
    return n


def prune(site: str, pointer: dict[str, Any], *, session: Any | None = None, now: datetime | None = None) -> int:
    """Delete the sets outside ``pointer``'s chain (older than PRUNE_GRACE_S) and leftover staged/retired
    pointers (older than SWAP_GRACE_S). Returns the number of objects deleted. Call it only after ``pointer``
    was installed."""
    s = coord.get_session(session)
    j = _jdir(site)
    now = now or env.utcnow()
    keep = {_set_dir(str(e.get("manifest") or "")) for e in [pointer, *(pointer.get("prev") or [])]
            if _valid_pointer(e)}
    groups: dict[str, list[coord.RemoteObject]] = {}
    for name, obj in _listing(s, site).items():
        parts = name.split("/")
        if parts[0] == VERSIONS and len(parts) >= 3:
            groups.setdefault(f"{VERSIONS}/{parts[1]}", []).append(obj)
        elif parts[0] in (STAGED, RETIRED) and len(parts) == 2:
            groups.setdefault(name, []).append(obj)
        elif len(parts) == 1 and (name == MANIFEST or name.endswith(".parquet")):
            groups.setdefault("", []).append(obj)  # the flat layout of earlier versions
    n = 0
    for key, objs in groups.items():
        if key in keep:
            continue
        grace = SWAP_GRACE_S if key.startswith((STAGED + "/", RETIRED + "/")) else PRUNE_GRACE_S
        if min(_age_s(o, now) for o in objs) < grace:
            continue
        n += _delete_objects(s, objs)
        if key.startswith(VERSIONS + "/"):
            with contextlib.suppress(Exception):
                s.collections.remove(f"{j}/{key}", recurse=False, force=True)
    return n


# ---------------------------------------------------------------- push

def push_site_journal(site: str, *, session: Any | None = None, con: sqlite3.Connection | None = None,
                      force: bool = False) -> dict[str, Any]:
    """Back up the site's journal rows as a new set under ``<IRODS_ROOT>/sites/<SITE>/.mesa-journal/`` and point
    ``CURRENT`` at it (see the module docstring).

    Raises :class:`JournalConflict` (nothing published) when the Data Store holds a newer journal of the site
    than this host has seen or another push replaced the pointer meanwhile, and :class:`ClaimLost` when
    ``coord.holds_site`` is False just before the pointer is written. ``force`` skips both checks.
    ``coord.ClaimUnreadable`` (transient; nothing published) when the site's claim stayed unreadable for the
    whole settle window of ``coord.holds_site`` (a claim merely being renewed is waited out).
    """
    from . import mirror

    site = site.upper()
    con = con or state.connect()
    s = coord.get_session(session)
    j = _jdir(site)
    with env.file_lock(f"journal-push-{site}", blocking=True):
        cur = read_pointer(site, session=s)
        if cur is not None and cur.get("state") == "swapping":
            cur = _settle_swap(s, site, cur)
        if not force and cur is not None and needs_restore(site, con=con, doc=cur):
            raise JournalConflict(f"journal of {site} on the Data Store was pushed by {cur.get('vm')} at "
                                  f"{cur.get('pushedAt')}, after this host's last push/restore: not "
                                  "overwriting it (restore it first)")
        now = env.utcnow()
        push_id = new_push_id(now)
        set_rel = f"{VERSIONS}/{push_id}"
        work = env.WORK_DIR / "journal-push" / site
        shutil.rmtree(work, ignore_errors=True)
        doc: dict[str, Any] = {"format": FORMAT, "site": site, "pushId": push_id, "vm": coord.vm_id(),
                               "host": coord.host(), "pushedAt": now.isoformat(), "tables": {}}
        uploading = False
        try:
            doc["tables"] = export_tables(con, site, work)
            files = [work / f"{t}.parquet" for t, info in doc["tables"].items() if info is not None]
            manifest = work / MANIFEST
            env.write_json(manifest, doc)
            uploading = True
            stats = mirror.push_tree([(p, f"{j}/{set_rel}/{p.name}", None) for p in files], threads=4, session=s,
                                     con=con)
            if stats.get("errors"):
                raise RuntimeError(f"journal push for {site} failed: {stats.get('error_samples')}")
            mstats = mirror.push_tree([(manifest, f"{j}/{set_rel}/{MANIFEST}", None)], threads=1, session=s,
                                      con=con)
            if mstats.get("errors"):
                raise RuntimeError(f"journal manifest push for {site} failed: {mstats.get('error_samples')}")
            pointer = {"format": FORMAT, "site": site, "pushId": push_id, "pushedAt": doc["pushedAt"],
                       "vm": doc["vm"], "host": doc["host"], "manifest": f"{set_rel}/{MANIFEST}",
                       "manifestSha256": _sha256(manifest), "prev": _chain(cur)}
            _publish(s, site, cur, pointer, force=force)
        except BaseException:
            if uploading:
                with contextlib.suppress(Exception):
                    _delete_set(s, site, set_rel)
            raise
        finally:
            shutil.rmtree(work, ignore_errors=True)
        _mark(con, site, "push", doc["pushedAt"], doc["vm"], push_id)
        try:
            pruned = prune(site, pointer, session=s, now=now)
        except Exception as exc:  # noqa: BLE001 - old sets are only clutter
            log.warning("journal of %s: pruning old sets failed: %s", site, exc)
            pruned = 0
    out = {"site": site, "remote": j, "pushId": push_id, "set": f"{j}/{set_rel}", "pushedAt": doc["pushedAt"],
           "tables": {t: (i or {}).get("rows") for t, i in doc["tables"].items() if i is not None},
           "uploaded": stats.get("uploaded", 0) + mstats.get("uploaded", 0),
           "present": stats.get("present", 0) + mstats.get("present", 0), "pruned": pruned,
           "prev": [e.get("pushId") for e in pointer["prev"]]}
    log.info("journal of %s pushed to %s: %s", site, out["set"], out["tables"])
    return out


def local_rows(site: str, *, con: sqlite3.Connection | None = None) -> int:
    """How many journal rows this host holds for ``site`` (0: a VM that never worked on it)."""
    con = con or state.connect()
    n = 0
    for table, key in TABLES.items():
        if _exists(con, table) and key in {c for c, _ in _columns(con, table)}:
            n += con.execute(f"SELECT count(*) FROM {_q(table)} WHERE {_q(key)}=?", (site.upper(),)).fetchone()[0]
    return n


# ---------------------------------------------------------------- restore

def _arrow_decl(typ: Any) -> str:
    import pyarrow as pa

    if pa.types.is_integer(typ) or pa.types.is_boolean(typ):
        return "INTEGER"
    if pa.types.is_floating(typ):
        return "REAL"
    return "TEXT"


def _create_if_missing(con: sqlite3.Connection, table: str, info: dict[str, Any], arrow_schema: Any) -> None:
    ddl = info.get("ddl")
    if ddl:
        con.execute(re.sub(r"^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?", "CREATE TABLE IF NOT EXISTS ", ddl,
                           count=1, flags=re.IGNORECASE))
        return
    cols = ", ".join(f"{_q(f.name)} {_arrow_decl(f.type)}" for f in arrow_schema)
    con.execute(f"CREATE TABLE IF NOT EXISTS {_q(table)} ({cols})")


def _rowid_alias(decls: list[dict[str, Any]]) -> set[str]:
    """The column that is SQLite's rowid (a lone ``INTEGER PRIMARY KEY``), from PRAGMA-style declarations."""
    pks = [d for d in decls if d.get("pk")]
    return {pks[0]["name"]} if len(pks) == 1 and str(pks[0].get("type") or "").upper() == "INTEGER" else set()


def _dropped(con: sqlite3.Connection, table: str, info: dict[str, Any]) -> set[str]:
    local = _column_decls(con, table) if _exists(con, table) else []
    return set(DROP_ON_RESTORE.get(table, ())) | _rowid_alias(local) | _rowid_alias(info.get("columns") or [])


def ensure_local_schema(con: sqlite3.Connection, tables: dict[str, tuple[dict[str, Any], Any]]) -> dict[str, list[str]]:
    """Create every journal table with its owner module's schema, then add the backup's missing columns.

    ``tables``: table -> (manifest entry, Arrow schema of the backup file). Returns {table: [added columns]}.
    Columns are only ever added (``ALTER TABLE ADD COLUMN``; NOT NULL kept only with a default).
    """
    from .avu import bulk
    from .site import chunks, evict
    from .site.fetch import SUPERSEDED_SCHEMA

    chunks.ensure_schema(con)
    evict.ensure_schema(con)
    con.executescript(SUPERSEDED_SCHEMA)
    bulk.ensure_desired(con)
    added: dict[str, list[str]] = {}
    for table, (info, arrow_schema) in tables.items():
        if not _exists(con, table):
            _create_if_missing(con, table, info, arrow_schema)
        have = {c for c, _ in _columns(con, table)}
        decls = {c["name"]: c for c in info.get("columns") or []}
        drop = _dropped(con, table, info)
        for f in arrow_schema:
            if f.name in have or f.name in drop:
                continue
            d = decls.get(f.name) or {}
            if d.get("pk"):
                raise RuntimeError(f"restore {table}: the backup's primary key column {f.name!r} is missing here")
            decl = (d.get("type") or _arrow_decl(f.type)).strip() or "TEXT"
            if d.get("default") is not None:
                decl += f" DEFAULT {d['default']}" + (" NOT NULL" if d.get("notnull") else "")
            try:
                con.execute(f"ALTER TABLE {_q(table)} ADD COLUMN {_q(f.name)} {decl}")
            except sqlite3.OperationalError as exc:  # another process added it first
                if "duplicate column" not in str(exc).lower():
                    raise
            added.setdefault(table, []).append(f.name)
    for table, cols in added.items():
        log.warning("restore: added column(s) %s to %s (present in the backup)", cols, table)
    return added


def _local_file(rel: str | None) -> bool:
    if not rel:
        return False
    p = Path(rel)
    return (p if p.is_absolute() else env.ROOT / p).is_file()


def reconcile_foreign(con: sqlite3.Connection, site: str) -> dict[str, list[str]]:
    """After restoring another VM's journal: re-plan chunks whose raw files are only on that VM's disk, and
    forget finalize steps whose outputs never reached the Data Store. Returns {chunks: [...], steps: [...]}."""
    from .site import chunks

    out: dict[str, list[str]] = {"chunks": [], "steps": []}
    ev = "coalesce(f.local_evicted, 0)" if "local_evicted" in {c for c, _ in _columns(con, "site_files")} else "0"
    uploaded = chunks.RANK["uploaded"]
    for r in con.execute("SELECT chunk_id, product, months, status, done FROM stream_chunks WHERE site=?",
                         (site,)).fetchall():
        done = r["done"] if r["done"] in chunks.RANK else "planned"
        if chunks.RANK[done] >= uploaded or r["status"] == "deleted" or done == "planned" and \
                r["status"] in ("planned", None):
            continue
        try:
            months = list(json.loads(r["months"] or "[]"))
        except ValueError:
            months = []
        if not months:
            continue
        files = con.execute(
            f"SELECT f.local_path FROM site_files f WHERE f.site=? AND f.product=? AND f.month IN"
            f" ({','.join('?' * len(months))}) AND f.dl_status='ok' AND {ev}=0", (site, r["product"], *months)
        ).fetchall()
        if all(_local_file(f["local_path"]) for f in files):
            continue
        con.execute("UPDATE stream_chunks SET status='planned', done='planned', attempt=0, error=?"
                    " WHERE site=? AND chunk_id=?",
                    (f"re-planned on journal restore: raw files of chunk at {done} are not on this host", site,
                     r["chunk_id"]))
        out["chunks"].append(r["chunk_id"])
    if _exists(con, "stream_sites"):
        up = con.execute("SELECT status FROM stream_sites WHERE site=? AND step='upload'", (site,)).fetchone()
        if up is None or up["status"] != "ok":
            steps = [s for (s,) in con.execute(
                f"SELECT step FROM stream_sites WHERE site=? AND step IN ({','.join('?' * len(PRE_UPLOAD_STEPS))})",
                (site, *PRE_UPLOAD_STEPS))]
            if steps:
                con.execute(f"DELETE FROM stream_sites WHERE site=? AND step IN ({','.join('?' * len(steps))})",
                            (site, *steps))
                out["steps"] = sorted(steps)
    if out["chunks"] or out["steps"]:
        log.warning("restore %s: re-planned chunk(s) %s and forgot finalize step(s) %s (their files were on the "
                    "other VM)", site, out["chunks"], out["steps"])
    return out


def release_torn_batches(con: sqlite3.Connection, scope: str) -> dict[str, int]:
    """Repair ``applied`` rows of ``scope`` whose ``batch_id`` names a batch that will never take them.

    An export that was not one snapshot (before export_tables) could pair ``avu_journal`` rows still
    ``applied`` with their batch already ``spooled`` (or ``abandoned``, or not in the backup at all).
    ``history.plan_batches`` only takes rows with no batch and ``_finish_spooling`` only completes
    ``spooling`` batches, so such rows were never spooled and ``verify_chunk`` stayed pending for good. Their
    ``batch_id`` is set to NULL (they are spooled again; the recorder's content dedup keeps MESA history
    single). Rows of a batch in flight (IN_FLIGHT_BATCHES) are left to the spool resume or ``recover``; rows of
    a ``recorded`` batch (direct mode) become ``recorded`` with it. Returns {released, recorded}."""
    if not (_exists(con, "avu_journal") and _exists(con, "batches")):
        return {"released": 0, "recorded": 0}
    recorded = con.execute(
        "UPDATE avu_journal SET status='recorded', snapshot_id=(SELECT b.snapshot_id FROM batches b WHERE"
        " b.batch_id=avu_journal.batch_id) WHERE scope=? AND status='applied' AND batch_id IN"
        " (SELECT batch_id FROM batches WHERE status='recorded')", (scope,)).rowcount
    held = ", ".join("?" * len(IN_FLIGHT_BATCHES))
    released = con.execute(
        "UPDATE avu_journal SET batch_id=NULL WHERE scope=? AND status='applied' AND batch_id IS NOT NULL AND"
        f" batch_id NOT IN (SELECT batch_id FROM batches WHERE status IN ({held}))",
        (scope, *IN_FLIGHT_BATCHES)).rowcount
    if released or recorded:
        log.warning("restore %s: %d applied row(s) released from finished/unknown batches (spooled again), %d "
                    "marked recorded with their batch", scope, released, recorded)
    return {"released": released, "recorded": recorded}


def _fetch_checked(s: Any, remote: str, local: Path) -> None:
    """coord.fetch, with a missing or mismatching object reported as :class:`JournalSetBroken`."""
    try:
        coord.fetch(s, remote, local)
    except FileNotFoundError as exc:
        raise JournalSetBroken(f"{remote} is missing") from exc
    except coord.TransferMismatch as exc:
        raise JournalSetBroken(str(exc)) from exc


def fetch_set(site: str, entry: dict[str, Any], work: Path, *, session: Any | None = None
              ) -> tuple[dict[str, Any], dict[str, Path]]:
    """Download one set (a pointer or a ``prev`` entry) into ``work``: (its manifest, {table: local Parquet}).

    Every file is checked against the manifest (sha256) and the manifest against the pointer; a failure raises
    :class:`JournalSetBroken`. Transient iRODS errors propagate unchanged."""
    s = coord.get_session(session)
    j = _jdir(site)
    mrel = str(entry.get("manifest") or "")
    setdir = posixpath.dirname(f"{j}/{mrel}")
    local_m = work / MANIFEST
    _fetch_checked(s, f"{j}/{mrel}", local_m)
    want = entry.get("manifestSha256")
    if want and _sha256(local_m) != want:
        raise JournalSetBroken(f"{j}/{mrel} does not match its pointer (sha256)")
    try:
        doc = json.loads(local_m.read_text("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise JournalSetBroken(f"{j}/{mrel} is not a readable manifest: {exc}") from exc
    if not isinstance(doc, dict) or not isinstance(doc.get("tables"), dict):
        raise JournalSetBroken(f"{j}/{mrel} is not a journal manifest")
    files: dict[str, Path] = {}
    for table, info in doc["tables"].items():
        if table not in TABLES or not info:
            continue
        local = work / f"{table}.parquet"
        _fetch_checked(s, f"{setdir}/{table}.parquet", local)
        if _sha256(local) != info.get("sha256"):
            raise JournalSetBroken(f"{setdir}/{table}.parquet does not match its manifest (sha256)")
        files[table] = local
    return doc, files


def restore_detail(site: str, *, session: Any | None = None, con: sqlite3.Connection | None = None
                   ) -> dict[str, Any]:
    """Replace the local journal rows of ``site`` with the Data Store backup (see module docstring).

    Uses the pointer's set, else the newest set of its chain that passes its checksums (``fallback`` names
    it, ``skipped`` lists why newer ones were not used). Raises :class:`JournalSetBroken` when no set of the
    chain can be restored; the local journal is then untouched."""
    import pyarrow.parquet as pq

    site = site.upper()
    con = con or state.connect()
    s = coord.get_session(session)
    remote = _jdir(site)
    ptr = read_pointer(site, session=s)
    if ptr is None:
        return {"site": site, "restored": False, "reason": f"no journal backup at {remote}"}
    work = env.WORK_DIR / "journal-restore" / site
    shutil.rmtree(work, ignore_errors=True)
    counts: dict[str, int] = {}
    fixed: dict[str, Any] = {}
    skipped: list[dict[str, Any]] = []
    try:
        chain = [ptr, *(e for e in ptr.get("prev") or [] if _valid_pointer(e))]
        for i, entry in enumerate(chain):
            try:
                doc, tables = fetch_set(site, entry, work / f"set-{i}", session=s)
            except JournalSetBroken as exc:
                log.error("journal of %s: set %s (pushed by %s at %s) is broken: %s", site, entry.get("pushId"),
                          entry.get("vm"), entry.get("pushedAt"), exc)
                skipped.append({"pushId": entry.get("pushId"), "error": str(exc)[:300]})
                continue
            used = entry
            break
        else:
            raise JournalSetBroken(f"no journal set of {site} can be restored: "
                                   + "; ".join(f"{e['pushId']}: {e['error']}" for e in skipped))
        files = {t: pq.ParquetFile(p) for t, p in tables.items()}
        added = ensure_local_schema(con, {t: (doc["tables"][t], f.schema_arrow) for t, f in files.items()})
        _ensure_marks(con)
        con.execute("BEGIN IMMEDIATE")
        try:
            for table, pf in files.items():
                drop = _dropped(con, table, doc["tables"][table])
                cols = [c for c in pf.schema_arrow.names if c not in drop]
                con.execute(f"DELETE FROM {_q(table)} WHERE {_q(TABLES[table])}=?", (site,))
                sql = f"INSERT INTO {_q(table)} ({', '.join(_q(c) for c in cols)}) VALUES ({', '.join('?' * len(cols))})"
                n = 0
                for batch in pf.iter_batches(batch_size=FETCH_ROWS, columns=cols):
                    rows = batch.to_pylist()
                    con.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
                    n += len(rows)
                expected = (doc["tables"][table] or {}).get("rows")
                if expected is not None and n != expected:
                    raise RuntimeError(f"restore {site}.{table}: {n} rows != manifest {expected}")
                counts[table] = n
            fixed["batches"] = release_torn_batches(con, site)
            if doc.get("vm") != coord.vm_id() or doc.get("host") not in (None, coord.host()):
                fixed.update(reconcile_foreign(con, site))
            # the pointer seen (not the fallback set): this host may now push over a broken newest set
            _mark(con, site, "restore", ptr.get("pushedAt"), ptr.get("vm"), ptr.get("pushId"))
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        shutil.rmtree(work, ignore_errors=True)
    log.warning("journal of %s restored from %s (set %s pushed by %s at %s): %s", site, remote, used.get("pushId"),
                doc.get("vm"), doc.get("pushedAt"), counts)
    out = {"site": site, "restored": True, "tables": counts, "from_vm": doc.get("vm"), "pushId": used.get("pushId"),
           "pushedAt": doc.get("pushedAt"), "addedColumns": added, **fixed}
    if used is not ptr:
        out["fallback"] = used.get("pushId")
        out["skipped"] = skipped
    return out


def restore_site_journal(site: str, *, force: bool = False, session: Any | None = None,
                         con: sqlite3.Connection | None = None) -> bool:
    """Restore the site's journal from the Data Store when :func:`needs_restore` (always with ``force``).

    Returns True when the local rows were replaced. Errors (no restorable set, a failed download) propagate
    and leave the local journal untouched.
    """
    con = con or state.connect()
    if not force and not needs_restore(site, session=session, con=con):
        return False
    return bool(restore_detail(site, session=session, con=con).get("restored"))


def main(action: str, site: str, *, session: Any | None = None, force: bool = False) -> int:
    """``neon-ducklake journal push|restore SITE [--force]`` (operator use; the worker calls both itself)."""
    try:
        if action == "push":
            out = push_site_journal(site, session=session, force=force)
        elif action == "restore":
            if not force and not needs_restore(site, session=session):
                out = {"site": site.upper(), "restored": False, "reason": "the local journal is current"}
            else:
                out = restore_detail(site, session=session)
        else:
            raise SystemExit(f"unknown journal action {action!r} (push or restore)")
    except (JournalConflict, JournalSetBroken) as exc:
        print(json.dumps({"site": site.upper(), "error": f"{type(exc).__name__}: {exc}"}, indent=1))
        return 1
    print(json.dumps(out, indent=1, default=str))
    return 0 if out.get("restored", True) or out.get("reason") == "the local journal is current" else 1
