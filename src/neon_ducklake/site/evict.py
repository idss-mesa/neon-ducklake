"""`site evict <SITE>` — free local disk: delete raw files that are safely in iRODS (docs/SCALING.md).

A journal row's file ``sites/<SITE>/raw/<DP>/<YYYY-MM>/<name>`` is deleted only when ALL of these hold:

1. ``site_files.dl_status = 'ok'`` and the file is not superseded (``fetch_superseded``);
2. ``up_status IN ('uploaded', 'present')`` and ``irods_checksum`` equals ``dl_md5`` (plain MD5; an
   ``md5:`` prefix and case are ignored);
3. its core AVUs are recorded in MESA history: the journal's latest ``neon.productCode`` row for the
   object's iRODS path is an ``add`` with status ``recorded`` (no later pending, failed or recorded remove);
4. the site's last verify report (``sites/<SITE>/reports/verify.json``) ran against iRODS, has no ``fail``
   check, covers the file (its product/month filters) and is newer than the file's upload;
5. the local file is a regular file (not a symlink) inside ``sites/<SITE>/raw/`` whose size is ``dl_bytes``.

``KEEP_KINDS`` always stay local: ``variables`` files are tiny (~0.5 % of the bytes) and are re-read
from disk by ``site build-avus`` (``neon.variableCount``) and by every re-stack (column types).
Nothing else is ever touched: lakes, cards, curation, reports, ref, tmp, superseded or unjournaled files.

Journal: ``site_files.local_evicted = 1`` plus one ``evict_log`` row per file are committed BEFORE the
unlink, so a crash leaves at worst a flagged file still on disk; the next run resets the flag of any
flagged file found on disk (re-downloaded or never deleted) and evaluates it again.

Helpers used by the orchestrator (``run.py``) keep re-stacks safe: stack fingerprints each lake unit over
the files it finds on disk, so a unit whose inputs are partly evicted would be rebuilt from a subset.
:func:`restore_candidates` names the products that must be restored first and :func:`mark_restore`
clears their flags so the download stage fetches them again.

MESA history rows count as recorded when their journal status is ``recorded`` (direct) or ``spooled``
(written to the Data Store spool for the central recorder, docs/STREAMING.md § History backends).

Streaming (docs/STREAMING.md):

- :func:`verify_chunk_objects` checks one chunk against iRODS: every downloaded file has a good replica
  (status 1, the local size, checksum == ``dl_md5``) and its desired core AVUs present, and the chunk's
  AVU journal rows are recorded or spooled (none intent/applied, no unresolved error);
- :func:`delete_chunk_files` deletes a verified chunk's raw files (every kind; stream stacking reads
  variables from the lake) with the same journal-first protocol and per-file re-checks as ``site evict``,
  plus the downloaded files of the chunk's months that a later listing superseded
  (:func:`delete_superseded`: their rows left the lake when the chunk was stacked; the journal row becomes
  ``dl_status='superseded'`` BEFORE the unlink, so a file that is listed again is simply downloaded again);
- :func:`cleanup_candidates` / :func:`verify_copies` / :func:`remove_files` delete a finished site's local
  lake ``data/**``, cards and curation after checking each file's Data Store copy (good replica, size and
  checksum == local md5). Reports, the lake catalog file and the journal stay.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .. import env, state
from . import site_lake

log = logging.getLogger("neon_ducklake.site.evict")

KEEP_KINDS = frozenset({"variables"})
UPLOADED = ("uploaded", "present")
RECORDED = ("recorded", "spooled")  # MESA history written directly, or spooled for the central recorder
PENDING = ("intent", "applied")  # AVU journal rows not yet in MESA history
REQUIRED_RECORDED = ("neon.productCode",)
MAX_SAMPLES = 20

EVICT_SCHEMA = """
CREATE TABLE IF NOT EXISTS evict_log (
  site TEXT NOT NULL, local_path TEXT NOT NULL, bytes INTEGER, evicted_at TEXT
);
CREATE INDEX IF NOT EXISTS evict_log_site ON evict_log(site);
"""


# ---------------------------------------------------------------- schema

def has_evict_column(con: sqlite3.Connection) -> bool:
    return any(r[1] == "local_evicted" for r in con.execute("PRAGMA table_info(site_files)"))


def ensure_schema(con: sqlite3.Connection) -> None:
    """``evict_log`` + the ``site_files.local_evicted`` column (added once; safe under concurrency)."""
    from .fetch import SUPERSEDED_SCHEMA

    con.executescript(SUPERSEDED_SCHEMA + EVICT_SCHEMA)
    if not has_evict_column(con):
        try:
            con.execute("ALTER TABLE site_files ADD COLUMN local_evicted INTEGER DEFAULT 0")
        except sqlite3.OperationalError as exc:  # another process added it first
            if "duplicate column" not in str(exc).lower():
                raise


def _ev(con: sqlite3.Connection) -> str:
    return "coalesce(f.local_evicted, 0)" if has_evict_column(con) else "0"


def norm_md5(value: str | None) -> str:
    return (value or "").strip().lower().removeprefix("md5:")


# ---------------------------------------------------------------- journal views

def site_rows(con: sqlite3.Connection, site: str) -> list[dict[str, Any]]:
    """Every non-superseded journal row of the site with ``evicted`` (flag) and ``superseded`` = False."""
    from .fetch import SUPERSEDED_SCHEMA

    con.executescript(SUPERSEDED_SCHEMA)
    rows = con.execute(
        f"SELECT f.product, f.month, f.name, f.kind, f.size, f.local_path, f.dl_status, f.dl_md5, f.dl_bytes,"
        f" f.irods_path, f.up_status, f.irods_checksum, f.uploaded_at, {_ev(con)} AS evicted"
        " FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND"
        " s.month=f.month AND s.name=f.name WHERE f.site=? AND s.name IS NULL"
        " AND coalesce(f.dl_status, '') != 'superseded' ORDER BY f.product, f.month, f.name", (site,)).fetchall()
    return [dict(r) for r in rows]


def _local(r: dict[str, Any]) -> Path:
    return env.ROOT / (r.get("local_path") or "")


def _on_disk(r: dict[str, Any]) -> bool:
    return bool(r.get("local_path")) and _local(r).is_file()


def input_state(con: sqlite3.Connection, site: str) -> dict[str, int]:
    """Downloaded (ok) inputs of the site: on disk vs missing (evicted or deleted)."""
    out = Counter()
    for r in site_rows(con, site.upper()):
        if r["dl_status"] != "ok":
            out["notDownloaded"] += 1
        elif _on_disk(r):
            out["local"] += 1
        else:
            out["missing"] += 1
            out["evicted"] += int(bool(r["evicted"]))
    return {k: int(out[k]) for k in ("local", "missing", "evicted", "notDownloaded")}


def restore_candidates(con: sqlite3.Connection, site: str) -> dict[str, int]:
    """product -> missing input files, for products whose next stack would see a partial input set.

    A product qualifies when some downloaded inputs are missing on disk (evicted) AND it also has files on
    disk (other than ``KEEP_KINDS``) or files still to download (new, stale or failed): stack would then
    rebuild a unit from a subset. Fully evicted, unchanged products are skipped by stack and stay evicted.
    """
    missing: Counter = Counter()
    other: Counter = Counter()
    for r in site_rows(con, site.upper()):
        p = r["product"]
        if r["dl_status"] != "ok":
            other[p] += 1
        elif not _on_disk(r):
            missing[p] += 1
        elif r["kind"] not in KEEP_KINDS:
            other[p] += 1
    return {p: n for p, n in sorted(missing.items()) if other[p]}


def mark_restore(con: sqlite3.Connection, site: str, products: list[str] | None = None) -> tuple[int, int]:
    """Clear ``local_evicted`` (all products, or ``products``) so the download stage fetches them again.

    Returns (files, bytes) cleared.
    """
    ensure_schema(con)
    where, args = "site=? AND coalesce(local_evicted, 0) = 1", [site.upper()]
    if products is not None:
        if not products:
            return 0, 0
        where += f" AND product IN ({','.join('?' * len(products))})"
        args += list(products)
    row = con.execute(f"SELECT count(*), coalesce(sum(coalesce(dl_bytes, size, 0)), 0) FROM site_files WHERE {where}",
                      args).fetchone()
    con.execute(f"UPDATE site_files SET local_evicted=0 WHERE {where}", args)
    return int(row[0]), int(row[1])


def pending_bytes(con: sqlite3.Connection, site: str) -> int:
    """Bytes the next download would write: files not yet ok + ok files missing on disk (evicted included)."""
    total = 0
    for r in site_rows(con, site.upper()):
        if r["dl_status"] != "ok":
            total += int(r["size"] or 0)
        elif not _on_disk(r):
            total += int(r["dl_bytes"] or r["size"] or 0)
    return total


def evicted_totals(con: sqlite3.Connection) -> dict[str, dict[str, int]]:
    """site -> {'files', 'bytes'} currently flagged evicted."""
    if not has_evict_column(con):
        return {}
    rows = con.execute("SELECT site, count(*), coalesce(sum(coalesce(dl_bytes, size, 0)), 0) FROM site_files "
                       "WHERE coalesce(local_evicted, 0) = 1 GROUP BY site").fetchall()
    return {r[0]: {"files": int(r[1]), "bytes": int(r[2])} for r in rows}


def recorded_paths(con: sqlite3.Connection, site: str) -> set[str]:
    """iRODS paths whose REQUIRED_RECORDED attributes are all recorded (or spooled) in MESA history (not removed)."""
    need = set(REQUIRED_RECORDED)
    have: dict[str, set[str]] = defaultdict(set)
    rows = con.execute(
        "SELECT a.path, a.attribute FROM avu_journal a WHERE a.scope = ? AND a.op = 'add'"
        " AND a.status IN ('recorded', 'spooled')"
        f" AND a.attribute IN ({','.join('?' * len(need))})"
        " AND NOT EXISTS (SELECT 1 FROM avu_journal r WHERE r.path = a.path AND r.attribute = a.attribute"
        "   AND r.id > a.id AND r.status IN ('intent', 'applied', 'recorded', 'spooled', 'error')"
        "   AND NOT (r.op = 'add' AND r.status IN ('recorded', 'spooled')))",
        (site, *sorted(need))).fetchall()
    for path, attr in rows:
        have[path].add(attr)
    return {p for p, attrs in have.items() if need <= attrs}


# ---------------------------------------------------------------- verify gate

def _ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def verify_gate(site: str) -> tuple[dict[str, Any] | None, str | None]:
    """(report, None) when the last verify report allows eviction, else (report-or-None, reason)."""
    path = site_lake.site_paths(site).reports_dir / "verify.json"
    if not path.exists():
        return None, f"no verify report ({path.relative_to(env.ROOT) if path.is_relative_to(env.ROOT) else path})"
    try:
        report = json.loads(path.read_text("utf-8"))
    except (OSError, ValueError) as exc:
        return None, f"verify report unreadable: {type(exc).__name__}"
    if str(report.get("site", "")).upper() != site:
        return report, f"verify report is for site {report.get('site')!r}"
    fails = [c.get("name") for c in report.get("checks") or [] if c.get("status") == "fail"]
    if fails or report.get("ok") is not True or (report.get("summary") or {}).get("fail"):
        return report, "last verify has failures: " + (", ".join(str(f) for f in fails) or "ok != true")
    if report.get("irods") is not True:
        return report, "last verify did not check iRODS"
    if _ts(report.get("generatedAt")) is None:
        return report, "verify report has no generatedAt"
    return report, None


# ---------------------------------------------------------------- evaluation

def evaluate(con: sqlite3.Connection, site: str, report: dict[str, Any]) -> dict[str, Any]:
    """Split the site's journal rows into evict / kept / blocked (with reasons) / already evicted."""
    site = site.upper()
    paths = site_lake.site_paths(site)
    raw = paths.raw_dir.resolve()
    filters = report.get("filters") or {}
    fp, fm = set(filters.get("products") or []), set(filters.get("months") or [])
    verified_at = _ts(report.get("generatedAt"))
    recorded = recorded_paths(con, site)
    out: dict[str, Any] = {"evict": [], "kept": [], "blocked": defaultdict(list), "already": [], "missing": [],
                           "reflagged": [], "notDownloaded": 0}
    for r in site_rows(con, site):
        if r["dl_status"] != "ok":
            out["notDownloaded"] += 1
            continue
        local = _local(r)
        present = _on_disk(r)
        if r["evicted"]:
            if not present:
                out["already"].append(r)
                continue
            out["reflagged"].append(r)  # flagged but on disk: re-downloaded, or a crash before the unlink
        elif not present:
            out["missing"].append(r)
            continue
        if r["kind"] in KEEP_KINDS:
            out["kept"].append(r)
            continue
        irods = r["irods_path"] or paths.irods_raw(r["product"], r["month"], r["name"])
        why = None
        if (fp and r["product"] not in fp) or (fm and r["month"] not in fm):
            why = "outside the verify report's filters"
        elif r["up_status"] not in UPLOADED:
            why = "not uploaded"
        elif not r["dl_md5"] or norm_md5(r["irods_checksum"]) != norm_md5(r["dl_md5"]):
            why = "iRODS checksum != local md5"
        elif irods not in recorded:
            why = "core AVUs not recorded in MESA history"
        elif (up := _ts(r["uploaded_at"])) is not None and verified_at is not None and up > verified_at:
            why = "uploaded after the last verify"
        else:
            try:
                resolved = local.resolve()
                st = local.lstat()
            except OSError:
                resolved, st = None, None
            if local.is_symlink() or resolved is None or not resolved.is_relative_to(raw):
                why = "local path is not a regular file under sites/<SITE>/raw"
            elif st is None or st.st_size != int(r["dl_bytes"] if r["dl_bytes"] is not None else -1):
                why = "local size != journal dl_bytes"
        if why:
            out["blocked"][why].append(r)
        else:
            out["evict"].append(r)
    out["blocked"] = dict(out["blocked"])
    return out


def _gb(n: int) -> str:
    return f"{n / 1e9:.2f} GB"


def _prune_empty_dirs(start: Path, stop: Path) -> None:
    """rmdir now-empty month/product directories up to (not including) ``stop``."""
    d = start
    while d != stop and d.is_relative_to(stop):
        try:
            d.rmdir()
        except OSError:
            return
        d = d.parent


def main(site: str, dry_run: bool = False, *, con: sqlite3.Connection | None = None) -> int:
    """Evict the site's verified raw files; 0 = done, 1 = verify gate closed, 3 = some files blocked."""
    site = site.upper()
    paths = site_lake.site_paths(site)
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        report, why = verify_gate(site)
        if why:
            print(f"evict {site}: nothing deleted: {why}")
            return 1
        assert report is not None
        if dry_run:
            return _run(con, site, paths, report, dry_run=True)
        with env.file_lock(f"site-fetch-{site}"), env.file_lock(f"site-lake-{site}", blocking=True):
            ensure_schema(con)
            return _run(con, site, paths, report, dry_run=False)
    finally:
        if own:
            con.close()


def _evict_file(con: sqlite3.Connection, site: str, r: dict[str, Any], raw: Path, *, unlink: bool = True
                ) -> tuple[int | None, str | None]:
    """Flag + log one journal row, then unlink its file (journal first: a crash leaves a flagged file on disk).

    Returns (bytes freed, None) or (None, error). ``unlink=False`` only flags a file that is already gone.
    """
    local = _local(r)
    size = 0
    if unlink:
        try:
            size = local.lstat().st_size
        except OSError as exc:
            return None, f"{r['local_path']}: {type(exc).__name__}"
    now = env.utcnow().isoformat()
    con.execute("BEGIN")
    try:
        con.execute("UPDATE site_files SET local_evicted=1 WHERE site=? AND product=? AND month=? AND name=?",
                    (site, r["product"], r["month"], r["name"]))
        con.execute("INSERT INTO evict_log(site, local_path, bytes, evicted_at) VALUES (?,?,?,?)",
                    (site, r["local_path"], size, now))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    if not unlink:
        return 0, None
    try:
        os.unlink(local)
    except OSError as exc:
        con.execute("UPDATE site_files SET local_evicted=0 WHERE site=? AND product=? AND month=? AND name=?",
                    (site, r["product"], r["month"], r["name"]))
        return None, f"{r['local_path']}: {type(exc).__name__}"
    _prune_empty_dirs(local.parent, raw)
    return size, None


def _run(con: sqlite3.Connection, site: str, paths: site_lake.SitePaths, report: dict[str, Any], *,
         dry_run: bool) -> int:
    plan = evaluate(con, site, report)
    if plan["reflagged"] and not dry_run:
        con.executemany("UPDATE site_files SET local_evicted=0 WHERE site=? AND product=? AND month=? AND name=?",
                        [(site, r["product"], r["month"], r["name"]) for r in plan["reflagged"]])
    freed = n = 0
    errors: list[str] = []
    raw = paths.raw_dir
    for r in plan["evict"]:
        if dry_run:
            try:
                freed += _local(r).lstat().st_size
                n += 1
            except OSError as exc:
                errors.append(f"{r['local_path']}: {type(exc).__name__}")
            continue
        size, err = _evict_file(con, site, r, raw)
        if err:
            errors.append(err)
            continue
        freed += size or 0
        n += 1
    blocked = plan["blocked"]
    n_blocked = sum(len(v) for v in blocked.values())
    kept_bytes = sum(int(r["dl_bytes"] or 0) for r in plan["kept"])
    verb = "would free" if dry_run else "freed"
    print(f"evict {site}{' (dry run)' if dry_run else ''}: verify {report.get('generatedAt')} ok")
    print(f"  evicted {n} file(s), {verb} {_gb(freed)}")
    print(f"  kept on disk: {len(plan['kept'])} {'/'.join(sorted(KEEP_KINDS))} file(s) ({_gb(kept_bytes)}); "
          f"already evicted {len(plan['already'])}; not downloaded {plan['notDownloaded']}; "
          f"missing on disk (never evicted) {len(plan['missing'])}")
    for reason, rows in sorted(blocked.items()):
        print(f"  blocked ({reason}): {len(rows)} file(s), {_gb(sum(int(r['dl_bytes'] or 0) for r in rows))}")
    for e in errors[:MAX_SAMPLES]:
        print(f"  unlink failed: {e}")
    summary = {"site": site, "dryRun": dry_run, "at": env.utcnow().isoformat(),
               "verifyGeneratedAt": report.get("generatedAt"), "evicted": n, "bytesFreed": freed,
               "kept": len(plan["kept"]), "keptBytes": kept_bytes, "alreadyEvicted": len(plan["already"]),
               "reflagged": len(plan["reflagged"]), "missingNotEvicted": len(plan["missing"]),
               "notDownloaded": plan["notDownloaded"],
               "blocked": {k: {"files": len(v), "sample": [r["local_path"] for r in v[:MAX_SAMPLES]]}
                           for k, v in sorted(blocked.items())},
               "unlinkErrors": errors[:MAX_SAMPLES]}
    if not dry_run:
        env.write_json(env.ROOT / "logs" / f"evict_summary_{site}.json", summary)
    log.info("evict %s: %d file(s) %s %s; blocked %d", site, n, verb, _gb(freed), n_blocked)
    return 3 if (n_blocked or errors) else 0


# ---------------------------------------------------------------- streaming: one chunk

def chunk_rows(con: sqlite3.Connection, site: str, product: str, months: list[str]) -> list[dict[str, Any]]:
    """The chunk's non-superseded journal rows (``site_rows`` of one product's months)."""
    ms = set(months)
    return [r for r in site_rows(con, site.upper()) if r["product"] == product and r["month"] in ms]


def _irods_path(paths: site_lake.SitePaths, r: dict[str, Any]) -> str:
    return r.get("irods_path") or paths.irods_raw(r["product"], r["month"], r["name"])


def _batched(values: list[str], n: int = 500) -> list[list[str]]:
    return [values[i:i + n] for i in range(0, len(values), n)]


def journal_state(con: sqlite3.Connection, site: str, paths: list[str]) -> dict[str, list[str]]:
    """AVU journal rows of ``paths`` that are not in MESA history yet: {'pending': [...], 'errors': [...]}.

    ``pending``: intent/applied rows (applied but neither recorded nor spooled). ``errors``: an ``error`` row with
    no later applied/recorded/spooled/noop row for the same (path, attribute, value, unit).
    """
    pending: list[str] = []
    errors: list[str] = []
    for part in _batched(sorted(set(paths))):
        qs = ",".join("?" * len(part))
        for r in con.execute(f"SELECT path, attribute, status FROM avu_journal WHERE scope=? AND path IN ({qs})"
                             f" AND status IN ('intent', 'applied')", (site, *part)):
            pending.append(f"{r[0]}: {r[1]} ({r[2]})")
        for r in con.execute(
                f"SELECT e.path, e.attribute FROM avu_journal e WHERE e.scope=? AND e.path IN ({qs}) AND"
                " e.status='error' AND NOT EXISTS (SELECT 1 FROM avu_journal x WHERE x.path=e.path AND"
                " x.attribute=e.attribute AND x.value=e.value AND x.unit=e.unit AND x.id > e.id AND"
                " x.status IN ('applied', 'recorded', 'spooled', 'noop'))", (site, *part)):
            errors.append(f"{r[0]}: {r[1]}")
    return {"pending": pending, "errors": errors}


def verify_chunk_objects(con: sqlite3.Connection, site: str, product: str, months: list[str], *,
                         session: Any) -> dict[str, Any]:
    """Check one chunk against iRODS and the AVU journal (read-only; see the module docstring)."""
    from ..avu import bulk
    from ..mirror import norm_checksum, remote_replicas
    from .avus import CORE_ATTRS

    site = site.upper()
    paths = site_lake.site_paths(site)
    rows = chunk_rows(con, site, product, months)
    fails: dict[str, list[str]] = defaultdict(list)
    ok_rows = [r for r in rows if r["dl_status"] == "ok"]
    for r in rows:
        if r["dl_status"] != "ok":
            fails["not downloaded"].append(r["name"])
    by_coll: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in ok_rows:
        by_coll[_irods_path(paths, r).rsplit("/", 1)[0]].append(r)
    bulk.ensure_desired(con)
    core = sorted(CORE_ATTRS)
    good_objects: list[str] = []
    for coll, members in sorted(by_coll.items()):
        want: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        for p, a, v, u in con.execute(
                f"SELECT path, attribute, value, unit FROM avu_desired WHERE scope=? AND unit_id=? AND"
                f" target_type='data_object' AND attribute IN ({','.join('?' * len(core))})", (site, coll, *core)):
            want[p].add((a, v, u or ""))
        reps = remote_replicas(session, coll)
        current = bulk.query_object_avus(session, coll)
        for r in members:
            ip, name = _irods_path(paths, r), r["name"]
            rr = reps.get(name, [])
            md5 = norm_md5(r["dl_md5"])
            if not rr:
                fails["not in iRODS"].append(name)
                continue
            if not md5 or not any(x.status == "1" and x.size == int(r["dl_bytes"] or -1)
                                  and norm_checksum(x.checksum) == md5 for x in rr):
                fails["no good replica with the local size and md5"].append(name)
                continue
            if not any(a == "neon.productCode" for a, _v, _u in want.get(ip, set())):
                fails["no desired core AVUs (build the chunk's AVUs)"].append(name)
                continue
            lack = want[ip] - current.get(ip, set())
            if lack:
                fails["core AVUs missing in iRODS"].append(f"{name}: {', '.join(sorted(a for a, _v, _u in lack))}")
                continue
            good_objects.append(ip)
    recorded = recorded_paths(con, site)
    for ip in good_objects:
        if ip not in recorded:
            fails["core AVUs not recorded or spooled in MESA history"].append(ip.rsplit("/", 1)[1])
    colls = sorted({f"{env.IRODS_ROOT}/sites/{site}/raw/{product}/{m}" for m in months})
    js = journal_state(con, site, [_irods_path(paths, r) for r in ok_rows] + colls)
    if js["pending"]:
        fails["AVU journal rows not recorded or spooled"] += js["pending"]
    if js["errors"]:
        fails["AVU journal errors"] += js["errors"]
    return {"ok": not fails, "files": len(rows), "checked": len(ok_rows), "good": len(good_objects),
            "failures": {k: len(v) for k, v in sorted(fails.items())},
            "samples": {k: v[:MAX_SAMPLES] for k, v in sorted(fails.items())}}


def delete_chunk_files(con: sqlite3.Connection, site: str, product: str, months: list[str], *,
                       verified_at: str | None) -> dict[str, Any]:
    """Delete a verified chunk's raw files; every file is re-checked against the journal first.

    A file is deleted only when it is a regular file under ``sites/<SITE>/raw`` with the journal size, was
    uploaded with ``irods_checksum == dl_md5`` no later than ``verified_at`` and its core AVUs are recorded or
    spooled. A file already gone locally that passes the same checks is only flagged. Returns counts.
    """
    site = site.upper()
    ensure_schema(con)
    paths = site_lake.site_paths(site)
    raw = paths.raw_dir.resolve()
    verified = _ts(verified_at)
    recorded = recorded_paths(con, site)
    out: dict[str, Any] = {"files": 0, "deleted": 0, "bytes": 0, "already": 0, "flagged": 0, "errors": []}
    blocked: dict[str, list[str]] = defaultdict(list)
    for r in chunk_rows(con, site, product, months):
        out["files"] += 1
        if r["dl_status"] != "ok":
            blocked["not downloaded"].append(r["name"])
            continue
        present = _on_disk(r)
        if r["evicted"] and not present:
            out["already"] += 1
            continue
        why = None
        if r["up_status"] not in UPLOADED:
            why = "not uploaded"
        elif not r["dl_md5"] or norm_md5(r["irods_checksum"]) != norm_md5(r["dl_md5"]):
            why = "iRODS checksum != local md5"
        elif _irods_path(paths, r) not in recorded:
            why = "core AVUs not recorded or spooled in MESA history"
        elif verified is None or ((up := _ts(r["uploaded_at"])) is not None and up > verified):
            why = "uploaded after the chunk was verified"
        elif present:
            local = _local(r)
            try:
                resolved, st = local.resolve(), local.lstat()
            except OSError:
                resolved, st = None, None
            if local.is_symlink() or resolved is None or not resolved.is_relative_to(raw):
                why = "local path is not a regular file under sites/<SITE>/raw"
            elif st is None or st.st_size != int(r["dl_bytes"] if r["dl_bytes"] is not None else -1):
                why = "local size != journal dl_bytes"
        if why:
            blocked[why].append(r["name"])
            continue
        size, err = _evict_file(con, site, r, paths.raw_dir, unlink=present)
        if err:
            out["errors"].append(err)
        elif present:
            out["deleted"] += 1
            out["bytes"] += size or 0
        else:
            out["flagged"] += 1
    sup = delete_superseded(con, site, product, months)
    out["superseded"], out["supersededBytes"] = sup["deleted"], sup["bytes"]
    out["bytes"] += sup["bytes"]
    out["errors"] += sup["errors"]
    out["blocked"] = {k: len(v) for k, v in sorted(blocked.items())}
    out["blockedSamples"] = {k: v[:MAX_SAMPLES] for k, v in sorted(blocked.items())}
    return out


def superseded_rows(con: sqlite3.Connection, site: str, product: str | None = None,
                    months: list[str] | None = None) -> list[dict[str, Any]]:
    """Journal rows of superseded files (``fetch_superseded``) that are still ``dl_status='ok'`` (downloaded)."""
    from .fetch import SUPERSEDED_SCHEMA

    con.executescript(SUPERSEDED_SCHEMA)
    where, args = ["f.site=?", "f.dl_status='ok'"], [site.upper()]
    if product:
        where.append("f.product=?")
        args.append(product)
    if months:
        where.append(f"f.month IN ({','.join('?' * len(months))})")
        args += list(months)
    rows = con.execute(
        "SELECT f.product, f.month, f.name, f.local_path, f.dl_bytes FROM site_files f JOIN fetch_superseded s ON "
        "s.site=f.site AND s.product=f.product AND s.month=f.month AND s.name=f.name WHERE "
        f"{' AND '.join(where)} ORDER BY f.product, f.month, f.name", args).fetchall()
    return [dict(r) for r in rows]


def delete_superseded(con: sqlite3.Connection, site: str, product: str | None = None,
                      months: list[str] | None = None) -> dict[str, Any]:
    """Delete the local copies of superseded downloaded files (one product's months, or the whole site).

    A later listing no longer has these files (a newer publication replaced them) and stacking removed their
    rows from the lake, so they are dead weight in the working set. Journal first: ``dl_status='superseded'``
    and an ``evict_log`` row are committed before the unlink (a file listed again is downloaded again). Only a
    regular file under ``sites/<SITE>/raw`` is removed. Returns {'deleted', 'bytes', 'errors'}.
    """
    site = site.upper()
    ensure_schema(con)
    raw_dir = site_lake.site_paths(site).raw_dir
    raw = raw_dir.resolve()
    out: dict[str, Any] = {"deleted": 0, "bytes": 0, "errors": []}
    for r in superseded_rows(con, site, product, months):
        local = _local(r)
        present = bool(r.get("local_path")) and local.is_file() and not local.is_symlink()
        if present:
            try:
                if not local.resolve().is_relative_to(raw):
                    continue
                size = local.lstat().st_size
            except OSError as exc:
                out["errors"].append(f"{r['local_path']}: {type(exc).__name__}")
                continue
        else:
            size = 0
        con.execute("BEGIN")
        try:
            con.execute("UPDATE site_files SET dl_status='superseded' WHERE site=? AND product=? AND month=? "
                        "AND name=?", (site, r["product"], r["month"], r["name"]))
            if present:
                con.execute("INSERT INTO evict_log(site, local_path, bytes, evicted_at) VALUES (?,?,?,?)",
                            (site, r["local_path"], size, env.utcnow().isoformat()))
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        if not present:
            continue
        try:
            os.unlink(local)
        except OSError as exc:
            out["errors"].append(f"{r['local_path']}: {type(exc).__name__}")
            continue
        _prune_empty_dirs(local.parent, raw_dir)
        out["deleted"] += 1
        out["bytes"] += size
    if out["deleted"]:
        log.info("superseded %s%s: %d local file(s) deleted (%.2f GB)", site, f" {product}" if product else "",
                 out["deleted"], out["bytes"] / 1e9)
    return out


# ---------------------------------------------------------------- streaming: site cleanup

CLEANUP_DIRS = ("ducklake/data", "cards", "curation")
NOT_UPLOADED_SUFFIXES = (".part", ".wal", ".tmp", ".lock")  # never uploaded (site/upload.py): left alone


def cleanup_candidates(site: str) -> tuple[list[Path], list[Path]]:
    """(files to verify and delete, files upload never pushes) under the site's lake data, cards and curation."""
    root = site_lake.site_paths(site).site_dir
    files, skipped = [], []
    for d in CLEANUP_DIRS:
        base = root / d
        if not base.is_dir():
            continue
        for p in sorted(base.rglob("*")):
            if p.is_symlink() or not p.is_file():
                continue
            if p.name.startswith(".") or p.name.endswith(NOT_UPLOADED_SUFFIXES):
                skipped.append(p)
            else:
                files.append(p)
    return files, skipped


def verify_copies(session: Any, files: list[Path]) -> tuple[list[tuple[Path, int]], dict[str, list[str]]]:
    """Files whose Data Store copy has a good replica with the local size and checksum == local md5.

    Returns ([(path, size)], {reason: [relative paths]}) — a missing iRODS checksum is never trusted.
    """
    from ..mirror import good_replica, md5_of, remote_replicas
    from .upload import irods_path_for

    by_coll: dict[str, list[tuple[Path, str]]] = defaultdict(list)
    for f in files:
        ip = irods_path_for(f)
        by_coll[ip.rsplit("/", 1)[0]].append((f, ip.rsplit("/", 1)[1]))
    ok: list[tuple[Path, int]] = []
    blocked: dict[str, list[str]] = defaultdict(list)
    for coll, members in sorted(by_coll.items()):
        reps = remote_replicas(session, coll)
        for f, name in members:
            rel = str(f.relative_to(env.ROOT)) if f.is_relative_to(env.ROOT) else str(f)
            rr = reps.get(name, [])
            if not rr:
                blocked["not in the Data Store"].append(rel)
                continue
            try:
                size = f.stat().st_size
                md5 = md5_of(f)
            except OSError as exc:
                blocked[f"unreadable locally ({type(exc).__name__})"].append(rel)
                continue
            if good_replica(rr, size, md5):
                ok.append((f, size))
            else:
                blocked["no good replica with the local size and md5"].append(rel)
    return ok, dict(blocked)


def remove_files(files: list[tuple[Path, int]], site: str) -> dict[str, Any]:
    """Delete verified files and prune the directories they leave empty (the cleanup roots stay)."""
    root = site_lake.site_paths(site).site_dir
    stops = [root / d for d in CLEANUP_DIRS]
    n = freed = 0
    errors: list[str] = []
    for f, size in files:
        try:
            os.unlink(f)
        except OSError as exc:
            errors.append(f"{f.name}: {type(exc).__name__}")
            continue
        n += 1
        freed += size
        stop = next((s for s in stops if f.is_relative_to(s)), root)
        _prune_empty_dirs(f.parent, stop)
    return {"deleted": n, "bytes": freed, "errors": errors[:MAX_SAMPLES]}
