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
    """iRODS paths whose REQUIRED_RECORDED attributes are all recorded in MESA history (and not removed)."""
    need = set(REQUIRED_RECORDED)
    have: dict[str, set[str]] = defaultdict(set)
    rows = con.execute(
        f"SELECT a.path, a.attribute FROM avu_journal a WHERE a.scope = ? AND a.op = 'add' AND a.status = 'recorded'"
        f" AND a.attribute IN ({','.join('?' * len(need))})"
        " AND NOT EXISTS (SELECT 1 FROM avu_journal r WHERE r.path = a.path AND r.attribute = a.attribute"
        "   AND r.id > a.id AND r.status IN ('intent', 'applied', 'recorded', 'error')"
        "   AND NOT (r.op = 'add' AND r.status = 'recorded'))",
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
        local = _local(r)
        try:
            size = local.lstat().st_size
        except OSError as exc:
            errors.append(f"{r['local_path']}: {type(exc).__name__}")
            continue
        if dry_run:
            freed += size
            n += 1
            continue
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
        try:
            os.unlink(local)
        except OSError as exc:
            errors.append(f"{r['local_path']}: {type(exc).__name__}")
            con.execute("UPDATE site_files SET local_evicted=0 WHERE site=? AND product=? AND month=? AND name=?",
                        (site, r["product"], r["month"], r["name"]))
            continue
        freed += size
        n += 1
        _prune_empty_dirs(local.parent, raw)
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
