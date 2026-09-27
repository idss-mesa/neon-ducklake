"""Phase 1 — local verification of the catalog lake against its sealed harvest run.

``verify(run_id)`` returns named checks ``{name: {ok, expected, actual, detail, hard}}``:
- lake row counts vs counts taken independently from the sealed payloads;
- the request ledger is all ok, every data_query pair landed;
- pair coverage: every non-AOP current or manifest cell has files, is external, or is a listed exception
  (``ref/catalog_coverage_exceptions.json``: ``[{productCode, siteCode, month?, reason}]``);
- zero signed URLs (SQL over every text column + a scan of ``sealed/``);
- one snapshot per build, and time travel is consistent with ``ducklake_table_changes``;
- ``parseConsistent`` is 100%;
- SRER/HARV current basic inventory totals (informational).
iRODS / MESA checks live elsewhere: they register a callable in ``HOOKS`` that receives the
``VerifyContext`` and returns more named checks. ``main`` exits non-zero when any hard check fails.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import env
from .flatten import AOP_CODE_RE, MANIFEST_DP_RE, TABLES, iter_sealed, strip_taxon_prefixes

log = logging.getLogger("neon_ducklake.verify_catalog")

ALIAS = "neon_verify"
REPORT_SITES = ("SRER", "HARV")
EXCEPTIONS_FILE = env.ROOT / "ref" / "catalog_coverage_exceptions.json"
# plan expectations (2026-09-26/27); the API may grow, so these are soft with <=0.5% upward drift
PLAN_COUNTS = {"products": 202, "sites": 81, "domains": 20, "releases": 6, "release_products": 683,
               "taxonomy": 134196, "sample_classes": 305, "prototype_datasets": 72, "documents": 510}


@dataclass
class VerifyContext:
    run_id: str
    run_dir: Path
    lake_dir: Path
    con: Any
    alias: str = ALIAS

    @property
    def sealed(self) -> Path:
        return self.run_dir / "sealed"


HOOKS: list[Callable[[VerifyContext], dict[str, dict[str, Any]]]] = []


def register_hook(fn: Callable[[VerifyContext], dict[str, dict[str, Any]]]) -> Callable:
    """Add an extra check provider (iRODS, MESA, …) run by ``verify``."""
    HOOKS.append(fn)
    return fn


def check(ok: bool, expected: Any = None, actual: Any = None, detail: str = "", hard: bool = True) -> dict[str, Any]:
    return {"ok": bool(ok), "expected": expected, "actual": actual, "detail": detail, "hard": hard}


# ---------------------------------------------------------------- independent payload counts


def _lines(sealed: Path, family: str) -> list[dict[str, Any]]:
    p = sealed / f"{family}.jsonl.gz"
    return list(iter_sealed(p)) if p.exists() else []


def _payload(sealed: Path, family: str) -> Any:
    lines = _lines(sealed, family)
    return lines[0]["payload"] if lines else None


def _as_list(v: Any, key: str = "data") -> list[Any]:
    return v if isinstance(v, list) else ((v or {}).get(key) or [])


def payload_counts(sealed: Path) -> dict[str, int]:
    """Expected row counts computed straight from the sealed payloads (not via flatten)."""
    products = _as_list(_payload(sealed, "products"))
    sites = _as_list(_payload(sealed, "sites"))
    releases = _as_list(_payload(sealed, "releases"), "releases")
    manifests = {ln["key"]: ln["payload"] for ln in _lines(sealed, "manifests")}
    cur_pairs = {(p["productCode"], s["siteCode"]) for p in products for s in p.get("siteCodes") or []}
    cur_cells = {(p["productCode"], s["siteCode"], m) for p in products for s in p.get("siteCodes") or []
                 for m in s.get("availableMonths") or []}
    man_cells = set()
    for tag, man in manifests.items():
        for dp in man.get("dpSiteMonths") or []:
            code = MANIFEST_DP_RE.sub("", dp["dp"])
            for sm in dp.get("siteMonths") or []:
                man_cells.update((tag, code, sm["site"], m) for m in sm.get("months") or [])
    ro_cells = {(c, s, m) for _t, c, s, m in man_cells} - cur_cells
    ro_pairs = {(c, s) for c, s, _m in ro_cells} - cur_pairs
    taxa_total = sum(int(ln["payload"].get("total") or 0) for ln in _lines(sealed, "taxonomy")
                     if ln["key"].endswith("__0"))
    taxa_rows = len({(r.get("taxonTypeCode"), r.get("taxonID")) for ln in _lines(sealed, "taxonomy")
                     for r in (strip_taxon_prefixes(x) for x in ln["payload"].get("data") or [])})
    rel_records = 0
    for ln in _lines(sealed, "release_products"):
        rel_records += sum(1 for p in _as_list(ln["payload"])
                           if any(r.get("release") == ln["key"] for r in p.get("releases") or []) or p.get("siteCodes"))
    proto_files = {(ln["payload"].get("datasetUuid"), f.get("fileName") or f.get("name"))
                   for ln in _lines(sealed, "prototype_data") for f in ln["payload"].get("files") or []}
    samples = _payload(sealed, "samples") or {}
    return {
        "products": len(products), "sites": len(sites), "domains": len({s.get("domainCode") for s in sites}),
        "product_site": len(cur_pairs) + len(ro_pairs), "product_site.current": len(cur_pairs),
        "availability_months": len(cur_cells) + len(ro_cells), "availability_months.current": len(cur_cells),
        "releases": len(releases), "release_products": sum(len(r.get("dataProducts") or []) for r in releases),
        "release_manifest_months": len(man_cells), "release_product_records": rel_records,
        "taxonomy": taxa_rows, "taxonomy.pageTotals": taxa_total,
        "sample_classes": len({e.get("key") for e in _as_list(samples, "entries")}),
        "prototype_datasets": len(_as_list(_payload(sealed, "prototype"))), "prototype_files": len(proto_files),
        "documents": len({s["specNumber"] for p in products for s in p.get("specs") or [] if s.get("specNumber")}),
        "product_change_logs": sum(len(p.get("changeLogs") or []) for p in products),
        "product_biorepository": sum(len(p.get("biorepositoryCollections") or []) for p in products),
        "site_products": sum(len(s.get("dataProducts") or []) for s in sites),
        "locations.rows": len({r.get("locationName") for ln in _lines(sealed, "locations")
                               for r in ln["payload"].get("rows") or []}),
        "location_history": sum(len(ln["payload"].get("locationHistory") or []) for ln in _lines(sealed, "location_history")),
    }


def _count_dq(path: str) -> tuple[int, int]:
    keys = set()
    size = {}
    for ln in iter_sealed(Path(path)):
        for r in ln["payload"].get("releases") or []:
            for pk in r.get("packages") or []:
                for f in pk.get("files") or []:
                    k = (pk.get("siteCode"), pk.get("month"), r.get("release"), f.get("name"))
                    keys.add(k)
                    size[k] = int(f.get("size") or 0)
    return len(keys), sum(size.values())


def payload_file_counts(sealed: Path, workers: int = 8) -> tuple[int, int]:
    """Distinct (product, site, month, release, name) files and their bytes over sealed data_query."""
    files = sorted((sealed / "data_query").glob("*.jsonl.gz")) if (sealed / "data_query").exists() else []
    if not files:
        return 0, 0
    if workers > 1 and len(files) > 1:
        with ProcessPoolExecutor(max_workers=min(workers, len(files))) as ex:
            res = list(ex.map(_count_dq, [str(f) for f in files]))
    else:
        res = [_count_dq(str(f)) for f in files]
    return sum(n for n, _b in res), sum(b for _n, b in res)


# ---------------------------------------------------------------- lake checks


def _count(con: Any, alias: str, table: str, where: str = "") -> int:
    return con.execute(f'SELECT count(*) FROM {alias}."{table}" {where}').fetchone()[0]


def text_columns(con: Any, alias: str) -> dict[str, list[str]]:
    rows = con.execute("SELECT table_name, column_name FROM duckdb_columns() WHERE database_name=? AND "
                       "schema_name='main' AND (data_type IN ('VARCHAR', 'JSON') OR data_type LIKE 'VARCHAR[]%' "
                       "OR data_type LIKE 'MAP(%' OR data_type LIKE 'STRUCT(%') AND table_name IN "
                       "(SELECT table_name FROM duckdb_tables() WHERE database_name=?)", [alias, alias]).fetchall()
    out: dict[str, list[str]] = defaultdict(list)
    for t, c in rows:
        out[t].append(c)
    return out


def load_exceptions(path: Path | None = None) -> list[dict[str, Any]]:
    path = path or EXCEPTIONS_FILE
    return json.loads(path.read_text()) if path.exists() else []


def coverage_checks(ctx: VerifyContext) -> dict[str, dict[str, Any]]:
    con, a = ctx.con, ctx.alias
    exc = load_exceptions()
    con.execute("CREATE OR REPLACE TEMP TABLE cov_exc (productCode VARCHAR, siteCode VARCHAR, month VARCHAR)")
    if exc:
        con.executemany("INSERT INTO cov_exc VALUES (?, ?, ?)",
                        [(e.get("productCode"), e.get("siteCode"), e.get("month")) for e in exc])
    missing = con.execute(
        f"SELECT productCode, siteCode, count(*) AS cells, min(month), max(month) FROM {a}.v_site_inventory i "
        "WHERE fileCount = 0 AND NOT isExternal AND NOT EXISTS (SELECT 1 FROM cov_exc e WHERE "
        "e.productCode = i.productCode AND e.siteCode = i.siteCode AND (e.month IS NULL OR e.month = i.month)) "
        "GROUP BY 1, 2 ORDER BY 3 DESC").fetchall()
    n_missing = sum(r[2] for r in missing)
    total = _count(con, a, "v_site_inventory")
    orphan = con.execute(
        f"SELECT count(*) FROM {a}.files f WHERE NOT EXISTS (SELECT 1 FROM {a}.availability_months m WHERE "
        "m.productCode = f.productCode AND m.siteCode = f.siteCode AND m.month = f.month)").fetchone()[0]
    ex = "; ".join(f"{p} {s} {n} cells {lo}..{hi}" for p, s, n, lo, hi in missing[:10])
    return {
        "coverage.cells": check(n_missing == 0, 0, n_missing,
                                f"{total:,} non-AOP cells; {len(missing)} pairs with uncovered cells; {ex}"),
        "coverage.orphanFiles": check(orphan == 0, 0, orphan, "files in months absent from availability_months",
                                      hard=False),
    }


def snapshot_checks(ctx: VerifyContext) -> dict[str, dict[str, Any]]:
    con, a = ctx.con, ctx.alias
    out = {}
    snaps = con.execute(f"SELECT snapshot_id, author, commit_message, changes FROM {a}.snapshots() "
                        "ORDER BY 1").fetchall()
    builds = _count(con, a, "catalog_builds")
    out["snapshots.onePerBuild"] = check(len(snaps) - 1 == builds, builds + 1, len(snaps),
                                         "snapshots = 1 (lake creation) + one per catalog_builds row")
    last = snaps[-1]
    out["snapshots.lastCommit"] = check(last[1] == env.ACTOR and last[2] == f"catalog build {ctx.run_id}",
                                        f"{env.ACTOR}: catalog build {ctx.run_id}", f"{last[1]}: {last[2]}")
    n, p = last[0], snaps[-2][0] if len(snaps) > 1 else None
    created_now = set((last[3] or {}).get("tables_created") or [])
    bad = []
    for name in TABLES:
        try:
            at_n = con.execute(f'SELECT count(*) FROM {a}."{name}" AT (VERSION => {n})').fetchone()[0]
        except Exception as exc:  # noqa: BLE001
            bad.append(f"{name}: unreadable at {n} ({exc})")
            continue
        try:
            at_p = con.execute(f'SELECT count(*) FROM {a}."{name}" AT (VERSION => {p})').fetchone()[0]
        except Exception:  # noqa: BLE001 - absent before the snapshot that created it
            at_p = None
        if at_p is None:
            if f"main.{name}" not in created_now:
                bad.append(f"{name}: absent at {p} but not created by snapshot {n}")
            continue
        ch = dict(con.execute(f"SELECT change_type, count(*) FROM ducklake_table_changes('{a}', 'main', '{name}', "
                              f"{n}, {n}) GROUP BY 1").fetchall())
        if at_p + ch.get("insert", 0) - ch.get("delete", 0) != at_n:
            bad.append(f"{name}: {at_p} + {ch.get('insert', 0)} - {ch.get('delete', 0)} != {at_n}")
    out["snapshots.timeTravel"] = check(not bad, 0, len(bad), "; ".join(bad[:10]) or
                                        f"counts at {p} + changes of {n} == counts at {n}")
    return out


def inventory_totals(ctx: VerifyContext, site: str) -> dict[str, Any]:
    r = ctx.con.execute(
        f"SELECT count(DISTINCT productCode) FILTER (WHERE NOT isReleaseOnly), count(*) FILTER (WHERE NOT isReleaseOnly), "
        f"sum(fileCount) FILTER (WHERE NOT isReleaseOnly), sum(totalBytes) FILTER (WHERE NOT isReleaseOnly), "
        f"count(DISTINCT productCode), count(*), sum(fileCount), sum(totalBytes), "
        f"count(*) FILTER (WHERE isExternal) FROM {ctx.alias}.v_site_inventory WHERE siteCode = ?", [site]).fetchone()
    keys = ("products", "siteMonths", "files", "bytes", "productsInclReleaseOnly", "siteMonthsInclReleaseOnly",
            "filesInclReleaseOnly", "bytesInclReleaseOnly", "externalSiteMonths")
    return {k: int(v or 0) for k, v in zip(keys, r, strict=True)}


def verify(run_id: str | None = None, *, lake_dir: Path | None = None, run_dir: Path | None = None,
           hooks: list[Callable[[VerifyContext], dict[str, dict[str, Any]]]] | None = None,
           workers: int = 8) -> dict[str, dict[str, Any]]:
    """Run every local check (and the registered hooks) against the catalog lake."""
    from .catalog_lake import connect
    from .seal import resolve_run, scan_signed

    rid = resolve_run(run_id)
    run_dir = run_dir or env.RUNS_DIR / rid
    lake_dir = (lake_dir or env.LAKE_DIR).resolve()
    con = connect(None)
    con.execute(f"ATTACH 'ducklake:{lake_dir / env.CATALOG_LAKE}' AS {ALIAS} (READ_ONLY, DATA_PATH "
                f"'{lake_dir / 'data'}/', OVERRIDE_DATA_PATH true)")
    ctx = VerifyContext(rid, run_dir, lake_dir, con)
    out: dict[str, dict[str, Any]] = {}
    try:
        a = ALIAS
        exp = payload_counts(ctx.sealed)
        lake = {t: _count(con, a, t) for t in TABLES}
        for t in ("products", "sites", "domains", "product_site", "availability_months", "releases",
                  "release_products", "release_manifest_months", "release_product_records", "taxonomy",
                  "sample_classes", "prototype_datasets", "prototype_files", "documents", "product_change_logs",
                  "product_biorepository", "site_products"):
            out[f"count.{t}"] = check(lake[t] == exp[t], exp[t], lake[t])
        out["count.taxonomy.pageTotals"] = check(lake["taxonomy"] == exp["taxonomy.pageTotals"],
                                                 exp["taxonomy.pageTotals"], lake["taxonomy"])
        cur_ps = _count(con, a, "product_site", "WHERE currentMonthCount > 0")
        diff_keys = con.execute(
            f"SELECT count(*) FROM (SELECT productCode, siteCode FROM {a}.site_products EXCEPT "
            f"SELECT productCode, siteCode FROM {a}.product_site WHERE currentMonthCount > 0 UNION ALL "
            f"(SELECT productCode, siteCode FROM {a}.product_site WHERE currentMonthCount > 0 EXCEPT "
            f"SELECT productCode, siteCode FROM {a}.site_products))").fetchone()[0]
        out["count.site_products=product_site.current"] = check(
            diff_keys == 0 and cur_ps == lake["site_products"], lake["site_products"], cur_ps,
            f"{diff_keys} keys differ")
        out["count.availability_months.current"] = check(
            _count(con, a, "availability_months", "WHERE NOT isReleaseOnly") == exp["availability_months.current"],
            exp["availability_months.current"], _count(con, a, "availability_months", "WHERE NOT isReleaseOnly"))
        documents_head = _count(con, a, "documents", "WHERE headStatus IS NOT NULL")
        out["documents.headStatus"] = check(documents_head == lake["documents"], lake["documents"], documents_head)
        nodes_path = ctx.sealed / "stage" / "location_nodes.json.gz"
        n_nodes = len(json.loads(gzip.decompress(nodes_path.read_bytes()))) if nodes_path.exists() else None
        out["count.locations"] = check(lake["locations"] == exp["locations.rows"], exp["locations.rows"],
                                       lake["locations"], f"hierarchy nodes {n_nodes}; not returned by "
                                       f"findLocations: {(n_nodes or 0) - lake['locations']}", hard=True)
        out["count.location_history"] = check(lake["location_history"] == exp["location_history"],
                                              exp["location_history"], lake["location_history"])
        n_files, n_bytes = payload_file_counts(ctx.sealed, workers)
        lake_bytes = con.execute(f"SELECT coalesce(sum(size), 0) FROM {a}.files").fetchone()[0]
        out["count.files"] = check(lake["files"] == n_files and lake_bytes == n_bytes, n_files, lake["files"],
                                   f"bytes sealed {n_bytes:,} lake {lake_bytes:,}")
        for t, n in PLAN_COUNTS.items():
            out[f"plan.{t}"] = check(n <= lake[t] <= n * 1.005, n, lake[t], "plan count, upward drift <= 0.5%",
                                     hard=False)

        # ledger
        led = dict(con.execute(f"SELECT status, count(*) FROM {a}.harvest_requests WHERE runId = ? GROUP BY 1",
                               [rid]).fetchall())
        out["ledger.allOk"] = check(led.get("error", 0) == 0 and led.get("ok", 0) > 0, 0, led.get("error", 0),
                                    json.dumps(led))
        dq = dict(con.execute(f"SELECT status, count(*) FROM {a}.data_query_pairs GROUP BY 1").fetchall())
        out["ledger.dataQueryPairs"] = check(set(dq) <= {"ok"}, "all ok", json.dumps(dq))
        aop = con.execute(f"SELECT count(*) FROM {a}.data_query_pairs WHERE regexp_matches(productCode, "
                          f"'{AOP_CODE_RE.pattern}')").fetchone()[0]
        out["inventory.noAop"] = check(aop == 0, 0, aop, "no AOP product in the file inventory")

        out.update(coverage_checks(ctx))

        # signed URLs
        hits = []
        for t, cols in text_columns(con, a).items():
            cond = " OR ".join(f"contains(CAST(\"{c}\" AS VARCHAR), 'X-Goog')" for c in cols)
            n = con.execute(f'SELECT count(*) FROM {a}."{t}" WHERE {cond}').fetchone()[0]
            if n:
                hits.append(f"{t}: {n}")
        out["signed.sql"] = check(not hits, 0, len(hits), "; ".join(hits))
        bad = scan_signed(ctx.sealed) if ctx.sealed.exists() else ["sealed/ missing"]
        out["signed.sealed"] = check(not bad, 0, len(bad), "; ".join(bad[:10]))

        out.update(snapshot_checks(ctx))

        pc = con.execute(f"SELECT count(*), count(*) FILTER (WHERE NOT parseConsistent) FROM {a}.files").fetchone()
        out["files.parseConsistent"] = check(pc[1] == 0, pc[0], pc[0] - pc[1],
                                             f"{pc[1]} inconsistent of {pc[0]}")
        for site in REPORT_SITES:
            tot = inventory_totals(ctx, site)
            out[f"inventory.{site}"] = check(True, None, tot["files"], json.dumps(tot), hard=False)
        for hook in (HOOKS if hooks is None else hooks):
            out.update(hook(ctx))
    finally:
        con.close()
    return out


def print_report(results: dict[str, dict[str, Any]]) -> None:
    for name, r in results.items():
        flag = "ok  " if r["ok"] else ("FAIL" if r["hard"] else "warn")
        exp = "" if r["expected"] is None else f" expected={r['expected']}"
        print(f"{flag} {name:42s}{exp} actual={r['actual']} {r['detail']}".rstrip())


def main(run_id: str | None = None, lake_dir: Path | None = None) -> int:
    results = verify(run_id, lake_dir=lake_dir, workers=min(8, os.cpu_count() or 4))
    print_report(results)
    failed = [n for n, r in results.items() if r["hard"] and not r["ok"]]
    if failed:
        log.error("%d hard check(s) failed: %s", len(failed), ", ".join(failed))
        return 1
    return 0


__all__ = ["HOOKS", "VerifyContext", "check", "main", "register_hook", "verify"]
