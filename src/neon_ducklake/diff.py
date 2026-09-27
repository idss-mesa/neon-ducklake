"""Markdown summary of what changed in the catalog lake between two snapshots.

Uses ``ducklake_table_changes`` over ``(from, to]`` and nets the result by ``rowHash`` (a row inserted and
deleted inside the range cancels out), then compares by logical key: added / removed / changed. Sections:
per-table counts, products (added, removed, status changes), new product x site pairs, months added,
PROVISIONAL -> released, files and bytes per product, new DOIs, taxonomy changes.
Default range: the last two snapshots.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from . import env
from .flatten import TABLES

log = logging.getLogger("neon_ducklake.diff")

ALIAS = "neon_diff"
LIST_LIMIT = 40


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def open_lake(lake_dir: Path | None = None) -> Any:
    """Read-only connection with the catalog lake attached as ``neon_diff``."""
    from .catalog_lake import connect

    lake_dir = (lake_dir or env.LAKE_DIR).resolve()
    con = connect(None)
    con.execute(f"ATTACH 'ducklake:{lake_dir / env.CATALOG_LAKE}' AS {ALIAS} (READ_ONLY, DATA_PATH "
                f"'{lake_dir / 'data'}/', OVERRIDE_DATA_PATH true)")
    return con


def snapshots(con: Any, alias: str = ALIAS) -> list[tuple[int, Any, str | None, str | None]]:
    return con.execute(f"SELECT snapshot_id, snapshot_time, author, commit_message FROM {alias}.snapshots() "
                       "ORDER BY snapshot_id").fetchall()


def default_range(con: Any, alias: str = ALIAS) -> tuple[int, int]:
    ids = [s[0] for s in snapshots(con, alias)]
    if len(ids) < 2:
        raise SystemExit("the catalog lake needs at least two snapshots to diff")
    return ids[-2], ids[-1]


def tables_at(con: Any, alias: str, snapshot: int) -> set[str]:
    out = set()
    for name in [r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name=? "
                                           "AND schema_name='main'", [alias]).fetchall()]:
        try:
            con.execute(f"SELECT 1 FROM {alias}.{q(name)} AT (VERSION => {int(snapshot)}) LIMIT 0")
        except Exception as exc:  # noqa: BLE001 - absent at that snapshot
            log.debug("%s absent at snapshot %s: %s", name, snapshot, exc)
        else:
            out.add(name)
    return out


def net_changes(con: Any, alias: str, table: str, frm: int, to: int) -> tuple[str, str] | None:
    """Temp tables ``ins_<table>``/``del_<table>`` of net inserted / deleted rows in (frm, to]."""
    ins, dele = f"ins_{table}", f"del_{table}"
    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE chg AS SELECT * FROM ducklake_table_changes('{alias}', 'main', "
                    f"'{table}', {int(frm) + 1}, {int(to)})")
    except Exception as exc:  # noqa: BLE001 - table absent in the range or schema changed
        log.debug("no changes for %s: %s", table, exc)
        return None
    cols = [r[0] for r in con.execute("DESCRIBE chg").fetchall()]
    if "rowHash" in cols:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {ins} AS SELECT * EXCLUDE (snapshot_id, rowid, change_type) "
                    "FROM chg WHERE change_type IN ('insert','update_postimage') AND rowHash NOT IN "
                    "(SELECT rowHash FROM chg WHERE change_type IN ('delete','update_preimage'))")
        con.execute(f"CREATE OR REPLACE TEMP TABLE {dele} AS SELECT * EXCLUDE (snapshot_id, rowid, change_type) "
                    "FROM chg WHERE change_type IN ('delete','update_preimage') AND rowHash NOT IN "
                    "(SELECT rowHash FROM chg WHERE change_type IN ('insert','update_postimage'))")
    else:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {ins} AS SELECT * EXCLUDE (snapshot_id, rowid, change_type) "
                    "FROM chg WHERE change_type IN ('insert','update_postimage')")
        con.execute(f"CREATE OR REPLACE TEMP TABLE {dele} AS SELECT * EXCLUDE (snapshot_id, rowid, change_type) "
                    "FROM chg WHERE change_type IN ('delete','update_preimage')")
    con.execute("DROP TABLE chg")
    return ins, dele


def _key_join(keys: tuple[str, ...], a: str, b: str) -> str:
    return " AND ".join(f"{a}.{q(k)} IS NOT DISTINCT FROM {b}.{q(k)}" for k in keys)


def key_counts(con: Any, table: str, keys: tuple[str, ...]) -> dict[str, int]:
    ins, dele = f"ins_{table}", f"del_{table}"
    j = _key_join(keys, "i", "d")
    added = con.execute(f"SELECT count(*) FROM {ins} i WHERE NOT EXISTS (SELECT 1 FROM {dele} d WHERE {j})").fetchone()[0]
    removed = con.execute(f"SELECT count(*) FROM {dele} d WHERE NOT EXISTS (SELECT 1 FROM {ins} i WHERE {j})"
                          ).fetchone()[0]
    changed = con.execute(f"SELECT count(*) FROM {ins} i WHERE EXISTS (SELECT 1 FROM {dele} d WHERE {j})").fetchone()[0]
    return {"added": added, "removed": removed, "changed": changed}


def md_table(header: list[str], rows: list[tuple[Any, ...]], limit: int = LIST_LIMIT) -> list[str]:
    if not rows:
        return ["_none_", ""]
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for r in rows[:limit]:
        out.append("| " + " | ".join("" if v is None else (f"{v:,}" if isinstance(v, int) else str(v)) for v in r)
                   + " |")
    if len(rows) > limit:
        out.append(f"| … {len(rows) - limit:,} more |" + " |" * (len(header) - 1))
    return out + [""]


def _fmt_bytes(n: int | None) -> str:
    n = int(n or 0)
    sign = "-" if n < 0 else ""
    n = abs(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1000 or unit == "TB":
            return f"{sign}{n:.1f} {unit}" if unit != "B" else f"{sign}{n} B"
        n /= 1000
    return f"{sign}{n}"


def diff_markdown(con: Any, frm: int, to: int, alias: str = ALIAS) -> str:
    """The markdown report for (frm, to] of the attached catalog lake."""
    snaps = {s[0]: s for s in snapshots(con, alias)}
    if frm not in snaps or to not in snaps:
        raise SystemExit(f"unknown snapshot(s): {frm}, {to} (have {sorted(snaps)})")
    lines = [f"# Catalog diff: snapshot {frm} → {to}", ""]
    for sid in (frm, to):
        s = snaps[sid]
        lines.append(f"- **{sid}** {s[1]} — {s[3] or '(no message)'}" + (f" ({s[2]})" if s[2] else ""))
    lines.append("")
    present = tables_at(con, alias, to)
    counts = []
    changed: set[str] = set()
    for table in sorted(present):
        res = net_changes(con, alias, table, frm, to)
        if res is None:
            continue
        n_ins = con.execute(f"SELECT count(*) FROM {res[0]}").fetchone()[0]
        n_del = con.execute(f"SELECT count(*) FROM {res[1]}").fetchone()[0]
        if not (n_ins or n_del):
            continue
        changed.add(table)
        kc = key_counts(con, table, TABLES[table].key) if table in TABLES else {}
        counts.append((table, n_ins, n_del, kc.get("added"), kc.get("removed"), kc.get("changed")))
    lines += ["## Tables", ""]
    lines += md_table(["table", "+rows", "-rows", "keys added", "keys removed", "keys changed"], counts, 200)

    if "products" in changed:
        j = _key_join(("productCode",), "i", "d")
        rows = con.execute(f"SELECT 'added', i.productCode, i.productName, NULL, i.productStatus FROM ins_products i "
                           f"WHERE NOT EXISTS (SELECT 1 FROM del_products d WHERE {j}) UNION ALL "
                           f"SELECT 'removed', d.productCode, d.productName, d.productStatus, NULL FROM del_products d "
                           f"WHERE NOT EXISTS (SELECT 1 FROM ins_products i WHERE {j}) UNION ALL "
                           f"SELECT 'status', i.productCode, i.productName, d.productStatus, i.productStatus "
                           f"FROM ins_products i JOIN del_products d ON {j} "
                           "WHERE i.productStatus IS DISTINCT FROM d.productStatus ORDER BY 1, 2").fetchall()
        lines += ["## Products", ""] + md_table(["change", "product", "name", "old status", "new status"], rows)
    if "product_site" in changed:
        j = _key_join(("productCode", "siteCode"), "i", "d")
        rows = con.execute(f"SELECT i.productCode, count(*) AS pairs, list(i.siteCode ORDER BY i.siteCode), "
                           f"string_agg(DISTINCT i.inventoryScope, ',') FROM ins_product_site i WHERE NOT EXISTS "
                           f"(SELECT 1 FROM del_product_site d WHERE {j}) GROUP BY 1 ORDER BY 2 DESC, 1").fetchall()
        rows = [(p, n, " ".join(s[:12]) + (f" … +{len(s) - 12}" if len(s) > 12 else ""), sc) for p, n, s, sc in rows]
        total = sum(r[1] for r in rows)
        lines += [f"## New product × site pairs ({total:,})", ""]
        lines += md_table(["product", "pairs", "sites", "scope"], rows)
    if "availability_months" in changed:
        j = _key_join(("productCode", "siteCode", "month"), "i", "d")
        rows = con.execute(f"SELECT i.productCode, count(*), count(*) FILTER (WHERE i.isReleaseOnly), "
                           f"count(*) FILTER (WHERE i.isProvisional), min(i.month), max(i.month) "
                           f"FROM ins_availability_months i WHERE NOT EXISTS "
                           f"(SELECT 1 FROM del_availability_months d WHERE {j}) GROUP BY 1 ORDER BY 2 DESC, 1").fetchall()
        lines += [f"## Months added ({sum(r[1] for r in rows):,} cells)", ""]
        lines += md_table(["product", "cells", "release-only", "provisional", "first", "last"], rows)
        rows = con.execute(f"SELECT i.productCode, i.release, count(*), min(i.month), max(i.month) "
                           f"FROM ins_availability_months i JOIN del_availability_months d ON {j} "
                           "WHERE d.release = 'PROVISIONAL' AND i.release <> 'PROVISIONAL' "
                           "GROUP BY 1, 2 ORDER BY 3 DESC, 1").fetchall()
        lines += [f"## PROVISIONAL → released ({sum(r[2] for r in rows):,} cells)", ""]
        lines += md_table(["product", "release", "cells", "first", "last"], rows)
        removed = con.execute(f"SELECT count(*) FROM del_availability_months d WHERE NOT EXISTS "
                              f"(SELECT 1 FROM ins_availability_months i WHERE {j})").fetchone()[0]
        if removed:
            lines += [f"Cells no longer available: {removed:,}", ""]
    if "files" in changed:
        rows = con.execute("WITH a AS (SELECT productCode, count(*) n, sum(coalesce(size,0)) b FROM ins_files "
                           "GROUP BY 1), r AS (SELECT productCode, count(*) n, sum(coalesce(size,0)) b "
                           "FROM del_files GROUP BY 1) SELECT productCode, coalesce(a.n,0), coalesce(r.n,0), "
                           "coalesce(a.b,0), coalesce(r.b,0) FROM a FULL JOIN r USING (productCode) "
                           "ORDER BY abs(coalesce(a.b,0) - coalesce(r.b,0)) DESC, 1").fetchall()
        fmt = [(p, a, r, _fmt_bytes(ab), _fmt_bytes(rb), _fmt_bytes(ab - rb)) for p, a, r, ab, rb in rows]
        tot = (sum(r[1] for r in rows), sum(r[2] for r in rows), sum(r[3] for r in rows) - sum(r[4] for r in rows))
        lines += [f"## Files per product (+{tot[0]:,} / -{tot[1]:,} files, net {_fmt_bytes(tot[2])})", ""]
        lines += md_table(["product", "+files", "-files", "+bytes", "-bytes", "net bytes"], fmt)
    doi_rows = []
    if "product_releases" in changed:
        j = _key_join(("productCode", "release"), "i", "d")
        doi_rows = con.execute(f"SELECT i.productCode, i.release, i.productDoi FROM ins_product_releases i "
                               f"WHERE NOT EXISTS (SELECT 1 FROM del_product_releases d WHERE {j}) "
                               "ORDER BY 2 DESC, 1").fetchall()
    if "releases" in changed:
        j = _key_join(("release",), "i", "d")
        new_rel = con.execute(f"SELECT i.release, i.generationDate, i.productCount FROM ins_releases i WHERE NOT EXISTS "
                              f"(SELECT 1 FROM del_releases d WHERE {j}) ORDER BY 1").fetchall()
        lines += ["## New releases", ""] + md_table(["release", "generated", "products"], new_rel)
    if doi_rows:
        lines += [f"## New DOIs ({len(doi_rows):,})", ""] + md_table(["product", "release", "DOI"], doi_rows)
    if "taxonomy" in changed:
        j = _key_join(("taxonTypeCode", "taxonID"), "i", "d")
        rows = con.execute(
            f"WITH a AS (SELECT taxonTypeCode t, count(*) n FROM ins_taxonomy i WHERE NOT EXISTS "
            f"(SELECT 1 FROM del_taxonomy d WHERE {j}) GROUP BY 1), "
            f"r AS (SELECT taxonTypeCode t, count(*) n FROM del_taxonomy d WHERE NOT EXISTS "
            f"(SELECT 1 FROM ins_taxonomy i WHERE {j}) GROUP BY 1), "
            f"c AS (SELECT i.taxonTypeCode t, count(*) n FROM ins_taxonomy i JOIN del_taxonomy d ON {j} GROUP BY 1) "
            "SELECT t, coalesce(a.n,0), coalesce(r.n,0), coalesce(c.n,0) FROM a FULL JOIN r USING (t) "
            "FULL JOIN c USING (t) ORDER BY 1").fetchall()
        lines += ["## Taxonomy", ""] + md_table(["taxonTypeCode", "added", "removed", "changed"], rows)
    if not changed:
        lines += ["No table changed.", ""]
    return "\n".join(lines)


def main(frm: int | None = None, to: int | None = None, lake_dir: Path | None = None) -> int:
    lake = (lake_dir or env.LAKE_DIR) / env.CATALOG_LAKE
    if not lake.exists():
        log.error("no catalog lake at %s", lake)
        return 1
    con = open_lake(lake_dir)
    try:
        if frm is None or to is None:
            a, b = default_range(con)
            frm = a if frm is None else frm
            to = b if to is None else to
        print(diff_markdown(con, frm, to))
    finally:
        con.close()
    return 0
