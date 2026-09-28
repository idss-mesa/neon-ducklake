"""`site cards <SITE>`: dataset cards built from the product record and real column profiles.

Outputs, for each site product:

- ``sites/<SITE>/cards/<productCode>.md``, the product card.
  - Name, code, science team, status, themes and keywords.
  - The site line, in the mesa-anyjev grammar.
  - Excerpts of the abstract and design descriptions, at most 1,500 characters each.
  - Units with their UO terms.
  - For each stacked table: ``Rows: n; months covered: a to b``, then every column as
    ``- name | description | dataType | units | profile``.
  - The ten taxa with the most records, with their NCBITaxon resolution status.
  - An external product (for example phenocam) gets a short card with the link instead.
  - Lab tables (``<dp>__lab_<table>``, stacked from ``NEON.<Lab>.<table>.<ts>.csv`` files) get their own
    column section, and the product's lab files are listed by lab and table.
- ``sites/<SITE>/cards/_site.md``, the site card: the input of the site-level Opus curation
  (``site curate``, ``curation/prompt_site.md``). It holds the site name, HABITAT, domain, state,
  lat/lon/elevation, NEON's description, TypeEco and the NLCD shares (terrestrial sites only: NEON copied
  a shortgrass-steppe TypeEco to every aquatic site), the site's products, and the heuristic biome /
  water-body proposal of ``site terms`` as a hint. Card files whose name starts with ``_`` are not
  product cards.
- ``sites/<SITE>/cards/anyjev/<productCode>.<table>.md``, one card per stacked data or HDF5 table.
  - It uses the mesa-anyjev ``DatasetCard`` grammar (see ``mesa_anyjev.cards.parse_card`` and
    ``neon-avu-eval/cards``).
  - Each card is self-checked against a copy of that grammar. When the mesa-anyjev venv exists,
    its own parser is run on the card through a subprocess.

Profiles come from the stacked lake tables and are never "not profiled":
- Numeric columns: n, distinct count, min / median / max, and the top values when there are few.
- String and boolean columns: distinct count and the top five values.
- Temporal columns: n, distinct count, min and max.
- A column with no values is "all blank".
- Distinct counts are exact up to 5M rows and approximate (``~``) above that.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from .. import env
from . import ontology_store as store
from . import site_lake, terms

log = logging.getLogger("neon_ducklake.site.cards")

EXCERPT_CHARS = 1500
TOP_N = 5
FEW_DISTINCT = 12
APPROX_ROWS = 5_000_000
VALUE_CHARS = 60
SOURCE = "NEON (National Ecological Observatory Network) Data API, basic package, stacked across site-months."
# mesa-anyjev card parser (optional): used when mesa_anyjev is not importable here.
ANYJEV_PYTHON = Path(os.environ.get("NEON_DUCKLAKE_ANYJEV_PYTHON")
                     or Path.home() / "github/idss-mesa/mesa-anyjev/.venv/bin/python").expanduser()

# Columns neon-ducklake adds to every stacked table (used when variables has no row for them).
ADDED_COLUMNS: dict[str, tuple[str, str]] = {
    "siteID": ("NEON site code", "string"),
    "domainID": ("Unique identifier of the NEON domain", "string"),
    "month": ("Month (YYYY-MM) of the source file; added by neon-ducklake", "string"),
    "release": ("NEON release tag of the source file (RELEASE-YYYY or PROVISIONAL); added by neon-ducklake",
                "string"),
    "publicationDate": ("Publication timestamp of the source file; added by neon-ducklake", "dateTime"),
    "sourceFile": ("NEON file name the row was read from; added by neon-ducklake", "string"),
    "horizontalPosition": ("Horizontal (HOR) index of the sensor position, from the file name", "string"),
    "verticalPosition": ("Vertical (VER) index of the sensor position, from the file name", "string"),
    "tmi": ("Temporal index (averaging interval code), from the file name", "string"),
}
NUMERIC_TYPES = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER",
                 "UBIGINT", "FLOAT", "DOUBLE", "REAL")
TEMPORAL_PREFIXES = ("DATE", "TIMESTAMP", "TIME")
H5_LEVELS = {"turb": "turbulent", "stor": "storage", "nsae": "net surface-atmosphere exchange"}

# mesa-anyjev DatasetCard grammar (copied from mesa_anyjev/cards.py for the self-check).
_HEADER = re.compile(r"^#\s*Dataset:\s*(?P<product>\S+)\s*/\s*(?P<table>\S+)\s*$")
_PRODUCT = re.compile(r"^Product:\s*(?P<code>\S+)\s*[—-]+\s*(?P<title>[^.]+)\.?\s*(?P<desc>.*)$")
_ROWS = re.compile(r"^Rows:\s*(?P<rows>\d+);\s*months covered:\s*(?P<a>\S+)\s+to\s+(?P<b>\S+)")
_SITE = re.compile(r"(?P<code>[A-Z]{4})\s*\((?P<name>[^,]+),\s*(?P<state>[^,]+),\s*domain\s+(?P<domain>D\d{2})"
                   r"\s*(?P<dname>[^;]*);\s*(?P<habitat>[^)]*)\)")
_COLUMN = re.compile(r"^-\s*(?P<name>[^|]+)\|(?P<desc>[^|]*)\|(?P<dtype>[^|]*)\|(?P<unit>[^|]*)\|(?P<profile>.*)$")


# ---------------------------------------------------------------- text helpers

def clean(text: Any, *, pipes: bool = True) -> str:
    """One line of card text: whitespace collapsed; '|' replaced (it is the column separator)."""
    s = " ".join(str(text or "").split())
    return s.replace("|", "/") if pipes else s


def excerpt(text: Any, limit: int = EXCERPT_CHARS) -> str:
    """At most ``limit`` characters, cut on a sentence (else word) boundary and marked with '…'."""
    s = " ".join(str(text or "").split())
    if len(s) <= limit:
        return s
    cut = s[: limit - 2]  # room for the ' …' marker
    ends = [m.end() for m in re.finditer(r"[.!?](?=\s)", cut) if m.end() >= limit // 2]
    if ends:
        cut = cut[: ends[-1]]
    elif " " in cut[limit // 2:]:
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip() + " …"


def fmt_num(x: Any) -> str:
    if x is None:
        return "NA"
    if isinstance(x, bool):
        return str(x).lower()
    if isinstance(x, int):
        return str(x)
    try:
        f = float(x)
    except (TypeError, ValueError):
        return clean(x)
    if math.isnan(f):
        return "NaN"
    return str(int(f)) if f.is_integer() and abs(f) < 1e15 else f"{f:.6g}"


def fmt_time(x: Any) -> str:
    if x is None:
        return "NA"
    if hasattr(x, "isoformat"):
        s = x.isoformat()
        s = s.replace("+00:00", "Z")
        return re.sub(r":00(\.0+)?Z$", "Z", s) if "T" in s else s
    return clean(x)


# Personal contact data never goes into cards (they are mirrored to iRODS and sent to curation models):
# NEON tables carry technician e-mail addresses in recordedBy / measuredBy / identifiedBy-style columns.
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
EMAIL_REDACTED = "<email redacted>"


def redact(text: str) -> str:
    return EMAIL_RE.sub(EMAIL_REDACTED, text)


def fmt_value(v: Any) -> str:
    s = fmt_time(v) if hasattr(v, "isoformat") else (fmt_num(v) if isinstance(v, (int, float)) else clean(v))
    s = redact(s)
    return s if len(s) <= VALUE_CHARS else s[: VALUE_CHARS - 1] + "…"


def neon_dtype(duck_type: str) -> str:
    t = duck_type.upper()
    if t.startswith(("TIMESTAMP", "DATE")):
        return "dateTime"
    if t in ("DOUBLE", "FLOAT", "REAL") or t.startswith("DECIMAL"):
        return "real"
    if t in NUMERIC_TYPES:
        return "unsigned integer" if t.startswith("U") else "signed integer"
    if t == "BOOLEAN":
        return "boolean"
    return "string"


# ---------------------------------------------------------------- site / product context

def site_line(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> str:
    """'SRER (Santa Rita Experimental Range NEON, AZ, domain D14 Desert Southwest; <TypeEco>)'.

    The habitat part is TypeEco for a terrestrial site and HABITAT for an aquatic one (never its TypeEco).
    """
    row = None
    if has_catalog:
        row = con.execute("SELECT siteName, stateCode, domainCode, domainName, siteProperties['TypeEco'], "
                          "siteProperties['HABITAT'] FROM neon.sites WHERE siteCode = ?", [site]).fetchone()
    name, state, dom, dname, eco, hab = row or (site, "?", "D00", "", None, None)

    def part(s: Any, bad: str) -> str:
        return " ".join(re.sub(f"[{re.escape(bad)}]", " ", clean(s)).split()) or "?"

    desc = hab if terms.is_aquatic(hab) else (eco or hab)
    habitat = part(desc or "habitat not recorded", ";()")
    return f"{site} ({part(name, ',()')}, {part(state, ',()')}, domain {dom} {part(dname, ';()')}; {habitat})"


def product_record(con: duckdb.DuckDBPyConnection, product: str, has_catalog: bool) -> dict[str, Any]:
    cols = ("productCode", "productName", "productDescription", "productAbstract", "productDesignDescription",
            "productScienceTeam", "productScienceTeamAbbr", "productStatus", "themes", "keywords",
            "latestRelease", "latestReleaseDoi", "productSensor")
    if has_catalog:
        row = con.execute(f"SELECT {', '.join(cols)} FROM neon.products WHERE productCode = ?", [product]).fetchone()
        if row:
            return dict(zip(cols, row, strict=True))
    return {"productCode": product, "productName": product}


def site_products(con: duckdb.DuckDBPyConnection, products: list[str] | None) -> list[dict[str, Any]]:
    """Products of the site lake (site_months ∪ table_catalog) with their month coverage."""
    rows = con.execute(
        "WITH p AS (SELECT DISTINCT productCode FROM lake.site_months UNION "
        "SELECT DISTINCT productCode FROM lake.table_catalog) "
        "SELECT p.productCode, count(m.month), min(m.month), max(m.month), "
        "count(*) FILTER (WHERE m.isProvisional), count(*) FILTER (WHERE m.isReleaseOnly), "
        "coalesce(bool_and(m.isExternal), false), list(DISTINCT m.release ORDER BY m.release) "
        "FROM p LEFT JOIN lake.site_months m USING (productCode) GROUP BY 1 ORDER BY 1").fetchall()
    out = []
    for r in rows:
        if products and r[0] not in products:
            continue
        out.append({"productCode": r[0], "nMonths": r[1], "firstMonth": r[2], "lastMonth": r[3],
                    "provisional": r[4], "releaseOnly": r[5], "isExternal": bool(r[6]),
                    "releases": [x for x in (r[7] or []) if x]})
    return out


def data_tables(con: duckdb.DuckDBPyConnection, product: str) -> list[dict[str, Any]]:
    cols = ("tableName", "neonTable", "kind", "nFiles", "nRows", "firstMonth", "lastMonth", "timeColumn")
    rows = con.execute(f"SELECT {', '.join(cols)} FROM lake.table_catalog WHERE productCode = ? "
                       "AND kind IN ('data', 'h5') ORDER BY neonTable, tableName", [product]).fetchall()
    out = [dict(zip(cols, r, strict=True)) for r in rows]
    return [t for t in out if site_lake.table_exists(con, t["tableName"])]


def lab_tables(con: duckdb.DuckDBPyConnection, product: str) -> list[dict[str, Any]]:
    """Stacked lab tables (``<dp>__lab_<table>``, table_catalog kind 'lab') of one product."""
    cols = ("tableName", "neonTable", "kind", "nFiles", "nRows", "firstMonth", "lastMonth", "timeColumn")
    rows = con.execute(f"SELECT {', '.join(cols)} FROM lake.table_catalog WHERE productCode = ? AND kind = 'lab' "
                       "ORDER BY neonTable, tableName", [product]).fetchall()
    out = [dict(zip(cols, r, strict=True)) for r in rows]
    return [t for t in out if site_lake.table_exists(con, t["tableName"])]


def lab_parts(name: str, product: str | None = None) -> tuple[str | None, str | None] | None:
    """(lab name, table) of a ``NEON.<Lab>.<table>.<ts>.csv`` file, else None (the shared classifier)."""
    try:
        from .. import filenames

        c = filenames.classify(name, product)
    except Exception:  # noqa: BLE001 - the classifier never raises by contract; be safe anyway
        return None
    return (c.lab_name, c.table) if c.kind == "lab" else None


def lab_file_lines(con: duckdb.DuckDBPyConnection, product: str) -> list[str]:
    """'- <Lab>: <table> (<n> files, published a to b)' for the product's lab files in ``files``."""
    if not site_lake.table_exists(con, "files"):
        return []
    have = site_lake.table_columns(con, "files")
    if "kind" not in have:
        return []
    rows = con.execute("SELECT name, publicationTimestamp FROM lake.files WHERE productCode = ? "
                       "AND (kind = 'lab' OR (kind = 'other' AND name LIKE 'NEON.%'))" if "publicationTimestamp" in have
                       else "SELECT name, NULL FROM lake.files WHERE productCode = ? AND (kind = 'lab' OR "
                       "(kind = 'other' AND name LIKE 'NEON.%'))", [product]).fetchall()
    groups: dict[tuple[str, str], list[Any]] = {}
    for name, pub in rows:
        lp = lab_parts(name, product)
        if lp:
            groups.setdefault((lp[0] or "?", lp[1] or "?"), []).append(pub)
    out = []
    for (lab, table), pubs in sorted(groups.items()):
        ts = sorted(str(fmt_time(p))[:10] for p in pubs if p is not None)
        span = f", published {ts[0]} to {ts[-1]}" if ts else ""
        out.append(f"- {clean(lab)}: {clean(table)} ({len(pubs)} files{span})")
    return out


def card_table_name(t: dict[str, Any]) -> str:
    """Table name used in the card header: the NEON table, or the lake suffix for HDF5 extracts."""
    if t["kind"] == "h5" and "__" in t["tableName"]:
        return t["tableName"].split("__", 1)[1]
    return t["neonTable"]


# ---------------------------------------------------------------- column metadata

def variables_meta(con: duckdb.DuckDBPyConnection, product: str, neon_table: str) -> dict[str, dict[str, Any]]:
    if not site_lake.table_exists(con, "variables"):
        return {}
    rows = con.execute("SELECT fieldName, description, dataType, units FROM lake.variables "
                       "WHERE productCode = ? AND tableName = ?", [product, neon_table]).fetchall()
    return {r[0]: {"description": r[1], "dataType": r[2], "units": r[3]} for r in rows}


def h5_objdesc(con: duckdb.DuckDBPyConnection, product: str) -> dict[str, str]:
    """The ``/objDesc`` term dictionary of the newest local HDF5 file of the product ({} when unavailable)."""
    row = con.execute("SELECT localRelPath FROM lake.files WHERE productCode = ? AND lower(name) LIKE '%.h5' "
                      "AND localRelPath IS NOT NULL ORDER BY month DESC, name DESC LIMIT 1", [product]).fetchone()
    if not row:
        return {}
    path = env.ROOT / row[0]
    try:
        import h5py

        with h5py.File(path, "r") as f:
            if "objDesc" not in f:
                return {}
            data = f["objDesc"][()]
    except (OSError, KeyError, ImportError) as exc:
        log.warning("objDesc of %s unreadable: %s", path.name, exc)
        return {}

    def txt(v: Any) -> str:
        return v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v)

    return {txt(r[0]).strip(): clean(txt(r[1])) for r in data if len(r) >= 2}


def h5_meta(con: duckdb.DuckDBPyConnection, product: str, site: str) -> dict[str, dict[str, Any]]:
    """Descriptions/units of the dp04 columns (site/h5.py naming) from /objDesc and h5_datasets units."""
    desc = h5_objdesc(con, product)
    units: dict[str, Counter[str]] = {}
    if site_lake.table_exists(con, "h5_datasets"):
        rows = con.execute("SELECT path, fields, unit FROM lake.h5_datasets WHERE productCode = ? "
                           "AND path LIKE '%/dp04/%' AND fields IS NOT NULL", [product]).fetchall()
        for path, fields, unit in rows:
            m = re.search(r"/dp04/(data|qfqm)/([^/]+)/([^/]+)$", path)
            if not m:
                continue
            group, a, b = m.groups()
            us = (unit or "").split(",")
            for i, fld in enumerate(fields):
                u = us[i].strip() if i < len(us) else ""
                col = _h5_column(group, a, b, fld)
                if col:
                    units.setdefault(col, Counter())[u] += 1
    out: dict[str, dict[str, Any]] = {}
    for col, cnt in units.items():
        u = cnt.most_common(1)[0][0]
        out[col] = {"units": None if u in ("", "NA", "-") else u, "description": _h5_description(col, desc)}
    for col in ("timeBgn", "timeEnd"):
        out[col] = {"units": None, "description": desc.get(col) or f"{col} of the aggregation period",
                    "dataType": "dateTime"}
    return out


def _h5_column(group: str, a: str, b: str, fld: str) -> str | None:
    if group == "data" and a.startswith("flux") and fld == "flux":
        return f"{a}{b.capitalize()}"
    if group == "qfqm" and a.startswith("flux") and fld == "qfFinl":
        return f"qfFinl{a[4:]}{b.capitalize()}"
    if group == "data" and a == "foot" and b == "stat" and fld not in ("timeBgn", "timeEnd"):
        return "foot" + fld[0].upper() + fld[1:]
    if group == "qfqm" and a == "foot" and fld == "qfFinl":
        return "qfFinlFoot"
    return None


def _h5_description(col: str, desc: dict[str, str]) -> str:
    m = re.match(r"^(flux(?:Co2|H2o|Temp|Mome))(Turb|Stor|Nsae)$", col)
    if m:
        flux, lvl = m.group(1), m.group(2).lower()
        return (f"{desc.get(flux, flux)}; level {lvl}: {desc.get(lvl, H5_LEVELS.get(lvl, lvl))} "
                f"(HDF5 dp04/data/{flux}/{lvl}, field flux)")
    m = re.match(r"^qfFinl(Co2|H2o|Temp|Mome)(Turb|Stor|Nsae)$", col)
    if m:
        return (f"{desc.get('qfFinl', 'Final quality flag')} (flux{m.group(1)} {m.group(2).lower()}; "
                f"HDF5 dp04/qfqm/flux{m.group(1)}/{m.group(2).lower()})")
    if col == "qfFinlFoot":
        return f"{desc.get('qfFinl', 'Final quality flag')} (footprint; HDF5 dp04/qfqm/foot/turb)"
    if col.startswith("foot"):
        fld = col[4].lower() + col[5:]
        return f"{desc.get(fld) or 'Footprint statistic ' + fld} (HDF5 dp04/data/foot/stat, field {fld})"
    return desc.get(col, "")


# ---------------------------------------------------------------- profiles

def _kind(duck_type: str) -> str:
    t = duck_type.upper()
    if t in NUMERIC_TYPES or t.startswith("DECIMAL"):
        return "numeric"
    if t.startswith(TEMPORAL_PREFIXES):
        return "temporal"
    return "text"


def profile_table(con: duckdb.DuckDBPyConnection, table: str) -> tuple[int, dict[str, str], tuple[str, str, int]]:
    """(rowCount, {column: profile}, (firstMonth, lastMonth, nMonths)) from real statistics."""
    cols = site_lake.table_columns(con, table)
    q = f"lake.{site_lake.ident(table)}"
    n_rows = int(con.execute(f"SELECT count(*) FROM {q}").fetchone()[0])
    approx = n_rows > APPROX_ROWS
    exprs: list[str] = []
    for c, t in cols.items():
        e = site_lake.ident(c)
        k = _kind(t)
        exprs.append(f"count({e})")
        exprs.append(f"approx_count_distinct({e})" if approx else f"count(DISTINCT {e})")
        if k == "numeric":
            exprs += [f"min({e})", f"quantile_cont({e}::DOUBLE, 0.5)", f"max({e})"]
        elif k == "temporal":
            exprs += [f"min({e})", "NULL", f"max({e})"]
        else:
            exprs += ["NULL", "NULL", "NULL"]
    stats = con.execute(f"SELECT {', '.join(exprs)} FROM {q}").fetchone() if cols else ()
    out: dict[str, str] = {}
    for i, (c, t) in enumerate(cols.items()):
        nn, nd, lo, med, hi = stats[i * 5: i * 5 + 5]
        k = _kind(t)
        tilde = "~" if approx else ""
        if not nn:
            out[c] = "all blank"
            continue
        blank = f"; {n_rows - nn} blank" if nn < n_rows else ""
        if k == "temporal":
            out[c] = f"datetime, n={nn}, {tilde}{nd} distinct, min={fmt_time(lo)}, max={fmt_time(hi)}{blank}"
            continue
        top = _top_values(con, q, c, t) if (k == "text" or nd <= FEW_DISTINCT) else []
        top_txt = ", ".join(f"{fmt_value(v)} ({cnt})" for v, cnt in top)
        if k == "numeric":
            s = f"numeric, n={nn}, {tilde}{nd} distinct, min={fmt_num(lo)}, median={fmt_num(med)}, max={fmt_num(hi)}"
            out[c] = s + (f"; top: {top_txt}" if top_txt else "") + blank
        elif nd == nn and nn > TOP_N:
            out[c] = f"{tilde}{nd} distinct (all unique); e.g. {', '.join(fmt_value(v) for v, _ in top)}{blank}"
        else:
            out[c] = f"{tilde}{nd} distinct; top: {top_txt}{blank}"
    months = ("?", "?", 0)
    if "month" in cols:
        r = con.execute(f"SELECT min(month), max(month), count(DISTINCT month) FROM {q}").fetchone()
        if r and r[0]:
            months = (r[0], r[1], int(r[2]))
    return n_rows, out, months


def _top_values(con: duckdb.DuckDBPyConnection, q: str, col: str, duck_type: str) -> list[tuple[Any, int]]:
    e = site_lake.ident(col)
    expr = f"CAST({e} AS VARCHAR)" if duck_type.upper().endswith("[]") or duck_type.upper() == "JSON" else e
    rows = con.execute(f"SELECT {expr} AS v, count(*) AS n FROM {q} WHERE {e} IS NOT NULL "
                       f"GROUP BY 1 ORDER BY n DESC, v LIMIT {TOP_N}").fetchall()
    return [(r[0], int(r[1])) for r in rows]


# ---------------------------------------------------------------- ontology context (read-only)

def ontology_context(site: str) -> dict[str, Any]:
    """Unit map, taxon map and the per-(product, table) taxonTypeCode chosen by `site terms`."""
    ctx: dict[str, Any] = {"units": {}, "taxa": {}, "types": {}}
    if not store.store_path().exists():
        return ctx
    try:
        with store.store(read_only=True, wait_s=30) as sc:
            ctx["units"] = {r[0]: r[1:] for r in sc.execute(
                "SELECT neonUnit, uoCurie, uoLabel, method, status FROM ref_unit_map").fetchall()}
            ctx["taxa"] = {(r[0], r[1]): r[2:] for r in sc.execute(
                "SELECT taxonTypeCode, taxonID, curie, label, method, status FROM ref_ncbitaxon_map").fetchall()}
            ctx["types"] = {(r[0], r[1]): r[2] for r in sc.execute(
                "SELECT productCode, tableName, taxonTypeCode FROM terms_taxon_types WHERE siteCode = ?",
                [site]).fetchall()}
    except duckdb.Error as exc:
        log.warning("ontology store unreadable (%s); cards carry no ontology status", str(exc).splitlines()[0])
    return ctx


def taxa_lines(con: duckdb.DuckDBPyConnection, product: str, onto: dict[str, Any], top: int = 10) -> list[str]:
    if not site_lake.table_exists(con, "file_taxa"):
        return []
    rows = con.execute(
        "SELECT tableName, taxonID, arg_max(scientificName, nRecords), arg_max(vernacularName, nRecords), "
        "arg_max(taxonRank, nRecords), sum(nRecords)::BIGINT FROM lake.file_taxa WHERE productCode = ? "
        "AND taxonID IS NOT NULL GROUP BY 1, 2 ORDER BY 6 DESC, 2", [product]).fetchall()
    if not rows:
        return []
    merged: dict[str, dict[str, Any]] = {}
    for tbl, tid, name, vern, rank, n in rows:
        m = merged.setdefault(tid, {"name": name, "vern": vern, "rank": rank, "n": 0, "tables": set(), "map": None})
        m["n"] += int(n)
        m["tables"].add(tbl)
        code = onto["types"].get((product, tbl))
        if m["map"] is None and code:
            m["map"] = onto["taxa"].get((code, tid))
    status = Counter((m["map"][2] if m["map"] and m["map"][3] == "ok" else (m["map"][3] if m["map"] else "not run"))
                     for m in merged.values())
    lines = [f"Distinct taxa: {len(merged)}; records: {sum(m['n'] for m in merged.values())}; NCBITaxon resolution: "
             + ", ".join(f"{k} {v}" for k, v in sorted(status.items())), ""]
    for tid, m in sorted(merged.items(), key=lambda kv: (-kv[1]["n"], kv[0]))[:top]:
        vern = f"; {clean(m['vern'])}" if m["vern"] else ""
        if m["map"]:
            curie, label, method, st = m["map"]
            res = f"{curie} '{label}' [{method}, {st}]" if curie else f"[{method}, {st}]"
        else:
            res = "[not resolved yet: run `neon-ducklake site terms`]"
        lines.append(f"- {clean(m['name'])} ({tid}{vern}; {m['rank'] or '?'}): {m['n']} records -> {res}")
    return lines


# ---------------------------------------------------------------- cards

def column_lines(con: duckdb.DuckDBPyConnection, t: dict[str, Any], product: str, site: str,
                 profiles: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    """Card column lines plus {unit: 'variables' | 'hdf5'} of the units they use."""
    var = variables_meta(con, product, t["neonTable"])
    h5 = h5_meta(con, product, site) if t["kind"] == "h5" else {}
    meta = {**h5, **var}
    lines = []
    units: dict[str, str] = {}
    for col, duck_type in site_lake.table_columns(con, t["tableName"]).items():
        m = meta.get(col) or {}
        added = ADDED_COLUMNS.get(col)
        desc = m.get("description") or (added[0] if added else "(no NEON description)")
        dtype = m.get("dataType") or (added[1] if added else neon_dtype(duck_type))
        unit = (m.get("units") or "").strip()
        unit = "-" if unit.upper() in ("", "NA", "NONE") else unit
        if unit != "-":
            units.setdefault(unit, "variables" if col in var else "hdf5")
        lines.append(f"- {clean(col)} | {clean(desc)} | {clean(dtype)} | {clean(unit)} | "
                     f"{clean(profiles.get(col, 'all blank'), pipes=False)}")
    return lines, units


def rows_line(n_rows: int, months: tuple[str, str, int]) -> str:
    return f"Rows: {n_rows}; months covered: {months[0]} to {months[1]} ({months[2]} distinct months)."


def anyjev_card(p: dict[str, Any], t: dict[str, Any], site_ln: str, n_rows: int, months: tuple[str, str, int],
                cols: list[str]) -> str:
    title = clean(p.get("productName") or p["productCode"]).replace(".", "")
    desc = clean(p.get("productDescription") or "")
    lines = [f"# Dataset: {p['productCode']} / {card_table_name(t)}",
             f"Product: {p['productCode']} — {title}. {desc}".rstrip(),
             f"Source: {SOURCE}",
             f"Sites: {site_ln}.",
             rows_line(n_rows, months),
             "",
             "## Columns (name | NEON description | type | unit | profile)",
             *cols]
    return "\n".join(lines) + "\n"


def product_card(p: dict[str, Any], sp: dict[str, Any], site_ln: str, units: dict[str, str],
                 tables: list[tuple[dict[str, Any], int, tuple[str, str, int], list[str], str]],
                 taxa: list[str], external: list[dict[str, Any]], onto: dict[str, Any],
                 labs: list[tuple[dict[str, Any], int, list[str]]] | None = None,
                 lab_files: list[str] | None = None) -> str:
    code = p["productCode"]
    team = clean(p.get("productScienceTeam") or p.get("productScienceTeamAbbr") or "?")
    rel = ", ".join(sp["releases"]) or "-"
    lines = [f"# {code} — {clean(p.get('productName') or code)}", "",
             f"Code: {code}", f"Team: {team}", f"Status: {clean(p.get('productStatus') or '?')}",
             f"Site: {site_ln}",
             f"Themes: {'; '.join(clean(x) for x in (p.get('themes') or [])) or '-'}",
             f"Keywords: {'; '.join(clean(x) for x in (p.get('keywords') or [])) or '-'}",
             (f"Months at this site: {sp['nMonths']} ({sp['firstMonth'] or '?'} to {sp['lastMonth'] or '?'}); "
              f"provisional {sp['provisional']}; release-only {sp['releaseOnly']}; releases: {rel}")]
    if p.get("latestRelease"):
        doi = f" (doi:{p['latestReleaseDoi']})" if p.get("latestReleaseDoi") else ""
        lines.append(f"Latest release: {p['latestRelease']}{doi}")
    if p.get("productSensor"):
        lines.append(f"Sensor: {clean(p['productSensor'])}")
    lines += ["", "## Description", excerpt(p.get("productDescription")) or "-",
              "", "## Abstract (excerpt)", excerpt(p.get("productAbstract")) or "-",
              "", "## Design (excerpt)", excerpt(p.get("productDesignDescription")) or "-"]
    if external:
        lines += ["", "## External data"]
        for e in external:
            months = e.get("months") or []
            span = f"; months {months[0]} to {months[-1]} ({len(months)})" if months else ""
            lines.append(f"- {clean(e.get('name'))} ({clean(e.get('type'))}): {e.get('url')}{span}")
        lines.append("")
        lines.append("Data are hosted outside NEON; no files are downloaded or stacked for this product.")
    if units:
        lines += ["", "## Units (UO)"]
        for u, src in sorted(units.items()):
            m = onto["units"].get(u)
            if src == "hdf5":
                res = "[HDF5 attribute unit; not a NEON variables unit, not mapped by `site terms`]"
            elif m is None:
                res = "[not mapped yet: run `neon-ducklake site terms`]"
            elif m[0]:
                res = f"{m[0]} '{m[1]}' [{m[2]}, {m[3]}]"
            else:
                res = f"[{m[2]}, {m[3]}]"
            lines.append(f"- {u} -> {res}")
    for t, n_rows, months, cols, extra in tables:
        lines += ["", f"## Table {card_table_name(t)} (lake table {t['tableName']}; {t['kind']})",
                  rows_line(n_rows, months)]
        if extra:
            lines.append(extra)
        lines += ["", "### Columns (name | NEON description | type | unit | profile)", *cols]
    if lab_files:
        lines += ["", "## Lab files (external laboratory results: NEON.<Lab>.<table>.<timestamp>.csv)", *lab_files]
    for t, n_rows, cols in labs or []:
        lines += ["", f"## Lab table {t['neonTable']} (lake table {t['tableName']}; lab)",
                  f"Rows: {n_rows} (distinct lab records); files: {t['nFiles']}",
                  "", "### Columns (name | NEON description | type | unit | profile)", *cols]
    if taxa:
        lines += ["", "## Taxa (top 10 by records)", *taxa]
    return "\n".join(lines) + "\n"


def positions_line(con: duckdb.DuckDBPyConnection, product: str, t: dict[str, Any]) -> str:
    if not site_lake.table_exists(con, "file_stats"):
        return ""
    rows = con.execute("SELECT DISTINCT hor || '.' || ver FROM lake.file_stats WHERE productCode = ? "
                       "AND lakeTable = ? AND hor IS NOT NULL ORDER BY 1", [product, t["tableName"]]).fetchall()
    pos = [r[0] for r in rows if r[0]]
    if not pos:
        return f"Files: {t['nFiles']}"
    return f"Sensor positions (HOR.VER): {', '.join(pos)} ({len(pos)}); files: {t['nFiles']}"


def grammar_problems(text: str, n_columns: int) -> list[str]:
    """Self-check against the mesa-anyjev grammar (a copy of its regexes)."""
    probs: list[str] = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not any(_HEADER.match(ln) for ln in lines):
        probs.append("header")
    if not any(_PRODUCT.match(ln) for ln in lines):
        probs.append("product line")
    site = next((ln for ln in lines if ln.startswith("Sites:")), "")
    if len(list(_SITE.finditer(site))) != 1:
        probs.append("site line")
    if not any(_ROWS.match(ln) for ln in lines):
        probs.append("rows line")
    n = sum(1 for ln in lines if _COLUMN.match(ln))
    if n != n_columns:
        probs.append(f"columns {n} != {n_columns}")
    if "not profiled" in text:
        probs.append("'not profiled'")
    return probs


def anyjev_parse(paths: list[Path]) -> dict[str, Any] | None:
    """Parse cards with mesa-anyjev's own ``parse_card`` (in-process, else its venv); None when unavailable."""
    if not paths:
        return {}
    code = ("import json,sys\nfrom mesa_anyjev.cards import load_card\nout={}\n"
            "for p in json.loads(sys.stdin.read()):\n"
            "    try:\n        c=load_card(p); out[p]={'ok':True,'columns':len(c.columns),'rows':c.rows,"
            "'sites':[s.code for s in c.sites],'product':c.product_code,'table':c.table,'from':c.months_from,"
            "'to':c.months_to}\n"
            "    except Exception as e:\n        out[p]={'ok':False,'error':str(e)[:200]}\n"
            "print(json.dumps(out))\n")
    try:
        import mesa_anyjev.cards  # noqa: F401

        python = sys.executable
    except ImportError:
        if not ANYJEV_PYTHON.exists():
            return None
        python = str(ANYJEV_PYTHON)
    cmd = [python, "-c", code]
    try:
        r = subprocess.run(cmd, input=json.dumps([str(p) for p in paths]), capture_output=True, text=True,
                           timeout=300, check=True)
        return json.loads(r.stdout)
    except (subprocess.SubprocessError, OSError, ValueError) as exc:
        log.warning("mesa-anyjev parser unavailable: %s", str(exc)[:200])
        return None


def build_cards(con: duckdb.DuckDBPyConnection, site: str, products: list[str] | None,
                has_catalog: bool) -> dict[str, Any]:
    paths = site_lake.site_paths(site)
    out_dir = paths.site_dir / "cards"
    any_dir = out_dir / "anyjev"
    any_dir.mkdir(parents=True, exist_ok=True)
    site_ln = site_line(con, site, has_catalog)
    onto = ontology_context(site)
    summary: dict[str, Any] = {"products": [], "anyjev": [], "problems": {}}
    for sp in site_products(con, products):
        code = sp["productCode"]
        p = product_record(con, code, has_catalog)
        ext_rows = con.execute("SELECT name, type, url, months FROM lake.external_links WHERE productCode = ? "
                               "ORDER BY url", [code]).fetchall() if site_lake.table_exists(con, "external_links") else []
        external = [{"name": r[0], "type": r[1], "url": r[2], "months": r[3]} for r in ext_rows]
        tables = []
        units: dict[str, str] = {}
        for t in data_tables(con, code):
            n_rows, profiles, months = profile_table(con, t["tableName"])
            cols, used = column_lines(con, t, code, site, profiles)
            for u, src in used.items():
                units.setdefault(u, src)
            tables.append((t, n_rows, months, cols, positions_line(con, code, t)))
            text = anyjev_card(p, t, site_ln, n_rows, months, cols)
            path = any_dir / f"{code}.{card_table_name(t)}.md"
            _write(path, text)
            probs = grammar_problems(text, len(cols))
            if probs:
                summary["problems"][str(path.relative_to(env.ROOT))] = probs
            summary["anyjev"].append({"path": str(path), "product": code, "table": card_table_name(t),
                                      "rows": n_rows, "columns": len(cols), "months": list(months[:2])})
        labs = []
        for t in lab_tables(con, code):
            n_rows, profiles, _months = profile_table(con, t["tableName"])
            cols, used = column_lines(con, t, code, site, profiles)
            for u, src in used.items():
                units.setdefault(u, src)
            labs.append((t, n_rows, cols))
        taxa = taxa_lines(con, code, onto)
        text = product_card(p, sp, site_ln, units, tables, taxa, external, onto, labs, lab_file_lines(con, code))
        path = out_dir / f"{code}.md"
        _write(path, text)
        summary["products"].append({"product": code, "path": str(path), "tables": len(tables),
                                    "labTables": len(labs), "external": bool(external) and not tables,
                                    "chars": len(text)})
    return summary


# ---------------------------------------------------------------- the site card

SITE_CARD = "_site.md"
MAX_CARD_PRODUCTS = 80


def site_card_path(site: str) -> Path:
    return site_lake.site_paths(site.upper()).site_dir / "cards" / SITE_CARD


def heuristic_proposals(site: str) -> dict[str, dict[str, Any]]:
    """{role: {envoCurie, envoLabel, rule}} of the ``site terms`` heuristic (read-only; {} when unavailable)."""
    out: dict[str, dict[str, Any]] = {}
    if not store.store_path().exists():
        return out
    try:
        with store.store(read_only=True, wait_s=30) as sc:
            for role, curie, label, rule in sc.execute(
                    "SELECT role, envoCurie, envoLabel, rule FROM site_envo_proposals WHERE siteCode = ? "
                    "AND source = 'heuristic'", [site]).fetchall():
                out[role] = {"envoCurie": curie, "envoLabel": label, "rule": rule}
    except duckdb.Error as exc:
        log.warning("ontology store unreadable (%s); the site card carries no heuristic proposal",
                    str(exc).splitlines()[0])
    return out


def site_card(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool,
              heuristic: dict[str, dict[str, Any]] | None = None) -> str:
    """The text of ``cards/_site.md`` (see the module docstring)."""
    ctx = terms.site_context(con, site, has_catalog)
    aquatic = terms.is_aquatic(ctx.get("habitat"))
    name = ctx.get("siteName") or site
    lines = [f"# Site: {site} — {clean(name)}", "",
             f"Code: {site}", f"Name: {clean(name)}",
             f"HABITAT: {clean(ctx.get('habitat') or 'not recorded')}"]
    if ctx.get("siteType"):
        lines.append(f"Site type: {clean(ctx['siteType'])}")
    if ctx.get("domainCode"):
        lines.append(f"Domain: {ctx['domainCode']} {clean(ctx.get('domainName') or '')}".rstrip())
    if ctx.get("stateCode"):
        lines.append(f"State: {ctx['stateCode']}" + (f" ({clean(ctx['stateName'])})" if ctx.get("stateName") else ""))
    lat, lon, elev = ctx.get("siteLatitude"), ctx.get("siteLongitude"), ctx.get("siteElevation")
    if lat is not None and lon is not None:
        lines.append(f"Location: latitude {fmt_num(lat)}, longitude {fmt_num(lon)}"
                     + (f"; elevation {fmt_num(round(float(elev), 1))} m" if elev is not None else ""))
    desc = excerpt(ctx.get("siteDescription"))
    if desc and desc.lower() != clean(name).lower():
        lines.append(f"Description: {desc}")
    if not aquatic:
        lines += ["", "## Terrestrial ecosystem",
                  f"TypeEco (NEON's ecosystem type): {clean(ctx.get('typeEco') or 'not recorded')}"]
        if ctx.get("nlcd"):
            shares = "; ".join(f"{x['nlcdClass']} {100 * (x['share'] or 0):.1f}%" for x in ctx["nlcd"]
                               if (x.get("share") or 0) >= 0.01)
            lines.append(f"NLCD land cover ({ctx.get('nlcdYear') or '?'}, NEON TOS site area): {shares or '-'}")
        else:
            lines.append("NLCD land cover: not available")
    prods = site_products(con, None)
    teams: Counter[str] = Counter()
    plines = []
    for sp in prods:
        rec = product_record(con, sp["productCode"], has_catalog)
        if rec.get("productScienceTeamAbbr"):
            teams[rec["productScienceTeamAbbr"]] += 1
        plines.append(f"- {sp['productCode']} {clean(rec.get('productName') or '')}".rstrip())
    lines += ["", "## Data at this site",
              f"Products: {len(prods)} (non-AOP, basic package)"
              + (f"; science teams: {', '.join(f'{k} {v}' for k, v in sorted(teams.items()))}" if teams else "")]
    lines += plines[:MAX_CARD_PRODUCTS]
    if len(plines) > MAX_CARD_PRODUCTS:
        lines.append(f"- … {len(plines) - MAX_CARD_PRODUCTS} more")
    heuristic = heuristic if heuristic is not None else heuristic_proposals(site)
    lines += ["", "## Heuristic proposal (a deterministic hint from `site terms`; verify it, do not copy it)"]
    for role, title in (("biome", "biome"), ("feature", "environmental feature (water body)")):
        if role == "feature" and not aquatic:
            continue
        h = heuristic.get(role) or {}
        what = f"{h['envoCurie']} {h['envoLabel']}" if h.get("envoCurie") else "no proposal"
        lines.append(f"- {title}: {what}" + (f" (rule {h['rule']})" if h.get("rule") else ""))
    return "\n".join(lines) + "\n"


def write_site_card(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> Path:
    path = site_card_path(site)
    _write(path, site_card(con, site, has_catalog))
    return path


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    tmp = path.with_name(path.name + ".part")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def main(site: str, products: list[str] | None = None) -> int:
    site = site.upper()
    con = site_lake.open_site_lake(site, read_only=True)
    try:
        has_catalog = site_lake.attach_catalog(con)
        summary = build_cards(con, site, products, has_catalog)
        summary["siteCard"] = str(write_site_card(con, site, has_catalog))
    finally:
        con.close()
    parsed = anyjev_parse([Path(a["path"]) for a in summary["anyjev"]])
    bad = dict(summary["problems"])
    if parsed is None:
        print("mesa-anyjev parser not available; only the built-in grammar check ran")
    else:
        for a in summary["anyjev"]:
            r = parsed.get(a["path"]) or {"ok": False, "error": "not parsed"}
            if not r["ok"] or r["columns"] != a["columns"] or r["rows"] != a["rows"] or r["sites"] != [site]:
                bad[a["path"]] = [f"parse_card: {r}"]
    for sp in summary["products"]:
        kind = "external" if sp["external"] else f"{sp['tables']} tables"
        print(f"  {sp['product']:<16} {kind:<10} {sp['chars']:>7} chars  {sp['path']}")
    for a in summary["anyjev"]:
        ok = "parse_card ok" if parsed and parsed.get(a["path"], {}).get("ok") else "grammar ok"
        if a["path"] in bad or str(Path(a["path"]).relative_to(env.ROOT)) in bad:
            ok = "PROBLEM"
        print(f"  anyjev {a['product']}.{a['table']:<22} rows={a['rows']:<8} cols={a['columns']:<3} {ok}")
    print(f"cards {site}: {len(summary['products'])} product cards, {len(summary['anyjev'])} anyjev table cards, "
          f"site card {summary['siteCard']}" + (f"; {len(bad)} with problems" if bad else ""))
    for k, v in bad.items():
        print(f"  problem {k}: {v}")
    env.write_json(site_lake.site_paths(site).reports_dir / "cards.json",
                   {**summary, "anyjevParse": parsed, "at": env.utcnow().isoformat()})
    return 1 if bad else 0
