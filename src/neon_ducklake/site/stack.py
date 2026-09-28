"""`site stack <SITE>` — neonUtilities ``stackByTable`` semantics into the site lake.

Inputs are the journal's downloaded files (``site_files.dl_status='ok'``, minus ``fetch_superseded``), named by
the shared classifier :func:`neon_ducklake.filenames.classify` (the journal kind wins unless it is missing or
``other``; a ``NEON.<Lab>.<table>.<ts>.csv`` name is always kind ``lab``). Per product:

* supporting tables in one snapshot: ``variables`` (latest definition per table/field by publication
  timestamp) + ``variables_history`` (distinct definitions), ``sensor_positions`` (legacy headers aliased,
  sensor lat/lon/elevation from reference + offsets, catalog ``locations``/``location_history`` joins when
  the catalog lake is attachable), ``categorical_codes``/``validation``/``science_review_flags`` (distinct
  rows), ``eml_index``, ``readme_index``;
* ``sensor_positions`` schema evolution: columns outside the known schema (e.g. ``effectiveStartDateTime``)
  are accepted and unioned by name (``*DateTime`` -> TIMESTAMPTZ like the other timestamps, else VARCHAR; a
  name that collides with a derived column gets a ``_neon`` suffix); every header group is recorded in
  ``schema_drift`` (``tableName='sensor_positions'``, ``unknownColumns``, first/last publication). The
  supporting unit fails only when a header group lacks a REQUIRED column (``SENSOR_REQUIRED``: HOR.VER,
  sensorLocationID, reference latitude/longitude/elevation, or their legacy aliases); the report is
  ``reports/sensor_positions_missing_required_<product>.json``;
* one snapshot per data table: CSVs grouped by header line, one ``read_csv`` per group (all VARCHAR,
  ``auto_detect=false``, ``filename=true``) ``UNION ALL BY NAME`` into an on-disk staging table; cast
  failure rates computed on staging; ``CREATE OR REPLACE TABLE`` with ``TRY_CAST`` types from
  ``variables`` (a column with >0.1% failures stays VARCHAR and is logged in ``cast_errors``); added
  siteID/domainID/month/release/publicationDate/sourceFile (+ horizontalPosition/verticalPosition/tmi for
  IS); ``uid`` tables keep the newest publication per uid; ``file_stats``/``file_taxa`` computed before
  dedup; ``schema_drift`` per header group; ``table_catalog`` with the input fingerprint;
* dateTime typing follows each column's NEON ``pubFormat`` (:func:`java_to_strptime`): the Java pattern is
  converted to a strptime pattern (``yyyy``->%Y, ``MM``->%m, ``dd``->%d, ``HH``->%H, ``mm``->%M, ``ss``->%S,
  quoted literals kept, a ``(floor)``/``(round)``/``(ceil)`` suffix dropped) and tried first, the generic
  list (``GENERIC_DT``) after it per value; pubFormat ``yyyy`` -> INTEGER year, ``yyyy-MM`` -> DATE of the
  first day of the month, a date-only pattern -> DATE, a pattern with hours -> TIMESTAMPTZ (UTC); NA or an
  unknown pattern -> the generic list (TIMESTAMPTZ). ``file_stats.timeMin/timeMax`` use the same pattern;
* one snapshot per lab table: a product's ``lab`` files (``NEON.<Lab>.<table>.<ts>.csv``) stacked into
  ``<dp>__lab_<table>``: the file columns (typed from the product's ``variables`` when they describe that
  table, with the same 0.1% rule and ``cast_errors``; otherwise auto-typed: BIGINT, DOUBLE, DATE or
  TIMESTAMPTZ when every value casts, else VARCHAR) + ``labName``, ``publicationDate``, ``sourceFile``,
  deduplicated to distinct rows (ignoring sourceFile/publicationDate) keeping the newest publication;
  ``table_catalog.kind='lab'``, ``schema_drift`` per header group (no ``file_stats`` rows);
* DP4.00200.001 HDF5 via :mod:`site.h5`.

A unit whose input fingerprint (sorted name + md5, plus the variables files for data tables, plus
``STACK_VERSION``) matches ``table_catalog.inputSha256`` is skipped unless ``--force``.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from .. import env, state
from . import site_lake
from .site_lake import ident, sql_str

log = logging.getLogger("neon_ducklake.site.stack")

STACK_VERSION = "2"  # 2: pubFormat-derived dateTime parsing, sensor_positions schema evolution, lab tables
TIME_COLUMNS = ("startDateTime", "collectDate", "startDate", "eventDate", "date", "setDate", "endDateTime")
# generic dateTime formats: the fallback when pubFormat is NA/unknown, and tried after the pubFormat pattern
GENERIC_DT = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%HZ", "%Y-%m-%dT%H:%M:%S.%gZ", "%Y-%m-%d")
DT_FORMATS = "[" + ", ".join(sql_str(f) for f in GENERIC_DT) + "]"
# Java SimpleDateFormat fields used by NEON pubFormat -> strptime
JAVA_FIELDS = {"yyyy": "%Y", "yy": "%y", "MM": "%m", "dd": "%d", "HH": "%H", "mm": "%M", "ss": "%S", "SSS": "%g"}
PUB_SUFFIX_RE = re.compile(r"\s*\([^()']*\)\s*$")  # '(floor)', '(round)', '(ceil)' …
NO_PATTERN = frozenset({"", "NA", "ASIS"})
NULLSTR = "['', 'NA']"
CAST_FAIL_MAX = 0.001
SUPPORT_KINDS = ("variables", "sensor_positions", "categorical_codes", "validation", "science_review_flags",
                 "eml", "readme")
DISTINCT_SUPPORT = {"categorical_codes": "categorical_codes", "validation": "validation",
                    "science_review_flags": "science_review_flags"}
SENSOR_COLUMNS = (
    "HOR.VER", "sensorLocationID", "sensorLocationDescription", "positionStartDateTime", "positionEndDateTime",
    "referenceLocationID", "referenceLocationIDDescription", "referenceLocationIDStartDateTime",
    "referenceLocationIDEndDateTime", "xOffset", "yOffset", "zOffset", "pitch", "roll", "azimuth",
    "locationReferenceLatitude", "locationReferenceLongitude", "locationReferenceElevation", "eastOffset",
    "northOffset", "xAzimuth", "yAzimuth")
SENSOR_ALIASES = {
    "name": "sensorLocationID", "description": "sensorLocationDescription", "start": "positionStartDateTime",
    "end": "positionEndDateTime", "referenceName": "referenceLocationID",
    "referenceDescription": "referenceLocationIDDescription", "referenceStart": "referenceLocationIDStartDateTime",
    "referenceEnd": "referenceLocationIDEndDateTime", "referenceLatitude": "locationReferenceLatitude",
    "referenceLongitude": "locationReferenceLongitude", "referenceElevation": "locationReferenceElevation"}
SENSOR_REQUIRED = ("HOR.VER", "sensorLocationID", "locationReferenceLatitude", "locationReferenceLongitude",
                   "locationReferenceElevation")  # after legacy aliasing
# output columns stack_sensor_positions derives itself: a NEON column with one of these names gets '_neon'
SENSOR_DERIVED = frozenset({
    "productCode", "month", "release", "publicationDate", "sourceFile", "horizontalPosition", "verticalPosition",
    "monthStart", "sensorLatitude", "sensorLongitude", "sensorElevation", "locationType", "locationLatitude",
    "locationLongitude", "locationElevationCatalog", "historyLatitude", "historyLongitude", "historyElevation",
    "filename", "__rid"})
SENSOR_TIMES = ("positionStartDateTime", "positionEndDateTime", "referenceLocationIDStartDateTime",
                "referenceLocationIDEndDateTime")
SENSOR_DOUBLES = ("xOffset", "yOffset", "zOffset", "pitch", "roll", "azimuth", "locationReferenceLatitude",
                  "locationReferenceLongitude", "locationReferenceElevation", "eastOffset", "northOffset",
                  "xAzimuth", "yAzimuth")
VARIABLES_COLUMNS = ("tableName", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat",
                     "primaryKey", "categoricalCodeName")
EML_RANGE_RE = re.compile(r"\.EML\.(\d{8})-(\d{8})\.")


class StackError(RuntimeError):
    """A unit that cannot be stacked (e.g. sensor_positions headers without a required column)."""


@dataclass
class SrcFile:
    product: str
    month: str
    name: str
    release: str | None
    kind: str
    table: str | None
    hor: str | None
    ver: str | None
    tmi: str | None
    ext: str | None
    domain: str | None
    path: Path
    pub_ts: datetime | None  # file publication timestamp (UTC-aware)
    md5: str | None
    size: int | None
    md5_verified: bool | None
    local_path: str
    lab_name: str | None = None  # kind 'lab' only: <Lab> of NEON.<Lab>.<table>.<ts>.csv


@dataclass
class Ctx:
    site: str
    con: duckdb.DuckDBPyConnection
    force: bool
    has_catalog: bool
    catalog_tables: set[str]
    fingerprints: dict[tuple[str, str], str]
    domain: str | None = None
    results: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    n_stage: int = 0


# ---------------------------------------------------------------- inputs

def input_files(site: str, products: list[str] | None = None) -> dict[str, list[SrcFile]]:
    """Downloaded, non-superseded files per product (from the journal), classified by the shared classifier."""
    from ..filenames import classify
    from .fetch import SUPERSEDED_SCHEMA

    con = state.connect(state.DB_PATH)
    try:
        con.executescript(SUPERSEDED_SCHEMA)
        where, args = "f.site=? AND f.dl_status='ok' AND s.name IS NULL", [site]
        if products:
            where += f" AND f.product IN ({','.join('?' * len(products))})"
            args += list(products)
        rows = con.execute(
            "SELECT f.* FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND "
            f"s.month=f.month AND s.name=f.name WHERE {where} ORDER BY f.product, f.month, f.name", args).fetchall()
    finally:
        con.close()
    out: dict[str, list[SrcFile]] = defaultdict(list)
    for r in rows:
        path = env.ROOT / r["local_path"]
        if not path.exists():
            log.warning("missing local file %s (journal says ok); skipped", r["local_path"])
            continue
        p = classify(r["name"], product_hint=r["product"])
        pub = None
        if p.timestamp:
            pub = datetime.strptime(p.timestamp + "+0000", "%Y%m%dT%H%M%SZ%z")
        kind = r["kind"]
        if p.kind == "lab" or kind in (None, "", "other"):  # journals written before the shared classifier
            kind = p.kind
        out[r["product"]].append(SrcFile(
            product=r["product"], month=r["month"], name=r["name"], release=r["release"], kind=kind,
            table=p.table, hor=p.hor, ver=p.ver, tmi=p.tmi, ext=p.ext, domain=p.domain, path=path, pub_ts=pub,
            md5=r["dl_md5"] or r["md5"], size=r["dl_bytes"] or r["size"],
            md5_verified=None if r["md5_verified"] is None else bool(r["md5_verified"]), local_path=r["local_path"],
            lab_name=p.lab_name))
    return out


def fingerprint(files: list[SrcFile], extra: str = "") -> str:
    h = hashlib.sha256(f"stack-v{STACK_VERSION}\n{extra}\n".encode())
    for line in sorted(f"{f.name}\t{f.md5 or ''}" for f in files):
        h.update(line.encode() + b"\n")
    return h.hexdigest()


def lake_table_name(product: str, table: str) -> str:
    return product.replace(".", "_").lower() + "__" + table.lower()


def header_of(path: Path) -> str:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        return fh.readline().rstrip("\r\n")


def parse_header(line: str) -> list[str]:
    cols = next(csv.reader([line])) if line else []
    seen: dict[str, int] = {}
    out = []
    for c in cols:
        c = c.strip()
        if c in seen:  # duplicate header names would break read_csv(columns=...)
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 1
        out.append(c)
    return out


def group_by_header(files: list[SrcFile]) -> list[tuple[str, list[str], list[SrcFile]]]:
    """[(header line, columns, files)] — most common header first; empty files dropped."""
    groups: dict[str, list[SrcFile]] = defaultdict(list)
    for f in files:
        line = header_of(f.path)
        if line:
            groups[line].append(f)
        else:
            log.warning("empty file %s skipped", f.name)
    return [(h, parse_header(h), fs) for h, fs in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))]


def group_rename(cols: list[str], rename: dict[str, str]) -> dict[str, str]:
    """Source column -> output name for one header group; an alias whose target the group already has is kept."""
    out = {}
    for c in cols:
        tgt = rename.get(c, c)
        out[c] = c if tgt != c and tgt in cols else tgt
    return out


def read_groups_sql(groups: list[tuple[str, list[str], list[SrcFile]]],
                    rename: dict[str, str] | None = None, lenient: bool = False) -> tuple[str, list[list[str]]]:
    """``SELECT … FROM read_csv($n …) UNION ALL BY NAME …`` over header groups (all VARCHAR + filename).

    ``lenient`` (the retry after a strict read failed) pads short rows and relaxes RFC-4180 strictness.
    """
    extra = ", strict_mode=false, null_padding=true" if lenient else ""
    parts, params = [], []
    for i, (_h, cols, fs) in enumerate(groups, start=1):
        spec = "{" + ", ".join(f"{sql_str(c)}: 'VARCHAR'" for c in cols) + "}"
        names = group_rename(cols, rename or {})
        sel = ", ".join(f"{ident(c)} AS {ident(names[c])}" for c in cols)
        parts.append(f"SELECT {sel}, filename FROM read_csv(${i}, auto_detect=false, header=true, delim=',', "
                     f"quote='\"', escape='\"', columns={spec}, filename=true, nullstr={NULLSTR}{extra})")
        params.append([str(f.path) for f in fs])
    return " UNION ALL BY NAME ".join(parts), params


def register_fmeta(con: duckdb.DuckDBPyConnection, site: str, domain: str | None, files: list[SrcFile],
                   name: str = "fmeta") -> None:
    import pyarrow as pa

    tbl = pa.table({
        "filename": pa.array([str(f.path) for f in files]),
        "sourceFile": pa.array([f.name for f in files]),
        "month": pa.array([f.month for f in files]),
        "release": pa.array([f.release for f in files]),
        "publicationDate": pa.array([f.pub_ts for f in files], type=pa.timestamp("us", tz="UTC")),
        "siteID": pa.array([site] * len(files)),
        "domainID": pa.array([f.domain or domain for f in files]),
        "horizontalPosition": pa.array([f.hor for f in files]),
        "verticalPosition": pa.array([f.ver for f in files]),
        "tmi": pa.array([f.tmi for f in files]),
        "md5Verified": pa.array([f.md5_verified for f in files], type=pa.bool_()),
        "localRelPath": pa.array([f.local_path for f in files]),
        "size": pa.array([f.size for f in files], type=pa.int64()),
    })
    con.register(name, tbl)


# ---------------------------------------------------------------- typing

@dataclass(frozen=True)
class PubFormat:
    """A NEON dateTime ``pubFormat`` as a strptime pattern plus the lake type it maps to.

    ``kind``: ``year`` (INTEGER), ``month`` (DATE, first day), ``date`` (DATE), ``timestamp`` (TIMESTAMPTZ)
    or ``unknown`` (NA / unparseable pattern: the generic format list decides, as TIMESTAMPTZ).
    """

    kind: str
    pattern: str | None = None


def java_to_strptime(pub_format: str | None) -> PubFormat:
    """``yyyy-MM-dd'T'HH'Z'(floor)`` -> PubFormat('timestamp', '%Y-%m-%dT%HZ').

    Java fields map through ``JAVA_FIELDS``; quoted literals are kept (``''`` is a quote); other characters are
    literal; trailing rounding suffixes such as ``(floor)``/``(round)``/``(ceil)`` are dropped. Any other letter
    field (e.g. ``D``, ``X``, ``YYYY``) makes the pattern ``unknown``.
    """
    s = (pub_format or "").strip()
    prev = None
    while prev != s:
        prev, s = s, PUB_SUFFIX_RE.sub("", s).strip()
    if s.upper() in NO_PATTERN:
        return PubFormat("unknown")
    out: list[str] = []
    fields: set[str] = set()
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'":
            j = s.find("'", i + 1)
            if j < 0:
                return PubFormat("unknown")
            out.append((s[i + 1:j] or "'").replace("%", "%%"))
            i = j + 1
        elif ch.isalpha():
            j = i
            while j < len(s) and s[j] == ch:
                j += 1
            spec = JAVA_FIELDS.get(s[i:j])
            if spec is None:
                return PubFormat("unknown")
            out.append(spec)
            fields.add(spec)
            i = j
        else:
            out.append("%%" if ch == "%" else ch)
            i += 1
    if not fields & {"%Y", "%y"}:
        return PubFormat("unknown")
    if fields & {"%H", "%M", "%S", "%g"}:
        kind = "timestamp"
    elif "%d" in fields:
        kind = "date"
    elif "%m" in fields:
        kind = "month"
    else:
        kind = "year"
    return PubFormat(kind, "".join(out))


def dt_formats(first: str | None = None) -> str:
    """SQL list literal: ``first`` (a pubFormat-derived pattern) then the generic formats."""
    fmts = ([first] if first else []) + [f for f in GENERIC_DT if f != first]
    return "[" + ", ".join(sql_str(f) for f in fmts) + "]"


def ts_expr(x: str, pattern: str | None = None) -> str:
    return f"timezone('UTC', try_strptime({x}, {dt_formats(pattern)}))"


def cast_for(col: str, data_type: str | None, pub_format: str | None, qual: str = "s.") -> tuple[str, str]:
    """(SQL expression over ``<qual>col``, target type) from NEON variables dataType/pubFormat."""
    x = qual + ident(col)
    dt = (data_type or "").strip().lower()
    if dt == "real":
        return f"TRY_CAST({x} AS DOUBLE)", "DOUBLE"
    if dt in ("integer", "signed integer", "unsigned integer"):
        d = f"TRY_CAST({x} AS DOUBLE)"
        # DuckDB rounds '1.5' -> 2 in a VARCHAR->BIGINT cast: accept integral values only
        expr = (f"CASE WHEN {d} = trunc({d}) AND abs({d}) < 9e18 "
                f"THEN coalesce(TRY_CAST({x} AS BIGINT), CAST({d} AS BIGINT)) END")
        return expr, "BIGINT"
    if dt == "datetime":
        pf = java_to_strptime(pub_format)
        if pf.kind == "year":  # '2021' -> 2021 (a full generic date/time value still yields its year)
            return (f"CAST(coalesce(CASE WHEN regexp_full_match(trim({x}), '[0-9]{{4}}') THEN "
                    f"TRY_CAST(trim({x}) AS INTEGER) END, year(try_strptime({x}, {DT_FORMATS}))) AS INTEGER)"
                    ), "INTEGER"
        if pf.kind == "month":  # '2024-06' -> DATE '2024-06-01'
            return f"CAST(date_trunc('month', try_strptime({x}, {dt_formats(pf.pattern)})) AS DATE)", "DATE"
        if pf.kind == "date":
            return f"CAST(try_strptime({x}, {dt_formats(pf.pattern)}) AS DATE)", "DATE"
        if pf.kind == "timestamp":
            return ts_expr(x, pf.pattern), "TIMESTAMPTZ"
        if pub_format and pub_format.strip().upper() not in NO_PATTERN and "HH" not in pub_format:
            return f"CAST(try_strptime({x}, {DT_FORMATS}) AS DATE)", "DATE"  # unknown date-only pattern
        return ts_expr(x), "TIMESTAMPTZ"
    return x, "VARCHAR"


def time_expr(col: str, data_type: str | None, pub_format: str | None, qual: str = "s.") -> str:
    """TIMESTAMPTZ expression for file_stats timeMin/timeMax (pubFormat pattern first, then generic)."""
    dt = (data_type or "").strip().lower()
    pattern = java_to_strptime(pub_format).pattern if dt == "datetime" else None
    return ts_expr(qual + ident(col), pattern)


def auto_candidates(x: str) -> list[tuple[str, str]]:
    """(type, expression) tried in order by :func:`auto_types` (tables no variables file describes)."""
    integral = f"CASE WHEN regexp_full_match(trim({x}), '[+-]?[0-9]{{1,18}}') THEN TRY_CAST(trim({x}) AS BIGINT) END"
    return [("BIGINT", integral),
            ("DOUBLE", f"TRY_CAST({x} AS DOUBLE)"),
            ("DATE", f"CAST(try_strptime({x}, '%Y-%m-%d') AS DATE)"),
            ("TIMESTAMPTZ", ts_expr(x))]


def auto_types(con: duckdb.DuckDBPyConnection, source: str, cols: list[str]) -> dict[str, tuple[str, str]]:
    """{col: (expression over ``s.col``, type)}: the first candidate every non-NULL value casts to, else VARCHAR."""
    if not cols:
        return {}
    cands = {c: auto_candidates("s." + ident(c)) for c in cols}
    aggs: list[str] = []
    for c in cols:
        aggs.append(f"count(s.{ident(c)})")
        aggs += [f"count(s.{ident(c)}) FILTER (WHERE ({e}) IS NULL)" for _t, e in cands[c]]
    row = con.execute(f"SELECT {', '.join(aggs)} FROM {source} s").fetchone()
    k = 1 + len(cands[cols[0]])
    out: dict[str, tuple[str, str]] = {}
    for i, c in enumerate(cols):
        nn, fails = int(row[i * k]), row[i * k + 1:(i + 1) * k]
        pick = next(((e, t) for (t, e), bad in zip(cands[c], fails, strict=True) if nn and not bad), None)
        out[c] = pick or (f"s.{ident(c)}", "VARCHAR")
    return out


def decide_types(con: duckdb.DuckDBPyConnection, product: str, lake_table: str, neon_table: str,
                 data_cols: list[str], exprs: dict[str, tuple[str, str]], source: str = "stage.stg"
                 ) -> tuple[dict[str, tuple[str, str]], list[dict[str, Any]], list[str]]:
    """Apply the 0.1% cast-failure rule on staging: (decided exprs, cast_errors rows, typed columns)."""
    typed = [c for c in data_cols if exprs[c][1] != "VARCHAR"]
    fail_rows: list[dict[str, Any]] = []
    decided: dict[str, tuple[str, str]] = {}
    if typed:
        aggs = ", ".join(f"count(s.{ident(c)}), count(s.{ident(c)}) FILTER (WHERE {exprs[c][0]} IS NULL)"
                         for c in typed)
        counts = con.execute(f"SELECT {aggs} FROM {source} s").fetchone()
        for i, c in enumerate(typed):
            nn, bad = int(counts[2 * i]), int(counts[2 * i + 1])
            pct = bad / nn if nn else 0.0
            keep_varchar = pct > CAST_FAIL_MAX
            decided[c] = (f"s.{ident(c)}", "VARCHAR") if keep_varchar else exprs[c]
            if bad:
                samples = [r[0] for r in con.execute(
                    f"SELECT DISTINCT s.{ident(c)} FROM {source} s WHERE s.{ident(c)} IS NOT NULL AND "
                    f"{exprs[c][0]} IS NULL LIMIT 5").fetchall()]
                fail_rows.append({"productCode": product, "tableName": lake_table, "neonTable": neon_table,
                                  "columnName": c, "targetType": exprs[c][1], "nonNull": nn, "failures": bad,
                                  "failPct": round(100.0 * pct, 6),
                                  "action": "kept VARCHAR" if keep_varchar else "cast; failures set NULL",
                                  "samples": samples, "builtAt": env.utcnow()})
                log.warning("cast_errors %s.%s -> %s: %d/%d (%.4f%%) %s", lake_table, c, exprs[c][1], bad, nn,
                            100 * pct, "kept VARCHAR" if keep_varchar else "nulled")
    for c in data_cols:
        decided.setdefault(c, exprs[c])
    return decided, fail_rows, typed


# ---------------------------------------------------------------- lake helpers

def _stage_drop(con: duckdb.DuckDBPyConnection, name: str) -> None:
    con.execute(f"DROP TABLE IF EXISTS stage.{ident(name)}")


def _stage_from(con: duckdb.DuckDBPyConnection, name: str, sql: str, params: list[Any] | None = None) -> int:
    _stage_drop(con, name)
    con.execute(f"CREATE TABLE stage.{ident(name)} AS {sql}", params or [])
    return int(con.execute(f"SELECT count(*) FROM stage.{ident(name)}").fetchone()[0])


def _catalog_row(ctx: Ctx, table: str, product: str, neon_table: str, kind: str, n_files: int, n_rows: int,
                 n_cols: int, months: list[str], time_col: str | None, fp: str, **extra: Any) -> str:
    """Stage a one-row table_catalog source; returns its staged name."""
    row = {"tableName": table, "productCode": product, "neonTable": neon_table, "kind": kind, "nFiles": n_files,
           "nRows": n_rows, "nColumns": n_cols, "firstMonth": min(months) if months else None,
           "lastMonth": max(months) if months else None, "timeColumn": time_col, "inputSha256": fp,
           "builtAt": env.utcnow(), "nTypedColumns": extra.get("nTyped"), "nVarcharColumns": extra.get("nVarchar"),
           "rowsBeforeDedup": extra.get("rowsBeforeDedup"), "dedupRemoved": extra.get("dedupRemoved")}
    name = f"tc_{ctx.n_stage}"
    ctx.n_stage += 1
    ctx.con.register(name, site_lake.arrow_table([row], site_lake.STACK_SCHEMAS["table_catalog"]))
    return name


def _write_catalog(ctx: Ctx, src: str, table: str, product: str) -> None:
    site_lake.replace_where(ctx.con, "table_catalog", src, "tableName = ? AND productCode = ?", [table, product])
    fp = ctx.con.execute(f"SELECT inputSha256 FROM {src}").fetchone()[0]
    ctx.fingerprints[(table, product)] = fp


def unchanged(ctx: Ctx, table: str, product: str, fp: str) -> bool:
    return not ctx.force and ctx.fingerprints.get((table, product)) == fp


def drift_row(product: str, neon_table: str, lake_table: str, header: str, cols: list[str], files: list[SrcFile],
              missing: list[str], not_in_variables: list[str], unknown: list[str] | None = None) -> dict[str, Any]:
    """One ``schema_drift`` row for a header group (months and publication range of its files)."""
    months = sorted({f.month for f in files})
    pubs = [f.pub_ts for f in files if f.pub_ts is not None]
    return {"productCode": product, "tableName": neon_table, "lakeTable": lake_table,
            "headerSha1": hashlib.sha1(header.encode(), usedforsecurity=False).hexdigest(),
            "nColumns": len(cols), "nFiles": len(files), "firstMonth": months[0] if months else None,
            "lastMonth": months[-1] if months else None, "columns": cols, "missingVsUnion": missing,
            "notInVariables": not_in_variables, "unknownColumns": unknown,
            "firstPublication": min(pubs) if pubs else None, "lastPublication": max(pubs) if pubs else None,
            "builtAt": env.utcnow()}


def _stage_read(con: duckdb.DuckDBPyConnection, product: str, table: str,
                groups: list[tuple[str, list[str], list[SrcFile]]]) -> tuple[int, bool]:
    """Stage ``stage.stg`` from header groups; strict read first, lenient retry. Returns (rows, lenient)."""
    sql, params = read_groups_sql(groups)
    try:
        return _stage_from(con, "stg", sql, params), False
    except duckdb.Error as exc:
        log.warning("stack %s %s: strict CSV read failed (%s); retrying leniently", product, table,
                    str(exc).splitlines()[0][:200])
    sql, params = read_groups_sql(groups, lenient=True)
    return _stage_from(con, "stg", sql, params), True


# ---------------------------------------------------------------- supporting tables

def stage_variables(ctx: Ctx, product: str, files: list[SrcFile]) -> dict[tuple[str, str], tuple[str, str]]:
    """Stage variables_hist/variables_latest; return {(table, field): (dataType, pubFormat)}."""
    con = ctx.con
    if not files:
        _stage_drop(con, "variables_latest")
        _stage_drop(con, "variables_history")
        return {}
    register_fmeta(con, ctx.site, ctx.domain, files, "fmeta_vars")
    groups = group_by_header(files)
    sql, params = read_groups_sql(groups, rename={"table": "tableName"})
    _stage_from(con, "variables_raw", f"SELECT r.*, m.sourceFile, m.month, m.publicationDate FROM ({sql}) r "
                                      f"JOIN fmeta_vars m USING (filename)", params)
    cols = [c for c in site_lake.relation_columns(con, "stage.variables_raw")
            if c not in ("filename", "sourceFile", "month", "publicationDate")]
    col_list = ", ".join(ident(c) for c in cols)
    missing = [c for c in VARIABLES_COLUMNS if c not in cols]
    col_list = ", ".join([ident(c) for c in cols] + [f"CAST(NULL AS VARCHAR) AS {ident(c)}" for c in missing])
    _stage_from(con, "variables_latest",
                f"SELECT {sql_str(product)} AS productCode, {col_list}, sourceFile, publicationDate "
                f"FROM stage.variables_raw QUALIFY row_number() OVER (PARTITION BY tableName, fieldName "
                f"ORDER BY publicationDate DESC NULLS LAST, sourceFile DESC) = 1")
    _stage_from(con, "variables_history",
                f"SELECT {sql_str(product)} AS productCode, {col_list}, count(*) AS nFiles, "
                f"min(publicationDate) AS firstPublication, max(publicationDate) AS lastPublication, "
                f"min(month) AS firstMonth, max(month) AS lastMonth, max(sourceFile) AS latestSourceFile "
                f"FROM stage.variables_raw GROUP BY ALL")
    types = {}
    for t, f, d, pf in con.execute(
            "SELECT tableName, fieldName, dataType, pubFormat FROM stage.variables_latest").fetchall():
        types[(t, f)] = (d, pf)
    return types


def stage_sensor_positions(ctx: Ctx, product: str, files: list[SrcFile]
                           ) -> tuple[int, list[dict[str, Any]], list[str]]:
    """Stage ``stage.sensor_positions``; returns (rows, schema_drift rows, accepted extra columns).

    Header groups are unioned by name after legacy aliasing. Columns outside ``SENSOR_COLUMNS`` are kept
    (``*DateTime`` -> TIMESTAMPTZ, else VARCHAR). Raises :class:`StackError` only when a group lacks one of
    ``SENSOR_REQUIRED``.
    """
    con = ctx.con
    groups = group_by_header(files)
    known = set(SENSOR_COLUMNS)
    canon_by_group: list[list[str]] = []
    extras: list[str] = []
    missing_req: dict[str, Any] = {}
    for header, cols, fs in groups:
        canon = list(group_rename(cols, SENSOR_ALIASES).values())
        canon_by_group.append(canon)
        miss = [c for c in SENSOR_REQUIRED if c not in canon]
        if miss:
            missing_req[header] = {"missingRequired": miss, "files": [f.name for f in fs][:20], "nFiles": len(fs)}
        extras += [c for c in canon if c not in known and c not in extras]
    reports = site_lake.site_paths(ctx.site).reports_dir
    if missing_req:
        report = reports / f"sensor_positions_missing_required_{product}.json"
        env.write_json(report, {"site": ctx.site, "productCode": product, "required": list(SENSOR_REQUIRED),
                                "headers": missing_req})
        raise StackError(f"{product}: sensor_positions header set(s) without required column(s); see {report}")
    out_name = {c: f"{c}_neon" if c in SENSOR_DERIVED else c for c in extras}
    rename = {**SENSOR_ALIASES, **{c: o for c, o in out_name.items() if o != c}}
    register_fmeta(con, ctx.site, ctx.domain, files, "fmeta_sp")
    sql, params = read_groups_sql(groups, rename=rename)
    present = {c for canon in canon_by_group for c in canon}
    sel = []
    for c in SENSOR_COLUMNS:
        if c not in present:
            typ = "TIMESTAMPTZ" if c in SENSOR_TIMES else "DOUBLE" if c in SENSOR_DOUBLES else "VARCHAR"
            sel.append(f"CAST(NULL AS {typ}) AS {ident(c)}")
        elif c in SENSOR_TIMES:
            sel.append(f"{ts_expr('r.' + ident(c))} AS {ident(c)}")
        elif c in SENSOR_DOUBLES:
            sel.append(f"TRY_CAST(r.{ident(c)} AS DOUBLE) AS {ident(c)}")
        else:
            sel.append(f"r.{ident(c)}")
    for c in extras:  # schema evolution (e.g. effectiveStartDateTime/effectiveEndDateTime, 2026-09)
        o = out_name[c]
        sel.append(f"{ts_expr('r.' + ident(o))} AS {ident(o)}" if c.endswith("DateTime") else f"r.{ident(o)}")
    loc = ("l.locationType, l.locationDecimalLatitude AS locationLatitude, "
           "l.locationDecimalLongitude AS locationLongitude, l.locationElevation AS locationElevationCatalog"
           if "locations" in ctx.catalog_tables else
           "CAST(NULL AS VARCHAR) AS locationType, CAST(NULL AS DOUBLE) AS locationLatitude, "
           "CAST(NULL AS DOUBLE) AS locationLongitude, CAST(NULL AS DOUBLE) AS locationElevationCatalog")
    hist = ("h.locationDecimalLatitude AS historyLatitude, h.locationDecimalLongitude AS historyLongitude, "
            "h.locationElevation AS historyElevation"
            if "location_history" in ctx.catalog_tables else
            "CAST(NULL AS DOUBLE) AS historyLatitude, CAST(NULL AS DOUBLE) AS historyLongitude, "
            "CAST(NULL AS DOUBLE) AS historyElevation")
    joins = ""
    if "locations" in ctx.catalog_tables:
        joins += f" LEFT JOIN {site_lake.CATALOG_ALIAS}.locations l ON l.locationName = s.sensorLocationID"
    if "location_history" in ctx.catalog_tables:
        joins += (f" LEFT JOIN {site_lake.CATALOG_ALIAS}.location_history h ON h.locationName = s.sensorLocationID"
                  " AND h.locationStartDate <= s.monthStart AND (h.locationEndDate IS NULL OR "
                  "h.locationEndDate > s.monthStart)")
    # __rid numbers the source rows, so the QUALIFY only undoes the fan-out of the history join
    inner = (f"SELECT {sql_str(product)} AS productCode, m.month, m.release, m.publicationDate, m.sourceFile, "
             f"{', '.join(sel)}, split_part(r.\"HOR.VER\", '.', 1) AS horizontalPosition, "
             f"split_part(r.\"HOR.VER\", '.', 2) AS verticalPosition, "
             f"timezone('UTC', strptime(m.month || '-01', '%Y-%m-%d')) AS monthStart, "
             f"row_number() OVER () AS __rid "
             f"FROM ({sql}) r JOIN fmeta_sp m USING (filename)")
    q = (f"SELECT s.* EXCLUDE (monthStart, __rid), "
         f"s.locationReferenceLatitude + s.northOffset / 111320.0 AS sensorLatitude, "
         f"s.locationReferenceLongitude + s.eastOffset / (111320.0 * cos(radians(s.locationReferenceLatitude))) "
         f"AS sensorLongitude, s.locationReferenceElevation + s.zOffset AS sensorElevation, {loc}, {hist} "
         f"FROM ({inner}) s{joins} "
         f"QUALIFY row_number() OVER (PARTITION BY s.__rid ORDER BY "
         f"{'h.locationStartDate DESC' if 'location_history' in ctx.catalog_tables else '1'}) = 1")
    n = _stage_from(con, "sensor_positions", q, params)
    union = list(dict.fromkeys(c for canon in canon_by_group for c in canon))
    drift = [drift_row(product, "sensor_positions", "sensor_positions", h, cols, fs,
                       missing=[c for c in union if c not in canon], not_in_variables=[],
                       unknown=[c for c in canon if c not in known])
             for (h, cols, fs), canon in zip(groups, canon_by_group, strict=True)]
    if extras:
        log.info("stack %s %s sensor_positions: accepted additional column(s) %s", ctx.site, product, extras)
    for stale in (f"sensor_positions_unknown_headers_{product}.json",
                  f"sensor_positions_missing_required_{product}.json"):
        (reports / stale).unlink(missing_ok=True)
    return n, drift, [out_name[c] for c in extras]


def stage_distinct(ctx: Ctx, name: str, product: str, files: list[SrcFile]) -> int:
    """categorical_codes / validation / science_review_flags: distinct content rows per product."""
    con = ctx.con
    register_fmeta(con, ctx.site, ctx.domain, files, f"fmeta_{name}")
    groups = group_by_header(files)
    if not groups:
        return 0
    sql, params = read_groups_sql(groups)
    content: list[str] = []
    for _h, cols, _fs in groups:
        content += [c for c in cols if c not in content]
    col_list = ", ".join(f"r.{ident(c)}" for c in content)
    q = (f"SELECT {sql_str(product)} AS productCode, {col_list}, count(*) AS nRecords, "
         f"count(DISTINCT m.sourceFile) AS nFiles, min(m.month) AS firstMonth, max(m.month) AS lastMonth, "
         f"max(m.sourceFile) AS latestSourceFile, max(m.publicationDate) AS latestPublication "
         f"FROM ({sql}) r JOIN fmeta_{name} m USING (filename) GROUP BY ALL")
    return _stage_from(con, name, q, params)


def stage_index(ctx: Ctx, name: str, product: str, files: list[SrcFile]) -> int:
    import pyarrow as pa

    rows = []
    for f in files:
        rng = EML_RANGE_RE.search(f.name) if name == "eml_index" else None
        row = {"productCode": product, "month": f.month, "release": f.release, "sourceFile": f.name,
               "publicationDate": f.pub_ts, "size": f.size, "md5": f.md5, "md5Verified": f.md5_verified,
               "localRelPath": f.local_path}
        if name == "eml_index":
            row["emlStart"] = datetime.strptime(rng.group(1), "%Y%m%d").date() if rng else None  # noqa: DTZ007
            row["emlEnd"] = datetime.strptime(rng.group(2), "%Y%m%d").date() if rng else None  # noqa: DTZ007
        rows.append(row)
    fields = [("productCode", pa.string()), ("month", pa.string()), ("release", pa.string()),
              ("sourceFile", pa.string()), ("publicationDate", pa.timestamp("us", tz="UTC")), ("size", pa.int64()),
              ("md5", pa.string()), ("md5Verified", pa.bool_()), ("localRelPath", pa.string())]
    if name == "eml_index":
        fields += [("emlStart", pa.date32()), ("emlEnd", pa.date32())]
    ctx.con.register(f"src_{name}", pa.Table.from_pylist(rows, schema=pa.schema(fields)))
    return len(rows)


def support_tables(by_kind: dict[str, list[SrcFile]]) -> list[str]:
    """Lake tables the supporting unit writes for a product with these file kinds."""
    out = []
    if by_kind.get("variables"):
        out += ["variables", "variables_history"]
    if by_kind.get("sensor_positions"):
        out.append("sensor_positions")
    out += [t for k, t in DISTINCT_SUPPORT.items() if by_kind.get(k)]
    out += [t for k, t in (("eml", "eml_index"), ("readme", "readme_index")) if by_kind.get(k)]
    return out


def stack_support(ctx: Ctx, product: str, by_kind: dict[str, list[SrcFile]]) -> dict[str, Any] | None:
    files = [f for k in SUPPORT_KINDS for f in by_kind.get(k, [])]
    if not files:
        return None
    fp = fingerprint(files, "supporting")
    expected = support_tables(by_kind)
    if expected and all(unchanged(ctx, t, product, fp) for t in expected):
        return {"product": product, "unit": "supporting", "skipped": True}
    t0 = time.monotonic()
    con = ctx.con
    stats: dict[str, int] = {}
    sources: list[tuple[str, str]] = []  # (lake table, staged source)
    if by_kind.get("variables"):
        stats["variables"] = int(con.execute("SELECT count(*) FROM stage.variables_latest").fetchone()[0])
        stats["variables_history"] = int(con.execute("SELECT count(*) FROM stage.variables_history").fetchone()[0])
        sources += [("variables", "stage.variables_latest"), ("variables_history", "stage.variables_history")]
    sp_extra: list[str] = []
    if by_kind.get("sensor_positions"):
        stats["sensor_positions"], sp_drift, sp_extra = stage_sensor_positions(ctx, product,
                                                                              by_kind["sensor_positions"])
        sources.append(("sensor_positions", "stage.sensor_positions"))
        con.register("src_sp_drift", site_lake.arrow_table(sp_drift, site_lake.STACK_SCHEMAS["schema_drift"]))
    for kind, table in DISTINCT_SUPPORT.items():
        if by_kind.get(kind):
            stats[table] = stage_distinct(ctx, table, product, by_kind[kind])
            sources.append((table, f"stage.{table}"))
    for kind, table in (("eml", "eml_index"), ("readme", "readme_index")):
        if by_kind.get(kind):
            stats[table] = stage_index(ctx, table, product, by_kind[kind])
            sources.append((table, f"src_{table}"))
    catalog = []
    for table, _src in sources:
        n_files = len(by_kind.get({"variables_history": "variables", "eml_index": "eml", "readme_index": "readme"}
                                  .get(table, table), []))
        months = sorted({f.month for f in files})
        catalog.append((table, _catalog_row(ctx, table, product, table, "support", n_files, stats[table],
                                            len(site_lake.relation_columns(con, _src)), months, None, fp)))
    with site_lake.commit(con, f"stack {ctx.site} {product} supporting",
                          {"product": product, "rows": stats, "inputSha256": fp}):
        for table, src in sources:
            site_lake.replace_where(con, table, src, "productCode = ?", [product])
        if by_kind.get("sensor_positions"):
            site_lake.replace_where(con, "schema_drift", "src_sp_drift", "productCode = ? AND tableName = ?",
                                    [product, "sensor_positions"])
        for table, src in catalog:
            _write_catalog(ctx, src, table, product)
    res = {"product": product, "unit": "supporting", "rows": stats, "seconds": round(time.monotonic() - t0, 2)}
    if sp_extra:
        res["sensorPositionsExtraColumns"] = sp_extra
    log.info("stack %s %s supporting: %s (%.1fs)", ctx.site, product, stats, res["seconds"])
    return res


# ---------------------------------------------------------------- data tables

def stack_table(ctx: Ctx, product: str, neon_table: str, files: list[SrcFile],
                types: dict[tuple[str, str], tuple[str, str]], vars_fp: str) -> dict[str, Any]:
    con = ctx.con
    lake_table = lake_table_name(product, neon_table)
    fp = fingerprint(files, vars_fp)
    if unchanged(ctx, lake_table, product, fp):
        return {"product": product, "table": lake_table, "skipped": True}
    t0 = time.monotonic()
    groups = group_by_header(files)
    if not groups:
        return {"product": product, "table": lake_table, "skipped": True, "reason": "no non-empty files"}
    register_fmeta(con, ctx.site, ctx.domain, [f for _h, _c, fs in groups for f in fs])
    n_raw, lenient = _stage_read(con, product, neon_table, groups)
    data_cols: list[str] = []
    for _h, cols, _fs in groups:
        data_cols += [c for c in cols if c not in data_cols]
    is_table = any(f.hor for f in files)
    # ---- cast-failure rates on staging
    exprs = {c: cast_for(c, *types.get((neon_table, c), (None, None))) for c in data_cols}
    decided, fail_rows, typed = decide_types(con, product, lake_table, neon_table, data_cols, exprs)
    # ---- file stats / taxa before dedup
    time_col = next((c for c in TIME_COLUMNS if c in data_cols), None)
    t_expr = (time_expr(time_col, *types.get((neon_table, time_col), (None, None))) if time_col
              else "CAST(NULL AS TIMESTAMPTZ)")
    qf_cols = [c for c in data_cols if c.lower().endswith("finalqf")]
    if qf_cols:
        num = " + ".join(f"count(*) FILTER (WHERE TRY_CAST(s.{ident(c)} AS DOUBLE) = 1)" for c in qf_cols)
        den = " + ".join(f"count(s.{ident(c)})" for c in qf_cols)
        qf_expr = f"CASE WHEN ({den}) > 0 THEN 100.0 * ({num}) / ({den}) END"
    else:
        qf_expr = "CAST(NULL AS DOUBLE)"

    def lst(c: str) -> str:
        return (f"list_sort(list(DISTINCT s.{ident(c)}) FILTER (WHERE s.{ident(c)} IS NOT NULL))" if c in data_cols
                else "CAST(NULL AS VARCHAR[])")
    plot_count = "count(DISTINCT s.plotID)" if "plotID" in data_cols else "CAST(NULL AS BIGINT)"
    taxon_count = "count(DISTINCT s.taxonID)" if "taxonID" in data_cols else "CAST(NULL AS BIGINT)"
    _stage_from(con, "file_stats",
                f"SELECT m.sourceFile, {sql_str(product)} AS productCode, {sql_str(neon_table)} AS tableName, "
                f"{sql_str(lake_table)} AS lakeTable, m.month, count(s.filename) AS rowCount, "
                f"min({t_expr}) AS timeMin, max({t_expr}) AS timeMax, "
                f"{sql_str(time_col) if time_col else 'CAST(NULL AS VARCHAR)'} AS timeColumn, "
                f"{qf_expr} AS qfFailPct, {lst('plotID')} AS plotIDs, {plot_count} AS plotCount, "
                f"{lst('samplingProtocolVersion')} AS protocolVersions, {taxon_count} AS taxonCount, "
                f"any_value(m.horizontalPosition) AS hor, any_value(m.verticalPosition) AS ver, "
                f"any_value(m.tmi) AS tmi "
                f"FROM fmeta m LEFT JOIN stage.stg s ON s.filename = m.filename GROUP BY m.sourceFile, m.month")
    has_taxa = "taxonID" in data_cols
    if has_taxa:
        tax = ", ".join(f"s.{ident(c)}" if c in data_cols else f"CAST(NULL AS VARCHAR) AS {ident(c)}"
                        for c in ("taxonID", "scientificName", "taxonRank", "vernacularName"))
        _stage_from(con, "file_taxa",
                    f"SELECT m.sourceFile, {sql_str(product)} AS productCode, {sql_str(neon_table)} AS tableName, "
                    f"{tax}, count(*) AS nRecords FROM stage.stg s JOIN fmeta m USING (filename) "
                    f"WHERE s.taxonID IS NOT NULL GROUP BY ALL")
    # ---- schema drift per header group
    var_fields = {f for (t, f) in types if t == neon_table}
    drift = [drift_row(product, neon_table, lake_table, h, cols, fs,
                       missing=[c for c in data_cols if c not in cols],
                       not_in_variables=[c for c in cols if var_fields and c not in var_fields])
             for h, cols, fs in groups]
    con.register("src_drift", site_lake.arrow_table(drift, site_lake.STACK_SCHEMAS["schema_drift"]))
    # ---- final typed select (+ added columns, uid dedup)
    select = [f"{decided[c][0]} AS {ident(c)}" for c in data_cols]
    added = ["siteID", "domainID", "month", "release", "publicationDate", "sourceFile"]
    if is_table:
        added += ["horizontalPosition", "verticalPosition", "tmi"]
    select += [f"m.{ident(c)}" for c in added if c not in data_cols]
    dedup = ""
    if "uid" in data_cols:
        dedup = (" QUALIFY s.uid IS NULL OR row_number() OVER (PARTITION BY s.uid ORDER BY m.publicationDate DESC "
                 "NULLS LAST, m.sourceFile DESC) = 1")
    body = f"SELECT {', '.join(select)} FROM stage.stg s JOIN fmeta m ON s.filename = m.filename{dedup}"
    n_typed = sum(1 for c in data_cols if decided[c][1] != "VARCHAR")
    n_varchar_kept = sum(1 for c in typed if decided[c][1] == "VARCHAR")
    months = sorted({f.month for f in files})
    if fail_rows:
        con.register("src_cast", site_lake.arrow_table(fail_rows, site_lake.STACK_SCHEMAS["cast_errors"]))
    extra = {"product": product, "neonTable": neon_table, "files": len(files), "headerGroups": len(groups),
             "rowsStaged": n_raw, "inputSha256": fp, "lenientRead": lenient}
    with site_lake.commit(con, f"stack {ctx.site} {product} {neon_table}", extra):
        con.execute(f"CREATE OR REPLACE TABLE {site_lake.ALIAS}.{ident(lake_table)} AS {body}")
        n_rows = int(con.execute(f"SELECT count(*) FROM {site_lake.ALIAS}.{ident(lake_table)}").fetchone()[0])
        n_cols = len(site_lake.table_columns(con, lake_table))
        where, args = "productCode = ? AND tableName = ?", [product, neon_table]
        site_lake.replace_where(con, "file_stats", "stage.file_stats", where, args)
        if has_taxa:
            site_lake.replace_where(con, "file_taxa", "stage.file_taxa", where, args)
        site_lake.replace_where(con, "schema_drift", "src_drift", where, args)
        if fail_rows:
            site_lake.replace_where(con, "cast_errors", "src_cast", "productCode = ? AND neonTable = ?", args)
        elif site_lake.table_exists(con, "cast_errors"):
            con.execute(f"DELETE FROM {site_lake.ALIAS}.cast_errors WHERE productCode = ? AND neonTable = ?", args)
        tc = _catalog_row(ctx, lake_table, product, neon_table, "data", len(files), n_rows, n_cols, months,
                          time_col, fp, nTyped=n_typed, nVarchar=len(data_cols) - n_typed, rowsBeforeDedup=n_raw,
                          dedupRemoved=n_raw - n_rows)
        _write_catalog(ctx, tc, lake_table, product)
    _stage_drop(con, "stg")
    res = {"product": product, "table": lake_table, "files": len(files), "headerGroups": len(groups),
           "rows": n_rows, "rowsStaged": n_raw, "dedupRemoved": n_raw - n_rows, "columns": n_cols,
           "typedColumns": n_typed, "castKeptVarchar": n_varchar_kept, "castErrors": len(fail_rows),
           "timeColumn": time_col, "lenientRead": lenient, "seconds": round(time.monotonic() - t0, 2)}
    log.info("stack %s %s: %d rows (%d staged, %d files, %d header group(s)), %d/%d typed, %d cast issue(s) "
             "(%.1fs)", ctx.site, lake_table, n_rows, n_raw, len(files), len(groups), n_typed, len(data_cols),
             len(fail_rows), res["seconds"])
    return res


def register_lab_meta(con: duckdb.DuckDBPyConnection, files: list[SrcFile], name: str = "fmeta_lab") -> None:
    import pyarrow as pa

    con.register(name, pa.table({
        "filename": pa.array([str(f.path) for f in files]),
        "sourceFile": pa.array([f.name for f in files]),
        "labName": pa.array([f.lab_name for f in files], type=pa.string()),
        "publicationDate": pa.array([f.pub_ts for f in files], type=pa.timestamp("us", tz="UTC")),
    }))


def stack_lab(ctx: Ctx, product: str, neon_table: str, files: list[SrcFile],
              types: dict[tuple[str, str], tuple[str, str]], vars_fp: str) -> dict[str, Any]:
    """A product's ``lab`` files of one table -> ``<dp>__lab_<table>`` (distinct rows, newest publication)."""
    con = ctx.con
    lake_table = lake_table_name(product, f"lab_{neon_table}")
    fp = fingerprint(files, f"lab\n{vars_fp}")
    if unchanged(ctx, lake_table, product, fp):
        return {"product": product, "table": lake_table, "skipped": True}
    t0 = time.monotonic()
    groups = group_by_header(files)
    if not groups:
        return {"product": product, "table": lake_table, "skipped": True, "reason": "no non-empty files"}
    register_lab_meta(con, [f for _h, _c, fs in groups for f in fs])
    n_raw, lenient = _stage_read(con, product, f"lab {neon_table}", groups)
    data_cols: list[str] = []
    for _h, cols, _fs in groups:
        data_cols += [c for c in cols if c not in data_cols]
    var_fields = {f for (t, f) in types if t == neon_table}
    fail_rows: list[dict[str, Any]] = []
    if var_fields:  # the product's variables describe this lab table
        exprs = {c: cast_for(c, *types.get((neon_table, c), (None, None))) for c in data_cols}
        decided, fail_rows, _typed = decide_types(con, product, lake_table, neon_table, data_cols, exprs)
        typing = "variables"
    else:
        decided = auto_types(con, "stage.stg", data_cols)
        typing = "auto"
    meta = [c for c in ("labName", "publicationDate", "sourceFile") if c not in data_cols]
    select = [f"{decided[c][0]} AS {ident(c)}" for c in data_cols] + [f"m.{ident(c)}" for c in meta]
    identity = ", ".join([f"s.{ident(c)}" for c in data_cols] + (["m.labName"] if "labName" in meta else []))
    body = (f"SELECT {', '.join(select)} FROM stage.stg s JOIN fmeta_lab m ON s.filename = m.filename "
            f"QUALIFY row_number() OVER (PARTITION BY {identity} ORDER BY m.publicationDate DESC NULLS LAST, "
            f"m.sourceFile DESC) = 1")
    drift = [drift_row(product, neon_table, lake_table, h, cols, fs,
                       missing=[c for c in data_cols if c not in cols],
                       not_in_variables=[c for c in cols if var_fields and c not in var_fields])
             for h, cols, fs in groups]
    con.register("src_lab_drift", site_lake.arrow_table(drift, site_lake.STACK_SCHEMAS["schema_drift"]))
    if fail_rows:
        con.register("src_lab_cast", site_lake.arrow_table(fail_rows, site_lake.STACK_SCHEMAS["cast_errors"]))
    n_typed = sum(1 for c in data_cols if decided[c][1] != "VARCHAR")
    months = sorted({f.month for f in files})
    labs = sorted({f.lab_name for f in files if f.lab_name})
    extra = {"product": product, "neonTable": neon_table, "kind": "lab", "labs": labs, "files": len(files),
             "headerGroups": len(groups), "rowsStaged": n_raw, "typing": typing, "inputSha256": fp,
             "lenientRead": lenient}
    with site_lake.commit(con, f"stack {ctx.site} {product} lab {neon_table}", extra):
        con.execute(f"CREATE OR REPLACE TABLE {site_lake.ALIAS}.{ident(lake_table)} AS {body}")
        n_rows = int(con.execute(f"SELECT count(*) FROM {site_lake.ALIAS}.{ident(lake_table)}").fetchone()[0])
        n_cols = len(site_lake.table_columns(con, lake_table))
        where, args = "productCode = ? AND lakeTable = ?", [product, lake_table]
        site_lake.replace_where(con, "schema_drift", "src_lab_drift", where, args)
        if fail_rows:
            site_lake.replace_where(con, "cast_errors", "src_lab_cast", "productCode = ? AND tableName = ?", args)
        elif site_lake.table_exists(con, "cast_errors"):
            con.execute(f"DELETE FROM {site_lake.ALIAS}.cast_errors WHERE productCode = ? AND tableName = ?", args)
        tc = _catalog_row(ctx, lake_table, product, neon_table, "lab", len(files), n_rows, n_cols, months, None,
                          fp, nTyped=n_typed, nVarchar=len(data_cols) - n_typed, rowsBeforeDedup=n_raw,
                          dedupRemoved=n_raw - n_rows)
        _write_catalog(ctx, tc, lake_table, product)
    _stage_drop(con, "stg")
    res = {"product": product, "table": lake_table, "kind": "lab", "labs": labs, "files": len(files),
           "headerGroups": len(groups), "rows": n_rows, "rowsStaged": n_raw, "dedupRemoved": n_raw - n_rows,
           "columns": n_cols, "typing": typing, "typedColumns": n_typed,
           "castKeptVarchar": sum(1 for r in fail_rows if r["action"] == "kept VARCHAR"),
           "castErrors": len(fail_rows), "lenientRead": lenient, "seconds": round(time.monotonic() - t0, 2)}
    log.info("stack %s %s: %d distinct rows (%d staged, %d lab file(s)), %d/%d typed (%s) (%.1fs)", ctx.site,
             lake_table, n_rows, n_raw, len(files), n_typed, len(data_cols), typing, res["seconds"])
    return res


def stack_h5(ctx: Ctx, product: str, files: list[SrcFile]) -> dict[str, Any]:
    from . import h5

    fp = fingerprint(files)
    if unchanged(ctx, h5.LAKE_TABLE, product, fp):
        return {"product": product, "table": h5.LAKE_TABLE, "skipped": True}
    t0 = time.monotonic()
    out = h5.build_tables(ctx.site, ctx.domain, product, files)
    con = ctx.con
    names = [f.name for f in files]
    months = sorted({f.month for f in files})
    if out["h5_attrs"] is not None:
        con.register("src_h5_attrs", out["h5_attrs"])
    if out["h5_datasets"] is not None:
        con.register("src_h5_datasets", out["h5_datasets"])
    con.register("src_h5_stats", site_lake.arrow_table(out["stats"], site_lake.STACK_SCHEMAS["file_stats"]))
    n_rows = 0
    with site_lake.commit(con, f"stack {ctx.site} {product} {h5.NEON_TABLE} (h5)",
                          {"product": product, "files": len(files), "missing": out["missing"], "inputSha256": fp}):
        where = f"productCode = ? AND sourceFile IN ({','.join('?' * len(names))})"
        if out["h5_attrs"] is not None:
            site_lake.replace_where(con, "h5_attrs", "src_h5_attrs", where, [product, *names])
        if out["h5_datasets"] is not None:
            site_lake.replace_where(con, "h5_datasets", "src_h5_datasets", where, [product, *names])
        if out["flux"] is not None:
            con.register("src_h5_flux", out["flux"])
            con.execute(f"CREATE OR REPLACE TABLE {site_lake.ALIAS}.{h5.LAKE_TABLE} AS SELECT * FROM src_h5_flux")
            n_rows = out["flux"].num_rows
        site_lake.replace_where(con, "file_stats", "src_h5_stats", "productCode = ? AND tableName = ?",
                                [product, h5.NEON_TABLE])
        n_cols = len(site_lake.table_columns(con, h5.LAKE_TABLE)) if out["flux"] is not None else 0
        tc = _catalog_row(ctx, h5.LAKE_TABLE, product, h5.NEON_TABLE, "h5", len(files), n_rows, n_cols, months,
                          "timeBgn", fp)
        _write_catalog(ctx, tc, h5.LAKE_TABLE, product)
    res = {"product": product, "table": h5.LAKE_TABLE, "files": len(files), "rows": n_rows,
           "h5Attrs": out["h5_attrs"].num_rows if out["h5_attrs"] is not None else 0,
           "h5Datasets": out["h5_datasets"].num_rows if out["h5_datasets"] is not None else 0,
           "missingPaths": out["missing"], "seconds": round(time.monotonic() - t0, 2)}
    log.info("stack %s %s: %d rows from %d h5 file(s), %d attrs, %d datasets (%.1fs)", ctx.site, h5.LAKE_TABLE,
             n_rows, len(files), res["h5Attrs"], res["h5Datasets"], res["seconds"])
    return res


# ---------------------------------------------------------------- driver

def stack_product(ctx: Ctx, product: str, files: list[SrcFile]) -> None:
    by_kind: dict[str, list[SrcFile]] = defaultdict(list)
    for f in files:
        by_kind[f.kind].append(f)
    vars_fp = fingerprint(by_kind.get("variables", []), "variables")
    try:
        types = stage_variables(ctx, product, by_kind.get("variables", []))
    except Exception as exc:  # noqa: BLE001 - one product must not abort the site
        ctx.errors.append({"product": product, "unit": "variables", "error": str(exc)[:500]})
        log.error("stack %s %s variables failed (%s); data tables stay VARCHAR", ctx.site, product, str(exc)[:300])
        types = {}
        by_kind = {k: v for k, v in by_kind.items() if k != "variables"}
    try:
        res = stack_support(ctx, product, by_kind)
        if res:
            ctx.results.append(res)
    except Exception as exc:  # noqa: BLE001
        ctx.errors.append({"product": product, "unit": "supporting", "error": str(exc)[:500]})
        log.error("stack %s %s supporting failed: %s", ctx.site, product, str(exc)[:300])
    tables: dict[str, list[SrcFile]] = defaultdict(list)
    h5_files: list[SrcFile] = []
    for f in by_kind.get("data", []):
        if f.ext == "h5":
            h5_files.append(f)
        elif f.ext == "csv" and f.table:
            tables[f.table].append(f)
        else:
            log.info("stack %s: %s is not a stackable data file; registered only", product, f.name)
    for table in sorted(tables):
        try:
            ctx.results.append(stack_table(ctx, product, table, tables[table], types, vars_fp))
        except Exception as exc:  # noqa: BLE001
            ctx.errors.append({"product": product, "unit": table, "error": str(exc)[:500]})
            log.error("stack %s %s %s failed: %s", ctx.site, product, table, str(exc)[:300])
            _stage_drop(ctx.con, "stg")
    if h5_files:
        try:
            ctx.results.append(stack_h5(ctx, product, h5_files))
        except Exception as exc:  # noqa: BLE001
            ctx.errors.append({"product": product, "unit": "h5", "error": str(exc)[:500]})
            log.error("stack %s %s h5 failed: %s", ctx.site, product, str(exc)[:300])
    labs: dict[str, list[SrcFile]] = defaultdict(list)
    for f in by_kind.get("lab", []):
        if f.ext == "csv" and f.table:
            labs[f.table].append(f)
        else:
            log.info("stack %s: lab file %s is not a CSV table; registered only", product, f.name)
    for table in sorted(labs):
        try:
            ctx.results.append(stack_lab(ctx, product, table, labs[table], types, vars_fp))
        except Exception as exc:  # noqa: BLE001
            ctx.errors.append({"product": product, "unit": f"lab_{table}", "error": str(exc)[:500]})
            log.error("stack %s %s lab %s failed: %s", ctx.site, product, table, str(exc)[:300])
            _stage_drop(ctx.con, "stg")
    other = [f.name for k, fs in by_kind.items() if k not in SUPPORT_KINDS and k not in ("data", "lab") for f in fs]
    if other:
        log.info("stack %s: %d file(s) of other kinds not stacked", product, len(other))
    if not any(e.get("product") == product for e in ctx.errors):
        expected = {lake_table_name(product, t) for t in tables} | {lake_table_name(product, f"lab_{t}") for t in labs}
        prune_stale_tables(ctx, product, expected)


def prune_stale_tables(ctx: Ctx, product: str, expected: set[str]) -> list[str]:
    """Drop data/lab lake tables this product recorded earlier but no longer produces.

    Happens when a file-name classification changes (e.g. DP4.00001.001 tables once collapsed into
    ``__000``) or NEON renames/retires a table. Only runs after an error-free product build; removes the
    table and its table_catalog, file_stats, cast_errors, schema_drift and file_taxa rows in one snapshot.
    """
    con = ctx.con
    if not site_lake.table_exists(con, "table_catalog"):
        return []
    rows = con.execute("SELECT tableName, neonTable FROM lake.table_catalog WHERE productCode = ? AND kind IN "
                       "('data', 'lab')", [product]).fetchall()
    stale = [(t, n) for t, n in rows if t not in expected]
    if not stale:
        return []
    with site_lake.commit(con, f"stack {ctx.site} {product} prune", {"dropped": [t for t, _ in stale]}):
        for table, neon_table in stale:
            con.execute(f"DROP TABLE IF EXISTS lake.{ident(table)}")
            con.execute("DELETE FROM lake.table_catalog WHERE productCode = ? AND tableName = ?", [product, table])
            for derived, col in (("file_stats", "lakeTable"), ("cast_errors", "tableName"), ("schema_drift", "lakeTable")):
                if site_lake.table_exists(con, derived):
                    con.execute(f"DELETE FROM lake.{derived} WHERE productCode = ? AND {col} = ?", [product, table])
            if neon_table and site_lake.table_exists(con, "file_taxa"):
                con.execute("DELETE FROM lake.file_taxa WHERE productCode = ? AND tableName = ?", [product, neon_table])
    log.warning("stack %s %s: dropped %d stale table(s): %s", ctx.site, product, len(stale), [t for t, _ in stale])
    return [t for t, _ in stale]


def _totals(results: list[dict[str, Any]], errors: list[dict[str, Any]]) -> dict[str, Any]:
    built = [r for r in results if not r.get("skipped")]
    return {"unitsBuilt": len(built), "unitsSkipped": len(results) - len(built), "unitErrors": len(errors),
            "castErrorColumns": sum(int(r.get("castErrors") or 0) for r in built),
            "castKeptVarchar": sum(int(r.get("castKeptVarchar") or 0) for r in built),
            "labTables": sorted(r["table"] for r in built if r.get("kind") == "lab"),
            "sensorPositionsExtraColumns": {r["product"]: r["sensorPositionsExtraColumns"] for r in built
                                            if r.get("sensorPositionsExtraColumns")}}


def main(site: str, products: list[str] | None = None, force: bool = False) -> int:
    site = site.upper()
    t0 = time.monotonic()
    paths = site_lake.site_paths(site)
    inputs = input_files(site, products)
    if not inputs:
        log.warning("stack %s: no downloaded files%s", site, f" for {products}" if products else "")
        return 1
    rid = env.run_id()
    scon = state.connect(state.DB_PATH)
    state.start_run(scon, rid, "site-stack", site, [site, ",".join(products or []), "force" if force else ""])
    status = "failed"
    ctx: Ctx | None = None
    stage_db = paths.tmp_dir / "stack_stage.duckdb"
    try:
        with env.file_lock("stack"), env.file_lock(f"site-lake-{site}", blocking=True):
            con = site_lake.connect(site)
            for p in (stage_db, stage_db.with_name(stage_db.name + ".wal")):
                p.unlink(missing_ok=True)
            con.execute(f"ATTACH {sql_str(stage_db)} AS stage")
            site_lake.attach(con, site)
            has_catalog = site_lake.attach_catalog(con)
            catalog_tables: set[str] = set()
            if has_catalog:
                catalog_tables = {r[0] for r in con.execute(
                    "SELECT table_name FROM duckdb_tables() WHERE database_name = ?",
                    [site_lake.CATALOG_ALIAS]).fetchall()}
            else:
                log.info("catalog lake not available: sensor location joins will be NULL")
            missing = [t for t, cols in site_lake.STACK_SCHEMAS.items() if not site_lake.table_exists(con, t)
                       or set(cols) - set(site_lake.table_columns(con, t))]
            if missing:
                with site_lake.commit(con, f"stack {site} schema", {"createdOrExtended": missing}):
                    for t in missing:
                        site_lake.ensure_table(con, t, site_lake.STACK_SCHEMAS[t])
            fps: dict[tuple[str, str], str] = {}
            if site_lake.table_exists(con, "table_catalog"):
                fps = {(r[0], r[1]): r[2] for r in con.execute(
                    f"SELECT tableName, productCode, inputSha256 FROM {site_lake.ALIAS}.table_catalog").fetchall()}
            domains = [f.domain for fs in inputs.values() for f in fs if f.domain]
            ctx = Ctx(site=site, con=con, force=force, has_catalog=has_catalog, catalog_tables=catalog_tables,
                      fingerprints=fps, domain=max(set(domains), key=domains.count) if domains else None)
            for product in sorted(inputs):
                stack_product(ctx, product, inputs[product])
            con.execute("DETACH stage")
            if has_catalog:
                con.execute(f"DETACH {site_lake.CATALOG_ALIAS}")
            site_lake.checkpoint_detach(con, site)
            con.close()
        status = "ok" if not ctx.errors else "partial"
    finally:
        for p in (stage_db, stage_db.with_name(stage_db.name + ".wal")):
            p.unlink(missing_ok=True)
        results = ctx.results if ctx else []
        summary = {"site": site, "runId": rid, "status": status, "products": sorted(inputs),
                   "seconds": round(time.monotonic() - t0, 1), "catalogLake": bool(ctx and ctx.has_catalog),
                   "totals": _totals(results, ctx.errors if ctx else []),
                   "results": results, "errors": ctx.errors if ctx else []}
        env.write_json(paths.reports_dir / "stack_summary.json", summary)
        state.finish_run(scon, rid, "site-stack", site, status,
                         {"units": len(summary["results"]), "errors": len(summary["errors"])})
        scon.close()
    built = [r for r in ctx.results if not r.get("skipped")]
    log.info("stack %s done in %.0fs: %d unit(s) built, %d skipped, %d error(s)", site, time.monotonic() - t0,
             len(built), len(ctx.results) - len(built), len(ctx.errors))
    return 0 if status == "ok" else 2
