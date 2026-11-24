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

import contextlib
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

from .. import duck, env, state
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
    missing: dict[str, int] = field(default_factory=dict)  # product -> downloaded inputs not on disk
    known: set[str] = field(default_factory=set)  # stream unit: its files the lake already held (per-file counts)
    results: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    n_stage: int = 0


# ---------------------------------------------------------------- inputs

def input_files(site: str, products: list[str] | None = None, months: list[str] | None = None, *,
                missing: dict[str, int] | None = None, count_evicted: bool = True) -> dict[str, list[SrcFile]]:
    """Downloaded, non-superseded files per product (from the journal), classified by the shared classifier.

    ``months`` restricts the rows (a stream chunk). ``missing`` (when given) receives, per product, the number
    of downloaded inputs that are not on disk (evicted or deleted): a unit rebuilt without them would lose rows.
    ``count_evicted=False`` (stream chunks) leaves evicted files (``local_evicted = 1``: verified in the Data
    Store, deleted locally, their rows already in the lake) out of ``missing``: a chunk re-planned after a
    re-list stacks only the files on disk, and the evicted files' rows stay in the lake untouched.
    """
    from ..filenames import classify
    from .fetch import SUPERSEDED_SCHEMA

    con = state.connect(state.DB_PATH)
    try:
        con.executescript(SUPERSEDED_SCHEMA)
        where, args = "f.site=? AND f.dl_status='ok' AND s.name IS NULL", [site]
        if products:
            where += f" AND f.product IN ({','.join('?' * len(products))})"
            args += list(products)
        if months:
            where += f" AND f.month IN ({','.join('?' * len(months))})"
            args += list(months)
        rows = con.execute(
            "SELECT f.* FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND "
            f"s.month=f.month AND s.name=f.name WHERE {where} ORDER BY f.product, f.month, f.name", args).fetchall()
    finally:
        con.close()
    out: dict[str, list[SrcFile]] = defaultdict(list)
    for r in rows:
        path = env.ROOT / r["local_path"]
        if not path.exists():
            evicted = "local_evicted" in r.keys() and r["local_evicted"] == 1  # noqa: SIM118 - sqlite3.Row
            if not evicted:
                log.warning("missing local file %s (journal says ok); skipped", r["local_path"])
            if missing is not None and (count_evicted or not evicted):
                missing[r["product"]] = missing.get(r["product"], 0) + 1
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
    duck.register(con, name, tbl)


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
                                  "samples": samples, "builtAt": env.utcnow(), "_expr": exprs[c][0]})
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
    duck.register(ctx.con, name, site_lake.arrow_table([row], site_lake.STACK_SCHEMAS["table_catalog"]))
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
            "builtAt": env.utcnow(), "_files": [f.name for f in files]}  # _files: per-file counts (stream)


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
        _stage_drop(con, "variables_files")
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
    # one row per variables file with its record count (neon.variableCount without re-reading the CSV)
    _stage_from(con, "variables_files",
                f"SELECT {sql_str(product)} AS productCode, m.month, m.sourceFile, m.publicationDate, "
                f"count(r.sourceFile) AS nVariables FROM fmeta_vars m LEFT JOIN stage.variables_raw r "
                f"ON r.sourceFile = m.sourceFile GROUP BY ALL")
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
    # the raw rows (with their file) first: the stream path counts records per file (stack_file_counts)
    _stage_from(con, f"{name}_raw", f"SELECT {sql_str(product)} AS productCode, {col_list}, "
                                    f"m.sourceFile AS __sourceFile, m.month AS __month, "
                                    f"m.publicationDate AS __publication FROM ({sql}) r "
                                    f"JOIN fmeta_{name} m USING (filename)", params)
    content_list = ", ".join(ident(c) for c in content)
    q = (f"SELECT productCode, {content_list}, count(*) AS nRecords, "
         f"count(DISTINCT __sourceFile) AS nFiles, min(__month) AS firstMonth, max(__month) AS lastMonth, "
         f"max(__sourceFile) AS latestSourceFile, max(__publication) AS latestPublication "
         f"FROM stage.{ident(name + '_raw')} GROUP BY ALL")
    return _stage_from(con, name, q)


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
    duck.register(ctx.con, f"src_{name}", pa.Table.from_pylist(rows, schema=pa.schema(fields)))
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
        duck.register(con, "src_sp_drift", site_lake.arrow_table(sp_drift, site_lake.STACK_SCHEMAS["schema_drift"]))
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
        if by_kind.get("variables"):
            site_lake.replace_where(con, "variables_files", "stage.variables_files", "productCode = ?", [product])
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

def stage_file_stats(con: duckdb.DuckDBPyConnection, product: str, neon_table: str, lake_table: str,
                     data_cols: list[str], types: dict[tuple[str, str], tuple[str, str]]) -> tuple[str | None, bool]:
    """Stage ``stage.file_stats`` (and ``stage.file_taxa``) of ``stage.stg`` + ``fmeta``, before dedup.

    Returns (time column, whether the table has taxa).
    """
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
    return time_col, has_taxa


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
    time_col, has_taxa = stage_file_stats(con, product, neon_table, lake_table, data_cols, types)
    # ---- schema drift per header group
    var_fields = {f for (t, f) in types if t == neon_table}
    drift = [drift_row(product, neon_table, lake_table, h, cols, fs,
                       missing=[c for c in data_cols if c not in cols],
                       not_in_variables=[c for c in cols if var_fields and c not in var_fields])
             for h, cols, fs in groups]
    duck.register(con, "src_drift", site_lake.arrow_table(drift, site_lake.STACK_SCHEMAS["schema_drift"]))
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
        duck.register(con, "src_cast", site_lake.arrow_table(fail_rows, site_lake.STACK_SCHEMAS["cast_errors"]))
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

    duck.register(con, name, pa.table({
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
    duck.register(con, "src_lab_drift", site_lake.arrow_table(drift, site_lake.STACK_SCHEMAS["schema_drift"]))
    if fail_rows:
        duck.register(con, "src_lab_cast", site_lake.arrow_table(fail_rows, site_lake.STACK_SCHEMAS["cast_errors"]))
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


OBJDESC_TABLE = "h5_objdesc"
OBJDESC_COLUMNS: dict[str, str] = {"productCode": "VARCHAR", "month": "VARCHAR", "sourceFile": "VARCHAR",
                                   "term": "VARCHAR", "description": "VARCHAR"}


def read_objdesc(path: Path) -> list[tuple[str, str]]:
    """(term, description) rows of an HDF5 file's ``/objDesc`` term dictionary ([] when absent/unreadable)."""
    try:
        import h5py

        with h5py.File(path, "r") as f:
            if "objDesc" not in f:
                return []
            data = f["objDesc"][()]
    except (OSError, KeyError, ImportError, ValueError) as exc:
        log.warning("objDesc of %s unreadable: %s", path.name, exc)
        return []

    def txt(v: Any) -> str:
        return v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v)

    out = []
    for r in data:
        try:
            if len(r) >= 2 and txt(r[0]).strip():
                out.append((txt(r[0]).strip(), txt(r[1])))
        except TypeError:  # not a record row
            continue
    return out


def objdesc_table(product: str, files: list[SrcFile]) -> Any:
    """``h5_objdesc`` rows of the files (the cards describe dp04 columns from it once the .h5 files are gone)."""
    rows = [{"productCode": product, "month": f.month, "sourceFile": f.name, "term": t, "description": d}
            for f in files for t, d in read_objdesc(f.path)]
    return site_lake.arrow_table(rows, OBJDESC_COLUMNS)


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
        duck.register(con, "src_h5_attrs", out["h5_attrs"])
    if out["h5_datasets"] is not None:
        duck.register(con, "src_h5_datasets", out["h5_datasets"])
    duck.register(con, "src_h5_stats", site_lake.arrow_table(out["stats"], site_lake.STACK_SCHEMAS["file_stats"]))
    duck.register(con, "src_h5_objdesc", objdesc_table(product, files))
    n_rows = 0
    with site_lake.commit(con, f"stack {ctx.site} {product} {h5.NEON_TABLE} (h5)",
                          {"product": product, "files": len(files), "missing": out["missing"], "inputSha256": fp}):
        where = f"productCode = ? AND sourceFile IN ({','.join('?' * len(names))})"
        if out["h5_attrs"] is not None:
            site_lake.replace_where(con, "h5_attrs", "src_h5_attrs", where, [product, *names])
        if out["h5_datasets"] is not None:
            site_lake.replace_where(con, "h5_datasets", "src_h5_datasets", where, [product, *names])
        if out["flux"] is not None:
            duck.register(con, "src_h5_flux", out["flux"])
            con.execute(f"CREATE OR REPLACE TABLE {site_lake.ALIAS}.{h5.LAKE_TABLE} AS SELECT * FROM src_h5_flux")
            n_rows = out["flux"].num_rows
        site_lake.replace_where(con, "file_stats", "src_h5_stats", "productCode = ? AND tableName = ?",
                                [product, h5.NEON_TABLE])
        site_lake.ensure_table(con, OBJDESC_TABLE, OBJDESC_COLUMNS)
        site_lake.replace_where(con, OBJDESC_TABLE, "src_h5_objdesc", where, [product, *names])
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
    _forget_counts(ctx, product)
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
    if ctx.missing.get(product):
        # evicted/streamed inputs: the tables this run did not see are not stale, only not on disk
        log.info("stack %s %s: %d input file(s) not on disk; stale-table pruning skipped", ctx.site, product,
                 ctx.missing[product])
    elif not any(e.get("product") == product for e in ctx.errors):
        expected = {lake_table_name(product, t) for t in tables} | {lake_table_name(product, f"lab_{t}") for t in labs}
        prune_stale_tables(ctx, product, expected)


def _forget_counts(ctx: Ctx, product: str) -> None:
    """The whole-product path rewrites the product's figures from all its files: drop the stream path's
    per-file counts of the product, so a later stream chunk seeds them again from the fresh figures."""
    con = ctx.con
    if not site_lake.table_exists(con, "stack_file_counts") or not con.execute(
            f"SELECT 1 FROM {site_lake.ALIAS}.stack_file_counts WHERE productCode = ? LIMIT 1", [product]).fetchone():
        return
    with site_lake.commit(con, f"stack {ctx.site} {product} per-file counts reset", {"product": product}):
        con.execute(f"DELETE FROM {site_lake.ALIAS}.stack_file_counts WHERE productCode = ?", [product])


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
    for table, _n in stale:  # DuckLake removes the files, not the directory
        d = site_lake.site_paths(ctx.site).data_dir / "main" / table
        with contextlib.suppress(OSError):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
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


def _attach_catalog(con: duckdb.DuckDBPyConnection) -> tuple[bool, set[str]]:
    """Attach the catalog lake read-only when available; (attached, its table names)."""
    has_catalog = site_lake.attach_catalog(con)
    if not has_catalog:
        log.info("catalog lake not available: sensor location joins will be NULL")
        return False, set()
    return True, {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = ?",
                                            [site_lake.CATALOG_ALIAS]).fetchall()}


def ensure_stack_schemas(con: duckdb.DuckDBPyConnection, site: str) -> list[str]:
    """Create/extend the contracted stack tables (one snapshot, only when something is missing)."""
    missing = [t for t, cols in site_lake.STACK_SCHEMAS.items() if not site_lake.table_exists(con, t)
               or set(cols) - set(site_lake.table_columns(con, t))]
    if missing:
        with site_lake.commit(con, f"stack {site} schema", {"createdOrExtended": missing}):
            for t in missing:
                site_lake.ensure_table(con, t, site_lake.STACK_SCHEMAS[t])
    return missing


def _fingerprints(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], str]:
    if not site_lake.table_exists(con, "table_catalog"):
        return {}
    return {(r[0], r[1]): r[2] for r in con.execute(
        f"SELECT tableName, productCode, inputSha256 FROM {site_lake.ALIAS}.table_catalog").fetchall()}


def main(site: str, products: list[str] | None = None, force: bool = False) -> int:
    site = site.upper()
    t0 = time.monotonic()
    paths = site_lake.site_paths(site)
    missing: dict[str, int] = {}
    inputs = input_files(site, products, missing=missing)
    if not inputs:
        log.warning("stack %s: no downloaded files%s", site, f" for {products}" if products else "")
        return 1
    from .stream import ensure_lake_data

    why = ensure_lake_data(site, missing="never")  # a stream site cleaned up earlier: its lake data comes back
    if why:
        log.error("stack %s: %s", site, why)
        return 2
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
            has_catalog, catalog_tables = _attach_catalog(con)
            ensure_stack_schemas(con, site)
            domains = [f.domain for fs in inputs.values() for f in fs if f.domain]
            ctx = Ctx(site=site, con=con, force=force, has_catalog=has_catalog, catalog_tables=catalog_tables,
                      fingerprints=_fingerprints(con), domain=max(set(domains), key=domains.count) if domains else None,
                      missing=missing)
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


# ---------------------------------------------------------------- streaming: incremental append of one chunk
#
# ``stack_chunk`` stacks ONE product's files of some months (a stream chunk, docs/STREAMING.md) INTO the
# existing site lake without re-reading earlier chunks' raw files:
#
# * types: the lake ``variables`` of the product merged with this chunk's variables files (newest
#   publication per table/field wins), then the usual 0.1 % cast-failure rule on the chunk's rows;
# * schema growth: new columns -> ``ALTER TABLE ... ADD COLUMN``; a column whose type conflicts with the
#   lake keeps the lake's type (TRY_CAST, failures recorded in ``cast_errors``) unless more than 0.1 % of
#   the chunk's values do not fit it: then the lake column is widened (integer -> DOUBLE when every value
#   is numeric, otherwise VARCHAR; the table is rewritten from the lake itself, no raw file needed);
# * ``uid`` tables: newest publication per uid across the lake and the chunk (the same order as the
#   whole-product path: publicationDate DESC NULLS LAST, sourceFile DESC); lab tables: distinct rows;
# * per file: ``file_stats``/``file_taxa``, ``sensor_positions``, ``eml_index``/``readme_index``,
#   ``variables_files``; merged: ``variables``/``variables_history``, ``categorical_codes``/``validation``/
#   ``science_review_flags``, ``schema_drift``, ``cast_errors``; ``table_catalog`` counts recomputed from the
#   lake (files and Σ rowCount from ``file_stats``, rows from the table);
# * idempotent: every unit is ONE snapshot whose commit message is ``stream stack <SITE> <chunk> <unit>``
#   with the unit's input fingerprint; a re-run skips committed units with the same fingerprint, and a unit
#   re-stacked with other inputs first deletes the rows of its source files (and of superseded files).

STREAM_MSG = "stream stack"
META_ADDED = ("siteID", "domainID", "month", "release", "publicationDate", "sourceFile")
IS_ADDED = ("horizontalPosition", "verticalPosition", "tmi")
LAB_META = ("labName", "publicationDate", "sourceFile")
HISTORY_AGG = {"nFiles": "sum", "firstPublication": "min", "lastPublication": "max", "firstMonth": "min",
               "lastMonth": "max", "latestSourceFile": "max"}
DISTINCT_AGG = {"nRecords": "sum", "nFiles": "sum", "firstMonth": "min", "lastMonth": "max",
                "latestSourceFile": "max", "latestPublication": "max"}
TYPE_ALIASES = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE", "INT": "INTEGER", "INT4": "INTEGER", "INT8": "BIGINT",
                "LONG": "BIGINT", "FLOAT8": "DOUBLE", "TEXT": "VARCHAR", "STRING": "VARCHAR", "BOOL": "BOOLEAN"}
INTEGER_TYPES = ("BIGINT", "INTEGER")


def canon_type(t: str) -> str:
    u = str(t).strip().upper()
    return TYPE_ALIASES.get(u, u)


def chain_fp(prev: str | None, fp: str) -> str:
    """``table_catalog.inputSha256`` of a streamed unit: a hash chain over the chunks stacked into it."""
    return "stream:" + hashlib.sha256(f"{prev or ''}\n{fp}".encode()).hexdigest()


def unit_message(site: str, chunk_id: str, unit: str) -> str:
    return f"{STREAM_MSG} {site} {chunk_id} {unit}"


def done_units(con: duckdb.DuckDBPyConnection, site: str, chunk_id: str) -> dict[str, str]:
    """unit -> input fingerprint of the units of this chunk already committed to the lake."""
    import json

    prefix = unit_message(site, chunk_id, "")
    out: dict[str, str] = {}
    for msg, extra in con.execute(f"SELECT commit_message, commit_extra_info FROM {site_lake.ALIAS}.snapshots() "
                                  "WHERE starts_with(commit_message, ?) ORDER BY snapshot_id", [prefix]).fetchall():
        try:
            out[msg[len(prefix):]] = json.loads(extra or "{}").get("inputSha256") or ""
        except ValueError:
            continue
    return out


def _names(ctx: Ctx, names: set[str] | list[str]) -> str:
    """Register a one-column (sourceFile) relation of file names; returns its name."""
    import pyarrow as pa

    name = f"names_{ctx.n_stage}"
    ctx.n_stage += 1
    duck.register(ctx.con, name, pa.table({"sourceFile": pa.array(sorted(set(names)), type=pa.string())}))
    return name


def _rows_of(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _prev_catalog(con: duckdb.DuckDBPyConnection, table: str, product: str) -> dict[str, Any]:
    if not site_lake.table_exists(con, "table_catalog"):
        return {}
    got = _rows_of(con, f"SELECT * FROM {site_lake.ALIAS}.table_catalog WHERE tableName = ? AND productCode = ?",
                   [table, product])
    return got[0] if got else {}


def _mn(*vals: Any) -> Any:
    xs = [v for v in vals if v is not None]
    return min(xs) if xs else None


def _mx(*vals: Any) -> Any:
    xs = [v for v in vals if v is not None]
    return max(xs) if xs else None


def append_rows(con: duckdb.DuckDBPyConnection, table: str, source: str, where: str | None = None,
                params: list[Any] | None = None) -> int:
    """DELETE ``where`` then INSERT BY NAME ``source`` into ``lake.table`` (inside a commit).

    Creates the table from the source when missing; adds new source columns; a source column whose type
    differs from the lake column is TRY_CAST to the lake type. Returns the inserted rows.
    """
    src_cols = site_lake.relation_columns(con, source)
    n = int(con.execute(f"SELECT count(*) FROM {source}").fetchone()[0])
    tq = f"{site_lake.ALIAS}.{ident(table)}"
    if not site_lake.table_exists(con, table):
        con.execute(f"CREATE TABLE {tq} AS SELECT * FROM {source}")
        return n
    have = site_lake.column_types(con, table)
    for c, t in src_cols.items():
        if c not in have:
            con.execute(f"ALTER TABLE {tq} ADD COLUMN {ident(c)} {t}")
            have[c] = t
    sel = ", ".join(f"TRY_CAST({ident(c)} AS {have[c]}) AS {ident(c)}" if canon_type(have[c]) != canon_type(t)
                    else ident(c) for c, t in src_cols.items())
    if where:
        con.execute(f"DELETE FROM {tq} WHERE {where}", params or [])
    con.execute(f"INSERT INTO {tq} BY NAME SELECT {sel} FROM {source}")
    return n


def _merge_grouped(con: duckdb.DuckDBPyConnection, table: str, product: str, source: str, aggs: dict[str, str],
                   out: str) -> None:
    """Stage ``stage.<out>``: the lake rows of ``product`` UNION the source, re-aggregated over content columns."""
    parts = [f"SELECT * FROM {source}"]
    if site_lake.table_exists(con, table):
        parts.insert(0, f"SELECT * FROM {site_lake.ALIAS}.{ident(table)} WHERE productCode = {sql_str(product)}")
    union = " UNION ALL BY NAME ".join(parts)
    cols = list(site_lake.relation_columns(con, f"({union})"))
    content = [c for c in cols if c not in aggs]
    sel = [ident(c) for c in content] + [f"{fn}({ident(c)}) AS {ident(c)}" for c, fn in aggs.items() if c in cols]
    _stage_from(con, out, f"SELECT {', '.join(sel)} FROM ({union}) GROUP BY ALL")


def stage_merged_variables(ctx: Ctx, product: str, chunk_has_variables: bool
                           ) -> dict[tuple[str, str], tuple[str, str]]:
    """``stage.variables_merged`` = lake ``variables`` of the product + this chunk's (newest publication wins)."""
    con = ctx.con
    parts = []
    if site_lake.table_exists(con, "variables") and {"tableName", "fieldName"} <= set(
            site_lake.table_columns(con, "variables")):
        parts.append(f"SELECT * FROM {site_lake.ALIAS}.variables WHERE productCode = {sql_str(product)}")
    if chunk_has_variables:
        parts.append("SELECT * FROM stage.variables_latest")
    if not parts:
        _stage_drop(con, "variables_merged")
        return {}
    union = " UNION ALL BY NAME ".join(parts)
    have = site_lake.relation_columns(con, f"({union})")
    order = ", ".join(x for x in ("publicationDate DESC NULLS LAST" if "publicationDate" in have else "",
                                  "sourceFile DESC" if "sourceFile" in have else "") if x) or "1"
    _stage_from(con, "variables_merged", f"SELECT * FROM ({union}) QUALIFY row_number() OVER "
                                         f"(PARTITION BY tableName, fieldName ORDER BY {order}) = 1")
    dt = "dataType" if "dataType" in have else "CAST(NULL AS VARCHAR)"
    pf = "pubFormat" if "pubFormat" in have else "CAST(NULL AS VARCHAR)"
    return {(t, f): (d, p) for t, f, d, p in con.execute(
        f"SELECT tableName, fieldName, {dt}, {pf} FROM stage.variables_merged").fetchall()}


def lake_type_expr(col: str, lake_type: str, data_type: str | None, pub_format: str | None) -> str:
    """Expression over ``s.<col>`` (VARCHAR) that yields the lake column's type."""
    x = "s." + ident(col)
    expr, typ = cast_for(col, data_type, pub_format)
    if canon_type(typ) == lake_type:
        return expr
    pattern = java_to_strptime(pub_format).pattern if (data_type or "").strip().lower() == "datetime" else None
    if lake_type == "VARCHAR":
        return x
    if lake_type == "DOUBLE":
        return f"TRY_CAST({x} AS DOUBLE)"
    if lake_type == "BIGINT":
        return cast_for(col, "integer", None)[0]
    if lake_type == "INTEGER":
        return cast_for(col, "dateTime", "yyyy")[0]
    if lake_type == "DATE":
        return f"CAST(try_strptime({x}, {dt_formats(pattern)}) AS DATE)"
    if lake_type == "TIMESTAMP WITH TIME ZONE":
        return ts_expr(x, pattern)
    return f"TRY_CAST({x} AS {lake_type})"


def _fail_counts(con: duckdb.DuckDBPyConnection, source: str, col: str, expr: str) -> tuple[int, int]:
    nn, bad = con.execute(f"SELECT count(s.{ident(col)}), count(s.{ident(col)}) FILTER (WHERE ({expr}) IS NULL) "
                          f"FROM {source} s").fetchone()
    return int(nn), int(bad)


def reconcile_types(ctx: Ctx, product: str, lake_table: str, neon_table: str, data_cols: list[str],
                    decided: dict[str, tuple[str, str]], types: dict[tuple[str, str], tuple[str, str]],
                    source: str = "stage.stg") -> tuple[dict[str, tuple[str, str]], list[dict[str, Any]], dict[str, str]]:
    """Fit the chunk's decided column types to an existing lake table.

    Returns (decided expressions, cast_errors rows, {column: widened lake type}).
    """
    con = ctx.con
    have = {c: canon_type(t) for c, t in site_lake.column_types(con, lake_table).items()}
    rows: list[dict[str, Any]] = []
    widen: dict[str, str] = {}
    out = dict(decided)
    for c in data_cols:
        if c not in have:
            continue
        lt, ct = have[c], canon_type(decided[c][1])
        if lt == ct:
            continue
        x = f"s.{ident(c)}"
        if lt == "VARCHAR":  # the lake kept the column as text: stay text
            out[c] = (x, "VARCHAR")
            continue
        expr = lake_type_expr(c, lt, *types.get((neon_table, c), (None, None)))
        nn, bad = _fail_counts(con, source, c, expr)
        pct = bad / nn if nn else 0.0
        samples = [r[0] for r in con.execute(
            f"SELECT DISTINCT {x} FROM {source} s WHERE {x} IS NOT NULL AND ({expr}) IS NULL LIMIT 5").fetchall()
                   ] if bad else []
        base = {"productCode": product, "tableName": lake_table, "neonTable": neon_table, "columnName": c,
                "targetType": lt, "nonNull": nn, "failures": bad, "failPct": round(100.0 * pct, 6),
                "samples": samples, "builtAt": env.utcnow(), "_expr": expr}
        if pct <= CAST_FAIL_MAX:
            out[c] = (expr, lt)
            if bad:
                rows.append({**base, "action": f"cast to lake type {lt}; failures set NULL"})
            continue
        target, texpr = "VARCHAR", x
        if lt in INTEGER_TYPES:
            d_expr = f"TRY_CAST({x} AS DOUBLE)"
            nn2, bad2 = _fail_counts(con, source, c, d_expr)
            if (bad2 / nn2 if nn2 else 0.0) <= CAST_FAIL_MAX:
                target, texpr = "DOUBLE", d_expr
        widen[c] = target
        out[c] = (texpr, target)
        rows.append({**base, "action": f"widened lake column {lt} -> {target}"})
        log.warning("stack %s %s.%s: %d/%d chunk value(s) do not fit the lake type %s; column widened to %s",
                    ctx.site, lake_table, c, bad, nn, lt, target)
    return out, rows, widen


def _widen(con: duckdb.DuckDBPyConnection, table: str, widen: dict[str, str]) -> None:
    """Rewrite ``lake.table`` with the widened column types (from the lake itself; inside a commit)."""
    if not widen:
        return
    cols = site_lake.table_columns(con, table)
    sel = ", ".join(f"CAST({ident(c)} AS {widen[c]}) AS {ident(c)}" if c in widen else ident(c) for c in cols)
    tq = f"{site_lake.ALIAS}.{ident(table)}"
    con.execute(f"CREATE OR REPLACE TABLE {tq} AS SELECT {sel} FROM {tq}")


def _add_columns(con: duckdb.DuckDBPyConnection, table: str, source: str) -> None:
    have = site_lake.table_columns(con, table)
    for c, t in site_lake.relation_columns(con, source).items():
        if c not in have:
            con.execute(f"ALTER TABLE {site_lake.ALIAS}.{ident(table)} ADD COLUMN {ident(c)} {t}")


# ---- per-file counts: nFiles / nRecords / rowsBeforeDedup / cast failures are recomputed from one row per
# (scope, group, source file) in ``stack_file_counts``, never summed onto the lake's figure: re-stacking a file
# (``--force``, changed inputs, a chunk re-planned after a re-list) replaces that file's contribution only.
# Scopes: ``variables_history`` / ``categorical_codes`` / ``validation`` / ``science_review_flags`` (groupKey =
# md5 of the row's non-NULL content columns), ``catalog:<table>`` (groupKey '', table_catalog nFiles and, for lab
# tables, rowsBeforeDedup), ``drift:<lakeTable|sensor_positions>`` (groupKey = headerSha1) and
# ``cast:<lakeTable>`` (groupKey = column). A scope's first chunk seeds it from the lake's stored figures
# (sourceFile ``<lake>``) when the lake was stacked before these counts existed.
COUNTS_TABLE = "stack_file_counts"
COUNTS_COLUMNS: dict[str, str] = {"productCode": "VARCHAR", "scope": "VARCHAR", "groupKey": "VARCHAR",
                                  "sourceFile": "VARCHAR", "nFiles": "BIGINT", "nRecords": "BIGINT",
                                  "nFailures": "BIGINT"}
LEGACY_SOURCE = "<lake>"
COUNT_SUMS = ("CAST(sum(nFiles) AS BIGINT) AS nFiles, CAST(sum(nRecords) AS BIGINT) AS nRecords, "
              "CAST(sum(nFailures) AS BIGINT) AS nFailures")


def content_key(cols: list[str] | set[str], qual: str = "") -> str:
    """SQL md5 over ``col=value`` of the non-NULL columns (sorted): a column added later (NULL in older rows)
    leaves the key of those rows unchanged."""
    parts = [f"{sql_str(c + '=')} || CAST({qual}{ident(c)} AS VARCHAR)" for c in sorted(set(cols))]
    return f"md5(concat_ws(chr(31), {', '.join(parts)}))" if parts else "md5('')"


def _register_counts(ctx: Ctx, rows: list[dict[str, Any]]) -> str:
    name = f"counts_{ctx.n_stage}"
    ctx.n_stage += 1
    duck.register(ctx.con, name, site_lake.arrow_table(rows, COUNTS_COLUMNS))
    return name


def put_counts(ctx: Ctx, product: str, scope: str, names: str, source: str, legacy_sql: str | None = None) -> None:
    """Inside a unit's commit: seed ``scope`` once from the lake (``legacy_sql``), drop the rows of the unit's
    source (and superseded) files (``names``), then add ``source`` (a relation with COUNTS_COLUMNS).

    The seed is the lake's stored figure minus what this unit's files add when the lake already holds them
    (``ctx.known``: a whole-product stack, or an earlier chunk stacked before the counts existed), so a file
    stacked again is not counted twice."""
    con = ctx.con
    site_lake.ensure_table(con, COUNTS_TABLE, COUNTS_COLUMNS)
    tq = f"{site_lake.ALIAS}.{COUNTS_TABLE}"
    if legacy_sql and not con.execute(f"SELECT 1 FROM {tq} WHERE productCode = ? AND scope = ? LIMIT 1",
                                      [product, scope]).fetchone():
        seed = _rows_of(con, f"SELECT * FROM ({legacy_sql}) WHERE nFiles IS NOT NULL OR nRecords IS NOT NULL "
                             "OR nFailures IS NOT NULL")
        if seed and ctx.known:
            again: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for r in _rows_of(con, f"SELECT * FROM {source}"):
                if r["sourceFile"] in ctx.known:
                    for k in ("nFiles", "nRecords", "nFailures"):
                        again[r["groupKey"]][k] += int(r.get(k) or 0)
            for r in seed:
                for k, n in again.get(r["groupKey"], {}).items():
                    if r.get(k) is not None:
                        r[k] = max(int(r[k]) - n, 0)
        if seed:
            con.execute(f"INSERT INTO {tq} BY NAME SELECT * FROM {_register_counts(ctx, seed)}")
    con.execute(f"DELETE FROM {tq} WHERE productCode = ? AND scope = ? AND sourceFile IN "
                f"(SELECT sourceFile FROM {names})", [product, scope])
    con.execute(f"INSERT INTO {tq} BY NAME SELECT * FROM {source}")


PER_FILE_TABLES = ("file_stats", "variables_files", "sensor_positions", "eml_index", "readme_index", "h5_attrs")


def already_stacked(con: duckdb.DuckDBPyConnection, product: str, files: list[SrcFile],
                    extra_tables: tuple[str, ...] = ()) -> set[str]:
    """The unit's files the lake already reflects (read before the unit writes): their names in a per-file
    table of the product, or, for DP0 support files (categorical codes, validation ...), a month whose variables
    file the lake holds (they are stacked in the same supporting unit)."""
    names = sorted({f.name for f in files})
    if not names:
        return set()
    out: set[str] = set()
    for t in (*PER_FILE_TABLES, *extra_tables):
        if not site_lake.table_exists(con, t):
            continue
        cols = site_lake.table_columns(con, t)
        if "sourceFile" not in cols:
            continue
        where = "sourceFile IN (SELECT unnest(?::VARCHAR[]))"
        args: list[Any] = [names]
        if "productCode" in cols:
            where += " AND productCode = ?"
            args.append(product)
        out |= {r[0] for r in con.execute(f"SELECT DISTINCT sourceFile FROM {site_lake.ALIAS}.{ident(t)} "
                                          f"WHERE {where}", args).fetchall()}
    if site_lake.table_exists(con, "variables_files"):
        months = {r[0] for r in con.execute(f"SELECT DISTINCT month FROM {site_lake.ALIAS}.variables_files "
                                            "WHERE productCode = ?", [product]).fetchall()}
        out |= {f.name for f in files if f.kind in DISTINCT_SUPPORT and f.month in months}
    return out


def counts_of(con: duckdb.DuckDBPyConnection, product: str, scope: str) -> dict[str, dict[str, int | None]]:
    """groupKey -> {'nFiles', 'nRecords', 'nFailures'} (sums over the scope's per-file rows)."""
    if not site_lake.table_exists(con, COUNTS_TABLE):
        return {}
    rows = con.execute(f"SELECT groupKey, {COUNT_SUMS} FROM {site_lake.ALIAS}.{COUNTS_TABLE} WHERE productCode = ? "
                       "AND scope = ? GROUP BY groupKey", [product, scope]).fetchall()
    return {r[0]: {"nFiles": r[1], "nRecords": r[2], "nFailures": r[3]} for r in rows}


def _legacy_grouped_sql(con: duckdb.DuckDBPyConnection, table: str, product: str, scope: str,
                        aggs: dict[str, str]) -> str | None:
    """Seed rows of a merged support table (variables_history, categorical_codes ...) from its stored counts."""
    if not site_lake.table_exists(con, table):
        return None
    cols = site_lake.table_columns(con, table)
    content = [c for c in cols if c not in aggs]
    n_files = "CAST(nFiles AS BIGINT)" if "nFiles" in cols else "CAST(NULL AS BIGINT)"
    n_records = "CAST(nRecords AS BIGINT)" if "nRecords" in cols else "CAST(NULL AS BIGINT)"
    return (f"SELECT productCode, {sql_str(scope)} AS scope, {content_key(content)} AS groupKey, "
            f"{sql_str(LEGACY_SOURCE)} AS sourceFile, {n_files} AS nFiles, {n_records} AS nRecords, "
            f"CAST(NULL AS BIGINT) AS nFailures FROM {site_lake.ALIAS}.{ident(table)} "
            f"WHERE productCode = {sql_str(product)}")


def _legacy_catalog_sql(con: duckdb.DuckDBPyConnection, table: str, product: str, *, rows_before: bool) -> str | None:
    if not site_lake.table_exists(con, "table_catalog"):
        return None
    rec = "CAST(rowsBeforeDedup AS BIGINT)" if rows_before else "CAST(NULL AS BIGINT)"
    return (f"SELECT productCode, {sql_str('catalog:' + table)} AS scope, '' AS groupKey, "
            f"{sql_str(LEGACY_SOURCE)} AS sourceFile, CAST(nFiles AS BIGINT) AS nFiles, {rec} AS nRecords, "
            f"CAST(NULL AS BIGINT) AS nFailures FROM {site_lake.ALIAS}.table_catalog "
            f"WHERE productCode = {sql_str(product)} AND tableName = {sql_str(table)}")


def catalog_counts(ctx: Ctx, product: str, table: str, names: str, files: list[str],
                   records: dict[str, int] | None = None) -> tuple[int, int | None]:
    """(nFiles, Σ records) of a table_catalog row from per-file rows (inside the unit's commit)."""
    scope = f"catalog:{table}"
    rows = [{"productCode": product, "scope": scope, "groupKey": "", "sourceFile": f, "nFiles": 1,
             "nRecords": (records or {}).get(f, 0) if records is not None else None} for f in sorted(set(files))]
    put_counts(ctx, product, scope, names, _register_counts(ctx, rows),
               _legacy_catalog_sql(ctx.con, table, product, rows_before=records is not None))
    got = counts_of(ctx.con, product, scope).get("", {})
    return int(got.get("nFiles") or 0), (None if got.get("nRecords") is None else int(got["nRecords"]))


def grouped_counts(ctx: Ctx, table: str, product: str, names: str, per_file_sql: str, merged: str,
                   aggs: dict[str, str]) -> str:
    """Inside the unit's commit: per-file rows of a merged support table, then ``merged`` (a staged relation)
    with nFiles/nRecords recomputed from them. Returns the registered relation to write."""
    con = ctx.con
    put_counts(ctx, product, table, names, f"({per_file_sql})", _legacy_grouped_sql(con, table, product, table, aggs))
    cols = site_lake.relation_columns(con, merged)
    content = [c for c in cols if c not in aggs]
    rep = ", ".join(f"coalesce(c.{ident(c)}, t.{ident(c)}) AS {ident(c)}" for c in ("nFiles", "nRecords")
                    if c in cols)
    sel = f"t.* REPLACE ({rep})" if rep else "t.*"
    # con.sql, not con.execute: DuckDB 2.0 warns that Arrow output from a DB-API result copies the rows.
    tbl = con.sql(f"SELECT {sel} FROM {merged} t LEFT JOIN (SELECT groupKey, {COUNT_SUMS} FROM "
                  f"{site_lake.ALIAS}.{COUNTS_TABLE} WHERE productCode = ? AND scope = ? GROUP BY groupKey) c "
                  f"ON c.groupKey = {content_key(content, 't.')}", params=[product, table]).to_arrow_table()
    name = f"recount_{ctx.n_stage}"
    ctx.n_stage += 1
    duck.register(con, name, tbl)
    return name


def per_file_grouped_sql(con: duckdb.DuckDBPyConnection, product: str, scope: str, raw: str, source_col: str,
                         meta: set[str], *, records: bool) -> str:
    """Per-file rows (one per distinct content row and file) of a staged raw relation whose non-``meta``
    columns are the merged table's content columns."""
    content = [c for c in site_lake.relation_columns(con, raw) if c not in meta]
    return (f"SELECT {sql_str(product)} AS productCode, {sql_str(scope)} AS scope, groupKey, "
            f"{ident(source_col)} AS sourceFile, CAST(1 AS BIGINT) AS nFiles, "
            f"{'CAST(count(*) AS BIGINT)' if records else 'CAST(NULL AS BIGINT)'} AS nRecords, "
            f"CAST(NULL AS BIGINT) AS nFailures FROM (SELECT *, {content_key(content)} AS groupKey FROM {raw}) "
            f"GROUP BY groupKey, {ident(source_col)}")


def cast_counts_per_file(con: duckdb.DuckDBPyConnection, fail_rows: list[dict[str, Any]], meta: str
                         ) -> dict[str, dict[str, tuple[int, int]]]:
    """column -> {sourceFile: (nonNull, failures)} over ``stage.stg`` for the columns of ``fail_rows``."""
    exprs: dict[str, str] = {}
    for r in fail_rows:
        if r.get("_expr"):
            exprs[r["columnName"]] = r["_expr"]
    if not exprs:
        return {}
    cols = list(exprs)
    aggs = ", ".join(f"count(s.{ident(c)}), count(s.{ident(c)}) FILTER (WHERE ({exprs[c]}) IS NULL)" for c in cols)
    out: dict[str, dict[str, tuple[int, int]]] = {c: {} for c in cols}
    for row in con.execute(f"SELECT m.sourceFile, {aggs} FROM stage.stg s JOIN {meta} m ON s.filename = m.filename "
                           "GROUP BY m.sourceFile").fetchall():
        for i, c in enumerate(cols):
            nn, bad = int(row[1 + 2 * i]), int(row[2 + 2 * i])
            if nn:
                out[c][row[0]] = (nn, bad)
    return out


NEWER = ("(n.publicationDate IS NOT NULL AND (l.publicationDate IS NULL OR n.publicationDate > l.publicationDate)) "
         "OR (n.publicationDate IS NOT DISTINCT FROM l.publicationDate AND n.sourceFile > l.sourceFile)")


def _merge_drift(ctx: Ctx, product: str, key_col: str, key_val: str, new_rows: list[dict[str, Any]], names: str,
                 *, var_fields: set[str] | None = None, recompute_missing: bool = True) -> None:
    """schema_drift of one table: lake rows + this chunk's header groups merged by header (inside a commit);
    nFiles per header comes from the per-file counts (``drift:<key>``)."""
    con = ctx.con
    exists = site_lake.table_exists(con, "schema_drift")
    old = _rows_of(con, f"SELECT * FROM {site_lake.ALIAS}.schema_drift WHERE productCode = ? AND {ident(key_col)} = ?",
                   [product, key_val]) if exists else []
    scope = f"drift:{key_val}"
    per_file = [{"productCode": product, "scope": scope, "groupKey": n["headerSha1"], "sourceFile": fn, "nFiles": 1}
                for n in new_rows for fn in n.get("_files") or []]
    legacy = (f"SELECT productCode, {sql_str(scope)} AS scope, headerSha1 AS groupKey, "
              f"{sql_str(LEGACY_SOURCE)} AS sourceFile, CAST(nFiles AS BIGINT) AS nFiles, "
              f"CAST(NULL AS BIGINT) AS nRecords, CAST(NULL AS BIGINT) AS nFailures FROM "
              f"{site_lake.ALIAS}.schema_drift WHERE productCode = {sql_str(product)} AND "
              f"{ident(key_col)} = {sql_str(key_val)}") if exists else None
    put_counts(ctx, product, scope, names, _register_counts(ctx, per_file), legacy)
    counts = counts_of(con, product, scope)
    by_sha: dict[str, dict[str, Any]] = {r["headerSha1"]: dict(r) for r in old}
    for n in new_rows:
        o = by_sha.get(n["headerSha1"])
        if o is None:
            by_sha[n["headerSha1"]] = dict(n)
            continue
        o.update(firstMonth=_mn(o.get("firstMonth"), n.get("firstMonth")),
                 lastMonth=_mx(o.get("lastMonth"), n.get("lastMonth")),
                 firstPublication=_mn(o.get("firstPublication"), n.get("firstPublication")),
                 lastPublication=_mx(o.get("lastPublication"), n.get("lastPublication")),
                 unknownColumns=n.get("unknownColumns"), builtAt=n.get("builtAt"))
    for sha, r in by_sha.items():
        if counts.get(sha, {}).get("nFiles") is not None:
            r["nFiles"] = int(counts[sha]["nFiles"])
    rows = sorted(by_sha.values(), key=lambda r: (-int(r.get("nFiles") or 0), ",".join(r.get("columns") or [])))
    if recompute_missing:
        union = list(dict.fromkeys(c for r in rows for c in r.get("columns") or []))
        for r in rows:
            cols = r.get("columns") or []
            r["missingVsUnion"] = [c for c in union if c not in cols]
            if var_fields is not None:
                r["notInVariables"] = [c for c in cols if var_fields and c not in var_fields]
    name = f"drift_{ctx.n_stage}"
    ctx.n_stage += 1
    duck.register(con, name, site_lake.arrow_table(rows, site_lake.STACK_SCHEMAS["schema_drift"]))
    site_lake.replace_where(con, "schema_drift", name, f"productCode = ? AND {ident(key_col)} = ?", [product, key_val])


def _merge_cast(ctx: Ctx, product: str, lake_table: str, new_rows: list[dict[str, Any]], names: str,
                per_file: dict[str, dict[str, tuple[int, int]]]) -> None:
    """cast_errors of one lake table: the lake's rows and the chunk's merged per column (inside a commit);
    nonNull/failures come from the per-file counts (``cast:<lakeTable>``), so a re-stacked file counts once."""
    con = ctx.con
    exists = site_lake.table_exists(con, "cast_errors")
    old = _rows_of(con, f"SELECT * FROM {site_lake.ALIAS}.cast_errors WHERE productCode = ? AND tableName = ?",
                   [product, lake_table]) if exists else []
    scope = f"cast:{lake_table}"
    rows = [{"productCode": product, "scope": scope, "groupKey": c, "sourceFile": f, "nRecords": nn, "nFailures": bad}
            for c, by_file in per_file.items() for f, (nn, bad) in by_file.items()]
    legacy = (f"SELECT productCode, {sql_str(scope)} AS scope, columnName AS groupKey, "
              f"{sql_str(LEGACY_SOURCE)} AS sourceFile, CAST(NULL AS BIGINT) AS nFiles, "
              f"CAST(nonNull AS BIGINT) AS nRecords, CAST(failures AS BIGINT) AS nFailures FROM "
              f"{site_lake.ALIAS}.cast_errors WHERE productCode = {sql_str(product)} AND "
              f"tableName = {sql_str(lake_table)}") if exists else None
    put_counts(ctx, product, scope, names, _register_counts(ctx, rows), legacy)
    if not new_rows and not old:
        return
    counts = counts_of(con, product, scope)
    by_col: dict[str, dict[str, Any]] = {r["columnName"]: dict(r) for r in old}
    for n in new_rows:
        o = by_col.get(n["columnName"])
        if o is None:
            by_col[n["columnName"]] = dict(n)
            continue
        o.update(action=n["action"], targetType=n["targetType"],
                 samples=list(dict.fromkeys([*(o.get("samples") or []), *(n.get("samples") or [])]))[:5],
                 builtAt=n["builtAt"])
    for c, r in list(by_col.items()):
        got = counts.get(c)
        if got is None:  # no per-file row at all (a lake stacked before the counts, never seeded): as stored
            continue
        nn, bad = int(got.get("nRecords") or 0), int(got.get("nFailures") or 0)
        if not bad and not any(n["columnName"] == c for n in new_rows):
            del by_col[c]  # no file of the table fails this column any more
            continue
        r.update(nonNull=nn, failures=bad, failPct=round(100.0 * bad / nn, 6) if nn else 0.0)
    name = f"cast_{ctx.n_stage}"
    ctx.n_stage += 1
    duck.register(con, name, site_lake.arrow_table(list(by_col.values()), site_lake.STACK_SCHEMAS["cast_errors"]))
    site_lake.replace_where(con, "cast_errors", name, "productCode = ? AND tableName = ?", [product, lake_table])


def _table_catalog(ctx: Ctx, lake_table: str, product: str, neon_table: str, kind: str, fp: str, *,
                   months: list[str], time_col: str | None, n_files: int | None = None,
                   rows_before: int | None = None) -> None:
    """Recompute one ``table_catalog`` row from the lake after an append (inside the unit's commit)."""
    con = ctx.con
    prev = _prev_catalog(con, lake_table, product)
    tq = f"{site_lake.ALIAS}.{ident(lake_table)}"
    n_rows = int(con.execute(f"SELECT count(*) FROM {tq}").fetchone()[0])
    cols = site_lake.table_columns(con, lake_table)
    first, last = _mn(prev.get("firstMonth"), *months), _mx(prev.get("lastMonth"), *months)
    extra: dict[str, Any] = {}
    if kind in ("data", "h5"):
        nf, total, fm, lm = con.execute(
            f"SELECT count(DISTINCT sourceFile), sum(rowCount), min(month), max(month) FROM {site_lake.ALIAS}.file_stats"
            " WHERE productCode = ? AND lakeTable = ?", [product, lake_table]).fetchone()
        n_files = int(nf or 0)
        first, last = _mn(fm, first), _mx(lm, last)
        if kind == "data":
            rows_before = int(total or 0)
    union: set[str] = set()
    if kind in ("data", "lab") and site_lake.table_exists(con, "schema_drift"):
        for (c,) in con.execute(f"SELECT DISTINCT unnest(columns) FROM {site_lake.ALIAS}.schema_drift "
                                "WHERE productCode = ? AND lakeTable = ?", [product, lake_table]).fetchall():
            union.add(c)
        data_cols = [c for c in cols if c in union]
        n_typed = sum(1 for c in data_cols if canon_type(cols[c]) != "VARCHAR")
        extra = {"nTyped": n_typed, "nVarchar": len(data_cols) - n_typed}
    if rows_before is not None:
        extra.update(rowsBeforeDedup=rows_before, dedupRemoved=rows_before - n_rows)
    tc = _catalog_row(ctx, lake_table, product, neon_table, kind,
                      n_files if n_files is not None else int(prev.get("nFiles") or 0), n_rows, len(cols),
                      [m for m in (first, last) if m], time_col or prev.get("timeColumn"),
                      chain_fp(prev.get("inputSha256"), fp), **extra)
    _write_catalog(ctx, tc, lake_table, product)


def _commit_extra(ctx: Ctx, chunk_id: str, unit: str, product: str, fp: str, **kw: Any) -> dict[str, Any]:
    return {"chunkId": chunk_id, "unit": unit, "product": product, "inputSha256": fp, **kw}


def append_support(ctx: Ctx, product: str, by_kind: dict[str, list[SrcFile]], chunk_id: str, fp: str,
                   superseded: set[str]) -> dict[str, Any]:
    """The chunk's supporting files merged into the lake (one snapshot)."""
    con, site = ctx.con, ctx.site
    t0 = time.monotonic()
    files = [f for k in SUPPORT_KINDS for f in by_kind.get(k, [])]
    months = sorted({f.month for f in files})
    stats: dict[str, int] = {}
    sp_drift: list[dict[str, Any]] = []
    sp_extra: list[str] = []
    if by_kind.get("variables"):
        _merge_grouped(con, "variables_history", product, "stage.variables_history", HISTORY_AGG,
                       "variables_history_merged")
    if by_kind.get("sensor_positions"):
        stats["sensor_positions"], sp_drift, sp_extra = stage_sensor_positions(ctx, product,
                                                                               by_kind["sensor_positions"])
    distinct: list[tuple[str, str]] = []
    for kind, table in DISTINCT_SUPPORT.items():
        if by_kind.get(kind) and stage_distinct(ctx, table, product, by_kind[kind]):
            _merge_grouped(con, table, product, f"stage.{table}", DISTINCT_AGG, f"{table}_merged")
            distinct.append((kind, table))
    for kind, table in (("eml", "eml_index"), ("readme", "readme_index")):
        if by_kind.get(kind):
            stats[table] = stage_index(ctx, table, product, by_kind[kind])
    names = _names(ctx, {f.name for f in files} | superseded)
    by_name = f"productCode = ? AND sourceFile IN (SELECT sourceFile FROM {names})"
    written: list[tuple[str, int]] = []  # (table, files of the chunk)
    if by_kind.get("variables"):
        hist_per_file = per_file_grouped_sql(
            con, product, "variables_history",
            f"(SELECT {sql_str(product)} AS productCode, * EXCLUDE (filename) FROM stage.variables_raw)", "sourceFile",
            {"sourceFile", "month", "publicationDate"}, records=False)
    distinct_per_file = {table: per_file_grouped_sql(con, product, table, f"stage.{ident(table + '_raw')}",
                                                     "__sourceFile", {"__sourceFile", "__month", "__publication"},
                                                     records=True) for _kind, table in distinct}
    with site_lake.commit(con, unit_message(site, chunk_id, "supporting"),
                          _commit_extra(ctx, chunk_id, "supporting", product, fp, months=months)):
        if by_kind.get("variables"):
            site_lake.replace_where(con, "variables", "stage.variables_merged", "productCode = ?", [product])
            hist = grouped_counts(ctx, "variables_history", product, names, hist_per_file,
                                  "stage.variables_history_merged", HISTORY_AGG)
            site_lake.replace_where(con, "variables_history", hist, "productCode = ?", [product])
            site_lake.replace_where(con, "variables_files", "stage.variables_files", by_name, [product])
            written += [("variables", len(by_kind["variables"])), ("variables_history", len(by_kind["variables"]))]
        if by_kind.get("sensor_positions"):
            append_rows(con, "sensor_positions", "stage.sensor_positions", by_name, [product])
            _merge_drift(ctx, product, "tableName", "sensor_positions", sp_drift, names, recompute_missing=False)
            written.append(("sensor_positions", len(by_kind["sensor_positions"])))
        for kind, table in distinct:
            merged = grouped_counts(ctx, table, product, names, distinct_per_file[table], f"stage.{table}_merged",
                                    DISTINCT_AGG)
            site_lake.replace_where(con, table, merged, "productCode = ?", [product])
            written.append((table, len(by_kind[kind])))
        for kind, table in (("eml", "eml_index"), ("readme", "readme_index")):
            if by_kind.get(kind):
                site_lake.replace_where(con, table, f"src_{table}", by_name, [product])
                written.append((table, len(by_kind[kind])))
        for table, _n_chunk in written:
            q = f"{site_lake.ALIAS}.{ident(table)}"
            if table in ("variables", "variables_history"):
                n_files = con.execute(f"SELECT count(DISTINCT sourceFile) FROM {site_lake.ALIAS}.variables_files "
                                      "WHERE productCode = ?", [product]).fetchone()[0]
            elif table in ("sensor_positions", "eml_index", "readme_index"):
                n_files = con.execute(f"SELECT count(DISTINCT sourceFile) FROM {q} WHERE productCode = ?",
                                      [product]).fetchone()[0]
            else:  # distinct support tables: one per-file row each, re-stacked files replace theirs
                kind = next(k for k, t in DISTINCT_SUPPORT.items() if t == table)
                n_files, _ = catalog_counts(ctx, product, table, names, [f.name for f in by_kind[kind]])
            stats.setdefault(table, int(con.execute(f"SELECT count(*) FROM {q} WHERE productCode = ?",
                                                    [product]).fetchone()[0]))
            prev = _prev_catalog(con, table, product)
            n_rows = int(con.execute(f"SELECT count(*) FROM {q} WHERE productCode = ?", [product]).fetchone()[0])
            ms = [m for m in (_mn(prev.get("firstMonth"), *months), _mx(prev.get("lastMonth"), *months)) if m]
            tc = _catalog_row(ctx, table, product, table, "support", int(n_files), n_rows,
                              len(site_lake.table_columns(con, table)), ms, None, chain_fp(prev.get("inputSha256"), fp))
            _write_catalog(ctx, tc, table, product)
    res = {"product": product, "unit": "supporting", "rows": stats, "tables": [t for t, _n in written],
           "seconds": round(time.monotonic() - t0, 2)}
    if sp_extra:
        res["sensorPositionsExtraColumns"] = sp_extra
    log.info("stack %s %s %s supporting: %s (%.1fs)", site, chunk_id, product, stats, res["seconds"])
    return res


def append_table(ctx: Ctx, product: str, neon_table: str, files: list[SrcFile],
                 types: dict[tuple[str, str], tuple[str, str]], chunk_id: str, fp: str,
                 superseded: set[str]) -> dict[str, Any]:
    """One data table of the chunk appended to ``<dp>__<table>`` (one snapshot)."""
    con, site = ctx.con, ctx.site
    lake_table = lake_table_name(product, neon_table)
    t0 = time.monotonic()
    groups = group_by_header(files)
    if not groups:
        return {"product": product, "table": lake_table, "skipped": True, "reason": "no non-empty files"}
    register_fmeta(con, site, ctx.domain, [f for _h, _c, fs in groups for f in fs])
    n_raw, lenient = _stage_read(con, product, neon_table, groups)
    data_cols: list[str] = []
    for _h, cols, _fs in groups:
        data_cols += [c for c in cols if c not in data_cols]
    is_table = any(f.hor for f in files)
    exprs = {c: cast_for(c, *types.get((neon_table, c), (None, None))) for c in data_cols}
    decided, fail_rows, _typed = decide_types(con, product, lake_table, neon_table, data_cols, exprs)
    time_col, has_taxa = stage_file_stats(con, product, neon_table, lake_table, data_cols, types)
    var_fields = {f for (t, f) in types if t == neon_table}
    drift = [drift_row(product, neon_table, lake_table, h, cols, fs, missing=[c for c in data_cols if c not in cols],
                       not_in_variables=[c for c in cols if var_fields and c not in var_fields])
             for h, cols, fs in groups]
    exists = site_lake.table_exists(con, lake_table)
    widen: dict[str, str] = {}
    if exists:
        decided, conflict_rows, widen = reconcile_types(ctx, product, lake_table, neon_table, data_cols, decided,
                                                        types)
        fail_rows += conflict_rows
    added = [*META_ADDED, *(IS_ADDED if is_table else ())]
    select = [f"{decided[c][0]} AS {ident(c)}" for c in data_cols] + [f"m.{ident(c)}" for c in added
                                                                       if c not in data_cols]
    has_uid = "uid" in data_cols
    dedup = (" QUALIFY s.uid IS NULL OR row_number() OVER (PARTITION BY s.uid ORDER BY m.publicationDate DESC "
             "NULLS LAST, m.sourceFile DESC) = 1") if has_uid else ""
    n_new = _stage_from(con, "new", f"SELECT {', '.join(select)} FROM stage.stg s JOIN fmeta m "
                                    f"ON s.filename = m.filename{dedup}")
    names = _names(ctx, {f.name for f in files} | superseded)
    cast_files = cast_counts_per_file(con, fail_rows, "fmeta")
    tq = f"{site_lake.ALIAS}.{ident(lake_table)}"
    extra = _commit_extra(ctx, chunk_id, f"data {neon_table}", product, fp, neonTable=neon_table, files=len(files),
                          headerGroups=len(groups), rowsStaged=n_raw, lenientRead=lenient, widened=widen)
    with site_lake.commit(con, unit_message(site, chunk_id, f"data {neon_table}"), extra):
        if not exists:
            con.execute(f"CREATE TABLE {tq} AS SELECT * FROM stage.new")
        else:
            _widen(con, lake_table, widen)
            _add_columns(con, lake_table, "stage.new")
            con.execute(f"DELETE FROM {tq} WHERE sourceFile IN (SELECT sourceFile FROM {names})")
            if has_uid:
                con.execute(f"DELETE FROM {tq} l WHERE l.uid IS NOT NULL AND EXISTS (SELECT 1 FROM stage.new n "
                            f"WHERE n.uid = l.uid AND ({NEWER}))")
                con.execute(f"INSERT INTO {tq} BY NAME SELECT n.* FROM stage.new n WHERE n.uid IS NULL OR NOT EXISTS "
                            f"(SELECT 1 FROM {tq} l WHERE l.uid = n.uid)")
            else:
                con.execute(f"INSERT INTO {tq} BY NAME SELECT * FROM stage.new")
        where, args = f"productCode = ? AND tableName = ? AND sourceFile IN (SELECT sourceFile FROM {names})", [
            product, neon_table]
        site_lake.replace_where(con, "file_stats", "stage.file_stats", where, args)
        if has_taxa:
            site_lake.replace_where(con, "file_taxa", "stage.file_taxa", where, args)
        elif site_lake.table_exists(con, "file_taxa"):
            con.execute(f"DELETE FROM {site_lake.ALIAS}.file_taxa WHERE {where}", args)
        _merge_drift(ctx, product, "lakeTable", lake_table, drift, names, var_fields=var_fields)
        _merge_cast(ctx, product, lake_table, fail_rows, names, cast_files)
        _table_catalog(ctx, lake_table, product, neon_table, "data", fp, months=sorted({f.month for f in files}),
                       time_col=time_col)
        n_rows = int(con.execute(f"SELECT count(*) FROM {tq}").fetchone()[0])
    _stage_drop(con, "stg")
    _stage_drop(con, "new")
    res = {"product": product, "table": lake_table, "files": len(files), "headerGroups": len(groups),
           "rowsStaged": n_raw, "rowsNew": n_new, "rows": n_rows, "created": not exists, "widened": widen,
           "castErrors": len(fail_rows), "lenientRead": lenient, "seconds": round(time.monotonic() - t0, 2)}
    log.info("stack %s %s %s: +%d row(s) from %d file(s) -> %d rows%s (%.1fs)", site, chunk_id, lake_table, n_new,
             len(files), n_rows, f", widened {widen}" if widen else "", res["seconds"])
    return res


def append_lab(ctx: Ctx, product: str, neon_table: str, files: list[SrcFile],
               types: dict[tuple[str, str], tuple[str, str]], chunk_id: str, fp: str,
               superseded: set[str]) -> dict[str, Any]:
    """The chunk's lab files of one table merged into ``<dp>__lab_<table>`` (distinct rows; one snapshot)."""
    con, site = ctx.con, ctx.site
    lake_table = lake_table_name(product, f"lab_{neon_table}")
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
    if var_fields:
        exprs = {c: cast_for(c, *types.get((neon_table, c), (None, None))) for c in data_cols}
        decided, fail_rows, _typed = decide_types(con, product, lake_table, neon_table, data_cols, exprs)
    else:
        decided = auto_types(con, "stage.stg", data_cols)
    exists = site_lake.table_exists(con, lake_table)
    widen: dict[str, str] = {}
    if exists:
        decided, conflict_rows, widen = reconcile_types(ctx, product, lake_table, neon_table, data_cols, decided,
                                                        types)
        fail_rows += conflict_rows
    meta = [c for c in LAB_META if c not in data_cols]
    select = [f"{decided[c][0]} AS {ident(c)}" for c in data_cols] + [f"m.{ident(c)}" for c in meta]
    identity = ", ".join([f"s.{ident(c)}" for c in data_cols] + (["m.labName"] if "labName" in meta else []))
    n_new = _stage_from(con, "new", f"SELECT {', '.join(select)} FROM stage.stg s JOIN fmeta_lab m "
                                    f"ON s.filename = m.filename QUALIFY row_number() OVER (PARTITION BY {identity} "
                                    f"ORDER BY m.publicationDate DESC NULLS LAST, m.sourceFile DESC) = 1")
    drift = [drift_row(product, neon_table, lake_table, h, cols, fs, missing=[c for c in data_cols if c not in cols],
                       not_in_variables=[c for c in cols if var_fields and c not in var_fields])
             for h, cols, fs in groups]
    names = _names(ctx, {f.name for f in files} | superseded)
    cast_files = cast_counts_per_file(con, fail_rows, "fmeta_lab")
    raw_by_file = {str(r[0]): int(r[1]) for r in con.execute(
        "SELECT m.sourceFile, count(*) FROM stage.stg s JOIN fmeta_lab m ON s.filename = m.filename "
        "GROUP BY m.sourceFile").fetchall()}
    tq = f"{site_lake.ALIAS}.{ident(lake_table)}"
    extra = _commit_extra(ctx, chunk_id, f"lab {neon_table}", product, fp, neonTable=neon_table, kind="lab",
                          files=len(files), rowsStaged=n_raw, widened=widen)
    with site_lake.commit(con, unit_message(site, chunk_id, f"lab {neon_table}"), extra):
        if not exists:
            con.execute(f"CREATE TABLE {tq} AS SELECT * FROM stage.new")
        else:
            _widen(con, lake_table, widen)
            _add_columns(con, lake_table, "stage.new")
            con.execute(f"DELETE FROM {tq} WHERE sourceFile IN (SELECT sourceFile FROM {names})")
            new_cols = set(site_lake.relation_columns(con, "stage.new"))
            ident_cols = [c for c in site_lake.table_columns(con, lake_table) if c not in ("publicationDate",
                                                                                         "sourceFile")]
            same = " AND ".join(f"l.{ident(c)} IS NOT DISTINCT FROM n.{ident(c)}" if c in new_cols
                                else f"l.{ident(c)} IS NULL" for c in ident_cols) or "true"
            con.execute(f"DELETE FROM {tq} l WHERE EXISTS (SELECT 1 FROM stage.new n WHERE {same} AND ({NEWER}))")
            con.execute(f"INSERT INTO {tq} BY NAME SELECT n.* FROM stage.new n WHERE NOT EXISTS "
                        f"(SELECT 1 FROM {tq} l WHERE {same})")
        _merge_drift(ctx, product, "lakeTable", lake_table, drift, names, var_fields=var_fields)
        _merge_cast(ctx, product, lake_table, fail_rows, names, cast_files)
        n_files, rows_before = catalog_counts(ctx, product, lake_table, names, [f.name for f in files],
                                              {f.name: raw_by_file.get(f.name, 0) for f in files})
        _table_catalog(ctx, lake_table, product, neon_table, "lab", fp, months=sorted({f.month for f in files}),
                       time_col=None, n_files=n_files, rows_before=rows_before)
        n_rows = int(con.execute(f"SELECT count(*) FROM {tq}").fetchone()[0])
    _stage_drop(con, "stg")
    _stage_drop(con, "new")
    res = {"product": product, "table": lake_table, "kind": "lab", "files": len(files), "rowsStaged": n_raw,
           "rowsNew": n_new, "rows": n_rows, "created": not exists, "widened": widen, "castErrors": len(fail_rows),
           "lenientRead": lenient, "seconds": round(time.monotonic() - t0, 2)}
    log.info("stack %s %s %s: +%d distinct row(s) from %d lab file(s) -> %d rows (%.1fs)", site, chunk_id, lake_table,
             n_new, len(files), n_rows, res["seconds"])
    return res


def append_h5(ctx: Ctx, product: str, files: list[SrcFile], chunk_id: str, fp: str,
              superseded: set[str]) -> dict[str, Any]:
    """The chunk's DP4.00200.001 HDF5 files appended (attrs, datasets, the dp04 table, file_stats)."""
    from . import h5

    con, site = ctx.con, ctx.site
    t0 = time.monotonic()
    out = h5.build_tables(site, ctx.domain, product, files)
    names = _names(ctx, {f.name for f in files} | superseded)
    by_name = f"productCode = ? AND sourceFile IN (SELECT sourceFile FROM {names})"
    if out["h5_attrs"] is not None:
        duck.register(con, "src_h5_attrs", out["h5_attrs"])
    if out["h5_datasets"] is not None:
        duck.register(con, "src_h5_datasets", out["h5_datasets"])
    duck.register(con, "src_h5_stats", site_lake.arrow_table(out["stats"], site_lake.STACK_SCHEMAS["file_stats"]))
    duck.register(con, "src_h5_objdesc", objdesc_table(product, files))
    tq = f"{site_lake.ALIAS}.{ident(h5.LAKE_TABLE)}"
    with site_lake.commit(con, unit_message(site, chunk_id, "h5"),
                          _commit_extra(ctx, chunk_id, "h5", product, fp, files=len(files), missing=out["missing"])):
        if out["h5_attrs"] is not None:
            site_lake.replace_where(con, "h5_attrs", "src_h5_attrs", by_name, [product])
        if out["h5_datasets"] is not None:
            site_lake.replace_where(con, "h5_datasets", "src_h5_datasets", by_name, [product])
        if out["flux"] is not None:
            duck.register(con, "src_h5_flux", out["flux"])
            append_rows(con, h5.LAKE_TABLE, "src_h5_flux", f"sourceFile IN (SELECT sourceFile FROM {names})")
        elif site_lake.table_exists(con, h5.LAKE_TABLE):
            con.execute(f"DELETE FROM {tq} WHERE sourceFile IN (SELECT sourceFile FROM {names})")
        site_lake.replace_where(con, "file_stats", "src_h5_stats", f"tableName = ? AND {by_name}",
                                [h5.NEON_TABLE, product])
        site_lake.ensure_table(con, OBJDESC_TABLE, OBJDESC_COLUMNS)
        site_lake.replace_where(con, OBJDESC_TABLE, "src_h5_objdesc", by_name, [product])
        n_rows = 0
        if site_lake.table_exists(con, h5.LAKE_TABLE):
            _table_catalog(ctx, h5.LAKE_TABLE, product, h5.NEON_TABLE, "h5", fp,
                           months=sorted({f.month for f in files}), time_col="timeBgn")
            n_rows = int(con.execute(f"SELECT count(*) FROM {tq}").fetchone()[0])
    res = {"product": product, "table": h5.LAKE_TABLE, "files": len(files), "rows": n_rows,
           "missingPaths": out["missing"], "seconds": round(time.monotonic() - t0, 2)}
    log.info("stack %s %s %s: %d h5 file(s) -> %d rows (%.1fs)", site, chunk_id, h5.LAKE_TABLE, len(files), n_rows,
             res["seconds"])
    return res


def _run_unit(ctx: Ctx, done: dict[str, str], unit: str, fp: str, fn: Any, *args: Any) -> None:
    """Run one chunk unit unless it was committed with the same inputs; errors are recorded, not raised."""
    product = args[0] if args else None
    if not ctx.force and done.get(unit) == fp:
        ctx.results.append({"product": product, "unit": unit, "skipped": True, "reason": "already stacked"})
        return
    try:
        unit_files = [f for a in args if isinstance(a, list) for f in a if isinstance(f, SrcFile)]
        unit_files += [f for a in args if isinstance(a, dict) for fs in a.values() if isinstance(fs, list)
                       for f in fs if isinstance(f, SrcFile)]
        lab = [lake_table_name(product, f"lab_{args[1]}")] if unit.startswith("lab ") and len(args) > 1 else []
        ctx.known = already_stacked(ctx.con, product, unit_files, tuple(t for t in lab
                                                                         if site_lake.table_exists(ctx.con, t)))
        res = fn(ctx, *args)
        ctx.results.append({"unit": unit, **res})
    except Exception as exc:  # noqa: BLE001 - one unit must not abort the chunk
        ctx.errors.append({"product": product, "unit": unit, "error": str(exc)[:500]})
        log.error("stack %s %s: unit %s failed: %s", ctx.site, product, unit, str(exc)[:300])
        _stage_drop(ctx.con, "stg")
        _stage_drop(ctx.con, "new")


def stack_chunk_product(ctx: Ctx, product: str, files: list[SrcFile], chunk_id: str, superseded: set[str]) -> None:
    """Every unit of one product's chunk files (supporting, data tables, HDF5, lab tables)."""
    done = done_units(ctx.con, ctx.site, chunk_id)
    by_kind: dict[str, list[SrcFile]] = defaultdict(list)
    for f in files:
        by_kind[f.kind].append(f)
    try:
        stage_variables(ctx, product, by_kind.get("variables", []))
        types = stage_merged_variables(ctx, product, bool(by_kind.get("variables")))
    except Exception as exc:  # noqa: BLE001 - the chunk's variables are unreadable: the lake's still type
        ctx.errors.append({"product": product, "unit": "variables", "error": str(exc)[:500]})
        log.error("stack %s %s variables failed (%s); typing from the lake only", ctx.site, product, str(exc)[:300])
        by_kind = defaultdict(list, {k: v for k, v in by_kind.items() if k != "variables"})
        types = stage_merged_variables(ctx, product, False)
    support = [f for k in SUPPORT_KINDS for f in by_kind.get(k, [])]
    if support:
        _run_unit(ctx, done, "supporting", fingerprint(support, "supporting"), append_support, product, by_kind,
                  chunk_id, fingerprint(support, "supporting"), superseded)
    tables: dict[str, list[SrcFile]] = defaultdict(list)
    h5_files: list[SrcFile] = []
    for f in by_kind.get("data", []):
        if f.ext == "h5":
            h5_files.append(f)
        elif f.ext == "csv" and f.table:
            tables[f.table].append(f)
    for table in sorted(tables):
        fp = fingerprint(tables[table], f"data {table}")
        _run_unit(ctx, done, f"data {table}", fp, append_table, product, table, tables[table], types, chunk_id, fp,
                  superseded)
    if h5_files:
        fp = fingerprint(h5_files, "h5")
        _run_unit(ctx, done, "h5", fp, append_h5, product, h5_files, chunk_id, fp, superseded)
    labs: dict[str, list[SrcFile]] = defaultdict(list)
    for f in by_kind.get("lab", []):
        if f.ext == "csv" and f.table:
            labs[f.table].append(f)
    for table in sorted(labs):
        fp = fingerprint(labs[table], f"lab {table}")
        _run_unit(ctx, done, f"lab {table}", fp, append_lab, product, table, labs[table], types, chunk_id, fp,
                  superseded)


def _superseded_names(site: str, product: str) -> set[str]:
    from .fetch import SUPERSEDED_SCHEMA

    con = state.connect(state.DB_PATH)
    try:
        con.executescript(SUPERSEDED_SCHEMA)
        return {r[0] for r in con.execute("SELECT name FROM fetch_superseded WHERE site=? AND product=?",
                                          (site, product))}
    finally:
        con.close()


def stack_chunk(site: str, product: str, months: list[str], *, chunk_id: str, force: bool = False,
                required_chunks: list[str] | None = None) -> dict[str, Any]:
    """Append one stream chunk (``product`` × ``months``) to the site lake; see the section comment above.

    Only the chunk's files on disk are stacked: evicted files (in the Data Store, their rows already in the
    lake) are skipped, a downloaded file that is neither on disk nor evicted fails the chunk.
    ``required_chunks``: chunk ids the local lake must already hold ``stream stack <SITE> <chunk>`` snapshots
    of (every chunk stacked earlier); when one is missing (a new VM without the lake, a lost local lake)
    nothing is written and the status is ``not_ready`` (restore the lake from the Data Store first).

    Returns a summary with ``status``: ``ok``, ``partial`` (some unit failed; ``errors``), ``failed`` (inputs
    missing, or the site's lake data was cleaned up), ``not_ready`` or ``empty`` (no file of the chunk on disk);
    ``lastSnapshot`` is the newest snapshot id of the chunk's units in the lake (None when it has none).
    """
    site = site.upper()
    months = sorted(set(months))
    t0 = time.monotonic()
    missing: dict[str, int] = {}
    files = input_files(site, [product], months, missing=missing, count_evicted=False).get(product, [])
    summary: dict[str, Any] = {"site": site, "chunkId": chunk_id, "product": product, "months": months,
                               "files": len(files), "results": [], "errors": [], "lastSnapshot": None}
    if missing.get(product):
        summary["errors"].append({"product": product, "unit": "inputs",
                                  "error": f"{missing[product]} downloaded input file(s) of the chunk are not on disk"})
        summary["status"] = "failed"
        return summary
    required = sorted(set(required_chunks or []) - {chunk_id})
    paths = site_lake.site_paths(site)
    if required and not paths.catalog_file.exists():
        summary["errors"].append({"product": product, "unit": "lake", "error": (
            f"the site lake is missing locally but {len(required)} chunk(s) were stacked into it: "
            "restore it from the Data Store (stream.restore_lake) first")})
        summary["status"] = "not_ready"
        return summary
    if not files:
        summary["status"] = "empty"
        return summary
    from .chunks import site_cleaned

    if site_cleaned(site):
        summary["errors"].append({"product": product, "unit": "lake", "error": "the site lake's data/** was cleaned "
                                  "up (it lives in the Data Store): restore it before stacking more chunks"})
        summary["status"] = "failed"
        return summary
    superseded = _superseded_names(site, product)
    stage_db = paths.tmp_dir / f"stack_stage_{re.sub(r'[^A-Za-z0-9._-]', '_', chunk_id)}.duckdb"
    ctx: Ctx | None = None
    try:
        with env.file_lock("stack", blocking=True), env.file_lock(f"site-lake-{site}", blocking=True):
            con = site_lake.connect(site)
            try:
                for p in (stage_db, stage_db.with_name(stage_db.name + ".wal")):
                    p.unlink(missing_ok=True)
                site_lake.attach(con, site)
                lacking = [c for c in required if c not in site_lake.stream_chunk_ids(con, site)]
                if lacking:
                    site_lake.checkpoint_detach(con, site)
                    summary["errors"].append({"product": product, "unit": "lake", "error": (
                        f"the local site lake lacks the snapshots of {len(lacking)} stacked chunk(s) "
                        f"({', '.join(lacking[:3])}{' ...' if len(lacking) > 3 else ''}): restore it from the "
                        "Data Store (stream.restore_lake) first")})
                    summary["status"] = "not_ready"
                    return summary
                con.execute(f"ATTACH {sql_str(stage_db)} AS stage")
                has_catalog, catalog_tables = _attach_catalog(con)
                ensure_stack_schemas(con, site)
                domains = [f.domain for f in files if f.domain]
                ctx = Ctx(site=site, con=con, force=force, has_catalog=has_catalog, catalog_tables=catalog_tables,
                          fingerprints=_fingerprints(con),
                          domain=max(set(domains), key=domains.count) if domains else None)
                stack_chunk_product(ctx, product, files, chunk_id, superseded)
                summary["lastSnapshot"] = site_lake.stream_chunk_ids(con, site).get(chunk_id)
                con.execute("DETACH stage")
                if has_catalog:
                    con.execute(f"DETACH {site_lake.CATALOG_ALIAS}")
                site_lake.checkpoint_detach(con, site)
            finally:
                con.close()
    finally:
        for p in (stage_db, stage_db.with_name(stage_db.name + ".wal")):
            p.unlink(missing_ok=True)
    assert ctx is not None
    built = [r for r in ctx.results if not r.get("skipped")]
    summary.update(results=ctx.results, errors=ctx.errors, status="ok" if not ctx.errors else "partial",
                   unitsBuilt=len(built), unitsSkipped=len(ctx.results) - len(built),
                   seconds=round(time.monotonic() - t0, 1))
    log.info("stack chunk %s %s: %d unit(s) built, %d skipped, %d error(s) in %.1fs", site, chunk_id, len(built),
             len(ctx.results) - len(built), len(ctx.errors), summary["seconds"])
    return summary
