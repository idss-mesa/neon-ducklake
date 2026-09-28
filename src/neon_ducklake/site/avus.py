"""`site build-avus <SITE>` — the desired AVUs of one site subtree (``avu_desired`` scope = site code).

Levels (plan § AVU vocabulary + Data-object profiles; ``neon.<lowerCamelCase>`` only):

- ``site``       ``sites/<SITE>``: identity, ``neon.habitat`` (catalog HABITAT), location, counts, coverage,
                 provenance, ``mesa.ducklake.catalog``, the curated ENVO biome and, at an aquatic site, the curated
                 ENVO water-body feature (``envo.<label> = site name``); NLCD classes >= 1 % area at terrestrial
                 sites only; canonical ``datacite.*``. TypeEco is never used for an aquatic site (NEON copied a
                 shortgrass-steppe TypeEco to all of them): no ``neon.typeEco``, and the biome value is HABITAT.
- ``container``  ``sites/<SITE>/{raw,ducklake,cards,curation,reports}`` and ``ref/`` (if uploaded).
- ``lake-dir`` / ``side-dir``  sub-collections of ``ducklake/data/**`` and of cards/curation/reports, so that
                 every collection of the subtree carries a ``neon.*``/``mesa.*`` AVU (verify rule).
- ``product``    ``raw/<DP>``: record, releases (DOI units), month ranges, counts, tables, ``uo.*``,
                 ``ncbitaxon.*`` (union of resolved taxa), accepted curated terms, canonical ``datacite.*``.
- ``month``      ``raw/<DP>/<YYYY-MM>``: release, generation date, counts, coverage, IS/OS summaries.
- ``object``     every site-lake ``files`` row, profile by kind: IS data, OS data, HDF5, supporting, lab
                 (``neon.table`` + ``neon.labName``). A file whose bytes match the storage md5 but not NEON's
                 listed md5 (``transferVerified`` and not ``md5Verified``, contract (b)) gets ``neon.md5Verified =
                 false`` and ``neon.neonMd5Mismatch = <NEON md5>``.
- ``lake-file`` / ``lake-catalog`` / ``side-file`` / ``ref-file``: site-lake Parquet, the ``.ducklake``
                 catalog, cards/curation/reports files and ``ref/ontology_maps`` exports.

``unit_id`` is the collection a row belongs to (a data object's parent collection). Sources: the site lake
(READ_ONLY), the catalog lake (READ_ONLY), ``work/state.sqlite`` (download facts) and the ontology/curation
store (``site/ontology_store.open_store(read_only=True)``); a missing ontology table only skips its generator.

Filters: ``products``/``months`` restrict which product, month and object units are rebuilt; site-level units
are always rebuilt (they summarise the whole lake). ``avu.bulk.replace_desired`` replaces a whole scope, so a
filtered build deletes exactly the rebuilt ``unit_id``s (and re-inserted paths) and inserts, in one transaction.
"""

from __future__ import annotations

import csv
import logging
import re
import sqlite3
import string
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .. import env, state
from ..avu import bulk, datacite
from ..avu.normalize import is_protected, normalize
from . import site_lake

log = logging.getLogger("neon_ducklake.site.avus")

GENERATOR = "site.avus"
API_DATA = "https://data.neonscience.org/api/v0/data"
PRODUCT_PAGE = "https://data.neonscience.org/data-products/{code}"
SITE_PAGE = "https://www.neonscience.org/field-sites/{site}"
CURATOR_AFFILIATION = "University of New Mexico"
PACKAGE = "basic"
MAX_PLOT_IDS = 50
NLCD_MIN_PCT = 1.0
ABSTRACT_CHARS = 1500
SKIP_SUFFIXES = (".part", ".wal", ".tmp", ".lock")
CONTAINERS = ("raw", "ducklake", "cards", "curation", "reports")
SUPPORT_TABLE_KINDS = frozenset({"categorical_codes", "validation", "science_review_flags"})
CURATED_OK = ("accepted", "human_accepted")
UPLOADED = ("uploaded", "present")
EML_RANGE_RE = re.compile(r"\.EML\.(\d{8})-(\d{8})\.", re.IGNORECASE)
LAKE_TABLE_RE = re.compile(r"^(dp\d)_(\d{5})_(\d{3})__")
PRODUCT_PREFIX_RE = re.compile(r"(DP\d\.\d{5}\.\d{3})")
CORE_ATTRS = ("neon.productCode", "neon.siteCode", "neon.domainCode", "neon.month", "neon.release",
              "neon.package", "neon.fileKind", "neon.publicationDate", "dc.source")
MIME = {"csv": "text/csv", "xml": "application/xml", "txt": "text/plain", "h5": "application/x-hdf5",
        "zip": "application/zip", "json": "application/json", "jsonl": "application/jsonl",
        "md": "text/markdown", "parquet": "application/vnd.apache.parquet", "ducklake": "application/vnd.duckdb",
        "duckdb": "application/vnd.duckdb", "html": "text/html", "pdf": "application/pdf", "gz": "application/gzip"}
FETCH_TABLES = frozenset({"files", "site_months", "external_links"})
# contract (b) download-integrity columns of the site lake ``files`` (absent in lakes built before them)
INTEGRITY_COLUMNS = ("storageMd5", "neonMd5", "transferVerified")
CONTAINER_TEXT = {
    "raw": (("NEON basic-package files of site {site} exactly as published by the NEON Data API: "
             "raw/<productCode>/<YYYY-MM>/<NEON file name> for every non-AOP product and month (current, "
             "PROVISIONAL and release-only)."), "neon-ducklake site download"),
    "ducklake": (("Per-site DuckLake of {site}: catalog neon_{lower}.ducklake plus Parquet under data/ (file "
                  "inventory, stacked tables, variables, sensor positions, HDF5 fluxes, statistics). Attach "
                  "READ_ONLY with DATA_PATH set to the local data/ and OVERRIDE_DATA_PATH true."),
                 "neon-ducklake site stack"),
    "cards": (("Product cards of site {site} (product record plus real column profiles) used for ontology "
               "curation."), "neon-ducklake site cards"),
    "curation": ("Ontology curation of site {site}: model runs, validation results and the review list.",
                 "neon-ducklake site curate"),
    "reports": ("Pipeline reports of site {site}: plan, list, download, stack, upload and verify summaries.",
                "neon-ducklake site verify"),
}
REPORT_GENERATORS = {"plan": "site plan", "list": "site list", "download": "site download", "stack": "site stack",
                     "upload": "site upload", "verify": "verify site", "avus": "site build-avus",
                     "terms": "site terms", "unresolved": "site terms", "review": "site validate"}

Avu = tuple[Any, ...]  # (attribute, value[, unit])
Row = tuple[str, str, str, str, str, str, str, str]


# ---------------------------------------------------------------- formatting

def num(x: Any, nd: int = 7) -> str:
    """A float without trailing zeros (``997.476``, ``31.9106747``)."""
    s = f"{round(float(x), nd):.{nd}f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def point(lat: Any, lon: Any) -> str | None:
    if lat is None or lon is None:
        return None
    return f"{num(lat)} {num(lon)}"


def as_utc(ts: Any) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return None
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)
    if isinstance(ts, date):
        return datetime(ts.year, ts.month, ts.day, tzinfo=UTC)
    return None


def iso(ts: Any) -> str | None:
    """ISO 8601 UTC ``YYYY-MM-DDTHH:MM:SSZ`` (naive values are UTC)."""
    t = as_utc(ts)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ") if t else None


def interval(t0: Any, t1: Any) -> str | None:
    a, b = iso(t0), iso(t1)
    return f"{a}/{b}" if a and b else None


def month_bounds(month: str) -> tuple[datetime, datetime]:
    y, m = int(month[:4]), int(month[5:7])
    start = datetime(y, m, 1, tzinfo=UTC)
    end = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=UTC)
    return start, end


def month_ranges(months: Iterable[str]) -> str | None:
    """Comma-joined ISO ranges ``YYYY-MM/YYYY-MM`` (neon-mcp MonthRanges)."""
    from neon_mcp.neon.months import MonthRanges

    ms = sorted({m for m in months if m})
    return ",".join(MonthRanges.from_months(ms).ranges()) if ms else None


def eml_coverage(name: str) -> str | None:
    m = EML_RANGE_RE.search(name)
    if not m:
        return None
    a, b = m.group(1), m.group(2)
    return f"{a[:4]}-{a[4:6]}-{a[6:]}/{b[:4]}-{b[4:6]}-{b[6:]}"


def mime(name: str) -> str:
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    return MIME.get(ext, "application/octet-stream")


def snake(label: str) -> str:
    from mesa_mcp.ols.client import _label_to_snake

    return _label_to_snake(label)


def ontology_avu(ontology_id: str, label: str | None, value: Any, curie: str | None) -> Avu | None:
    """``<prefix>.<snake(label)> = value`` with the CURIE as unit (mesa-mcp ontology_annotations_to_avus)."""
    from mesa_mcp.ols.transform import ontology_annotations_to_avus

    key = snake(label or "")
    if not key or not curie or value is None:
        return None
    out = ontology_annotations_to_avus(ontology_id, [{"key": key, "value": str(value), "curie": curie}])
    return (out[0]["attribute"], out[0]["value"], out[0]["unit"]) if out else None


def is_aquatic(habitat: str | None) -> bool:
    """HABITAT 'Aquatic Wadeable Stream' / 'Aquatic Lake' / 'Aquatic Non-wadeable Stream' (or bare 'Aquatic')."""
    return (habitat or "").strip().lower().startswith("aquatic")


def lab_name_of(name: str, product: str | None = None) -> tuple[str | None, str | None] | None:
    """(lab name, table) of a ``NEON.<Lab>.<table>.<ts>.csv`` file via the shared classifier, else None."""
    try:
        from .. import filenames

        c = filenames.classify(name, product)
    except Exception:  # noqa: BLE001 - never raises by contract; stay safe for odd names
        return None
    return (c.lab_name, c.table) if c.kind == "lab" else None


def md5_state(r: dict[str, Any], download: dict[str, Any] | None = None) -> tuple[bool | None, bool | None, str | None]:
    """(md5Verified, transferVerified, NEON md5) of a files row; journal columns fill what the lake lacks."""
    d = download or {}

    def flag(v: Any) -> bool | None:
        return None if v is None else bool(v)

    md5_ok = r.get("md5Verified")
    if md5_ok is None:
        md5_ok = flag(d.get("md5_verified"))
    transfer = r.get("transferVerified") if "transferVerified" in r else None
    if transfer is None:
        transfer = flag(d.get("transfer_verified"))
    neon_md5 = r.get("neonMd5") or d.get("neon_md5") or r.get("md5")
    return md5_ok, transfer, neon_md5


def norm_key(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def lake_product(table: str) -> str | None:
    m = LAKE_TABLE_RE.match(table or "")
    return f"{m.group(1).upper()}.{m.group(2)}.{m.group(3)}" if m else None


def csv_records(path: Path) -> int | None:
    """Data records of a CSV file (header excluded; quoted newlines handled)."""
    try:
        with path.open(newline="", encoding="utf-8", errors="replace") as fh:
            n = sum(1 for _ in csv.reader(fh))
    except OSError:
        return None
    return max(n - 1, 0)


# ---------------------------------------------------------------- inputs

@dataclass
class Ontology:
    """Read-only snapshot of the ontology/curation store (contract § Ontology and curation stores)."""

    tables: set[str] = field(default_factory=set)
    taxa: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    units: dict[str, dict[str, Any]] = field(default_factory=dict)
    nlcd: dict[str, dict[str, Any]] = field(default_factory=dict)
    site_envo: dict[str, Any] | None = None
    site_feature: dict[str, Any] | None = None
    curated: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def has(self, table: str) -> bool:
        return table in self.tables


@dataclass
class Facts:
    site: str
    site_info: dict[str, Any] = field(default_factory=dict)
    catalog_ok: bool = False
    products: dict[str, dict[str, Any]] = field(default_factory=dict)
    dois: dict[tuple[str, str], str] = field(default_factory=dict)
    specs: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    nlcd: list[dict[str, Any]] = field(default_factory=list)
    taxon_types: dict[str, list[tuple[str, str]]] = field(default_factory=dict)  # taxonID -> [(type, sciName)]
    files: list[dict[str, Any]] = field(default_factory=list)
    site_months: list[dict[str, Any]] = field(default_factory=list)
    external_links: list[dict[str, Any]] = field(default_factory=list)
    file_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    file_taxa: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    sensor_positions: dict[tuple[str, str, str], list[dict[str, Any]]] = field(default_factory=dict)
    sensor_counts: dict[str, int] = field(default_factory=dict)
    h5_tower: dict[str, tuple[Any, Any]] = field(default_factory=dict)
    lake_tables: dict[str, list[str]] = field(default_factory=dict)
    variables: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    downloads: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    uploaded_ref: set[str] = field(default_factory=set)
    ontology: Ontology = field(default_factory=Ontology)
    neon_mcp_version: str = "?"


def _rows(con: Any, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    cur = con.execute(sql, list(params))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _lake_rows(con: Any, table: str, want: Iterable[str], where: str = "", params: Iterable[Any] = ()
               ) -> list[dict[str, Any]]:
    """Rows of ``lake.<table>`` with the wanted columns that exist ([] when the table is missing)."""
    if not site_lake.table_exists(con, table):
        return []
    have = site_lake.table_columns(con, table)
    cols = [c for c in want if c in have]
    if not cols:
        return []
    sel = ", ".join(site_lake.ident(c) for c in cols)
    return _rows(con, f"SELECT {sel} FROM {site_lake.ALIAS}.{site_lake.ident(table)} {where}", params)


def _load_site_lake(con: Any, f: Facts) -> None:
    site = f.site
    f.files = _lake_rows(con, "files", list(dict.fromkeys([*site_lake.FILES_COLUMNS, *INTEGRITY_COLUMNS])),
                         "ORDER BY productCode, month, name")
    f.site_months = _lake_rows(con, "site_months", site_lake.SITE_MONTHS_COLUMNS, "ORDER BY productCode, month")
    f.external_links = _lake_rows(con, "external_links", site_lake.EXTERNAL_LINKS_COLUMNS,
                                  "ORDER BY productCode, url")
    for r in _lake_rows(con, "file_stats", site_lake.STACK_SCHEMAS["file_stats"]):
        f.file_stats[r["sourceFile"]] = r
    for r in _lake_rows(con, "file_taxa", site_lake.STACK_SCHEMAS["file_taxa"], "ORDER BY sourceFile, taxonID"):
        f.file_taxa.setdefault(r["sourceFile"], []).append(r)
    sp = _lake_rows(con, "sensor_positions", ["productCode", "month", "sourceFile", "HOR.VER", "sensorLocationID",
                                              "sensorLocationDescription", "positionStartDateTime",
                                              "positionEndDateTime", "sensorLatitude", "sensorLongitude",
                                              "sensorElevation", "zOffset"])
    by_file: dict[str, set[str]] = defaultdict(set)
    for r in sp:
        hv = r.get("HOR.VER")
        if hv is None:
            continue
        f.sensor_positions.setdefault((r["productCode"], r["month"], str(hv)), []).append(r)
        if r.get("sourceFile"):
            by_file[r["sourceFile"]].add(str(hv))
    f.sensor_counts = {k: len(v) for k, v in by_file.items()}
    towers: dict[str, dict[str, str]] = defaultdict(dict)
    for r in _lake_rows(con, "h5_attrs", ["sourceFile", "path", "attr", "value"],
                        "WHERE attr IN ('LatTow', 'LonTow') AND path = ?", [f"/{site}"]):
        towers[r["sourceFile"]][r["attr"]] = r["value"]
    f.h5_tower = {k: (v.get("LatTow"), v.get("LonTow")) for k, v in towers.items()
                  if v.get("LatTow") and v.get("LonTow")}
    for r in _lake_rows(con, "table_catalog", ["tableName", "productCode"], "ORDER BY productCode, tableName"):
        f.lake_tables.setdefault(r["productCode"], []).append(r["tableName"])
    for r in _lake_rows(con, "variables", ["productCode", "tableName", "fieldName", "units", "downloadPkg",
                                           "dataType"], "ORDER BY productCode, tableName, fieldName"):
        f.variables.setdefault(r["productCode"], []).append(r)


def _load_catalog(con: Any, f: Facts, codes: list[str], taxon_ids: list[str]) -> None:
    a = site_lake.CATALOG_ALIAS
    rows = _rows(con, f"SELECT * FROM {a}.sites WHERE siteCode = ?", [f.site])
    if rows:
        info = dict(rows[0])
        props = info.get("siteProperties") or {}
        if not isinstance(props, dict):  # DuckDB MAP comes back as dict; be tolerant of key/value lists
            props = dict(zip(props.get("key", []), props.get("value", []), strict=False)) if props else {}
        info["siteProperties"] = props
        info["habitat"] = props.get("HABITAT")
        # NEON copied one shortgrass-steppe TypeEco to every aquatic site: never use it there.
        info["typeEco"] = None if is_aquatic(info["habitat"]) else props.get("TypeEco")
        f.site_info = info
    if codes:
        cols = ("productCode, productName, productDescription, productAbstract, productDesignDescription, "
                "productStatus, productCategory, productScienceTeam, productScienceTeamAbbr, themes, keywords")
        for r in _rows(con, f"SELECT {cols} FROM {a}.products WHERE list_contains(?, productCode)", [codes]):
            f.products[r["productCode"]] = r
        for r in _rows(con, f"SELECT productCode, release, productDoi FROM {a}.product_releases "
                            "WHERE list_contains(?, productCode)", [codes]):
            if r["productDoi"]:
                f.dois[(r["productCode"], r["release"])] = datacite.bare_doi(r["productDoi"])
        for r in _rows(con, f"SELECT productCode, specNumber, specDescription, specUrl FROM {a}.product_specs "
                            "WHERE list_contains(?, productCode) ORDER BY productCode, specNumber", [codes]):
            f.specs.setdefault(r["productCode"], []).append(r)
    try:
        f.nlcd = _rows(con, f"SELECT nlcdYear, nlcdClass, area_km2 FROM {a}.neon_tos_nlcdcoverbysite "
                            "WHERE siteID = ? ORDER BY area_km2 DESC, nlcdClass", [f.site])
    except Exception as exc:  # noqa: BLE001 - optional published layer
        log.warning("NLCD cover layer unreadable (%s); NLCD AVUs skipped", str(exc).splitlines()[0][:160])
    if taxon_ids:
        for r in _rows(con, f"SELECT taxonTypeCode, taxonID, scientificName FROM {a}.taxonomy "
                            "WHERE list_contains(?, taxonID) ORDER BY taxonTypeCode", [taxon_ids]):
            f.taxon_types.setdefault(r["taxonID"], []).append((r["taxonTypeCode"], r["scientificName"]))


def _load_state(f: Facts) -> None:
    if not state.DB_PATH.exists():
        return
    con = sqlite3.connect(f"file:{state.DB_PATH}?mode=ro", uri=True, timeout=60)
    con.row_factory = sqlite3.Row
    try:
        have = {r[1] for r in con.execute("PRAGMA table_info(site_files)")}
        extra = [c for c in ("storage_md5", "neon_md5", "transfer_verified") if c in have]  # contract (b)
        cols = ", ".join(["product", "month", "name", "dl_status", "dl_bytes", "dl_md5", "md5_verified",
                          "downloaded_at", *extra])
        for r in con.execute(f"SELECT {cols} FROM site_files WHERE site=?", (f.site,)):
            f.downloads[(r["product"], r["month"], r["name"])] = dict(r)
        try:
            f.uploaded_ref = {r[0] for r in con.execute(
                "SELECT irods_path FROM uploads WHERE irods_path LIKE ? AND status IN (?, ?)",
                (f"{env.IRODS_ROOT}/ref/%", *UPLOADED))}
        except sqlite3.Error:
            f.uploaded_ref = set()
    except sqlite3.Error as exc:
        log.warning("state journal unreadable (%s); download facts skipped", exc)
    finally:
        con.close()


def open_ontology_store() -> Any | None:
    """``ontology_store.open_store(read_only=True)``, else the DuckDB file read-only, else None."""
    import duckdb

    try:
        from . import ontology_store  # written by the terms pipeline
    except ImportError:
        ontology_store = None
    if ontology_store is not None and hasattr(ontology_store, "open_store"):
        try:
            store = ontology_store.open_store(read_only=True)
            if hasattr(store, "execute"):
                return store
            if hasattr(store, "__enter__"):
                return store.__enter__()
        except Exception as exc:  # noqa: BLE001 - missing file, writer lock, …
            log.warning("ontology store not available (%s)", str(exc).splitlines()[0][:160])
            return None
    path = env.WORK_DIR / "ontology_cache.duckdb"
    if not path.exists():
        return None
    try:
        return duckdb.connect(str(path), read_only=True)
    except duckdb.Error as exc:
        log.warning("ontology store %s not readable (%s)", path, str(exc).splitlines()[0][:160])
        return None


def load_ontology(site: str, store: Any | None = None) -> Ontology:
    o = Ontology()
    own = store is None
    store = store if store is not None else open_ontology_store()
    if store is None:
        o.warnings.append("ontology store missing: ncbitaxon/uo/envo/curated generators skipped")
        return o
    try:
        o.tables = {r[0] for r in store.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()}

        def rows(table: str, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
            if table not in o.tables:
                o.warnings.append(f"ontology table {table} missing: its generator is skipped")
                return []
            try:
                return _rows(store, sql, params)
            except Exception as exc:  # noqa: BLE001 - schema drift in a concurrently written store
                o.tables.discard(table)
                o.warnings.append(f"ontology table {table} unreadable ({str(exc).splitlines()[0][:120]})")
                return []

        for r in rows("ref_ncbitaxon_map", "SELECT * FROM ref_ncbitaxon_map"):
            o.taxa[(r.get("taxonTypeCode") or "", r.get("taxonID") or "")] = r
        for r in rows("ref_unit_map", "SELECT * FROM ref_unit_map"):
            if r.get("neonUnit"):
                o.units[r["neonUnit"]] = r
        for r in rows("ref_nlcd_envo", "SELECT * FROM ref_nlcd_envo"):
            o.nlcd[norm_key(r.get("nlcdClass"))] = r
        got = rows("ref_site_envo", "SELECT * FROM ref_site_envo WHERE siteCode = ?", [site])
        o.site_envo = got[0] if got else None
        if "ref_site_envo_feature" in o.tables:  # newer store table: its absence is not worth a warning
            got = rows("ref_site_envo_feature", "SELECT * FROM ref_site_envo_feature WHERE siteCode = ?", [site])
            o.site_feature = got[0] if got else None
        for r in rows("curation_avus", "SELECT * FROM curation_avus ORDER BY productCode, attribute, value"):
            if (r.get("status") or "") in CURATED_OK and r.get("productCode"):
                o.curated.setdefault(r["productCode"], []).append(r)
    finally:
        if own:
            try:
                store.close()
            except Exception as exc:  # noqa: BLE001
                log.debug("ontology store close failed: %s", exc)
    for w in o.warnings:
        log.warning(w)
    return o


def load_facts(site: str, *, ontology_store: Any | None = None) -> Facts:
    """Everything the builder needs, read once (site lake + catalog lake READ_ONLY, journal, ontology store)."""
    import neon_mcp

    site = site.upper()
    f = Facts(site=site, neon_mcp_version=getattr(neon_mcp, "__version__", "?"))
    paths = site_lake.site_paths(site)
    if not paths.catalog_file.exists():
        raise SystemExit(f"site lake {paths.catalog_file} missing: run `site list {site}` first")
    import duckdb

    con = site_lake.connect(site)
    try:
        try:
            site_lake.attach(con, site, read_only=True)
        except duckdb.Error as exc:
            raise SystemExit(f"site lake {paths.catalog_file} not readable (a writer holds it?): "
                             f"{str(exc).splitlines()[0][:200]}") from exc
        _load_site_lake(con, f)
        codes = sorted({r["productCode"] for r in f.files} | {r["productCode"] for r in f.site_months}
                       | {r["productCode"] for r in f.external_links})
        taxon_ids = sorted({t["taxonID"] for ts in f.file_taxa.values() for t in ts if t.get("taxonID")})
        f.catalog_ok = site_lake.attach_catalog(con)
        if f.catalog_ok:
            try:
                _load_catalog(con, f, codes, taxon_ids)
            except Exception as exc:  # noqa: BLE001 - never crash on a catalog schema surprise
                log.warning("catalog lake read failed (%s); catalog-derived AVUs skipped",
                            str(exc).splitlines()[0][:200])
                f.catalog_ok = False
        else:
            log.warning("catalog lake not attachable: site/product records, DOIs and NLCD are skipped")
    finally:
        con.close()
    _load_state(f)
    f.ontology = load_ontology(site, ontology_store)
    return f


# ---------------------------------------------------------------- the builder

class Builder:
    """Pure row builder over :class:`Facts` (no I/O except small local reads of the site tree)."""

    def __init__(self, facts: Facts, products: list[str] | None = None, months: list[str] | None = None) -> None:
        self.f = facts
        self.site = facts.site
        self.P = set(products) if products else None
        self.M = set(months) if months else None
        self.S = f"{env.IRODS_ROOT}/sites/{self.site}"
        self.lake_rel = f"sites/{self.site}/ducklake/neon_{self.site.lower()}.ducklake"
        self.lake_unit = f"ducklake:{self.lake_rel}"
        self.rows: list[Row] = []
        self.targets: dict[str, tuple[str, str]] = {}  # path -> (level, profile)
        self.info: dict[str, Any] = {"datacite_errors": [], "skipped": list(facts.ontology.warnings)}
        self.local = site_lake.site_paths(self.site)
        self._files_by_product: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._files_by_month: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for r in facts.files:
            self._files_by_product[r["productCode"]].append(r)
            self._files_by_month[(r["productCode"], r["month"])].append(r)
        self._months_by_product: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in facts.site_months:
            self._months_by_product[r["productCode"]].append(r)
        self._links: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in facts.external_links:
            self._links[r["productCode"]].append(r)

    # ------------------------------------------------------------ selection
    def product_ok(self, p: str) -> bool:
        return self.P is None or p in self.P

    def month_ok(self, p: str, m: str) -> bool:
        return self.product_ok(p) and (self.M is None or m in self.M)

    def rebuilds(self, unit_id: str) -> bool:
        """Whether an existing ``unit_id`` of this scope is rebuilt by this (possibly filtered) build."""
        raw = f"{self.S}/raw/"
        if not unit_id.startswith(raw):
            return True
        parts = unit_id[len(raw):].split("/")
        return self.product_ok(parts[0]) if len(parts) == 1 else self.month_ok(parts[0], parts[1])

    # ------------------------------------------------------------ emit
    def _emit(self, unit: str, path: str, ttype: str, level: str, avus: Iterable[Avu | None],
              generator: str = GENERATOR, profile: str = "") -> None:
        self.targets.setdefault(path, (level, profile))
        for t in avus:
            if t is None or t[1] is None or t[1] == "":
                continue
            unit_v = t[2] if len(t) > 2 and t[2] is not None else ""
            gen = t[3] if len(t) > 3 else generator
            self.rows.append((unit, path, ttype, str(t[0]), str(t[1]), str(unit_v), level, gen))

    def coll(self, path: str, level: str, avus: Iterable[Avu | None], generator: str = GENERATOR) -> None:
        self._emit(path, path, "collection", level, avus, generator)

    def obj(self, path: str, level: str, avus: Iterable[Avu | None], generator: str = GENERATOR,
            profile: str = "") -> None:
        self._emit(path.rsplit("/", 1)[0], path, "data_object", level, avus, generator, profile)

    # ------------------------------------------------------------ shared facts
    def size_of(self, r: dict[str, Any]) -> int:
        if r.get("size") is not None:
            return int(r["size"])
        d = self.f.downloads.get((r["productCode"], r["month"], r["name"])) or {}
        return int(d.get("dl_bytes") or 0)

    def doi_unit(self, p: str, release: str | None) -> str:
        if not release or release == "PROVISIONAL":
            return ""
        doi = self.f.dois.get((p, release))
        return f"doi:{doi}" if doi else ""

    def stats_for(self, files: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        return [s for r in files if (s := self.f.file_stats.get(r["name"])) is not None]

    @staticmethod
    def coverage(stats: list[dict[str, Any]]) -> str | None:
        t0 = [s["timeMin"] for s in stats if s.get("timeMin") is not None]
        t1 = [s["timeMax"] for s in stats if s.get("timeMax") is not None]
        return interval(min(t0), max(t1)) if t0 and t1 else None

    def download_date(self, product: str | None = None) -> str | None:
        ds = [d["downloaded_at"] for (p, _m, _n), d in self.f.downloads.items()
              if d.get("downloaded_at") and (product is None or p == product)]
        return max(ds)[:10] if ds else None

    def pub_year(self, product: str | None = None) -> int:
        d = self.download_date(product)
        if d:
            return int(d[:4])
        pubs = [as_utc(r.get("publicationTimestamp")) for r in self.f.files
                if product is None or r["productCode"] == product]
        pubs = [p for p in pubs if p]
        if pubs:
            return max(pubs).year
        probed = [str(r.get("monthProbed") or "") for r in self._links.get(product or "", [])]
        return int(max(probed)[:4]) if any(probed) else env.utcnow().year

    def resolve_taxon(self, t: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        """(taxonTypeCode, ref_ncbitaxon_map row) of a file_taxa row."""
        tid, sci = t.get("taxonID") or "", t.get("scientificName") or ""
        cands = self.f.taxon_types.get(tid, [])
        exact = [c for c in cands if (c[1] or "").lower() == sci.lower()]
        types = [c[0] for c in (exact or cands)]
        taxa = self.f.ontology.taxa
        for tt in types:
            if (tt, tid) in taxa:
                return tt, taxa[(tt, tid)]
        by_id = [(k[0], v) for k, v in taxa.items() if k[1] == tid]
        if len(by_id) == 1:
            return by_id[0]
        return (types[0] if types else None), None

    def taxon_avus(self, taxa: Iterable[dict[str, Any]], *, unresolved: bool) -> list[Avu]:
        if not self.f.ontology.has("ref_ncbitaxon_map"):
            return []
        out: list[Avu] = []
        seen: set[str] = set()
        for t in taxa:
            tid = t.get("taxonID")
            if not tid or tid in seen:
                continue
            seen.add(tid)
            tt, m = self.resolve_taxon(t)
            if m and (m.get("status") == "ok") and m.get("curie") and m.get("label"):
                avu = ontology_avu("ncbitaxon", m["label"], t.get("vernacularName") or t.get("scientificName")
                                   or m.get("neonName"), m["curie"])
                if avu:
                    out.append((*avu, GENERATOR + ".ncbitaxon"))
            elif unresolved and t.get("scientificName"):
                out.append(("neon.taxon", t["scientificName"], f"NEON:{tt or 'UNKNOWN'}:{tid}",
                            GENERATOR + ".ncbitaxon"))
        return out

    # ------------------------------------------------------------ site level
    def site_level(self) -> None:
        f, s = self.f, self.site
        info = f.site_info
        months = f.site_months
        files = f.files
        stats = list(f.file_stats.values())
        cov = self.coverage(stats)
        products = {r["productCode"] for r in months} | {r["productCode"] for r in files}
        external = {r["productCode"] for r in months if r.get("isExternal")} | {r["productCode"]
                                                                              for r in f.external_links}
        avus: list[Avu | None] = [
            ("neon.siteCode", s),
            ("neon.siteName", info.get("siteName")),
            ("neon.siteType", info.get("siteType")),
            ("neon.domainCode", info.get("domainCode") or next((r.get("domainCode") for r in files), None)),
            ("neon.domainName", info.get("domainName")),
            ("neon.stateCode", info.get("stateCode")),
            ("neon.sitePoint", point(info.get("siteLatitude"), info.get("siteLongitude")), "WGS84 lat lon"),
            ("neon.siteElevation", num(info["siteElevation"], 3) if info.get("siteElevation") is not None else None,
             "meter"),
            ("neon.deimsId", info.get("deimsId"), "URL"),
            ("neon.habitat", info.get("habitat")),
            ("neon.typeEco", None if is_aquatic(info.get("habitat")) else info.get("typeEco")),
            ("neon.package", PACKAGE),
            ("neon.excludedTeam", "AOP"),
            ("neon.productCount", str(len(products)), "count"),
            ("neon.externalProductCount", str(len(external)), "count"),
            ("neon.siteMonths", str(len(months)), "count"),
            ("neon.provisionalSiteMonths", str(sum(1 for r in months if r.get("isProvisional"))), "count"),
            ("neon.releaseOnlySiteMonths", str(sum(1 for r in months if r.get("isReleaseOnly"))), "count"),
            ("neon.fileCount", str(len(files)), "count"),
            ("neon.totalBytes", str(sum(self.size_of(r) for r in files)), "byte"),
            ("neon.temporalCoverage", cov, "ISO8601 interval"),
            ("neon.downloadedWith", f"neon-mcp {f.neon_mcp_version}"),
            ("neon.downloadDate", self.download_date(), "ISO8601"),
            ("mesa.ducklake.catalog", self.lake_rel, "relative path"),
        ]
        avus += [a for a, _label in self.envo()]
        self.coll(self.S, "site", avus)
        self.coll(self.S, "site", self.site_datacite(cov), GENERATOR + ".datacite")

    def envo(self) -> list[tuple[Avu, str]]:
        """(AVU, ENVO label) of the curated site biome and the NLCD classes >= 1 % of the site area."""
        if not hasattr(self, "_envo"):
            self._envo = self._envo_avus()
        return self._envo

    @staticmethod
    def _curated(row: dict[str, Any] | None) -> bool:
        if not row or not row.get("envoCurie"):
            return False
        status, method = (row.get("status") or "").lower(), (row.get("method") or "").lower()
        return "curated" in (status, method) and status not in ("pending", "rejected", "obsolete", "unresolved",
                                                                  "proposed")

    def _envo_avus(self) -> list[tuple[Avu, str]]:
        """Curated biome (value: TypeEco, or HABITAT at an aquatic site), the curated water-body feature of an
        aquatic site (value: the site name), and NLCD classes >= 1 % (terrestrial sites only)."""
        o, info = self.f.ontology, self.f.site_info
        out: list[tuple[Avu, str]] = []
        aquatic = is_aquatic(info.get("habitat"))
        se = o.site_envo
        if o.has("ref_site_envo") and self._curated(se):
            value = (info.get("habitat") if aquatic else (se.get("typeEco") or info.get("typeEco"))) \
                or se.get("envoLabel")
            avu = ontology_avu("envo", se.get("envoLabel"), value, se.get("envoCurie"))
            if avu:
                out.append(((*avu, GENERATOR + ".envo"), se.get("envoLabel")))
        sf = o.site_feature
        if aquatic and self._curated(sf):
            avu = ontology_avu("envo", sf.get("envoLabel"), info.get("siteName") or self.site, sf.get("envoCurie"))
            if avu:
                out.append(((*avu, GENERATOR + ".envo"), sf.get("envoLabel")))
        if o.has("ref_nlcd_envo") and self.f.nlcd and not aquatic:
            total = sum(float(r["area_km2"] or 0) for r in self.f.nlcd)
            for r in self.f.nlcd:
                area = float(r["area_km2"] or 0)
                pct = 100.0 * area / total if total else 0.0
                if pct < NLCD_MIN_PCT:
                    continue
                m = o.nlcd.get(norm_key(r["nlcdClass"]))
                if not m:
                    self.info.setdefault("nlcd_unmapped", []).append(r["nlcdClass"])
                    continue
                year = str(r["nlcdYear"] or "")
                head = f"NLCD {year}" if year.isdigit() else (year or "NLCD")
                value = f"{head} {r['nlcdClass']} {area:.2f} km2 ({pct:.1f}%)"
                avu = ontology_avu("envo", m.get("envoLabel"), value, m.get("envoCurie"))
                if avu:
                    out.append(((*avu, GENERATOR + ".envo"), m.get("envoLabel")))
        return out

    def site_datacite(self, cov: str | None) -> list[Avu]:
        f, s, info = self.f, self.site, self.f.site_info
        name = info.get("siteName") or s
        months = sorted(r["month"] for r in f.site_months)
        years = f"{months[0][:4]}-{months[-1][:4]}" if months else ""
        subjects: list[dict[str, str]] = [{"value": "NEON"}]
        eco = None if is_aquatic(info.get("habitat")) else info.get("typeEco")
        for v in (info.get("domainName"), info.get("siteType"), info.get("habitat"), eco, info.get("stateName")):
            if v:
                subjects.append({"value": str(v)})
        seen_curies: set[str] = set()
        for avu, label in self.envo():
            if avu[2] not in seen_curies:
                seen_curies.add(avu[2])
                subjects.append({"value": f"{label} ({avu[2]})", "subjectScheme": "ENVO"})
        n_files, n_bytes = len(f.files), sum(self.size_of(r) for r in f.files)
        products = sorted({r["productCode"] for r in f.site_months} | {r["productCode"] for r in f.files})
        tech = (f"Non-AOP NEON data products at {name} ({s}), basic package: {len(products)} products, "
                f"{len(f.site_months)} site-months, {n_files} files ({n_bytes} bytes) under raw/<productCode>/"
                f"<YYYY-MM>/, stacked into the site DuckLake {self.lake_rel}.")
        descs = [{"value": self.site_abstract()[:ABSTRACT_CHARS], "descriptionType": "Abstract"},
                 {"value": tech, "descriptionType": "TechnicalInfo"}]
        rel = [{"value": SITE_PAGE.format(site=s.lower()), "relatedIdentifierType": "URL",
                "relationType": "IsDescribedBy"}]
        if info.get("deimsId"):
            rel.append({"value": info["deimsId"], "relatedIdentifierType": "URL", "relationType": "References"})
        fields: dict[str, Any] = {
            "irods_path": self.S,
            "title": f"NEON field site {name} ({s}): non-AOP basic-package data" + (f", {years}" if years else ""),
            "resourceTypeGeneral": "Collection",
            "resourceType": "NEON field site data subtree (non-AOP, basic package)",
            "publicationYear": self.pub_year(),
            "subjects": subjects, "descriptions": descs, "relatedIdentifiers": rel,
            "contributors": [self.curator()],
            "formats": sorted({mime(r["name"]) for r in f.files}),
            "sizes": [f"{n_files} files", f"{n_bytes} bytes"],
        }
        geo = {k: v for k, v in (("point", point(info.get("siteLatitude"), info.get("siteLongitude"))),
                                 ("place", ", ".join(x for x in (f"{name} ({s})", info.get("stateName"), "USA")
                                                     if x))) if v}
        fields["geoLocations"] = [geo]
        collected = self.date_range(cov, months)
        if collected:
            fields["dates"] = [{"date": collected, "dateType": "Collected"}]
        return self.datacite_avus(self.S, fields)

    def site_abstract(self) -> str:
        """NEON's siteDescription, or (when it only repeats the name) a sentence from the site record."""
        info, s = self.f.site_info, self.site
        name = info.get("siteName") or s
        desc = (info.get("siteDescription") or "").strip()
        if desc and desc.lower() != name.lower():
            return desc
        where = ", ".join(x for x in (f"{info['domainName']} domain ({info.get('domainCode')})"
                                      if info.get("domainName") else None, info.get("stateName")) if x)
        kind = f"{info['siteType'].lower()} " if info.get("siteType") else ""
        text = f"All non-AOP NEON data (basic package) of the {kind}NEON field site {name} ({s})"
        text += f" in the {where}" if where else ""
        if is_aquatic(info.get("habitat")):
            return text + f". Habitat: {info['habitat']}."
        text += f". Habitat: {info['habitat']}" if info.get("habitat") else ""
        return text + (f". Ecosystem: {info['typeEco']}." if info.get("typeEco") else ".")

    @staticmethod
    def curator() -> dict[str, str]:
        return {"name": env.ACTOR, "contributorType": "DataCurator", "affiliation": CURATOR_AFFILIATION}

    @staticmethod
    def date_range(cov: str | None, months: list[str]) -> str | None:
        """DataCite ``Collected`` range: dates of a timestamp coverage, else the month range."""
        if cov and "T" in cov:
            a, b = cov.split("/")
            return f"{a[:10]}/{b[:10]}"
        if months:
            return f"{months[0]}/{months[-1]}"
        return cov

    def datacite_avus(self, path: str, fields: dict[str, Any]) -> list[Avu]:
        try:
            rec = datacite.build_record(**fields)
            return [(a, v, u) for a, v, u in datacite.to_avus(rec)]
        except Exception as exc:  # noqa: BLE001 - a bad record must not stop the build
            log.error("DataCite record for %s rejected: %s", path, str(exc)[:300])
            self.info["datacite_errors"].append(f"{path}: {str(exc)[:200]}")
            return []

    # ------------------------------------------------------------ containers + non-raw subtrees
    def containers(self) -> None:
        s = self.site
        for name in CONTAINERS:
            d = self.local.site_dir / name
            if not d.is_dir():
                continue
            text, gen = CONTAINER_TEXT[name]
            self.coll(f"{self.S}/{name}", "container", [
                ("dc.description", text.format(site=s, lower=s.lower())), ("dc.type", "Collection"),
                ("provenance.generator", gen), ("neon.siteCode", s)])
        self.lake_tree()
        for name in ("cards", "curation", "reports"):
            self.side_tree(name)
        self.ref_tree()

    def _tree(self, root: Path) -> tuple[list[Path], list[Path]]:
        if not root.is_dir():
            return [], []
        dirs, files = [], []
        for p in sorted(root.rglob("*")):
            if p.name.startswith(".") or any(part.startswith(".") for part in p.relative_to(root).parts):
                continue
            if p.is_dir():
                dirs.append(p)
            elif p.is_file() and not p.name.endswith(SKIP_SUFFIXES):
                files.append(p)
        keep = {root, *dirs}
        return dirs, [f for f in files if f.parent in keep]

    def irods_of(self, local: Path) -> str:
        return f"{env.IRODS_ROOT}/{local.relative_to(env.ROOT).as_posix()}"

    def lake_tree(self) -> None:
        import pyarrow.parquet as pq

        s = self.site
        data = self.local.data_dir
        cat = self.local.catalog_file
        stack_gen = CONTAINER_TEXT["ducklake"][1]
        if cat.exists():
            self.obj(self.irods_of(cat), "lake-catalog", [
                ("mesa.ducklake.catalog", self.lake_rel, "relative path"), ("dc.format", MIME["ducklake"]),
                ("provenance.generator", "neon-ducklake site list + site stack")])
        dirs, files = self._tree(data)
        if not data.is_dir():
            return
        n_files = 0
        # only directories that hold files exist in iRODS (upload creates collections for files; a dropped
        # table leaves an empty local directory behind)
        with_files = {p for f in files for p in f.parents}
        for d in [data, *dirs]:
            if d not in with_files:
                continue
            rel = d.relative_to(data).parts
            avus: list[Avu] = [("neon.siteCode", s), ("provenance.generator", stack_gen)]
            if not rel:
                avus.append(("dc.description", f"DuckLake Parquet data files of {self.lake_rel}: <schema>/<table>/."))
            elif len(rel) == 1:
                avus.append(("dc.description", f"DuckLake schema {rel[0]} of {self.lake_rel}."))
            else:
                avus += [("dc.description", f"Parquet files of site-lake table {rel[0]}.{rel[1]}."),
                         ("mesa.ducklake.table", rel[1], self.lake_unit)]
            self.coll(self.irods_of(d), "lake-dir", avus)
        for fp in files:
            rel = fp.relative_to(data).parts
            table = rel[1] if len(rel) >= 3 else (rel[0] if len(rel) == 2 else fp.stem)
            avus = [("mesa.ducklake.table", table, self.lake_unit),
                    ("provenance.generator", "neon-ducklake site list" if table in FETCH_TABLES else stack_gen)]
            if fp.suffix == ".parquet":
                avus.append(("dc.format", MIME["parquet"]))
                n_rows, code = self.parquet_facts(pq, fp)
                if n_rows is not None:
                    avus.append(("neon.rowCount", str(n_rows), "count"))
                code = code or lake_product(table)
                if code:
                    avus.append(("neon.productCode", code))
            else:
                avus.append(("dc.format", mime(fp.name)))
            self.obj(self.irods_of(fp), "lake-file", avus)
            n_files += 1
        self.info["lake_files"] = n_files

    @staticmethod
    def parquet_facts(pq: Any, path: Path) -> tuple[int | None, str | None]:
        """(rows, single productCode from row-group statistics or None) of a Parquet file."""
        try:
            md = pq.read_metadata(path)
        except Exception as exc:  # noqa: BLE001
            log.warning("parquet metadata unreadable %s: %s", path, exc)
            return None, None
        codes: set[str] = set()
        idx = next((i for i in range(md.num_columns) if md.schema.column(i).name == "productCode"), None)
        if idx is not None and md.num_rows:
            for g in range(md.num_row_groups):
                st = md.row_group(g).column(idx).statistics
                if st is None or not st.has_min_max:
                    return md.num_rows, None
                mn, mx = st.min, st.max
                mn = mn.decode() if isinstance(mn, bytes) else mn
                mx = mx.decode() if isinstance(mx, bytes) else mx
                codes.update({mn, mx})
        return md.num_rows, (codes.pop() if len(codes) == 1 else None)

    def side_tree(self, name: str) -> None:
        root = self.local.site_dir / name
        dirs, files = self._tree(root)
        gen = CONTAINER_TEXT[name][1]
        for d in dirs:
            rel = d.relative_to(root).as_posix()
            self.coll(self.irods_of(d), "side-dir", [
                ("dc.description", f"{name}/{rel} of site {self.site}."), ("neon.siteCode", self.site),
                ("provenance.generator", gen)])
        for fp in files:
            avus: list[Avu] = [("dc.format", mime(fp.name)), ("provenance.generator", self.side_generator(name, fp))]
            m = PRODUCT_PREFIX_RE.search(fp.name)
            if m:
                avus.append(("neon.productCode", m.group(1)))
            n = self.local_rows(fp)
            if n is not None:
                avus.append(("neon.rowCount", str(n), "count"))
            self.obj(self.irods_of(fp), "side-file", avus)

    @staticmethod
    def side_generator(container: str, fp: Path) -> str:
        if container == "reports":
            head = re.split(r"[_.\-]", fp.name, maxsplit=1)[0].lower()
            return "neon-ducklake " + REPORT_GENERATORS.get(head, "site " + head)
        if container == "curation" and ("validated" in fp.name or fp.name.startswith("review")):
            return "neon-ducklake site validate"
        return CONTAINER_TEXT[container][1]

    @staticmethod
    def local_rows(fp: Path) -> int | None:
        if fp.suffix == ".parquet":
            import pyarrow.parquet as pq

            try:
                return pq.read_metadata(fp).num_rows
            except Exception:  # noqa: BLE001
                return None
        if fp.suffix == ".csv":
            return csv_records(fp)
        if fp.suffix in (".jsonl", ".ndjson"):
            try:
                with fp.open("rb") as fh:
                    return sum(1 for line in fh if line.strip())
            except OSError:
                return None
        return None

    def ref_tree(self) -> None:
        """``ref/`` (shared by all sites): tagged only when its files were uploaded (uploads journal)."""
        up = self.f.uploaded_ref
        if not up:
            return
        R = f"{env.IRODS_ROOT}/ref"
        colls = {R} | {p.rsplit("/", 1)[0] for p in up}
        for c in sorted(colls):
            rel = c[len(R):].strip("/")
            desc = ("Reference maps shared by every site subtree." if not rel else
                    "Ontology maps (NCBITaxon taxa, UO units, NLCD to ENVO, site biomes, curated terms) exported "
                    "from work/ontology_cache.duckdb as Parquet." if rel == "ontology_maps" else f"ref/{rel}.")
            self.coll(c, "container", [("dc.description", desc), ("dc.type", "Collection"),
                                       ("provenance.generator", "neon-ducklake site terms")])
        for p in sorted(up):
            local = env.ROOT / p[len(env.IRODS_ROOT) + 1:]
            avus: list[Avu] = [("dc.format", mime(p)), ("provenance.generator", "neon-ducklake site terms")]
            if local.exists():
                n = self.local_rows(local)
                if n is not None:
                    avus.append(("neon.rowCount", str(n), "count"))
            self.obj(p, "ref-file", avus)

    # ------------------------------------------------------------ product level
    def product_level(self, p: str) -> None:
        f = self.f
        rec = f.products.get(p, {})
        path = f"{self.S}/raw/{p}"
        months = self._months_by_product.get(p, [])
        files = self._files_by_product.get(p, [])
        all_months = sorted({r["month"] for r in months} | {r["month"] for r in files})
        prov = sorted({r["month"] for r in months if r.get("isProvisional")})
        ronly = sorted({r["month"] for r in months if r.get("isReleaseOnly")})
        stats = self.stats_for(files)
        cov = self.coverage(stats) or (f"{all_months[0]}/{all_months[-1]}" if all_months else None)
        releases = self.product_releases(p, months, files)
        taxa = [t for r in files for t in f.file_taxa.get(r["name"], [])]
        avus: list[Avu | None] = [
            ("neon.productCode", p),
            ("neon.productName", rec.get("productName")),
            ("neon.productScienceTeam", rec.get("productScienceTeam")),
            ("neon.productStatus", rec.get("productStatus")),
            *[("neon.theme", t) for t in rec.get("themes") or []],
            *[("neon.keyword", k) for k in rec.get("keywords") or []],
            *[("neon.release", rel, f"doi:{doi}") for rel, doi in releases],
            ("neon.months", month_ranges(all_months), "ISO8601 interval"),
            ("neon.provisionalMonths", month_ranges(prov), "ISO8601 interval"),
            ("neon.releaseOnlyMonths", month_ranges(ronly), "ISO8601 interval"),
            ("neon.siteMonths", str(len(all_months)), "count"),
            ("neon.fileCount", str(len(files)), "count"),
            ("neon.totalBytes", str(sum(self.size_of(r) for r in files)), "byte"),
            ("neon.rowCount", str(sum(int(s["rowCount"] or 0) for s in stats)), "count") if stats else None,
            ("neon.taxaCount", str(len({t["taxonID"] for t in taxa if t.get("taxonID")})), "count") if taxa else None,
            ("neon.temporalCoverage", cov, "ISO8601 interval"),
            *[("neon.table", t) for t in sorted({r["tableName"] for r in files
                                                 if r.get("kind") == "data" and r.get("tableName")})],
            *[("mesa.ducklake.table", t, self.lake_unit) for t in sorted(set(f.lake_tables.get(p, [])))],
        ]
        for link in self._links.get(p, []):
            avus.append(("neon.externalDataUrl", link.get("url"), "URL"))
            host = urlparse(link.get("url") or "").hostname
            avus.append(("neon.externalHost", host))
        if all_months and ronly and set(ronly) >= set(all_months):
            avus.append(("neon.releaseOnly", "true"))
        self.coll(path, "product", avus)
        self.coll(path, "product", self.uo_avus(p), GENERATOR + ".uo")
        self.coll(path, "product", self.taxon_avus(taxa, unresolved=False))
        curated = self.curated_avus(p)
        self.coll(path, "product", curated, GENERATOR + ".curation")
        self.coll(path, "product", self.product_datacite(p, rec, all_months, prov, releases, files, cov),
                  GENERATOR + ".datacite")

    def product_releases(self, p: str, months: list[dict[str, Any]], files: list[dict[str, Any]]
                         ) -> list[tuple[str, str]]:
        """(release, bare DOI) of the releases that contribute months of ``p`` at this site."""
        tags = {r.get("release") for r in months} | {r.get("release") for r in files}
        out = []
        for tag in sorted(t for t in tags if t and t != "PROVISIONAL"):
            doi = self.f.dois.get((p, tag))
            if doi:
                out.append((tag, doi))
            else:
                self.info.setdefault("releases_without_doi", []).append(f"{p} {tag}")
        return out

    def uo_avus(self, p: str) -> list[Avu]:
        o = self.f.ontology
        if not o.has("ref_unit_map"):
            return []
        out: list[Avu] = []
        for v in self.f.variables.get(p, []):
            if not v.get("units") or (v.get("downloadPkg") or "").lower() == "expanded":
                continue
            m = o.units.get(v["units"])
            if not m or m.get("status") != "ok" or not m.get("uoCurie"):
                continue
            avu = ontology_avu("uo", m.get("uoLabel"), f"{v['tableName']}.{v['fieldName']}", m["uoCurie"])
            if avu:
                out.append(avu)
        return out

    def curated_avus(self, p: str) -> list[Avu]:
        o = self.f.ontology
        if not o.has("curation_avus"):
            return []
        return [(r["attribute"], r["value"], r.get("unit") or "") for r in o.curated.get(p, [])
                if r.get("attribute") and r.get("value")]

    def citation(self, rec: dict[str, Any], p: str, release: str | None, doi: str | None, accessed: str) -> str:
        from neon_mcp.resources import citation_templates

        t = citation_templates()
        tmpl = string.Template(t.provisional if release is None else t.released)
        values = {"productName": rec.get("productName") or p, "productCode": p, "release": release or "PROVISIONAL",
                  "doiUrl": f"https://doi.org/{doi}" if doi else "", "accessedOn": accessed}
        try:
            return tmpl.substitute(values)
        except (KeyError, ValueError) as exc:
            log.warning("citation template has unexpected variables (%s); using safe_substitute", exc)
            return tmpl.safe_substitute(values)

    def product_datacite(self, p: str, rec: dict[str, Any], months: list[str], prov: list[str],
                         releases: list[tuple[str, str]], files: list[dict[str, Any]], cov: str | None) -> list[Avu]:
        f, s = self.f, self.site
        site_name = f.site_info.get("siteName") or s
        name = rec.get("productName") or p
        years = f"{months[0][:4]}-{months[-1][:4]}" if months else ""
        subjects: list[dict[str, str]] = [{"value": k} for k in rec.get("keywords") or []]
        subjects += [{"value": t, "subjectScheme": "NEON theme"} for t in rec.get("themes") or []]
        for r in f.ontology.curated.get(p, []) if f.ontology.has("curation_avus") else []:
            if r.get("label") and r.get("curie"):
                subjects.append({"value": f"{r['label']} ({r['curie']})",
                                 "subjectScheme": (r.get("ontologyId") or r["curie"].split(":")[0]).upper()})
        rel = [{"value": doi, "relatedIdentifierType": "DOI", "relationType": "IsDerivedFrom"} for _t, doi in releases]
        rel.append({"value": PRODUCT_PAGE.format(code=p), "relatedIdentifierType": "URL",
                    "relationType": "IsDescribedBy"})
        rel += [{"value": sp["specUrl"], "relatedIdentifierType": "URL", "relationType": "IsDocumentedBy"}
                for sp in f.specs.get(p, []) if sp.get("specUrl")]
        rel.append({"value": SITE_PAGE.format(site=s.lower()), "relatedIdentifierType": "URL",
                    "relationType": "References"})
        for link in self._links.get(p, []):
            if link.get("url"):
                rel.append({"value": link["url"], "relatedIdentifierType": "URL", "relationType": "References"})
        n_files, n_bytes = len(files), sum(self.size_of(r) for r in files)
        tables = sorted({r["tableName"] for r in files if r.get("kind") == "data" and r.get("tableName")})
        lake_tables = sorted(set(f.lake_tables.get(p, [])))
        tech = (f"NEON {p} basic package at {s}: {n_files} files ({n_bytes} bytes) over {len(months)} site-months "
                f"({month_ranges(months) or 'none'}), stored as raw/{p}/<YYYY-MM>/<NEON file name>. "
                + (f"NEON tables: {', '.join(tables)}. " if tables else "")
                + (f"Stacked site-lake tables in {self.lake_rel}: {', '.join(lake_tables)}." if lake_tables else "")
                + (" Externally hosted data: " + ", ".join(lk["url"] for lk in self._links.get(p, []) if lk.get("url"))
                   if self._links.get(p) else ""))
        descs = []
        abstract = rec.get("productAbstract") or rec.get("productDescription")
        if abstract:
            descs.append({"value": abstract[:ABSTRACT_CHARS], "descriptionType": "Abstract"})
        if rec.get("productDesignDescription"):
            descs.append({"value": rec["productDesignDescription"][:ABSTRACT_CHARS], "descriptionType": "Methods"})
        descs.append({"value": tech, "descriptionType": "TechnicalInfo"})
        accessed = self.download_date(p) or self.download_date() or env.utcnow().date().isoformat()
        for tag, doi in releases:
            descs.append({"value": self.citation(rec, p, tag, doi, accessed), "descriptionType": "Other"})
        if prov:
            descs.append({"value": self.citation(rec, p, None, None, accessed), "descriptionType": "Other"})
        version = " + ".join([t for t, _d in releases] + (["PROVISIONAL"] if prov else []))
        info = f.site_info
        fields: dict[str, Any] = {
            "irods_path": f"{self.S}/raw/{p}",
            "title": f"NEON {name} ({p}) at {site_name} ({s})" + (f", {years}" if years else ""),
            "resourceTypeGeneral": "Dataset",
            "resourceType": " ".join(x for x in (rec.get("productCategory"), "(basic package)") if x),
            "publicationYear": self.pub_year(p),
            "subjects": subjects, "relatedIdentifiers": rel, "descriptions": descs,
            "contributors": [self.curator()],
            "formats": sorted({mime(r["name"]) for r in files}),
            "sizes": [f"{n_files} files", f"{n_bytes} bytes"],
        }
        if version:
            fields["version"] = version
        geo = {k: v for k, v in (("point", point(info.get("siteLatitude"), info.get("siteLongitude"))),
                                 ("place", f"{site_name} ({s})")) if v}
        fields["geoLocations"] = [geo]
        collected = self.date_range(cov, months)
        if collected:
            fields["dates"] = [{"date": collected, "dateType": "Collected"}]
        return self.datacite_avus(f"{self.S}/raw/{p}", fields)

    # ------------------------------------------------------------ month level + objects
    def month_level(self, p: str, m: str) -> None:
        f = self.f
        path = f"{self.S}/raw/{p}/{m}"
        files = self._files_by_month.get((p, m), [])
        sm = next((r for r in self._months_by_product.get(p, []) if r["month"] == m), {})
        release = sm.get("release") or next((r.get("release") for r in files if r.get("release")), None)
        release_only = bool(sm.get("isReleaseOnly")) or any(r.get("isReleaseOnly") for r in files)
        stats = self.stats_for(files)
        data = [r for r in files if r.get("kind") == "data"]
        is_files = [r for r in data if r.get("hor")]
        os_files = [r for r in data if not r.get("hor") and (r.get("ext") or "").lower() != "h5"]
        os_stats = self.stats_for(os_files)
        avus: list[Avu | None] = [
            ("neon.month", m),
            ("neon.release", release, self.doi_unit(p, release)),
            ("neon.releaseOnly", "true") if release_only else None,
            ("neon.generationDate", iso(sm.get("packageGenerationDate") or sm.get("releaseGenerationDate")),
             "ISO8601"),
            ("neon.fileCount", str(len(files)), "count"),
            ("neon.totalBytes", str(sum(self.size_of(r) for r in files)), "byte"),
            ("neon.rowCount", str(sum(int(s["rowCount"] or 0) for s in stats)), "count") if stats else None,
            ("neon.temporalCoverage", self.coverage(stats), "ISO8601 interval"),
        ]
        if is_files:
            avus.append(("neon.sensorCount", str(len({(r["hor"], r["ver"]) for r in is_files})), "count"))
        if os_stats:
            plots = {pid for s in os_stats for pid in s.get("plotIDs") or []}
            taxa = {t["taxonID"] for r in os_files for t in f.file_taxa.get(r["name"], []) if t.get("taxonID")}
            protos = sorted({pv for s in os_stats for pv in s.get("protocolVersions") or []})
            if plots:
                avus.append(("neon.plotCount", str(len(plots)), "count"))
            if taxa:
                avus.append(("neon.taxaCount", str(len(taxa)), "count"))
            avus += [("neon.protocolVersion", pv) for pv in protos]
        self.coll(path, "month", avus)
        for r in files:
            profile, oavus = self.object_avus(r)
            self.obj(f"{path}/{r['name']}", "object", oavus, profile=profile)

    def sensor_row(self, p: str, m: str, hor: str, ver: str) -> dict[str, Any] | None:
        rows = self.f.sensor_positions.get((p, m, f"{hor}.{ver}")) or []
        if not rows:
            return None
        start, end = month_bounds(m)

        def active(r: dict[str, Any]) -> bool:
            a, b = as_utc(r.get("positionStartDateTime")), as_utc(r.get("positionEndDateTime"))
            return (a is None or a < end) and (b is None or b >= start)

        cands = [r for r in rows if active(r)] or rows
        return max(cands, key=lambda r: as_utc(r.get("positionStartDateTime")) or datetime.min.replace(tzinfo=UTC))

    def object_avus(self, r: dict[str, Any]) -> tuple[str, list[Avu | None]]:
        f, s = self.f, self.site
        p, m, name, kind = r["productCode"], r["month"], r["name"], r.get("kind") or "other"
        lab = lab_name_of(name, p) if kind in ("lab", "other") else None
        if lab and kind == "other":  # lakes listed before the shared classifier knew lab files
            kind = "lab"
        ext = (r.get("ext") or (name.rsplit(".", 1)[-1] if "." in name else "")).lower()
        release = r.get("release")
        avus: list[Avu | None] = [
            ("neon.productCode", p), ("neon.siteCode", s),
            ("neon.domainCode", r.get("domainCode") or f.site_info.get("domainCode")),
            ("neon.month", m), ("neon.release", release, self.doi_unit(p, release)),
            ("neon.package", PACKAGE), ("neon.fileKind", kind),
            ("neon.publicationDate", iso(r.get("publicationTimestamp")), "ISO8601"),
            ("dc.source", f"{API_DATA}/{p}/{s}/{m}/{name}", "URL"),
        ]
        if r.get("isReleaseOnly"):
            avus.append(("neon.releaseOnly", "true"))
        md5_ok, transfer, neon_md5 = md5_state(r, f.downloads.get((p, m, name)))
        if md5_ok is False:
            avus.append(("neon.md5Verified", "false"))
            if transfer:  # intact transfer, stale NEON md5 (probe finding F1): record what NEON lists
                avus.append(("neon.neonMd5Mismatch", neon_md5, ""))
        st = f.file_stats.get(name) or {}
        rows_avu = ("neon.rowCount", str(st["rowCount"]), "count") if st.get("rowCount") is not None else None
        cov = ("neon.temporalCoverage", interval(st.get("timeMin"), st.get("timeMax")), "ISO8601 interval")
        if kind == "data" and ext == "h5":
            tower = f.h5_tower.get(name)
            avus += [("neon.table", r.get("tableName")), cov, rows_avu,
                     ("neon.towerPoint", point(*tower), "WGS84 lat lon") if tower else None]
            return "hdf5", avus
        if kind == "data" and r.get("hor"):
            hor, ver = r["hor"], r.get("ver")
            sp = self.sensor_row(p, m, hor, ver) if ver else None
            avus += [("neon.table", r.get("tableName")), ("neon.hor", hor), ("neon.ver", ver),
                     ("neon.tmi", r.get("tmi")), rows_avu, cov,
                     ("neon.finalQFFailPercent", num(st["qfFailPct"], 4), "percent")
                     if st.get("qfFailPct") is not None else None,
                     ("mesa.ducklake.table", st.get("lakeTable") or self.lake_table(p, r.get("tableName")),
                      self.lake_unit)]
            if sp:
                avus += [("neon.namedLocation", sp.get("sensorLocationID")),
                         ("neon.locationDescription", sp.get("sensorLocationDescription")),
                         ("neon.sensorPoint", point(sp.get("sensorLatitude"), sp.get("sensorLongitude")),
                          "WGS84 lat lon"),
                         ("neon.sensorElevation", num(sp["sensorElevation"], 3)
                          if sp.get("sensorElevation") is not None else None, "meter"),
                         ("neon.zOffset", num(sp["zOffset"], 3) if sp.get("zOffset") is not None else None,
                          "meter")]
            return "is", avus
        if kind == "data":
            avus += [("neon.table", r.get("tableName")), rows_avu, cov]
            avus += [("neon.protocolVersion", pv) for pv in sorted(set(st.get("protocolVersions") or []))]
            plots = sorted(set(st.get("plotIDs") or []))
            if st.get("plotCount") is not None or plots:
                avus.append(("neon.plotCount", str(st.get("plotCount") if st.get("plotCount") is not None
                                                   else len(plots)), "count"))
            avus += [("neon.plotID", pid) for pid in plots[:MAX_PLOT_IDS]]
            taxa = f.file_taxa.get(name, [])
            if st.get("taxonCount") is not None or taxa:
                n_taxa = st.get("taxonCount") if st.get("taxonCount") is not None else len(
                    {t["taxonID"] for t in taxa})
                avus.append(("neon.taxaCount", str(n_taxa), "count"))
            avus += self.taxon_avus(taxa, unresolved=True)
            return "os", avus
        if kind == "lab":
            avus += [("neon.table", r.get("tableName") or (lab[1] if lab else None)),
                     ("neon.labName", lab[0] if lab else None)]
            return "lab", avus
        if kind in SUPPORT_TABLE_KINDS:
            avus.append(("neon.table", r.get("tableName")))
        elif kind == "eml":
            avus.append(("neon.temporalCoverage", eml_coverage(name), "ISO8601 interval"))
        elif kind == "variables":
            n = self.variable_count(r)
            avus.append(("neon.variableCount", str(n), "count") if n is not None else None)
        elif kind == "sensor_positions" and name in f.sensor_counts:
            avus.append(("neon.sensorCount", str(f.sensor_counts[name]), "count"))
        return "support", avus

    def lake_table(self, p: str, table: str | None) -> str | None:
        """The stacked lake table of (product, NEON table) when the site lake has it."""
        if not table:
            return None
        name = f"{p.lower().replace('.', '_')}__{table.lower()}"
        return name if name in self.f.lake_tables.get(p, []) else None

    def variable_count(self, r: dict[str, Any]) -> int | None:
        rel = r.get("localRelPath") or self.local.raw_rel(r["productCode"], r["month"], r["name"])
        return csv_records(env.ROOT / rel)

    # ------------------------------------------------------------ driver
    def build(self) -> list[Row]:
        self.site_level()
        self.containers()
        products = sorted({r["productCode"] for r in self.f.site_months} | set(self._files_by_product)
                          | set(self._links))
        self.info["products_built"] = []
        self.info["months_built"] = 0
        for p in products:
            if not self.product_ok(p):
                continue
            self.product_level(p)
            self.info["products_built"].append(p)
            for m in sorted({mm for (pp, mm) in self._files_by_month if pp == p}):
                if self.month_ok(p, m):
                    self.month_level(p, m)
                    self.info["months_built"] += 1
        if self.P:
            missing = sorted(self.P - set(products))
            if missing:
                log.warning("products not in the site lake (nothing built): %s", ", ".join(missing))
                self.info["products_missing"] = missing
        return self.rows


# ---------------------------------------------------------------- writing avu_desired

def _normalise(scope: str, rows: Iterable[Row]) -> tuple[dict[tuple[str, str, str, str], tuple], int, int]:
    """Exactly ``bulk.replace_desired``'s normalisation (strip, drop empties, 2,000-byte cap, no protected)."""
    out: dict[tuple[str, str, str, str], tuple] = {}
    dropped = protected = 0
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
    return out, dropped, protected


def replace_units(con: sqlite3.Connection, scope: str, rows: list[Row], rebuilds: Callable[[str], bool]
                  ) -> dict[str, int]:
    """Filtered replace: delete the rebuilt units (and re-inserted paths) of ``scope``, insert ``rows``; one txn."""
    bulk.ensure_desired(con)
    out, dropped, protected = _normalise(scope, rows)
    existing = [r[0] for r in con.execute("SELECT DISTINCT unit_id FROM avu_desired WHERE scope=?", (scope,))]
    units = sorted({u for u in existing if rebuilds(u)} | {v[1] for v in out.values()})
    paths = sorted({v[2] for v in out.values()})
    deleted = 0
    con.execute("BEGIN IMMEDIATE")
    try:
        for i in range(0, len(units), 500):
            chunk = units[i:i + 500]
            deleted += con.execute(f"DELETE FROM avu_desired WHERE scope=? AND unit_id IN ({','.join('?' * len(chunk))})",
                                   (scope, *chunk)).rowcount
        for i in range(0, len(paths), 500):
            chunk = paths[i:i + 500]
            deleted += con.execute(f"DELETE FROM avu_desired WHERE scope=? AND path IN ({','.join('?' * len(chunk))})",
                                   (scope, *chunk)).rowcount
        con.executemany(
            "INSERT INTO avu_desired(scope, unit_id, path, target_type, attribute, value, unit, level, generator)"
            " VALUES (?,?,?,?,?,?,?,?,?)", list(out.values()))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    if protected:
        log.warning("replace_units(%s): dropped %d protected AVUs (ipc*/mesa.enabled)", scope, protected)
    return {"rows": len(out), "dropped_empty": dropped, "dropped_protected": protected, "units_replaced": len(units),
            "rows_deleted": deleted}


# ---------------------------------------------------------------- stats + entry points

def level_stats(con: sqlite3.Connection, scope: str, units: set[str] | None = None) -> dict[str, dict[str, Any]]:
    """Per level: targets, rows and AVUs per target (min/mean/max) of the stored rows."""
    per: dict[str, Counter] = defaultdict(Counter)
    for unit_id, level, path, n in con.execute(
            "SELECT unit_id, level, path, count(*) FROM avu_desired WHERE scope=? GROUP BY unit_id, level, path",
            (scope,)):
        if units is not None and unit_id not in units:
            continue
        per[level or "?"][path] += n
    out = {}
    for level in sorted(per):
        counts = list(per[level].values())
        out[level] = {"targets": len(counts), "rows": sum(counts), "min": min(counts),
                      "mean": round(sum(counts) / len(counts), 1), "max": max(counts)}
    return out


def build_desired(site: str, products: list[str] | None = None, months: list[str] | None = None, *,
                  con: sqlite3.Connection | None = None, facts: Facts | None = None) -> dict[str, Any]:
    """Build and store the desired AVUs of one site (scope = site code). Returns a summary dict."""
    site = site.upper()
    products = [p.strip() for p in products or [] if p and p.strip()] or None
    months = [m.strip() for m in months or [] if m and m.strip()] or None
    facts = facts or load_facts(site)
    b = Builder(facts, products, months)
    rows = b.build()
    con = con or state.connect(state.DB_PATH)
    filtered = bool(products or months)
    res = replace_units(con, site, rows, b.rebuilds) if filtered else bulk.replace_desired(con, site, rows)
    built_units = {r[0] for r in rows}
    profiles = Counter(prof for lvl, prof in b.targets.values() if lvl == "object")
    summary: dict[str, Any] = {
        "site": site, "scope": site, "filtered": filtered, "products": products, "months": months,
        **res,
        "units_built": len(built_units),
        "scope_rows": con.execute("SELECT count(*) FROM avu_desired WHERE scope=?", (site,)).fetchone()[0],
        "scope_units": con.execute("SELECT count(DISTINCT unit_id) FROM avu_desired WHERE scope=?",
                                   (site,)).fetchone()[0],
        "levels": level_stats(con, site, built_units),
        "object_profiles": dict(sorted(profiles.items())),
        "products_built": b.info.get("products_built", []),
        "months_built": b.info.get("months_built", 0),
        "catalog": facts.catalog_ok,
        "ontology_tables": sorted(facts.ontology.tables),
    }
    for k in ("datacite_errors", "skipped", "releases_without_doi", "nlcd_unmapped", "products_missing"):
        if b.info.get(k):
            summary[k] = b.info[k]
    log.info("site %s desired: %d rows over %d units (%s)", site, res["rows"], len(built_units),
             "filtered" if filtered else "full scope")
    return summary


def render(summary: dict[str, Any]) -> str:
    scope = (f"filtered: products={','.join(summary['products'] or ['*'])} months={','.join(summary['months'] or ['*'])}"
             if summary["filtered"] else "full scope")
    head = (f"rows written {summary['rows']} (units {summary['units_built']}); scope now {summary['scope_rows']} "
            f"rows over {summary['scope_units']} units; catalog={'yes' if summary['catalog'] else 'NO'}; "
            f"ontology tables: {', '.join(summary['ontology_tables']) or 'none'}")
    lines = [f"site build-avus {summary['site']} ({scope})", head, "",
             f"{'level':14} {'targets':>8} {'rows':>9} {'min':>5} {'mean':>7} {'max':>5}"]
    for lvl, st in summary["levels"].items():
        lines.append(f"{lvl:14} {st['targets']:>8} {st['rows']:>9} {st['min']:>5} {st['mean']:>7} {st['max']:>5}")
    if summary["object_profiles"]:
        lines.append("object profiles: " + ", ".join(f"{k}={v}" for k, v in summary["object_profiles"].items()))
    for k in ("skipped", "datacite_errors", "releases_without_doi", "nlcd_unmapped", "products_missing"):
        if summary.get(k):
            lines.append(f"{k}: {summary[k]}")
    return "\n".join(lines)


def samples(con: sqlite3.Connection, scope: str, per_level: int = 1, max_avus: int = 14) -> str:
    """A few stored AVUs per level (first target of each level)."""
    lines = []
    for (level,) in con.execute("SELECT DISTINCT level FROM avu_desired WHERE scope=? ORDER BY level", (scope,)):
        paths = [r[0] for r in con.execute("SELECT DISTINCT path FROM avu_desired WHERE scope=? AND level=? "
                                           "ORDER BY path LIMIT ?", (scope, level, per_level))]
        for path in paths:
            rows = con.execute("SELECT attribute, value, unit FROM avu_desired WHERE scope=? AND path=? "
                               "ORDER BY attribute, value", (scope, path)).fetchall()
            lines.append(f"-- [{level}] {path} ({len(rows)} AVUs)")
            for a, v, u in rows[:max_avus]:
                vv = v if len(v) <= 90 else v[:87] + "..."
                lines.append(f"   {a} = {vv}" + (f"  [{u}]" if u else ""))
            if len(rows) > max_avus:
                lines.append(f"   ... {len(rows) - max_avus} more")
    return "\n".join(lines)


def main(site: str, products: list[str] | None = None, months: list[str] | None = None) -> int:
    """``neon-ducklake site build-avus <SITE>``: build ``avu_desired`` rows for the site, print stats."""
    site = site.upper()
    con = state.connect(state.DB_PATH)
    summary = build_desired(site, products, months, con=con)
    print(render(summary))
    print()
    print(samples(con, site))
    return 1 if summary.get("datacite_errors") else 0
