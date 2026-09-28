"""Phase 1 — flatten sealed NEON API payloads into typed Arrow tables, one per catalog-lake table.

Every ``flatten_*`` function is pure: payload(s) in, ``pyarrow.Table``(s) out, typed by the ``TABLES`` specs
that catalog_lake.py also turns into DDL. Tables are snake_case, columns keep NEON's camelCase.

The file inventory (``sealed/data_query/<DP>.jsonl.gz``, ~3.5M rows) is flattened per product in a
``ProcessPoolExecutor`` into ``api/runs/<runId>/stage/files/<DP>.parquet`` and
``stage/packages/<DP>.parquet``, cached by the sealed input's sha256 (+ ``FLATTEN_VERSION``).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# external-lab files (NEON.<Lab_Name>.<table>.<YYYYMMDDTHHMMSSZ>.<ext>): the grammar lives in filenames.py,
# shared with the site fetch/stack/AVU code; LAB_FILE_RE stays importable from here
from .filenames import LAB_FILE_RE, classify  # noqa: F401

log = logging.getLogger("neon_ducklake.flatten")

FLATTEN_VERSION = 2  # bump whenever the files/site_month_packages rows change (invalidates stage/ caches)
CURRENT = "CURRENT"  # data_query_pairs.requestRelease of a current (+ PROVISIONAL) request
PROVISIONAL = "PROVISIONAL"
AOP_CODE_RE = re.compile(r"^DP[1-4]\.30\d{3}\.\d{3}$")
DQ_KEY_RE = re.compile(r"^(?P<product>DP\d\.\d{5}\.\d{3})__(?P<site>[A-Z0-9]+)__(?P<start>\d{4}-\d{2})_"
                       r"(?P<end>\d{4}-\d{2})(?:__(?P<release>.+))?$")
MANIFEST_DP_RE = re.compile(r"^NEON\.DOM\.[A-Z]+\.")
SIGNED_MARK = "X-Goog"

# ---------------------------------------------------------------- table specs

ARROW: dict[str, pa.DataType] = {
    "str": pa.string(), "int": pa.int32(), "big": pa.int64(), "dbl": pa.float64(), "bool": pa.bool_(),
    "tstz": pa.timestamp("us", tz="UTC"), "ts": pa.timestamp("us"), "json": pa.string(),
    "list": pa.list_(pa.string()), "map": pa.map_(pa.string(), pa.string()),
}
SQL_TYPE: dict[str, str] = {
    "str": "VARCHAR", "int": "INTEGER", "big": "BIGINT", "dbl": "DOUBLE", "bool": "BOOLEAN",
    "tstz": "TIMESTAMPTZ", "ts": "TIMESTAMP", "json": "JSON", "list": "VARCHAR[]", "map": "MAP(VARCHAR, VARCHAR)",
}


@dataclass(frozen=True)
class TableSpec:
    """One catalog-lake table: typed columns, logical key, provenance."""

    name: str
    group: str
    key: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]
    description: str
    sources: tuple[str, ...] = ()
    scope_col: str | None = None  # upsert only touches rows whose scope_col equals the staged value(s)
    derived: tuple[tuple[str, str, str], ...] = ()  # (name, SQL type, SQL expr) computed after the row hash

    @property
    def names(self) -> list[str]:
        return [c for c, _t in self.columns]

    def arrow_schema(self) -> pa.Schema:
        return pa.schema([(c, ARROW[t]) for c, t in self.columns])

    def sql_columns(self) -> list[tuple[str, str]]:
        return [(c, SQL_TYPE[t]) for c, t in self.columns]


def _cols(spec: str) -> tuple[tuple[str, str], ...]:
    out = []
    for tok in spec.split():
        name, _, typ = tok.partition(":")
        out.append((name, typ or "str"))
    return tuple(out)


PRODUCT_SCALARS = ("productCode productCodeLong productCodePresentation productName productDescription productAbstract "
                   "productDesignDescription productStudyDescription productBasicDescription "
                   "productExpandedDescription productSensor productRemarks productStatus productCategory "
                   "productScienceTeam productScienceTeamAbbr productPublicationFormatType productHasExpanded:bool")
CHANGE_LOG = ("id:big parentIssueID:big issueDate:tstz resolvedDate:tstz dateRangeStart:tstz dateRangeEnd:tstz "
              "locationAffected issue resolution")
LOCATION = ("locationName locationType locationDescription siteCode domainCode locationDecimalLatitude:dbl "
            "locationDecimalLongitude:dbl locationElevation:dbl locationUtmEasting:dbl locationUtmNorthing:dbl "
            "locationUtmZone:int locationUtmHemisphere alphaOrientation:dbl betaOrientation:dbl "
            "gammaOrientation:dbl xOffset:dbl yOffset:dbl zOffset:dbl offsetLocationName locationParent "
            "activePeriods:json locationProperties:json")

_SPECS = [
    # ---- products
    TableSpec("products", "products", ("productCode",), _cols(
        PRODUCT_SCALARS + " isAop:bool themes:list keywords:list siteCount:int cellCount:int "
        "provisionalCellCount:int firstMonth lastMonth releaseCount:int latestRelease latestReleaseDoi "
        "specCount:int changeLogCount:int biorepositoryCount:int"),
        "One row per NEON data product (current /products); site/cell counts from current availability.",
        ("products",)),
    TableSpec("product_themes", "products", ("productCode", "theme"), _cols("productCode theme"),
              "Product x theme.", ("products",)),
    TableSpec("product_keywords", "products", ("productCode", "keyword"), _cols("productCode keyword"),
              "Product x keyword.", ("products",)),
    TableSpec("product_specs", "products", ("productCode", "specNumber"), _cols(
        "productCode specNumber specId specType specSize:big specDescription specUrl"),
        "Documents (specs) attached to each product.", ("products",)),
    TableSpec("documents", "products", ("specNumber",), _cols(
        "specNumber specId specType specSize:big specDescription specUrl productCodes:list productCount:int "
        "headStatus:int contentType contentLength:big fileName headError"),
        "Distinct documents referenced by products, with the HEAD /documents/{spec} status.",
        ("products", "documents_head")),
    TableSpec("product_change_logs", "products", ("productCode", "id"), _cols("productCode " + CHANGE_LOG),
              "Product change logs (issues) as served by the current /products.", ("products",)),
    TableSpec("product_releases", "products", ("productCode", "release"), _cols(
        "productCode release generationDate:tstz url productDoiUrl productDoi productDoiGenerationDate:tstz"),
        "Product x release with the release DOI (productDoi is the bare 10.48443/... DOI).", ("products",)),
    TableSpec("product_biorepository", "products", ("productCode", "collectionCode"), _cols(
        "productCode collectionCode collectionName collectionUrl collectionContentUrl collectionDownloadUrl"),
        "Biorepository collections linked to a product.", ("products",)),
    # ---- releases
    TableSpec("releases", "releases", ("release",), _cols(
        "release uuid generationDate:tstz productCount:int siteCount:int artifactCount:int manifestName "
        "manifestMd5 manifestSize:big manifestCellCount:big"),
        "NEON releases (tags) with their MANIFEST_AVAILABLE artifact.", ("releases", "manifests", "release_sites")),
    TableSpec("release_artifacts", "releases", ("release", "name"), _cols("release name type url size:big md5"),
              "Release artifacts (URLs unsigned).", ("releases",)),
    TableSpec("release_products", "releases", ("release", "productCode"), _cols(
        "release productCode productName productDescription productDoiUrl productDoi"),
        "Products in each release with the release DOI (from /releases).", ("releases",)),
    TableSpec("release_product_records", "releases", ("release", "productCode"), _cols(
        "release " + PRODUCT_SCALARS + " themes:list keywords:list generationDate:tstz productDoiUrl productDoi "
        "productDoiGenerationDate:tstz siteCount:int cellCount:int firstMonth lastMonth specNumbers:list "
        "changeLogCount:int"),
        "Product records as published in each release (/releases/{tag}/products), in-release products only.",
        ("release_products",)),
    TableSpec("release_product_change_logs", "releases", ("release", "productCode", "id"),
              _cols("release productCode " + CHANGE_LOG),
              "Change logs of the in-release product records.", ("release_products",)),
    TableSpec("release_site_products", "releases", ("release", "siteCode", "productCode"), _cols(
        "release siteCode productCode productTitle monthCount:int firstMonth lastMonth availableMonths:list"),
        "Site x product availability as published in each release (/releases/{tag}/sites).",
        ("release_sites", "release_products")),
    TableSpec("release_manifest_months", "releases", ("release", "productCode", "siteCode", "month"),
              _cols("release productCode siteCode month"),
              "Every product x site x month cell of each release manifest (MANIFEST_AVAILABLE).", ("manifests",)),
    # ---- sites and locations
    TableSpec("sites", "sites", ("siteCode",), _cols(
        "siteCode siteName siteDescription siteType siteLatitude:dbl siteLongitude:dbl stateCode stateName "
        "domainCode domainName deimsId siteElevation:dbl siteUtmZone:int siteUtmEasting:dbl siteUtmNorthing:dbl "
        "siteUtmHemisphere siteProperties:map productCount:int releaseCount:int"),
        "NEON field sites (/sites) + elevation/UTM/properties of the /locations/sites SITE record "
        "(property names without the 'Value for ' prefix).", ("sites", "locations_sites")),
    TableSpec("site_releases", "sites", ("siteCode", "release"), _cols("siteCode release generationDate:tstz url"),
              "Releases containing each site.", ("sites",)),
    TableSpec("site_products", "sites", ("siteCode", "productCode"), _cols(
        "siteCode productCode productTitle monthCount:int provisionalMonthCount:int firstMonth lastMonth "
        "availableMonths:list releases:list"),
        "Current site x product availability (/sites dataProducts).", ("sites",)),
    TableSpec("site_locations", "sites", ("locationName",), _cols(LOCATION + " locationChildrenCount:int"),
              "SITE named-location records from /locations/sites.", ("locations_sites",)),
    TableSpec("domains", "sites", ("domainCode",), _cols("domainCode domainName siteCount:int siteCodes:list"),
              "NEON domains (from /sites).", ("sites",)),
    TableSpec("locations", "sites", ("locationName",), _cols(
        LOCATION + " locationPolygon:json hasPolygon:bool hierarchyDepth:int hierarchyParent hierarchySite"),
        "Every named location (GraphQL findLocations over the /locations hierarchy walk).",
        ("locations", "stage/location_nodes")),
    TableSpec("location_polygons", "sites", ("locationName",), _cols(
        "locationName locationType siteCode domainCode vertexCount:int wkt"),
        "Locations with a polygon (WKT, lon/lat, ring closed); geometry derived with ST_GeomFromText.",
        ("locations",), derived=(("geometry", "GEOMETRY", "TRY(ST_GeomFromText(wkt))"),)),
    TableSpec("location_history", "sites", ("locationName", "historyIndex"), _cols(
        "locationName historyIndex:int isCurrent:bool locationStartDate:tstz locationEndDate:tstz "
        "locationDecimalLatitude:dbl locationDecimalLongitude:dbl locationElevation:dbl locationUtmEasting:dbl "
        "locationUtmNorthing:dbl locationUtmZone:int locationUtmHemisphere xOffset:dbl yOffset:dbl zOffset:dbl "
        "alphaOrientation:dbl betaOrientation:dbl gammaOrientation:dbl offsetLocationName locationProperties:json"),
        "Location history (/locations/{name}?history=true) of CONFIG/TOWER/SITE locations.",
        ("location_history",)),
    # ---- availability
    TableSpec("availability_months", "availability", ("productCode", "siteCode", "month"), _cols(
        "productCode siteCode month release isProvisional:bool isReleaseOnly:bool availableDataUrl"),
        "Product x site x month: current availability (release or PROVISIONAL) plus release-only cells "
        "(in a manifest, not current; release = newest manifest containing the cell).",
        ("products", "manifests")),
    TableSpec("product_site", "availability", ("productCode", "siteCode"), _cols(
        "productCode siteCode isAop:bool inventoryScope inventoryStatus currentMonthCount:int "
        "provisionalMonthCount:int releaseOnlyMonthCount:int firstMonth lastMonth releaseOnlyReleases:list "
        "fileCount:big totalBytes:big externalLinkCount:int"),
        "Product x site pairs (current + release-only). inventoryScope current/release-only/excluded:AOP; "
        "inventoryStatus ok/external/empty/error/missing (NULL for AOP).",
        ("products", "manifests", "data_query", "external")),
    # ---- other API
    TableSpec("taxonomy", "other", ("taxonTypeCode", "taxonID"), _cols(
        "taxonTypeCode taxonID acceptedTaxonID scientificName scientificNameAuthorship taxonRank vernacularName "
        "kingdom phylum class order family genus subgenus specificEpithet infraspecificEpithet nameAccordingToID "
        "nameAccordingToTitle taxonProtocolCategory speciesGroup updateDate:tstz record:json"),
        "NEON taxonomy (verbose), dwc:/gbif: prefixes stripped; the full record as JSON.", ("taxonomy",)),
    TableSpec("sample_classes", "other", ("sampleClass",), _cols("sampleClass description"),
              "Supported sample classes (/samples/supportedClasses).", ("samples",)),
    TableSpec("prototype_datasets", "other", ("uuid",), _cols(
        "uuid projectTitle projectDescription designDescription metadataDescription studyAreaDescription "
        "datasetAbstract startYear:int endYear:int dateUploaded:tstz isPublished:bool version versionDescription "
        "doiUrl doi doiGenerationDate:tstz dataUrl fileCount:int dataThemes:list scienceTeams:list keywords:list "
        "fileTypes:list relatedDataProducts:list relatedVersions:json publicationCitations:json dataLocations:json"),
        "Prototype datasets (/prototype/datasets).", ("prototype",)),
    TableSpec("prototype_files", "other", ("datasetUuid", "fileName"), _cols(
        "datasetUuid fileName name description size:big md5 typeName typeDescription url"),
        "Files of each prototype dataset (/prototype/data/{uuid}).", ("prototype_data", "prototype")),
    TableSpec("prototype_locations", "other", ("datasetUuid", "siteCode"), _cols(
        "datasetUuid siteCode siteName domain state latitude:dbl longitude:dbl"),
        "Sites of each prototype dataset.", ("prototype",)),
    TableSpec("graphql_types", "other", ("name",), _cols(
        "name kind description fieldCount:int inputFieldCount:int enumValueCount:int interfaces:list "
        "possibleTypes:list rootOperation"),
        "GraphQL schema types (introspection).", ("graphql",)),
    TableSpec("graphql_fields", "other", ("typeName", "memberKind", "name"), _cols(
        "typeName memberKind name description type args:json defaultValue isDeprecated:bool deprecationReason"),
        "GraphQL fields, input fields and enum values (memberKind field/inputField/enumValue).", ("graphql",)),
    # ---- inventory
    TableSpec("data_query_pairs", "inventory", ("productCode", "siteCode", "requestRelease"), _cols(
        "productCode siteCode requestRelease scope startMonth endMonth plannedMonthCount:int landingCount:int "
        "splitCount:int errorCount:int status fileCount:big totalBytes:big packageMonthCount:int "
        "lastFetchedAt:tstz error"),
        "POST /data/query requests per non-AOP product x site (requestRelease CURRENT = current + PROVISIONAL).",
        ("data_query", "stage/pairs")),
    TableSpec("site_month_packages", "inventory", ("productCode", "siteCode", "month", "release"), _cols(
        "productCode siteCode month release domainCode packageType generationDate:tstz "
        "releaseGenerationDate:tstz fileCount:int totalBytes:big isProvisional:bool isReleaseOnly:bool"),
        "Basic packages (product x site x month x release) of the file inventory.", ("data_query",)),
    TableSpec("files", "inventory", ("productCode", "siteCode", "month", "release", "name"), _cols(
        "productCode siteCode domainCode month release releaseGenerationDate:tstz packageType "
        "packageGenerationDate:tstz name size:big md5 crc32c objectUrl kind tableName hor ver tmi "
        "publicationTimestamp:ts ext isProvisional:bool isReleaseOnly:bool parseConsistent:bool"),
        "Non-AOP basic-package file inventory (DP0 support files attributed to the requested DP1 product; "
        "objectUrl unsigned).", ("data_query",)),
    TableSpec("external_data", "inventory", ("productCode", "siteCode", "url"), _cols(
        "productCode siteCode monthProbed name type url"),
        "External data links of products hosted outside NEON (probed on one month per zero-file pair).",
        ("external",)),
    # ---- meta
    TableSpec("harvest_runs", "meta", ("runId",), _cols(
        "runId status stages:list pilot:bool startedAt:tstz finishedAt:tstz seconds:dbl pairs:int "
        "failureCount:int versions:json tools:json tunables:json counts:json transforms:list sealedManifestMd5"),
        "Harvest runs (fetch_manifest.json) loaded into the catalog.", ("fetch_manifest",), scope_col="runId"),
    TableSpec("harvest_requests", "meta", ("runId", "family", "key"), _cols(
        "runId family key method path params:json httpStatus:int attempts:int elapsedMs:dbl responseBytes:big "
        "landedPath landedBytes:big sha256 status error fetchedAt:tstz"),
        "The request ledger of each run (work/state.sqlite requests).", ("ledger",), scope_col="runId"),
    TableSpec("catalog_builds", "meta", ("buildId",), _cols(
        "buildId builtAt:tstz runId runStatus allowPartial:bool inserted:json deleted:json rowCounts:json "
        "versions:json tools:json"),
        "One row per catalog build (append-only; guarantees one snapshot per build).", (), scope_col="buildId"),
    TableSpec("table_catalog", "meta", ("tableName",), _cols(
        "tableName tableGroup description keyColumns:list columnCount:int rowCount:big sourceFamilies:list "
        "source definitionSha256"),
        "Every table and view of the catalog lake with its provenance.", ()),
]
TABLES: dict[str, TableSpec] = {s.name: s for s in _SPECS}
PARQUET_TABLES = ("files", "site_month_packages")  # staged as per-product Parquet files

# ---------------------------------------------------------------- value helpers


def is_aop(code: str, team: str | None = None) -> bool:
    return (team or "").upper() == "AOP" or bool(AOP_CODE_RE.match(code or ""))


def bare_doi(url: str | None) -> str | None:
    from neon_mcp.projections.releases import bare_doi as _bare

    return _bare(url)


def strip_query(url: Any) -> Any:
    if isinstance(url, str) and "?" in url:
        return url.split("?", 1)[0]
    return url


def tstz(value: Any) -> datetime | None:
    """ISO 8601 (with Z/offset or naive=UTC) -> aware UTC datetime; None/'' -> None."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(UTC)


def neon_ts(value: str | None) -> datetime | None:
    """NEON file-name timestamp '20250128T000000Z' -> naive UTC datetime."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%S%z").replace(tzinfo=None)
    except ValueError:
        return None


def _int(v: Any) -> int | None:
    if v is None or v == "" or isinstance(v, bool):
        return None if not isinstance(v, bool) else int(v)
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _dbl(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return str(v)


def _json(v: Any) -> str | None:
    return None if v is None else json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def _list(v: Any) -> list[str] | None:
    if v is None:
        return None
    return [_str(x) for x in v if x is not None]


def _map(v: Any) -> list[tuple[str, str]] | None:
    if v is None:
        return None
    items = v.items() if isinstance(v, dict) else v
    return [(str(k), _str(val)) for k, val in items]


CONVERT = {"str": _str, "int": _int, "big": _int, "dbl": _dbl, "bool": lambda v: None if v is None else bool(v),
           "tstz": tstz, "ts": lambda v: v if isinstance(v, datetime) else neon_ts(v), "json": _json,
           "list": _list, "map": _map}


def to_table(name: str, rows: Iterable[dict[str, Any]]) -> pa.Table:
    """Rows (dicts keyed by column name; missing keys -> NULL) -> Arrow table typed by TABLES[name]."""
    spec = TABLES[name]
    rows = list(rows)
    arrays = []
    for col, typ in spec.columns:
        conv = CONVERT[typ]
        arrays.append(pa.array([conv(r.get(col)) for r in rows], type=ARROW[typ]))
    return pa.Table.from_arrays(arrays, schema=spec.arrow_schema())


def empty_table(name: str) -> pa.Table:
    return TABLES[name].arrow_schema().empty_table()


def month_of_url(url: str | None) -> str | None:
    return url.rstrip("/").rsplit("/", 1)[-1] if url else None


# ---------------------------------------------------------------- products


def _change_log_row(cl: dict[str, Any]) -> dict[str, Any]:
    return {k: cl.get(k) for k in ("id", "parentIssueID", "issueDate", "resolvedDate", "dateRangeStart",
                                   "dateRangeEnd", "locationAffected", "issue", "resolution")}


def _product_scalars(p: dict[str, Any]) -> dict[str, Any]:
    return {c: p.get(c) for c, _t in _cols(PRODUCT_SCALARS)}


def _availability_stats(site_codes: list[dict[str, Any]]) -> dict[str, Any]:
    months = sorted({m for s in site_codes for m in s.get("availableMonths") or []})
    cells = sum(len(s.get("availableMonths") or []) for s in site_codes)
    prov = sum(len(r.get("availableMonths") or []) for s in site_codes for r in s.get("availableReleases") or []
               if r.get("release") == PROVISIONAL)
    return {"siteCount": len({s.get("siteCode") for s in site_codes}), "cellCount": cells,
            "provisionalCellCount": prov, "firstMonth": months[0] if months else None,
            "lastMonth": months[-1] if months else None}


def flatten_products(products: list[dict[str, Any]]) -> dict[str, pa.Table]:
    """/products -> products, product_themes, product_keywords, product_specs, product_change_logs,
    product_releases, product_biorepository."""
    rows, themes, kws, specs, logs, rels, bio = [], {}, {}, {}, {}, {}, {}
    for p in products:
        code = p["productCode"]
        sites = p.get("siteCodes") or []
        prels = [r for r in p.get("releases") or [] if r.get("release")]
        latest = max(prels, key=lambda r: r["release"], default=None)
        row = {**_product_scalars(p), **_availability_stats(sites),
               "isAop": is_aop(code, p.get("productScienceTeamAbbr")),
               "themes": p.get("themes") or [], "keywords": p.get("keywords") or [],
               "releaseCount": len(prels), "latestRelease": latest and latest["release"],
               "latestReleaseDoi": latest and bare_doi((latest.get("productDoi") or {}).get("url")),
               "specCount": len(p.get("specs") or []), "changeLogCount": len(p.get("changeLogs") or []),
               "biorepositoryCount": len(p.get("biorepositoryCollections") or [])}
        rows.append(row)
        for t in p.get("themes") or []:
            themes[(code, t)] = {"productCode": code, "theme": t}
        for k in p.get("keywords") or []:
            kws[(code, k)] = {"productCode": code, "keyword": k}
        for s in p.get("specs") or []:
            if s.get("specNumber"):
                specs[(code, s["specNumber"])] = {"productCode": code, **s}
        for cl in p.get("changeLogs") or []:
            if cl.get("id") is not None:
                logs[(code, cl["id"])] = {"productCode": code, **_change_log_row(cl)}
        for r in prels:
            doi = r.get("productDoi") or {}
            rels[(code, r["release"])] = {
                "productCode": code, "release": r["release"], "generationDate": r.get("generationDate"),
                "url": r.get("url"), "productDoiUrl": doi.get("url"), "productDoi": bare_doi(doi.get("url")),
                "productDoiGenerationDate": doi.get("generationDate")}
        for c in p.get("biorepositoryCollections") or []:
            cc = c.get("collectionCode") or c.get("collectionName")
            if cc:
                bio[(code, cc)] = {"productCode": code, **c, "collectionCode": cc}
    return {"products": to_table("products", rows), "product_themes": to_table("product_themes", themes.values()),
            "product_keywords": to_table("product_keywords", kws.values()),
            "product_specs": to_table("product_specs", specs.values()),
            "product_change_logs": to_table("product_change_logs", logs.values()),
            "product_releases": to_table("product_releases", rels.values()),
            "product_biorepository": to_table("product_biorepository", bio.values())}


def flatten_documents(products: list[dict[str, Any]], heads: list[dict[str, Any]]) -> pa.Table:
    """Distinct specs over all products + HEAD /documents/{spec} results."""
    docs: dict[str, dict[str, Any]] = {}
    owners: dict[str, set[str]] = defaultdict(set)
    for p in products:
        for s in p.get("specs") or []:
            num = s.get("specNumber")
            if not num:
                continue
            docs.setdefault(num, dict(s))
            owners[num].add(p["productCode"])
    for h in heads:
        num = h.get("specNumber")
        if not num:
            continue
        d = docs.setdefault(num, {"specNumber": num})
        d.update({"headStatus": h.get("status"), "contentType": h.get("contentType"),
                  "contentLength": h.get("contentLength"), "fileName": h.get("fileName"), "headError": h.get("error")})
    rows = []
    for num, d in sorted(docs.items()):
        codes = sorted(owners.get(num, ()))
        rows.append({**d, "productCodes": codes, "productCount": len(codes)})
    return to_table("documents", rows)


# ---------------------------------------------------------------- releases


def manifest_cells(manifests: dict[str, dict[str, Any]]) -> dict[tuple[str, str, str], list[str]]:
    """(productCode, siteCode, month) -> sorted release tags whose manifest contains the cell."""
    out: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for tag, man in manifests.items():
        for dp in man.get("dpSiteMonths") or []:
            code = MANIFEST_DP_RE.sub("", dp.get("dp") or "")
            for sm in dp.get("siteMonths") or []:
                for m in sm.get("months") or []:
                    out[(code, sm.get("site"), m)].add(tag)
    return {k: sorted(v) for k, v in out.items()}


def flatten_releases(releases: list[dict[str, Any]], manifests: dict[str, dict[str, Any]],
                     release_products: dict[str, list[dict[str, Any]]],
                     release_sites: dict[str, list[dict[str, Any]]]) -> dict[str, pa.Table]:
    """/releases + manifests + /releases/{tag}/{products,sites} -> the release tables."""
    rel_rows, arts, rprods, records, rlogs, rsp, rmm = [], {}, {}, {}, {}, {}, {}
    cells_by_tag: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for tag, man in manifests.items():
        for dp in man.get("dpSiteMonths") or []:
            code = MANIFEST_DP_RE.sub("", dp.get("dp") or "")
            for sm in dp.get("siteMonths") or []:
                for m in sm.get("months") or []:
                    cells_by_tag[tag].add((code, sm.get("site"), m))
    for r in releases:
        tag = r["release"]
        man_art = next((a for a in r.get("artifacts") or [] if a.get("type") == "MANIFEST_AVAILABLE"), {})
        rel_rows.append({"release": tag, "uuid": r.get("uuid"), "generationDate": r.get("generationDate"),
                         "productCount": len(r.get("dataProducts") or []),
                         "siteCount": len(release_sites[tag]) if tag in release_sites else None,
                         "artifactCount": len(r.get("artifacts") or []), "manifestName": man_art.get("name"),
                         "manifestMd5": man_art.get("md5"), "manifestSize": man_art.get("size"),
                         "manifestCellCount": len(cells_by_tag[tag]) if tag in manifests else None})
        for a in r.get("artifacts") or []:
            if a.get("name"):
                arts[(tag, a["name"])] = {"release": tag, "name": a["name"], "type": a.get("type"),
                                          "url": strip_query(a.get("url")), "size": a.get("size"), "md5": a.get("md5")}
        for dp in r.get("dataProducts") or []:
            rprods[(tag, dp["productCode"])] = {
                "release": tag, "productCode": dp["productCode"], "productName": dp.get("productName"),
                "productDescription": dp.get("productDescription"), "productDoiUrl": dp.get("productDoi"),
                "productDoi": bare_doi(dp.get("productDoi"))}
    for tag, prods in release_products.items():
        for p in prods:
            mine = [x for x in p.get("releases") or [] if x.get("release") == tag]
            if not mine and not p.get("siteCodes"):
                continue  # not part of this release
            code = p["productCode"]
            doi = (mine[0].get("productDoi") if mine else None) or {}
            stats = _availability_stats(p.get("siteCodes") or [])
            records[(tag, code)] = {
                "release": tag, **_product_scalars(p), "themes": p.get("themes") or [],
                "keywords": p.get("keywords") or [], "generationDate": mine[0].get("generationDate") if mine else None,
                "productDoiUrl": doi.get("url"), "productDoi": bare_doi(doi.get("url")),
                "productDoiGenerationDate": doi.get("generationDate"), "siteCount": stats["siteCount"],
                "cellCount": stats["cellCount"], "firstMonth": stats["firstMonth"], "lastMonth": stats["lastMonth"],
                "specNumbers": [s["specNumber"] for s in p.get("specs") or [] if s.get("specNumber")],
                "changeLogCount": len(p.get("changeLogs") or [])}
            for cl in p.get("changeLogs") or []:
                if cl.get("id") is not None:
                    rlogs[(tag, code, cl["id"])] = {"release": tag, "productCode": code, **_change_log_row(cl)}
            for sc in p.get("siteCodes") or []:
                months = sorted(set(sc.get("availableMonths") or []))
                rsp.setdefault((tag, sc["siteCode"], code), {
                    "release": tag, "siteCode": sc["siteCode"], "productCode": code,
                    "productTitle": p.get("productName"), "availableMonths": months})
    for tag, sites in release_sites.items():
        for s in sites:
            for dp in s.get("dataProducts") or []:
                months = sorted(set(dp.get("availableMonths") or []))
                rsp[(tag, s["siteCode"], dp["dataProductCode"])] = {
                    "release": tag, "siteCode": s["siteCode"], "productCode": dp["dataProductCode"],
                    "productTitle": dp.get("dataProductTitle"), "availableMonths": months}
    for row in rsp.values():
        ms = row["availableMonths"]
        row.update({"monthCount": len(ms), "firstMonth": ms[0] if ms else None, "lastMonth": ms[-1] if ms else None})
    for tag, cells in cells_by_tag.items():
        for code, site, m in cells:
            rmm[(tag, code, site, m)] = {"release": tag, "productCode": code, "siteCode": site, "month": m}
    return {"releases": to_table("releases", rel_rows), "release_artifacts": to_table("release_artifacts", arts.values()),
            "release_products": to_table("release_products", rprods.values()),
            "release_product_records": to_table("release_product_records", records.values()),
            "release_product_change_logs": to_table("release_product_change_logs", rlogs.values()),
            "release_site_products": to_table("release_site_products", rsp.values()),
            "release_manifest_months": to_table("release_manifest_months", [rmm[k] for k in sorted(rmm)])}


# ---------------------------------------------------------------- sites and locations


def location_row(r: dict[str, Any]) -> dict[str, Any]:
    """Common named-location columns from a REST or GraphQL location record."""
    out = {c: r.get(c) for c, _t in _cols(LOCATION)}
    off = r.get("offsetLocation")
    out["offsetLocationName"] = off.get("locationName") if isinstance(off, dict) else off
    return out


def site_properties(props: list[dict[str, Any]] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in props or []:
        name = (p.get("locationPropertyName") or "").removeprefix("Value for ").strip()
        if name:
            out[name] = _str(p.get("locationPropertyValue"))
    return out


def flatten_sites(sites: list[dict[str, Any]], site_locations: list[dict[str, Any]]) -> dict[str, pa.Table]:
    """/sites + /locations/sites -> sites, site_releases, site_products, site_locations, domains."""
    by_site: dict[str, dict[str, Any]] = {}
    for loc in site_locations:
        if (loc.get("locationType") or "").upper() != "SITE":
            continue
        code = loc.get("siteCode") or loc.get("locationName")
        if code not in by_site or loc.get("locationName") == code:
            by_site[code] = loc
    rows, srel, sprod, doms = [], {}, {}, {}
    for s in sites:
        code = s["siteCode"]
        loc = by_site.get(code) or {}
        row = {k: s.get(k) for k in ("siteCode", "siteName", "siteDescription", "siteType", "siteLatitude",
                                     "siteLongitude", "stateCode", "stateName", "domainCode", "domainName", "deimsId")}
        row.update({"siteElevation": loc.get("locationElevation"), "siteUtmZone": loc.get("locationUtmZone"),
                    "siteUtmEasting": loc.get("locationUtmEasting"), "siteUtmNorthing": loc.get("locationUtmNorthing"),
                    "siteUtmHemisphere": loc.get("locationUtmHemisphere"),
                    "siteProperties": site_properties(loc.get("locationProperties")) if loc else None,
                    "productCount": len(s.get("dataProducts") or []), "releaseCount": len(s.get("releases") or [])})
        rows.append(row)
        for r in s.get("releases") or []:
            srel[(code, r["release"])] = {"siteCode": code, "release": r["release"],
                                          "generationDate": r.get("generationDate"), "url": r.get("url")}
        for dp in s.get("dataProducts") or []:
            months = sorted(set(dp.get("availableMonths") or []))
            rels = sorted({r.get("release") for r in dp.get("availableReleases") or [] if r.get("release")})
            prov = sum(len(r.get("availableMonths") or []) for r in dp.get("availableReleases") or []
                       if r.get("release") == PROVISIONAL)
            sprod[(code, dp["dataProductCode"])] = {
                "siteCode": code, "productCode": dp["dataProductCode"], "productTitle": dp.get("dataProductTitle"),
                "monthCount": len(months), "provisionalMonthCount": prov, "firstMonth": months[0] if months else None,
                "lastMonth": months[-1] if months else None, "availableMonths": months, "releases": rels}
        d = doms.setdefault(s.get("domainCode"), {"domainCode": s.get("domainCode"),
                                                  "domainName": s.get("domainName"), "siteCodes": []})
        d["siteCodes"].append(code)
    for d in doms.values():
        d["siteCodes"] = sorted(d["siteCodes"])
        d["siteCount"] = len(d["siteCodes"])
    locs = {}
    for loc in site_locations:
        locs[loc["locationName"]] = {**location_row(loc), "locationChildrenCount": len(loc.get("locationChildren") or [])}
    return {"sites": to_table("sites", rows), "site_releases": to_table("site_releases", srel.values()),
            "site_products": to_table("site_products", sprod.values()),
            "site_locations": to_table("site_locations", locs.values()),
            "domains": to_table("domains", [doms[k] for k in sorted(doms, key=str) if k])}


def polygon_wkt(coords: list[dict[str, Any]]) -> str | None:
    pts = [(c.get("longitude"), c.get("latitude")) for c in coords or []
           if c.get("longitude") is not None and c.get("latitude") is not None]
    if len(pts) < 3:
        return None
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    return "POLYGON((" + ", ".join(f"{x!r} {y!r}" for x, y in pts) + "))"


def flatten_locations(batches: list[dict[str, Any]], nodes: dict[str, dict[str, Any]]) -> dict[str, pa.Table]:
    """locations/batch_* ({names, rows, errors}) joined with stage/location_nodes -> locations, location_polygons."""
    rows: dict[str, dict[str, Any]] = {}
    polys: dict[str, dict[str, Any]] = {}
    for b in batches:
        for r in b.get("rows") or []:
            name = r.get("locationName")
            if not name or name in rows:
                continue
            node = nodes.get(name) or {}
            poly = r.get("locationPolygon")
            coords = (poly or {}).get("coordinates") or []
            rows[name] = {**location_row(r), "locationType": r.get("locationType") or node.get("locationType"),
                          "locationPolygon": poly, "hasPolygon": bool(coords),
                          "hierarchyDepth": node.get("hierarchyDepth"), "hierarchyParent": node.get("hierarchyParent"),
                          "hierarchySite": node.get("hierarchySite")}
            if coords:
                polys[name] = {"locationName": name, "locationType": r.get("locationType"),
                               "siteCode": r.get("siteCode"), "domainCode": r.get("domainCode"),
                               "vertexCount": len(coords), "wkt": polygon_wkt(coords)}
    return {"locations": to_table("locations", (rows[k] for k in sorted(rows))),
            "location_polygons": to_table("location_polygons", (polys[k] for k in sorted(polys)))}


def flatten_location_history(payloads: list[dict[str, Any]]) -> pa.Table:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for p in payloads:
        name = p.get("locationName")
        if not name:
            continue
        for i, h in enumerate(p.get("locationHistory") or []):
            off = h.get("offsetLocation")
            out[(name, i)] = {**{c: h.get(c) for c in TABLES["location_history"].names}, "locationName": name,
                              "historyIndex": i, "isCurrent": h.get("current"),
                              "offsetLocationName": off.get("locationName") if isinstance(off, dict) else off}
    return to_table("location_history", (out[k] for k in sorted(out)))


# ---------------------------------------------------------------- availability


def current_cells(products: list[dict[str, Any]]) -> dict[tuple[str, str, str], tuple[str | None, str | None]]:
    """(productCode, siteCode, month) -> (release incl. PROVISIONAL, availableDataUrl) from /products."""
    out: dict[tuple[str, str, str], tuple[str | None, str | None]] = {}
    for p in products:
        code = p["productCode"]
        for sc in p.get("siteCodes") or []:
            site = sc["siteCode"]
            rel_of = {m: r.get("release") for r in sc.get("availableReleases") or []
                      for m in r.get("availableMonths") or []}
            urls = {month_of_url(u): u for u in sc.get("availableDataUrls") or []}
            for m in sc.get("availableMonths") or []:
                out[(code, site, m)] = (rel_of.get(m), urls.get(m))
    return out


def release_only_cells(products: list[dict[str, Any]], manifests: dict[str, dict[str, Any]]
                       ) -> dict[tuple[str, str, str], str]:
    """Cells in any manifest but not in current availability -> newest release containing them."""
    cur = {(c, s, m) for (c, s, m) in current_cells(products)}
    return {k: tags[-1] for k, tags in manifest_cells(manifests).items() if k not in cur}


def flatten_availability(products: list[dict[str, Any]], manifests: dict[str, dict[str, Any]]) -> pa.Table:
    rows = []
    for (code, site, m), (rel, url) in current_cells(products).items():
        rows.append({"productCode": code, "siteCode": site, "month": m, "release": rel,
                     "isProvisional": rel == PROVISIONAL, "isReleaseOnly": False, "availableDataUrl": url})
    for (code, site, m), rel in release_only_cells(products, manifests).items():
        rows.append({"productCode": code, "siteCode": site, "month": m, "release": rel, "isProvisional": False,
                     "isReleaseOnly": True, "availableDataUrl": None})
    rows.sort(key=lambda r: (r["productCode"], r["siteCode"], r["month"]))
    return to_table("availability_months", rows)


def flatten_product_site(products: list[dict[str, Any]], manifests: dict[str, dict[str, Any]],
                         pairs: list[dict[str, Any]], external: list[dict[str, Any]],
                         pair_files: dict[tuple[str, str], tuple[int, int]]) -> pa.Table:
    """Product x site pairs with inventory scope and status.

    ``pairs`` are data_query_pairs rows; ``pair_files`` maps (product, site) -> (files, bytes) after dedup.
    """
    team = {p["productCode"]: p.get("productScienceTeamAbbr") for p in products}
    cur: dict[tuple[str, str], list[str]] = defaultdict(list)
    prov: dict[tuple[str, str], int] = defaultdict(int)
    for (code, site, m), (rel, _url) in current_cells(products).items():
        cur[(code, site)].append(m)
        prov[(code, site)] += rel == PROVISIONAL
    ro: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for (code, site, m), rel in release_only_cells(products, manifests).items():
        ro[(code, site)][m] = rel
    status_of: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in pairs:
        status_of[(r["productCode"], r["siteCode"])].append(r["status"])
    ext = defaultdict(int)
    for e in external:
        ext[(e["productCode"], e["siteCode"])] += 1
    rows = []
    for key in sorted(set(cur) | set(ro)):
        code, site = key
        aop = is_aop(code, team.get(code))
        months = sorted(set(cur.get(key, [])) | set(ro.get(key, {})))
        n_files, n_bytes = pair_files.get(key, (0, 0))
        if aop:
            scope, status = "excluded:AOP", None
        else:
            scope = "current" if cur.get(key) else "release-only"
            st = status_of.get(key, [])
            if "error" in st:
                status = "error"
            elif not st or "missing" in st:
                status = "missing"
            elif n_files > 0:
                status = "ok"
            elif ext.get(key):
                status = "external"
            else:
                status = "empty"
        rows.append({"productCode": code, "siteCode": site, "isAop": aop, "inventoryScope": scope,
                     "inventoryStatus": status, "currentMonthCount": len(cur.get(key, [])),
                     "provisionalMonthCount": prov.get(key, 0), "releaseOnlyMonthCount": len(ro.get(key, {})),
                     "firstMonth": months[0] if months else None, "lastMonth": months[-1] if months else None,
                     "releaseOnlyReleases": sorted(set(ro.get(key, {}).values())),
                     "fileCount": None if aop else n_files, "totalBytes": None if aop else n_bytes,
                     "externalLinkCount": ext.get(key, 0)})
    return to_table("product_site", rows)


# ---------------------------------------------------------------- other API


def strip_taxon_prefixes(rec: dict[str, Any]) -> dict[str, Any]:
    return {re.sub(r"^(dwc|gbif):", "", k): v for k, v in rec.items()}


def flatten_taxonomy(pages: list[dict[str, Any]]) -> pa.Table:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for page in pages:
        for rec in page.get("data") or []:
            r = strip_taxon_prefixes(rec)
            key = (r.get("taxonTypeCode"), r.get("taxonID"))
            out[key] = {**r, "record": r}
    return to_table("taxonomy", (out[k] for k in sorted(out, key=lambda k: (str(k[0]), str(k[1])))))


def flatten_sample_classes(payload: dict[str, Any] | list[Any]) -> pa.Table:
    entries = payload.get("entries") if isinstance(payload, dict) else payload
    rows = {e.get("key"): {"sampleClass": e.get("key"), "description": e.get("value")}
            for e in entries or [] if e.get("key")}
    return to_table("sample_classes", (rows[k] for k in sorted(rows)))


def flatten_prototypes(datasets: list[dict[str, Any]], data_payloads: list[dict[str, Any]]) -> dict[str, pa.Table]:
    ds_rows, files, locs = [], {}, {}
    by_uuid = {d.get("datasetUuid"): d for d in data_payloads if d.get("datasetUuid")}
    for d in datasets:
        uuid = d.get("uuid")
        doi = d.get("doi") or {}
        data = d.get("data") or {}
        payload = by_uuid.get(uuid) or data
        pfiles = payload.get("files") or []
        ds_rows.append({**{c: d.get(c) for c in TABLES["prototype_datasets"].names}, "uuid": uuid,
                        "doiUrl": doi.get("url"), "doi": bare_doi(doi.get("url")),
                        "doiGenerationDate": doi.get("generationDate"), "dataUrl": data.get("url"),
                        "fileCount": len(pfiles),
                        "fileTypes": [ft.get("name") for ft in d.get("fileTypes") or [] if ft.get("name")],
                        "relatedDataProducts": [r.get("dataProductCode") for r in d.get("relatedDataProducts") or []
                                                if r.get("dataProductCode")],
                        "dataLocations": payload.get("dataLocations", data.get("dataLocations"))})
        for f in pfiles:
            fname = f.get("fileName") or f.get("name")
            typ = f.get("type") or {}
            files[(uuid, fname)] = {"datasetUuid": uuid, "fileName": fname, "name": f.get("name"),
                                    "description": f.get("description"), "size": f.get("size"), "md5": f.get("md5"),
                                    "typeName": typ.get("name"), "typeDescription": typ.get("description"),
                                    "url": strip_query(f.get("url"))}
        for loc in d.get("locations") or []:
            site = loc.get("siteCode") or loc.get("siteName")
            if site:
                locs[(uuid, site)] = {"datasetUuid": uuid, **loc, "siteCode": site}
    return {"prototype_datasets": to_table("prototype_datasets", ds_rows),
            "prototype_files": to_table("prototype_files", files.values()),
            "prototype_locations": to_table("prototype_locations", locs.values())}


def gql_type_str(t: dict[str, Any] | None) -> str | None:
    if not t:
        return None
    kind = t.get("kind")
    if kind == "NON_NULL":
        return f"{gql_type_str(t.get('ofType'))}!"
    if kind == "LIST":
        return f"[{gql_type_str(t.get('ofType'))}]"
    return t.get("name")


def flatten_graphql(payload: dict[str, Any]) -> dict[str, pa.Table]:
    schema = ((payload or {}).get("data") or {}).get("__schema") or {}
    roots = {(schema.get(k) or {}).get("name"): k.removesuffix("Type")
             for k in ("queryType", "mutationType", "subscriptionType") if schema.get(k)}
    types, fields = [], {}
    for t in schema.get("types") or []:
        name = t.get("name")
        types.append({"name": name, "kind": t.get("kind"), "description": t.get("description"),
                      "fieldCount": len(t.get("fields") or []), "inputFieldCount": len(t.get("inputFields") or []),
                      "enumValueCount": len(t.get("enumValues") or []),
                      "interfaces": [gql_type_str(i) for i in t.get("interfaces") or []],
                      "possibleTypes": [gql_type_str(i) for i in t.get("possibleTypes") or []],
                      "rootOperation": roots.get(name)})
        for kind, items in (("field", t.get("fields")), ("inputField", t.get("inputFields")),
                            ("enumValue", t.get("enumValues"))):
            for f in items or []:
                fields[(name, kind, f.get("name"))] = {
                    "typeName": name, "memberKind": kind, "name": f.get("name"), "description": f.get("description"),
                    "type": gql_type_str(f.get("type")),
                    "args": [{"name": a.get("name"), "type": gql_type_str(a.get("type")),
                              "defaultValue": a.get("defaultValue"), "description": a.get("description")}
                             for a in f.get("args") or []] if kind == "field" else None,
                    "defaultValue": f.get("defaultValue"), "isDeprecated": f.get("isDeprecated"),
                    "deprecationReason": f.get("deprecationReason")}
    return {"graphql_types": to_table("graphql_types", sorted(types, key=lambda r: str(r["name"]))),
            "graphql_fields": to_table("graphql_fields", (fields[k] for k in sorted(fields, key=str)))}


def flatten_external(lines: list[dict[str, Any]]) -> pa.Table:
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for line in lines:
        p = line.get("payload") or {}
        m = DQ_EXT_RE.match(line.get("key") or "")
        code = p.get("productCode") or (m and m["product"])
        site = p.get("siteCode") or (m and m["site"])
        month = p.get("month") or (m and m["month"])
        for e in p.get("externalData") or []:
            url = strip_query(e.get("url"))
            if url:
                rows[(code, site, url)] = {"productCode": code, "siteCode": site, "monthProbed": month,
                                           "name": e.get("name"), "type": e.get("type"), "url": url}
    return to_table("external_data", (rows[k] for k in sorted(rows)))


DQ_EXT_RE = re.compile(r"^(?P<product>DP\d\.\d{5}\.\d{3})__(?P<site>[A-Z0-9]+)__(?P<month>\d{4}-\d{2})$")


# ---------------------------------------------------------------- file inventory (data_query)


@dataclass(frozen=True)
class LabFile:
    """An external-lab file name (no domain/site/product/month by design)."""

    kind: str
    table: str
    timestamp: str
    ext: str
    lab: str
    hor: None = None
    ver: None = None
    tmi: None = None
    product_code: None = None
    site: None = None
    domain: None = None
    month: None = None


def parse_file_name(name: str) -> Any:
    """``filenames.classify`` (``parse_neon_filename`` plus the lab-file grammar, kind ``lab``).

    Lab files come back as :class:`LabFile` (``.lab``, no product/site/month), everything else as the
    classifier's result; no product hint is passed, so DP0 support files keep their parsed code.
    """
    parsed = classify(name)
    if parsed.kind == "lab":
        return LabFile("lab", parsed.table, parsed.timestamp, parsed.ext, parsed.lab_name)
    return parsed


def parse_consistent(parsed: Any, product: str, site: str, domain: str | None, month: str) -> bool:
    """The file name agrees with the package it was listed in (DP0 support files match their DP1 product;
    lab files carry nothing that could disagree)."""
    if parsed.kind == "lab":
        return True
    if parsed.kind == "other" or not parsed.product_code:
        return False
    return (parsed.site == site and (domain is None or parsed.domain == domain)
            and parsed.product_code.split(".", 1)[1] == product.split(".", 1)[1]
            and (parsed.month is None or parsed.month == month))


@dataclass
class DataQueryResult:
    """Flattened data_query landings of one product."""

    files: pa.Table
    packages: pa.Table
    landings: list[dict[str, Any]]
    pair_counts: dict[str, dict[str, int]] = field(default_factory=dict)  # "<site>|<requestRelease>" -> counts


def flatten_data_query(lines: Iterable[dict[str, Any]], product: str | None = None) -> DataQueryResult:
    """Sealed data_query lines of one product -> files + site_month_packages + per-landing summaries.

    Duplicate (product, site, month, release, name) rows from overlapping windows keep the most recently
    fetched landing.
    """
    ordered = sorted(lines, key=lambda ln: (ln.get("fetchedAt") or "", ln.get("key") or ""))
    files: dict[tuple[str, str, str, str], tuple] = {}
    pkgs: dict[tuple[str, str, str], dict[str, Any]] = {}
    landings = []
    for line in ordered:
        p = line.get("payload") or {}
        req = p.get("_request") or {}
        pair = req.get("pair") or {}
        m = DQ_KEY_RE.match(line.get("key") or "")
        code = pair.get("product") or p.get("productCode") or (m and m["product"]) or product
        req_rel = pair.get("release") or (m and m["release"]) or None
        scope = pair.get("scope") or ("release-only" if req_rel else "current")
        release_only = scope == "release-only"
        window = req.get("window") or ([m["start"], m["end"]] if m else [None, None])
        n = b = 0
        months = set()
        for r in p.get("releases") or []:
            rel = r.get("release")
            rel_gen = tstz(r.get("generationDate"))
            for pk in r.get("packages") or []:
                site, month, dom = pk.get("siteCode"), pk.get("month"), pk.get("domainCode")
                pgen = tstz(pk.get("generationDate"))
                months.add(month)
                pkg_files = pk.get("files") or []
                pkgs[(site, month, rel)] = {
                    "productCode": code, "siteCode": site, "month": month, "release": rel, "domainCode": dom,
                    "packageType": pk.get("packageType"), "generationDate": pgen, "releaseGenerationDate": rel_gen,
                    "fileCount": len(pkg_files), "totalBytes": sum(int(f.get("size") or 0) for f in pkg_files),
                    "isProvisional": rel == PROVISIONAL, "isReleaseOnly": release_only,
                    "_reqRel": req_rel or CURRENT}
                for f in pkg_files:
                    name = f.get("name")
                    if not name:
                        continue
                    pf = parse_file_name(name)
                    size = _int(f.get("size"))
                    n += 1
                    b += size or 0
                    files[(site, month, rel, name)] = (
                        code, site, dom, month, rel, rel_gen, pk.get("packageType"), pgen, name, size, f.get("md5"),
                        f.get("crc32c"), strip_query(f.get("url")), pf.kind, pf.table, pf.hor, pf.ver, pf.tmi,
                        neon_ts(pf.timestamp), pf.ext, rel == PROVISIONAL, release_only,
                        parse_consistent(pf, code, site, dom, month), req_rel or CURRENT)
        landings.append({"key": line.get("key"), "productCode": code, "siteCode": pair.get("site") or (m and m["site"]),
                         "requestRelease": req_rel or CURRENT, "scope": scope, "window": window,
                         "fetchedAt": line.get("fetchedAt"), "fileCount": n, "totalBytes": b,
                         "months": sorted(x for x in months if x)})
    spec = TABLES["files"]
    ordered_keys = sorted(files)
    cols = list(zip(*(files[k] for k in ordered_keys), strict=True)) if ordered_keys else [()] * (len(spec.columns) + 1)
    arrays = [pa.array(list(cols[i]), type=ARROW[t]) for i, (_c, t) in enumerate(spec.columns)]
    files_tbl = pa.Table.from_arrays(arrays, schema=spec.arrow_schema())
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"files": 0, "bytes": 0, "months": 0})
    for k in ordered_keys:
        row = files[k]
        c = counts[f"{row[1]}|{row[-1]}"]
        c["files"] += 1
        c["bytes"] += row[9] or 0
    for (site, _m, _rel), pk in pkgs.items():
        counts[f"{site}|{pk['_reqRel']}"]["months"] += 1
    pk_tbl = to_table("site_month_packages", (pkgs[k] for k in sorted(pkgs, key=lambda k: tuple(map(str, k)))))
    return DataQueryResult(files=files_tbl, packages=pk_tbl, landings=landings, pair_counts=dict(counts))


def iter_sealed(path: Path) -> Iterator[dict[str, Any]]:
    """Lines of a sealed jsonl.gz file."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def flatten_data_query_file(sealed_path: str, stage_dir: str) -> dict[str, Any]:
    """Worker: one sealed/data_query/<DP>.jsonl.gz -> stage/files/<DP>.parquet + stage/packages/<DP>.parquet.

    Cached: skipped when stage/files/<DP>.meta.json records the same input sha256 and FLATTEN_VERSION.
    Returns the meta dict (landings, pair counts, row counts, parquet paths).
    """
    src = Path(sealed_path)
    product = src.name.removesuffix(".jsonl.gz")
    stage = Path(stage_dir)
    fpath, ppath = stage / "files" / f"{product}.parquet", stage / "packages" / f"{product}.parquet"
    mpath = stage / "files" / f"{product}.meta.json"
    digest = sha256_file(src)
    if mpath.exists() and fpath.exists() and ppath.exists():
        meta = json.loads(mpath.read_text())
        if meta.get("inputSha256") == digest and meta.get("flattenVersion") == FLATTEN_VERSION:
            meta.update({"cached": True, "files": str(fpath), "packages": str(ppath)})
            return meta
    res = flatten_data_query(iter_sealed(src), product)
    for path, tbl in ((fpath, res.files), (ppath, res.packages)):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".part")
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, path)
    meta = {"product": product, "inputSha256": digest, "flattenVersion": FLATTEN_VERSION,
            "files": str(fpath), "packages": str(ppath), "fileRows": res.files.num_rows,
            "packageRows": res.packages.num_rows, "landings": res.landings, "pairCounts": res.pair_counts}
    tmp = mpath.with_name(mpath.name + ".part")
    tmp.write_text(json.dumps(meta, default=str))
    os.replace(tmp, mpath)
    meta["cached"] = False
    return meta


def flatten_data_query_pairs(plan: list[dict[str, Any]], landings: list[dict[str, Any]],
                             pair_counts: dict[tuple[str, str, str], dict[str, int]],
                             ledger: list[dict[str, Any]]) -> pa.Table:
    """Planned pairs (stage/pairs) + landings + ledger (data_query rows) -> data_query_pairs."""
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}

    def row(code: str, site: str, rr: str) -> dict[str, Any]:
        return rows.setdefault((code, site, rr), {
            "productCode": code, "siteCode": site, "requestRelease": rr,
            "scope": "current" if rr == CURRENT else "release-only", "startMonth": None, "endMonth": None,
            "plannedMonthCount": None, "landingCount": 0, "splitCount": 0, "errorCount": 0, "status": None,
            "fileCount": 0, "totalBytes": 0, "packageMonthCount": 0, "lastFetchedAt": None, "error": None})

    for p in plan:
        r = row(p["product"], p["site"], p.get("release") or CURRENT)
        r.update({"scope": p.get("scope") or r["scope"], "startMonth": p.get("start"), "endMonth": p.get("end"),
                  "plannedMonthCount": len(p.get("months") or [])})
    for ld in landings:
        r = row(ld["productCode"], ld["siteCode"], ld["requestRelease"])
        r["landingCount"] += 1
        r["scope"] = ld.get("scope") or r["scope"]
        w = ld.get("window") or [None, None]
        if r["startMonth"] is None or (w[0] and w[0] < r["startMonth"]):
            r["startMonth"] = w[0] or r["startMonth"]
        if r["endMonth"] is None or (w[1] and w[1] > r["endMonth"]):
            r["endMonth"] = w[1] or r["endMonth"]
        if ld.get("fetchedAt") and (r["lastFetchedAt"] is None or ld["fetchedAt"] > r["lastFetchedAt"]):
            r["lastFetchedAt"] = ld["fetchedAt"]
    for led in ledger:
        if led.get("family") != "data_query":
            continue
        m = DQ_KEY_RE.match(led.get("key") or "")
        if not m:
            continue
        r = row(m["product"], m["site"], m["release"] or CURRENT)
        if led.get("status") == "split":
            r["splitCount"] += 1
        elif led.get("status") == "error":
            r["errorCount"] += 1
            r["error"] = r["error"] or (led.get("error") or "")[:500]
    for key, r in rows.items():
        c = pair_counts.get(key) or {}
        r.update({"fileCount": c.get("files", 0), "totalBytes": c.get("bytes", 0),
                  "packageMonthCount": c.get("months", 0)})
        r["status"] = "error" if r["errorCount"] else ("ok" if r["landingCount"] else "missing")
    return to_table("data_query_pairs", (rows[k] for k in sorted(rows)))


# ---------------------------------------------------------------- meta


def scrub_text(value: Any) -> Any:
    """Drop URL query strings from any text that carries a signed-URL marker."""
    if isinstance(value, str) and SIGNED_MARK in value:
        return re.sub(r"(https?://[^\s?'\"]+)\?[^\s'\"]*", r"\1", value)
    return value


def flatten_requests(ledger: list[dict[str, Any]]) -> pa.Table:
    rows = []
    for r in ledger:
        r = {k: scrub_text(v) for k, v in r.items()}
        try:
            params = json.loads(r["params"]) if r.get("params") is not None else None
        except (TypeError, ValueError):
            params = r.get("params")
        rows.append({"runId": r.get("run_id"), "family": r.get("family"), "key": r.get("key"),
                     "method": r.get("method"), "path": r.get("path"), "params": params,
                     "httpStatus": r.get("http_status"), "attempts": r.get("attempts"),
                     "elapsedMs": r.get("elapsed_ms"), "responseBytes": r.get("response_bytes"),
                     "landedPath": r.get("landed_path"), "landedBytes": r.get("landed_bytes"),
                     "sha256": r.get("sha256"), "status": r.get("status"), "error": r.get("error"),
                     "fetchedAt": r.get("fetched_at")})
    return to_table("harvest_requests", rows)


def flatten_harvest_run(run_id: str, fetch_manifest: dict[str, Any] | None, sealed_md5: str | None) -> pa.Table:
    fm = fetch_manifest or {}
    return to_table("harvest_runs", [{
        "runId": run_id, "status": fm.get("status"), "stages": fm.get("stages"), "pilot": fm.get("pilot"),
        "startedAt": fm.get("startedAt"), "finishedAt": fm.get("finishedAt"), "seconds": fm.get("seconds"),
        "pairs": fm.get("pairs"), "failureCount": fm.get("failureCount"), "versions": fm.get("versions"),
        "tools": fm.get("tools"), "tunables": fm.get("tunables"), "counts": fm.get("counts"),
        "transforms": fm.get("transforms"), "sealedManifestMd5": sealed_md5}])


# ---------------------------------------------------------------- orchestration


@dataclass
class Flattened:
    """Everything a catalog build stages: in-memory Arrow tables + per-product Parquet files."""

    run_id: str
    tables: dict[str, pa.Table]
    parquet: dict[str, list[Path]]
    stats: dict[str, Any]


def _payloads(sealed: Path, family: str) -> list[dict[str, Any]]:
    path = sealed / f"{family}.jsonl.gz"
    return list(iter_sealed(path)) if path.exists() else []


def _one(sealed: Path, family: str) -> Any:
    lines = _payloads(sealed, family)
    return lines[0]["payload"] if lines else None


def _stage_json(sealed: Path, name: str) -> Any:
    path = sealed / "stage" / f"{name}.json.gz"
    return json.loads(gzip.decompress(path.read_bytes())) if path.exists() else None


def _as_list(payload: Any, key: str = "data") -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return payload.get(key) or []


def flatten_run(run_dir: Path, ledger: list[dict[str, Any]], *, workers: int = 12,
                fetch_manifest: dict[str, Any] | None = None) -> Flattened:
    """Flatten a sealed run (``run_dir/sealed``) into every catalog table."""
    import time

    t0 = time.time()
    sealed = run_dir / "sealed"
    if not (sealed / "MANIFEST.json").exists():
        raise SystemExit(f"{sealed}/MANIFEST.json missing: run `neon-ducklake catalog seal` first")
    sealed_md5 = hashlib.md5((sealed / "MANIFEST.json").read_bytes(), usedforsecurity=False).hexdigest()
    tables: dict[str, pa.Table] = {}
    products = _as_list(_one(sealed, "products"))
    manifests = {ln["key"]: ln["payload"] for ln in _payloads(sealed, "manifests")}
    releases = _as_list(_one(sealed, "releases"), "releases")
    tables.update(flatten_products(products))
    tables["documents"] = flatten_documents(products, [ln["payload"] for ln in _payloads(sealed, "documents_head")])
    tables.update(flatten_releases(
        releases, manifests, {ln["key"]: _as_list(ln["payload"]) for ln in _payloads(sealed, "release_products")},
        {ln["key"]: _as_list(ln["payload"]) for ln in _payloads(sealed, "release_sites")}))
    tables.update(flatten_sites(_as_list(_one(sealed, "sites")), _as_list(_one(sealed, "locations_sites"))))
    nodes = _stage_json(sealed, "location_nodes") or {}
    tables.update(flatten_locations([ln["payload"] for ln in _payloads(sealed, "locations")], nodes))
    tables["location_history"] = flatten_location_history([ln["payload"] for ln in _payloads(sealed, "location_history")])
    tables["availability_months"] = flatten_availability(products, manifests)
    tables["taxonomy"] = flatten_taxonomy([ln["payload"] for ln in _payloads(sealed, "taxonomy")])
    tables["sample_classes"] = flatten_sample_classes(_one(sealed, "samples") or {})
    tables.update(flatten_prototypes(_as_list(_one(sealed, "prototype")),
                                     [ln["payload"] for ln in _payloads(sealed, "prototype_data")]))
    tables.update(flatten_graphql(_one(sealed, "graphql") or {}))
    tables["external_data"] = flatten_external(_payloads(sealed, "external"))
    t_meta = time.time() - t0

    # file inventory, one worker per product
    dq_files = sorted((sealed / "data_query").glob("*.jsonl.gz")) if (sealed / "data_query").exists() else []
    stage_dir = run_dir / "stage"
    metas: list[dict[str, Any]] = []
    if dq_files:
        if workers > 1 and len(dq_files) > 1:
            with ProcessPoolExecutor(max_workers=min(workers, len(dq_files))) as ex:
                metas = list(ex.map(flatten_data_query_file, [str(f) for f in dq_files],
                                    [str(stage_dir)] * len(dq_files)))
        else:
            metas = [flatten_data_query_file(str(f), str(stage_dir)) for f in dq_files]
    landings = [ld for m in metas for ld in m["landings"]]
    pair_counts: dict[tuple[str, str, str], dict[str, int]] = {}
    for m in metas:
        for k, c in m["pairCounts"].items():
            site, rr = k.split("|", 1)
            pair_counts[(m["product"], site, rr)] = c
    plan = _stage_json(sealed, "pairs") or []
    tables["data_query_pairs"] = flatten_data_query_pairs(plan, landings, pair_counts, ledger)
    pair_files: dict[tuple[str, str], tuple[int, int]] = defaultdict(lambda: (0, 0))
    for (code, site, _rr), c in pair_counts.items():
        n, b = pair_files[(code, site)]
        pair_files[(code, site)] = (n + c["files"], b + c["bytes"])
    tables["product_site"] = flatten_product_site(
        products, manifests, tables["data_query_pairs"].to_pylist(), tables["external_data"].to_pylist(),
        dict(pair_files))
    tables["harvest_requests"] = flatten_requests(ledger)
    tables["harvest_runs"] = flatten_harvest_run(run_dir.name, fetch_manifest, sealed_md5)
    stats = {"seconds": round(time.time() - t0, 1), "metaSeconds": round(t_meta, 1), "products": len(dq_files),
             "cachedProducts": sum(1 for m in metas if m.get("cached")),
             "fileRows": sum(m["fileRows"] for m in metas), "packageRows": sum(m["packageRows"] for m in metas),
             "landings": len(landings), "sealedManifestMd5": sealed_md5}
    parquet = {"files": [Path(m["files"]) for m in metas], "site_month_packages": [Path(m["packages"]) for m in metas]}
    log.info("flattened %s in %.1fs (metadata %.1fs; %d products, %d cached, %d file rows)", run_dir.name,
             stats["seconds"], t_meta, len(dq_files), stats["cachedProducts"], stats["fileRows"])
    return Flattened(run_id=run_dir.name, tables=tables, parquet=parquet, stats=stats)
