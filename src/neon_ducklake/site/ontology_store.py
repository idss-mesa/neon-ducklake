"""The shared ontology store ``work/ontology_cache.duckdb`` and cached OLS / OBO Foundry lookups.

Owner: site/terms.py. Also imported by site/curate.py, site/validate.py and site/avus.py.
The tables are those in docs/CONTRACTS.md, "Ontology and curation stores".

Public API (keep it small; everything else in this module is private):

``open_store(read_only=False, *, wait_s=120) -> duckdb.DuckDBPyConnection``
    Opens the store and creates any missing contract tables. DuckDB allows one writer
    *process*, so keep connections short and close them. A lock conflict is retried for
    ``wait_s`` seconds. ``store(read_only=False)`` is the same thing as a context manager
    that closes the connection.

``ols_term(curie, *, con=None, refresh=False) -> dict | None``
    Returns ``{curie, iri, label, ontologyId, isObsolete, oboFoundry}``.
    - Source: the raw OLS v1 term API (``/ontologies/<prefix>/terms/<double-encoded IRI>``).
    - ``isObsolete`` is OLS ``is_obsolete``, or a label that starts with "obsolete".
    - Cached in ``ols_term_cache``.
    - ``None`` means OLS has no such term (a 404). That miss is remembered for this process only.
    - Raises ``OLSUnavailable`` when OLS cannot be reached.

``ols_terms(curies, *, con=None) -> dict[str, dict | None]``
    The batch form, with at most 4 concurrent requests. A CURIE whose fetch failed is left out
    of the result, so callers should mark it 'pending'.

``search_ols(query, ontology_id, size=25, *, within=None, con=None, refresh=False) -> list[dict]``
    Returns ``[{curie, label, iri, ontologyId}]``, deduplicated by CURIE and kept in OLS rank order.
    - Uses mesa-mcp's ``OLSClient.search_terms``.
    - With ``within`` (a parent IRI or CURIE) it uses ``search_term_descendants`` instead, i.e.
      the OLS ``allChildrenOf`` filter.
    - Cached in ``ols_search_cache``. Raises ``OLSUnavailable``.

``search_many(queries, ontology_id, size=25, *, within=None, con=None) -> dict[str, list[dict]]``
    The batch form, with at most 4 concurrent requests. A query whose search failed is left out.

``exact_matches(results, label, prefix=None) -> list[dict]``
    Search hits whose label equals ``label``, ignoring case and surrounding whitespace.
    ``prefix`` optionally keeps only one CURIE prefix.

``search_exact_many(queries, ontology_id, size=25, *, fields=("label", "synonym"), within=None, con=None)``
    OLS v1 ``/search`` with ``exact=true`` over ``fields`` (label and/or synonym), optionally restricted
    to the descendants of ``within``. Returns ``{query: [{curie, label, iri, ontologyId, synonyms}]}``;
    ``synonyms`` lists every synonym OLS indexes for the term (exact and related). Cached in
    ``ols_search_cache`` under ``within = 'exact:<fields>[|<parent IRI>]'``. A query whose search failed is
    left out. ``search_exact(query, …)`` is the single form and raises ``OLSUnavailable``.

``synonym_matches(results, name, prefix=None) -> list[dict]``
    Hits (from ``search_exact_many``) that carry ``name`` among their synonyms (case-insensitive).

``term_synonyms(curies, *, con=None) -> dict[str, list[dict]]``
    ``{curie: [{name, scope, type}]}`` from the OLS v1 term record (``obo_synonym``: scope such as
    ``hasExactSynonym`` / ``hasRelatedSynonym`` and the OBO synonym type such as ``synonym``,
    ``scientific name``, ``in-part``). Cached in ``ols_term_synonyms``; a CURIE whose fetch failed is left out.

``obo_foundry_prefixes(refresh=False) -> set[str]``
    Lower-case ids of the active, non-obsolete OBO Foundry ontologies. They are read from
    ``ref/obofoundry_ontologies.jsonld``, which is downloaded once and then reused.

``export_parquet(con=None) -> dict[str, int]``
    Writes every store table except the search cache to ``ref/ontology_maps/<table>.parquet``,
    ordered by key. A file is replaced only when its bytes change.
    ``export_parquet(vm_scoped=True) -> Path`` writes the same files to this VM's own export directory
    ``work/ontology_export/vm/<vm>/`` and returns it: several VMs publish their exports side by side to
    ``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/`` (``bootstrap.publish_ontology``) instead of overwriting one
    shared copy, and ``bootstrap`` merges all of them (:func:`merge_parquet`).

``merge_parquet(con, table, path) -> dict[str, int]``
    Merges an exported Parquet file into the store: rows with a new key are inserted; a row whose key exists
    replaces the local row only when its ``status`` ranks higher (:data:`STATUS_RANK`); otherwise local rows
    win.

``norm_curie(value) -> str | None``, ``iri_for(curie) -> str``
    CURIE helpers, the same as neon-avu-eval ``validate_load.py``.

Beyond the contract, the store has these tables:
- ``ols_search_cache`` (ontologyId, query, size, within, results JSON, fetchedAt)
- ``ols_term_synonyms`` (curie PK, synonyms JSON, fetchedAt): see ``term_synonyms``
- ``terms_taxon_types``: the taxonTypeCode chosen per (site, product, table) by site/terms.py
- ``ref_site_envo_feature`` (siteCode PK, envoCurie, envoLabel, habitat, method, status): the ENVO
  environmental feature (a water body: stream, river, lake) of an aquatic site. It mirrors
  ``ref_site_envo`` (the biome) and follows the same method/status rules: ``heuristic``/``proposed``
  from ``site terms``, ``curated``/``curated`` after two agreeing Opus replicates (``site validate``).
- ``site_envo_proposals`` (siteCode, role, source, envoCurie, envoLabel, rule, detail, createdAt;
  PK siteCode, role, source): every biome/feature proposal of a site by source (``heuristic``, ``rep1``,
  ``rep2``, ``human``), so the heuristic survives curation and can be compared with it.

Network use is polite: at most 4 concurrent requests, retries with backoff, and everything is
cached. No secrets are involved.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.parse
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import duckdb

from .. import env

log = logging.getLogger("neon_ducklake.site.ontology_store")

OLS_API = "https://www.ebi.ac.uk/ols4/api"
OBO_REGISTRY_URL = "https://obofoundry.org/registry/ontologies.jsonld"
MAX_CONCURRENCY = 4
RETRIES = 3
USER_AGENT = "neon-ducklake/0.1 (ontology store; +https://www.ebi.ac.uk/ols4)"

# Contract tables (docs/CONTRACTS.md) + module tables. Column order is the export order.
SCHEMAS: dict[str, dict[str, str]] = {
    "ols_term_cache": {
        "curie": "VARCHAR PRIMARY KEY", "iri": "VARCHAR", "label": "VARCHAR", "ontologyId": "VARCHAR",
        "isObsolete": "BOOLEAN", "oboFoundry": "BOOLEAN", "fetchedAt": "TIMESTAMPTZ"},
    "ref_ncbitaxon_map": {
        "taxonTypeCode": "VARCHAR NOT NULL", "taxonID": "VARCHAR NOT NULL", "neonName": "VARCHAR",
        "queryName": "VARCHAR", "curie": "VARCHAR", "label": "VARCHAR", "method": "VARCHAR", "status": "VARCHAR",
        "resolvedAt": "TIMESTAMPTZ"},
    "ref_unit_map": {
        "neonUnit": "VARCHAR PRIMARY KEY", "uoCurie": "VARCHAR", "uoLabel": "VARCHAR", "method": "VARCHAR",
        "status": "VARCHAR", "validatedAt": "TIMESTAMPTZ"},
    "ref_nlcd_envo": {"nlcdClass": "VARCHAR PRIMARY KEY", "envoCurie": "VARCHAR", "envoLabel": "VARCHAR"},
    "ref_site_envo": {
        "siteCode": "VARCHAR PRIMARY KEY", "envoCurie": "VARCHAR", "envoLabel": "VARCHAR", "typeEco": "VARCHAR",
        "method": "VARCHAR", "status": "VARCHAR"},
    "curation_avus": {
        "proposalId": "VARCHAR PRIMARY KEY", "productCode": "VARCHAR", "attribute": "VARCHAR", "value": "VARCHAR",
        "unit": "VARCHAR", "curie": "VARCHAR", "label": "VARCHAR", "ontologyId": "VARCHAR", "aspect": "VARCHAR",
        "columnName": "VARCHAR", "rationale": "VARCHAR", "model": "VARCHAR", "reps": "INTEGER",
        "grounded": "BOOLEAN", "status": "VARCHAR", "reason": "VARCHAR", "createdAt": "TIMESTAMPTZ"},
    # module tables
    "ols_search_cache": {
        "ontologyId": "VARCHAR NOT NULL", "query": "VARCHAR NOT NULL", "size": "INTEGER NOT NULL",
        "within": "VARCHAR NOT NULL", "results": "JSON", "fetchedAt": "TIMESTAMPTZ"},
    "terms_taxon_types": {
        "siteCode": "VARCHAR NOT NULL", "productCode": "VARCHAR NOT NULL", "tableName": "VARCHAR NOT NULL",
        "taxonTypeCode": "VARCHAR", "nTaxa": "BIGINT", "nMatched": "BIGINT", "matchRate": "DOUBLE",
        "tiedWith": "VARCHAR[]", "chosenAt": "TIMESTAMPTZ"},
    "ols_term_synonyms": {"curie": "VARCHAR PRIMARY KEY", "synonyms": "JSON", "fetchedAt": "TIMESTAMPTZ"},
    "ref_site_envo_feature": {
        "siteCode": "VARCHAR PRIMARY KEY", "envoCurie": "VARCHAR", "envoLabel": "VARCHAR", "habitat": "VARCHAR",
        "method": "VARCHAR", "status": "VARCHAR"},
    "site_envo_proposals": {
        "siteCode": "VARCHAR NOT NULL", "role": "VARCHAR NOT NULL", "source": "VARCHAR NOT NULL",
        "envoCurie": "VARCHAR", "envoLabel": "VARCHAR", "rule": "VARCHAR", "detail": "JSON",
        "createdAt": "TIMESTAMPTZ"},
}
COMPOSITE_KEYS: dict[str, tuple[str, ...]] = {
    "ref_ncbitaxon_map": ("taxonTypeCode", "taxonID"),
    "ols_search_cache": ("ontologyId", "query", "size", "within"),
    "terms_taxon_types": ("siteCode", "productCode", "tableName"),
    "site_envo_proposals": ("siteCode", "role", "source"),
}
EXPORT_TABLES: tuple[str, ...] = ("ols_term_cache", "ref_ncbitaxon_map", "ref_unit_map", "ref_nlcd_envo",
                                  "ref_site_envo", "curation_avus", "terms_taxon_types", "ref_site_envo_feature",
                                  "site_envo_proposals")
# which of two rows with the same key is better when merging several VMs' exports (higher wins; unknown -1)
STATUS_RANK: dict[str, int] = {"human_accepted": 7, "curated": 6, "accepted": 6, "ok": 5, "not_applicable": 4,
                               "obsolete": 4, "rejected": 3, "proposed": 2, "heuristic": 2, "unmapped": 1,
                               "unresolved": 1, "pending": 0}


class OLSUnavailable(RuntimeError):
    """OLS (or the OBO Foundry registry) could not be reached after retries."""


# ---------------------------------------------------------------- paths

def store_path() -> Path:
    return env.WORK_DIR / "ontology_cache.duckdb"


def maps_dir() -> Path:
    return env.ROOT / "ref" / "ontology_maps"


def vm_export_dir(vm: str | None = None) -> Path:
    """This VM's own export directory (scratch): ``work/ontology_export/vm/<vm>/``."""
    return env.WORK_DIR / "ontology_export" / "vm" / (vm or env.vm_id())


def registry_path() -> Path:
    return env.ROOT / "ref" / "obofoundry_ontologies.jsonld"


def table_key(table: str) -> tuple[str, ...]:
    if table in COMPOSITE_KEYS:
        return COMPOSITE_KEYS[table]
    return tuple(c for c, t in SCHEMAS[table].items() if "PRIMARY KEY" in t)


# ---------------------------------------------------------------- store

def _create(con: duckdb.DuckDBPyConnection) -> None:
    for table, cols in SCHEMAS.items():
        body = ", ".join(f'"{c}" {t}' for c, t in cols.items())
        if table in COMPOSITE_KEYS:
            body += ", PRIMARY KEY (" + ", ".join(f'"{c}"' for c in COMPOSITE_KEYS[table]) + ")"
        con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({body})')
        have = {r[0] for r in con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE database_name = current_database() "
            "AND schema_name = 'main' AND table_name = ?",
            [table]).fetchall()}
        for c, t in cols.items():
            if c not in have:  # schema grows, never shrinks (constraints only on creation)
                base = t.replace("PRIMARY KEY", "").replace("NOT NULL", "").strip()
                con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" {base}')


def _is_lock_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "lock" in msg and ("conflicting" in msg or "could not set" in msg)


def open_store(read_only: bool = False, *, wait_s: float = 120.0) -> duckdb.DuckDBPyConnection:
    """Open ``work/ontology_cache.duckdb`` (created with every contract table when missing).

    Lock conflicts with another process are retried for ``wait_s`` seconds.
    """
    path = store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if read_only and not path.exists():
        open_store(read_only=False, wait_s=wait_s).close()
    deadline = time.monotonic() + wait_s
    delay = 0.5
    while True:
        try:
            con = duckdb.connect(str(path), read_only=read_only)
            break
        except duckdb.Error as exc:
            if not _is_lock_error(exc) or time.monotonic() >= deadline:
                raise
            log.info("ontology store is locked by another process; retrying in %.1fs", delay)
            time.sleep(delay)
            delay = min(delay * 2, 10.0)
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("SET threads=2")
        if not read_only:
            _create(con)
    except BaseException:
        con.close()
        raise
    return con


@contextlib.contextmanager
def store(read_only: bool = False, *, wait_s: float = 120.0) -> Iterator[duckdb.DuckDBPyConnection]:
    con = open_store(read_only=read_only, wait_s=wait_s)
    try:
        yield con
    finally:
        con.close()


@contextlib.contextmanager
def _use(con: duckdb.DuckDBPyConnection | None) -> Iterator[duckdb.DuckDBPyConnection]:
    """The caller's connection, or a short-lived writable one."""
    if con is not None:
        yield con
        return
    with store() as own:
        yield own


# ---------------------------------------------------------------- CURIEs

_IRI_RE = re.compile(r"/obo/([A-Za-z][A-Za-z0-9]*)_([A-Za-z0-9_]+)$")
_PREFIX_CASE = {"ncbitaxon": "NCBITaxon"}


def norm_curie(value: Any) -> str | None:
    """Canonical CURIE from a CURIE, an OBO IRI, ``PREFIX_ID``, or a dict with curie/unit/iri.

    Prefixes are upper-cased except NCBITaxon. Returns ``None`` when nothing CURIE-like is found.
    """
    if isinstance(value, dict):
        c = str(value.get("curie") or value.get("unit") or "").strip()
        if not c and value.get("iri"):
            c = str(value["iri"]).strip()
    else:
        c = str(value or "").strip()
    if not c:
        return None
    if c.startswith("http"):
        m = _IRI_RE.search(c)
        if not m:
            return None
        p, i = m.group(1), m.group(2)
    elif ":" in c:
        p, i = c.split(":", 1)
    elif re.fullmatch(r"[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9_]+", c):
        p, i = c.split("_", 1)
    else:
        return None
    p, i = p.strip(), i.strip()
    if not p or not i or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", p):
        return None
    return f"{_PREFIX_CASE.get(p.lower(), p.upper())}:{i}"


def iri_for(curie: str) -> str:
    p, i = curie.split(":", 1)
    return f"http://purl.obolibrary.org/obo/{p}_{i}"


def _ensure_iri(parent: str) -> str:
    if parent.startswith("http"):
        return parent
    c = norm_curie(parent)
    if c is None:
        raise ValueError(f"not an IRI or CURIE: {parent!r}")
    return iri_for(c)


# ---------------------------------------------------------------- HTTP (swappable in tests)

_HTTP: Any = None
_HTTP_LOCK = threading.Lock()


def _http() -> Any:
    global _HTTP
    with _HTTP_LOCK:
        if _HTTP is None:
            import httpx

            _HTTP = httpx.Client(timeout=httpx.Timeout(30.0, connect=15.0), follow_redirects=True,
                                 headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                                 limits=httpx.Limits(max_connections=MAX_CONCURRENCY))
    return _HTTP


def _get(url: str) -> tuple[int, bytes]:
    """GET with retries on transport errors, 429 and 5xx. Returns (status, body)."""
    import httpx

    last: str = ""
    for attempt in range(RETRIES):
        try:
            r = _http().get(url)
        except httpx.HTTPError as exc:
            last = type(exc).__name__
        else:
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
            else:
                return r.status_code, r.content
        time.sleep(min(2.0 ** attempt, 8.0))
    raise OLSUnavailable(f"GET {url.split('?')[0]} failed after {RETRIES} attempts ({last})")


def _fetch_term(curie: str) -> dict[str, Any] | None:
    """Raw OLS v1 term record → our dict (without oboFoundry); None on 404/4xx."""
    ont = curie.split(":", 1)[0].lower()
    enc = urllib.parse.quote(urllib.parse.quote(iri_for(curie), safe=""), safe="")
    status, body = _get(f"{OLS_API}/ontologies/{ont}/terms/{enc}")
    if status != 200:
        return None
    d = json.loads(body)
    label = d.get("label")
    if isinstance(label, list):
        label = label[0] if label else None
    obsolete = bool(d.get("is_obsolete")) or str(label or "").lower().startswith("obsolete")
    return {"curie": curie, "iri": d.get("iri") or iri_for(curie), "label": label,
            "ontologyId": d.get("ontology_name") or ont, "isObsolete": obsolete, "synonyms": _term_synonyms(d)}


def _term_synonyms(d: dict[str, Any]) -> list[dict[str, Any]]:
    """``[{name, scope, type}]`` from an OLS v1 term record (``obo_synonym``, else the bare ``synonyms``)."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for s in d.get("obo_synonym") or []:
        if isinstance(s, dict) and s.get("name"):
            key = (str(s["name"]), s.get("scope"))
            if key not in seen:
                seen.add(key)
                out.append({"name": str(s["name"]), "scope": s.get("scope"), "type": s.get("type")})
    names = {o["name"] for o in out}
    for s in d.get("synonyms") or []:
        if isinstance(s, str) and s and s not in names:
            names.add(s)
            out.append({"name": s, "scope": None, "type": None})
    return out


_TLS = threading.local()


def _ols_client() -> Any:
    if getattr(_TLS, "client", None) is None:
        from mesa_mcp.ols.client import get_ols_client

        _TLS.client = get_ols_client()
    return _TLS.client


def _fetch_search(query: str, ontology_id: str, size: int, within: str) -> list[dict[str, Any]]:
    """mesa-mcp OLSClient search (v2 entities, or v1 allChildrenOf) with retries."""
    last = ""
    for attempt in range(RETRIES):
        try:
            client = _ols_client()
            if within:
                raw = client.search_term_descendants(query, ontology_id, within, size=size)
            else:
                raw = client.search_terms(query, ontology_id=ontology_id, size=size)
            return _clean_hits(raw)
        except Exception as exc:  # noqa: BLE001 - OLSAPIError / requests errors
            status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and 400 <= int(status) < 500 and int(status) != 429:
                log.warning("OLS search %r in %s rejected (HTTP %s)", query, ontology_id, status)
                return []
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
        time.sleep(min(2.0 ** attempt, 8.0))
    raise OLSUnavailable(f"OLS search {query!r} in {ontology_id} failed after {RETRIES} attempts ({last})")


def _clean_hits(raw: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for h in raw:
        curie = norm_curie(h.get("curie") or "") or norm_curie(h.get("iri") or "")
        if not curie or curie in seen:
            continue
        seen.add(curie)
        out.append({"curie": curie, "label": h.get("label") or "", "iri": h.get("iri") or iri_for(curie),
                    "ontologyId": h.get("ontologyId") or ""})
    return out


def _parallel(fn: Any, items: list[Any]) -> dict[Any, Any]:
    """Run ``fn(item)`` for every item with <= MAX_CONCURRENCY threads; failures map to the exception."""
    out: dict[Any, Any] = {}
    if not items:
        return out

    def run(item: Any) -> tuple[Any, Any]:
        try:
            return item, fn(item)
        except Exception as exc:  # noqa: BLE001
            return item, exc

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as ex:
        for item, res in ex.map(run, items):
            out[item] = res
    return out


# ---------------------------------------------------------------- OBO Foundry registry

_OBO_MEMO: dict[str, Any] = {}


def obo_foundry_prefixes(refresh: bool = False) -> set[str]:
    """Active, non-obsolete OBO Foundry ontology ids (lower case) from the cached registry file."""
    path = registry_path()
    if refresh or not path.exists():
        status, body = _get(OBO_REGISTRY_URL)
        if status != 200:
            raise OLSUnavailable(f"OBO Foundry registry returned HTTP {status}")
        json.loads(body)  # validate before replacing the cache
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".part")
        tmp.write_bytes(body)
        os.replace(tmp, path)
        log.info("cached the OBO Foundry registry (%d bytes) in %s", len(body), path)
    key = f"{path}:{path.stat().st_mtime_ns}"
    if _OBO_MEMO.get("key") != key:
        reg = json.loads(path.read_text(encoding="utf-8"))
        onts = reg.get("ontologies", reg) if isinstance(reg, dict) else reg
        _OBO_MEMO["key"] = key
        _OBO_MEMO["ids"] = {str(o["id"]).lower() for o in onts
                            if o.get("id") and o.get("activity_status") == "active" and not o.get("is_obsolete")}
    return set(_OBO_MEMO["ids"])


def _obo_flag(curie: str) -> bool | None:
    try:
        return curie.split(":", 1)[0].lower() in obo_foundry_prefixes()
    except (OLSUnavailable, OSError, ValueError) as exc:
        log.warning("OBO Foundry registry unavailable (%s); oboFoundry left NULL", exc)
        return None


# ---------------------------------------------------------------- term lookups

_TERM_MISSES: set[str] = set()
TERM_FIELDS = ("curie", "iri", "label", "ontologyId", "isObsolete", "oboFoundry")


def _cached_terms(con: duckdb.DuckDBPyConnection, curies: list[str]) -> dict[str, dict[str, Any]]:
    if not curies:
        return {}
    rows = con.execute(f"SELECT {', '.join(TERM_FIELDS)} FROM ols_term_cache WHERE curie IN "
                       f"(SELECT unnest(?::VARCHAR[]))", [curies]).fetchall()
    return {r[0]: dict(zip(TERM_FIELDS, r, strict=True)) for r in rows}


def _store_terms(con: duckdb.DuckDBPyConnection, terms: list[dict[str, Any]]) -> None:
    if not terms:
        return
    now = env.utcnow()
    con.executemany("INSERT OR REPLACE INTO ols_term_cache (curie, iri, label, ontologyId, isObsolete, oboFoundry, "
                    "fetchedAt) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [[t["curie"], t["iri"], t["label"], t["ontologyId"], t["isObsolete"], t["oboFoundry"], now]
                     for t in terms])


def ols_terms(curies: Iterable[str], *, con: duckdb.DuckDBPyConnection | None = None,
              refresh: bool = False) -> dict[str, dict[str, Any] | None]:
    """Cached term records for many CURIEs. Unreachable ones are omitted from the result."""
    wanted: list[str] = []
    out: dict[str, dict[str, Any] | None] = {}
    for raw in curies:
        c = norm_curie(raw)
        if c is None:
            continue
        if c in _TERM_MISSES and not refresh:
            out[c] = None
        elif c not in wanted:
            wanted.append(c)
    if not wanted:
        return out
    hits: dict[str, dict[str, Any]] = {}
    if not refresh:
        with _use(con) as c:
            hits = _cached_terms(c, wanted)
    out.update(hits)
    missing = [c for c in wanted if c not in hits]
    fetched = _parallel(_fetch_term, missing)
    new: list[dict[str, Any]] = []
    for c in missing:
        res = fetched.get(c)
        if isinstance(res, Exception):
            log.warning("OLS term %s unavailable: %s", c, res)
            continue
        if res is None:
            _TERM_MISSES.add(c)
            out[c] = None
            continue
        res["oboFoundry"] = _obo_flag(c)
        new.append(res)
        out[c] = {k: res[k] for k in TERM_FIELDS}
    if new:
        with _use(con) as c:
            _store_terms(c, new)
            _store_synonyms(c, new)
    return out


def _store_synonyms(con: duckdb.DuckDBPyConnection, terms: list[dict[str, Any]]) -> None:
    rows = [[t["curie"], json.dumps(t["synonyms"], ensure_ascii=False), env.utcnow()]
            for t in terms if isinstance(t.get("synonyms"), list)]
    if rows:
        con.executemany("INSERT OR REPLACE INTO ols_term_synonyms (curie, synonyms, fetchedAt) VALUES (?, ?, ?)",
                        rows)


def term_synonyms(curies: Iterable[str], *, con: duckdb.DuckDBPyConnection | None = None
                  ) -> dict[str, list[dict[str, Any]]]:
    """``{curie: [{name, scope, type}]}`` (cached). CURIEs that could not be fetched are left out; a term
    that does not exist maps to ``[]``."""
    wanted = list(dict.fromkeys(c for raw in curies if (c := norm_curie(raw))))
    if not wanted:
        return {}
    with _use(con) as c:
        rows = c.execute("SELECT curie, synonyms FROM ols_term_synonyms WHERE curie IN "
                         "(SELECT unnest(?::VARCHAR[]))", [wanted]).fetchall()
    out = {r[0]: json.loads(r[1]) if r[1] else [] for r in rows}
    missing = [x for x in wanted if x not in out]
    if missing:
        got = ols_terms(missing, con=con, refresh=True)  # the term fetch stores the synonyms as well
        with _use(con) as c:
            rows = c.execute("SELECT curie, synonyms FROM ols_term_synonyms WHERE curie IN "
                             "(SELECT unnest(?::VARCHAR[]))", [missing]).fetchall()
        out.update({r[0]: json.loads(r[1]) if r[1] else [] for r in rows})
        for x in missing:
            if x not in out and x in got and got[x] is None:
                out[x] = []  # no such term
    return out


def ols_term(curie: str, *, con: duckdb.DuckDBPyConnection | None = None,
             refresh: bool = False) -> dict[str, Any] | None:
    """One cached term record (see the module docstring). Raises OLSUnavailable when unreachable."""
    c = norm_curie(curie)
    if c is None:
        return None
    res = ols_terms([c], con=con, refresh=refresh)
    if c not in res:
        raise OLSUnavailable(f"OLS term {c} could not be fetched")
    return res[c]


# ---------------------------------------------------------------- searches

def _search_key(query: str, ontology_id: str, size: int, within: str) -> tuple[str, str, int, str]:
    return ontology_id.lower(), " ".join(query.split()).lower(), int(size), within


def search_many(queries: Iterable[str], ontology_id: str, size: int = 25, *, within: str | None = None,
                con: duckdb.DuckDBPyConnection | None = None,
                refresh: bool = False) -> dict[str, list[dict[str, Any]]]:
    """Cached OLS searches for many queries (<= 4 concurrent). Failed queries are omitted."""
    parent = _ensure_iri(within) if within else ""
    qs = list(dict.fromkeys(q for q in queries if q and q.strip()))
    keys = {q: _search_key(q, ontology_id, size, parent) for q in qs}
    out: dict[str, list[dict[str, Any]]] = {}
    if not qs:
        return out
    cached: dict[tuple[str, str, int, str], list[dict[str, Any]]] = {}
    if not refresh:
        with _use(con) as c:
            rows = c.execute("SELECT ontologyId, query, size, within, results FROM ols_search_cache "
                             "WHERE ontologyId = ? AND size = ? AND within = ? AND query IN "
                             "(SELECT unnest(?::VARCHAR[]))",
                             [ontology_id.lower(), int(size), parent, [k[1] for k in keys.values()]]).fetchall()
        cached = {(r[0], r[1], r[2], r[3]): json.loads(r[4]) for r in rows}
    todo: list[str] = []
    for q in qs:
        if keys[q] in cached:
            out[q] = cached[keys[q]]
        elif keys[q][1] not in {keys[t][1] for t in todo}:
            todo.append(q)
    fetched = _parallel(lambda q: _fetch_search(keys[q][1], ontology_id.lower(), size, parent), todo)
    new: list[list[Any]] = []
    now = env.utcnow()
    for q in todo:
        res = fetched.get(q)
        if isinstance(res, Exception):
            log.warning("OLS search %r (%s) unavailable: %s", q, ontology_id, res)
            continue
        new.append([*keys[q], json.dumps(res, ensure_ascii=False), now])
        cached[keys[q]] = res
    for q in qs:  # queries that normalise to the same key share one fetch
        if q not in out and keys[q] in cached:
            out[q] = cached[keys[q]]
    if new:
        with _use(con) as c:
            c.executemany("INSERT OR REPLACE INTO ols_search_cache (ontologyId, query, size, within, results, "
                          "fetchedAt) VALUES (?, ?, ?, ?, ?, ?)", new)
    return out


def search_ols(query: str, ontology_id: str, size: int = 25, *, within: str | None = None,
               con: duckdb.DuckDBPyConnection | None = None, refresh: bool = False) -> list[dict[str, Any]]:
    """One cached OLS search (see the module docstring). Raises OLSUnavailable when unreachable."""
    if not query or not query.strip():
        return []
    res = search_many([query], ontology_id, size, within=within, con=con, refresh=refresh)
    if query not in res:
        raise OLSUnavailable(f"OLS search {query!r} in {ontology_id} failed")
    return res[query]


def exact_matches(results: Iterable[dict[str, Any]], label: str, prefix: str | None = None) -> list[dict[str, Any]]:
    """Hits whose label equals ``label`` (case-insensitive, whitespace-normalised), deduplicated by CURIE."""
    want = " ".join(label.split()).lower()
    pre = prefix.lower() if prefix else None
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for h in results:
        curie = h.get("curie") or ""
        if curie in seen or " ".join(str(h.get("label") or "").split()).lower() != want:
            continue
        if pre and curie.split(":", 1)[0].lower() != pre:
            continue
        seen.add(curie)
        out.append(h)
    return out


def _fold(text: Any) -> str:
    return " ".join(str(text or "").split()).casefold()


def synonym_matches(results: Iterable[dict[str, Any]], name: str, prefix: str | None = None) -> list[dict[str, Any]]:
    """Hits whose ``synonyms`` contain ``name`` (case-insensitive, whitespace-normalised), deduplicated."""
    want = _fold(name)
    pre = prefix.lower() if prefix else None
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for h in results:
        curie = h.get("curie") or ""
        if curie in seen or not any(_fold(s) == want for s in h.get("synonyms") or []):
            continue
        if pre and curie.split(":", 1)[0].lower() != pre:
            continue
        seen.add(curie)
        out.append(h)
    return out


EXACT_FIELDS = ("label", "synonym")


def _exact_within(fields: tuple[str, ...], parent: str) -> str:
    return "exact:" + ",".join(fields) + (f"|{parent}" if parent else "")


def _fetch_exact(query: str, ontology_id: str, size: int, fields: tuple[str, ...], parent: str
                 ) -> list[dict[str, Any]]:
    """OLS v1 ``/search`` with ``exact=true`` on ``fields`` (raw HTTP; keeps each hit's synonyms)."""
    params: dict[str, Any] = {"q": query, "ontology": ontology_id, "exact": "true", "queryFields": ",".join(fields),
                              "rows": int(size), "fieldList": "iri,label,obo_id,synonym,ontology_name"}
    if parent:
        params["allChildrenOf"] = parent
    status, body = _get(f"{OLS_API}/search?{urllib.parse.urlencode(params)}")
    if status != 200:
        log.warning("OLS exact search %r in %s rejected (HTTP %s)", query, ontology_id, status)
        return []
    docs = (json.loads(body).get("response") or {}).get("docs") or []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for d in docs:
        curie = norm_curie(d.get("obo_id") or "") or norm_curie(d.get("iri") or "")
        if not curie or curie in seen:
            continue
        seen.add(curie)
        label = d.get("label")
        if isinstance(label, list):
            label = label[0] if label else ""
        syn = d.get("synonym") or []
        out.append({"curie": curie, "label": label or "", "iri": d.get("iri") or iri_for(curie),
                    "ontologyId": d.get("ontology_name") or ontology_id,
                    "synonyms": [str(x) for x in (syn if isinstance(syn, list) else [syn]) if x]})
    return out


def search_exact_many(queries: Iterable[str], ontology_id: str, size: int = 25, *,
                      fields: tuple[str, ...] = EXACT_FIELDS, within: str | None = None,
                      con: duckdb.DuckDBPyConnection | None = None,
                      refresh: bool = False) -> dict[str, list[dict[str, Any]]]:
    """Cached exact OLS v1 searches on label and/or synonym (<= 4 concurrent). Failed queries are omitted."""
    fields = tuple(fields)
    parent = _ensure_iri(within) if within else ""
    tag = _exact_within(fields, parent)
    qs = list(dict.fromkeys(q for q in queries if q and q.strip()))
    keys = {q: _search_key(q, ontology_id, size, tag) for q in qs}
    out: dict[str, list[dict[str, Any]]] = {}
    if not qs:
        return out
    cached: dict[tuple[str, str, int, str], list[dict[str, Any]]] = {}
    if not refresh:
        with _use(con) as c:
            rows = c.execute("SELECT ontologyId, query, size, within, results FROM ols_search_cache "
                             "WHERE ontologyId = ? AND size = ? AND within = ? AND query IN "
                             "(SELECT unnest(?::VARCHAR[]))",
                             [ontology_id.lower(), int(size), tag, [k[1] for k in keys.values()]]).fetchall()
        cached = {(r[0], r[1], r[2], r[3]): json.loads(r[4]) for r in rows}
    todo: list[str] = []
    for q in qs:
        if keys[q] in cached:
            out[q] = cached[keys[q]]
        elif keys[q][1] not in {keys[t][1] for t in todo}:
            todo.append(q)
    fetched = _parallel(lambda q: _fetch_exact(keys[q][1], ontology_id.lower(), size, fields, parent), todo)
    new: list[list[Any]] = []
    now = env.utcnow()
    for q in todo:
        res = fetched.get(q)
        if isinstance(res, Exception):
            log.warning("OLS exact search %r (%s) unavailable: %s", q, ontology_id, res)
            continue
        new.append([*keys[q], json.dumps(res, ensure_ascii=False), now])
        cached[keys[q]] = res
    for q in qs:
        if q not in out and keys[q] in cached:
            out[q] = cached[keys[q]]
    if new:
        with _use(con) as c:
            c.executemany("INSERT OR REPLACE INTO ols_search_cache (ontologyId, query, size, within, results, "
                          "fetchedAt) VALUES (?, ?, ?, ?, ?, ?)", new)
    return out


def search_exact(query: str, ontology_id: str, size: int = 25, *, fields: tuple[str, ...] = EXACT_FIELDS,
                 within: str | None = None, con: duckdb.DuckDBPyConnection | None = None,
                 refresh: bool = False) -> list[dict[str, Any]]:
    """One cached exact search (see ``search_exact_many``). Raises OLSUnavailable when unreachable."""
    if not query or not query.strip():
        return []
    res = search_exact_many([query], ontology_id, size, fields=fields, within=within, con=con, refresh=refresh)
    if query not in res:
        raise OLSUnavailable(f"OLS exact search {query!r} in {ontology_id} failed")
    return res[query]


# ---------------------------------------------------------------- export

def _md5(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.md5(path.read_bytes(), usedforsecurity=False).hexdigest()


def export_parquet(con: duckdb.DuckDBPyConnection | None = None, *, out_dir: Path | None = None,
                   tables: Iterable[str] = EXPORT_TABLES, vm_scoped: bool = False) -> dict[str, int] | Path:
    """Export store tables to ``ref/ontology_maps/<table>.parquet`` (ordered by key; replaced only on change).

    ``vm_scoped=True`` exports to :func:`vm_export_dir` (unless ``out_dir``) and returns that directory, the
    local tree to upload to ``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/``; otherwise returns row counts.
    """
    target = out_dir or (vm_export_dir() if vm_scoped else maps_dir())
    target.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    ctx = contextlib.nullcontext(con) if con is not None else store(read_only=True)
    with ctx as c:
        for table in tables:
            order = ", ".join(f'"{k}"' for k in table_key(table)) or "ALL"
            final = target / f"{table}.parquet"
            tmp = final.with_name(final.name + ".part")
            dest = str(tmp).replace("'", "''")
            c.execute(f"COPY (SELECT * FROM \"{table}\" ORDER BY {order}) TO '{dest}' "
                      f"(FORMAT parquet, COMPRESSION zstd)")
            counts[table] = int(c.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
            if _md5(tmp) == _md5(final):
                tmp.unlink()
            else:
                os.replace(tmp, final)
    log.info("exported %d ontology tables to %s", len(counts), target)
    return target if vm_scoped else counts


def _rank_sql(alias: str) -> str:
    whens = " ".join(f"WHEN '{k}' THEN {v}" for k, v in STATUS_RANK.items())
    return f"(CASE {alias}.\"status\" {whens} ELSE -1 END)"


def merge_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> dict[str, int]:
    """Merge one exported Parquet file into ``table``: {inserted, upgraded}. See the module docstring."""
    lit = "'" + str(path).replace("'", "''") + "'"
    in_file = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({lit})").fetchall()}
    cols = [c for c in SCHEMAS[table] if c in in_file]
    key = [k for k in table_key(table) if k in cols]
    q = ", ".join(f'"{c}"' for c in cols)
    before = int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
    upgraded = 0
    con.execute(f'CREATE OR REPLACE TEMP TABLE _merge_in AS SELECT {q} FROM read_parquet({lit})')
    try:
        if "status" in cols and key and len(key) == len(table_key(table)):
            on = " AND ".join(f'i."{k}" = t."{k}"' for k in key)
            con.execute(f'CREATE OR REPLACE TEMP TABLE _merge_better AS SELECT i.* FROM _merge_in i JOIN "{table}" t '
                        f"ON {on} WHERE {_rank_sql('i')} > {_rank_sql('t')}")
            upgraded = int(con.execute("SELECT count(*) FROM _merge_better").fetchone()[0])
            if upgraded:
                con.execute(f'INSERT OR REPLACE INTO "{table}" ({q}) SELECT {q} FROM _merge_better')
            con.execute("DROP TABLE IF EXISTS _merge_better")
        con.execute(f'INSERT OR IGNORE INTO "{table}" ({q}) SELECT {q} FROM _merge_in')
    finally:
        con.execute("DROP TABLE IF EXISTS _merge_in")
    inserted = int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]) - before
    return {"inserted": inserted, "upgraded": upgraded}
