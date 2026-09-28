"""Phase-1 AVU vocabulary: the desired AVUs of the catalog targets (scope ``catalog``).

Targets (each collection is its own work unit, together with its direct files):
- collections: the project root, ``api/``, ``api/runs/``, ``api/runs/<runId>/``, ``…/sealed/`` (and each
  sealed sub-collection such as ``sealed/data_query/``), ``ducklake/``, ``ducklake/data/``,
  ``data/<schema>/`` and ``data/<schema>/<table>/``, ``sites/``, and the code trees (``scripts/``,
  ``src/…``, ``sql/``, ``docs/``, ``curation/``, ``tests/``) that exist locally.
- data objects: every sealed file, the run's manifest/log, every lake Parquet file and the
  ``neon_catalog.ducklake`` catalog file.

Only ``neon.<lowerCamelCase>`` names are used in the neon namespace. Counts and snapshot ids come
from the catalog lake (attached READ_ONLY) when it exists, else from ``fetch_manifest.json``.
Every value goes through :func:`normalize` (via ``replace_desired``).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from .. import env, state
from .bulk import replace_desired

log = logging.getLogger("neon_ducklake.avu.vocab")

SCOPE = "catalog"
SEAL_MANIFEST = "MANIFEST.json"
SEALED_DIRS = {
    "data_query": "Per-product POST /data/query file inventories (non-AOP, basic package): one bundle per product.",
    "stage": "Harvest stage files (derived work lists: product x site pairs, zero-file pairs, location nodes).",
}
GENERATOR = "vocab.catalog"
NEON_PUBLISHER = "National Ecological Observatory Network (NEON)"
LICENSE = ("CC-BY-4.0", "https://creativecommons.org/licenses/by/4.0/")
API_BASE = "https://data.neonscience.org/api/v0"
SOURCES = ("https://data.neonscience.org/data-api/", API_BASE, "https://data.neonscience.org/data-api/graphql/")
DERIVED_FROM = f"{env.PRECEDENT_IRODS}@{env.PRECEDENT_VERSION}" if env.PRECEDENT_IRODS else None
PIPELINE = ("neon-ducklake: catalog fetch (neon-mcp NeonClient in-process) -> seal -> build (DuckDB "
            "ducklake, one snapshot per build) -> mirror -> tag catalog (journaled bulk AVU writer + MESA history)")
CODE_DIRS = {
    "scripts": "Helper scripts (scripts/with-secrets.sh runs every command with keyring secrets).",
    "src": "Python source of the neon-ducklake pipeline (package neon_ducklake).",
    "sql": "SQL helpers (attach_all.sql attaches every lake read-only).",
    "docs": "Design notes and internal interface contracts.",
    "curation": "Ontology curation prompts, MCP config and validated per-product terms.",
    "tests": "Hermetic pytest suite.",
}

# family -> the NEON API endpoint it came from
ENDPOINTS = {
    "products": "GET /products",
    "sites": "GET /sites",
    "releases": "GET /releases",
    "manifests": "GET MANIFEST_AVAILABLE release artifact (GCS)",
    "release_products": "GET /releases/{release}/products",
    "release_sites": "GET /releases/{release}/sites",
    "locations_sites": "GET /locations/sites",
    "samples": "GET /samples/supportedClasses",
    "prototype": "GET /prototype/datasets",
    "prototype_data": "GET /prototype/data/{uuid}",
    "graphql": "POST /graphql (__schema introspection)",
    "taxonomy": "GET /taxonomy",
    "documents_head": "HEAD /documents/{spec}",
    "location_hierarchy": "GET /locations/{site}?hierarchy=true",
    "locations": "POST /graphql findLocations",
    "location_history": "GET /locations/{name}?history=true",
    "data_query": "POST /data/query",
    "external": "GET /data/{product}/{site}/{month}",
}

# catalog-lake table -> neon.api.<attribute> count
LAKE_COUNTS = {
    "products": "products", "sites": "sites", "domains": "domains", "releases": "releases",
    "release_products": "releaseProducts", "release_manifest_months": "releaseManifestMonths",
    "product_site": "productSites", "availability_months": "availabilityMonths", "files": "files",
    "external_data": "externalData", "taxonomy": "taxa", "locations": "locations",
    "location_history": "locationHistory", "documents": "documents", "sample_classes": "sampleClasses",
    "prototype_datasets": "prototypeDatasets", "prototype_files": "prototypeFiles",
}


def lower_camel(name: str) -> str:
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", name) if p]
    if not parts:
        return name
    return parts[0][:1].lower() + parts[0][1:] + "".join(p[:1].upper() + p[1:] for p in parts[1:])


def latest_run_id() -> str | None:
    """api/LATEST, else the newest run directory with a sealed/ tree, else the newest run."""
    latest = env.API_DIR / "LATEST"
    if latest.exists() and latest.read_text().strip():
        return latest.read_text().strip()
    if not env.RUNS_DIR.exists():
        return None
    runs = sorted(d for d in env.RUNS_DIR.iterdir() if d.is_dir())
    sealed = [d for d in runs if (d / "sealed").is_dir()]
    pick = (sealed or runs)
    return pick[-1].name if pick else None


def read_manifest(run_id: str) -> dict[str, Any]:
    p = env.RUNS_DIR / run_id / "fetch_manifest.json"
    try:
        return json.loads(p.read_text()) if p.exists() else {}
    except ValueError:
        return {}


# ---------------------------------------------------------------- catalog lake (READ_ONLY)

def open_catalog_lake_ro() -> Any | None:
    """A DuckDB connection with the catalog lake attached READ_ONLY as ``neon``, or None."""
    import duckdb

    cat = env.LAKE_DIR / env.CATALOG_LAKE
    if not cat.exists():
        return None
    con = duckdb.connect()
    try:
        con.execute("LOAD ducklake")
        data = (env.LAKE_DIR / "data").as_posix().rstrip("/") + "/"
        con.execute(f"ATTACH 'ducklake:{cat.as_posix()}' AS neon (READ_ONLY, DATA_PATH '{data}', "
                    "OVERRIDE_DATA_PATH true)")
        return con
    except Exception as exc:  # noqa: BLE001 - a writer holds it, or it is not a lake yet
        log.warning("catalog lake not readable (%s); using fetch_manifest.json", exc)
        con.close()
        return None


def lake_facts(con: Any) -> dict[str, Any]:
    tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables "
                                        "WHERE table_catalog='neon' AND table_schema='main'").fetchall()}
    facts: dict[str, Any] = {"tables": {}, "counts": {}}
    row = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('neon')").fetchone()
    facts["snapshotId"] = row[0] if row else None
    for t in sorted(tables):
        try:
            facts["tables"][t] = con.execute(f'SELECT count(*) FROM neon.main."{t}"').fetchone()[0]
        except Exception as exc:  # noqa: BLE001 - views over missing tables etc.
            log.debug("count %s failed: %s", t, exc)
    for t, attr in LAKE_COUNTS.items():
        if t in facts["tables"]:
            facts["counts"][attr] = facts["tables"][t]
    if "files" in tables:
        facts["fileBytes"] = con.execute("SELECT sum(size) FROM neon.main.files").fetchone()[0]
    if "harvest_runs" in tables:
        try:
            cols = [r[0] for r in con.execute("DESCRIBE neon.main.harvest_runs").fetchall()]
            key = next((c for c in ("runId", "run_id") if c in cols), None)
            if key:
                facts["harvestRuns"] = [r[0] for r in con.execute(
                    f'SELECT DISTINCT "{key}" FROM neon.main.harvest_runs ORDER BY 1').fetchall()]
        except Exception as exc:  # noqa: BLE001
            log.debug("harvest_runs unreadable: %s", exc)
    return facts


# ---------------------------------------------------------------- sealed files

def _gz_lines(path: Path) -> int:
    n = 0
    last = b"\n"
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            n += chunk.count(b"\n")
            last = chunk[-1:]
    return n + (0 if last == b"\n" else 1)


def row_count(path: Path) -> int:
    """Records in a sealed file: lines for JSONL, list length (else 1) for a JSON document."""
    name = path.name
    if name.endswith((".jsonl.gz", ".jsonl", ".ndjson", ".ndjson.gz")):
        return _gz_lines(path)
    if name.endswith((".json.gz", ".json")):
        raw = gzip.decompress(path.read_bytes()) if name.endswith(".gz") else path.read_bytes()
        doc = json.loads(raw)
        return len(doc) if isinstance(doc, list) else 1
    if name.endswith(".parquet"):
        import pyarrow.parquet as pq

        return pq.read_metadata(path).num_rows
    return _gz_lines(path)


def md5_file(path: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        while chunk := fh.read(4 << 20):
            h.update(chunk)
    return h.hexdigest()


def fmt_of(name: str) -> str:
    if name.endswith((".jsonl.gz", ".ndjson.gz")):
        return "application/jsonl+gzip"
    if name.endswith(".jsonl"):
        return "application/jsonl"
    if name.endswith(".json.gz"):
        return "application/json+gzip"
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".parquet"):
        return "application/vnd.apache.parquet"
    return "application/octet-stream"


def family_of(rel: str) -> str:
    first = rel.split("/", 1)[0]
    return re.sub(r"(\.jsonl|\.json|\.ndjson|\.parquet)?(\.gz)?$", "", first)


def seal_index(run_dir: Path) -> dict[str, dict[str, Any]]:
    """Per-file facts from a seal manifest when seal wrote one (keys: rel path under sealed/)."""
    out: dict[str, dict[str, Any]] = {}
    for cand in (run_dir / "sealed" / SEAL_MANIFEST, run_dir / "sealed" / "manifest.json",
                 run_dir / "seal_manifest.json"):
        if not cand.exists():
            continue
        try:
            doc = json.loads(cand.read_text())
        except ValueError:
            continue
        items = doc.get("files", doc) if isinstance(doc, dict) else doc
        if isinstance(items, dict):
            items = [dict(v, path=k) if isinstance(v, dict) else {"path": k} for k, v in items.items()]
        for it in items if isinstance(items, list) else []:
            rel = it.get("path") or it.get("file") or it.get("name")
            if rel:
                out[str(rel).removeprefix("sealed/")] = it
        break
    return out


def family_fetched_at(run_id: str) -> dict[str, str]:
    """max(fetched_at) per family, and per ``data_query/<DP>`` product, from the request ledger (read-only)."""
    db = state.DB_PATH
    if not db.exists():
        return {}
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    try:
        out = {f: t for f, t in con.execute("SELECT family, max(fetched_at) FROM requests WHERE run_id=?"
                                            " AND status='ok' GROUP BY family", (run_id,))}
        for k, t in con.execute("SELECT substr(key, 1, instr(key, '__') - 1), max(fetched_at) FROM requests"
                                " WHERE run_id=? AND status='ok' AND family='data_query' AND instr(key, '__') > 0"
                                " GROUP BY 1", (run_id,)):
            out[f"data_query/{k}"] = t
        return out
    except sqlite3.Error:
        return {}
    finally:
        con.close()


# ---------------------------------------------------------------- builder

class _Rows:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []

    def coll(self, path: str, level: str, avus: list[tuple[Any, ...]]) -> None:
        for t in avus:
            self.rows.append((path, path, "collection", t[0], t[1], t[2] if len(t) > 2 else "", level, GENERATOR))

    def obj(self, path: str, level: str, avus: list[tuple[Any, ...]]) -> None:
        unit = path.rsplit("/", 1)[0]
        for t in avus:
            self.rows.append((unit, path, "data_object", t[0], t[1], t[2] if len(t) > 2 else "", level, GENERATOR))


def _root_avus(run_id: str | None, manifest: dict[str, Any], facts: dict[str, Any] | None) -> list[tuple]:
    harvested = manifest.get("finishedAt") or env.utcnow().isoformat()
    avus: list[tuple] = [
        ("dc.title", "NEON Data API metadata catalog (neon-ducklake)"),
        ("dc.description", ("A DuckLake lakehouse of the complete NEON Data API metadata — products, sites, "
                           "releases, availability (current and release-only), taxonomy, named locations with "
                           "history, documents, samples, prototype datasets and the non-AOP basic-package file "
                           "inventory — harvested through neon-mcp, plus per-site data subtrees under sites/. "
                           "AVU history is kept by MESA in .mesa/ducklake/.")),
        ("dc.publisher", NEON_PUBLISHER),
        ("dc.creator", env.ACTOR, "iRODS user"),
        ("dc.license", LICENSE[0], LICENSE[1]),
        *[("dc.source", u, "URL") for u in SOURCES],
        ("dc.date", harvested[:10], "ISO8601"),
        ("neon.project", "neon-ducklake"),
        ("mesa.project.kind", "ducklake"),
        ("mesa.ducklake.catalog", f"ducklake/{env.CATALOG_LAKE}", "relative path"),
        ("mesa.local_root", env.ROOT.as_posix(), "sparky-1 path"),
        ("provenance.pipeline", PIPELINE),
        *([("provenance.derivedFrom", DERIVED_FROM, "iRODS path")] if DERIVED_FROM else []),
    ]
    for name, sha in sorted(env.repo_versions().items()):
        avus.append(("provenance.tool", f"{name} {sha}"))
    for name, ver in sorted(env.tool_versions().items()):
        avus.append(("provenance.tool", f"{name} {ver}"))
    if run_id:
        avus.append(("neon.catalog.harvestRun", run_id))
        avus.append(("neon.catalog.harvestedAt", harvested, "ISO8601"))
    if facts:
        if facts.get("snapshotId") is not None:
            avus.append(("neon.catalog.snapshotId", str(facts["snapshotId"])))
        for attr, n in sorted(facts["counts"].items()):
            avus.append((f"neon.api.{attr}", str(n), "count"))
        if facts.get("fileBytes") is not None:
            avus.append(("neon.api.fileBytes", str(int(facts["fileBytes"])), "byte"))
    else:
        for fam, c in sorted((manifest.get("counts") or {}).items()):
            n = int((c or {}).get("ok", 0)) + int((c or {}).get("cached", 0))
            if n:
                avus.append((f"neon.api.{lower_camel(fam)}Requests", str(n), "count"))
    return avus


def build_rows(run_id: str | None) -> tuple[list[tuple[Any, ...]], dict[str, Any]]:
    """Compute the catalog-scope desired rows (no database writes)."""
    R = env.IRODS_ROOT
    out = _Rows()
    info: dict[str, Any] = {"run_id": run_id, "source": "manifest"}
    manifest = read_manifest(run_id) if run_id else {}
    lake = open_catalog_lake_ro()
    facts = None
    if lake is not None:
        try:
            facts = lake_facts(lake)
            info["source"] = "lake"
            info["snapshotId"] = facts.get("snapshotId")
        finally:
            lake.close()

    out.coll(R, "root", _root_avus(run_id, manifest, facts))
    out.coll(f"{R}/api", "api", [
        ("dc.description", ("NEON Data API pulls: one directory per harvest run under runs/; sealed/ holds the "
                           "only build input (one gzip JSONL bundle per endpoint family). Raw landings stay local.")),
        ("neon.api.baseUrl", API_BASE, "URL"),
    ])
    if (env.API_DIR / "LATEST").exists():
        out.obj(f"{R}/api/LATEST", "api-file", [("dc.description", "Run id of the latest complete harvest."),
                                                ("neon.harvestRun", (env.API_DIR / "LATEST").read_text().strip())])
    if run_id:
        run_dir = env.RUNS_DIR / run_id
        out.coll(f"{R}/api/runs", "api", [("dc.description", "Harvest runs of the NEON Data API (one per runId).")])
        rc = f"{R}/api/runs/{run_id}"
        run_avus: list[tuple] = [
            ("dc.description", (f"NEON Data API harvest run {run_id}: sealed endpoint bundles, the fetch manifest "
                               "and the request ledger.")),
            ("neon.harvestRun", run_id),
        ]
        if manifest:
            run_avus += [("neon.harvestStatus", manifest.get("status")),
                         ("neon.fetchedAt", manifest.get("finishedAt"), "ISO8601"),
                         ("neon.startedAt", manifest.get("startedAt"), "ISO8601"),
                         ("neon.failureCount", str(manifest.get("failureCount", "")), "count")]
        out.coll(rc, "run", run_avus)
        if (run_dir / "fetch_manifest.json").exists():
            out.obj(f"{rc}/fetch_manifest.json", "run-file", [
                ("dc.description", "Harvest manifest: status, stages, code/tool versions, tunables, counts, failures."),
                ("dc.format", "application/json"), ("neon.harvestRun", run_id)])
        if (run_dir / "fetch_log.jsonl").exists():
            out.obj(f"{rc}/fetch_log.jsonl", "run-file", [
                ("dc.description", "Request ledger export: one JSON line per API request (no signed URLs)."),
                ("dc.format", "application/jsonl"), ("neon.harvestRun", run_id),
                ("neon.rowCount", str(_gz_lines(run_dir / "fetch_log.jsonl")), "count")])
        sealed = run_dir / "sealed"
        if sealed.is_dir():
            idx = seal_index(run_dir)
            fetched = family_fetched_at(run_id)
            out.coll(f"{rc}/sealed", "sealed", [
                ("dc.description", ("Sealed NEON API pulls: one bundle per endpoint family (signed URL query "
                                   "strings stripped). The catalog lake is built only from these files.")),
                ("neon.harvestRun", run_id)])
            n_files = 0
            for d in sorted(p for p in sealed.rglob("*") if p.is_dir()):
                rel = d.relative_to(sealed).as_posix()
                fam = family_of(rel)
                avus = [("dc.description", SEALED_DIRS.get(rel, f"Sealed NEON API pulls, family {fam}.")),
                        ("neon.harvestRun", run_id)]
                if fam != "stage":
                    avus.append(("neon.api.family", fam))
                out.coll(f"{rc}/sealed/{rel}", "sealed", avus)
            smf = sealed / SEAL_MANIFEST
            if smf.exists():
                try:
                    sdoc = json.loads(smf.read_text())
                except ValueError:
                    sdoc = {}
                out.obj(f"{rc}/sealed/{SEAL_MANIFEST}", "sealed-file", [
                    ("dc.description", "Seal manifest: every sealed file with its family, lines, bytes and md5."),
                    ("dc.format", "application/json"), ("neon.harvestRun", run_id),
                    ("neon.sealVersion", str(sdoc.get("sealVersion", ""))),
                    ("neon.sealedAt", sdoc.get("sealedAt"), "ISO8601")])
            for f in sorted(p for p in sealed.rglob("*") if p.is_file()):
                rel = f.relative_to(sealed).as_posix()
                if f.name.endswith(".part") or rel in (SEAL_MANIFEST, "manifest.json"):
                    continue
                meta = idx.get(rel, {})
                fam = str(meta.get("family") or family_of(rel))
                rows = meta.get("lines", meta.get("rows", meta.get("rowCount")))
                md5 = meta.get("md5") or md5_file(f)
                common = [("neon.rowCount", str(rows if rows is not None else row_count(f)), "count"),
                          ("neon.md5", md5), ("dc.format", fmt_of(f.name)), ("neon.harvestRun", run_id)]
                if fam == "stage":
                    out.obj(f"{rc}/sealed/{rel}", "sealed-file", [
                        ("dc.description", f"Harvest stage file {f.name.split('.')[0]} (derived, not an API response)."),
                        *common])
                    n_files += 1
                    continue
                avus = [("neon.api.family", fam),
                        ("neon.api.endpoint", meta.get("endpoint") or ENDPOINTS.get(fam.split("/")[0], fam)),
                        ("neon.fetchedAt", meta.get("fetchedAt") or fetched.get(rel.split(".jsonl")[0])
                         or fetched.get(fam) or manifest.get("finishedAt"), "ISO8601"),
                        *common]
                if fam == "data_query" and "/" in rel:
                    avus.append(("neon.productCode", f.name.split(".jsonl")[0]))
                out.obj(f"{rc}/sealed/{rel}", "sealed-file", avus)
                n_files += 1
            info["sealed_files"] = n_files
        else:
            log.warning("no sealed/ for run %s: sealed targets skipped (run `catalog seal` first)", run_id)

    # ---- the lake
    L = f"{R}/ducklake"
    out.coll(L, "lake", [
        ("dc.description", ("Phase-1 catalog lake: DuckDB ducklake catalog neon_catalog.ducklake + Parquet under "
                           "data/ (relative DATA_PATH). Attach READ_ONLY with DATA_PATH set to the local data/ "
                           "and OVERRIDE_DATA_PATH true.")),
        ("mesa.ducklake.catalog", f"ducklake/{env.CATALOG_LAKE}", "relative path"),
        ("ducklake.dataPath", "data/", "relative path"),
        ("ducklake.reader", "duckdb 1.5.5 with LOAD ducklake; LOAD spatial"),
    ])
    cat = env.LAKE_DIR / env.CATALOG_LAKE
    if cat.exists():
        cat_avus: list[tuple] = [("mesa.ducklake.catalog", f"ducklake/{env.CATALOG_LAKE}", "relative path"),
                                 ("dc.format", "application/vnd.duckdb"),
                                 ("dc.description", "DuckLake metadata catalog (DuckDB file); upload it after data/.")]
        if facts and facts.get("snapshotId") is not None:
            cat_avus.append(("ducklake.snapshotId", str(facts["snapshotId"])))
        out.obj(f"{L}/{env.CATALOG_LAKE}", "lake-catalog", cat_avus)
    data = env.LAKE_DIR / "data"
    table_rows = (facts or {}).get("tables", {})
    if data.is_dir():
        out.coll(f"{L}/data", "lake", [("dc.description", "DuckLake Parquet data files, <schema>/<table>/*.parquet.")])
        import pyarrow.parquet as pq

        n_parquet = 0
        for d in sorted(p for p in data.rglob("*") if p.is_dir()):
            rel = d.relative_to(data).as_posix()
            parts = rel.split("/")
            if len(parts) == 1:
                out.coll(f"{L}/data/{rel}", "lake", [("dc.description", f"DuckLake schema {parts[0]}.")])
            elif len(parts) == 2:
                avus: list[tuple] = [("mesa.ducklake.table", parts[1]),
                                     ("dc.description", f"Parquet files of table {parts[0]}.{parts[1]}.")]
                if parts[1] in table_rows:
                    avus.append(("neon.rowCount", str(table_rows[parts[1]]), "count"))
                out.coll(f"{L}/data/{rel}", "lake-table", avus)
        for f in sorted(data.rglob("*.parquet")):
            rel = f.relative_to(data).as_posix()
            parts = rel.split("/")
            table = parts[1] if len(parts) >= 3 else parts[0]
            try:
                n = pq.read_metadata(f).num_rows
            except Exception as exc:  # noqa: BLE001
                log.warning("parquet metadata unreadable %s: %s", f, exc)
                n = None
            avus = [("mesa.ducklake.table", table), ("dc.format", "application/vnd.apache.parquet"),
                    ("ducklake.fileKind", "delete" if f.stem.endswith("-delete") else "data")]
            if n is not None:
                avus.append(("neon.rowCount", str(n), "count"))
            out.obj(f"{L}/data/{rel}", "lake-file", avus)
            n_parquet += 1
        info["lake_files"] = n_parquet

    out.coll(f"{R}/sites", "sites", [
        ("dc.description", ("Phase-2 site subtrees: sites/<SITE>/ holds every non-AOP basic-package file of that "
                           "site (raw/<productCode>/<YYYY-MM>/), its site lake, cards, curation and reports. "
                           "Part of this one MESA project (no nested mesa.enabled).")),
        ("mesa.ducklake.catalog", f"ducklake/{env.CATALOG_LAKE}", "relative path")])

    # ---- code trees that the mirror uploads
    for top, desc in CODE_DIRS.items():
        d = env.ROOT / top
        if not d.is_dir() or not any(p.is_file() for p in d.rglob("*")):
            continue
        out.coll(f"{R}/{top}", "code", [("dc.description", desc), ("neon.project", "neon-ducklake")])
        if top == "src":
            for sub in sorted(p for p in d.rglob("*") if p.is_dir() and "__pycache__" not in p.parts):
                rel = sub.relative_to(env.ROOT).as_posix()
                mod = ".".join(sub.relative_to(d).parts)
                out.coll(f"{R}/{rel}", "code", [("dc.description", f"Python package {mod}.")])
    info["rows"] = len(out.rows)
    return out.rows, info


def build_catalog_desired(run_id: str | None = None, *, con: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Replace the ``catalog`` scope of ``avu_desired`` with the Phase-1 vocabulary; returns a summary."""
    run_id = run_id or latest_run_id()
    rows, info = build_rows(run_id)
    con = con or state.connect()
    res = replace_desired(con, SCOPE, rows)
    info.update(res)
    info["units"] = con.execute("SELECT count(DISTINCT unit_id) FROM avu_desired WHERE scope=?",
                                (SCOPE,)).fetchone()[0]
    log.info("catalog desired: %s", info)
    return info
