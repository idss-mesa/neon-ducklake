# Design plan

| | |
|---|---|
| **Status** | Phase 1 complete: the catalog lake is built, verified and mirrored to iRODS. Phase 2: the SRER dry run (`kill -9` during upload and tag, then idempotent re-runs) and the SRER sample run (all stages, `--sample 2`) passed. The site pipeline is generalised to all 81 NEON sites (`docs/SCALING.md`). |
| **Date** | 2026-09-28 |
| **Amendments** | [`SCALING.md`](SCALING.md) (81-site limits, orchestrator, eviction, measured throughput), [`CONTRACTS.md`](CONTRACTS.md) (module interfaces and their amendment rounds), [`RUNBOOK.md`](RUNBOOK.md) (operations). Where they differ from the plan below, they win. |

The main amendments since approval:

- **History batching.** Batches hold up to 250,000 rows instead of 60,000, and a guard refuses to record
  past 900 MESA snapshots. The plan's budget of "about 45 snapshots in total" assumed two sites; all 81
  sites need a few hundred.
- **All sites.** A multi-site orchestrator (`neon-ducklake run`) with resource classes, a disk guard and
  an `evict` stage that deletes raw files only after they are verified in iRODS. Local disk is smaller
  than the full archive.
- **Heterogeneity.** Download integrity is checked against the storage MD5, and a stale NEON MD5 is
  flagged rather than fatal. Lab files are stacked. Timestamp types come from each column's `pubFormat`.
  The site biome depends on `HABITAT`: aquatic sites get an ENVO water-body feature, and a site-level Opus
  curation pass decides the biome.
- **Portability.** Paths, the CyVerse user and the MESA install are configurable. The code dependencies
  are pinned to git commits rather than local editable paths.

The approved plan follows verbatim. Paths in it are the build host's.

---

# neon-ducklake — NEON API metadata lakehouse + full-AVU site subtrees

## Context

The user wants a new MESA DuckLake project, **neon-ducklake**, built in two phases:

1. **Phase 1, catalog.** Harvest the whole NEON Data API through neon-mcp (every product including
   AOP, every site, year and release) into a DuckLake of API metadata. No bulk data is downloaded.
2. **Phase 2, site build.** For one site at a time, download all non-AOP data for every year and
   give every collection and file full AVUs. AOP comes later.

**User decisions (2026-09-26):**
- Phase 1 also inventories every non-AOP file (basic package) at every site. AOP stays at
  availability grain.
- Phase 2 does SRER first, then HARV, basic package only, including PROVISIONAL months.
- Site data is a **subtree of the single MESA project** at
  `/iplant/home/tswetnam/neon-ducklake/sites/<SITE>/`: one AVU history, no nested `mesa.enabled`.

**User decisions (2026-09-27):**
- **Release-only data** (products or months found only in an earlier release): Phase 1 catalogs it,
  and Phase 2 also downloads it from its newest containing release. Those objects carry
  `neon.releaseOnly=true`.
  - SRER: 93 site-months / 1,298 files / 1.16 GB.
  - HARV: 124 site-months / 1,853 files / 1.66 GB.
  - Mostly DP1.00006.001 Precipitation, plus DP1.10045.001 and trimmed first months at HARV.
- **Full per-file AVU profile**, with paced writes.
- **Curated ontology terms** are auto-applied when both Opus replicates agree. Single-replicate
  terms go to `review.md`.

**Precedent to copy: `/home/tswetnam/neon-downloads`.**
- A local root with a DuckDB `ducklake`-extension lakehouse.
- An iRODS mirror enrolled through mesa-mcp's `handle_mesa_ducklake_init_project` in-process.
- `scripts/fetch_neon_locations.py` drives neon-mcp's `NeonClient` in-process.
- The iRODS subtree `/iplant/home/tswetnam/neon-downloads/2026-09-10/` is the AVU model at product
  and file level, including per-file `ncbitaxon.*` for only the taxa observed in that file.

**Verified live (2026-09-26/27):**
- neon-mcp has a token (2000 burst / 8 rps).
- mesa-mcp is authenticated as tswetnam, and its OLS tools never open the MESA catalog.
- CyVerse iRODS is **4.3.1**, so atomic metadata calls and parallel put work.
- Checksums are **plain MD5**. Replication to corral4 is asynchronous, so there are 2 replicas.
- The MESA catalog has 1 project and no lock holder.
- The keyring and D-Bus work in tmux.

**Scale:**

| Item | Count |
|---|---|
| Products | 202 (173 ACTIVE / 29 FUTURE; 155 with current data) |
| Sites | 81 |
| Releases | 6 |
| Current product×site×month cells | 281,819 (non-AOP 274,264) |
| Non-AOP product×site pairs | 4,291 |
| Taxa | 134,196 |
| Sample classes | 305 |
| Prototype datasets | 72 |
| Documents | 510 |
| Non-AOP file rows | ~3.5M |
| SRER (current, non-AOP) | 55 products / 3,634 site-months / 48,587 files / 81.3 GB |
| HARV (current, non-AOP) | 58 products / 4,391 site-months / 53,369 files / 78.0 GB |

- **Release-only data.** Some products and months exist only in earlier releases. Across the
  observatory, 4 of the 18 zero-availability products have earlier data:
  - DP1.00006: 4,517 site-months
  - DP1.20046: 398
  - DP1.10045: 438
  - DP1.20197: 271
- The current feed can't see these. Phase 1 therefore takes the **union of current availability and
  all 6 release manifests**.
- Phase-2 totals including release-only data:
  - SRER: 57 products, 3,727 site-months, 49,885 files, 82.5 GB.
  - HARV: 60 products, 4,515 site-months, 55,222 files, 79.6 GB.

## Architecture

```
/home/tswetnam/neon-ducklake/                 local root (mesa.local_root), git-tracked code
  pyproject.toml uv.lock README.md .gitignore scripts/with-secrets.sh
  src/neon_ducklake/  cli.py env.py preflight.py state.py harvest.py flatten.py catalog_lake.py enroll.py
                      mirror.py verify.py diff.py avu/{vocab,bulk,datacite,history}.py
                      site/{plan,fetch,stack,h5,terms,cards,curate,validate,site_lake}.py
  sql/  tests/  curation/{prompt_product.md,prompt_units.md,mcp-mesa-ols.json,generic/<DP>.validated.json}
  api/runs/<runId>/{raw/,sealed/,fetch_log.jsonl,fetch_manifest.json}  api/LATEST
  ducklake/neon_catalog.ducklake + data/                      Phase-1 catalog lake
  sites/<SITE>/raw/<DP>/<YYYY-MM>/<NEON filename>             Phase-2 data
  sites/<SITE>/ducklake/neon_<site>.ducklake + data/          Phase-2 per-site lake
  sites/<SITE>/{cards,curation,reports}/   ref/ontology_maps/*.parquet
  work/{state.sqlite (0600), mesa-cache/, quarantine/, locks/}   logs/     (never mirrored)
/iplant/home/tswetnam/neon-ducklake/          mesa.enabled=true, .mesa/ducklake/ (the one AVU history)
  mirror of everything except .venv, work/, logs/, api/runs/*/raw
```

**Lakes.**
- One catalog lake, plus one lake per site. Each has its own writer lock and snapshot history.
- `sql/attach_all.sql` attaches every lake read-only, with absolute `DATA_PATH` and
  `OVERRIDE_DATA_PATH true`.
- The MESA scope is the AVU history. `sites/<SITE>` collections get a `mesa.ducklake.catalog`
  pointer and never `mesa.enabled`.
- Every lake process sets `memory_limit='40GB'`, `threads=8` and a `temp_directory`.
- Only one `stack` runs at a time.

**Environment: a project uv venv, Python 3.11.**
- `duckdb==1.5.5`, pinned because the `ducklake` and `spatial` extensions for v1.5.5/aarch64 are in
  `~/.duckdb/extensions`. The code only `LOAD`s them.
- Also: `pyarrow`, `numpy`, `h5py`, `python-irodsclient==3.3.0`.
- Editable path deps on the **installed** code the MCP servers run: `~/.mesa/repos/mesa-mcp`
  (8fbaedf-dirty), `~/.mesa/repos/mesa-ducklake` (7bc143f) and `~/github/idss-mesa/neon-mcp`
  (a5949d2).
- `[tool.uv] constraint-dependencies` pins mcp 2.2.0, pydantic 2.13.5, structlog 26.1.0,
  httpx 0.28.1, anyio 4.15.1 and starlette 1.7.0, to match the running servers.
- The first `uv sync` needs PyPI, for the h5py and cp311 pyarrow wheels.
- `~/.mesa/.venv` is never modified.
- Preflight asserts each module's `__file__` and records SHAs and dirty state.

**Secrets: `scripts/with-secrets.sh <cmd>`.**
- Sources `~/.mesa/bin/mesa-keyring-common.sh`.
- Exports `NEON_TOKEN`, using the same keyring lookup as `neon-mcp-keyring`.
- Exports `MESA_MCP_IRODS__USER` and `MESA_MCP_IRODS__PASSWORD`, the names mesa-mcp's config
  reads. This is the same lookup `mesa-mcp-keyring` uses.
- Exports `MESA_MCP_DUCKLAKE__CATALOG_DSN=duckdb:///$HOME/.mesa/ducklake/catalog.duckdb`,
  `NEON_MCP_NEON__MAX_CONCURRENCY=8`, `NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S=300` and
  `NEON_MCP_SERVER__LOG_LEVEL=warning`.
- Never echoes a value. Exits non-zero if the keyring is locked.
- Every command calls `neon_mcp.logging.setup_logging("warning","stdio")`.
- Each role gets its own `NeonClient`, built from `create_server(load_config(flag_overrides=…))`:

| Role | Retries | Timeout | Behaviour |
|---|---|---|---|
| Harvest | `retries.max_attempts=2` | 180 s | Bisects on the first transport, 5xx or 413 error. This avoids ~25-minute internal retry stalls. |
| Listing | — | — | Separate client. |
| Download | `max_attempts=5` | — | Semaphore of 8. Its streaming bodies hold all 8 pool connections, so it gets a client of its own. |

- **Signed URLs** exist only in `work/state.sqlite` and are nulled after download. They never go
  into `api/`, a lake, logs or iRODS.

**Journal: `work/state.sqlite`** (WAL mode).
- Tables: runs, harvest ledger, cells, files (download, upload and tag status), avu_journal, batches.
- `fcntl` locks in `work/locks/`, plus a global `mesa-tag.lock` and an iRODS connection budget of
  ≤16 across all processes.

**Commands (`neon-ducklake …`):**
- `preflight`
- `catalog fetch|seal|build`
- `enroll`, `mirror`
- `tag catalog`
- `site plan|list|download|stack|terms|cards|curate|validate|upload|build-avus|tag <SITE>`
- `verify catalog|site <SITE>|mesa`
- `recover`, `diff --from --to`

`site plan <SITE>` is the read-only "query a single site" preview. It reads `v_site_inventory` and
shows products, months, files, bytes and what is excluded.

## Phase 1 — the catalog lake

**Fetch** (in-process client, as in `fetch_neon_locations.py`).
- MCP tools are not used, because of the 50 KB result budget and 200-row pages.
- Concurrency ≤8, app-level retry, and a circuit breaker.
- Landing is atomic, `.part` → `os.replace`.
- Resumable (`--resume`), with a `--pilot` mode.
- `seal` bundles the landings into one gz/jsonl per endpoint in `sealed/`. That is the only build
  input.

| Entity | Source | Requests |
|---|---|---|
| products (+ themes, keywords, specs, change logs, biorepository, per-site `availableMonths`/`availableReleases`/`availableDataUrls`) | REST `GET /products` (30 MB) | 1 |
| sites (+ per-site product lists) and SITE locations | `GET /sites`, `GET /locations/sites` | 2 |
| releases, artifacts, DOIs; per-release availability | `GET /releases`; 6 `MANIFEST_AVAILABLE` via `fetch_bytes`, MD5-checked | 7 |
| **per-release product and site records** (DOIs, change logs and specs as published) | `GET /releases/{tag}/products`, `/releases/{tag}/sites` × 6, 180 s timeout | 12 |
| taxonomy, 11 types, verbose | `GET /taxonomy`, paged by offset in parallel | ~274 |
| sample classes; prototype datasets + files | `/samples/supportedClasses`; `/prototype/datasets` + 72 `/prototype/data/{uuid}` | 74 |
| documents index | union of `specs[]` + `HEAD /documents/{spec}` | 510 |
| named locations **with history** | 81 × `GET /locations/{site}?hierarchy=true`, then GraphQL `findLocations` (the precedent query + `locationHistory`) in batches of 500, reusing `flatten_hierarchy` | ~842 (~35 min) |
| GraphQL schema | `__schema` introspection; fallback one `__type` per type | 1+ |
| **file inventory** (token) | `POST /data/query` per non-AOP product×site pair (full window, basic, `includeProvisional:true`). For **release-only** cells (in a manifest, not in the current view): `release:<newest release containing the cell>`. Bisect by year; biggest pairs first | ≈4,300 + release-only |
| external links (DP1.00033/00042 phenocam, DP1.00043 AERONET) | one single-cell `GET /data/{p}/{s}/{m}` per pair with 0 files; all availability months of that pair count as external | ≤124 |

**Spatial layers.** The 16 `neon_*` published layers (not API data) are copied from the
neon-downloads lake. The copy source is:

```
ATTACH '…/neon_spatial.ducklake' AS src (READ_ONLY, DATA_PATH '/home/tswetnam/neon-downloads/ducklake/data/', OVERRIDE_DATA_PATH true)
```

pinned `AT (VERSION => 90)`.

**Not harvested** (not enumerable, or out of scope):
- `/samples/view|download`
- document bodies
- `/data/package` ZIPs
- expanded-package and AOP file listings
- file lists of earlier release versions of re-released months

**Flatten.**
- Our own flattener in a `ProcessPoolExecutor`, writing Arrow/Parquet.
- Reuses `parse_neon_filename`. It does not use `normalize_query`, which drops `generationDate` and
  `domainCode`.
- DP0 files (categoricalCodes, validation) are attributed to their DP1 product.
- URL query strings are stripped.

**Build** (`catalog_lake.py`).
1. `chdir ducklake/`, `LOAD ducklake; LOAD spatial`.
2. `ATTACH 'ducklake:neon_catalog.ducklake' AS neon (DATA_PATH 'data/')`.
3. Options: zstd compression, `data_inlining_row_limit=0`, `require_commit_message=true`.
4. One `BEGIN…COMMIT`, i.e. **one DuckLake snapshot per build**.
   - Before COMMIT:
     `CALL ducklake_set_commit_message('neon','tswetnam','catalog build <runId>', extra_info => '<json counts>')`.
   - The build is a hash-keyed upsert, so refreshes produce real `ducklake_table_changes` diffs.
   - Key-uniqueness and no-`X-Goog-` assertions roll back on failure.
5. `CHECKPOINT; DETACH`. Fail if a `.wal` remains.

Naming: snake_case tables, NEON camelCase columns, logical keys.

| Group | Tables |
|---|---|
| Products | `products`, `product_themes`, `product_keywords`, `product_specs`, `documents`, `product_change_logs`, `product_releases` (DOIs), `product_biorepository` |
| Releases | `releases`, `release_artifacts`, `release_products`, `release_product_records`, `release_product_change_logs`, `release_site_products`, `release_manifest_months` (~1.1M) |
| Sites and locations | `sites`, `site_releases`, `site_products`, `site_locations`, `domains`, `locations` (~380k), `location_polygons`, `location_history`, 16 `neon_*` layers |
| Availability | `product_site` (with `inventoryScope` = current / release-only / excluded:AOP, and `inventoryStatus`), `availability_months` (current + release-only, with release / isProvisional / isReleaseOnly) |
| Other API | `taxonomy`, `sample_classes`, `prototype_datasets`, `prototype_files`, `prototype_locations`, `graphql_types`, `graphql_fields` |
| Inventory | `data_query_pairs`, `site_month_packages`, `files` (~3.5M: product/site/month/release/name keys; size, md5, crc32c, unsigned objectUrl, parsed kind/table/hor/ver/tmi/publicationTimestamp), `external_data` |
| Meta and views | `harvest_runs`, `harvest_requests`, `table_catalog`; `v_site_inventory` (Phase-2 planner), `v_pair_coverage`, `v_product_summary` |

**Enroll and mirror.**
1. Create the root collection.
2. `set_active_config(cfg)`, then `set_default_client(<our one DuckLakeClient>)`.
3. `handle_mesa_ducklake_init_project(InitProjectInput(irods_path=ROOT), auth_value=auth)`
   in-process. Never use the MCP tool, which would leave this session's server holding the
   single-writer lock.
4. `mirror`: code, then `sealed/` and manifests, then `ducklake/data/**`, then the catalog file
   **last**. Replica-aware skip and verify. Nothing is deleted remotely.
5. **Phase-1 AVUs are written by the bulk writer too**, after the dry run proves it. It is journaled,
   so volatile values can be reconciled, and it never hides failures. The targets are:
   - collections: root, `api/`, `api/runs/<runId>/`, `ducklake/`, `sites/`
   - files: sealed files and lake files
   - Vocabulary:

| Namespace | Attributes |
|---|---|
| `dc.*` | title, description, publisher NEON, creator, `dc.license=CC-BY-4.0` (unit URL, per neon-mcp `citing-neon-data.md`), source, date |
| `neon.*` | `neon.project`; volatile `neon.catalog.*` and `neon.api.*` counts |
| `mesa.*` | `mesa.project.kind=ducklake`, `mesa.ducklake.catalog`, `mesa.local_root` |
| `provenance.*` | `provenance.tool` (SHAs, duckdb/ducklake versions), `provenance.pipeline`, `provenance.derivedFrom` |

## Phase 2 — a site subtree (`site … SRER`, then HARV)

0. **plan.** `v_site_inventory` gives the site's current and release-only cells, excluding AOP.
1. **list.**
   - One `POST /data/query` per product (release-only cells use the release parameter), with year
     bisection.
   - Writes `files`, `site_months` and `external_links` (the Phase-1 per-pair link) into
     `neon_<site>.ducklake`.
   - Signed URLs are stored in the journal with `listed_at`. A product is re-listed after 6 days or
     on a GCS 400/403.
2. **download.** `stream_to_file(url, dest, token=None, max_bytes, expected_md5)`, plus our own
   per-file retry.
   - EML and readme have no size (`max_bytes`=64 MiB). They may be regenerated, so MD5 is soft for
     them: keep the file with `md5Verified=false`.
   - The journal keeps `dl_md5`, the local MD5.
   - Resume by journal plus size and `dl_md5`.
3. **stack** (neonUtilities `stackByTable` semantics). One CSV pass:
   - Group files by header line.
   - `read_csv(..., auto_detect=false, all-VARCHAR columns, filename=true)`, one read per group.
   - `UNION ALL BY NAME` into staging.
   - Cast-failure rates are computed on the staging table.
   - `CREATE … AS SELECT TRY_CAST`, with types from `variables`. A column with more than 0.1%
     failures stays VARCHAR and is logged.
   - Added columns: siteID, month, release, publicationDate, sourceFile, and HOR/VER/TMI for IS.
   - `uid` tables are deduplicated to the newest publication.
   - One transaction per (product, table), skipped when the fingerprint is unchanged.
   - Supporting tables:
     - `variables` (+ history)
     - `sensor_positions`: legacy headers aliased; lat/lon/elevation from reference + offsets;
       joined to `locations` and month-valid `location_history`
     - `categorical_codes`, `validation`, `science_review_flags`, `eml_index`, `readme_index`
     - `file_stats`, `file_taxa`, `schema_drift`, `cast_errors`
     - `h5_attrs`, `h5_datasets`, `dp4_00200_001__nsae_dp04` (h5py fluxes plus finalQF)
     - `table_catalog`
4. **terms.** Deterministic, cached in `ref/ontology_maps`.
   - **Taxa:** NEON `taxonID` → accepted `scientificName` → NCBITaxon, via
     `OLSClient.search_terms(name, ontology_id='ncbitaxon', size=25)`.
     - Filter on an exact label on our side; fall back to genus, then family.
     - Obsolescence is checked with raw OLS `is_obsolete`, using `validate_load.py`'s `ols_term`.
     - Unresolved taxa go to `unresolved_taxa.csv`.
   - **Units:** UO, from a seed-label dictionary plus exact matching plus one curated pass.
   - **NLCD→ENVO:** a fixed dictionary, already checked in OLS.
   - **Site biome:** curated.
5. **cards → curate → validate.** Product-level ontology AVUs, generic per product, stored in
   `curation/generic/`. HARV only curates products SRER didn't have.
   - **Cards** come from the product record plus real column profiles.
   - **Runs:** `env -i HOME PATH DBUS_SESSION_BUS_ADDRESS claude -p --model claude-opus-5-5 --strict-mcp-config --mcp-config curation/mcp-mesa-ols.json --allowedTools <3 OLS tools>`.
     - The MCP config runs `~/.mesa/.venv/bin/mesa-mcp --transport stdio` directly, with
       `MESA_MCP_DUCKLAKE__CATALOG_DSN=""` and an anonymous iRODS user. So the run gets no secrets
       and no catalog access.
     - This is the neon-avu-eval protocol; Opus 5.5 was best.
   - 2 replicates, 4 in parallel. Traces are kept for groundedness.
   - **Scope:** organism group, measured property, environmental material, method/device, data
     type. No species, units or biome; those come from the data.
   - **Validator** (generalises `validate_load.py`): resolves in OLS, not obsolete, label match,
     active OBO Foundry prefix, canonical rebuild with
     `ontology_annotations_to_avus`/`_label_to_snake`, column exists, not a duplicate, ≤25 per
     product.
   - **Outcome:** valid in both replicates → auto-applied. Otherwise → `review.md`, applied only
     with `--accept`. (User decision.)
6. **upload** (`push_tree`, one work unit per leaf collection, iRODS connections ≤16 in total).
   - One replica-aware GenQuery per collection: group by `DataObject.id`. An object is present when
     it has a `replica_status=='1'` replica whose checksum equals `dl_md5`.
   - Otherwise put with checksum registration: `num_threads=1` below 512 MiB, 4 on a 2-slot lane.
     Verify afterwards.
   - `LOCKED_DATA_OBJECT_ACCESS`, `SYS_REPLICA_INACCESSIBLE` and `SYS_MAX_CONNECT_COUNT_EXCEEDED`
     trigger a backoff and retry; the last also halves C.
   - C comes from a canary on ~300 files at C=4, 8 and 16. The precedent measured 2.1 s per file
     sequentially.
   - After stack and terms: `CHECKPOINT; DETACH` the site lake, fail if a `.wal` remains, upload
     `data/**`, then the `.ducklake` file last. Cards, curation, reports and `ref/` also go up.
7. **build-avus → tag** (see below).

### AVU vocabulary

`neon.<lowerCamelCase>` is the only casing. Values are normalised like `add_avu_to_irods`: strip
whitespace, drop empties, cap at 2,000 bytes on a sentence boundary. Long text stays in the lakes.

| Level | AVUs |
|---|---|
| `sites/<SITE>` (~90) | siteCode, siteName, siteType, domainCode/Name, stateCode, sitePoint (`WGS84 lat lon`), siteElevation (`meter`), deimsId (`URL`), typeEco, package=basic, excludedTeam=AOP, counts, temporalCoverage (`ISO8601 interval`), downloadedWith, downloadDate; `mesa.ducklake.catalog`; `envo.<biome>`, `envo.<nlcd class>` (≥1% area); canonical `datacite.*` |
| containers `sites/<SITE>/{raw,ducklake,cards,curation,reports}`, `ref/` | `dc.description`, `dc.type`, `provenance.generator` |
| `raw/<DP>` product (~130–180 + taxa) | productCode, productName, productScienceTeam, status, theme×n, keyword×n; `neon.release`×n (unit `doi:10.48443/…`); months, provisionalMonths, releaseOnlyMonths (ISO ranges); counts; temporalCoverage; `neon.table`×n; `mesa.ducklake.table`×n; externalDataUrl; `uo.<unit>`=`<table>.<field>` (unit UO CURIE); `ncbitaxon.<label>` for the union of taxa (value = vernacular name); accepted curated terms; canonical `datacite.*` |
| `<YYYY-MM>` month (~12) | month, release (+DOI unit, or PROVISIONAL with unit ""), generationDate, fileCount, totalBytes, rowCount, temporalCoverage; IS: sensorCount; OS: plotCount, taxaCount, protocolVersion×n |
| data object | see the file profiles below |
| site-lake files, cards, reports, ref files | `mesa.ducklake.table` / `mesa.ducklake.catalog`, `neon.rowCount`, `dc.format`, `neon.productCode` (where it applies), `provenance.*` |

**Data-object profiles:**

| Kind | ≈ AVUs | Attributes |
|---|---|---|
| core (every file) | — | productCode, siteCode, domainCode, month, release (+DOI), package, fileKind, publicationDate, `dc.source` (API file URL) |
| IS data | ~22 | core + table, hor, ver, tmi, namedLocation, locationDescription, sensorPoint, sensorElevation, zOffset, rowCount, temporalCoverage, finalQFFailPercent, `mesa.ducklake.table` |
| OS data | ~33 | core + table, rowCount, temporalCoverage, protocolVersion×n, plotCount, plotID×n (≤50), taxaCount, **`ncbitaxon.*` for only the taxa observed in the file**, `neon.taxon` (unresolved) |
| HDF5 | — | core + table=nsae, temporalCoverage, rowCount, towerPoint |
| supporting | ~9 | core only |
| flagged EML/readme | — | adds `neon.md5Verified=false` |

**DataCite** (site and product collections).
- `datacite_to_avus(record, naming="canonical")` after `DataCiteMetadata` validation.
- `identifier` is the collection's CyVerse DE URL. Release DOIs go in as `relatedIdentifier`
  IsDerivedFrom.
- Creator is NEON with **ROR `https://ror.org/04j43p132`**. Preflight re-resolves it; do not copy
  the precedent's wrong `01a7c8x72`.
- Citations come from `neon_mcp.resources.citation_templates()` (`.released`/`.provisional`) filled
  with `string.Template`.

**Release-only objects.** Objects, months and products present only in an earlier release carry:
- `neon.release=<that release>`, unit = that release's product DOI
- `neon.releaseOnly=true`

**Volume:** the full profile (chosen) is ≈1.0M AVU rows for SRER plus ≈1.1M for HARV, written
through paced atomic calls.

### Bulk AVU writer and history (`avu/bulk.py`, `avu/history.py`)

The bulk writer replaces MCP tools for every AVU write. Per-target `ds_add_avus` would mean one
snapshot per target, about 102k, against `_ENSURE_CACHED_LIMIT = 1000`.

**Apply** (no MESA lock needed).
- **Work units:**
  - each month collection with its objects
  - each product collection
  - one unit for `sites/<SITE>` plus its non-raw subtrees
  - the Phase-1 targets
- **Per unit:**
  1. One GenQuery with equality on `Collection.name`, replica-aware.
  2. `add = desired − current`.
  3. `remove = journal-owned − desired`. `ipc*`, `mesa.enabled` and foreign AVUs are never touched.
  4. Journal the rows as `intent`.
  5. `apply_atomic_operations(Model, path, *removes, *adds)`: ≤500 ops per call, C_meta=8, paced by a
     token bucket of ~20 calls/s. Stop if p95 is over 2 s.
  6. On failure, re-read and retry once, then fall back to per-AVU calls.
  7. Mark rows `applied` with `applied_at = datetime.now(UTC)`. It must be timezone-aware; a naive
     time shifts history by 6–7 h.

**Record** (under `mesa-tag.lock`, ~12–15 min per site).
- One `DuckLakeClient(catalog_dsn, irods_session=None, cache_dir=work/mesa-cache, cache_cap_bytes=0)`
  per process, set as the default client. There is never a second `duckdb.connect` on
  `catalog.duckdb`.
- Run recovery first (below).
- Applied rows are grouped into batches of ≤60,000 rows, in (product, month) order, without splitting
  an object. Each batch is one call:
  `record_changes(pid, "tswetnam", [AvuChange(..., op=add|delete (PRC remove→delete), actor="tswetnam", ts=applied_at, source="neon-ducklake:<step>")], note="neon-ducklake <scope> batch=<uuid> k/K", session=main)`
- **Budget:** about 17–20 snapshots per site plus about 3 for Phase 1, roughly **45 in total**.
  Staging runs at about 1,380 rows/s.

**Recovery.**
1. Snapshots are listed with `client._get_catalog().list_snapshots(pid, 1000, include_pending=True)`
   and `.list_pending_pushes()`. This is the only private-API use, and it lives in one helper.
2. A batch counts as recorded only when its `batch=` snapshot has
   `parquet_file ∉ {'pending','failed'}`.
3. Failed, orphaned or partial Parquet files are moved to `work/quarantine/` **before**
   `recover_pending_pushes` runs. Local reads glob every cached Parquet file, so leaving them would
   double-count or crash reads.
4. The affected batches are then re-recorded.

## Reused code (installed copies)

| Need | Function |
|---|---|
| NEON HTTP | `neon_mcp.server.create_server(cfg, transport="stdio").client`<br>`NeonClient.get_json` / `post_json` / `graphql` / `head` / `fetch_bytes` / `stream_to_file`<br>`neon_mcp.neon.auth.resolve_token`<br>`neon_mcp.config.load_config(flag_overrides=)`<br>`neon_mcp.logging.setup_logging` |
| NEON parsing | `neon_mcp.neon.filenames.parse_neon_filename`<br>`neon_mcp.neon.months.MonthRanges`<br>`neon_mcp.projections.releases.bare_doi`<br>`neon_mcp.projections.locations.flatten_hierarchy`<br>`neon_mcp.neon.downloads.BASENAME_RE`<br>`neon_mcp.resources.citation_templates` |
| MESA / iRODS | `mesa_mcp.config.load_config` / `set_active_config`<br>`auth.extract.extract_from_env`<br>`irods.client_pool.default_pool` + `session.clone()`<br>`ducklake.client.set_default_client`<br>`ducklake.tools.init_project.handle_mesa_ducklake_init_project`<br>`irods._avu_helpers.add_avu_to_irods` normalisation rules |
| Ontology / DataCite | `mesa_mcp.ols.transform.ontology_annotations_to_avus` + `RESERVED_PREFIXES`<br>`mesa_mcp.ols.client._label_to_snake` / `get_ols_client`<br>`mesa_mcp.datacite.schema.DataCiteMetadata`<br>`datacite.transform.datacite_to_avus(naming="canonical")` / `avus_to_datacite` |
| History | `mesa_ducklake.DuckLakeClient` (`find_project_by_path`, `record_changes`, `recover_pending_pushes`, `get_avus`, `get_avus_as_of`, `get_history`, `diff`, `close`)<br>`AvuChange` |
| Patterns | `neon-downloads/scripts/{fetch_neon_locations,build_ducklake,mesa_enroll_project}.py`<br>`neon-avu-eval/scripts/validate_load.py`<br>`neon-avu-eval/prompt.md` |

**Not used:**
- `neon_download_files`
- `ds_add_avus` / `mesa_avu_apply_term`, which write one snapshot per target
- the `mesa-ducklake record` CLI
- mesa-anyjev `apply`

## Run order and estimates

1. **P0 preflight** (read-only). Checks:
   - `uv sync --locked`, and that `h5py`, `pyarrow` and `duckdb` import
   - the token, and a real `/data/query`
   - clock skew
   - iRODS auth and version
   - quota ("unknown", so it asks you)
   - no `/proc` holder of `catalog.duckdb`, and no pending pushes or failed snapshots (checked
     through the client)
   - the ROR ID
   - disk
   - module paths
   - no `mesa.enabled` below the root
   - stale `.part` files
2. **P1 fetch → seal → build → enroll → mirror.** About 1.5–2.5 h: inventory 35–75 min, locations
   with history about 35 min. Commit to git.
3. **Dry run** on SRER DP1.00002.001 (one released and one PROVISIONAL month) through every Phase-2
   stage, plus the bulk writer.
   - `kill -9` during the upload and during the tag, then re-run.
   - A second re-run must do **0** downloads, puts, atomic calls and snapshots.
   - `get_avus_as_of` before and after returns ∅ and then the desired set.
4. **`tag catalog`**, the Phase-1 AVUs.
5. **SRER**, about 5.5–8 h plus review:
   - plan → list → download (0.5–1 h) → stack + h5 (0.5–1 h)
   - terms → cards/curate/validate (1.5–2.5 h)
   - upload (1.8–3.5 h)
   - build-avus → tag (apply 45–60 min; record 12–15 min under lock)
   - verify
6. **HARV**, same, ~10% larger.
   - Its curate starts after SRER's validate, and only covers new products.
   - Its list and download may overlap SRER's upload.
   - Two record steps never overlap.

## Verification

**Phase 1** (`verify catalog`, non-zero on failure; drift allowed only upward ≤0.5%). Expected counts:

| Table | Expected |
|---|---|
| products | 202, by status and team |
| sites / domains | 81 / 20 |
| `product_site` | 6,112 + release-only pairs |
| `availability_months` | 281,819 + release-only |
| releases / `release_products` | 6 / 683 |
| `release_manifest_months` | Σ of the manifest months |
| `release_product_records` | per-release product counts |
| taxonomy | 134,196, equal to the page totals |
| sample classes / prototype datasets | 305 / 72 |
| documents | 510, with HEAD status recorded |
| `prototype_files` | Σ over the 72 `/prototype/data` |
| `product_change_logs`, `product_biorepository` | Σ over the arrays |
| `site_products` | ≡ `product_site` |
| locations, `location_history` | counts reported |

Other Phase-1 checks:
- `data_query_pairs` are all ok, and every final sub-request is 200.
- Every (product, site, month) in current availability **or any manifest** has files, is external,
  or is listed as an exception.
- SRER and HARV inventory equals fresh `neon_list_files` summaries.
- `parseConsistent` is 100%. There are 0 signed URLs (SQL over the lake, plus `zgrep` over
  `sealed/`).
- Exactly one new snapshot. On the first build, tables are absent at n-1; refreshes compare with
  n-1.
- iRODS has one good replica per object, with matching size and checksum. The catalog file is
  uploaded last. A read-back ATTACH works.
- MESA: `get_avus` equals iRODS for every Phase-1 target, after removing `ipc*`, `mesa.enabled` and
  non-journal triples. `pending_pushes = 0`.

**Phase 2** (`verify site <SITE>`):
- **Listing:** equals a fresh listing. Differences from `v_site_inventory` are reported through
  `diff`.
- **Local files:** `dl_md5` = NEON md5 for 100%, except the flagged EML/readme count.
- **iRODS:** distinct object count and Σsize, with one good replica each, equal the journal. The
  checksum equals `dl_md5` for 100%.
- **Independent AVU completeness:**
  - 0 objects missing any core attribute.
  - Every collection under the subtree has ≥1 `neon.*` or `mesa.*` AVU.
  - Per-`fileKind` counts carrying AVUs equal the listing.
  - `desired − current = ∅`, re-read one collection at a time.
- **Stacking:**
  - Σ `file_stats.row_count` equals the CSV line counts (spot-check `wc -l`).
  - ≥95% of columns are typed.
  - ≥99% of IS files resolve to a sensor position.
  - The taxa resolution rate is reported.
- **MESA:**
  - Journal rows marked recorded = Σ rows in our snapshots.
  - `get_avus` equals iRODS for 50 random paths per level, after the exclusions above.
  - 0 pending or failed; snapshots ≤200; every snapshot has actor `tswetnam`.
  - Row sources are `neon-ducklake:*`.
- **Ontology:**
  - Every `ncbitaxon.*`, `envo.*`, `uo.*` and curated CURIE re-resolves as not obsolete.
  - Each attribute equals `<prefix>.<_label_to_snake(label)>`.
  - DataCite round-trips.
- **Unit tests** (hermetic, with pilot fixtures): flattener, filename→AVU, diff and ownership,
  batching and recovery matching, `tz`, taxon normalisation, stack on tiny CSVs.

## To confirm

- **CyVerse quota: unlimited** (confirmed by the user 2026-09-27). Local scratch is the 3.7 TB root
  NVMe (~3.3 TB free). Preflight records the quota as confirmed and does not block.
- **MESA lock windows.** About 12–15 min per site. If another mesa-mcp session holds
  `catalog.duckdb`, I'll ask you to close it.
- **Git.** Local `git init`, Conventional Commits. Creating `idss-mesa/neon-ducklake` on GitHub
  needs your OK first.
- **Later, out of scope now:** AOP, a scheduled monthly refresh, the upstream mesa-ducklake Arrow
  insert, and a superseded-file policy (kept and reported for now).
