# neon-ducklake internal contracts

Every module codes against these contracts. Changing one means updating this file first.
The approved design is in `/home/tswetnam/.claude/plans/use-the-neon-mcp-to-eventual-treehouse.md`.

## Shared modules (do not modify without coordination)
- `env.py`:
  - paths: `ROOT`, `IRODS_ROOT`, `RUNS_DIR`, `LAKE_DIR`, `SITES_DIR`, `WORK_DIR`, `MESA_CACHE_DIR`, `QUARANTINE_DIR`
  - `neon_bundle(roles)` returns `.client(role)` and `.token`. Roles: harvest, list, download, meta.
  - `mesa()` returns `Mesa(config, auth, session, clone())`.
  - `mesa_ducklake_client(m)` is the one `DuckLakeClient` per process. `close_mesa_ducklake_client()`.
  - `file_lock(name)`, `catalog_lock_holders()`, `repo_versions()`, `tool_versions()`, `write_json`, `utcnow`, `ACTOR="tswetnam"`, `NEON_ROR`.
- `state.py`:
  - `connect()` opens SQLite at `work/state.sqlite`.
  - Tables: `runs`, `requests`, `site_files`, `listings`, `avu_journal`, `batches`, `uploads`.
  - A module may add its own tables through `CREATE TABLE IF NOT EXISTS` in its own code, with names prefixed by the module (for example `stack_*`).
- `harvest.py`: the Phase-1 fetcher (running). Landings live at
  `api/runs/<runId>/raw/<family>/<key>.json.gz`. Families:
  - `products`, `sites`, `releases`, `manifests/<RELEASE>`, `release_products/<TAG>`, `release_sites/<TAG>`
  - `locations_sites`, `samples/supportedClasses`, `prototype/datasets`, `prototype_data/<uuid>`, `graphql/schema`
  - `taxonomy/<TYPE>__<offset>`, `documents_head/<spec>`
  - `location_hierarchy/<SITE>`, `locations/batch_NNNNN` (`{names, rows, errors}`), `location_history/<name>`
  - `data_query/<DP>__<SITE>__<start>_<end>[__<RELEASE>]` (the payload has `_request.pair{product,site,release,scope}` and `_request.window`)
  - `external/<DP>__<SITE>__<month>`
  - stage files: `api/runs/<runId>/stage/{pairs,zero_pairs,location_nodes}.json.gz`
  - Signed URL query strings are already stripped.

## Catalog lake: `ducklake/neon_catalog.ducklake`, schema `main` (owner: catalog_lake.py)
Tables are snake_case and columns keep NEON's camelCase. Downstream code **depends on** these tables and columns:

**`products`**
- productCode, productCodeLong, productName, productDescription, productAbstract
- productDesignDescription, productStudyDescription, productBasicDescription, productExpandedDescription
- productSensor, productRemarks, productStatus, productCategory
- productScienceTeam, productScienceTeamAbbr, productPublicationFormatType, productHasExpanded
- isAop BOOLEAN, themes VARCHAR[], keywords VARCHAR[], siteCount INT, firstMonth, lastMonth
- latestRelease, latestReleaseDoi

**`product_releases`**: productCode, release, generationDate TIMESTAMPTZ, url, productDoiUrl, productDoi (bare `10.48443/…`), productDoiGenerationDate

**`product_specs`**: productCode, specNumber, specId, specType, specSize BIGINT, specDescription, specUrl

**`sites`**:
- siteCode, siteName, siteDescription, siteType, siteLatitude DOUBLE, siteLongitude DOUBLE
- stateCode, stateName, domainCode, domainName, deimsId
- plus (from the `locations_sites` SITE record) siteElevation DOUBLE, siteUtmZone, siteUtmEasting, siteUtmNorthing
- siteProperties MAP(VARCHAR, VARCHAR) (the locationProperties with the "Value for " prefix removed)

**`taxonomy`**:
- taxonTypeCode, taxonID, acceptedTaxonID, scientificName, scientificNameAuthorship, taxonRank, vernacularName
- kingdom, phylum, class, "order", family, genus, specificEpithet, infraspecificEpithet
- nameAccordingToID, record JSON

**`locations`**:
- locationName, locationType, locationDescription, siteCode, domainCode
- locationDecimalLatitude, locationDecimalLongitude, locationElevation
- locationUtmEasting, locationUtmNorthing, locationUtmZone, locationUtmHemisphere
- alphaOrientation, betaOrientation, gammaOrientation, xOffset, yOffset, zOffset
- offsetLocationName, locationParent, activePeriods JSON, locationProperties JSON
- hierarchyDepth, hierarchyParent, hierarchySite

**`location_history`**:
- locationName, isCurrent BOOLEAN, locationStartDate TIMESTAMPTZ, locationEndDate TIMESTAMPTZ
- locationDecimalLatitude, locationDecimalLongitude, locationElevation
- xOffset, yOffset, zOffset, alphaOrientation, betaOrientation, gammaOrientation
- locationProperties JSON

**`availability_months`** (one row per product×site×month, current plus release-only):
- productCode, siteCode, month 'YYYY-MM'
- release ('RELEASE-YYYY' or 'PROVISIONAL'), isProvisional, isReleaseOnly
- availableDataUrl (NULL for release-only)

**`files`** (the non-AOP basic inventory):
- productCode (DP1 code, including DP0 support files), siteCode, domainCode, month
- release, releaseGenerationDate, packageType, packageGenerationDate
- name, size BIGINT, md5, crc32c, objectUrl (unsigned)
- kind, tableName, hor, ver, tmi, publicationTimestamp TIMESTAMP, ext
- isProvisional, isReleaseOnly, parseConsistent

**`external_data`**: productCode, siteCode, monthProbed, name, type, url

**`v_site_inventory`** (view; the Phase-2 planner):
- One row per non-AOP (productCode, siteCode, month).
- Columns: productName, productScienceTeamAbbr, release, isProvisional, isReleaseOnly, isExternal, fileCount, totalBytes.

## Site lake: `sites/<SITE>/ducklake/neon_<site>.ducklake`, schema `main` (owner: site/fetch.py + site/stack.py)

**`files`**:
- productCode, siteCode, domainCode, month, release, isProvisional, isReleaseOnly
- name, size, md5, crc32c, kind, tableName, hor, ver, tmi, publicationTimestamp, ext
- localRelPath (relative to the local root, e.g. `sites/SRER/raw/DP1.00002.001/2024-06/<name>`)
- irodsPath, dlMd5, md5Verified BOOLEAN

**`site_months`**: productCode, month, release, isProvisional, isReleaseOnly, releaseGenerationDate, fileCount, totalBytes

**`external_links`**: productCode, url, name, type, monthProbed, months VARCHAR[]

**`variables`**: productCode, tableName, fieldName, description, dataType, units, downloadPkg, pubFormat, primaryKey, categoricalCodeName (the latest definition), and `variables_history`

**`sensor_positions`**:
- productCode, month, sourceFile, "HOR.VER", sensorLocationID, sensorLocationDescription
- positionStartDateTime, positionEndDateTime, referenceLocationID, …offsets…
- sensorLatitude, sensorLongitude, sensorElevation, locationType

**Supporting tables**: `categorical_codes`, `validation`, `science_review_flags`, `eml_index`, `readme_index`

**`file_stats`** (one row per data file):
- sourceFile (file name), productCode, tableName
- rowCount, timeMin TIMESTAMPTZ, timeMax TIMESTAMPTZ, timeColumn
- qfFailPct DOUBLE, plotIDs VARCHAR[], plotCount, protocolVersions VARCHAR[], taxonCount

**`file_taxa`**: sourceFile, productCode, tableName, taxonID, scientificName, taxonRank, vernacularName, nRecords

**Stacked data tables**:
- Named `lower(productCode with '.'→'_') || '__' || lower(tableName)`.
- Typed NEON columns, plus siteID, domainID, month, release, publicationDate, sourceFile.
- IS tables also get horizontalPosition, verticalPosition, tmi.

**HDF5 tables**: `h5_attrs`, `h5_datasets`, `dp4_00200_001__nsae_dp04`

**`table_catalog`**: tableName, productCode, neonTable, nFiles, nRows, nColumns, firstMonth, lastMonth, timeColumn, inputSha256, builtAt

**`schema_drift`, `cast_errors`**

## Local data layout
`sites/<SITE>/raw/<productCode>/<YYYY-MM>/<NEON filename>` is mirrored to
`/iplant/home/tswetnam/neon-ducklake/sites/<SITE>/raw/<productCode>/<YYYY-MM>/<NEON filename>`.

## Desired AVUs (owner: avu/vocab.py for catalog; site/avus.py for sites) → applied by avu/bulk.py
SQLite table `avu_desired` in `work/state.sqlite`:
```
avu_desired(scope TEXT, unit_id TEXT, path TEXT, target_type TEXT CHECK IN ('collection','data_object'),
            attribute TEXT, value TEXT, unit TEXT DEFAULT '', level TEXT, generator TEXT,
            PRIMARY KEY (scope, path, attribute, value, unit))
```
- `scope` is 'catalog' or a site code.
- `unit_id` groups one work unit, usually one collection plus its direct objects. The key is the collection path.
- Builders **replace** a scope's rows transactionally (DELETE scope, then INSERT).
- Values are normalised: stripped, non-empty, UTF-8 ≤ 2000 bytes.
- The bulk writer computes `add = desired − current` and `remove = journal-owned − desired` per path. It journals to `avu_journal` and records MESA history in batches (`batches` table).

## Uploads (owner: mirror.py)
- `push_tree(items: list[tuple[local Path, irods path, expected_md5|None]], threads)` is idempotent.
- It skips a file when iRODS has a good replica (`replica_status == '1'`) with the same size and a checksum equal to the local MD5.
- Results go to the `uploads` table. For site raw files it also updates `site_files.up_status`, `irods_checksum` and `irods_path`.
- A `.ducklake` catalog file is always uploaded after its `data/**`.
