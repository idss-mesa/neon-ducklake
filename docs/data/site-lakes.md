---
type: Dataset
title: "Site lakes (neon_<site>.ducklake)"
description: "Dataset card for the Phase-2 site lakes: stacked, typed NEON data tables plus file, variable, sensor-position and taxa tables for one NEON site each. Two sites are published (SRER and KONZ), both partial sample runs, readable anonymously over HTTPS."
tags:
  - neon-ducklake
  - dataset
  - ducklake
  - neon
  - site-lake
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: srer-lake
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/SRER/ducklake/neon_srer.ducklake"
    title: "SRER site lake (queried read-only on 2026-11-24)"
    author: "team:idss-mesa"
  - id: konz-lake
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/KONZ/ducklake/CURRENT"
    title: "KONZ site lake pointer and catalogs (queried read-only on 2026-11-24)"
    author: "team:idss-mesa"
  - id: site-lake-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/site_lake.py"
    title: "site_lake.py: contracted site-lake tables"
    author: "team:idss-mesa"
  - id: stack-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/stack.py"
    title: "stack.py: stackByTable semantics"
    author: "team:idss-mesa"
  - id: upload-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/upload.py"
    title: "upload.py: versioned site-lake catalogs and the CURRENT pointer"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-02-15T00:00:00Z"
---

# Site lakes

A site lake holds the NEON data of one field site, stacked into typed tables with neonUtilities
`stackByTable` semantics, plus tables that describe the files, variables, sensor positions and
taxa. Each site lake is a DuckLake in DuckDB's `ducklake` extension format under
`sites/<SITE>/ducklake/` in the published collection.

!!! warning "Two sites, both partial"

    On 2026-11-24 the Data Store held site lakes for **SRER** and **KONZ** only. Both are sample
    runs, not full sites: the SRER lake holds 1,274 of the 49,885 files the catalog inventory lists
    for SRER, and the KONZ lake holds 1,241 of 54,157. Most products have one or two months. Do
    not use these lakes to answer "all data for site X"; use the
    [catalog lake](catalog-lake.md) to see what exists, and the lake to read what was stacked.

All numbers on this page come from queries run read-only over HTTPS on 2026-11-24.

## At a glance

| | SRER | KONZ |
|---|---|---|
| Site | Santa Rita Experimental Range NEON, D14, CORE, Terrestrial | Konza Prairie Biological Station NEON, D06, CORE, Terrestrial |
| Catalog file | `sites/SRER/ducklake/neon_srer.ducklake` (10,235,904 bytes) | `sites/KONZ/ducklake/CURRENT` points to `catalogs/neon_konz.0000000919-66a0ae9483-532c94029a3a.ducklake` (12,857,344 bytes); `neon_konz.ducklake` is a copy with the same md5 |
| How it was built | whole-site stages only (commit messages `site list`, `site download`, `stack SRER ...`), 2026-09-27 14:58 to 2026-09-28 04:07 UTC; no versioned catalogs | whole-site stages (snapshots 1 to 705, from 2026-09-28 02:48 UTC), then a streaming run (`stream stack ...`, snapshots 706 to 919, 2026-09-29); versioned catalogs behind `CURRENT` |
| Snapshots | 322 (0 to 321); last at 2026-09-28 04:07:22 UTC | 920 (0 to 919); last at 2026-09-29 16:15:26 UTC |
| Tables | 163: 146 stacked tables (`dp*`) and 17 others | 166: 145 stacked tables and 21 others |
| `files` rows | 1,274 (catalog inventory for SRER: 49,885) | 1,241 (catalog inventory for KONZ: 54,157) |
| `site_months` rows | 120 product-months, 2013-11 to 2026-09, 46 PROVISIONAL | 112 product-months, 2014-06 to 2026-09, 46 PROVISIONAL |
| Verify report | `sites/SRER/reports/verify.json`, 2026-09-28: 12 pass, 1 info | no `verify.json` in `sites/KONZ/reports/` |
| Raw files | `sites/SRER/raw/<DP>/<YYYY-MM>/` (57 product folders) | `sites/KONZ/raw/<DP>/<YYYY-MM>/` (58 product folders) |

The SRER `verify.json` describes the listing as "inventory 3727 site-months (328 external,
link-only; 3279 outside the listed windows, e.g. a --sample run), lake 120".

## How to read it

### HTTPS, read-only, no account

```sql
INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;
SET TimeZone = 'UTC';
ATTACH 'ducklake:https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/SRER/ducklake/neon_srer.ducklake'
    AS neon_srer (READ_ONLY,
                  DATA_PATH 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/SRER/ducklake/data/',
                  OVERRIDE_DATA_PATH true);
ATTACH 'ducklake:https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/KONZ/ducklake/neon_konz.ducklake'
    AS neon_konz (READ_ONLY,
                  DATA_PATH 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/KONZ/ducklake/data/',
                  OVERRIDE_DATA_PATH true);
SELECT table_catalog, count(*) AS n_tables
FROM information_schema.tables
WHERE table_catalog IN ('neon_srer', 'neon_konz')
GROUP BY 1 ORDER BY 1;
```

```text
table_catalog | n_tables
neon_konz | 166
neon_srer | 163
```

Measured times on 2026-11-24 with DuckDB 1.5.5: attaching both lakes took about 6 s, and queries
on one stacked table or on `files` took 1 to 4 s. A scan of a table stored as many tiny Parquet
files is slow: `table_catalog` keeps one file per row, and a `GROUP BY` over the SRER
`table_catalog` (446 files) took 82 s in one run and 17 s in another. For repeated or wide scans, copy the lake first.

`DATA_PATH` plus `OVERRIDE_DATA_PATH true` is required: the writers recorded an absolute local
path (for example `/home/.../sites/SRER/ducklake/data/`) as the lake's data path.

### Which catalog file to attach (KONZ and future streamed sites)

A streamed site keeps one immutable catalog per push in `catalogs/` and a JSON pointer,
`CURRENT`, to the current one. The canonical `neon_<site>.ducklake` is refreshed after the
pointer, for readers that predate versioning. The pipeline's own readers take the pointer's
target when its md5 matches, else the newest versioned catalog that verifies, else the canonical
copy. Read the pointer with DuckDB:

```sql
SELECT catalog, md5, snapshotId, pushedAt
FROM read_json('https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/KONZ/ducklake/CURRENT');
```

```text
catalog | md5 | snapshotId | pushedAt
neon_konz.0000000919-66a0ae9483-532c94029a3a.ducklake | 532c94029a3ac5765425c63de1f97341 | 919 | 2026-09-29 16:16:52.454961
```

On 2026-11-24 the canonical `neon_konz.ducklake` had the same md5 (`532c9402...`), so attaching
either gives snapshot 919. To pin a version, attach
`sites/KONZ/ducklake/catalogs/<catalog>` with the same `DATA_PATH`.

### Local copy

Copy the site's `ducklake/` folder (the `.ducklake` file and `data/` together) with `gocmd get`
or `iget -r`, then attach the copy with its local `data/` as `DATA_PATH`, as `sql/attach_all.sql`
does. `sql/attach_all.sql` also lists HARV, which has no lake on the Data Store; that ATTACH fails.
The local-copy route was not re-run for this page.

### Other access paths

- **neon-mcp** does not read site lakes; it calls the live NEON API.
- **mesa-mcp** has no tool that reads these lakes. Its `mesa_ducklake_*` tools read AVU history.
- **The MESA Quack endpoint** serves a MESA history catalog, not site lakes. None is documented
  for this project.

## Tables

### Stacked data tables

One table per product and NEON table, named
`lower(productCode with '.' replaced by '_') || '__' || lower(tableName)`, for example
`dp1_00002_001__saat_30min` (single aspirated air temperature, 30-minute) or
`dp1_10003_001__brd_countdata` (breeding landbird point counts). Columns are NEON's, typed from
the product's `variables` file (a column with more than 0.1% cast failures stays VARCHAR and is
logged in `cast_errors`), plus `siteID`, `domainID`, `month`, `release`, `publicationDate` and
`sourceFile`. Instrumented-system (IS) tables also get `horizontalPosition`,
`verticalPosition` and `tmi`. Tables with a `uid` keep the newest publication per `uid`. Lab
files go to `<dp>__lab_<table>`; the DP4.00200.001 eddy-covariance HDF5 files go to
`dp4_00200_001__nsae_dp04` plus `h5_attrs`, `h5_datasets` and (KONZ) `h5_objdesc`.

`table_catalog` lists every stacked table with its product, NEON table name, kind, file count,
row count and month range:

```sql
SELECT kind, count(*) AS n_tables, sum(nRows) AS n_rows
FROM neon_srer.table_catalog
GROUP BY 1 ORDER BY 1;
```

```text
kind | n_tables | n_rows
data | 144 | 12731568
h5 | 1 | 7344
lab | 1 | 2
support | 300 | 28251
```

(`support` rows describe per-product supporting tables such as `variables`; one table can have a
row per product.)

Three example schemas:

`dp1_00002_001__saat_30min`: `startDateTime` TIMESTAMP WITH TIME ZONE, `endDateTime` TIMESTAMP WITH TIME ZONE, `tempSingleMean` DOUBLE, `tempSingleMinimum` DOUBLE, `tempSingleMaximum` DOUBLE, `tempSingleVariance` DOUBLE, `tempSingleNumPts` DOUBLE, `tempSingleExpUncert` DOUBLE, `tempSingleStdErMean` DOUBLE, `finalQF` BIGINT, `siteID` VARCHAR, `domainID` VARCHAR, `month` VARCHAR, `release` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `sourceFile` VARCHAR, `horizontalPosition` VARCHAR, `verticalPosition` VARCHAR, `tmi` VARCHAR.

`dp1_10003_001__brd_countdata`: `uid` VARCHAR, `namedLocation` VARCHAR, `domainID` VARCHAR, `siteID` VARCHAR, `plotID` VARCHAR, `plotType` VARCHAR, `pointID` VARCHAR, `startDate` TIMESTAMP WITH TIME ZONE, `boutNumber` BIGINT, `eventID` VARCHAR, `pointCountMinute` BIGINT, `targetTaxaPresent` VARCHAR, `taxonID` VARCHAR, `scientificName` VARCHAR, `taxonRank` VARCHAR, `vernacularName` VARCHAR, `observerDistance` DOUBLE, `detectionMethod` VARCHAR, `visualConfirmation` VARCHAR, `sexOrAge` VARCHAR, `clusterSize` BIGINT, `clusterCode` VARCHAR, `identifiedBy` VARCHAR, `identificationHistoryID` VARCHAR, `month` VARCHAR, `release` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `sourceFile` VARCHAR.

`dp4_00200_001__nsae_dp04`: `siteID` VARCHAR, `domainID` VARCHAR, `month` VARCHAR, `release` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `sourceFile` VARCHAR, `timeBgn` TIMESTAMP WITH TIME ZONE, `timeEnd` TIMESTAMP WITH TIME ZONE, `fluxCo2Turb` DOUBLE, `qfFinlCo2Turb` BIGINT, `fluxCo2Nsae` DOUBLE, `qfFinlCo2Nsae` BIGINT, `fluxCo2Stor` DOUBLE, `qfFinlCo2Stor` BIGINT, `fluxH2oTurb` DOUBLE, `qfFinlH2oTurb` BIGINT, `fluxH2oNsae` DOUBLE, `qfFinlH2oNsae` BIGINT, `fluxH2oStor` DOUBLE, `qfFinlH2oStor` BIGINT, `fluxTempTurb` DOUBLE, `qfFinlTempTurb` BIGINT, `fluxTempNsae` DOUBLE, `qfFinlTempNsae` BIGINT, `fluxTempStor` DOUBLE, `qfFinlTempStor` BIGINT, `footAngZaxsErth` DOUBLE, `footDistReso` DOUBLE, `footVeloYaxsHorSd` DOUBLE, `footVeloZaxsHorSd` DOUBLE, `footVeloFric` DOUBLE, `footDistZaxsMeasDisp` DOUBLE, `footDistZaxsRgh` DOUBLE, `footDistObkv` DOUBLE, `footParaStbl` DOUBLE, `footDistZaxsAbl` DOUBLE, `footDistXaxs90` DOUBLE, `footDistXaxsMax` DOUBLE, `footDistYaxs90` DOUBLE, `qfFinlFoot` BIGINT.

### Other tables

Column names and types below come from `information_schema.columns` of the KONZ lake.

#### `files`

One row per NEON file downloaded for the site, with download checks and its Data Store path (`irodsPath`). Key `productCode, month, name`. Rows: SRER 1,274, KONZ 1,241.

Columns: `productCode` VARCHAR, `siteCode` VARCHAR, `domainCode` VARCHAR, `month` VARCHAR, `release` VARCHAR, `isProvisional` BOOLEAN, `isReleaseOnly` BOOLEAN, `name` VARCHAR, `size` BIGINT, `md5` VARCHAR, `crc32c` VARCHAR, `kind` VARCHAR, `tableName` VARCHAR, `hor` VARCHAR, `ver` VARCHAR, `tmi` VARCHAR, `publicationTimestamp` TIMESTAMP, `ext` VARCHAR, `localRelPath` VARCHAR, `irodsPath` VARCHAR, `dlMd5` VARCHAR, `md5Verified` BOOLEAN, `storageMd5` VARCHAR, `neonMd5` VARCHAR, `transferVerified` BOOLEAN.

#### `site_months`

One row per product x month the site lake holds, with release and file totals. Key `productCode, month`. Rows: SRER 120, KONZ 112.

Columns: `productCode` VARCHAR, `month` VARCHAR, `release` VARCHAR, `isProvisional` BOOLEAN, `isReleaseOnly` BOOLEAN, `releaseGenerationDate` TIMESTAMP WITH TIME ZONE, `packageGenerationDate` TIMESTAMP WITH TIME ZONE, `fileCount` BIGINT, `totalBytes` BIGINT, `isExternal` BOOLEAN.

#### `external_links`

Links of products hosted outside NEON. Key `productCode, url`. Rows: SRER 3, KONZ 3.

Columns: `productCode` VARCHAR, `url` VARCHAR, `name` VARCHAR, `type` VARCHAR, `monthProbed` VARCHAR, `months` VARCHAR[].

#### `variables`

The latest NEON variables definition per product, table and field (description, units, dataType, pubFormat). Rows: SRER 6,735, KONZ 6,827.

Columns: `productCode` VARCHAR, `tableName` VARCHAR, `fieldName` VARCHAR, `description` VARCHAR, `dataType` VARCHAR, `units` VARCHAR, `downloadPkg` VARCHAR, `pubFormat` VARCHAR, `primaryKey` VARCHAR, `categoricalCodeName` VARCHAR, `sourceFile` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE.

#### `variables_history`

Every distinct variables definition, with first and last publication. Rows: SRER 6,995, KONZ 7,070.

Columns: `productCode` VARCHAR, `tableName` VARCHAR, `fieldName` VARCHAR, `description` VARCHAR, `dataType` VARCHAR, `units` VARCHAR, `downloadPkg` VARCHAR, `pubFormat` VARCHAR, `primaryKey` VARCHAR, `categoricalCodeName` VARCHAR, `nFiles` BIGINT, `firstPublication` TIMESTAMP WITH TIME ZONE, `lastPublication` TIMESTAMP WITH TIME ZONE, `firstMonth` VARCHAR, `lastMonth` VARCHAR, `latestSourceFile` VARCHAR.

#### `variables_files`

One row per variables file with its record count. Rows: SRER absent, KONZ 104.

Columns: `productCode` VARCHAR, `month` VARCHAR, `sourceFile` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `nVariables` BIGINT.

#### `sensor_positions`

Sensor positions per product and month (`HOR.VER`), with sensor latitude, longitude and elevation derived from the reference location plus offsets. Rows: SRER 317, KONZ 312.

Columns: `productCode` VARCHAR, `month` VARCHAR, `release` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `sourceFile` VARCHAR, `HOR.VER` VARCHAR, `sensorLocationID` VARCHAR, `sensorLocationDescription` VARCHAR, `positionStartDateTime` TIMESTAMP WITH TIME ZONE, `positionEndDateTime` TIMESTAMP WITH TIME ZONE, `referenceLocationID` VARCHAR, `referenceLocationIDDescription` VARCHAR, `referenceLocationIDStartDateTime` TIMESTAMP WITH TIME ZONE, `referenceLocationIDEndDateTime` TIMESTAMP WITH TIME ZONE, `xOffset` DOUBLE, `yOffset` DOUBLE, `zOffset` DOUBLE, `pitch` DOUBLE, `roll` DOUBLE, `azimuth` DOUBLE, `locationReferenceLatitude` DOUBLE, `locationReferenceLongitude` DOUBLE, `locationReferenceElevation` DOUBLE, `eastOffset` DOUBLE, `northOffset` DOUBLE, `xAzimuth` DOUBLE, `yAzimuth` DOUBLE, `horizontalPosition` VARCHAR, `verticalPosition` VARCHAR, `sensorLatitude` DOUBLE, `sensorLongitude` DOUBLE, `sensorElevation` DOUBLE, `locationType` VARCHAR, `locationLatitude` DOUBLE, `locationLongitude` DOUBLE, `locationElevationCatalog` DOUBLE, `historyLatitude` DOUBLE, `historyLongitude` DOUBLE, `historyElevation` DOUBLE.

#### `categorical_codes`

Distinct categorical code lists per product. Rows: SRER 7,650, KONZ 7,711.

Columns: `productCode` VARCHAR, `name` VARCHAR, `pubCode` VARCHAR, `description` VARCHAR, `startDate` VARCHAR, `endDate` VARCHAR, `nRecords` BIGINT, `nFiles` BIGINT, `firstMonth` VARCHAR, `lastMonth` VARCHAR, `latestSourceFile` VARCHAR, `latestPublication` TIMESTAMP WITH TIME ZONE.

#### `validation`

Distinct validation rules per product. Rows: SRER 6,332, KONZ 6,363.

Columns: `productCode` VARCHAR, `table` VARCHAR, `fieldName` VARCHAR, `description` VARCHAR, `dataType` VARCHAR, `units` VARCHAR, `parserToCreate` VARCHAR, `entryValidationRulesParser` VARCHAR, `entryValidationRulesForm` VARCHAR, `nRecords` BIGINT, `nFiles` BIGINT, `firstMonth` VARCHAR, `lastMonth` VARCHAR, `latestSourceFile` VARCHAR, `latestPublication` TIMESTAMP WITH TIME ZONE.

#### `science_review_flags`

Distinct science review flags per product. Rows: SRER absent, KONZ 9.

Columns: `productCode` VARCHAR, `srfID` VARCHAR, `startDateTime` VARCHAR, `endDateTime` VARCHAR, `domainID` VARCHAR, `siteID` VARCHAR, `dpID` VARCHAR, `fieldName` VARCHAR, `horizontalPosition` VARCHAR, `verticalPosition` VARCHAR, `srfValue` VARCHAR, `srfValueJustification` VARCHAR, `createDateTime` VARCHAR, `lastUpdateDateTime` VARCHAR, `nRecords` BIGINT, `nFiles` BIGINT, `firstMonth` VARCHAR, `lastMonth` VARCHAR, `latestSourceFile` VARCHAR, `latestPublication` TIMESTAMP WITH TIME ZONE.

#### `eml_index`

One row per EML metadata file, with its coverage dates. Rows: SRER 111, KONZ 106.

Columns: `productCode` VARCHAR, `month` VARCHAR, `release` VARCHAR, `sourceFile` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `size` BIGINT, `md5` VARCHAR, `md5Verified` BOOLEAN, `localRelPath` VARCHAR, `emlStart` DATE, `emlEnd` DATE.

#### `readme_index`

One row per readme file. Rows: SRER 111, KONZ 106.

Columns: `productCode` VARCHAR, `month` VARCHAR, `release` VARCHAR, `sourceFile` VARCHAR, `publicationDate` TIMESTAMP WITH TIME ZONE, `size` BIGINT, `md5` VARCHAR, `md5Verified` BOOLEAN, `localRelPath` VARCHAR.

#### `file_stats`

One row per stacked data file: row count, time range, finalQF fail percentage, plots, protocol versions, taxon count. Rows: SRER 774, KONZ 752.

Columns: `sourceFile` VARCHAR, `productCode` VARCHAR, `tableName` VARCHAR, `lakeTable` VARCHAR, `month` VARCHAR, `rowCount` BIGINT, `timeMin` TIMESTAMP WITH TIME ZONE, `timeMax` TIMESTAMP WITH TIME ZONE, `timeColumn` VARCHAR, `qfFailPct` DOUBLE, `plotIDs` VARCHAR[], `plotCount` BIGINT, `protocolVersions` VARCHAR[], `taxonCount` BIGINT, `hor` VARCHAR, `ver` VARCHAR, `tmi` VARCHAR.

#### `file_taxa`

Taxa observed per data file, with record counts. Rows: SRER 438, KONZ 624.

Columns: `sourceFile` VARCHAR, `productCode` VARCHAR, `tableName` VARCHAR, `taxonID` VARCHAR, `scientificName` VARCHAR, `taxonRank` VARCHAR, `vernacularName` VARCHAR, `nRecords` BIGINT.

#### `table_catalog`

One row per table the stacker built: product, NEON table name, kind (data, lab, h5, support), files, rows, columns, months, input fingerprint. Rows: SRER 446, KONZ 454.

Columns: `tableName` VARCHAR, `productCode` VARCHAR, `neonTable` VARCHAR, `kind` VARCHAR, `nFiles` BIGINT, `nRows` BIGINT, `nColumns` BIGINT, `firstMonth` VARCHAR, `lastMonth` VARCHAR, `timeColumn` VARCHAR, `inputSha256` VARCHAR, `builtAt` TIMESTAMP WITH TIME ZONE, `nTypedColumns` BIGINT, `nVarcharColumns` BIGINT, `rowsBeforeDedup` BIGINT, `dedupRemoved` BIGINT.

#### `schema_drift`

One row per header group of a data, lab or sensor_positions table. Rows: SRER 171, KONZ 167.

Columns: `productCode` VARCHAR, `tableName` VARCHAR, `lakeTable` VARCHAR, `headerSha1` VARCHAR, `nColumns` BIGINT, `nFiles` BIGINT, `firstMonth` VARCHAR, `lastMonth` VARCHAR, `columns` VARCHAR[], `missingVsUnion` VARCHAR[], `notInVariables` VARCHAR[], `builtAt` TIMESTAMP WITH TIME ZONE, `unknownColumns` VARCHAR[], `firstPublication` TIMESTAMP WITH TIME ZONE, `lastPublication` TIMESTAMP WITH TIME ZONE.

#### `cast_errors`

Columns that stayed VARCHAR because more than 0.1% of values failed to cast. Rows: SRER 0, KONZ 0.

Columns: `productCode` VARCHAR, `tableName` VARCHAR, `neonTable` VARCHAR, `columnName` VARCHAR, `targetType` VARCHAR, `nonNull` BIGINT, `failures` BIGINT, `failPct` DOUBLE, `action` VARCHAR, `samples` VARCHAR[], `builtAt` TIMESTAMP WITH TIME ZONE.

#### `stack_file_counts`

Records per source file counted by the streaming stacker. Rows: SRER absent, KONZ 61,610.

Columns: `productCode` VARCHAR, `scope` VARCHAR, `groupKey` VARCHAR, `sourceFile` VARCHAR, `nFiles` BIGINT, `nRecords` BIGINT, `nFailures` BIGINT.

#### `h5_attrs`

Attributes of the DP4.00200.001 HDF5 files. Rows: SRER 105, KONZ 42.

Columns: `productCode` VARCHAR, `sourceFile` VARCHAR, `month` VARCHAR, `path` VARCHAR, `attr` VARCHAR, `value` VARCHAR.

#### `h5_datasets`

Datasets of the DP4.00200.001 HDF5 files (path, shape, dtype, unit). Rows: SRER 13,715, KONZ 5,646.

Columns: `productCode` VARCHAR, `sourceFile` VARCHAR, `month` VARCHAR, `path` VARCHAR, `shape` VARCHAR, `nRows` BIGINT, `dtype` VARCHAR, `fields` VARCHAR[], `unit` VARCHAR.

#### `h5_objdesc`

The `/objDesc` term dictionary of the HDF5 files. Rows: SRER absent, KONZ 1,038.

Columns: `productCode` VARCHAR, `month` VARCHAR, `sourceFile` VARCHAR, `term` VARCHAR, `description` VARCHAR.

## Keys and joins

- `files` is keyed by `productCode, month, name`; `site_months` by `productCode, month`.
- Every stacked table carries `month`, `release` and `sourceFile`. `sourceFile` equals
  `files.name` and `file_stats.sourceFile`, so a row can be traced to its NEON file and its Data
  Store copy.
- `variables` joins a stacked table's columns on `productCode`, `tableName` (NEON's table name,
  for example `SAAT_30min`) and `fieldName`, and gives description and units.
- The site code is fixed per lake. To join the catalog lake, use `productCode` and `month` with
  `siteCode = '<SITE>'`.

## Example queries

### Which months the lake holds for a product

```sql
SELECT productCode, month, release, isProvisional, fileCount
FROM neon_srer.site_months
WHERE productCode = 'DP1.00002.001'
ORDER BY month;
```

```text
productCode | month | release | isProvisional | fileCount
DP1.00002.001 | 2019-05 | RELEASE-2026 | False | 10
DP1.00002.001 | 2024-05 | RELEASE-2026 | False | 10
DP1.00002.001 | 2024-06 | RELEASE-2026 | False | 10
DP1.00002.001 | 2025-06 | RELEASE-2026 | False | 10
DP1.00002.001 | 2026-08 | PROVISIONAL | True | 10
```

The catalog lists 110 months for the same product at SRER.

### Daily mean air temperature, with units

```sql
SELECT verticalPosition, date_trunc('day', startDateTime) AS day,
       round(avg(tempSingleMean), 2) AS mean_c, count(*) AS n
FROM neon_srer.dp1_00002_001__saat_30min
WHERE month = '2026-08' AND finalQF = 0
GROUP BY ALL ORDER BY day, verticalPosition
LIMIT 3;
```

```text
verticalPosition | day | mean_c | n
010 | 2026-08-01 00:00:00+00:00 | 32.59 | 48
020 | 2026-08-01 00:00:00+00:00 | 32.3 | 48
030 | 2026-08-01 00:00:00+00:00 | 32.41 | 48
```

Days are UTC days. `finalQF = 0` keeps records that passed NEON's quality assessment:

```sql
SELECT fieldName, units, description
FROM neon_srer.variables
WHERE productCode = 'DP1.00002.001' AND tableName = 'SAAT_30min'
  AND fieldName IN ('tempSingleMean', 'finalQF')
ORDER BY fieldName;
```

```text
fieldName | units | description
finalQF | NULL | Quality flag indicating whether a data product has passed or failed an overall assessment of its quality, detailed in NEON.DOC.001113 (1=fail, 0=pass)
tempSingleMean | celsius | Arithmetic mean of single aspirated air temperature
```

### Taxa observed

```sql
SELECT taxonID, scientificName, vernacularName, sum(nRecords) AS n
FROM neon_konz.file_taxa
WHERE productCode = 'DP1.10003.001'
GROUP BY ALL ORDER BY n DESC
LIMIT 3;
```

```text
taxonID | scientificName | vernacularName | n
DICK | Spiza americana | Dickcissel | 272
EAME | Sturnella magna | Eastern Meadowlark | 135
BHCO | Molothrus ater | Brown-headed Cowbird | 100
```

### From a row to its file on the Data Store

```sql
SELECT name, 'https://data.cyverse.org/dav-anon' || irodsPath AS url, size, md5Verified
FROM neon_srer.files
WHERE productCode = 'DP1.00002.001' AND tableName = 'SAAT_30min' AND month = '2026-08'
ORDER BY name
LIMIT 1;
```

```text
name | url | size | md5Verified
NEON.D14.SRER.DP1.00002.001.000.010.030.SAAT_30min.2026-08.basic.20260907T191004Z.csv | https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/sites/SRER/raw/DP1.00002.001/2026-08/NEON.D14.SRER.DP1.00002.001.000.010.030.SAAT_30min.2026-08.basic.20260907T191004Z.csv | 152702 | True
```

That URL returned HTTP 200 on 2026-11-24. Each file on the Data Store also carries AVUs (see
[AVU history](avu-history.md)).

### Compare a site lake with the catalog inventory

This joins the site lake with the [catalog lake](catalog-lake.md); attach the catalog as `neon`
first.

```sql
ATTACH 'ducklake:https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/neon_catalog.ducklake'
    AS neon (READ_ONLY,
             DATA_PATH 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/data/',
             OVERRIDE_DATA_PATH true);
SELECT i.productCode,
       sum(i.fileCount) AS inventory_files,
       (SELECT count(*) FROM neon_srer.files f WHERE f.productCode = i.productCode) AS lake_files
FROM neon.v_site_inventory i
WHERE i.siteCode = 'SRER' AND i.productCode IN ('DP1.00002.001', 'DP1.10003.001')
GROUP BY 1 ORDER BY 1;
```

```text
productCode | inventory_files | lake_files
DP1.00002.001 | 1100 | 50
DP1.10003.001 | 97 | 13
```

## Freshness

```sql
SELECT 'SRER' AS site, max(snapshot_id) AS last_snapshot, max(snapshot_time) AS last_change
FROM neon_srer.snapshots()
UNION ALL
SELECT 'KONZ', max(snapshot_id), max(snapshot_time) FROM neon_konz.snapshots()
ORDER BY site;
```

```text
site | last_snapshot | last_change
KONZ | 919 | 2026-09-29 16:15:26.615633+00:00
SRER | 321 | 2026-09-28 04:07:22.695171+00:00
```

- **Built:** SRER from 2026-09-27 to 2026-09-28 and KONZ from 2026-09-28 to 2026-09-29 (UTC). Neither has changed since.
- **NEON state:** files were listed against the catalog lake built on 2026-09-27
  (RELEASE-2026 plus PROVISIONAL months); each row's `release` and `publicationDate` say which
  NEON publication it came from.
- **How to tell from the data:** `snapshots()` (every stage commits one snapshot with a message
  such as `stream stack KONZ DP4.00200.001__2025-06_2026-08 h5`), `table_catalog.builtAt`, and for
  streamed sites the `pushedAt` in `CURRENT`.
- **Refresh:** re-run the site from `list` after a catalog refresh
  (`neon-ducklake run SITE --from-stage list`); see
  [Running the pipeline](../engineering/running.md#refresh-after-a-new-neon-release).

## Known limitations

- **Two sites, sampled.** See the warning at the top. The full 81-site sweep had not been
  published as of 2026-11-24.
- **Schemas differ between the two lakes.** KONZ has `h5_objdesc`, `science_review_flags`,
  `stack_file_counts` and `variables_files`; SRER does not. Stacked tables exist only for the
  products and tables the run reached.
- **AOP excluded, basic package only,** as in the catalog inventory.
- **Times are UTC.** `TIMESTAMPTZ` columns are stored in UTC; set `TimeZone = 'UTC'` to print them
  that way.
- **Fragmented small tables** (`table_catalog`, `schema_drift`) are slow to scan over HTTPS.
- **DuckDB versions.** These lakes were written by DuckDB 1.5.x and read with 1.5.5. After the
  pipeline's move to DuckDB 2.0, lakes it writes will need DuckDB 2.0 to read.
