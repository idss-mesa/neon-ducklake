---
type: Dataset
title: "Catalog lake (neon_catalog.ducklake)"
description: "Dataset card for the Phase-1 catalog lake: NEON Data API metadata (202 products, 81 sites, 6 releases, availability, locations, taxonomy) and an inventory of 3,215,813 non-AOP data files, as one DuckDB ducklake-extension lake readable anonymously over HTTPS."
tags:
  - neon-ducklake
  - dataset
  - ducklake
  - neon
  - catalog
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: lake
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/neon_catalog.ducklake"
    title: "The published catalog lake (queried read-only on 2026-11-24)"
    author: "team:idss-mesa"
  - id: catalog-lake-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/catalog_lake.py"
    title: "catalog_lake.py: the build"
    author: "team:idss-mesa"
  - id: flatten-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/flatten.py"
    title: "flatten.py: table specs"
    author: "team:idss-mesa"
  - id: views
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/sql/catalog_views.sql"
    title: "sql/catalog_views.sql: the three views"
    author: "team:idss-mesa"
  - id: neon-api
    resource: "https://data.neonscience.org/data-api/"
    title: "NEON Data API"
    author: "team:neon"
status: stable
stale_after: "2027-02-15T00:00:00Z"
---

# Catalog lake

The catalog lake is NEON Data API metadata harvested into one
[DuckLake](https://ducklake.select/) in DuckDB's `ducklake` extension format. It holds every
NEON data product (AOP included), site, release, availability month, named location with its
history, taxon, sample class, prototype dataset and document, plus an inventory of every
non-AOP basic-package data file. It holds no measurements. For stacked data, see the
[site lakes](site-lakes.md).

All numbers on this page come from queries run read-only over HTTPS on 2026-11-24.

## At a glance

| | |
|---|---|
| Location | `https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/` (`neon_catalog.ducklake` plus `data/`) |
| Format | DuckLake 1.0 (`ducklake` extension), Parquet with zstd, schema `main` |
| Size | catalog file 4,468,736 bytes; README states about 230 MB of Parquet (not re-measured) |
| Tables | 56 tables and 3 views: 40 tables from the NEON API, 16 spatial layers, and the views `v_site_inventory`, `v_pair_coverage`, `v_product_summary` |
| Snapshots | One build: snapshot 1, `catalog build 20260927T143926Z`, 2026-09-27 15:30:49 UTC |
| Source | NEON Data API (REST and GraphQL) through neon-mcp's client, harvest run `20260927T143926Z` (complete, 5,048 product x site pairs, 0 failures) |
| NEON releases | RELEASE-2021 to RELEASE-2026, plus PROVISIONAL months |
| Coverage | 202 products (161 non-AOP, 41 AOP), 81 sites, 20 domains, months 2010-12 to 2026-09 |
| File inventory | 3,215,813 files, 4,034.1 GB, 121 products, 81 sites |
| Licence | NEON data and metadata are CC BY 4.0 |

## How to read it

### HTTPS, read-only, no account

This works with DuckDB 1.5.5 and 2.0.0 (both checked on 2026-11-24). `DATA_PATH` plus
`OVERRIDE_DATA_PATH true` replaces the relative `data/` path the build recorded.

```sql
INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;
SET TimeZone = 'UTC';
ATTACH 'ducklake:https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/neon_catalog.ducklake'
    AS neon (READ_ONLY,
             DATA_PATH 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/data/',
             OVERRIDE_DATA_PATH true);
SELECT count(*) AS products FROM neon.products;
```

```text
products
202
```

The attach took about 2 s. Most queries below took 0.1 to 5 s.

From Python, `uv run --with duckdb==1.5.5 --with pytz python` works. Without `pytz`, fetching a
`TIMESTAMP WITH TIME ZONE` value into Python fails.

### Local copy

Copy `ducklake/neon_catalog.ducklake` and `ducklake/data/` together (for example with `gocmd get`
or `iget -r`) and attach the copy with a local `DATA_PATH`. In a checkout of this repository,
`sql/attach_all.sql` does this from the data root, and `neon_ducklake.catalog_lake.attach_all(con)`
does it from Python. The local-copy route was not re-run for this page.

### Other access paths

- **neon-mcp** does not read this lake. It calls the live NEON Data API, which is the same source
  the lake was harvested from. Use it for data newer than the harvest, and for signed download
  URLs (`neon_list_files`, which needs a NEON API token).
- **The MESA Quack endpoint** does not apply. Quack serves a MESA *history* catalog, not this
  lake, and no public Quack endpoint for this project is documented.
- **mesa-mcp's `mesa_ducklake_*` tools** read the AVU history, not this lake. See
  [AVU history](avu-history.md).

## Tables

`table_catalog` describes every table and view in the lake: group, key, row count, and source.
Every NEON-derived table also has two bookkeeping columns, `rowHash` (md5 of the row's JSON, used
for upserts) and `loadedRun` (the harvest run that loaded the row). The full column list with types
is in [Catalog lake: column reference](catalog-lake-columns.md).

```sql
SELECT tableGroup, count(*) AS n_tables, sum(rowCount) AS n_rows
FROM neon.table_catalog
GROUP BY 1 ORDER BY 1;
```

```text
tableGroup | n_tables | n_rows
availability | 2 | 297354
inventory | 4 | 3504420
meta | 4 | 17560
other | 7 | 136601
products | 8 | 10872
releases | 7 | 966423
sites | 8 | 400669
spatial | 16 | 157082
view | 3 | NULL
```

The main tables, their grain and their keys:

| Table | One row per | Key | Rows |
|---|---|---|---|
| `products` | NEON data product (current `/products`) | `productCode` | 202 |
| `product_releases` | product x release, with the release DOI (`productDoi` is the bare `10.48443/...` DOI) | `productCode, release` | 683 |
| `releases` | NEON release tag | `release` | 6 |
| `release_manifest_months` | product x site x month cell of each release manifest | `release, productCode, siteCode, month` | 918,893 |
| `sites` | NEON field site, with elevation, UTM and `siteProperties` (a MAP, for example `siteProperties['HABITAT']`) | `siteCode` | 81 |
| `domains` | NEON domain | `domainCode` | 20 |
| `locations` | named location (GraphQL `findLocations` over the `/locations` hierarchy) | `locationName` | 379,745 |
| `location_history` | history entry of a CONFIG, TOWER or SITE location | `locationName, historyIndex` | 13,902 |
| `site_products` | current site x product availability (`/sites`) | `siteCode, productCode` | 6,112 |
| `availability_months` | product x site x month: current availability (a release or PROVISIONAL) plus release-only months | `productCode, siteCode, month` | 291,080 |
| `product_site` | product x site pair, with `inventoryScope` (current, release-only, excluded:AOP) and `inventoryStatus` (ok, external, empty, error, missing) | `productCode, siteCode` | 6,274 |
| `site_month_packages` | basic package: product x site x month x release | `productCode, siteCode, month, release` | 283,401 |
| `files` | non-AOP basic-package file; DP0 support files are attributed to the requested DP1 product; `objectUrl` is unsigned | `productCode, siteCode, month, release, name` | 3,215,813 |
| `external_data` | link to data hosted outside NEON | `productCode, siteCode, url` | 158 |
| `taxonomy` | NEON taxon (verbose record; `dwc:`/`gbif:` prefixes stripped; full record as JSON) | `taxonTypeCode, taxonID` | 134,196 |
| `catalog_builds` | catalog build (append-only) | `buildId` | 1 |
| `harvest_runs` | harvest run loaded into the catalog | `runId` | 1 |

The three views, from `sql/catalog_views.sql`:

| View | One row per | Use it for |
|---|---|---|
| `v_site_inventory` | non-AOP product x site x month, with `release`, `isProvisional`, `isReleaseOnly`, `isExternal`, `fileCount`, `totalBytes` | the months and bytes available for a product at a site; the Phase-2 planner reads it |
| `v_pair_coverage` | non-AOP product x site | cells with files, external or missing, and the first and last month |
| `v_product_summary` | product | status, team, latest release DOI, pair and cell counts, file totals |

### Keys and joins

- Products: `productCode` (for example `DP1.00002.001`) joins `products`, `product_releases`,
  `product_site`, `availability_months`, `files` and the views.
- Sites: `siteCode` (for example `SRER`) joins `sites`, `site_products`, `availability_months`,
  `files`, `locations` and the views. The spatial layers use `siteID`.
- Months are `VARCHAR` `'YYYY-MM'`. `release` is `'RELEASE-YYYY'` or `'PROVISIONAL'`.
- Site lakes use the same `productCode` and `month` values, so a site lake joins the catalog on
  `(productCode, month)` with `siteCode` fixed (see [Site lakes](site-lakes.md#compare-a-site-lake-with-the-catalog-inventory)).

## Coverage

```sql
SELECT count(*) AS files, count(DISTINCT productCode) AS products,
       count(DISTINCT siteCode) AS sites, min(month) AS first_month,
       max(month) AS last_month, round(sum(size) / 1e9, 1) AS gb
FROM neon.files;
```

```text
files | products | sites | first_month | last_month | gb
3215813 | 121 | 81 | 2010-12 | 2026-09 | 4034.1
```

```sql
SELECT release, isProvisional, isReleaseOnly, count(*) AS n_files
FROM neon.files
GROUP BY ALL ORDER BY 1;
```

```text
release | isProvisional | isReleaseOnly | n_files
PROVISIONAL | True | False | 428819
RELEASE-2021 | False | True | 12345
RELEASE-2022 | False | True | 26237
RELEASE-2023 | False | True | 345
RELEASE-2024 | False | True | 7851
RELEASE-2025 | False | True | 56508
RELEASE-2026 | False | False | 2683708
```

A release-only file belongs to a month that appears in an earlier release manifest but not in
current availability; its `release` is the newest manifest that contains it.

Other counts measured the same day:

- `products`: 202 rows; `isAop` false for 161 and true for 41; `productStatus` ACTIVE 173,
  FUTURE 29.
- `availability_months`: 159 distinct products, 81 sites, months 2010-12 to 2026-09.
- `v_site_inventory` (non-AOP only): 125 products, 81 sites, 283,401 cells, 3,215,813 files.
- `sites`: 40 CORE and 41 GRADIENT; `siteProperties['HABITAT']` is Terrestrial for 47,
  Aquatic Wadeable Stream for 24, Aquatic Lake for 7 and Aquatic Non-wadeable Stream for 3.
- `files.kind`: data 2,016,986; eml 266,246; readme 266,058; variables 260,943;
  sensor_positions 160,351; categorical_codes 119,429; validation 100,592;
  science_review_flags 20,080; lab 5,128.

## Example queries

### Which months exist for a product at a site

```sql
SELECT month, release, isProvisional, isReleaseOnly, fileCount, totalBytes
FROM neon.v_site_inventory
WHERE productCode = 'DP1.00002.001' AND siteCode = 'SRER'
ORDER BY month DESC
LIMIT 5;
```

```text
month | release | isProvisional | isReleaseOnly | fileCount | totalBytes
2026-08 | PROVISIONAL | True | False | 10 | 14013500
2026-07 | PROVISIONAL | True | False | 10 | 14085588
2026-06 | PROVISIONAL | True | False | 10 | 13631992
2026-05 | PROVISIONAL | True | False | 10 | 14080149
2026-04 | PROVISIONAL | True | False | 10 | 13569169
```

Over all months, that pair has 110 months from 2017-07 to 2026-08, 14 of them PROVISIONAL.
`availability_months` gives the same months for AOP products too.

### From a product to its data files

```sql
SELECT name, kind, tableName, hor, ver, tmi, size
FROM neon.files
WHERE productCode = 'DP1.00002.001' AND siteCode = 'SRER' AND month = '2026-08'
ORDER BY name
LIMIT 3;
```

```text
name | kind | tableName | hor | ver | tmi | size
NEON.D14.SRER.DP1.00002.001.000.010.001.SAAT_1min.2026-08.basic.20260907T191004Z.csv | data | SAAT_1min | 000 | 010 | 001 | 4485203
NEON.D14.SRER.DP1.00002.001.000.010.030.SAAT_30min.2026-08.basic.20260907T191004Z.csv | data | SAAT_30min | 000 | 010 | 030 | 152702
NEON.D14.SRER.DP1.00002.001.000.020.001.SAAT_1min.2026-08.basic.20260907T191004Z.csv | data | SAAT_1min | 000 | 020 | 001 | 4480142
```

`files.objectUrl` holds NEON's storage URL with the signature removed, for example
`https://storage.googleapis.com/neon-publication/NEON.DOM.SITE.DP1.00002.001/SRER/20260801T000000--20260901T000000/basic/<name>`.
That URL returned HTTP 403 on 2026-11-24, so it is an identifier, not a download link. To get the
file:

- if the site and month are in a [site lake](site-lakes.md), the Data Store copy is public at
  `https://data.cyverse.org/dav-anon` + the site lake's `files.irodsPath`;
- otherwise ask the NEON API for a signed URL, for example with neon-mcp's `neon_list_files`
  (needs a NEON API token).

### Which NEON release the lake reflects, and the DOI to cite

```sql
SELECT release, generationDate, productCount, manifestCellCount
FROM neon.releases
ORDER BY release;
```

```text
release | generationDate | productCount | manifestCellCount
RELEASE-2021 | 2021-01-23 02:30:02+00:00 | 106 | 88729
RELEASE-2022 | 2022-01-20 17:39:46+00:00 | 109 | 113326
RELEASE-2023 | 2023-01-27 12:07:53+00:00 | 109 | 135593
RELEASE-2024 | 2024-01-27 00:04:25+00:00 | 109 | 161471
RELEASE-2025 | 2025-01-29 00:07:30+00:00 | 114 | 195631
RELEASE-2026 | 2026-01-23 00:07:49+00:00 | 136 | 224143
```

```sql
SELECT release, productDoi
FROM neon.product_releases
WHERE productCode = 'DP1.00002.001'
ORDER BY release DESC
LIMIT 2;
```

```text
release | productDoi
RELEASE-2026 | 10.48443/p69b-5e50
RELEASE-2025 | 10.48443/hejx-aa82
```

Cite released data at its release DOI. Cite PROVISIONAL data by product and download date (see
NEON's [data policies](https://www.neonscience.org/data-samples/data-policies-citation)).

### The largest non-AOP products

```sql
SELECT productCode, productName, pairsWithFiles, fileCount, round(totalBytes / 1e9, 1) AS gb
FROM neon.v_product_summary
WHERE NOT isAop
ORDER BY totalBytes DESC NULLS LAST
LIMIT 3;
```

```text
productCode | productName | pairsWithFiles | fileCount | gb
DP1.00094.001 | Soil water content and water salinity | 46 | 405961 | 995.2
DP1.00041.001 | Soil temperature | 46 | 465324 | 897.2
DP4.00200.001 | Bundled data products - eddy covariance | 47 | 15345 | 646.5
```

## Freshness

```sql
SELECT snapshot_id, snapshot_time, author, commit_message
FROM neon.snapshots();
```

```text
snapshot_id | snapshot_time | author | commit_message
0 | 2026-09-27 15:30:49.814570+00:00 | NULL | NULL
1 | 2026-09-27 15:30:49.880350+00:00 | tswetnam | catalog build 20260927T143926Z
```

```sql
SELECT runId, status, startedAt, finishedAt, pairs, failureCount
FROM neon.harvest_runs;
```

```text
runId | status | startedAt | finishedAt | pairs | failureCount
20260927T143926Z | complete | 2026-09-27 14:40:11.442263+00:00 | 2026-09-27 15:22:34.437292+00:00 | 5048 | 0
```

- **Built:** 2026-09-27, from one harvest that ran 14:40 to 15:22 UTC that day.
- **NEON state it reflects:** RELEASE-2026 (generated 2026-01-23) plus PROVISIONAL months up to
  2026-09 as the API served them on 2026-09-27.
- **How to tell from the data:** `snapshots()` (one build per snapshot), `catalog_builds.builtAt`,
  `harvest_runs`, `max(month)` in `availability_months`, and the newest row of `releases`.
  `catalog_builds.versions` and `.tools` record the code and tool versions of the build
  (DuckDB 1.5.5 for this one).
- **Refresh:** a new build adds one snapshot (`catalog fetch --new`, `catalog seal`,
  `catalog build`, then `mirror --what api,lake`). See
  [Running the pipeline](../engineering/running.md#refresh-after-a-new-neon-release).
  NEON publishes a release each January (the `generationDate` values above), so after
  January 2027 this lake no longer reflects the latest release until it is rebuilt.

## Known limitations

- **Metadata only.** No measurements. Measurements are in the site lakes, which cover only a
  sample of two sites today.
- **AOP files are not inventoried.** AOP products are in `products`, `product_site`
  (`inventoryScope = 'excluded:AOP'`, 1,824 pairs) and `availability_months`, but `files` and the
  views exclude them.
- **Basic package only.** The inventory lists the basic package, not the expanded one.
- **`objectUrl` is unsigned** and does not download (HTTP 403).
- **External products** (data hosted outside NEON) have no files; their links are in
  `external_data` (158 rows; `product_site.inventoryStatus = 'external'` for 158 pairs).
- **A snapshot in time.** PROVISIONAL months change and can be replaced when NEON releases them.
- **Spatial layers** were copied once from a separate spatial lake (version 90). Their `GEOMETRY`
  columns read without extensions; `ST_*` functions need `INSTALL spatial; LOAD spatial;`.
- **DuckDB versions.** The lake was written by DuckDB 1.5.5 and reads in 1.5.5 and 2.0.0. The
  README warns that a lake DuckDB 2.0 has written to can no longer be opened by DuckDB 1.x; that
  will apply once a 2.0 build is published.
