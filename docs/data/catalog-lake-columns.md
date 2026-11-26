---
type: Reference
title: "Catalog lake: column reference"
description: "Every table and view of the neon-ducklake catalog lake with its one-line description, row count, key and the name and DuckDB type of every column, taken from information_schema and table_catalog on 2026-11-24."
tags:
  - neon-ducklake
  - dataset
  - schema
  - catalog
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: lake
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/neon_catalog.ducklake"
    title: "The published catalog lake (information_schema.columns and table_catalog, 2026-11-24)"
    author: "team:idss-mesa"
  - id: flatten-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/flatten.py"
    title: "flatten.py: table specs"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-02-15T00:00:00Z"
---

# Catalog lake: column reference

This page lists every table and view of the [catalog lake](catalog-lake.md). The descriptions,
keys and row counts come from the lake's own `table_catalog`; the columns and types come from
`information_schema.columns`. Both were read on 2026-11-24 from snapshot 1 (built 2026-09-27).

Two bookkeeping columns are left out of every table below: `rowHash` (VARCHAR, md5 of the row's
JSON, used by the hash-keyed upsert) and `loadedRun` (VARCHAR, the harvest run that loaded the
row). The 16 spatial tables have neither. Columns keep NEON's camelCase names; tables are
snake_case.

To list the columns of one table yourself:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/neon_catalog.ducklake'
    AS neon (READ_ONLY,
             DATA_PATH 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/data/',
             OVERRIDE_DATA_PATH true);
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog = 'neon' AND table_name = 'releases'
ORDER BY ordinal_position;
```

The output below is an excerpt (`...` marks omitted rows); the full list is in the
[`releases`](#releases) section.

```text
column_name | data_type
release | VARCHAR
...
generationDate | TIMESTAMP WITH TIME ZONE
...
manifestCellCount | BIGINT
...
```

## Products

### `documents`

Distinct documents referenced by products, with the HEAD /documents/{spec} status.

Rows: 510. Key: `specNumber`.

| Column | Type |
|---|---|
| `specNumber` | VARCHAR |
| `specId` | VARCHAR |
| `specType` | VARCHAR |
| `specSize` | BIGINT |
| `specDescription` | VARCHAR |
| `specUrl` | VARCHAR |
| `productCodes` | VARCHAR[] |
| `productCount` | INTEGER |
| `headStatus` | INTEGER |
| `contentType` | VARCHAR |
| `contentLength` | BIGINT |
| `fileName` | VARCHAR |
| `headError` | VARCHAR |

### `product_biorepository`

Biorepository collections linked to a product.

Rows: 225. Key: `productCode, collectionCode`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `collectionCode` | VARCHAR |
| `collectionName` | VARCHAR |
| `collectionUrl` | VARCHAR |
| `collectionContentUrl` | VARCHAR |
| `collectionDownloadUrl` | VARCHAR |

### `product_change_logs`

Product change logs (issues) as served by the current /products.

Rows: 4,849. Key: `productCode, id`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `id` | BIGINT |
| `parentIssueID` | BIGINT |
| `issueDate` | TIMESTAMP WITH TIME ZONE |
| `resolvedDate` | TIMESTAMP WITH TIME ZONE |
| `dateRangeStart` | TIMESTAMP WITH TIME ZONE |
| `dateRangeEnd` | TIMESTAMP WITH TIME ZONE |
| `locationAffected` | VARCHAR |
| `issue` | VARCHAR |
| `resolution` | VARCHAR |

### `product_keywords`

Product x keyword.

Rows: 2,976. Key: `productCode, keyword`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `keyword` | VARCHAR |

### `product_releases`

Product x release with the release DOI (productDoi is the bare 10.48443/... DOI).

Rows: 683. Key: `productCode, release`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `release` | VARCHAR |
| `generationDate` | TIMESTAMP WITH TIME ZONE |
| `url` | VARCHAR |
| `productDoiUrl` | VARCHAR |
| `productDoi` | VARCHAR |
| `productDoiGenerationDate` | TIMESTAMP WITH TIME ZONE |

### `product_specs`

Documents (specs) attached to each product.

Rows: 1,111. Key: `productCode, specNumber`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `specNumber` | VARCHAR |
| `specId` | VARCHAR |
| `specType` | VARCHAR |
| `specSize` | BIGINT |
| `specDescription` | VARCHAR |
| `specUrl` | VARCHAR |

### `product_themes`

Product x theme.

Rows: 316. Key: `productCode, theme`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `theme` | VARCHAR |

### `products`

One row per NEON data product (current /products); site/cell counts from current availability.

Rows: 202. Key: `productCode`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `productCodeLong` | VARCHAR |
| `productCodePresentation` | VARCHAR |
| `productName` | VARCHAR |
| `productDescription` | VARCHAR |
| `productAbstract` | VARCHAR |
| `productDesignDescription` | VARCHAR |
| `productStudyDescription` | VARCHAR |
| `productBasicDescription` | VARCHAR |
| `productExpandedDescription` | VARCHAR |
| `productSensor` | VARCHAR |
| `productRemarks` | VARCHAR |
| `productStatus` | VARCHAR |
| `productCategory` | VARCHAR |
| `productScienceTeam` | VARCHAR |
| `productScienceTeamAbbr` | VARCHAR |
| `productPublicationFormatType` | VARCHAR |
| `productHasExpanded` | BOOLEAN |
| `isAop` | BOOLEAN |
| `themes` | VARCHAR[] |
| `keywords` | VARCHAR[] |
| `siteCount` | INTEGER |
| `cellCount` | INTEGER |
| `provisionalCellCount` | INTEGER |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |
| `releaseCount` | INTEGER |
| `latestRelease` | VARCHAR |
| `latestReleaseDoi` | VARCHAR |
| `specCount` | INTEGER |
| `changeLogCount` | INTEGER |
| `biorepositoryCount` | INTEGER |

## Releases

### `release_artifacts`

Release artifacts (URLs unsigned).

Rows: 6. Key: `release, name`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `name` | VARCHAR |
| `type` | VARCHAR |
| `url` | VARCHAR |
| `size` | BIGINT |
| `md5` | VARCHAR |

### `release_manifest_months`

Every product x site x month cell of each release manifest (MANIFEST_AVAILABLE).

Rows: 918,893. Key: `release, productCode, siteCode, month`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `month` | VARCHAR |

### `release_product_change_logs`

Change logs of the in-release product records.

Rows: 21,590. Key: `release, productCode, id`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `productCode` | VARCHAR |
| `id` | BIGINT |
| `parentIssueID` | BIGINT |
| `issueDate` | TIMESTAMP WITH TIME ZONE |
| `resolvedDate` | TIMESTAMP WITH TIME ZONE |
| `dateRangeStart` | TIMESTAMP WITH TIME ZONE |
| `dateRangeEnd` | TIMESTAMP WITH TIME ZONE |
| `locationAffected` | VARCHAR |
| `issue` | VARCHAR |
| `resolution` | VARCHAR |

### `release_product_records`

Product records as published in each release (/releases/{tag}/products), in-release products only.

Rows: 683. Key: `release, productCode`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `productCode` | VARCHAR |
| `productCodeLong` | VARCHAR |
| `productCodePresentation` | VARCHAR |
| `productName` | VARCHAR |
| `productDescription` | VARCHAR |
| `productAbstract` | VARCHAR |
| `productDesignDescription` | VARCHAR |
| `productStudyDescription` | VARCHAR |
| `productBasicDescription` | VARCHAR |
| `productExpandedDescription` | VARCHAR |
| `productSensor` | VARCHAR |
| `productRemarks` | VARCHAR |
| `productStatus` | VARCHAR |
| `productCategory` | VARCHAR |
| `productScienceTeam` | VARCHAR |
| `productScienceTeamAbbr` | VARCHAR |
| `productPublicationFormatType` | VARCHAR |
| `productHasExpanded` | BOOLEAN |
| `themes` | VARCHAR[] |
| `keywords` | VARCHAR[] |
| `generationDate` | TIMESTAMP WITH TIME ZONE |
| `productDoiUrl` | VARCHAR |
| `productDoi` | VARCHAR |
| `productDoiGenerationDate` | TIMESTAMP WITH TIME ZONE |
| `siteCount` | INTEGER |
| `cellCount` | INTEGER |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |
| `specNumbers` | VARCHAR[] |
| `changeLogCount` | INTEGER |

### `release_products`

Products in each release with the release DOI (from /releases).

Rows: 683. Key: `release, productCode`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `productCode` | VARCHAR |
| `productName` | VARCHAR |
| `productDescription` | VARCHAR |
| `productDoiUrl` | VARCHAR |
| `productDoi` | VARCHAR |

### `release_site_products`

Site x product availability as published in each release (/releases/{tag}/sites).

Rows: 24,562. Key: `release, siteCode, productCode`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `siteCode` | VARCHAR |
| `productCode` | VARCHAR |
| `productTitle` | VARCHAR |
| `monthCount` | INTEGER |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |
| `availableMonths` | VARCHAR[] |

### `releases`

NEON releases (tags) with their MANIFEST_AVAILABLE artifact.

Rows: 6. Key: `release`.

| Column | Type |
|---|---|
| `release` | VARCHAR |
| `uuid` | VARCHAR |
| `generationDate` | TIMESTAMP WITH TIME ZONE |
| `productCount` | INTEGER |
| `siteCount` | INTEGER |
| `artifactCount` | INTEGER |
| `manifestName` | VARCHAR |
| `manifestMd5` | VARCHAR |
| `manifestSize` | BIGINT |
| `manifestCellCount` | BIGINT |

## Sites and locations

### `domains`

NEON domains (from /sites).

Rows: 20. Key: `domainCode`.

| Column | Type |
|---|---|
| `domainCode` | VARCHAR |
| `domainName` | VARCHAR |
| `siteCount` | INTEGER |
| `siteCodes` | VARCHAR[] |

### `location_history`

Location history (/locations/{name}?history=true) of CONFIG/TOWER/SITE locations.

Rows: 13,902. Key: `locationName, historyIndex`.

| Column | Type |
|---|---|
| `locationName` | VARCHAR |
| `historyIndex` | INTEGER |
| `isCurrent` | BOOLEAN |
| `locationStartDate` | TIMESTAMP WITH TIME ZONE |
| `locationEndDate` | TIMESTAMP WITH TIME ZONE |
| `locationDecimalLatitude` | DOUBLE |
| `locationDecimalLongitude` | DOUBLE |
| `locationElevation` | DOUBLE |
| `locationUtmEasting` | DOUBLE |
| `locationUtmNorthing` | DOUBLE |
| `locationUtmZone` | INTEGER |
| `locationUtmHemisphere` | VARCHAR |
| `xOffset` | DOUBLE |
| `yOffset` | DOUBLE |
| `zOffset` | DOUBLE |
| `alphaOrientation` | DOUBLE |
| `betaOrientation` | DOUBLE |
| `gammaOrientation` | DOUBLE |
| `offsetLocationName` | VARCHAR |
| `locationProperties` | JSON |

### `location_polygons`

Locations with a polygon (WKT, lon/lat, ring closed); geometry derived with ST_GeomFromText.

Rows: 235. Key: `locationName`.

| Column | Type |
|---|---|
| `locationName` | VARCHAR |
| `locationType` | VARCHAR |
| `siteCode` | VARCHAR |
| `domainCode` | VARCHAR |
| `vertexCount` | INTEGER |
| `wkt` | VARCHAR |
| `geometry` | GEOMETRY |

### `locations`

Every named location (GraphQL findLocations over the /locations hierarchy walk).

Rows: 379,745. Key: `locationName`.

| Column | Type |
|---|---|
| `locationName` | VARCHAR |
| `locationType` | VARCHAR |
| `locationDescription` | VARCHAR |
| `siteCode` | VARCHAR |
| `domainCode` | VARCHAR |
| `locationDecimalLatitude` | DOUBLE |
| `locationDecimalLongitude` | DOUBLE |
| `locationElevation` | DOUBLE |
| `locationUtmEasting` | DOUBLE |
| `locationUtmNorthing` | DOUBLE |
| `locationUtmZone` | INTEGER |
| `locationUtmHemisphere` | VARCHAR |
| `alphaOrientation` | DOUBLE |
| `betaOrientation` | DOUBLE |
| `gammaOrientation` | DOUBLE |
| `xOffset` | DOUBLE |
| `yOffset` | DOUBLE |
| `zOffset` | DOUBLE |
| `offsetLocationName` | VARCHAR |
| `locationParent` | VARCHAR |
| `activePeriods` | JSON |
| `locationProperties` | JSON |
| `locationPolygon` | JSON |
| `hasPolygon` | BOOLEAN |
| `hierarchyDepth` | INTEGER |
| `hierarchyParent` | VARCHAR |
| `hierarchySite` | VARCHAR |

### `site_locations`

SITE named-location records from /locations/sites.

Rows: 88. Key: `locationName`.

| Column | Type |
|---|---|
| `locationName` | VARCHAR |
| `locationType` | VARCHAR |
| `locationDescription` | VARCHAR |
| `siteCode` | VARCHAR |
| `domainCode` | VARCHAR |
| `locationDecimalLatitude` | DOUBLE |
| `locationDecimalLongitude` | DOUBLE |
| `locationElevation` | DOUBLE |
| `locationUtmEasting` | DOUBLE |
| `locationUtmNorthing` | DOUBLE |
| `locationUtmZone` | INTEGER |
| `locationUtmHemisphere` | VARCHAR |
| `alphaOrientation` | DOUBLE |
| `betaOrientation` | DOUBLE |
| `gammaOrientation` | DOUBLE |
| `xOffset` | DOUBLE |
| `yOffset` | DOUBLE |
| `zOffset` | DOUBLE |
| `offsetLocationName` | VARCHAR |
| `locationParent` | VARCHAR |
| `activePeriods` | JSON |
| `locationProperties` | JSON |
| `locationChildrenCount` | INTEGER |

### `site_products`

Current site x product availability (/sites dataProducts).

Rows: 6,112. Key: `siteCode, productCode`.

| Column | Type |
|---|---|
| `siteCode` | VARCHAR |
| `productCode` | VARCHAR |
| `productTitle` | VARCHAR |
| `monthCount` | INTEGER |
| `provisionalMonthCount` | INTEGER |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |
| `availableMonths` | VARCHAR[] |
| `releases` | VARCHAR[] |

### `site_releases`

Releases containing each site.

Rows: 486. Key: `siteCode, release`.

| Column | Type |
|---|---|
| `siteCode` | VARCHAR |
| `release` | VARCHAR |
| `generationDate` | TIMESTAMP WITH TIME ZONE |
| `url` | VARCHAR |

### `sites`

NEON field sites (/sites) + elevation/UTM/properties of the /locations/sites SITE record (property names without the 'Value for ' prefix).

Rows: 81. Key: `siteCode`.

| Column | Type |
|---|---|
| `siteCode` | VARCHAR |
| `siteName` | VARCHAR |
| `siteDescription` | VARCHAR |
| `siteType` | VARCHAR |
| `siteLatitude` | DOUBLE |
| `siteLongitude` | DOUBLE |
| `stateCode` | VARCHAR |
| `stateName` | VARCHAR |
| `domainCode` | VARCHAR |
| `domainName` | VARCHAR |
| `deimsId` | VARCHAR |
| `siteElevation` | DOUBLE |
| `siteUtmZone` | INTEGER |
| `siteUtmEasting` | DOUBLE |
| `siteUtmNorthing` | DOUBLE |
| `siteUtmHemisphere` | VARCHAR |
| `siteProperties` | MAP(VARCHAR, VARCHAR) |
| `productCount` | INTEGER |
| `releaseCount` | INTEGER |

## Availability

### `availability_months`

Product x site x month: current availability (release or PROVISIONAL) plus release-only cells (in a manifest, not current; release = newest manifest containing the cell).

Rows: 291,080. Key: `productCode, siteCode, month`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `month` | VARCHAR |
| `release` | VARCHAR |
| `isProvisional` | BOOLEAN |
| `isReleaseOnly` | BOOLEAN |
| `availableDataUrl` | VARCHAR |

### `product_site`

Product x site pairs (current + release-only). inventoryScope current/release-only/excluded:AOP; inventoryStatus ok/external/empty/error/missing (NULL for AOP).

Rows: 6,274. Key: `productCode, siteCode`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `isAop` | BOOLEAN |
| `inventoryScope` | VARCHAR |
| `inventoryStatus` | VARCHAR |
| `currentMonthCount` | INTEGER |
| `provisionalMonthCount` | INTEGER |
| `releaseOnlyMonthCount` | INTEGER |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |
| `releaseOnlyReleases` | VARCHAR[] |
| `fileCount` | BIGINT |
| `totalBytes` | BIGINT |
| `externalLinkCount` | INTEGER |

## File inventory

### `data_query_pairs`

POST /data/query requests per non-AOP product x site (requestRelease CURRENT = current + PROVISIONAL).

Rows: 5,048. Key: `productCode, siteCode, requestRelease`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `requestRelease` | VARCHAR |
| `scope` | VARCHAR |
| `startMonth` | VARCHAR |
| `endMonth` | VARCHAR |
| `plannedMonthCount` | INTEGER |
| `landingCount` | INTEGER |
| `splitCount` | INTEGER |
| `errorCount` | INTEGER |
| `status` | VARCHAR |
| `fileCount` | BIGINT |
| `totalBytes` | BIGINT |
| `packageMonthCount` | INTEGER |
| `lastFetchedAt` | TIMESTAMP WITH TIME ZONE |
| `error` | VARCHAR |

### `external_data`

External data links of products hosted outside NEON (probed on one month per zero-file pair).

Rows: 158. Key: `productCode, siteCode, url`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `monthProbed` | VARCHAR |
| `name` | VARCHAR |
| `type` | VARCHAR |
| `url` | VARCHAR |

### `files`

Non-AOP basic-package file inventory (DP0 support files attributed to the requested DP1 product; objectUrl unsigned).

Rows: 3,215,813. Key: `productCode, siteCode, month, release, name`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `domainCode` | VARCHAR |
| `month` | VARCHAR |
| `release` | VARCHAR |
| `releaseGenerationDate` | TIMESTAMP WITH TIME ZONE |
| `packageType` | VARCHAR |
| `packageGenerationDate` | TIMESTAMP WITH TIME ZONE |
| `name` | VARCHAR |
| `size` | BIGINT |
| `md5` | VARCHAR |
| `crc32c` | VARCHAR |
| `objectUrl` | VARCHAR |
| `kind` | VARCHAR |
| `tableName` | VARCHAR |
| `hor` | VARCHAR |
| `ver` | VARCHAR |
| `tmi` | VARCHAR |
| `publicationTimestamp` | TIMESTAMP |
| `ext` | VARCHAR |
| `isProvisional` | BOOLEAN |
| `isReleaseOnly` | BOOLEAN |
| `parseConsistent` | BOOLEAN |

### `site_month_packages`

Basic packages (product x site x month x release) of the file inventory.

Rows: 283,401. Key: `productCode, siteCode, month, release`.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `month` | VARCHAR |
| `release` | VARCHAR |
| `domainCode` | VARCHAR |
| `packageType` | VARCHAR |
| `generationDate` | TIMESTAMP WITH TIME ZONE |
| `releaseGenerationDate` | TIMESTAMP WITH TIME ZONE |
| `fileCount` | INTEGER |
| `totalBytes` | BIGINT |
| `isProvisional` | BOOLEAN |
| `isReleaseOnly` | BOOLEAN |

## Taxonomy, samples, prototypes, GraphQL

### `graphql_fields`

GraphQL fields, input fields and enum values (memberKind field/inputField/enumValue).

Rows: 261. Key: `typeName, memberKind, name`.

| Column | Type |
|---|---|
| `typeName` | VARCHAR |
| `memberKind` | VARCHAR |
| `name` | VARCHAR |
| `description` | VARCHAR |
| `type` | VARCHAR |
| `args` | JSON |
| `defaultValue` | VARCHAR |
| `isDeprecated` | BOOLEAN |
| `deprecationReason` | VARCHAR |

### `graphql_types`

GraphQL schema types (introspection).

Rows: 45. Key: `name`.

| Column | Type |
|---|---|
| `name` | VARCHAR |
| `kind` | VARCHAR |
| `description` | VARCHAR |
| `fieldCount` | INTEGER |
| `inputFieldCount` | INTEGER |
| `enumValueCount` | INTEGER |
| `interfaces` | VARCHAR[] |
| `possibleTypes` | VARCHAR[] |
| `rootOperation` | VARCHAR |

### `prototype_datasets`

Prototype datasets (/prototype/datasets).

Rows: 72. Key: `uuid`.

| Column | Type |
|---|---|
| `uuid` | VARCHAR |
| `projectTitle` | VARCHAR |
| `projectDescription` | VARCHAR |
| `designDescription` | VARCHAR |
| `metadataDescription` | VARCHAR |
| `studyAreaDescription` | VARCHAR |
| `datasetAbstract` | VARCHAR |
| `startYear` | INTEGER |
| `endYear` | INTEGER |
| `dateUploaded` | TIMESTAMP WITH TIME ZONE |
| `isPublished` | BOOLEAN |
| `version` | VARCHAR |
| `versionDescription` | VARCHAR |
| `doiUrl` | VARCHAR |
| `doi` | VARCHAR |
| `doiGenerationDate` | TIMESTAMP WITH TIME ZONE |
| `dataUrl` | VARCHAR |
| `fileCount` | INTEGER |
| `dataThemes` | VARCHAR[] |
| `scienceTeams` | VARCHAR[] |
| `keywords` | VARCHAR[] |
| `fileTypes` | VARCHAR[] |
| `relatedDataProducts` | VARCHAR[] |
| `relatedVersions` | JSON |
| `publicationCitations` | JSON |
| `dataLocations` | JSON |

### `prototype_files`

Files of each prototype dataset (/prototype/data/{uuid}).

Rows: 1,502. Key: `datasetUuid, fileName`.

| Column | Type |
|---|---|
| `datasetUuid` | VARCHAR |
| `fileName` | VARCHAR |
| `name` | VARCHAR |
| `description` | VARCHAR |
| `size` | BIGINT |
| `md5` | VARCHAR |
| `typeName` | VARCHAR |
| `typeDescription` | VARCHAR |
| `url` | VARCHAR |

### `prototype_locations`

Sites of each prototype dataset.

Rows: 220. Key: `datasetUuid, siteCode`.

| Column | Type |
|---|---|
| `datasetUuid` | VARCHAR |
| `siteCode` | VARCHAR |
| `siteName` | VARCHAR |
| `domain` | VARCHAR |
| `state` | VARCHAR |
| `latitude` | DOUBLE |
| `longitude` | DOUBLE |

### `sample_classes`

Supported sample classes (/samples/supportedClasses).

Rows: 305. Key: `sampleClass`.

| Column | Type |
|---|---|
| `sampleClass` | VARCHAR |
| `description` | VARCHAR |

### `taxonomy`

NEON taxonomy (verbose), dwc:/gbif: prefixes stripped; the full record as JSON.

Rows: 134,196. Key: `taxonTypeCode, taxonID`.

| Column | Type |
|---|---|
| `taxonTypeCode` | VARCHAR |
| `taxonID` | VARCHAR |
| `acceptedTaxonID` | VARCHAR |
| `scientificName` | VARCHAR |
| `scientificNameAuthorship` | VARCHAR |
| `taxonRank` | VARCHAR |
| `vernacularName` | VARCHAR |
| `kingdom` | VARCHAR |
| `phylum` | VARCHAR |
| `class` | VARCHAR |
| `order` | VARCHAR |
| `family` | VARCHAR |
| `genus` | VARCHAR |
| `subgenus` | VARCHAR |
| `specificEpithet` | VARCHAR |
| `infraspecificEpithet` | VARCHAR |
| `nameAccordingToID` | VARCHAR |
| `nameAccordingToTitle` | VARCHAR |
| `taxonProtocolCategory` | VARCHAR |
| `speciesGroup` | VARCHAR |
| `updateDate` | TIMESTAMP WITH TIME ZONE |
| `record` | JSON |

## Build and harvest bookkeeping

### `catalog_builds`

One row per catalog build (append-only; guarantees one snapshot per build).

Rows: 1. Key: `buildId`.

| Column | Type |
|---|---|
| `buildId` | VARCHAR |
| `builtAt` | TIMESTAMP WITH TIME ZONE |
| `runId` | VARCHAR |
| `runStatus` | VARCHAR |
| `allowPartial` | BOOLEAN |
| `inserted` | JSON |
| `deleted` | JSON |
| `rowCounts` | JSON |
| `versions` | JSON |
| `tools` | JSON |

### `harvest_requests`

The request ledger of each run (work/state.sqlite requests).

Rows: 17,499. Key: `runId, family, key`.

| Column | Type |
|---|---|
| `runId` | VARCHAR |
| `family` | VARCHAR |
| `key` | VARCHAR |
| `method` | VARCHAR |
| `path` | VARCHAR |
| `params` | JSON |
| `httpStatus` | INTEGER |
| `attempts` | INTEGER |
| `elapsedMs` | DOUBLE |
| `responseBytes` | BIGINT |
| `landedPath` | VARCHAR |
| `landedBytes` | BIGINT |
| `sha256` | VARCHAR |
| `status` | VARCHAR |
| `error` | VARCHAR |
| `fetchedAt` | TIMESTAMP WITH TIME ZONE |

### `harvest_runs`

Harvest runs (fetch_manifest.json) loaded into the catalog.

Rows: 1. Key: `runId`.

| Column | Type |
|---|---|
| `runId` | VARCHAR |
| `status` | VARCHAR |
| `stages` | VARCHAR[] |
| `pilot` | BOOLEAN |
| `startedAt` | TIMESTAMP WITH TIME ZONE |
| `finishedAt` | TIMESTAMP WITH TIME ZONE |
| `seconds` | DOUBLE |
| `pairs` | INTEGER |
| `failureCount` | INTEGER |
| `versions` | JSON |
| `tools` | JSON |
| `tunables` | JSON |
| `counts` | JSON |
| `transforms` | VARCHAR[] |
| `sealedManifestMd5` | VARCHAR |

### `table_catalog`

Every table and view of the catalog lake with its provenance.

Rows: 59. Key: `tableName`.

| Column | Type |
|---|---|
| `tableName` | VARCHAR |
| `tableGroup` | VARCHAR |
| `description` | VARCHAR |
| `keyColumns` | VARCHAR[] |
| `columnCount` | INTEGER |
| `rowCount` | BIGINT |
| `sourceFamilies` | VARCHAR[] |
| `source` | VARCHAR |
| `definitionSha256` | VARCHAR |

## Spatial layers (copied from neonscience.org layers)

### `neon_aop_flight_boxes`

neonscience.org spatial layer

Rows: 103.

| Column | Type |
|---|---|
| `domain` | VARCHAR |
| `domainName` | VARCHAR |
| `siteName` | VARCHAR |
| `siteID` | VARCHAR |
| `siteType` | VARCHAR |
| `sampleType` | VARCHAR |
| `priority` | BIGINT |
| `version` | BIGINT |
| `flightbxID` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_aquatic_drainage_lines`

neonscience.org spatial layer

Rows: 34.

| Column | Type |
|---|---|
| `DomainNum` | VARCHAR |
| `SiteID` | VARCHAR |
| `Source` | VARCHAR |
| `Shape_Leng` | VARCHAR |
| `SHAPE_Le_1` | DOUBLE |
| `geometry` | GEOMETRY |

### `neon_aquatic_pour_points`

neonscience.org spatial layer

Rows: 34.

| Column | Type |
|---|---|
| `DomainNum` | VARCHAR |
| `SiteID` | VARCHAR |
| `Source` | VARCHAR |
| `Latitude` | VARCHAR |
| `Longitude` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_aquatic_watersheds`

neonscience.org spatial layer

Rows: 34.

| Column | Type |
|---|---|
| `DomainName` | VARCHAR |
| `SiteName` | VARCHAR |
| `SiteID` | VARCHAR |
| `SiteType` | VARCHAR |
| `Science` | VARCHAR |
| `StateID` | VARCHAR |
| `UTM_Zone` | VARCHAR |
| `WSAreaKm2` | VARCHAR |
| `Source` | VARCHAR |
| `Area_NED` | VARCHAR |
| `AOPLiDAR` | VARCHAR |
| `AOP_Flight` | VARCHAR |
| `AOPCoverag` | DOUBLE |
| `TIS_WS` | VARCHAR |
| `TIS_Dist` | VARCHAR |
| `TIS_Bear` | VARCHAR |
| `HUC12Name` | VARCHAR |
| `HUC12` | VARCHAR |
| `NLCD_11` | VARCHAR |
| `NLCD_12` | VARCHAR |
| `NLCD_21` | VARCHAR |
| `NLCD_22` | VARCHAR |
| `NLCD_23` | VARCHAR |
| `NLCD_24` | VARCHAR |
| `NLCD_31` | VARCHAR |
| `NLCD_41` | VARCHAR |
| `NLCD_42` | VARCHAR |
| `NLCD_43` | VARCHAR |
| `NLCD_51` | VARCHAR |
| `NLCD_52` | VARCHAR |
| `NLCD_71` | VARCHAR |
| `NLCD_72` | VARCHAR |
| `NLCD_81` | VARCHAR |
| `NLCD_82` | VARCHAR |
| `NLCD_90` | VARCHAR |
| `NLCD_95` | VARCHAR |
| `NRCS_ALF` | VARCHAR |
| `NRCS_ARI` | VARCHAR |
| `NRCS_AND` | VARCHAR |
| `NRCS_ENT` | VARCHAR |
| `NRCS_GEL` | VARCHAR |
| `NRCS_HIS` | VARCHAR |
| `NRCS_INC` | VARCHAR |
| `NRCS_MOL` | VARCHAR |
| `NRCS_OXI` | VARCHAR |
| `NRCS_SPO` | VARCHAR |
| `NRCS_ULT` | VARCHAR |
| `NRCS_VER` | VARCHAR |
| `NRCS_WATER` | VARCHAR |
| `NoSoilData` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_domains`

neonscience.org spatial layer

Rows: 20.

| Column | Type |
|---|---|
| `OBJECTID` | BIGINT |
| `domainID` | VARCHAR |
| `domainName` | VARCHAR |
| `shapeLengt` | INTEGER |
| `shapeArea` | INTEGER |
| `geometry` | GEOMETRY |

### `neon_field_sites_kmz`

neonscience.org spatial layer

Rows: 81.

| Column | Type |
|---|---|
| `name` | VARCHAR |
| `description` | VARCHAR |
| `kmlLayer` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_terrestrial_sampling_boundaries`

neonscience.org spatial layer

Rows: 61.

| Column | Type |
|---|---|
| `domainNumb` | VARCHAR |
| `domainName` | VARCHAR |
| `siteType` | VARCHAR |
| `siteName` | VARCHAR |
| `siteID` | VARCHAR |
| `siteHost` | VARCHAR |
| `areaKm2` | DOUBLE |
| `acres` | DOUBLE |
| `activeSmpl` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_tos_columnnamedescriptions`

neonscience.org table

Rows: 58.

| Column | Type |
|---|---|
| `fieldName` | VARCHAR |
| `Description` | VARCHAR |
| `Further.Documentation` | VARCHAR |

### `neon_tos_decommissionedplots`

neonscience.org table

Rows: 411.

| Column | Type |
|---|---|
| `country` | VARCHAR |
| `stateProvince` | VARCHAR |
| `county` | VARCHAR |
| `domainName` | VARCHAR |
| `domainID` | VARCHAR |
| `siteName` | VARCHAR |
| `siteID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `subtypeSpecification` | VARCHAR |
| `plotID` | VARCHAR |
| `plotSize` | BIGINT |
| `plotDimensions` | VARCHAR |
| `referencePointPosition` | VARCHAR |
| `decimalLatitude` | DOUBLE |
| `decimalLongitude` | DOUBLE |
| `geodeticDatum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `coordinateUncertainty` | DOUBLE |
| `elevation` | DOUBLE |
| `elevationUncertainty` | DOUBLE |
| `minimumElevation` | DOUBLE |
| `maximumElevation` | DOUBLE |
| `slopeGradient` | DOUBLE |
| `slopeAspect` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `soilTypeOrder` | VARCHAR |
| `coordinateSource` | VARCHAR |
| `recordedDate` | VARCHAR |
| `filteredPositions` | DOUBLE |
| `plotPdop` | DOUBLE |
| `plotHdop` | DOUBLE |
| `plotName` | VARCHAR |
| `sampledAppMod` | VARCHAR |
| `plotStatus` | VARCHAR |
| `plotStartDate` | VARCHAR |
| `plotEndDate` | VARCHAR |

### `neon_tos_nlcdcoverbysite`

neonscience.org table

Rows: 389.

| Column | Type |
|---|---|
| `domainID` | VARCHAR |
| `siteID` | VARCHAR |
| `nlcdYear` | VARCHAR |
| `nlcdClass` | VARCHAR |
| `area_km2` | DOUBLE |

### `neon_tos_plot_centroids`

neonscience.org spatial layer

Rows: 3,753.

| Column | Type |
|---|---|
| `plotID` | VARCHAR |
| `pointID` | VARCHAR |
| `country` | VARCHAR |
| `state` | VARCHAR |
| `county` | VARCHAR |
| `domain` | VARCHAR |
| `domainID` | VARCHAR |
| `siteName` | VARCHAR |
| `siteID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `subSpec` | VARCHAR |
| `plotSize` | DOUBLE |
| `plotDim` | VARCHAR |
| `latitude` | DOUBLE |
| `longitude` | DOUBLE |
| `datum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `horzUncert` | DOUBLE |
| `elevation` | DOUBLE |
| `vertUncert` | DOUBLE |
| `minElev` | DOUBLE |
| `maxElev` | DOUBLE |
| `slope` | DOUBLE |
| `aspect` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `soilOrder` | VARCHAR |
| `coordSrc` | VARCHAR |
| `date` | VARCHAR |
| `gpsLogs` | DOUBLE |
| `plotPdop` | DOUBLE |
| `plotHdop` | DOUBLE |
| `appMods` | VARCHAR |
| `plotEdge` | DOUBLE |
| `geometry` | GEOMETRY |

### `neon_tos_plot_points`

neonscience.org spatial layer

Rows: 51,692.

| Column | Type |
|---|---|
| `plotID` | VARCHAR |
| `pointID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `latitude` | DOUBLE |
| `longitude` | DOUBLE |
| `datum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `horzUncert` | DOUBLE |
| `crdSource` | VARCHAR |
| `elevation` | DOUBLE |
| `vertUncert` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `appMods` | VARCHAR |
| `siteID` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_tos_plot_polygons`

neonscience.org spatial layer

Rows: 3,752.

| Column | Type |
|---|---|
| `plotID` | VARCHAR |
| `pointID` | VARCHAR |
| `country` | VARCHAR |
| `state` | VARCHAR |
| `county` | VARCHAR |
| `domain` | VARCHAR |
| `domainID` | VARCHAR |
| `siteName` | VARCHAR |
| `siteID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `subSpec` | VARCHAR |
| `plotSize` | DOUBLE |
| `plotDim` | VARCHAR |
| `latitude` | DOUBLE |
| `longitude` | DOUBLE |
| `datum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `horzUncert` | DOUBLE |
| `elevation` | DOUBLE |
| `vertUncert` | DOUBLE |
| `minElev` | DOUBLE |
| `maxElev` | DOUBLE |
| `slope` | DOUBLE |
| `aspect` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `soilOrder` | VARCHAR |
| `coordSrc` | VARCHAR |
| `date` | VARCHAR |
| `gpsLogs` | DOUBLE |
| `plotPdop` | DOUBLE |
| `plotHdop` | DOUBLE |
| `appMods` | VARCHAR |
| `plotEdge` | DOUBLE |
| `geometry` | GEOMETRY |

### `neon_tos_plot_subplots`

neonscience.org spatial layer

Rows: 94,541.

| Column | Type |
|---|---|
| `plotID` | VARCHAR |
| `subplotID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `latitude` | DOUBLE |
| `longitude` | DOUBLE |
| `datum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `horzUncert` | DOUBLE |
| `coordSrc` | VARCHAR |
| `elevation` | DOUBLE |
| `vertUncert` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `appMods` | VARCHAR |
| `subpltSize` | INTEGER |
| `subpltDim` | VARCHAR |
| `refPoint` | INTEGER |
| `plotName` | VARCHAR |
| `plotStatus` | VARCHAR |
| `geometry` | GEOMETRY |

### `neon_tos_versionedpoints`

neonscience.org table

Rows: 1,785.

| Column | Type |
|---|---|
| `plotID` | VARCHAR |
| `pointID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `decimalLatitude` | DOUBLE |
| `decimalLongitude` | DOUBLE |
| `geodeticDatum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `coordinateUncertainty` | DOUBLE |
| `coordinateSource` | VARCHAR |
| `elevation` | DOUBLE |
| `elevationUncertainty` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `applicableModules` | VARCHAR |
| `versionStartDate` | BIGINT |
| `versionEndDate` | BIGINT |
| `site` | VARCHAR |

### `neon_tos_versionedsubplots`

neonscience.org table

Rows: 334.

| Column | Type |
|---|---|
| `plotID` | VARCHAR |
| `subplotID` | VARCHAR |
| `plotType` | VARCHAR |
| `subtype` | VARCHAR |
| `decimalLatitude` | DOUBLE |
| `decimalLongitude` | DOUBLE |
| `geodeticDatum` | VARCHAR |
| `utmZone` | VARCHAR |
| `easting` | DOUBLE |
| `northing` | DOUBLE |
| `coordinateUncertainty` | DOUBLE |
| `coordinateSource` | VARCHAR |
| `elevation` | DOUBLE |
| `elevationUncertainty` | DOUBLE |
| `nlcdClass` | VARCHAR |
| `applicableModules` | VARCHAR |
| `subplotSize` | BIGINT |
| `subplotDimensions` | VARCHAR |
| `referencePointPosition` | BIGINT |
| `versionStartDate` | BIGINT |
| `versionEndDate` | BIGINT |
| `site` | VARCHAR |

## Views

### `v_pair_coverage`

per non-AOP product x site: cells with files, external, or missing, with the inventory status

Rows: view.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `inventoryScope` | VARCHAR |
| `inventoryStatus` | VARCHAR |
| `cells` | BIGINT |
| `cellsWithFiles` | BIGINT |
| `cellsExternal` | BIGINT |
| `cellsMissing` | BIGINT |
| `provisionalCells` | BIGINT |
| `releaseOnlyCells` | BIGINT |
| `fileCount` | BIGINT |
| `totalBytes` | BIGINT |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |

### `v_product_summary`

per product: status, team, DOI, pairs, cells (provisional / release-only) and inventory totals

Rows: view.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `productName` | VARCHAR |
| `productScienceTeamAbbr` | VARCHAR |
| `productStatus` | VARCHAR |
| `isAop` | BOOLEAN |
| `latestRelease` | VARCHAR |
| `latestReleaseDoi` | VARCHAR |
| `pairCount` | BIGINT |
| `releaseOnlyPairs` | BIGINT |
| `pairsWithFiles` | BIGINT |
| `externalPairs` | BIGINT |
| `failedPairs` | BIGINT |
| `cells` | BIGINT |
| `provisionalCells` | BIGINT |
| `releaseOnlyCells` | BIGINT |
| `firstMonth` | VARCHAR |
| `lastMonth` | VARCHAR |
| `fileCount` | BIGINT |
| `totalBytes` | BIGINT |

### `v_site_inventory`

Phase-2 planner: one row per non-AOP product x site x month (current + release-only) with file totals

Rows: view.

| Column | Type |
|---|---|
| `productCode` | VARCHAR |
| `siteCode` | VARCHAR |
| `month` | VARCHAR |
| `productName` | VARCHAR |
| `productScienceTeamAbbr` | VARCHAR |
| `release` | VARCHAR |
| `isProvisional` | BOOLEAN |
| `isReleaseOnly` | BOOLEAN |
| `isExternal` | BOOLEAN |
| `fileCount` | BIGINT |
| `totalBytes` | BIGINT |
