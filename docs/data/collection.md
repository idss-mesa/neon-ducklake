---
type: Reference
title: "The published collection"
description: "What is in the public neon-ducklake collection on the CyVerse Data Store (catalog lake, site lakes, raw NEON files, reports, AVU history, the history spool), which code writes each path, and how to read it anonymously over HTTPS."
tags:
  - neon-ducklake
  - dataset
  - cyverse
  - data-store
  - access
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: collection
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/"
    title: "The public collection (listed with WebDAV PROPFIND on 2026-11-24)"
    author: "team:idss-mesa"
  - id: readme
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/README.md"
    title: "README: data layout and reading the published lakes"
    author: "team:idss-mesa"
  - id: de-sandbox
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/docs/DE_SANDBOX.md"
    title: "DE_SANDBOX.md: shared Data Store paths"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-02-15T00:00:00Z"
---

# The published collection

neon-ducklake publishes everything it keeps to one MESA project collection on the CyVerse Data
Store. The collection is public: anyone can read it over HTTPS without an account, a copy, or
this repository.

```
https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/
```

The iRODS path is `/iplant/home/tswetnam/neon-ducklake` (the default
`NEON_DUCKLAKE_IRODS_ROOT`). To turn any iRODS path in this collection into a URL, prefix it with
`https://data.cyverse.org/dav-anon`.

## What is where

Listed with WebDAV `PROPFIND` on 2026-11-24.

| Path | What it is | Written by | Read it with |
|---|---|---|---|
| `ducklake/neon_catalog.ducklake`, `ducklake/data/` | The [catalog lake](catalog-lake.md): NEON API metadata and the file inventory, `ducklake`-extension format | `catalog build`, uploaded by `mirror` (catalog file last) | `ATTACH 'ducklake:...' (READ_ONLY, DATA_PATH ..., OVERRIDE_DATA_PATH true)` |
| `sites/<SITE>/ducklake/` | A [site lake](site-lakes.md): stacked data tables of one site. Holds `neon_<site>.ducklake` and `data/`; streamed sites add `catalogs/` and `CURRENT` | the site stages; `stream.push_lake` for streamed sites | `ATTACH`, as above |
| `sites/<SITE>/raw/<DP>/<YYYY-MM>/<file>` | NEON's original files, each with its full AVU profile | the upload stage | plain HTTPS GET |
| `sites/<SITE>/reports/` | JSON and CSV stage reports: `plan.json`, `list_summary.json`, `stack_summary.json`, `terms.json`, `verify.json` and others | each stage | `read_json`, or any JSON reader |
| `sites/<SITE>/cards/`, `sites/<SITE>/curation/` | Product and site cards for curation; Opus traces, proposals and `review.md` | `site cards`, `curate`, `validate` | HTTPS GET |
| `sites/<SITE>/.mesa-journal/` | Versioned backups of the site's local journal, behind a `CURRENT` pointer (streamed sites) | `journal push` | internal; for restoring a site on another VM |
| `.mesa/ducklake/snapshot_<id>.parquet` | The [AVU history](avu-history.md): plain Parquet written by mesa-ducklake, not an extension lake | mesa-ducklake, called by the recorder or by direct-mode history | `read_parquet` |
| `.mesa/spool/<vm>/<batch>.parquet` + `<batch>.json` | AVU history batches a streaming run or worker VM wrote, waiting for the recorder | `avu.history` in spool mode | `read_parquet`, `read_json` |
| `.mesa/spool-done/<vm>/` | Batches the recorder has already recorded into `.mesa/ducklake/`; kept, never deleted | `neon-ducklake recorder` | `read_parquet`, `read_json` |
| `api/runs/<runId>/sealed/` | The sealed NEON API harvest (the catalog build's only input), its manifest and fetch log | `mirror --what api` | HTTPS GET |
| `ref/` | OBO Foundry registry and ontology map exports | `mirror` | HTTPS GET |
| `src/`, `sql/`, `scripts/`, `docs/`, `curation/`, `tests/`, `pyproject.toml`, `uv.lock`, `README.md` | A copy of this repository's code | `mirror --what code` | HTTPS GET |
| `web/index.html` | One HTML file (66,411 bytes, 2026-10-02) | not found in this repository's code | HTTPS GET |

On 2026-11-24, `sites/` held two sites, **SRER** and **KONZ**. `.mesa/spool/sparky-1/` was empty
and `.mesa/spool-done/sparky-1/` held 57 recorded batches. See [AVU history](avu-history.md) for
how a batch moves from `spool` to `spool-done`.

The project root carries `mesa.enabled=true` and `mesa.project.kind=ducklake`. Each
`sites/<SITE>` collection carries a `mesa.ducklake.catalog` pointer and is never itself
MESA-enabled (README, "Data layout"; the AVUs themselves were not read for this page).

## Two different kinds of "ducklake"

| | Catalog and site lakes | `.mesa/ducklake/` |
|---|---|---|
| Format | DuckDB `ducklake` extension: a `.ducklake` catalog file (a DuckDB database of lake metadata) plus Parquet in `data/` | Plain Parquet, one file per MESA snapshot |
| Catalog | The `.ducklake` file next to the data | The MESA catalog on the recording host; not published |
| Open with | `ATTACH 'ducklake:<url>' AS x (READ_ONLY, DATA_PATH '<url>/data/', OVERRIDE_DATA_PATH true)` | `read_parquet([...])` |
| Holds | NEON metadata, stacked NEON data | AVU add/delete events with provenance |

mesa-ducklake follows the DuckLake *pattern* (a separate catalog plus Parquet files), but it does
not use the `ducklake` extension, so there is no `.ducklake` file to attach. Its Parquet rows carry
their own `project_id`, `snapshot_id`, `ts`, `actor` and `source`, so they can be read without the
catalog. See mesa-ducklake's
[reading a project's history without the catalog](https://github.com/idss-mesa/mesa-ducklake/blob/main/docs/deploy/per-project-storage.md#reading-a-projects-history-without-the-catalog).

## Reading tips

- **DuckDB.** Install the `httpfs` and `ducklake` extensions once (`INSTALL httpfs; INSTALL ducklake;`).
  DuckDB 1.5.5 and 2.0.0 both read the catalog lake (checked 2026-11-24).
- **Python.** `uv run --with duckdb==1.5.5 --with pytz python`; fetching `TIMESTAMPTZ` values into
  Python needs `pytz`.
- **Time zone.** Timestamps are stored in UTC. Run `SET TimeZone = 'UTC';` to print them in UTC.
- **Listing.** HTTP has no glob. List a collection with
  `curl -s -X PROPFIND -H 'Depth: 1' <collection URL>/`.
- **Flaky reads.** The Data Store occasionally returned an empty body on 2026-11-24. Retry, or set
  `SET http_retries = 10;`.
- **Big scans.** For repeated queries over a site lake, copy its `ducklake/` folder (the
  `.ducklake` file and `data/` together) with `gocmd get` or `iget -r` and attach the copy.
