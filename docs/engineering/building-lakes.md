---
type: Explanation
title: "How lakes are built and published"
description: "Why neon-ducklake builds every lake on local disk and uploads it, how a build ends (one snapshot, CHECKPOINT, DETACH, no .wal), the upload order with checksums and the catalog file last, versioned site-lake catalogs behind CURRENT, and how AVU history reaches .mesa/ducklake through the spool and the recorder."
tags:
  - neon-ducklake
  - engineering
  - ducklake
  - publishing
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: catalog-lake-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/catalog_lake.py"
    title: "catalog_lake.py: build, CHECKPOINT, DETACH, .wal check"
    author: "team:idss-mesa"
  - id: mirror-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/mirror.py"
    title: "mirror.py: replica-aware, checksum-verified uploads; catalog files last"
    author: "team:idss-mesa"
  - id: site-lake-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/site_lake.py"
    title: "site/site_lake.py: site-lake commits"
    author: "team:idss-mesa"
  - id: upload-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/upload.py"
    title: "site/upload.py: versioned catalogs and CURRENT"
    author: "team:idss-mesa"
  - id: recorder-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/avu/recorder.py"
    title: "avu/recorder.py"
    author: "team:idss-mesa"
  - id: mesa-ducklake-duckdb2
    resource: "https://github.com/idss-mesa/mesa-ducklake/blob/main/docs/dev/duckdb-2-migration.md"
    title: "mesa-ducklake: DuckDB 2.0 migration, and why the ducklake extension is not adopted"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-05-24T00:00:00Z"
---

# How lakes are built and published

## Built locally, then uploaded

Every lake is built on local disk and then uploaded. Nothing is written in place on the Data
Store, because DuckDB cannot write to iRODS (README, "How the lakes get there"). Readers therefore
always see a complete, closed lake.

### The catalog lake build

`neon-ducklake catalog build` (`catalog_lake.build`) does this:

1. Refuses to build unless the harvest run's `fetch_manifest.json` says `complete` (override:
   `--allow-partial`) and the sealed manifest lists no refused outputs.
2. Flattens `api/runs/<runId>/sealed/` into typed staging tables (`flatten.TABLES` defines every
   table: columns, key, group, description).
3. Opens **one** `BEGIN ... COMMIT`. Every table is a hash-keyed upsert: `rowHash` is the md5 of
   the row's JSON; lake rows whose hash is not staged are deleted, staged rows whose hash is not in
   the lake are inserted. Refreshes therefore produce real `ducklake_table_changes` diffs.
4. Inside the transaction it asserts key uniqueness, non-NULL keys and zero signed URLs
   (`X-Goog`) in any text column. A failure rolls back (exit 3).
5. Writes one `catalog_builds` row and the `table_catalog` rows, sets the commit message
   (`catalog build <runId>`, author, and the insert/delete counts as `extra_info`) and commits:
   exactly one DuckLake snapshot per build. Lake options: zstd Parquet, no data inlining
   (`data_inlining_row_limit = 0`), commit messages required.
6. Runs `CHECKPOINT __ducklake_metadata_neon` and `DETACH neon`. If a `.wal` file remains next to
   the catalog file, the build fails (status `wal-remains`, exit 4).

`--dry-run` rolls back instead of committing and, against a new lake, leaves nothing behind.

### Site lakes

Each site stage commits its own snapshot with a message (for example
`stack SRER DP1.00002.001 SAAT_30min`; streamed chunks write `stream stack ...`). Upserts use
`MERGE INTO` on logical keys and touch only changed rows. Before an upload the lake is
checkpointed and detached (`site_lake.checkpoint_detach`). The site lake records an absolute
local `data/` path, which is why every reader passes its own `DATA_PATH` with
`OVERRIDE_DATA_PATH true`.

## Upload order and checksums

`mirror.push_tree` uploads a list of `(local file, iRODS path, md5)` items:

- One GenQuery per collection lists existing replicas. A file is skipped when a good replica
  already has the same size and MD5, so re-runs upload nothing new.
- Otherwise it is put with a registered checksum (`REG_CHKSUM_KW`), then size and checksum are
  verified.
- **`*.ducklake` catalog files go last**, and only when every other upload succeeded. A reader
  never sees a catalog that points at Parquet files that are not there yet.
- Transient iRODS errors are retried with backoff; concurrency adapts (AIMD) and stays under the
  shared iRODS connection budget. Nothing is ever deleted remotely; orphans are reported.

`neon-ducklake mirror --what code,api,lake` uploads in that order, so the catalog lake's
`.ducklake` file is the last object of a mirror run.

## Versioned site-lake catalogs (streamed sites)

A streamed site pushes its lake after every chunk. `stream.push_lake`:

1. checkpoints and detaches the local lake and uploads the new `data/**` files;
2. uploads the catalog as `catalogs/neon_<site>.<vkey>.ducklake`, written once and never
   overwritten (`<vkey>` = last snapshot id, a hash of the snapshot key, and the md5);
3. writes the pointer `CURRENT` (JSON with the catalog name, md5, snapshot id, VM and time) as
   `CURRENT.new-<id>`, reads it back, and moves it over `CURRENT`;
4. refreshes the canonical `neon_<site>.ducklake` for readers that predate versioning.

Before each write it checks that this VM still holds the site's claim. A catalog guard refuses to
publish when the Data Store holds a lake this host has not seen. A crash at any point leaves the
previous version readable. Details: [STREAMING, "The site lake on the Data Store"](../STREAMING.md#the-site-lake-on-the-data-store).

## AVU history

AVUs are applied to iRODS by a journaled bulk writer (`avu/bulk.py`): intent, applied, recorded.
It only removes AVUs it owns and never touches `ipc*`, `mesa.enabled` or other writers' AVUs.
History then reaches the project's `.mesa/ducklake/` collection in one of two ways:

- **Direct** (`--no-stream` default): the run records batches of up to 250,000 rows into the MESA
  catalog under its single-writer lock, one snapshot per batch.
- **Spool** (every streaming run and worker): the run uploads `<batch>.parquet` and `<batch>.json`
  to `.mesa/spool/<vm>/` and never opens the MESA catalog. `neon-ducklake recorder --watch`, on
  the host that owns the MESA catalog, coalesces batches into snapshots of up to 250,000 rows
  (recording when a group has at least 200,000 rows, is older than 12 hours, or on `--flush`),
  then moves the recorded batches to `.mesa/spool-done/<vm>/`. A budget guard refuses to record
  past 900 snapshots.

In both cases mesa-ducklake writes the snapshot's Parquet to a local cache, pushes it to
`.mesa/ducklake/snapshot_<id>.parquet` with a checksum check, and commits the snapshot in its
catalog only after the push. The full walk-through, with what the collection held on 2026-11-24,
is in [AVU history](../data/avu-history.md#how-history-reaches-mesaducklake).

## Why `.mesa/ducklake` is not an extension lake

The catalog and site lakes use DuckDB's `ducklake` extension: a `.ducklake` catalog file plus
Parquet, opened with `ATTACH 'ducklake:...'`. The AVU history in `.mesa/ducklake/` is written by
[mesa-ducklake](https://github.com/idss-mesa/mesa-ducklake), which implements the DuckLake
*pattern* itself: its own catalog (Postgres, or a DuckDB file such as
`$MESA_HOME/ducklake/catalog.duckdb`) plus one plain Parquet file per snapshot. There is no
`.ducklake` file to attach; read the files with `read_parquet`.

mesa-ducklake's own docs give the reason it has not adopted the extension: DuckLake writes data
files through DuckDB file systems, and the iRODS Data Store has none, so its push-before-commit
protocol to `<project>/.mesa/ducklake/` would need rework
([mesa-ducklake, DuckDB 2.0 migration](https://github.com/idss-mesa/mesa-ducklake/blob/main/docs/dev/duckdb-2-migration.md)).
