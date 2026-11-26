---
type: Dataset
title: "AVU history (.mesa/ducklake)"
description: "Dataset card for the MESA AVU history of the neon-ducklake project: 104,964 add/delete events on 11,393 Data Store paths in six plain Parquet snapshot files, how they get there (journal, spool, recorder, mesa-ducklake), their columns, and how to read them without the MESA catalog."
tags:
  - neon-ducklake
  - dataset
  - avu
  - metadata-history
  - mesa-ducklake
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: history-files
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/.mesa/ducklake/"
    title: "The published snapshot Parquet files (read on 2026-11-24)"
    author: "team:idss-mesa"
  - id: spool-done
    resource: "https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/.mesa/spool-done/sparky-1/"
    title: "Recorded spool batches (57 manifests read on 2026-11-24)"
    author: "team:idss-mesa"
  - id: recorder-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/avu/recorder.py"
    title: "avu/recorder.py: spool to MESA history"
    author: "team:idss-mesa"
  - id: history-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/avu/history.py"
    title: "avu/history.py: direct and spool history backends"
    author: "team:idss-mesa"
  - id: mesa-ducklake
    resource: "https://github.com/idss-mesa/mesa-ducklake"
    title: "mesa-ducklake: AVU history for MESA projects"
    author: "team:idss-mesa"
  - id: mesa-ducklake-no-catalog
    resource: "https://github.com/idss-mesa/mesa-ducklake/blob/main/docs/deploy/per-project-storage.md#reading-a-projects-history-without-the-catalog"
    title: "mesa-ducklake: reading a project's history without the catalog"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-02-15T00:00:00Z"
---

# AVU history

Every AVU (attribute, value, unit) that neon-ducklake writes on the Data Store is also recorded
as an event in the project's MESA history. [mesa-ducklake](https://github.com/idss-mesa/mesa-ducklake)
writes that history as plain Parquet files in the project's `.mesa/ducklake/` collection. Each
row is one `add` or `delete` of one AVU on one path at one time, with its provenance.

All numbers on this page come from the published files, read on 2026-11-24.

## At a glance

| | |
|---|---|
| Location | `https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/.mesa/ducklake/snapshot_<id>.parquet` |
| Files | `snapshot_5` to `snapshot_10` (six files, 1,086,864 bytes in total) |
| Format | Plain Parquet, one file per MESA snapshot. **Not** a `ducklake`-extension lake: there is no `.ducklake` catalog file here |
| Catalog | The MESA catalog (`$MESA_HOME/ducklake/catalog.duckdb`) on the host that ran the recorder. It is not published |
| Rows | 104,964 events on 11,393 distinct paths, one `project_id` (`8a83b1f3-e50f-4530-92ed-4135985a4761`), one actor |
| Coverage | paths under `sites/KONZ` (63,680 rows), `sites/SRER` (41,239) and `ref` (45). No rows for the project root, `ducklake/` or other Phase-1 targets |
| Time range | 2026-09-28 01:49 UTC to 2026-10-04 19:43 UTC |
| Writers | `source` is always `neon-ducklake:site.avus` or one of its sub-sources (`.curation`, `.datacite`, `.envo`, `.ncbitaxon`, `.uo`) |

## How history reaches `.mesa/ducklake/`

The history has two paths in, chosen by `NEON_DUCKLAKE_HISTORY_MODE`:

1. **Journal.** Each AVU the bulk writer applies to iRODS is journaled locally first
   (`work/state.sqlite`): intent, applied, then recorded.
2. **Direct mode** (the default for `--no-stream` runs). The applying host records journal rows
   straight into the MESA catalog, in batches of up to 250,000 rows, one snapshot per batch, under
   the MESA catalog's single-writer lock.
3. **Spool mode** (every streaming run, and every worker VM). The run writes each batch as
   `<batch_id>.parquet` plus a `<batch_id>.json` manifest, uploads both checksum-verified to
   `.mesa/spool/<vm>/` (Parquet first, manifest last) and never opens the MESA catalog.
4. **Recorder.** `neon-ducklake recorder --watch` runs on the one host that owns the MESA catalog.
   Each pass lists `.mesa/spool/*/`, skips batches already in a committed snapshot, and coalesces
   the rest, in order, into groups of at most 250,000 rows. A group is recorded when it holds at
   least 200,000 rows, its oldest batch is older than 12 hours, or `--flush` is given. Each batch
   is checked against its manifest (sha256 and row count), and rows already recorded under another
   batch id are skipped. Each group becomes one snapshot with the note
   `neon-ducklake spool batch=<id>[,<id>...] n=<batches> rows=<rows>`.
5. **mesa-ducklake** writes the snapshot's Parquet to its local cache, pushes it to
   `.mesa/ducklake/snapshot_<id>.parquet` with a checksum check, and only then commits the
   snapshot in its catalog (push-before-commit).
6. After the commit the recorder moves the batch's two files to `.mesa/spool-done/<vm>/`.
   Spool files are never deleted. A batch whose Parquet does not match its manifest stays in
   `.mesa/spool/<vm>/`.

What the collection showed on 2026-11-24:

- `.mesa/spool/sparky-1/` was empty: no batch was waiting.
- `.mesa/spool-done/sparky-1/` held 57 batches (114 files), all with `scope` KONZ, created
  2026-09-29 13:44 to 2026-10-04 19:43 UTC. Their manifests sum to 63,682 rows, which equals the
  row count of `snapshot_10`. So the recorder coalesced those 57 KONZ batches into snapshot 10.
- Snapshots 5 to 9 hold SRER and `ref` rows from 2026-09-28 and match no spool batch. That fits
  direct mode, which SRER's whole-site run used by default; the MESA catalog notes that would
  confirm it are not published.

A spool batch's Parquet has the AVU columns plus `batch_id`, `scope` (the site), `vm` and
`journal_id`, and no `project_id`, `snapshot_id`, `via_ticket` or `rule_invocation`. Its manifest
holds `format` (`neon-ducklake-spool/1`), `batchId`, `scope`, `vm`, `host`, `rows`, `paths`,
`sha256`, `md5`, `bytes`, `createdAt`, `actor` and `journalIds`.

## Columns

The snapshot files have the 13 columns of mesa-ducklake's `avu_changes` fact table:

| Column | Type | Meaning |
|---|---|---|
| `project_id` | VARCHAR (UUID) | The MESA project; one value for the whole neon-ducklake collection |
| `snapshot_id` | BIGINT | The MESA snapshot this event belongs to; equals the `<id>` in the file name |
| `irods_path` | VARCHAR | Full Data Store path the AVU is bound to |
| `target_type` | VARCHAR | `collection` or `data_object` here |
| `attribute` | VARCHAR | AVU attribute, for example `neon.release`, `datacite.title`, `ncbitaxon.<taxon>` |
| `value` | VARCHAR | AVU value |
| `unit` | VARCHAR | AVU unit; often a CURIE or DOI, for example `doi:10.48443/p69b-5e50` on `neon.release` |
| `op` | VARCHAR | `add` or `delete` |
| `actor` | VARCHAR | The iRODS user who made the change |
| `ts` | TIMESTAMP WITH TIME ZONE | When the change was applied (UTC) |
| `source` | VARCHAR | The writer, here `neon-ducklake:site.avus[.<sub>]` |
| `via_ticket` | VARCHAR | iRODS ticket used, if any; NULL in every row here |
| `rule_invocation` | VARCHAR | iRODS rule that emitted the change, if any; NULL in every row here |

The AVU vocabulary itself (`neon.*`, `dc.*`, `datacite.*`, `mesa.*`, `provenance.*`, and
ontology attributes such as `uo.*`, `ncbitaxon.*`, `envo.*`) is defined in the
[design plan](../PLAN.md) and its amendments in [CONTRACTS](../CONTRACTS.md).

## How to read it

### HTTPS with DuckDB, no catalog

HTTP has no directory glob, so list the files by name. Snapshot numbers have gaps because the
MESA catalog numbers snapshots across all of its projects. To list what exists, send a WebDAV
`PROPFIND` (for example `curl -s -X PROPFIND -H 'Depth: 1' <collection URL>`).

```sql
INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';
SET http_retries = 10;
SET VARIABLE base = 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/.mesa/ducklake/';
SELECT snapshot_id, count(*) AS n, min(ts) AS first_ts, max(ts) AS last_ts
FROM read_parquet(list_transform([5, 6, 7, 8, 9, 10],
                                 i -> getvariable('base') || 'snapshot_' || i || '.parquet'))
GROUP BY 1 ORDER BY 1;
```

```text
snapshot_id | n | first_ts | last_ts
5 | 496 | 2026-09-28 01:49:19.151720+00:00 | 2026-09-28 01:49:33.998472+00:00
6 | 498 | 2026-09-28 01:49:24.453661+00:00 | 2026-09-28 01:49:40.864431+00:00
7 | 1536 | 2026-09-28 01:49:19.966993+00:00 | 2026-09-28 01:49:42.987872+00:00
8 | 433 | 2026-09-28 01:52:42.204252+00:00 | 2026-09-28 01:52:48.231917+00:00
9 | 38319 | 2026-09-28 04:29:00.540246+00:00 | 2026-09-28 04:30:25.162560+00:00
10 | 63682 | 2026-09-29 13:43:14.301568+00:00 | 2026-10-04 19:43:34.369538+00:00
```

On 2026-11-24 the Data Store sometimes answered a request for one of these files with zero bytes,
and DuckDB failed with "No magic bytes found at end of file". The run with
`SET http_retries = 10` succeeded; whether the setting caused that is not proven. Retrying the
query, or downloading the files first with `curl`, also works.

### The current AVU set of a path

This is mesa-ducklake's effective-AVU rule: for each `(attribute, value, unit)` take the latest
event, ordered by `ts` then `snapshot_id`, and keep it if it is an `add`. Partition on the whole
triple, never on the attribute alone, because iRODS allows several values per attribute.

```sql
WITH h AS (
    SELECT * FROM read_parquet(list_transform([5, 6, 7, 8, 9, 10],
                                              i -> getvariable('base') || 'snapshot_' || i || '.parquet'))
), events AS (
    SELECT attribute, value, unit, op,
           row_number() OVER (PARTITION BY attribute, value, unit ORDER BY ts DESC, snapshot_id DESC) AS rn
    FROM h
    WHERE irods_path = '/iplant/home/tswetnam/neon-ducklake/sites/SRER/raw/DP1.00002.001'
)
SELECT attribute, value, unit
FROM events
WHERE rn = 1 AND op = 'add' AND attribute IN ('neon.release', 'neon.fileCount', 'neon.siteMonths')
ORDER BY attribute;
```

```text
attribute | value | unit
neon.fileCount | 50 | count
neon.release | RELEASE-2026 | doi:10.48443/p69b-5e50
neon.siteMonths | 5 | count
```

Add `AND ts <= TIMESTAMPTZ '...'` to the `WHERE` of `events` for the set at an earlier time.

### Who wrote what

```sql
SELECT source, op, target_type, count(*) AS n
FROM read_parquet(list_transform([5, 6, 7, 8, 9, 10],
                                 i -> getvariable('base') || 'snapshot_' || i || '.parquet'))
GROUP BY ALL ORDER BY n DESC
LIMIT 5;
```

```text
source | op | target_type | n
neon-ducklake:site.avus | add | data_object | 81581
neon-ducklake:site.avus.datacite | add | collection | 10806
neon-ducklake:site.avus | add | collection | 7941
neon-ducklake:site.avus.uo | add | collection | 2423
neon-ducklake:site.avus.ncbitaxon | add | data_object | 1065
```

### A recorded spool batch

```sql
SELECT batchId, scope, vm, rows, createdAt
FROM read_json('https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/.mesa/spool-done/sparky-1/02df8245-d193-4ced-bcdf-b7cb56ee70fa.json');
```

```text
batchId | scope | vm | rows | createdAt
02df8245-d193-4ced-bcdf-b7cb56ee70fa | KONZ | sparky-1 | 181 | 2026-09-29 15:21:42.549759
```

### Through MESA tools

mesa-mcp's `mesa_ducklake_history` and `mesa_ducklake_time_travel` tools read this history through
the MESA catalog the server is configured with. They work only where the project is registered in
that catalog, which today is the MESA install of the host that ran the recorder. Anonymous readers
should use `read_parquet` as above. No public MESA Quack endpoint for this catalog is documented.

## Freshness

- The newest event is at 2026-10-04 19:43:34 UTC (`snapshot_10`). The spool was empty on
  2026-11-24, so no recorded change was waiting.
- The history grows with every pipeline run that writes AVUs. A new snapshot appears as a new
  `snapshot_<id>.parquet`; existing files are never rewritten.
- How to tell from the data: the largest `snapshot_id` and `max(ts)`, and the newest files in
  `.mesa/spool-done/<vm>/`.

## Known limitations

- **No catalog.** Without the MESA catalog you cannot see snapshot notes (batch ids), and you
  cannot tell whether a file's snapshot was committed. mesa-ducklake pushes a snapshot's Parquet
  before it commits the snapshot, so a crash between the two can leave a file whose snapshot is
  later committed by recovery, or marked `failed`. The six files here all predate 2026-10-05.
- **Coverage.** Only site subtrees and `ref` are in these files. The Phase-1 catalog AVUs are not;
  why is not recorded here.
- **Partial sites.** The history covers the AVUs of the SRER and KONZ sample runs only.
- **AVUs that other tools write** (for example `imeta` by hand) are not in this history unless a
  MESA rule or tool recorded them.
