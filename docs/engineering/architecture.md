---
type: Architecture
title: "Architecture"
description: "neon-ducklake's two phases (the catalog lake from the NEON API, then per-site lakes with full AVUs), the stages of a streamed and a whole-site run, and the local and Data Store layouts."
tags:
  - neon-ducklake
  - engineering
  - architecture
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: readme
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/README.md"
    title: "README: architecture, stages, data layout"
    author: "team:idss-mesa"
  - id: run-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/run.py"
    title: "run.py: the orchestrator"
    author: "team:idss-mesa"
  - id: stream-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/stream.py"
    title: "site/stream.py: per-chunk stages"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-05-24T00:00:00Z"
---

# Architecture

neon-ducklake is a Python 3.11 package (`src/neon_ducklake/`, CLI `neon-ducklake`) that builds two
kinds of DuckLake from the NEON Data API and publishes them, with full AVU metadata, as one MESA
project on the CyVerse Data Store. It is research software from the University of New Mexico and
is not affiliated with NEON, Battelle or NSF.

## Two phases

- **Phase 1, the catalog lake.** `catalog fetch` harvests the NEON Data API (REST and GraphQL,
  through neon-mcp's `NeonClient`, in process) into landings under `api/runs/<runId>/`.
  `catalog seal` bundles them into `sealed/`, the build's only input. `catalog build` flattens
  them into `ducklake/neon_catalog.ducklake` in one snapshot. No bulk data is downloaded. See
  [Catalog lake](../data/catalog-lake.md).
- **Phase 2, site subtrees.** For each site, `neon-ducklake run SITE` lists every non-AOP
  basic-package file (including PROVISIONAL and release-only months), downloads it, stacks the
  tables into `sites/<SITE>/ducklake/neon_<site>.ducklake`, maps taxa, units and biomes to
  ontology terms, curates product terms with Claude Opus, writes AVUs on every collection and
  file, uploads everything to the Data Store and records the AVUs in MESA history. See
  [Site lakes](../data/site-lakes.md) and [AVU history](../data/avu-history.md).

The catalog lake's `v_site_inventory` view plans each site: one row per non-AOP product x site x
month with file counts and bytes.

```
                         NEON Data API (REST + GraphQL, API token)
                                          |
                    neon-mcp NeonClient, in-process (rate-limited, retried)
               +--------------------------+-----------------------------+
      Phase 1  | catalog fetch/seal/build               Phase 2         | run SITE...
               v                                                        v
+----------------------------------+  v_site_inventory  +------------------------------------------+
| catalog lake (DuckLake)          | -----------------> | site lakes, one per site (DuckLake)      |
| ducklake/neon_catalog.ducklake   |  plans each site   | sites/<SITE>/ducklake/neon_<site>.duck.. |
+----------------------------------+                    +------------------------------------------+
               |  mirror + chunk upload: checksum-verified puts; local copies deleted after verify
               v
+----------------------------------------------------------------------------------------------------+
| CyVerse Data Store  /iplant/home/<user>/neon-ducklake   (one MESA project, mesa.enabled=true)      |
|   AVUs on every collection and data object (journaled bulk writer)                                  |
+----------------------------------------------------------------------------------------------------+
               |  history spool (.mesa/spool/<vm>/) -> recorder: coalesced, <= 250,000 rows/snapshot
               v
+----------------------------------------------------------------------------------------------------+
| MESA AVU history (mesa-ducklake): catalog on the recorder host; Parquet in .mesa/ducklake/          |
+----------------------------------------------------------------------------------------------------+
```

(Simplified from the README's diagram.)

## Stages of a site run

`neon-ducklake run` **streams** each site by default ([STREAMING](../STREAMING.md)):

| Stage | Does |
|---|---|
| restore | Restores the site's journal and site lake from the Data Store when another VM worked on the site |
| plan, list | Previews the site from `v_site_inventory`; lists files per product with NEON `POST /data/query` |
| chunk-plan | Splits the listed files into chunks: one product, ascending months, at most 8 GB each |
| chunks | Per chunk, pipelined: download, stack (incremental), AVUs, upload (raw files, then the site lake), apply, history (spooled), verify against the Data Store, delete the local files |
| finalize | Terms, cards, curation, the site, product and lake AVUs, the final upload, apply, history, a metadata-level verify |
| cleanup | Deletes the local site-lake data, cards and curation once their Data Store copies are checked |

With `--no-stream` the orchestrator runs 13 whole-site stages: plan, list, download, stack,
terms, cards, curate, validate, upload, apply, record, verify and evict. Each belongs to a resource
class (local, net-in, cpu, llm, net-out, irods-meta, mesa), so different sites can overlap. The
SRER lake was built this way; KONZ was built whole-site and then streamed.

Every stage is idempotent, resumable and safe to kill. A stage that hits a transient condition
exits 4 and is retried with backoff. Several CyVerse Discovery Environment VMs can stream
different sites at once; they coordinate only through the Data Store (site claims, journal
backups, the history spool). See [Workers on DE VMs](../DE_SANDBOX.md).

## Layout

Local (`NEON_DUCKLAKE_ROOT`, default the checkout):

```
src/ tests/ sql/ scripts/ docs/        code (git)
curation/                              prompts, MCP config template, generic/<DP>.validated.json (git)
ref/                                   OBO Foundry registry, ontology_maps/*.parquet exports (git)
api/runs/<runId>/{raw,stage,sealed}/   harvest landings and sealed bundles; api/LATEST
ducklake/neon_catalog.ducklake, data/  Phase-1 catalog lake
sites/<SITE>/raw/<DP>/<YYYY-MM>/<file> NEON files (deleted once verified on the Data Store)
sites/<SITE>/ducklake/                 neon_<site>.ducklake + data/, the per-site lake
sites/<SITE>/{cards,curation,reports}/ product cards, Opus traces/proposals/review.md, reports
work/state.sqlite                      journal: files, listings, uploads, avu_desired, avu_journal,
                                       batches, site_runs
work/ontology_cache.duckdb             OLS term cache, taxa/unit/NLCD/biome maps, curation results
logs/                                  run/<SITE>/<stage>.log, sweep-<UTC>.log, status.json
```

The Data Store mirrors everything except `.venv`, `work/`, `logs/` and the raw harvest landings;
see [The published collection](../data/collection.md).

## Module map

| Module | Role |
|---|---|
| `harvest.py`, `seal.py`, `flatten.py`, `catalog_lake.py` | Phase 1: fetch, seal, flatten (table specs in `flatten.TABLES`), build |
| `site/plan.py`, `site/fetch.py`, `site/stack.py`, `site/h5.py` | Phase 2 planning, listing and download, stacking, HDF5 |
| `site/terms.py`, `site/cards.py`, `site/curate.py`, `site/validate.py` | Ontology mapping, cards, Opus curation, OLS validation |
| `site/avus.py`, `avu/vocab.py`, `avu/bulk.py` | Desired AVUs for sites and the catalog; the journaled bulk writer |
| `avu/history.py`, `avu/recorder.py` | MESA history: direct and spool backends; the spool recorder |
| `mirror.py`, `site/upload.py` | Checksum-verified uploads; the versioned site-lake catalog |
| `site/stream.py`, `site/chunks.py`, `run.py`, `worker.py`, `coord.py`, `journal_sync.py` | Streaming, orchestration, multi-VM claims and journal backups |
| `verify_catalog.py`, `verify_site.py`, `verify_mesa.py` | Verification |

Interfaces between modules are fixed in [CONTRACTS](../CONTRACTS.md). Where the design records
disagree, SCALING, CONTRACTS, STREAMING and RUNBOOK win over PLAN, and STREAMING wins on
streaming and multi-VM questions.
