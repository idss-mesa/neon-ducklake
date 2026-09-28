# Changelog

All notable changes to neon-ducklake are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-09-28

First release. It builds a NEON Data API metadata lakehouse and full-AVU site subtrees as one MESA DuckLake
project on CyVerse iRODS.

### Added

- **Phase 1: catalog harvest.**
  - `catalog fetch` harvests the whole NEON Data API in-process through neon-mcp's `NeonClient`: products,
    sites, releases and their manifests, per-release records, taxonomy, sample classes, prototype datasets,
    the documents index, named locations with history, the GraphQL schema and the non-AOP basic file
    inventory.
  - Pulls are resumable, landed atomically and rate-limited. Oversized requests are bisected. Release-only
    months come from all release manifests.
  - `catalog seal` bundles the landings.
  - `catalog build` writes the catalog DuckLake (`ducklake/neon_catalog.ducklake`) in a single snapshot,
    as a hash-keyed upsert. Every row carries `rowHash` and `loadedRun`.
  - Views: `v_site_inventory` (the Phase-2 planner), `v_pair_coverage` and `v_product_summary`.
  - `verify catalog` checks the expected counts, coverage, parse consistency and that no signed URL is
    stored.
  - `diff` reports what changed between two catalog snapshots.
- **MESA project and mirror.**
  - `enroll` creates the iRODS project in-process and enrolls it in MESA, so this session never holds the
    single-writer lock.
  - `mirror` uploads code, sealed API pulls and the catalog lake. It skips objects already present
    (replica- and checksum-aware), verifies each object after upload and uploads the catalog file last.
- **AVU engine.**
  - A journaled bulk writer (`tag`) computes `desired − current` and `journal-owned − desired` per
    collection. It applies them with paced atomic metadata calls and never touches `ipc*`, `mesa.enabled`
    or foreign AVUs.
  - MESA history is recorded in large batches: up to 250,000 rows per snapshot, with a 900-snapshot
    budget guard.
  - `recover` drains pending pushes, quarantines bad Parquet files and reconciles batches.
  - `verify mesa` compares MESA history with iRODS.
  - Phase-1 AVU vocabulary (`dc.*`, `neon.*`, `mesa.*`, `provenance.*`) on the catalog targets.
- **Phase 2: site pipeline.** `site plan|list|download|stack|terms|cards|curate|validate|upload|build-avus|evict
  <SITE>`.
  - `list` builds a per-site listing and the site DuckLake.
  - `download` verifies every file against the storage MD5. A stale NEON MD5 is flagged
    (`neon.md5Verified=false`), not treated as fatal.
  - `stack` follows neonUtilities stackByTable semantics: types come from each column's `pubFormat`, with
    supporting and lab tables, HDF5 fluxes, sensor positions, file statistics and taxa.
  - `terms` maps NCBITaxon taxa (exact, synonym, infraspecific, genus and family fallbacks), UO units and
    NLCD→ENVO classes, cached in `work/ontology_cache.duckdb`.
  - `curate` runs two isolated Claude Opus replicates per product on product cards, with only the OLS
    tools. `validate` auto-applies terms both replicates agree on and sends the rest to `review.md`.
  - The site biome depends on `HABITAT`, with an ENVO water-body feature at aquatic sites. It is decided
    by a site-level two-replicate pass.
  - `build-avus` builds the full per-level and per-file AVU profile, with canonical `datacite.*` on site
    and product collections and release DOIs as units.
  - `verify <SITE>` checks listing, local files, iRODS, AVU completeness, stacking, MESA and ontology
    terms.
- **Scale to all 81 sites.**
  - `run` is a resumable multi-site orchestrator. It uses resource classes, a disk guard, a cap on sites
    in flight, sample mode (`--sample N`) and graceful SIGINT/SIGTERM handling.
  - `status` prints a site × stage table with totals and writes `logs/status.json`.
  - `site evict` deletes a raw file only after it is verified in iRODS and recorded in MESA.
  - Adaptive (AIMD) concurrency: uploads learn a connection ceiling, downloads use 3 clients with up to
    24 streams, and AVU apply runs at 5–60 calls per second. A cross-process iRODS connection budget caps
    connections across processes.
- **Operations.**
  - `preflight` runs read-only readiness checks.
  - `scripts/with-secrets.sh` exports keyring secrets and never echoes them.
  - `scripts/full_sweep.sh` runs the full sweep: preflight, plan and size/time estimate, confirmation,
    optional catalog refresh, and optional tmux or nohup detach.
  - `scripts/sweep_status.sh` monitors a sweep.
  - `sql/attach_all.sql` attaches every lake read-only.
- **Configuration.** The data root, iRODS root, CyVerse user, MESA install and precedent lake are set by
  environment variables. neon-mcp, mesa-mcp and mesa-ducklake are pinned to GitHub commits.
- **Documentation.** README, the approved design (`docs/PLAN.md`), scaling notes, internal contracts and
  a runbook.
- **Tests.** A hermetic pytest suite (over 500 tests) covering the flattener, filename classification, the AVU
  engine, history batching and recovery, the stacker, ontology terms, curation validation, the orchestrator,
  eviction safety and adaptive tuning.

[0.1.0]: https://github.com/idss-mesa/neon-ducklake/releases/tag/v0.1.0
