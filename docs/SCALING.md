# Scaling the site pipeline to all 81 NEON sites

Status: design, 2026-09-27. Measured from the catalog lake (snapshot 1) and the SRER dry run.

## What varies across sites

| Habitat (sites.siteProperties['HABITAT']) | Sites | Products (non-AOP) | Basic size | Files |
|---|---|---|---|---|
| Terrestrial | 47 | 47–60 (TIS/TOS) | 29–101 GB | 23k–65k |
| Aquatic Wadeable Stream | 24 | 51–61 (mostly AIS/AOS) | 3–10 GB | 16k–36k |
| Aquatic Lake | 7 | 52–61 | 8–13 GB | 26k–34k |
| Aquatic Non-wadeable Stream | 3 | 46–54 | 5–9 GB | 21k–28k |

- **Totals:** about 3.2M files and 4.03 TB (basic). With the full AVU profile (~20 per file) that is roughly 65M AVUs and history rows.
- **Aquatic `TypeEco` is wrong.** Every aquatic site carries a copied `TypeEco` ("lightly grazed North American natural shortgrass steppe"). Aquatic logic must use `HABITAT`, never `TypeEco`.
- **Taxa and tables.** Aquatic sites span 74 products and 225 distinct tables, plus 4,278 `lab` files (9 lab tables). Their taxa come from FISH, MACROINVERTEBRATE, ALGAE and more. Terrestrial sites add PLANT (93,698 taxa in NEON's list) and more.
- **Location differences.**
  - Alaska (BARR, BONA, DEJU, HEAL, TOOL, CARI, OKSR, TOOK), Puerto Rico (GUAN, LAJA, CUPE, GUIL) and Hawaii (PUUM) differ in biome and in which products are present.
  - NLCD land-cover rows exist only for sites NEON covers, and not for aquatic sites. Aquatic sites have watersheds instead.

## Hard limits at 81-site scale → decisions

| Limit | Decision |
|---|---|
| Local disk 3.3 TB < 4.03 TB | **Stream sites.** `evict` deletes a site's raw files once each is verified in iRODS with a good replica, a matching checksum and AVUs present. Lakes, cards and reports stay local. Peak usage ≈ the largest site (~101 GB) × sites in flight. |
| MESA `_ENSURE_CACHED_LIMIT = 1000` snapshots (no compaction) | **≤ 250,000 rows per history batch** (≈ 3 min staging), so about 4–5 snapshots per site and ≈ 400 for all sites. A **budget guard** refuses to record when the project would pass 900 snapshots. |
| mesa-ducklake staging 1,380 rows/s | ≈ 12 min per site under the MESA lock. Acceptable. Upstream fix (Arrow or CSV insert in `LakeStore.write_changes`) proposed separately. |
| Per-file iRODS overhead (~0.5 s/file at C=8) | **Adaptive upload concurrency** (AIMD on error rate and per-file latency, C from 8 to 32, total connection budget ≤ 32). Per-object verify query. Canary at start. |
| AVU apply at 20 calls/s | **Adaptive rate** (AIMD on p95 latency: grow while p95 < 250 ms, halve on p95 > 1 s or errors; ceiling 60/s). |
| GCS downloads | Up to 3 download clients × 8 connections. Bytes/s are logged. |
| Curation cost | Generic per product, cached in `curation/generic/`. About 118 products × 2 Opus runs, once. Site biome uses one small 2-replicate Opus run per site with the same agreement policy. |
| Site biome | `HABITAT`-aware. Aquatic sites get an ENVO water-body feature (stream, lake, river) plus a freshwater biome. Terrestrial sites use `TypeEco` + NLCD + domain + lat/lon/elevation. Everything goes through the Opus two-replicate agreement; the heuristic is only a hint. |

## Orchestrator (`neon-ducklake run`)

- **Invocation:** `neon-ducklake run SITE [SITE…] | --all | --habitat Terrestrial|Aquatic* [--sample N] [--stages …] [--no-evict]`
- **Stages:** plan → list → download → stack → terms → cards → curate → validate → upload → apply → record → verify → evict.
- **Persistence:** `site_runs(site, stage, status, attempt, started_at, finished_at, info)` in `work/state.sqlite`. Every stage is idempotent, so a restart resumes at the first stage not marked ok.
- **Resource classes** let sites overlap safely:

| Class | Stages | Concurrency |
|---|---|---|
| net-in | list / download | 2 sites |
| cpu | stack / terms / cards / validate | 1 site |
| llm | curate | 1 site, 4 runs |
| net-out | upload | 1 site, adaptive C |
| irods-meta | apply | 1 site |
| mesa | record | 1 site, global lock |
| local | verify / evict | any |

- **Sample mode:** `--sample N` keeps only the N most recent months per product (released and PROVISIONAL), for probing.
- **Status:** `neon-ducklake status` prints a per-site stage table with totals, and writes `logs/status.json`.
