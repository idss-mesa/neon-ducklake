# neon-ducklake

[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Python 3.11](https://img.shields.io/badge/python-3.11-3776ab)](pyproject.toml)
[![DuckDB 1.5.5 + DuckLake](https://img.shields.io/badge/DuckDB-1.5.5%20%2B%20DuckLake-fff000)](https://ducklake.select/)

A metadata lakehouse of the [NEON Data API](https://data.neonscience.org/data-api/) together with fully
annotated copies of NEON's field-site data on [CyVerse](https://cyverse.org/) iRODS. Everything is managed
as a single [MESA](https://github.com/idss-mesa/mesa-mcp) DuckLake project.

The National Ecological Observatory Network (NEON) publishes about 200 data products from 81 field sites.
neon-ducklake builds two things from them:

- **Catalog lake (Phase 1).** It harvests the NEON Data API into one [DuckLake](https://ducklake.select/):
  every product (AOP included), site, release, availability month, named location with its history,
  taxon, sample class, prototype dataset and document, plus an inventory of the 3.2 million non-AOP data
  files. It downloads no bulk data.
- **Site subtrees (Phase 2).** For each site it downloads every non-AOP file in the basic package,
  including PROVISIONAL and release-only months. It stacks the tables into a per-site DuckLake and mirrors
  everything to iRODS. Every collection and file gets a full set of AVUs (attribute–value–unit metadata):
  NEON identity, release DOIs, coverage, sensor positions, NCBITaxon taxa, UO units, ENVO biomes and
  Opus-curated ontology terms. The same changes are recorded in MESA's AVU history.

What it is built for:

- **Unattended runs at observatory scale.** One command runs all 81 sites: about 3.2 million files, 4 TB
  and 65 million AVUs, over roughly 4–5 days. Every stage is idempotent, resumable and safe to kill.
- **Metadata, not data.** Each site streams in bounded chunks: fetch, analyze, write AVUs, push the data
  and the stacked lake to the CyVerse Data Store, verify the Data Store copy, delete the local copy. Local
  disk never holds more than a working set (50 GB by default), so several CyVerse Discovery Environment VMs
  can work on different sites at once, coordinated only through the Data Store.
- **One MESA project, one history.** Site data lives in subtrees of that project. Every AVU the pipeline
  writes is journaled locally, spooled to the Data Store and recorded in MESA by one recorder in large,
  coalesced snapshots, so the snapshot count stays within the 900-snapshot budget.
- **Provenance end to end.** Each lake snapshot carries a commit message, and each object carries its
  NEON source URL and release DOI. The tool versions are recorded as AVUs.

Status: **0.1.0**. Phase 1 is complete: the catalog lake is built, verified and mirrored. Phase 2 passed
the SRER dry run and the SRER sample run, and it has been generalised to all 81 sites (see
[`docs/SCALING.md`](docs/SCALING.md)). The full sweep is the next step. neon-ducklake is research software
from the University of New Mexico. It is not affiliated with NEON, Battelle or NSF.

## Architecture

```
                         NEON Data API (REST + GraphQL, API token)
                                          |
                    neon-mcp NeonClient, in-process (rate-limited, retried)
               +--------------------------+-----------------------------+
      Phase 1  | catalog fetch/seal/build               Phase 2         | run SITE... (13 stages per site)
               v                                                        v
+----------------------------------+  v_site_inventory  +------------------------------------------+
| catalog lake (DuckLake)          | -----------------> | site lakes, one per site (DuckLake)      |
| ducklake/neon_catalog.ducklake   |  plans each site   | sites/<SITE>/ducklake/neon_<site>.duck.. |
| products, releases, sites,       |                    | files, site_months, stacked data tables, |
| availability, locations+history, |                    | variables, sensor_positions, h5 fluxes,  |
| taxonomy, files (3.2M), views    |                    | file_stats, file_taxa, table_catalog     |
+----------------------------------+                    +------------------------------------------+
   local root: code (git), api/runs/, ducklake/, sites/<SITE>/{raw,ducklake,cards,curation,reports}/
   journal: work/state.sqlite (files, uploads, AVU journal, history batches, site_runs); logs/
   curation: claude -p (Opus) with only mesa-mcp's OLS tools, no secrets -> curation/generic/<DP>.json
               |
               |  mirror + chunk upload: replica- and checksum-aware puts; local copies deleted after verify
               v
+----------------------------------------------------------------------------------------------------+
| CyVerse iRODS  /iplant/home/<user>/neon-ducklake            (one MESA project, mesa.enabled=true)  |
|   api/runs/<runId>/sealed/   ducklake/   sites/<SITE>/{raw,ducklake,cards,curation,reports}/  ref/ |
|   AVUs on every collection and data object: journaled bulk writer (tag --apply-only)                |
+----------------------------------------------------------------------------------------------------+
               |
               |  history spool (.mesa/spool/<vm>/) -> neon-ducklake recorder: coalesced, <= 250,000 rows/snapshot
               v
+----------------------------------------------------------------------------------------------------+
| MESA AVU history (mesa-ducklake): one history for the whole project                                |
|   catalog $MESA_HOME/ducklake/catalog.duckdb; snapshot Parquet in the project's .mesa/ducklake/    |
+----------------------------------------------------------------------------------------------------+
```

The Phase-2 orchestrator (`neon-ducklake run`) **streams** each site by default
([`docs/STREAMING.md`](docs/STREAMING.md)):

| Stage | Does |
|---|---|
| restore | Restores the site's journal and site lake from the Data Store when another VM worked on the site |
| plan, list | As below |
| chunk-plan | Splits the listed files into chunks: one product, ascending months, at most 8 GB each |
| chunks | Per chunk, in a pipeline (chunk k+1 downloads while chunk k uploads): download, stack (incremental), AVUs, upload (raw files, then the site lake), apply, history (spooled), verify against the Data Store, delete the local files |
| finalize | Terms, cards, curation, the site/product/lake AVUs, the final upload, apply, history, a metadata-level verify |
| cleanup | Deletes the local site lake data, cards and curation once their Data Store copies are checked |

Before a site's first stage, `run` claims it on the Data Store, so sparky-1 and worker VMs never work on
the same site; the site's journal is backed up to the Data Store after every chunk's apply, history and
delete. The site lake's catalog and the journal backup are versioned on the Data Store (a `CURRENT` pointer
is replaced last), so a VM that dies mid-write never leaves them unreadable. A stage that hits a transient
condition exits 4 and is retried with backoff instead of failing the site.

With `--no-stream`, the orchestrator takes each site through the previous 13 whole-site stages. Each stage
belongs to a resource class, so different sites can overlap safely. For example, site B downloads while
site A uploads. Stream chunk stages use the same classes.

| Stage | Does | Class (sites at once) |
|---|---|---|
| plan | Previews products, months, files and bytes from `v_site_inventory` | local |
| list | Runs `POST /data/query` per product and writes `files` and `site_months` to the site lake | net-in (2) |
| download | Downloads every listed file, verified against the storage MD5 | net-in (2) |
| stack | Builds typed, stacked tables per product × table (neonUtilities `stackByTable` semantics) | cpu (1) |
| terms | Maps taxa to NCBITaxon, units to UO, and NLCD classes and the site biome to ENVO | cpu (1) |
| cards | Writes product cards (record plus real column profiles) and a site card for curation | cpu (1) |
| curate | Runs two Claude Opus replicates per new product plus a site-level pass | llm (1) |
| validate | Validates the proposals in OLS; terms both replicates agree on are accepted, the rest go to `review.md` | cpu (1) |
| upload | Puts raw files and the site lake into iRODS | net-out (1) |
| apply | Builds the desired AVUs (`site build-avus`) and applies the difference (`tag --apply-only`) | irods-meta (1) |
| record | Records the applied AVUs in MESA history (`tag --record-only`) | mesa (1) |
| verify | Checks listing, files, iRODS, AVU completeness, stacking, MESA and ontology terms | local |
| evict | Deletes local raw files that are verified in iRODS | local |

## Requirements

- **Linux, Python 3.11 and [uv](https://docs.astral.sh/uv/).** It was built and measured on an aarch64
  host with 3.3 TB of free NVMe.
- **DuckDB 1.5.5 with the `ducklake` and `spatial` extensions installed.** The code only `LOAD`s them and
  never installs anything. See [Install](#install).
- **A NEON API token.** It is free with a NEON account: open <https://data.neonscience.org/myaccount>
  and choose **GET API TOKEN**. NEON requires it for file listings and downloads.
- **A CyVerse account with Data Store access.** The iRODS project defaults to
  `/iplant/home/<user>/neon-ducklake`.
- **A MESA local install** in `$MESA_HOME` (default `~/.mesa`). It must provide:
  - `bin/mesa-keyring-common.sh` and the keyring helpers `mesa-keyring-set` and `neon-keyring-set`;
  - `cyverse-user`;
  - a `.venv` running [mesa-mcp](https://github.com/idss-mesa/mesa-mcp);
  - the MESA catalog `ducklake/catalog.duckdb`.

  The OLS server used for curation is mesa-mcp, started from that `.venv`.
- **An unlocked login keyring** holding the NEON token (service `neon`, account `api-token`) and the
  CyVerse password. Both work inside tmux.
- **Optional: the [Claude Code](https://claude.com/claude-code) CLI (`claude`)** for the Opus curation
  stages. Without it, leave `curate` out of `--stages`. Taxa, units, NLCD classes and the heuristic biome
  proposal still come from the data, but no curated terms are applied.

## Install

```bash
git clone https://github.com/idss-mesa/neon-ducklake && cd neon-ducklake
uv sync --locked --extra dev
.venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('ducklake', 'spatial')]"
```

- **Dependencies.** None of [neon-mcp](https://github.com/idss-mesa/neon-mcp),
  [mesa-mcp](https://github.com/idss-mesa/mesa-mcp) and
  [mesa-ducklake](https://github.com/idss-mesa/mesa-ducklake) is on PyPI. `pyproject.toml` pins each to
  a GitHub commit in `[tool.uv.sources]`: the commit the MESA install's MCP servers run.
- **Shared libraries.** `[tool.uv] constraint-dependencies` keeps mcp, pydantic, structlog, httpx and
  similar libraries identical to that runtime.
- **Checks.** `neon-ducklake preflight` reports each dependency's version and commit, and warns when it
  differs from the pin.

To develop against local checkouts of the three dependencies, use one of these:

- **One-off.** Run `uv pip install -e ../neon-mcp -e ../mesa-mcp -e ../mesa-ducklake` after `uv sync`,
  then run the tools as `.venv/bin/...` or with `uv run --no-sync`. A plain `uv sync` or `uv run`
  reinstalls the pinned commits.
- **Sticky.** Replace a line in `[tool.uv.sources]` with a path source, for example
  `neon-mcp = { path = "../neon-mcp", editable = true }`, then run `uv lock && uv sync --extra dev`.
  Do not commit that change: it writes local paths into `uv.lock`.

Store the secrets once, using the MESA helpers:

```bash
~/.mesa/bin/neon-keyring-set        # NEON API token -> keyring (service neon, account api-token)
~/.mesa/bin/mesa-keyring-set        # CyVerse password -> keyring
```

## Configuration

Every command that talks to NEON, iRODS or MESA runs through `scripts/with-secrets.sh`:

- It reads the NEON token and the CyVerse password from the keyring and exports them as `NEON_TOKEN` and
  `MESA_MCP_IRODS__USER` / `MESA_MCP_IRODS__PASSWORD`. On a VM without a keyring, it takes them from the
  environment and from `~/.irods` (written by `iinit`) instead: `NEON_DUCKLAKE_SECRETS=env|keyring|auto`
  (default `auto`, per secret).
- It sets neon-mcp's concurrency (8), rate-limit wait (300 s) and log level (`warning`).
- It then `exec`s the command. It never prints a secret.
- Exit codes: 2 if the MESA install is missing, 3 if the NEON token is unavailable, 4 if the CyVerse
  password is unavailable.

The defaults need no user-specific paths. Override them with these environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `NEON_DUCKLAKE_ROOT` | the checkout | Data root: `api/`, `ducklake/`, `sites/`, `work/`, `logs/` |
| `NEON_DUCKLAKE_IRODS_ROOT` | `/iplant/home/<user>/neon-ducklake` | The iRODS project collection |
| `CYVERSE_USERNAME` | `$MESA_HOME/cyverse-user` | CyVerse user: the iRODS writer and the MESA history actor |
| `MESA_HOME` | `~/.mesa` | MESA local install |
| `NEON_DUCKLAKE_PRECEDENT_LAKE`, `_DATA`, `_VERSION`, `_IRODS` | `~/neon-downloads/...`, snapshot 90 | Optional spatial lake whose 16 `neon_*` layers the catalog build copies. `none` skips it |
| `NEON_DUCKLAKE_CLAUDE` | `~/.local/bin/claude`, else `claude` on `PATH` | Claude CLI used by `curate` |
| `NEON_DUCKLAKE_ANYJEV_PYTHON` | a mesa-anyjev `.venv` | Optional interpreter whose mesa-anyjev card parser `site cards` uses |
| `NEON_DUCKLAKE_ROWS_PER_BATCH` | 250,000 | MESA history rows per batch and per recorder snapshot |
| `NEON_DUCKLAKE_IRODS_CONNECTIONS` | 8 | iRODS connection budget shared by all neon-ducklake processes on one machine (CyVerse refuses connections above about 10 per user) |
| `NEON_DUCKLAKE_WORKSET_BYTES` | 50 GB | Cap on the local working set of stream runs: raw files plus site lakes (`run --workset-bytes`) |
| `NEON_DUCKLAKE_CHUNK_BYTES` | 8 GB | Largest chunk (`run --chunk-bytes`) |
| `NEON_DUCKLAKE_SECRETS` | `auto` | Where `with-secrets.sh` finds the secrets: `env`, `keyring` or `auto` |
| `NEON_DUCKLAKE_VM_ID` | the short host name | Names this machine's site claims, history spool and ontology export; unique per VM |
| `NEON_DUCKLAKE_HISTORY_MODE` | `spool` (stream), `direct` (`--no-stream`) | MESA history backend. Stream runs always spool; `direct` is refused there until DuckDB 2.0 multi-writer |
| `NEON_DUCKLAKE_MESA_MULTIWRITER` | off | `1` once the MESA catalog takes parallel writers (DuckDB 2.0): allows `direct` history in stream runs |
| `NEON_DUCKLAKE_CURATION_SYNC` | `auto` | Share curation claims and results through the Data Store (on for stream runs) |
| `NEON_DUCKLAKE_WAIT_TIMEOUT_S` | 21600 | How long a stage waits for the MESA catalog or another VM's curation before it fails |

## Quick start

Every command below runs from the repository root. The shell function saves typing:

```bash
nd() { scripts/with-secrets.sh .venv/bin/neon-ducklake "$@"; }

nd preflight                      # read-only readiness checks; fix every "fail" first

# Phase 1: the catalog lake (about 1.5-2.5 h, mostly the file inventory and location history)
nd catalog fetch                  # resumes the latest incomplete run; --new starts a new one, --pilot a trial
nd catalog seal                   # bundle the landings into sealed/ (the only build input)
nd catalog build                  # one DuckLake snapshot; --dry-run to check first
nd verify catalog

# The MESA project, the iRODS mirror and the Phase-1 AVUs
nd enroll                         # once: create and enroll the iRODS project
nd mirror                         # code, sealed API pulls and the catalog lake (--what code,api,lake)
nd tag catalog --dry-run
nd tag catalog                    # apply the Phase-1 AVUs and record them in MESA history

# Phase 2: one site
nd site plan SRER                 # read-only preview: products, months, files, bytes, what is excluded
nd run SRER --dry-run             # site order, resume point and the exact commands; runs nothing
nd run SRER --no-stream --sample 2 --stages plan,list,download,stack,terms,cards   # optional local-only probe
nd run SRER                       # stream the site: restore ... chunks ... cleanup; re-run to resume
nd recorder --flush               # record the spooled MESA history now (full_sweep.sh runs `recorder --watch`)
.venv/bin/neon-ducklake status    # site x stage table and totals (needs no secrets)
```

`neon-ducklake --help` and `neon-ducklake <command> --help` list every option. Each stage can also be run
by hand, for example `nd site list SRER --sample 2`, `nd site evict SRER --dry-run` or `nd verify SRER`.

## Full sweep

`scripts/full_sweep.sh` runs every site, or a selection, with the checks an unattended multi-day run needs.
In order, it:

1. **Checks the setup.** It checks the checkout and `.venv` and refuses to start while another
   orchestrator is running.
2. **Runs `preflight`** through `with-secrets.sh`. Any hard failure, or a locked keyring, aborts the sweep.
3. **Prints the plan.** It shows `neon-ducklake run … --dry-run` and a size and time estimate: files, GB,
   upload, apply and record hours, MESA snapshots against the budget, and free disk against the disk
   guard. The estimate uses the measured rates in `docs/SCALING.md` and counts only the stages each site
   still has to run.
4. **Asks for confirmation**, unless you pass `--yes`.
5. **With `--refresh-catalog`, refreshes the catalog.** It runs `catalog fetch --new`, `catalog seal`,
   `catalog build`, `verify catalog`, `diff`, and `mirror --what api,lake`.
6. **Starts the MESA history recorder** (`neon-ducklake recorder --watch --interval 300 --min-rows 200000
   --max-rows 250000 --max-age-h 12`) in the tmux session `neon-recorder`, unless one already runs or you pass
   `--no-recorder`. Stream runs spool their history; the recorder writes it into MESA in few, large
   snapshots.
7. **Runs `neon-ducklake run …`** through `with-secrets.sh`, then prints `neon-ducklake status`. Sites
   that are done or claimed by a worker VM are skipped.

All output also goes to `logs/sweep-<UTC>.log`, and the script never prints a secret.

```bash
scripts/full_sweep.sh --dry-run                  # plan + estimate for all 81 sites; no secrets, starts nothing
scripts/full_sweep.sh --detach                   # preflight, plan, confirm, then run in tmux session neon-sweep-<UTC>
scripts/full_sweep.sh --habitat aquatic --yes    # the 34 aquatic sites, no prompt
scripts/full_sweep.sh --sites 'SRER HARV' --no-stream --sample 2 --no-evict   # a local-only probe
scripts/full_sweep.sh --worker --detach          # on a CyVerse DE VM: claim and stream sites one by one
scripts/full_sweep.sh --refresh-catalog --from-stage list --detach   # after a new NEON release
scripts/full_sweep.sh --all -- --order size --threads 12            # options after -- go to `neon-ducklake run`
scripts/sweep_status.sh                          # progress: table, chunks, working set, spool, recorder, disk, MESA budget
```

| Option | Effect |
|---|---|
| `--all` (default), `--sites 'A B'`, `--habitat X` | Selects the sites. `--all` orders aquatic sites first, smallest first, then terrestrial sites by size. `--sites` keeps your order. |
| `--stream` (default) / `--no-stream` | Streams each site in bounded chunks, or runs the previous whole-site stages. |
| `--chunk-bytes N`, `--workset-bytes N` | Largest chunk (8 GB) and the cap on the local working set (50 GB). |
| `--history-mode MODE` | Stream runs always spool (`direct` needs `NEON_DUCKLAKE_MESA_MULTIWRITER=1`); `--no-stream` defaults to direct. |
| `--no-recorder` | Does not start the recorder; start it yourself on the MESA-catalog host. |
| `--worker`, `--max-sites N`, `--once` | Runs `neon-ducklake worker` instead of `run` (a CyVerse DE VM). |
| `--sample N` | Probe mode: only the N most recent months per product. With `--no-stream` it never evicts. |
| `--no-evict` | `--no-stream` only: keeps raw files on disk; the evict stage is skipped. |
| `--from-stage STAGE` | Re-runs from `STAGE` even when later stages are ok. |
| `--max-in-flight N` | Caps the sites between their first and last stage (default 3). |
| `--refresh-catalog` | Harvests and mirrors a new catalog run first (about 1.5–2.5 h). |
| `--dry-run` | Prints the plan and the estimate, then exits. |
| `--yes` | Skips the confirmation prompt. |
| `--detach[=tmux\|nohup]` | Runs in the background after the checks. Uses tmux when available, otherwise nohup. |

For all 81 sites the estimate currently reads (excerpt):

```
estimate for 81 site(s) with work (mode full; rates from docs/SCALING.md 'Measured'):
  size      3,215,813 files, 4,034.1 GB (non-AOP basic package, catalog v_site_inventory)
  upload    3,215,813 files: ~89.3 h at ~10 files/s (the long pole)
  apply     3,215,813 objects: ~14.9 h at ~60 calls/s
  record    ~64,316,260 AVU rows: ~16.2 h at ~1,100 rows/s under the MESA lock, ~294 snapshot(s)
  wall      ~89.3 h to ~120.5 h (3.7-5.0 days) plus the first site's download, stack,
            terms and curation; ...
  disk      3,585.7 GB free; needs ~394.3 GB (1.3 x largest site x sites in flight; evict on)
```

To manage a running sweep:

- **Stop it.** Press Ctrl-C once in the tmux session, or send `kill -INT <pid>` to the orchestrator
  (`scripts/sweep_status.sh` shows the pid). Scheduling stops and running stages finish. A second signal
  terminates them. Every stage survives `kill -9`.
- **Resume it.** Run the same command again. Each site resumes at its first stage that is not `ok`, and
  a failed stage is retried.
- **Close other MESA writers.** Stream runs never open the MESA catalog; the recorder (and the whole-site
  `record` stage) need its single-writer lock. Close any other mesa-mcp session; they wait for the lock
  and name the process that holds it.
- **Review curation.** Terms that only one Opus replicate proposed wait in `sites/<SITE>/curation/review.md`.
  Reviews never block a run. See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) sections 5–7 for monitoring,
  recovery and review.

## Workers on several VMs

Several CyVerse Discovery Environment VMs ("AI Sandboxes") can stream different sites at the same time,
next to sparky-1. They share nothing but the Data Store: atomic site claims (`sites/<SITE>/.claim`, `.done`),
per-site journal backups (`sites/<SITE>/.mesa-journal/`), the site lakes, the history spool
(`.mesa/spool/<vm>/`), shared curation results and each VM's ontology export. Setup, secrets without a
keyring, and troubleshooting are in [`docs/DE_SANDBOX.md`](docs/DE_SANDBOX.md).

```bash
export NEON_DUCKLAKE_SECRETS=env NEON_DUCKLAKE_VM_ID=de-vm-01     # on each VM; NEON_TOKEN and iinit first
nd worker                         # bootstrap, then claim -> stream -> .done -> release, site after site
nd claims [--json]                # every site and curation claim, and the finished sites
nd claims --release SRER [--force]
nd journal push SRER              # back up a site's journal now; `journal restore SRER` brings it back
nd recorder --watch --interval 300 --min-rows 200000 --max-rows 250000 --max-age-h 12   # MESA-catalog host only
```

## Data layout

Local (`NEON_DUCKLAKE_ROOT`):

```
src/ tests/ sql/ scripts/ docs/        code (git)
curation/                              prompts, MCP config template, generic/<DP>.validated.json (git)
ref/                                   OBO Foundry registry, ontology_maps/*.parquet exports (git)
api/runs/<runId>/{raw,stage,sealed}/   harvest landings and sealed bundles; api/LATEST
ducklake/neon_catalog.ducklake, data/  Phase-1 catalog lake
sites/<SITE>/raw/<DP>/<YYYY-MM>/<file> NEON files (deleted by evict once verified in iRODS)
sites/<SITE>/ducklake/                 neon_<site>.ducklake + data/, the per-site lake
sites/<SITE>/{cards,curation,reports}/ product cards, Opus traces/proposals/review.md, plan/terms/verify reports
work/state.sqlite                      journal (mode 0600): files, listings, uploads, avu_desired,
                                       avu_journal, batches, site_runs
work/ontology_cache.duckdb             OLS term cache, taxa/unit/NLCD/biome maps, curation results
work/{mesa-cache,quarantine,locks}/    MESA snapshot cache, quarantined Parquet, process locks
logs/                                  run/<SITE>/<stage>.log, sweep-<UTC>.log, status.json, summaries
```

iRODS (`NEON_DUCKLAKE_IRODS_ROOT`) mirrors everything except `.venv`, `work/`, `logs/` and the raw
harvest landings:

```
/iplant/home/<user>/neon-ducklake/        mesa.enabled=true, mesa.project.kind=ducklake
  .mesa/ducklake/                         MESA AVU history (written by mesa-ducklake)
  api/runs/<runId>/sealed/                sealed API pulls, manifest and fetch log
  ducklake/neon_catalog.ducklake, data/   catalog lake (the .ducklake file is uploaded last)
  sites/<SITE>/                           mesa.ducklake.catalog pointer, never mesa.enabled
    raw/<DP>/<YYYY-MM>/<NEON filename>    every NEON file, with its full AVU profile
    ducklake/ cards/ curation/ reports/
  ref/ src/ sql/ scripts/ docs/ curation/ tests/
```

`sql/attach_all.sql` attaches the catalog lake and every site lake read-only, for example with
`duckdb -init sql/attach_all.sql`. Then run
`SELECT * FROM neon.v_site_inventory WHERE siteCode = 'SRER'`.

## AVU vocabulary

The authoritative definition is the plan's AVU vocabulary ([`docs/PLAN.md`](docs/PLAN.md)) with its
amendments in [`docs/CONTRACTS.md`](docs/CONTRACTS.md). In summary:

**Levels.** Each level gets its own AVUs:

- **Project and catalog targets.** `dc.*`, `neon.project`, the `neon.catalog.*` and `neon.api.*` counts,
  `mesa.*` and `provenance.*`.
- **`sites/<SITE>`.** About 90 AVUs: identity, `neon.habitat`, location, counts, temporal coverage,
  biome and feature, and DataCite.
- **Containers.** `dc.description`, `dc.type` and `provenance.generator`.
- **Product `raw/<DP>`.** The product record, releases, month ranges, tables, units, taxa, curated terms
  and DataCite.
- **Month `raw/<DP>/<YYYY-MM>`.** Release, generation date, counts, coverage and IS/OS summaries.
- **Data objects.** A profile per kind:
  - every file: the core `neon.productCode`, `siteCode`, `domainCode`, `month`, `release`, `package`,
    `fileKind` and `publicationDate`, plus `dc.source`;
  - IS data (about 22 AVUs): adds sensor position and elevation, HOR/VER/TMI, row count, coverage and
    the `finalQF` fail percentage;
  - OS data (about 33 AVUs): adds plots, protocol versions and the taxa observed in that file;
  - HDF5, lab and supporting files.
- **Lake files, cards and reports.** `mesa.ducklake.table` or `mesa.ducklake.catalog` pointers,
  `dc.format` and `neon.rowCount`.

**Naming and values.** NEON attributes are `neon.<lowerCamelCase>` only. Values are trimmed, empties
dropped, and values capped at 2,000 bytes on a sentence boundary. Long text stays in the lakes.

**Ontology AVUs** are `<prefix>.<snake_case label>`, with the CURIE as the unit. mesa-mcp's
`ontology_annotations_to_avus` builds them, and every term re-resolves in OLS as not obsolete. For
example:

- `ncbitaxon.<taxon>` = the vernacular name;
- `uo.<unit>` = `<table>.<field>`;
- `envo.<biome>`, and at aquatic sites `envo.<water body>` = the site name;
- curated terms such as `pato.distance` = `observer-to-bird distance` (unit `PATO:0000040`).

**Curation rule.** Curated terms are applied only when both Opus replicates propose them and the validator
accepts them: the term is live in an OBO Foundry ontology, its label matches, and the column exists.
Everything else waits in `review.md`.

**DataCite.** Site and product collections carry canonical `datacite.*` records, built with mesa-mcp and
validated with a round-trip:

- the identifier is the collection's CyVerse Discovery Environment URL;
- the creator is NEON with ROR `https://ror.org/04j43p132`;
- the licence is CC-BY-4.0;
- release DOIs are `relatedIdentifier` `IsDerivedFrom`.

**Releases.** `neon.release` = `RELEASE-YYYY`, with the product's release DOI (`doi:10.48443/…`) as the
unit. PROVISIONAL has an empty unit. Data found only in an earlier release adds `neon.releaseOnly=true`.

**Checksum flags.** A file whose bytes match the storage MD5 but not NEON's listed MD5 carries
`neon.md5Verified=false` and `neon.neonMd5Mismatch=<NEON md5>`.

**Habitat and biome.**

- `neon.habitat` comes from the catalog's `HABITAT` site property.
- Aquatic sites never use NEON's `TypeEco`, which NEON copied from a grassland site to all of them. They
  get a freshwater biome and an ENVO stream, river or lake feature instead.
- Terrestrial sites use `TypeEco`, NLCD classes covering at least 1 % of the area, and the domain.
- Either way, a two-replicate Opus pass decides the biome.

## Verification

```bash
nd verify catalog                  # row counts vs the sealed payloads, coverage, 0 signed URLs, one snapshot
nd verify SRER                     # listing, local MD5s, iRODS replicas/checksums, AVU completeness,
                                   # stacking, MESA and ontology checks; writes sites/SRER/reports/verify.json
nd verify SRER --products DP1.00002.001 --months 2024-06
nd verify mesa                     # MESA history == journal == iRODS, 0 pending pushes, actors and sources
nd site evict SRER --dry-run       # what eviction would free, and why files are blocked
nd recover                         # after a crash during record: drain pending pushes, reconcile batches
.venv/bin/neon-ducklake diff       # the last two catalog snapshots: new months, PROVISIONAL -> released
.venv/bin/neon-ducklake status --json
.venv/bin/pytest -q && .venv/bin/ruff check src tests    # 500+ hermetic tests and lint
```

Every `verify` exits non-zero when a hard check fails. Eviction additionally requires a passing `verify`
report that is newer than the file's upload.

## Performance and expected durations

These rates were measured on 2026-09-27/28, in a 9-site probe and the first orchestrated SRER run.
Details are in [`docs/SCALING.md`](docs/SCALING.md).

| Stage | Measured |
|---|---|
| list | 17–24 s per site (57–61 products) |
| download | 32–59 small files/s (3 clients, adaptive up to 24 streams); large files are bandwidth-bound |
| stack | 4–85 s per sampled site, 0 unit errors on 10 sites |
| terms | 3–90 s per site; plant-rich sites are slowest, and results are cached across sites |
| upload | ~0.8 s p95 per file. CyVerse resets connections at about 10 concurrent puts; AIMD settles at about 9 |
| AVU apply | 60 calls/s (the configured ceiling), p95 70–140 ms; 38k AVUs over 3.2k objects in 85 s |
| record | ~1,100 rows/s (38k rows in 35 s, one snapshot) |
| verify | 68 s for a sample |

Expected durations:

- **One aquatic site:** about 2–3 h.
- **One terrestrial site:** about 4–7 h.
- **All 81 sites:** about 4–5 days unattended. Uploading ~3.2M objects at ~10 files/s takes about 90 h
  and is the long pole. AVU apply (~15 h) and record (~16 h) overlap with it across sites.
- **Curation:** 1.5–2.5 h on the first sites. It is generic per product, so later sites curate only
  products that no earlier site had.
- **Disk:** a stream run holds at most the working set (`NEON_DUCKLAKE_WORKSET_BYTES`, 50 GB) of raw files
  and site-lake data. With `--no-stream`, peak local use is about the largest site (~101 GB) times the
  sites in flight.

## Safety properties

- **Idempotent.** Downloads skip verified files, stack skips tables whose fingerprint is unchanged,
  uploads skip objects that already have a good replica with the same checksum, and `tag` writes only the
  difference between desired and current AVUs. Re-running a finished stage finds nothing to do.
- **Resumable.** The orchestrator stores every stage in `work/state.sqlite` (`site_runs`). A re-run
  resumes each site at its first stage that is not `ok`. A sample-mode stage never counts as done for a
  full run.
- **Safe to kill.** Downloads and harvest landings are written to `.part` files and renamed into place.
  Every stage survives `kill -9`; the dry run proved this for upload and tag. The first Ctrl-C stops
  scheduling and lets running stages finish.
- **Journaled AVU ownership.** Every AVU the pipeline writes is journaled: intent, then applied, then
  recorded. It removes only AVUs it owns and never touches `ipc*`, `mesa.enabled` or anyone else's AVUs.
- **Bounded MESA history.**
  - Stream runs spool history batches to the Data Store; one recorder records them in coalesced
    snapshots of up to 250,000 rows (at least 200,000 rows, or batches older than 12 h).
  - A budget guard refuses to record past 900 snapshots; MESA caches at most 1,000.
  - The recorder skips batches already committed and rows already recorded under another batch id, and
    `recover` quarantines failed Parquet and re-records dead batches, so nothing is recorded twice.
- **Deletion only after verification (stream).** A chunk's raw files are deleted only after each one has a
  good Data Store replica with the downloaded checksum, its core AVUs are in iRODS, its history rows are
  spooled or recorded, and the site lake holding the chunk is on the Data Store.
- **One VM per site.** Site claims are atomic Data Store objects with a heartbeat; a VM whose claim is lost
  stops the site at once and never overwrites the new owner's journal or lake catalog.
- **Eviction only after verification (`--no-stream`).** A raw file is deleted only when all of these hold:
  - it downloaded ok;
  - it is in iRODS with a good replica whose checksum matches;
  - its AVUs are recorded in MESA;
  - a passing `verify` report newer than its upload covers it.

  The journal row is committed before the file is unlinked.
- **Disk and connection guards.**
  - A download starts only if free disk is at least 1.3 × its bytes plus what other downloads reserved,
    and, in stream mode, only while the working set plus the chunk fits the cap.
  - A cross-process budget caps iRODS connections.
  - Uploads back off when CyVerse resets connections.
- **Secrets stay out.**
  - Tokens and passwords reach processes only through `with-secrets.sh` and are never logged.
  - Signed URLs live only in the local journal and are nulled after download.
  - Curation runs in a scrubbed environment, with no secrets and no MESA catalog.

## Development

```bash
uv sync --locked --extra dev
.venv/bin/pytest -q                 # hermetic: no NEON, iRODS or MESA access
.venv/bin/ruff check src tests
```

Before changing a module interface, read [`docs/CONTRACTS.md`](docs/CONTRACTS.md). Commits follow
[Conventional Commits](https://www.conventionalcommits.org/).

## Documentation

- [`docs/index.md`](docs/index.md): a map of the documentation.
- [`docs/PLAN.md`](docs/PLAN.md): the approved design.
- [`docs/SCALING.md`](docs/SCALING.md): scaling to 81 sites and the measured throughput.
- [`docs/CONTRACTS.md`](docs/CONTRACTS.md): internal interfaces.
- [`docs/STREAMING.md`](docs/STREAMING.md): the streaming pipeline and the multi-VM contract.
- [`docs/RUNBOOK.md`](docs/RUNBOOK.md): operations.
- [`docs/DE_SANDBOX.md`](docs/DE_SANDBOX.md): workers on CyVerse Discovery Environment VMs.
- [`CHANGELOG.md`](CHANGELOG.md): release history.

## Citing NEON data and related projects

NEON data are licensed [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

- Cite every product you use at its release DOI; the DOIs are in `neon.release` units and in
  `product_releases`.
- Cite PROVISIONAL data by product and download date.
- Follow NEON's [data policies and citation guidelines](https://www.neonscience.org/data-samples/data-policies-citation).
- neon-mcp's `neon_get_citation` tool produces NEON's own wording and BibTeX.

Related projects:

- [neon-mcp](https://github.com/idss-mesa/neon-mcp): MCP server and client for the NEON Data API.
- [mesa-mcp](https://github.com/idss-mesa/mesa-mcp): CyVerse Data Store, OBO/OLS and DataCite tools.
- [mesa-ducklake](https://github.com/idss-mesa/mesa-ducklake): AVU history for MESA projects.
- [mesa-anyjev](https://github.com/idss-mesa/mesa-anyjev): calibrated ontology decisions.
- [DuckLake](https://ducklake.select/): the lakehouse format.
- [NEON](https://www.neonscience.org/) and [CyVerse](https://cyverse.org/).

## License

MIT, Copyright (c) 2026 The Regents of the University of New Mexico. See [LICENSE](LICENSE). NEON data
remain under NEON's CC BY 4.0 terms.
