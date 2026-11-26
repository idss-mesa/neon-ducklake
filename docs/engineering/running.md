---
type: Guide
title: "Running the pipeline"
description: "Requirements, install, secrets and configuration for neon-ducklake; the quick start for the catalog lake and one site; the full sweep; workers on several VMs; and how to rebuild or refresh the lakes after a new NEON release."
tags:
  - neon-ducklake
  - engineering
  - operations
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: readme
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/README.md"
    title: "README: requirements, install, configuration, quick start, full sweep"
    author: "team:idss-mesa"
  - id: cli-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/cli.py"
    title: "cli.py: commands and options"
    author: "team:idss-mesa"
  - id: full-sweep
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/scripts/full_sweep.sh"
    title: "scripts/full_sweep.sh"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-05-24T00:00:00Z"
---

# Running the pipeline

You only need this page to *build* lakes. To read the published ones, see [Data](../data/index.md).

## Requirements

- Linux, Python 3.11 (`requires-python = ">=3.11,<3.12"`) and [uv](https://docs.astral.sh/uv/).
- DuckDB 2.0.0 (pinned in `pyproject.toml`) with the `ducklake` and `spatial` extensions
  installed. The code only `LOAD`s extensions and never installs them.
- A NEON API token (free with a NEON account; NEON requires it for file listings and downloads).
- A CyVerse account with Data Store access.
- A MESA local install in `$MESA_HOME` (default `~/.mesa`) with the keyring helpers, `cyverse-user`,
  a `.venv` running mesa-mcp, and the MESA catalog `ducklake/catalog.duckdb`.
- An unlocked login keyring holding the NEON token and the CyVerse password (or, on a VM,
  environment variables; see below).
- Optional: the Claude Code CLI (`claude`) for the Opus curation stage. Without it, leave `curate`
  out of `--stages`.

## Install

```bash
git clone https://github.com/idss-mesa/neon-ducklake && cd neon-ducklake
uv sync --locked --extra dev
.venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('ducklake', 'spatial')]"
```

neon-mcp, mesa-mcp and mesa-ducklake are not on PyPI; `pyproject.toml` pins each to a GitHub
commit in `[tool.uv.sources]`. `neon-ducklake preflight` reports each dependency's version and
commit and warns when it differs from the pin.

Store the secrets once with the MESA helpers:

```bash
~/.mesa/bin/neon-keyring-set        # NEON API token -> keyring (service neon, account api-token)
~/.mesa/bin/mesa-keyring-set        # CyVerse password -> keyring
```

## Configuration

Every command that talks to NEON, iRODS or MESA runs through `scripts/with-secrets.sh`. It reads
the secrets from the keyring (or from the environment and `~/.irods` on a VM:
`NEON_DUCKLAKE_SECRETS=env|keyring|auto`), exports them, and `exec`s the command. It never prints
a secret. Exit codes: 2 MESA install missing, 3 NEON token unavailable, 4 CyVerse password
unavailable.

The main environment variables (the README lists all of them):

| Variable | Default | Meaning |
|---|---|---|
| `NEON_DUCKLAKE_ROOT` | the checkout | Data root: `api/`, `ducklake/`, `sites/`, `work/`, `logs/` |
| `NEON_DUCKLAKE_IRODS_ROOT` | `/iplant/home/<user>/neon-ducklake` | The iRODS project collection |
| `NEON_DUCKLAKE_VM_ID` | short host name | Names this machine's claims, history spool and ontology export |
| `NEON_DUCKLAKE_HISTORY_MODE` | `spool` (stream), `direct` (`--no-stream`) | MESA history backend |
| `NEON_DUCKLAKE_WORKSET_BYTES` | 50 GB | Cap on the local working set of stream runs |
| `NEON_DUCKLAKE_CHUNK_BYTES` | 8 GB | Largest chunk |
| `NEON_DUCKLAKE_ROWS_PER_BATCH` | 250,000 | MESA history rows per batch and per recorder snapshot |
| `NEON_DUCKLAKE_IRODS_CONNECTIONS` | 8 | iRODS connections shared by all neon-ducklake processes on one machine |

## Quick start

```bash
nd() { scripts/with-secrets.sh .venv/bin/neon-ducklake "$@"; }

nd preflight                      # read-only readiness checks; fix every "fail" first

# Phase 1: the catalog lake (about 1.5-2.5 h)
nd catalog fetch                  # resumes the latest incomplete run; --new starts a new one
nd catalog seal                   # bundle the landings into sealed/ (the only build input)
nd catalog build                  # one DuckLake snapshot; --dry-run to check first
nd verify catalog

# The MESA project, the Data Store mirror and the Phase-1 AVUs
nd enroll                         # once: create and enroll the iRODS project
nd mirror                         # code, sealed API pulls and the catalog lake (--what code,api,lake)
nd tag catalog                    # apply the Phase-1 AVUs and record them in MESA history

# Phase 2: one site
nd site plan SRER                 # read-only preview: products, months, files, bytes
nd run SRER --dry-run             # site order, resume point and the exact commands
nd run SRER                       # stream the site; re-run to resume
nd recorder --flush               # record the spooled MESA history now
.venv/bin/neon-ducklake status    # site x stage table (needs no secrets)
```

`neon-ducklake --help` and `neon-ducklake <command> --help` list every option. The commands are
`preflight`, `catalog`, `enroll`, `mirror`, `tag`, `site`, `run`, `status`, `worker`, `recorder`,
`claims`, `journal`, `bootstrap`, `verify`, `recover` and `diff`.

## Full sweep

`scripts/full_sweep.sh` runs every site, or a selection, with the checks an unattended multi-day
run needs: setup checks, `preflight`, a plan and estimate, confirmation (unless `--yes`), an
optional catalog refresh, the history recorder in tmux session `neon-recorder`, then
`neon-ducklake run` and `status`.

```bash
scripts/full_sweep.sh --dry-run                  # plan + estimate for all 81 sites; starts nothing
scripts/full_sweep.sh --detach                   # run in a tmux session after the checks
scripts/full_sweep.sh --sites 'SRER HARV' --no-stream --sample 2 --no-evict   # a local probe
scripts/full_sweep.sh --refresh-catalog --from-stage list --detach   # after a new NEON release
scripts/sweep_status.sh                          # progress, working set, spool, recorder, disk
```

The README's estimate for all 81 sites: 3,215,813 files, 4,034.1 GB, about 3.7 to 5.0 days; the
upload of about 3.2 million objects at about 10 files/s is the long pole. Stop with one Ctrl-C
(running stages finish); resume by running the same command again.

## Workers on several VMs

CyVerse Discovery Environment VMs can stream different sites at once. They share only the Data
Store: atomic site claims (`sites/<SITE>/.claim`, `.done`), journal backups
(`sites/<SITE>/.mesa-journal/`), the site lakes, the history spool and shared curation results.

```bash
export NEON_DUCKLAKE_SECRETS=env NEON_DUCKLAKE_VM_ID=de-vm-01     # on each VM; NEON_TOKEN and iinit first
nd worker                         # claim -> stream -> .done -> release, site after site
nd recorder --watch --interval 300 --min-rows 200000 --max-rows 250000 --max-age-h 12   # MESA-catalog host only
```

See [Workers on DE VMs](../DE_SANDBOX.md).

## Refresh after a new NEON release

The catalog lake is a snapshot of the NEON API on the day it was harvested. To refresh it:

```bash
nd catalog fetch --new            # harvest a new run
nd catalog seal
nd catalog build                  # adds one snapshot to the existing lake (hash-keyed upsert)
nd verify catalog
.venv/bin/neon-ducklake diff      # last two catalog snapshots: new months, PROVISIONAL -> released
nd mirror --what api,lake         # upload; the .ducklake file goes last
```

Then re-run the affected sites from `list`:

```bash
nd run SRER KONZ --from-stage list
```

In stream mode, `chunk-plan` keeps every started chunk's months, packs new months into new chunks,
and resets a deleted chunk whose months gained files to `planned`; only those chunks run again, followed by finalize and cleanup. Changed
files are downloaded again, only changed tables are re-stacked, and `apply` writes only the AVU
difference. `--restack` rebuilds every table of a site from scratch.
`scripts/full_sweep.sh --refresh-catalog --from-stage list` does both steps. Details:
[RUNBOOK section 8](../RUNBOOK.md#8-refreshing-after-a-new-neon-release).

A rebuilt lake is published only by `mirror` (catalog) or the site upload (site lakes). Until then
the public copy stays at its previous snapshot.
