# neon-ducklake documentation

| Document | Read it when you want to |
|---|---|
| [`../README.md`](../README.md) | Understand what neon-ducklake builds, install it, and run the quick start or the full sweep. |
| [`PLAN.md`](PLAN.md) | See the approved design: phases, architecture, catalog tables, AVU vocabulary, bulk writer and history, and verification. |
| [`SCALING.md`](SCALING.md) | Understand how the site pipeline scales to all 81 sites: limits and decisions, the orchestrator, resource classes, and measured throughput with the all-sites projection. |
| [`CONTRACTS.md`](CONTRACTS.md) | Change code. It lists the interfaces every module codes against: shared modules, lake tables and columns, `avu_desired`, uploads and the ontology stores, plus their amendment rounds. |
| [`STREAMING.md`](STREAMING.md) | Understand the streaming pipeline and the multi-VM contract: chunks, per-chunk stages and their return codes (4 = retry later), the site lake on the Data Store (versioned catalogs behind a `CURRENT` pointer), spool-only history and the coalescing recorder, site claims and `coord.holds_site`, versioned journal backups, and the orchestrator's rules (retries with backoff, missing `.done` markers). |
| [`RUNBOOK.md`](RUNBOOK.md) | Operate the pipeline: prerequisites, the orchestrator, one site or all sites, monitoring, recovery, review steps, refreshing after a NEON release, eviction, streaming (section 10), and workers, claims and the recorder (section 11). |
| [`DE_SANDBOX.md`](DE_SANDBOX.md) | Run workers on several CyVerse Discovery Environment VMs: roles, VM setup, secrets without a keyring, the worker, the recorder, stale claims and troubleshooting. |
| [`../CHANGELOG.md`](../CHANGELOG.md) | See what changed between releases. |

## Scripts

| Script | Purpose |
|---|---|
| `scripts/with-secrets.sh CMD…` | Runs `CMD` with the NEON token and CyVerse credentials: from the login keyring, or from the environment and `~/.irods` on a VM (`NEON_DUCKLAKE_SECRETS=env\|keyring\|auto`). It never prints them. |
| `scripts/full_sweep.sh` | Runs the full sweep safely: preflight, plan and estimate, confirmation, the MESA history recorder (tmux session `neon-recorder`), `neon-ducklake run`, then status. Options include `--dry-run`, `--detach`, `--refresh-catalog`, `--no-recorder`, and `--worker` for a DE VM. |
| `scripts/sweep_status.sh` | Shows sweep progress: the stage table, running (chunk) stages, chunk progress, the working set against its cap, the spool backlog and the recorder, the tail of the latest sweep log, free disk and the MESA snapshot budget. |

## Order of precedence

`PLAN.md` is the approved design as written. `SCALING.md`, `CONTRACTS.md`, `STREAMING.md` and
`RUNBOOK.md` record the decisions made since then. Where they disagree with the plan, they win; where
`STREAMING.md` disagrees with `SCALING.md` or `CONTRACTS.md` about streaming or several VMs, it wins.
