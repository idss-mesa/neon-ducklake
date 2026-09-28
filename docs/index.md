# neon-ducklake documentation

| Document | Read it when you want to |
|---|---|
| [`../README.md`](../README.md) | Understand what neon-ducklake builds, install it, and run the quick start or the full sweep. |
| [`PLAN.md`](PLAN.md) | See the approved design: phases, architecture, catalog tables, AVU vocabulary, bulk writer and history, and verification. |
| [`SCALING.md`](SCALING.md) | Understand how the site pipeline scales to all 81 sites: limits and decisions, the orchestrator, resource classes, and measured throughput with the all-sites projection. |
| [`CONTRACTS.md`](CONTRACTS.md) | Change code. It lists the interfaces every module codes against: shared modules, lake tables and columns, `avu_desired`, uploads and the ontology stores, plus their amendment rounds. |
| [`RUNBOOK.md`](RUNBOOK.md) | Operate the pipeline: prerequisites, the orchestrator, one site or all sites, monitoring, recovery, review steps, refreshing after a NEON release, and eviction. |
| [`../CHANGELOG.md`](../CHANGELOG.md) | See what changed between releases. |

## Scripts

| Script | Purpose |
|---|---|
| `scripts/with-secrets.sh CMD…` | Runs `CMD` with the NEON token and CyVerse credentials exported from the login keyring. It never prints them. |
| `scripts/full_sweep.sh` | Runs the full sweep safely: preflight, plan and estimate, confirmation, `neon-ducklake run`, then status. Options include `--dry-run`, `--detach` and `--refresh-catalog`. |
| `scripts/sweep_status.sh` | Shows sweep progress: the stage table, running stages, the tail of the latest sweep log, free disk and the MESA snapshot budget. |

## Order of precedence

`PLAN.md` is the approved design as written. `SCALING.md`, `CONTRACTS.md` and `RUNBOOK.md` record the
decisions made since then. Where they disagree with the plan, they win.
