---
okf_version: "0.2"
title: neon-ducklake
description: "neon-ducklake harvests the NEON Data API into a DuckDB ducklake-extension catalog lake, builds per-site lakes of stacked NEON data with full AVU metadata on the CyVerse Data Store, and records every AVU change in a MESA history. The lakes are public and readable anonymously over HTTPS."
---

# neon-ducklake

neon-ducklake is a pipeline and a published product. The pipeline harvests the
[NEON Data API](https://data.neonscience.org/data-api/) and NEON's field-site data into
[DuckLake](https://ducklake.select/) lakes on the CyVerse Data Store. The product is the public
collection it writes:

```
https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/
```

Anyone can read it with DuckDB over HTTPS, without an account:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/neon_catalog.ducklake'
    AS neon (READ_ONLY,
             DATA_PATH 'https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/ducklake/data/',
             OVERRIDE_DATA_PATH true);
SELECT count(*) AS products FROM neon.products;
```

```text
products
202
```

## The datasets

| Dataset | What it holds (measured 2026-11-24) | Card |
|---|---|---|
| Catalog lake | NEON API metadata: 202 products, 81 sites, releases RELEASE-2021 to RELEASE-2026, availability, locations, taxonomy, and an inventory of 3,215,813 non-AOP files (4,034.1 GB). Built 2026-09-27 | [Catalog lake](data/catalog-lake.md) |
| Site lakes | Stacked, typed NEON data for one site per lake. SRER and KONZ are published, both as partial sample runs | [Site lakes](data/site-lakes.md) |
| AVU history | 104,964 AVU add/delete events with provenance, in plain Parquet under `.mesa/ducklake/` | [AVU history](data/avu-history.md) |

[The published collection](data/collection.md) explains every path in the collection, including
the history spool (`.mesa/spool/`, `.mesa/spool-done/`).

## Engineering

- [Architecture](engineering/architecture.md): the two phases and the stages of a site run.
- [How lakes are built and published](engineering/building-lakes.md): local build, one snapshot
  per build, checksum-verified uploads with the catalog file last, and the history spool.
- [Running the pipeline](engineering/running.md): install, quick start, full sweep, refresh.
- [Adding a site or product](engineering/extending.md).
- [Testing and verification](engineering/testing.md).

## Design records

These are the project's original design documents, kept at their paths.

| Document | Read it when you want to |
|---|---|
| [Design plan](PLAN.md) | See the approved design: phases, architecture, catalog tables, AVU vocabulary, bulk writer and history, and verification. |
| [Scaling to 81 sites](SCALING.md) | Understand how the site pipeline scales to all 81 sites: limits and decisions, the orchestrator, resource classes, and measured throughput. |
| [Internal contracts](CONTRACTS.md) | Change code: the interfaces every module codes against, lake tables and columns, `avu_desired`, uploads and the ontology stores. |
| [Streaming and multi-VM](STREAMING.md) | Understand the streaming pipeline and the multi-VM contract: chunks, per-chunk stages and return codes, the versioned site lake on the Data Store, spool-only history and the recorder, site claims, journal backups, and the orchestrator. |
| [Runbook](RUNBOOK.md) | Operate the pipeline: prerequisites, the orchestrator, monitoring, recovery, review, refreshing after a NEON release, eviction, streaming, workers and the recorder. |
| [Workers on DE VMs](DE_SANDBOX.md) | Run workers on several CyVerse Discovery Environment VMs: roles, setup, secrets without a keyring, the worker, the recorder, stale claims and troubleshooting. |

The [README](https://github.com/idss-mesa/neon-ducklake/blob/main/README.md) and the
[CHANGELOG](https://github.com/idss-mesa/neon-ducklake/blob/main/CHANGELOG.md) are in the
repository root.

### Order of precedence

`PLAN.md` is the approved design as written. `SCALING.md`, `CONTRACTS.md`, `STREAMING.md` and
`RUNBOOK.md` record the decisions made since then. Where they disagree with the plan, they win;
where `STREAMING.md` disagrees with `SCALING.md` or `CONTRACTS.md` about streaming or several VMs,
it wins.

### Scripts

| Script | Purpose |
|---|---|
| `scripts/with-secrets.sh CMD...` | Runs `CMD` with the NEON token and CyVerse credentials: from the login keyring, or from the environment and `~/.irods` on a VM (`NEON_DUCKLAKE_SECRETS=env\|keyring\|auto`). It never prints them. |
| `scripts/full_sweep.sh` | Runs the full sweep safely: preflight, plan and estimate, confirmation, the MESA history recorder (tmux session `neon-recorder`), `neon-ducklake run`, then status. |
| `scripts/sweep_status.sh` | Shows sweep progress: stages, chunks, the working set, the spool backlog and the recorder, the latest sweep log, free disk and the MESA snapshot budget. |
| `scripts/okf_validate.py`, `scripts/gen_llms_txt.py`, `scripts/postbuild_agent_surface.py` | Validate this documentation (OKF v0.2), generate `llms.txt` and `llms-full.txt`, and add the agent surface to the built site. |

## For AI agents

Start at [`llms.txt`](https://idss-mesa.github.io/neon-ducklake/llms.txt) or fetch the whole corpus
in one file, [`llms-full.txt`](https://idss-mesa.github.io/neon-ducklake/llms-full.txt). Every page
is also available as Markdown at its URL plus `index.md`, and as raw source at
`https://raw.githubusercontent.com/idss-mesa/neon-ducklake/main/docs/<path>.md`. See
[For AI agents](about/ai-agents.md).

neon-ducklake is research software from the University of New Mexico, MIT licensed. It is not
affiliated with NEON, Battelle or NSF. NEON data are licensed CC BY 4.0.
