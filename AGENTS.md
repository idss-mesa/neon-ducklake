# AGENTS.md

Instructions for coding agents (Codex, opencode, goose, Claude Code and others). The documentation
site is https://idss-mesa.github.io/neon-ducklake/ (source in `docs/`); read it, and
`docs/CONTRACTS.md`, before anything non-trivial.

## What this repo is

`neon-ducklake` is a Python 3.11 pipeline (`src/neon_ducklake/`, CLI `neon-ducklake`) that:

- harvests the NEON Data API into a **catalog lake** (`ducklake/neon_catalog.ducklake`, DuckDB
  `ducklake` extension format);
- builds per-site **site lakes** (`sites/<SITE>/ducklake/neon_<site>.ducklake`) of stacked NEON data,
  writes AVUs on every collection and file on the CyVerse Data Store, and records them in MESA's AVU
  history (`.mesa/ducklake/`, written by mesa-ducklake).

The published collection is public: https://data.cyverse.org/dav-anon/iplant/home/tswetnam/neon-ducklake/

## Commands

```bash
uv sync --locked --extra dev
.venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('ducklake', 'spatial')]"
.venv/bin/pytest -q                       # hermetic: no NEON, iRODS or MESA access
.venv/bin/ruff check src tests            # lint, line length 110

# Anything that talks to NEON, iRODS or MESA goes through the secrets wrapper:
scripts/with-secrets.sh .venv/bin/neon-ducklake preflight
scripts/with-secrets.sh .venv/bin/neon-ducklake run SRER --dry-run
.venv/bin/neon-ducklake status            # needs no secrets

# Documentation (Python 3.12 via uv; the scripts need 3.11+ and PyYAML):
uv run -q --no-project --python 3.12 --with pyyaml python scripts/okf_validate.py docs
uv run -q --no-project --python 3.12 --with pyyaml python scripts/gen_llms_txt.py   # commit docs/llms*.txt
uv run -q --no-project --python 3.12 --with zensical --with pyyaml zensical build --clean --strict
uv run -q --no-project --python 3.12 --with pyyaml python scripts/postbuild_agent_surface.py site
```

## Layout

- `src/neon_ducklake/`: `catalog_lake.py`, `flatten.py`, `harvest.py`, `seal.py` (Phase 1);
  `site/` (plan, fetch, stack, h5, terms, cards, curate, validate, avus, upload, stream);
  `avu/` (vocab, bulk writer, history, recorder); `mirror.py`; `run.py`, `worker.py`, `coord.py`.
- `sql/catalog_views.sql` (the catalog views), `sql/attach_all.sql` (attach every local lake).
- `scripts/`: `with-secrets.sh`, `full_sweep.sh`, `sweep_status.sh`, and the OKF doc scripts.
- `docs/`: the OKF v0.2 documentation bundle. Dataset cards in `docs/data/`, engineering pages in
  `docs/engineering/`, design records (`PLAN.md`, `CONTRACTS.md`, `STREAMING.md`, `SCALING.md`,
  `RUNBOOK.md`, `DE_SANDBOX.md`) at the top of `docs/`. `docs/agent-questions.yml` is the question
  bank the docs must answer.

## Hard contracts

- **Interfaces are in `docs/CONTRACTS.md`.** Update it in the same change as the code. Where the
  design records disagree, SCALING, CONTRACTS, STREAMING and RUNBOOK win over PLAN; STREAMING wins
  on streaming and multi-VM questions.
- **Lakes are built locally, never in place.** A build ends with `CHECKPOINT` and `DETACH`; a
  leftover `.wal` is an error. Uploads are checksum-verified, and `*.ducklake` catalog files go last.
- **One catalog build = one DuckLake snapshot** (hash-keyed upsert in one transaction, with a
  commit message).
- **Streamed site-lake catalogs are versioned:** immutable `catalogs/` copies, `CURRENT` replaced
  last. Never overwrite a published version.
- **AVU ownership is journaled.** The pipeline removes only AVUs it owns and never touches `ipc*`,
  `mesa.enabled` or other writers' AVUs. History goes through `avu/history.py` (direct or spool) and
  the recorder; never write `.mesa/ducklake/` by hand.
- **Secrets** reach processes only through `scripts/with-secrets.sh`; never log or commit them.
- **Every stage stays idempotent, resumable and safe to kill.**

## Documentation rules

- Every `docs/**/*.md` page has OKF v0.2 frontmatter (`type`, `title`, `description`,
  `generated`, `sources`, `status`; dataset cards also `stale_after`), except section `index.md`
  files and `log.md`, which must have none. The root `docs/index.md` may carry only `okf_version`,
  `title` and `description`.
- Every page must be in the `nav` of `zensical.toml`.
- Numbers and SQL on dataset cards come from real queries; rerun the queries when the lakes change
  and record the date. Regenerate and commit `docs/llms.txt` and `docs/llms-full.txt`; CI fails on
  drift.
- Add a dated entry to `docs/log.md` for each documentation change.
- Commits follow Conventional Commits.
