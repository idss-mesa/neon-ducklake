---
type: Guide
title: "Testing and verification"
description: "neon-ducklake's hermetic pytest suite (no NEON, iRODS or MESA access; in-memory fakes of python-irodsclient and the other services), ruff lint, and the verify commands that check the catalog lake, a site, and the MESA history and gate eviction."
tags:
  - neon-ducklake
  - engineering
  - testing
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: tests
    resource: "https://github.com/idss-mesa/neon-ducklake/tree/main/tests"
    title: "tests/"
    author: "team:idss-mesa"
  - id: verify-catalog
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/verify_catalog.py"
    title: "verify_catalog.py"
    author: "team:idss-mesa"
  - id: verify-site
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/verify_site.py"
    title: "verify_site.py"
    author: "team:idss-mesa"
  - id: verify-mesa
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/verify_mesa.py"
    title: "verify_mesa.py"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-05-24T00:00:00Z"
---

# Testing and verification

## Unit tests and lint

```bash
uv sync --locked --extra dev
.venv/bin/pytest -q                 # hermetic: no NEON, iRODS or MESA access
.venv/bin/ruff check src tests      # line length 110
```

The suite has 79 test modules. A run on 2026-11-24 (macOS, Python 3.11, DuckDB 2.0.0, with the
`ducklake` and `spatial` extensions installed) gave 869 passed, 1 skipped and 1 failed. The failure,
`test_curation_curate.py::test_main_runs_replicates_scrubbed_and_skips`, comes from the macOS-only
environment variable `__CF_USER_TEXT_ENCODING` in the scrubbed curation environment; the project
targets Linux. Without the `spatial` extension installed, the catalog-lake tests error out, because
the code only `LOAD`s extensions. `ruff check src tests` with ruff 0.17.6 reported two `RUF100`
(unused `noqa`) findings in `src/neon_ducklake/site/stack.py`; the dev extra pins only `ruff>=0.6`.

- **Hermetic.** `tests/conftest.py` fixes the CyVerse user (`testuser`) and clears the path
  variables before any module imports `neon_ducklake.env`, so results do not depend on who runs
  them.
- **Fakes, not services.** The `tests/test_*_fakes.py` modules are in-memory stand-ins imported by
  other tests. `test_avu_fakes.py` fakes the python-irodsclient surface used by `avu/bulk.py` and
  `mirror.py`, evaluating GenQuery against the real `irods.models` columns. The catalog, coordinator
  and ontology-store tests have their own fakes (`test_catalog_fakes.py`, `test_coord_fakes.py`,
  `test_terms_fakes.py`).
- **Real DuckDB.** Lake tests build real DuckLakes in temporary directories.
- **Where to add a test.** Next to the module's existing tests (`tests/test_<module>_*.py`). A
  change to an interface in [CONTRACTS](../CONTRACTS.md) needs its tests updated in the same
  change.

## Verify commands

Each `verify` exits non-zero when a hard check fails.

```bash
nd() { scripts/with-secrets.sh .venv/bin/neon-ducklake "$@"; }
nd verify catalog       # row counts vs the sealed payloads, coverage, 0 signed URLs, one snapshot
nd verify SRER          # listing, local MD5s, iRODS replicas and checksums, AVU completeness,
                        # stacking, MESA and ontology checks; writes sites/SRER/reports/verify.json
nd verify SRER --products DP1.00002.001 --months 2024-06
nd verify mesa          # MESA history == journal == iRODS, 0 pending pushes, actors and sources
nd site evict SRER --dry-run   # what eviction would free, and why files are blocked
nd recover              # after a crash during record: drain pending pushes, reconcile batches
.venv/bin/neon-ducklake diff   # the last two catalog snapshots: new months, PROVISIONAL -> released
```

The published SRER report (`sites/SRER/reports/verify.json`, 2026-09-28) ran these checks: listing,
local files, irods objects, avu desired, avu core, avu file kinds, avu collections, stack rows,
stack typed, stack sensors, taxa (info), ontology and datacite; all but `taxa` passed.

## What verification gates

- **Eviction (`--no-stream`).** A raw file is deleted only when it downloaded ok, has a good iRODS
  replica with a matching checksum, its AVUs are recorded in MESA, and a passing `verify` report
  newer than its upload covers it. The journal row is committed before the file is unlinked.
- **Deletion in stream mode.** A chunk's raw files are deleted only after each has a good Data
  Store replica with the downloaded checksum, its core AVUs are in iRODS, its history rows are
  spooled or recorded, and the site lake holding the chunk is on the Data Store.
- **Catalog build.** Key uniqueness, non-NULL keys and zero signed URLs are asserted inside the
  build transaction; a failure rolls the build back.

## Checking the published data

The SQL examples on the [dataset cards](../data/index.md) double as a smoke test of the public
collection: each was run against it on 2026-11-24, and each output excerpt is copied from that
run. Rerun them after publishing a new build.
