---
type: Guide
title: "Adding a site or product"
description: "How neon-ducklake selects sites and products (from the catalog lake, not from a hand-kept list), how to build and publish one more site, what happens when NEON adds a product, and which code to change for products that need special handling or for a new catalog table."
tags:
  - neon-ducklake
  - engineering
  - extending
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: run-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/run.py"
    title: "run.py: site_universe, select_sites, order_sites"
    author: "team:idss-mesa"
  - id: plan-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/plan.py"
    title: "site/plan.py: v_site_inventory planner, EXTERNAL_PRODUCTS"
    author: "team:idss-mesa"
  - id: stack-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/site/stack.py"
    title: "site/stack.py: generic stacking, lab tables, HDF5"
    author: "team:idss-mesa"
  - id: flatten-py
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/src/neon_ducklake/flatten.py"
    title: "flatten.py: catalog table specs"
    author: "team:idss-mesa"
  - id: attach-all
    resource: "https://github.com/idss-mesa/neon-ducklake/blob/main/sql/attach_all.sql"
    title: "sql/attach_all.sql"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-05-24T00:00:00Z"
---

# Adding a site or product

Neither sites nor products are kept in a hand-written list. Both come from the
[catalog lake](../data/catalog-lake.md).

## Sites

`run.site_universe()` reads every site from the catalog's `sites` table, with its habitat
(`siteProperties['HABITAT']`) and its non-AOP basic bytes from `v_site_inventory`. All 81 NEON
sites are therefore already known. `select_sites` accepts explicit codes, `--all` or `--habitat`,
and rejects a code that is not in the catalog. `--all` orders aquatic sites first, smallest first.

To build and publish one more site, for example HARV:

```bash
nd() { scripts/with-secrets.sh .venv/bin/neon-ducklake "$@"; }
nd site plan HARV                 # read-only: products, months, files, bytes, exclusions
nd run HARV --dry-run             # the exact stages and commands
nd run HARV                       # stream the site to the Data Store; re-run to resume
nd recorder --flush               # on the MESA-catalog host: record the spooled history
nd verify HARV                    # hard checks; writes sites/HARV/reports/verify.json
```

Or on a worker VM, `nd worker` claims the next unfinished site by itself. A finished site gets a
`sites/<SITE>/.done` marker on the Data Store.

Then, if you want local readers to see it, add a block for the site to `sql/attach_all.sql`
(the Python `catalog_lake.attach_all(con)` globs `sites/*/ducklake/neon_*.ducklake` and needs no
change). Note that `sql/attach_all.sql` already lists HARV, which has no lake yet.

Nothing in this documentation updates itself: after publishing a site, update the
[site lakes card](../data/site-lakes.md) (sites, counts, freshness) and `docs/log.md`.

## Products

Phase 2 takes every non-AOP product that `v_site_inventory` lists for the site, in the basic
package, including PROVISIONAL and release-only months. A product NEON adds appears after the next
catalog refresh (`catalog fetch --new`, `seal`, `build`) and is then picked up by every site run
from `list` on. No code change is needed for an ordinary product:

- **Stacking is generic.** `site/stack.py` follows neonUtilities `stackByTable`: files grouped by
  header, typed from the product's `variables` file, one table per product and NEON table.
- **File kinds** come from the shared classifier `filenames.classify` (data, variables, readme,
  sensor_positions, eml, science_review_flags, categorical_codes, validation, lab).
- **Curation is per product.** The first site that has a product runs two Opus replicates and
  validates the proposals in OLS; the result is shared as `curation/generic/<DP>.validated.json`
  (55 files in the repository on 2026-11-24), so later sites only curate products no earlier site
  had.

Products that need code:

- **Externally hosted products** have no NEON files. `site/plan.py` lists three
  (`EXTERNAL_PRODUCTS`: DP1.00033.001, DP1.00042.001, DP1.00043.001); others are detected from
  zero-file listings with an `externalData` link.
- **Non-CSV formats.** DP4.00200.001 (eddy covariance) is HDF5 and has its own reader
  (`site/h5.py`) and tables (`h5_attrs`, `h5_datasets`, `h5_objdesc`, `dp4_00200_001__nsae_dp04`).
  A new non-CSV product needs a reader like it.
- **AOP products** are excluded throughout (`products.isAop`, or a code matching
  `DP[1-4].30xxx.xxx`).

## A new catalog table

Catalog tables are defined in `flatten.TABLES` (`TableSpec`: name, group, key, typed columns,
description, sources). A new table needs a spec, the flattening code that fills it from the sealed
harvest, and an update of [CONTRACTS](../CONTRACTS.md), since downstream modules code against the
catalog's tables and columns. The next `catalog build` creates it (or evolves an existing table to
its spec) inside the build's single transaction. A new view goes into `sql/catalog_views.sql` as a
`-- view: <name>: <description>` block.

## Before you change an interface

Read [CONTRACTS](../CONTRACTS.md) first and update it in the same change. Commits follow
Conventional Commits.
