# Directory Update Log

## 2026-11-24

* **Creation**: Published the documentation as an Open Knowledge Format v0.2 site at
  https://idss-mesa.github.io/neon-ducklake/ with `llms.txt` and `llms-full.txt`.
* **Creation**: Dataset cards for the [catalog lake](data/catalog-lake.md) (with a
  [column reference](data/catalog-lake-columns.md)), the [site lakes](data/site-lakes.md) and the
  [AVU history](data/avu-history.md), plus [the published collection](data/collection.md). Every
  count and every SQL example was run read-only against the public collection on 2026-11-24, and
  all 29 SQL blocks were re-run with matching output on 2026-11-26.
* **Creation**: Engineering pages: [architecture](engineering/architecture.md),
  [how lakes are built and published](engineering/building-lakes.md),
  [running the pipeline](engineering/running.md),
  [adding a site or product](engineering/extending.md) and
  [testing and verification](engineering/testing.md).
* **Update**: Added OKF frontmatter to the existing design records (PLAN, CONTRACTS, STREAMING,
  SCALING, RUNBOOK, DE_SANDBOX) without changing their text, and turned `docs/index.md` into the
  site's home page, keeping its document map and order of precedence.
* **Creation**: `docs/agent-questions.yml`, the question bank this documentation must answer, and
  `AGENTS.md` for coding agents.
