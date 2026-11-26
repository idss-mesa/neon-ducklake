# Data

neon-ducklake publishes three datasets in one public collection on the CyVerse Data Store. Each has
a dataset card with its tables, columns, coverage, freshness, tested queries and known limitations.

* [The published collection](collection.md) - What is in the public collection, which code writes each path, the difference between the two kinds of "ducklake", and reading tips.
* [Catalog lake](catalog-lake.md) - NEON Data API metadata for 202 products, 81 sites and 6 releases, plus an inventory of 3,215,813 non-AOP files. Start here to find what NEON data exists.
* [Catalog lake: column reference](catalog-lake-columns.md) - Every catalog table and view with its columns and types.
* [Site lakes](site-lakes.md) - Stacked, typed NEON data for one site per lake. SRER and KONZ are published, both as partial sample runs.
* [AVU history](avu-history.md) - Every AVU the pipeline wrote, as add/delete events in plain Parquet, and how history travels from the spool through the recorder to `.mesa/ducklake/`.

All three were queried read-only over HTTPS on 2026-11-24.
