# Engineering

How neon-ducklake builds, publishes, refreshes and tests its lakes. The design records in the
navigation (PLAN, CONTRACTS, STREAMING, SCALING, RUNBOOK, DE_SANDBOX) hold the full detail; these
pages are the short path through them.

* [Architecture](architecture.md) - The two phases, the stages of a site run, and the local and Data Store layouts.
* [How lakes are built and published](building-lakes.md) - Local build, one snapshot per build, CHECKPOINT and DETACH, checksum-verified uploads with the catalog file last, versioned site catalogs, and how AVU history reaches `.mesa/ducklake/`.
* [Running the pipeline](running.md) - Requirements, install, secrets, the quick start, the full sweep, workers, and refreshing after a new NEON release.
* [Adding a site or product](extending.md) - How sites and products are selected, and what to change to add one.
* [Testing and verification](testing.md) - The hermetic test suite, lint, and the `verify` commands that gate eviction.
