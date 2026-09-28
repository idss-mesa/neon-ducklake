# neon-ducklake runbook: building site subtrees

This guide covers running the Phase-2 pipeline for one NEON site, a group of sites, or all 81 sites. It
also covers monitoring, recovery, review, and refreshing after a new NEON release.

The design is in `docs/SCALING.md` and the contracts are in `docs/CONTRACTS.md`. AOP products are
excluded throughout: every stage works on the non-AOP basic package, including PROVISIONAL and
release-only months.

All commands run from `/home/tswetnam/neon-ducklake`.

## 1. Prerequisites

1. **Unlock the keyring.** Every command that talks to NEON, iRODS or MESA runs through
   `scripts/with-secrets.sh`. The wrapper reads the NEON token and the CyVerse password from the login
   keyring, exports them, and then `exec`s the command. It never prints a secret. It exits with 3 when
   the NEON token is unavailable and with 4 when the CyVerse password is unavailable. The keyring and
   D-Bus work inside tmux.
2. **Run preflight.** It is read-only.

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake preflight --site SRER
   ```

   Fix every failure before you start. Pay attention to these checks:
   - the NEON token works;
   - iRODS authentication works;
   - no process holds `~/.mesa/ducklake/catalog.duckdb`;
   - there are no pending MESA pushes;
   - there is enough disk space.
3. **Check the catalog lake.** `ducklake/neon_catalog.ducklake` must exist. The orchestrator reads it
   read-only to choose sites, and `site plan` uses it too. If it is missing, run `catalog fetch`, then
   `catalog seal`, then `catalog build`.
4. **Close other MESA writers.** The `record` stage needs the MESA catalog's single-writer lock. A
   mesa-mcp server running in another session (for example a Claude Code session) holds that lock, so
   close it before a run reaches `record`. Preflight and the `record` stage both name the process that
   holds the lock.

## 2. The orchestrator

`neon-ducklake run` runs these stages for each site, in order:

| Stage | Command it runs | Resource class (sites at once) |
|---|---|---|
| plan | `site plan <SITE>` | local (no limit) |
| list | `site list <SITE>` | net-in (2) |
| download | `site download <SITE>` | net-in (2) |
| stack | `site stack <SITE>` | cpu (1) |
| terms | `site terms <SITE>` | cpu (1) |
| cards | `site cards <SITE>` | cpu (1) |
| curate | `site curate <SITE>` | llm (1) |
| validate | `site validate <SITE>` | cpu (1) |
| upload | `site upload <SITE>` | net-out (1) |
| apply | `site build-avus <SITE>`, then `tag <SITE> --apply-only` | irods-meta (1) |
| record | `tag <SITE> --record-only` | mesa (1) |
| verify | `verify <SITE>` | local (no limit) |
| evict | `site evict <SITE>` | local (no limit) |

How the orchestrator runs stages:

- **Subprocesses.** Each stage is a subprocess of the same Python interpreter and inherits the
  environment, so start the orchestrator itself through `with-secrets.sh`.
- **Logs.** Each stage writes to `logs/run/<SITE>/<stage>.log`. Every attempt is appended with a
  header line that shows the command and a footer line that shows the exit code.
- **Overlap.** The resource classes let sites overlap safely. For example, site B can download while
  site A uploads, and site C can wait for the one cpu slot.
- **Site order.** Sites you name run in the order you give. With `--all` or `--habitat`, aquatic
  sites run first (smallest first), then terrestrial sites by size. `--order size|name|given|aquatic-first`
  overrides this.
- **Sites in flight.** `--max-sites-in-flight` (default 3) caps how many sites are between their first
  and last stage at once.
- **State.** Stage status is stored in `work/state.sqlite`, table `site_runs`. The status values are
  pending, running, ok, failed and skipped.
- **Resuming.** A re-run resumes each site at its first stage that is not `ok`. If a stage fails, that
  site stops and the other sites carry on; the next invocation retries the failed stage.
- **Only one orchestrator.** Only one orchestrator can run at a time; it holds `work/locks/run.lock`.

Useful options:

| Option | Effect |
|---|---|
| `--dry-run` | Prints the site order, the resume point for each site and the exact commands. Runs nothing. |
| `--stages plan,list,download` | Runs only these stages. |
| `--from-stage apply` | Re-runs from this stage, even when the later stages are already `ok`. |
| `--sample N` | Probe mode: only the N most recent months per product, always including the latest released month and the latest PROVISIONAL month. Implies no evict. A stage done in sample mode never counts as done for a full run. |
| `--no-evict` | Keeps the raw files on disk. The evict stage is marked `skipped`, and a later run without this flag performs only the evict. |
| `--restack` | Re-downloads the evicted inputs and forces a full re-stack (`site stack --force`). It starts at download. |
| `--threads N` | Passed to list, download and upload. |

## 3. Running one site

1. Preview the site:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake site plan HARV
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --dry-run
   ```

2. Optionally, probe the site with a sample first. This runs only local stages and writes nothing to
   iRODS:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --sample 2 \
       --stages plan,list,download,stack,terms,cards
   ```

3. Run the whole site:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV
   ```

You can also run a single stage by hand, for example `site list HARV --sample 2`. The orchestrator
does not know about stages you run by hand, so its next run re-checks them. That is cheap because every
stage is idempotent: downloads skip files already present, stack skips unchanged tables by fingerprint,
upload skips objects already present, and apply and record find nothing to do.

## 4. Running all sites

Use tmux, so the run survives a lost SSH session and the keyring stays reachable:

```bash
tmux new -s neon
cd ~/neon-ducklake
scripts/with-secrets.sh .venv/bin/neon-ducklake run --all 2>&1 | tee -a logs/run/orchestrator.log
```

To run one habitat, use `--habitat aquatic` or `--habitat Terrestrial`. A single aquatic class also
works, for example `--habitat 'Aquatic Lake'`.

`nohup` also works:

```bash
nohup scripts/with-secrets.sh .venv/bin/neon-ducklake run --all >> logs/run/orchestrator.log 2>&1 &
```

The wrapper `exec`s the command, so `$!` is the orchestrator's own pid.

### Expected durations

These estimates come from docs/SCALING.md and the SRER dry run:

| Item | Estimate |
|---|---|
| One terrestrial site (30–100 GB, 23k–65k files) | 5.5–8 h end to end |
| of which download | 0.5–1 h |
| of which stack | 0.5–1 h |
| of which upload | 1.8–3.5 h |
| of which apply | 45–60 min |
| of which record (under the MESA lock) | 12–15 min |
| One aquatic site (3–13 GB) | a fraction of a terrestrial site; per-file work dominates (16k–36k files) |
| Curation | 1.5–2.5 h on the first sites. It is generic per product (`curation/generic/`), so later sites curate only products that no earlier site had. |
| All 81 sites | about 3.2M files and 4.0 TB. Upload and AVU apply dominate, so plan for several days. The overlap from resource classes hides most list, download, stack and curation time behind uploads. |

### Disk streaming

Local disk (about 3.3 TB free) is smaller than all sites together (4.0 TB). The evict stage deletes a
site's raw files once each one is verified in iRODS. Lakes, cards, curation, reports and `ref/` stay
local.

- **Peak use.** Peak disk use is roughly the largest site (about 101 GB) times the sites in flight.
- **Disk guard.** Before a download starts, the orchestrator requires free disk of at least 1.3 × the
  bytes that download will write, plus what downloads already in flight have reserved. Otherwise the
  download waits for other sites to evict. If nothing is running that could free space, it fails with
  a `disk guard` message.

## 5. Monitoring

```bash
.venv/bin/neon-ducklake status                 # site x stage table + totals; writes logs/status.json
.venv/bin/neon-ducklake status SRER HARV --json
tail -f logs/run/HARV/upload.log
```

- **Table symbols.** `+` ok, `X` failed, `>` running, `.` pending, `-` skipped, blank never run.
- **Totals.** The totals show files, GB, evicted GB, AVU rows recorded (from `avu_journal`) and MESA
  snapshots (from `batches`), compared against the 900-snapshot budget.
- **Failures.** Each failed stage is listed with its exit code, its message and its log path.
- **Stale rows.** If the orchestrator is not running, stages still marked `running` show as failed.
- **Secrets.** `status` needs no secrets.

## 6. Recovery

- **Any interruption.** Re-run the same command. Each site resumes at its first stage that is not ok.
  A `running` row left by a killed orchestrator is marked failed and retried. The orchestrator refuses
  to start while a child of the dead orchestrator is still alive, and names its pid.
- **Stopping cleanly.** Press Ctrl-C once, or send `kill -INT` / `kill -TERM` to the orchestrator.
  Scheduling stops and running stages finish. A second signal terminates the running stages, with
  SIGTERM and then SIGKILL after 15 s. Stages are idempotent and survive `kill -9`, and the dry run
  proved this for upload and tag.
- **MESA history.** Run `neon-ducklake recover` (through `with-secrets.sh`). It drains pending MESA
  pushes, quarantines failed Parquet files and reconciles `batches`. Run it after any crash during
  `record`, and whenever preflight reports pending pushes.
- **MESA lock holders.** When `record` fails with "MESA catalog is open by another process: pid N",
  close that process (usually another mesa-mcp session), then re-run. The site resumes at `record`.
- **Failed downloads.** A failed download is a failed download stage. Re-run it: signed URLs older than
  6 days are refreshed automatically.
- **Disk guard failures.** Free space, or let other sites evict, then re-run.
- **Stack refused because of evicted inputs.** A product that has both evicted files and new files would
  be re-stacked from a subset. Re-run from `download`, which restores those products' files, or use
  `--restack`.

## 7. Review steps (per site, after `validate`)

1. **Curated product terms.** Terms that both Opus replicates proposed are applied automatically. Terms
   from only one replicate go to `sites/<SITE>/curation/review.md`. To accept one, tick its box
   (`- [x]`), then run:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake site validate <SITE>
   scripts/with-secrets.sh .venv/bin/neon-ducklake run <SITE> --from-stage apply
   ```

   Reviews never block a run. Unreviewed terms are simply not applied.
2. **Site biome.** `site terms` proposes an ENVO biome in `ref_site_envo` with method `heuristic` and
   status `proposed`. It is shown in `sites/<SITE>/reports/terms.json` and at the end of `terms.log`.
   Aquatic sites use `HABITAT` (stream, river or lake), never the copied `TypeEco`. Only a curated row
   becomes an `envo.*` AVU. To accept the proposal, first make sure no terms or validate stage is running,
   because the store has a single writer:

   ```bash
   .venv/bin/python -c "import duckdb; c = duckdb.connect('work/ontology_cache.duckdb'); \
   c.execute(\"UPDATE ref_site_envo SET method='curated', status='ok' WHERE siteCode='HARV'\"); c.close()"
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --from-stage apply
   ```

   To choose a different biome, also set `envoCurie` and `envoLabel`, using a live ENVO term under
   ENVO:00000428.
3. **Unresolved names.** Check `sites/<SITE>/reports/unresolved_taxa.csv` and `unmapped_units.csv`.
   They are informational and never block a run.

## 8. Refreshing after a new NEON release

1. Harvest a new catalog run, build it, and see what changed:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake catalog fetch --new
   scripts/with-secrets.sh .venv/bin/neon-ducklake catalog seal
   scripts/with-secrets.sh .venv/bin/neon-ducklake catalog build
   .venv/bin/neon-ducklake diff          # last two catalog snapshots: new months, PROVISIONAL -> released, files
   ```

2. Re-run the affected sites from `list`:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake run SRER HARV --from-stage list
   ```

   - **Listing.** Changed files are marked stale and downloaded again, and superseded files are kept
     and reported.
   - **Download.** The download stage first restores the evicted files of every product that changed.
   - **Stack.** Only the changed tables are rebuilt. Fully evicted, unchanged products are skipped and
     stay evicted.
   - **Tagging.** `apply` and `record` write only the AVU difference.
3. To rebuild every table of a site from scratch, for example after a stack code change, use
   `--restack`. It re-downloads the whole site first.

## 9. Eviction details and current limits

`site evict <SITE>` (and the evict stage) deletes a raw file only when all of these hold:

- **Download.** It downloaded ok and is not superseded.
- **Upload.** It was uploaded (`uploaded` or `present`) and its iRODS checksum equals the local MD5.
- **AVUs.** Its `neon.productCode` AVU is recorded in MESA history.
- **Verify report.** The last `verify <SITE>` ran against iRODS, has no failed check, covers the file
  and is newer than the file's upload.
- **Local file.** The local file is a regular file under `sites/<SITE>/raw/` with the journal size.

`variables` files always stay, because they are tiny and later stages re-read them. Use
`site evict <SITE> --dry-run` to see what would be freed and why files are blocked. The command exits
with 1 when the verify report does not allow eviction, and with 3 when some files are blocked. Each run
writes `logs/evict_summary_<SITE>.json`.

Until the listed changes land in the modules owned by other stages, these limits apply:

- **Download.** `site download` restores every evicted file of the site, because it treats a missing
  `ok` file as not downloaded. The orchestrator never runs download for a finished site unless you ask
  (`--from-stage` up to download, or `--restack`). The disk guard counts evicted bytes, so this is safe,
  only slower.
- **Verify.** `verify <SITE>` reports evicted files as missing in its `local files` check, which fails.
  A refresh run of an evicted site therefore stops at verify, and no further eviction happens until
  verify accepts evicted files.
- **Upload.** `site upload` logs one "missing local file" warning per evicted file. This is harmless.


## Addendum (2026-09-27, heterogeneity round)

**Downloads**
- Integrity is checked against the storage provider's MD5.
- Stale NEON MD5s no longer fail a download. Such files are kept, flagged (`neon.md5Verified=false`) and reported, not re-listed.
- Evicted files are never re-downloaded unless NEON republishes them.

**Verify and upload with evicted files**
- Evicted raw files count as "evicted", not missing.
- Upload skips them quietly.

**Site biome and feature**
- Run `site curate <SITE> --site-level`, then `site validate <SITE> --site-level`.
- Two Opus replicates must agree. Anything else goes to `sites/<SITE>/curation/review_site.md`; accept it by ticking the box or with `--accept`.
