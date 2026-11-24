# neon-ducklake runbook: building site subtrees

This guide covers running the Phase-2 pipeline for one NEON site, a group of sites, or all 81 sites. It
also covers monitoring, recovery, review, and refreshing after a new NEON release.

The design is in `docs/SCALING.md` and `docs/STREAMING.md`, and the contracts are in `docs/CONTRACTS.md`.
AOP products are excluded throughout: every stage works on the non-AOP basic package, including PROVISIONAL
and release-only months.

The pipeline builds a metadata database; it does not keep the data. By default the orchestrator
**streams** each site in bounded chunks: fetch, analyze, generate AVUs, push the data, AVUs and history to
the CyVerse Data Store, verify the Data Store copy, then delete the local copy (section 10). Local disk only
ever holds a bounded working set, so the same pipeline runs on sparky-1 (about 4 TB) and on several
CyVerse Discovery Environment VMs at once, one site per VM (section 11).

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
4. **Close other MESA writers.** Stream runs (the default) never open the MESA catalog: they spool their
   history to the Data Store, and the recorder (section 11) is the only process that writes MESA. The
   recorder, and the whole-site `record` stage, need the MESA catalog's single-writer lock. A mesa-mcp
   server running in another session (for example a Claude Code session) holds that lock, so close it
   while they run. They wait for the lock instead of failing (`NEON_DUCKLAKE_WAIT_TIMEOUT_S`, default 6 h)
   and name the process that holds it.

## 2. The orchestrator

`neon-ducklake run` streams each site by default (section 10). Per site it runs these stages, in order:

| Stage | Command it runs | Resource class |
|---|---|---|
| restore | `site restore <SITE>`: the journal backup when another VM pushed a newer one, then the site lake from the Data Store when the local copy is missing or older. Runs first whenever the site has work. | net-in (2) |
| plan | `site plan <SITE>` | local (no limit) |
| list | `site list <SITE>` | net-in (2) |
| chunk-plan | `site chunk-plan <SITE>` | local |
| chunks | the chunk loop: for every chunk, `site chunk <stage> <SITE> --chunk <ID>` for download (net-in), stack and avus (cpu), upload (net-out), apply (irods-meta), history (spool; local), verify and delete (local) | per chunk stage |
| finalize | `site finalize <SITE>`, only after every chunk is deleted | finalize (1) |
| cleanup | `site cleanup <SITE>`, last | local |

Before a site's first stage, `run` claims it on the Data Store (section 11); a site that is done or
claimed by a worker VM is skipped.

With `--no-stream` it runs the previous whole-site stages instead:

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
- **Logs.** Each stage writes to `logs/run/<SITE>/<stage>.log`, and each chunk stage to
  `logs/run/<SITE>/chunks/<ID>/<stage>.log`. Every attempt is appended with a header line that shows the
  command and a footer line that shows the exit code.
- **Overlap.** The resource classes let sites overlap safely. For example, site B can download while
  site A uploads, and site C can wait for the one cpu slot. In stream mode the classes count chunk stages,
  so chunk k+1 of a site can download while chunk k uploads (section 10).
- **Site order.** Sites you name run in the order you give. With `--all` or `--habitat`, aquatic
  sites run first (smallest first), then terrestrial sites by size. `--order size|name|given|aquatic-first`
  overrides this.
- **Sites in flight.** `--max-sites-in-flight` (default 3) caps how many sites are between their first
  and last stage at once.
- **State.** Stage status is stored in `work/state.sqlite`: table `site_runs` for site-level stages and
  `site_run_chunks` for chunk stages. The status values are pending, running, ok, failed and skipped.
  Chunk progress itself is the `stream_chunks` journal of `site/chunks.py`.
- **Resuming.** A re-run resumes each site at its first stage that is not `ok`, and each chunk at the
  stage after its journal status. If a stage fails, that site stops and the other sites carry on; the next
  invocation retries the failed stage.
- **Waiting, not failing.** A stage whose precondition is not met yet waits and is retried: the MESA
  catalog held by another process (whole-site `record`), another VM curating one of the site's products
  (stream `finalize`), a chunk download blocked by the working set (rc 3), and any stream stage that
  reports a transient condition (rc 4, `RC_RETRY`: OLS unavailable with taxa pending, the site lake's Data
  Store catalog being written). Retries back off: 5, 10, 20, 40, 60 and 60 minutes. Only after
  `NEON_DUCKLAKE_WAIT_TIMEOUT_S` (6 h) of waiting, or 6 retries, does the stage fail.
- **Only one orchestrator.** Only one orchestrator can run at a time; it holds `work/locks/run.lock`.

Useful options:

| Option | Effect |
|---|---|
| `--dry-run` | Prints the site order, the resume point for each site (and its chunk progress) and the exact commands. Runs nothing. |
| `--stream` / `--no-stream` | Stream mode (the default) or the previous whole-site stages. |
| `--chunk-bytes N` | Largest chunk (default 8 GB; accepts `8G`, `500M`, ...). Passed to the stages as `NEON_DUCKLAKE_CHUNK_BYTES`. |
| `--workset-bytes N` | Cap on the local working set: raw files (superseded ones included) plus site lakes (default 50 GB). Passed as `NEON_DUCKLAKE_WORKSET_BYTES`. |
| `--history-mode direct\|spool` | MESA history backend of the stages (`NEON_DUCKLAKE_HISTORY_MODE`). Stream mode always spools, and refuses `direct` until DuckDB 2.0 multi-writer (`NEON_DUCKLAKE_MESA_MULTIWRITER=1`). With `--no-stream` the default is direct. See section 11. |
| `--chunks-in-flight N` | Chunks of one site between download and delete at once (default 2). |
| `--no-claims` | Stream mode: do not claim sites on the Data Store and write no `.done`. Only for single-host runs that no worker VM can touch. |
| `--stages plan,list,chunk-plan` | Runs only these stages (stream stage names; whole-site names with `--no-stream`). |
| `--from-stage finalize` | Re-runs from this stage, even when the later stages are already `ok`. |
| `--sample N` | Probe mode: only the N most recent months per product, always including the latest released month and the latest PROVISIONAL month. A stage done in sample mode never counts as done for a full run. With `--no-stream` it implies no evict; in stream mode verified chunks are still deleted. |
| `--no-evict` | `--no-stream` only. Keeps the raw files on disk. The evict stage is marked `skipped`, and a later run without this flag performs only the evict. |
| `--restack` | `--no-stream` only. Re-downloads the evicted inputs and forces a full re-stack (`site stack --force`). It starts at download. |
| `--threads N` | Passed to list, download and upload (whole-site), or to list, the chunk download, upload and apply stages and finalize (stream). |

## 3. Running one site

1. Preview the site:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake site plan HARV
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --dry-run
   ```

2. Optionally, look at the chunk plan first. These stages only list and plan; they download nothing
   and write nothing to iRODS:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --stages plan,list,chunk-plan
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --dry-run       # chunk counts per site
   ```

   The previous local-only probe (downloads a sample and keeps it on disk) is still available:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV --no-stream --sample 2 \
       --stages plan,list,download,stack,terms,cards
   ```

3. Run the whole site:

   ```bash
   scripts/with-secrets.sh .venv/bin/neon-ducklake run HARV
   ```

You can also run a single stage by hand, for example `site list HARV --sample 2`, or one chunk stage,
for example `site chunk upload HARV --chunk DP1.00001.001__2024-01_2024-06` (chunk ids are in
`neon-ducklake status --json` and in the dry run). The orchestrator does not know about stages you run by
hand, so its next run re-checks them. That is cheap because every stage is idempotent: downloads skip files
already present, stack skips unchanged tables by fingerprint, upload skips objects already present, and
apply and record find nothing to do. A chunk stage you run by hand updates the chunk journal, so the
orchestrator resumes that chunk after it.

## 4. Running all sites

Use tmux, so the run survives a lost SSH session and the keyring stays reachable:

```bash
tmux new -s neon
cd ~/neon-ducklake
scripts/with-secrets.sh .venv/bin/neon-ducklake run --all 2>&1 | tee -a logs/run/orchestrator.log
```

A stream run spools its MESA history, so run the recorder next to it on sparky-1 (section 11);
`scripts/full_sweep.sh` starts it for you in the tmux session `neon-recorder`.

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

Local disk (about 3.3 TB free) is smaller than all sites together (4.0 TB). In stream mode (the default)
raw data never exceeds the working-set cap: see section 10. The rest of this subsection describes
`--no-stream`, where the evict stage deletes a site's raw files once each one is verified in iRODS and
lakes, cards, curation, reports and `ref/` stay local.

- **Peak use.** Peak disk use is roughly the largest site (about 101 GB) times the sites in flight.
- **Disk guard.** Before a download starts, the orchestrator requires free disk of at least 1.3 × the
  bytes that download will write, plus what downloads already in flight have reserved. Otherwise the
  download waits for other sites to evict. If nothing is running that could free space, it fails with
  a `disk guard` message.

## 5. Monitoring

```bash
.venv/bin/neon-ducklake status                 # site x stage table + totals; writes logs/status.json
.venv/bin/neon-ducklake status SRER HARV --json
scripts/sweep_status.sh                        # one screen: status, running (chunk) stages, chunks, spool, disk
tail -f logs/run/HARV/upload.log               # whole-site stage log
tail -f logs/run/HARV/chunks/<ID>/upload.log   # chunk stage log
```

- **Table symbols.** `+` ok, `X` failed, `>` running, `.` pending, `-` skipped, blank never run. Only the
  stage columns present in the journal are shown (stream: rst = restore, plan, list, cpl = chunk-plan,
  chk = the chunk loop, fin = finalize, cln = cleanup). A site skipped because it is done or claimed by
  another VM shows `-` in its first column.
- **Chunks.** Stream sites show `chunks` as deleted/total and `heldGB`, the raw bytes still on local disk.
  Running chunk stages are listed with their log; failed chunk stages with their exit code and message.
- **Working set.** The working set on local disk (raw files, superseded ones included, plus the site
  lakes) against the cap.
- **Spool backlog.** The history batches this host spooled to the Data Store and has not seen recorded
  (section 11), plus any interrupted spool uploads. A recorder on the same host marks them recorded; on a
  worker VM they stay `spooled`, so the real backlog is `recorder --dry-run`.
- **Totals.** The totals show files, GB, evicted GB, AVU rows recorded (from `avu_journal`) and MESA
  snapshots (from `batches`), compared against the 900-snapshot budget.
- **Failures.** Each failed stage is listed with its exit code, its message and its log path.
- **Stale rows.** If the orchestrator is not running, stages still marked `running` show as failed, and
  chunk stages still marked `running` are listed as stale.
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
- **MESA lock holders.** While another process holds the MESA catalog, `record` (and the recorder) wait
  and say so ("waits: MESA catalog is open by another process: pid N"). Close that process (usually
  another mesa-mcp session). After `NEON_DUCKLAKE_WAIT_TIMEOUT_S` (6 h) the stage fails; re-run and the
  site resumes at `record`.
- **Failed downloads.** A failed download is a failed download stage. Re-run it: signed URLs older than
  6 days are refreshed automatically.
- **Disk guard failures.** Free space, or let other sites evict, then re-run.
- **Stack refused because of evicted inputs.** A product that has both evicted files and new files would
  be re-stacked from a subset. Re-run from `download`, which restores those products' files, or use
  `--restack`.
- **A failed chunk (stream).** Re-run the same command. The chunk resumes at the stage after the last
  status it reached (`stream_chunks.done`); the chunks before it are untouched and the site's later chunks
  continue in order. The chunk's log is `logs/run/<SITE>/chunks/<ID>/<stage>.log`.
- **Finalize refused (stream).** `finalize` runs only after every chunk is deleted. If it reports chunks
  that are not deleted yet, run the site again without `--stages`/`--from-stage finalize` so the chunk
  loop runs first.
- **Working set full (stream).** A chunk download waits while the working set plus this chunk exceed
  `--workset-bytes`. If nothing is running that could free space (for example chunks of a failed site
  still hold raw bytes, or raw files of earlier whole-site runs), the download fails with a `working set`
  message: fix and re-run the failed site so its chunks verify and delete, evict old raw files
  (`site evict <SITE>`), or raise `--workset-bytes`.
- **Site skipped: done or claimed (stream).** `run` skips a site with a `.done` marker or a live claim of
  another VM. `neon-ducklake claims` lists them; see section 11 for stale claims.
- **Claim lost (stream).** When the heartbeat cannot renew a site's claim (an iRODS outage of about 2 h),
  the site stops at once, its running stages are terminated and it reports `claim lost`. Another VM may
  already have taken it over; check `claims` before re-running it. A stage that finds the claim gone right
  before it writes the site lake's catalog or journal pointer writes nothing and exits 1 ("claim lost").
- **Retried stages (stream).** A chunk or site stage that exits 4 (`RC_RETRY`) is dispatched again with
  backoff (5 minutes up to 1 hour between tries, 6 retries). The run log says `retry n/6 in Ns: transient
  (exit 4)`; the stage log says why (OLS unavailable, the lake catalog being written). Nothing needs to be
  done unless it gives up; then re-run the site.
- **A finished site without `.done` (stream).** A site whose cleanup is ok here but that has no `.done`
  marker (its write failed, or it finished under `--no-claims`) is claimed briefly by the next `run` and
  marked done ("marked done" in the run summary). When another VM pushed a newer journal of the site since,
  `run` instead restores it and runs `chunk-plan`, the chunk loop, `finalize` and `cleanup` under the
  claim first. A site that another VM holds is left alone.
- **Torn Data Store writes.** The site lake's catalog and the site's journal backup are versioned: a VM
  that died mid-write leaves the previous version readable (the `CURRENT` pointers are replaced last), and
  a restore falls back to the previous journal set when the newest one does not verify. Nothing needs to be
  repaired by hand.
- **Restore failed (stream).** `restore` fails when the Data Store holds a newer journal backup that could
  not be restored, or when the site lake does not cover the chunks already stacked. Inspect with
  `neon-ducklake journal restore <SITE>` (`--force` replaces the local journal rows).

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

   In stream mode `chunk-plan` then keeps every started chunk's months, packs new months into new chunks,
   and resets a deleted chunk whose months gained files to `planned`; only those chunks run again, followed
   by finalize and cleanup. The bullets below describe `--no-stream`.

   - **Listing.** Changed files are marked stale and downloaded again, and superseded files are kept
     and reported.
   - **Download.** The download stage first restores the evicted files of every product that changed.
   - **Stack.** Only the changed tables are rebuilt. Fully evicted, unchanged products are skipped and
     stay evicted.
   - **Tagging.** `apply` and `record` write only the AVU difference.
3. To rebuild every table of a site from scratch, for example after a stack code change, use
   `--restack`. It re-downloads the whole site first.

## 9. Eviction details and current limits (whole-site mode, `--no-stream`)

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


## 10. Streaming (the default)

Stream mode applies the MESA principle: keep the metadata, not the data. `docs/STREAMING.md` is the
contract; this section is the operator's view.

**Per site.**

1. `restore` restores the site's journal backup when another VM pushed a newer one, and fetches the site
   lake from the Data Store when the local copy is missing or older. It runs first whenever the site has
   work.
2. `plan` and `list` run as before.
3. `chunk-plan` splits the listed files into chunks: one product, ascending months, at most
   `--chunk-bytes` (a single month larger than that is a chunk by itself). The chunk id is stable:
   `<product>__<firstMonth>_<lastMonth>`.
4. The chunk loop takes every chunk through eight stages:

| Chunk stage | What it does | Class |
|---|---|---|
| download | downloads only this chunk's files | net-in (2) |
| stack | appends the chunk to the site lake (incremental; never needs earlier chunks' raw files) | cpu (1) |
| avus | maps the chunk's taxa, builds the month-collection and data-object AVUs of the chunk | cpu (1) |
| upload | pushes the chunk's raw files, then the site lake (new `data/**`, the `.ducklake` catalog last) | net-out (1) |
| apply | applies the chunk's AVUs in iRODS | irods-meta (1) |
| history | spools the MESA history rows to the Data Store | local |
| verify | every chunk object has a good replica with the downloaded checksum and its core AVUs, the history rows are spooled, and the Data Store lake holds the chunk | local |
| delete | deletes the chunk's local raw files (only after verify passed) and superseded files of its months | local |

5. `finalize` builds and pushes everything site-level: terms, cards, curation, site/product/lake AVUs,
   the site lake with its `.ducklake` catalog last, and reports. It then verifies at metadata level. Each
   step is journaled, so a re-run skips finished steps. It waits (retry later) while another VM curates
   one of the site's products.
6. `cleanup` deletes the local lake data, cards and curation traces after checking their Data Store
   copies. It keeps small reports, the lake catalog file and the journal. When it has to keep some files
   (rc 3) the site still counts as done, with a warning in the stage row.

**Data Store coordination.**

- `run` claims each site (`<IRODS_ROOT>/sites/<SITE>/.claim`) before its first stage and heartbeats the
  claim (not for a run of only plan, list and chunk-plan, which write nothing to the Data Store). Sites
  that are done (`.done`) or claimed by a worker VM are skipped. A finished site gets `.done`; a site
  finished earlier whose `.done` is missing gets it at the next run (section 6). `--no-claims` turns this
  off.
- Every stage is told whether a claim is in force (`NEON_DUCKLAKE_SITE_CLAIMS=on|off`). With a claim, a
  stage checks it (`coord.holds_site`) right before it writes the site lake's catalog or the journal
  pointer, and writes nothing once it is gone.
- The site lake's catalog is never overwritten in place: each push writes a new versioned catalog under
  `sites/<SITE>/ducklake/catalogs/`, then moves the `CURRENT` pointer to it, then refreshes the canonical
  `neon_<site>.ducklake`.
- The site's journal is pushed to `<IRODS_ROOT>/sites/<SITE>/.mesa-journal/` after every chunk's apply,
  history and delete stages and after finalize and cleanup, as a new versioned set (`v/<pushId>/`) whose
  `CURRENT` pointer is replaced last. A failed push fails that stage, so the backup is never more than one
  chunk stage behind.
- After finalize, this host's ontology mappings are published to `ref/ontology_maps/vm/<vm>/`.

**Scheduling.**

- A chunk runs one stage at a time. Chunk k+1 runs a stage only after chunk k has finished it, so the
  chunks form a pipeline: chunk k+1 downloads while chunk k stacks, uploads or applies.
- At most `--chunks-in-flight` (default 2) chunks of a site are between download and delete.
- The resource classes of section 2 hold across all sites and chunks: for example, only one upload runs at
  a time.

**Working set.** A chunk download starts only when the working set on disk plus this chunk fit
`--workset-bytes` (default 50 GB), and free disk covers 1.3 × the chunk. The working set counts every raw
file on disk (superseded ones included) and the site lakes' `data/**`. When no other chunk holds raw bytes,
any chunk may start, so a chunk larger than the cap runs alone. The cap is exported to the stages as
`NEON_DUCKLAKE_WORKSET_BYTES`, so the stages' own guard agrees with the orchestrator; a download the stage
could not fit in time (rc 3) is retried later.

**History.** Stream runs always spool (`NEON_DUCKLAKE_HISTORY_MODE=spool` for every stage). The recorder
turns the spool into MESA snapshots (section 11). `--history-mode direct` is refused in stream mode: it
would take the MESA lock and spend a MESA snapshot per chunk, and the project has a 900-snapshot budget for
81 sites. It becomes possible with DuckDB 2.0 multi-writer catalogs (`NEON_DUCKLAKE_MESA_MULTIWRITER=1`).

**Resume.** Chunk progress is `stream_chunks.status`: planned → downloaded → stacked → avus → uploaded →
applied → recorded → verified → deleted. A re-run resumes each chunk at the stage after its status; a
`failed` chunk resumes after the last status it reached. `finalize` and `cleanup` run again whenever the
chunk loop had work. `status` shows deleted/total chunks, the raw bytes still held and the working set.

**By hand.**

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake site restore HARV
scripts/with-secrets.sh .venv/bin/neon-ducklake site chunk-plan HARV [--chunk-bytes 4G]
scripts/with-secrets.sh .venv/bin/neon-ducklake site chunk download HARV --chunk DP1.00001.001__2024-01_2024-06
scripts/with-secrets.sh .venv/bin/neon-ducklake site finalize HARV
scripts/with-secrets.sh .venv/bin/neon-ducklake site cleanup HARV
scripts/with-secrets.sh .venv/bin/neon-ducklake journal push HARV       # back up the site's journal now
```

A stage run by hand does not claim the site: check `neon-ducklake claims` first. It still refuses to write
the site lake's catalog or the journal pointer while another VM holds a live claim on the site. Chunk
stages and `finalize` run by hand spool their history, like `run`, and exit 4 when they should simply be
run again later.

**Sites finished in whole-site mode.** Sites that already ran with the whole-site stages have no chunk
journal. A stream run of such a site starts at `chunk-plan`, which plans every listed month, including
months whose raw files were uploaded and evicted. Their chunks pass quickly: download never fetches an
evicted file again, stack treats a month with no file on disk as empty, and the upload, apply, history and
verify stages are idempotent. Then `finalize` pushes the site lake and `cleanup` removes the local copy.
Raw files a whole-site run kept on disk (for example `--sample` probes or `--no-evict`) count against the
working set; evict them first (`site evict <SITE>`) or raise `--workset-bytes`.

## 11. Workers on CyVerse DE VMs, and the recorder

Several CyVerse Discovery Environment VMs ("AI Sandboxes") can each stream a different site at the same
time, next to sparky-1's own `run`. They coordinate through the Data Store, not through a shared disk.
Setting up a VM is covered in `docs/DE_SANDBOX.md`.

**On each VM.**

1. Provide the secrets in the environment: `NEON_TOKEN`, and the iRODS credentials either as
   `MESA_MCP_IRODS__USER`/`MESA_MCP_IRODS__PASSWORD` or through `iinit` (`~/.irods/irods_environment.json`
   plus `.irodsA`). `scripts/with-secrets.sh` uses them without a keyring when
   `NEON_DUCKLAKE_SECRETS=env` (or by default, `auto`, when they are present).
2. Give the VM a unique name for claims, spool paths and its ontology export:
   `NEON_DUCKLAKE_VM_ID=<name>` (default: the short host name). The worker registers it on the Data Store
   and refuses to start while another live worker uses the same id.
3. Start the worker, preferably detached:

   ```bash
   scripts/full_sweep.sh --worker --detach                 # preflight, confirm, then a tmux session
   scripts/with-secrets.sh .venv/bin/neon-ducklake worker --habitat aquatic --max-sites 5
   ```

   The worker:
   - bootstraps (`neon-ducklake bootstrap`: the catalog lake from the Data Store when missing or stale,
     the ontology cache from every VM's `ref/ontology_maps/vm/<vm>/` export, and `curation/generic/`);
     `--no-bootstrap` skips this;
   - repeatedly picks the next site (aquatic first, then by size, skipping sites that are done or
     claimed) and claims it at `<IRODS_ROOT>/sites/<SITE>/.claim`;
   - restores the site's journal and lake when another VM worked on it;
   - runs it in stream mode with spooled history;
   - pushes the site journal, writes `<IRODS_ROOT>/sites/<SITE>/.done` and releases the claim.

   A heartbeat renews the claim every 5 minutes (2 h TTL). When the claim is lost, the run stops at once
   (its stages are terminated) and the VM leaves the site to its new owner. One Ctrl-C (or `kill -INT`)
   stops after the running stages finish and releases the claim. `--sites`, `--habitat`, `--max-sites N`,
   `--once`, `--chunk-bytes` and `--workset-bytes` limit and size the work.

**Claims.**

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake claims                                # every claim, and done sites
scripts/with-secrets.sh .venv/bin/neon-ducklake claims --json
scripts/with-secrets.sh .venv/bin/neon-ducklake claims --release SRER --release HARV  # ours or stale ones
scripts/with-secrets.sh .venv/bin/neon-ducklake claims --release SRER --force         # a dead VM's live claim
```

A claim whose `expiresAt` has passed is stale. The next worker (or `run`) takes it over, restores the
journal and the site lake the previous VM pushed, and resumes the site. Use `--force` only when you know
the owning VM is gone.

**Journal backups.**

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake journal push SRER              # back up SRER's journal now
scripts/with-secrets.sh .venv/bin/neon-ducklake journal restore SRER [--force] # restore it on this host
```

Each push is an immutable set `.mesa-journal/v/<pushId>/` (one Parquet per table plus a manifest); the
`CURRENT` pointer names the current set and the two before it, and is replaced last, after the push
re-checked the site's claim. A restore verifies the set against its manifest and falls back to the previous
set when it does not verify. A push is refused while the Data Store holds a journal this host has not seen
(restore it first).

**The recorder (MESA history).** Stream runs and workers never open the MESA catalog. With the spool
backend, each history batch is written to `<IRODS_ROOT>/.mesa/spool/<vm>/<batch_id>.parquet` with a JSON
manifest, and the local journal rows become `spooled`. One recorder, on the host that owns
`~/.mesa/ducklake/catalog.duckdb` (sparky-1), writes them into MESA. `scripts/full_sweep.sh` starts it in
the tmux session `neon-recorder`; by hand:

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake recorder --watch --interval 300 --min-rows 200000 --max-rows 250000 --max-age-h 12
scripts/with-secrets.sh .venv/bin/neon-ducklake recorder --dry-run         # the backlog and its grouping
scripts/with-secrets.sh .venv/bin/neon-ducklake recorder --flush           # record everything pending now
```

What one recorder pass does:

- It takes the `mesa-tag` lock and ingests the spool in order.
- It coalesces: a group of batches (at most `--max-rows`, 250,000 rows) is recorded only once it holds
  `--min-rows` rows (default 200,000) or its oldest batch is `--max-age-h` hours old (default 12). The
  900-snapshot budget then lasts: about one snapshot per 250,000 rows, plus at most two small ones a day.
  Keep `--min-rows` below `--max-rows`: a group never exceeds `--max-rows`, so with both equal nearly every
  group would wait the full 12 hours.
- It notes each snapshot `neon-ducklake spool batch=<id>,...` and skips batches already committed and rows
  already recorded under another batch id.
- It moves each ingested file to `.mesa/spool-done/<vm>/` and never deletes one.

Close other MESA writers (mesa-mcp sessions) on that host; a pass that finds the catalog held retries at
the next interval. `status` and `scripts/sweep_status.sh` show the spool backlog of the local journal and
whether a recorder runs on this host.

**History backends.** `NEON_DUCKLAKE_HISTORY_MODE` (or `run --history-mode`) selects the backend. `spool`
is the only backend of stream runs and workers. `direct` records into the MESA catalog from the stage
itself and needs its lock: it is the default of whole-site (`--no-stream`) runs. It is also the path for
DuckDB 2.0 multi-writer catalogs: set `NEON_DUCKLAKE_MESA_MULTIWRITER=1` (and, for workers,
`NEON_DUCKLAKE_WORKER_HISTORY_MODE=direct`) once the MESA catalog supports parallel writers. Until then
`run` refuses `direct` in stream mode.

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
