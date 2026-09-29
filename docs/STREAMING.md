# Streaming and multi-VM execution

This is the contract of the streaming pipeline and of workers on several VMs, as implemented (review round 3,
2026-09-28). `docs/RUNBOOK.md` sections 10-11 and `docs/DE_SANDBOX.md` are the operator's view.

Nothing shared on the Data Store is overwritten in place: the site lake's catalog and the site's journal
backup are both written as immutable versions, and a small pointer is replaced last. A VM that dies, or is
killed when its claim is lost, at any moment leaves the previous version readable.

## Principle

MESA projects build a metadata *database*; they do not keep data locally. For every bounded chunk of NEON
data the steps are:

1. fetch;
2. analyze (stack, profile);
3. generate AVUs;
4. push the data, the stacked site lake, the AVUs and the history to the CyVerse Data Store;
5. verify against the Data Store copy;
6. delete the local copy.

Local disk is a bounded working set, `NEON_DUCKLAKE_WORKSET_BYTES` (default 50 GB): raw files plus the site
lakes' `data/**`. Workers run as CyVerse Discovery Environment VMs ("AI Sandboxes"), several at a time, each
on different sites. sparky-1 runs the same stream pipeline with `neon-ducklake run` and owns the MESA
catalog.

User decisions:

- Nothing is held locally beyond a bounded working set. Stacked Parquet lives on the Data Store: the site
  lake is pushed after every chunk, and its local `data/**` is deleted at cleanup.
- MESA history uses a spool plus one central recorder now. DuckDB 2.0 multi-writer catalogs (`direct` from
  every VM) come later and stay a switch (`NEON_DUCKLAKE_MESA_MULTIWRITER=1`).
- The MESA project has a 900-snapshot budget (mesa-ducklake `_ENSURE_CACHED_LIMIT` = 1000) for about 81
  sites. Stream runs therefore never record history into MESA per chunk: they spool, and the recorder
  coalesces.

## Chunks (`site/chunks.py`)

- `plan_chunks(site, *, max_bytes=None, products=None, months=None, con=None, record=True) -> list[Chunk]`.
  `max_bytes=None` reads `$NEON_DUCKLAKE_CHUNK_BYTES` (default 8 GB) at call time. Strings like `"10G"`
  work.
- A chunk holds one product with ascending months, packed greedily. A single month larger than `max_bytes`
  is a chunk by itself. Superseded files are left out.
- `chunk_id` is stable: `<product>__<firstMonth>_<lastMonth>`.
  - A chunk that has started never changes; months that appear later are packed around it.
  - Stale `planned` rows are removed.
  - A `deleted` chunk whose months gain new files (a re-list) goes back to `planned`, and the site's
    finalize steps are reset with it.
- `Chunk(site, chunk_id, product, months, n_files, bytes, status, done, attempt, error, started_at,
  finished_at, info)`.

Journal tables (`work/state.sqlite`):

| Table | Holds |
|---|---|
| `stream_chunks(site, chunk_id, product, months JSON, n_files, bytes, status, attempt, started_at, finished_at, error, done, info, planned_at)` | One row per chunk. `done` is the last status reached, so a `failed` chunk resumes after it. `info` holds per-stage JSON. |
| `stream_sites(site, step, status, attempt, started_at, finished_at, error, info)` | The finalize steps (terms, cards, curate, validate, avus, upload, apply, history, verify) and cleanup. An `ok` step is skipped on a re-run. |
| `lake_pushes(site, catalog_md5, pushed_at, snapshot_id, kind, vm, snapshot_key)` | Every site-lake catalog this host pushed to (`kind='push'`) or restored from (`kind='restore'`) the Data Store. |

Chunk statuses, in order: planned → downloaded → stacked → avus → uploaded → applied → recorded → verified
→ deleted. `spooled` history counts as `recorded`. Any stage can instead end in `failed`.

## Per-chunk stages (`site/stream.py`, each `(site, chunk_id, **kw) -> int`)

| Stage | What it does |
|---|---|
| `download_chunk` | Downloads only this chunk's rows (`fetch.download_rows`). Waits while the working set (`fetch.local_workset_bytes()`) plus the chunk would exceed the cap. Returns 3 when it never fits within `NEON_DUCKLAKE_WORKSET_TIMEOUT_S` (default 4 h). |
| `stack_chunk` | Appends the chunk's files on disk to the site lake (`stack.stack_chunk`); see the notes below the table. Refuses (rc 1) unless the local lake holds the stack snapshots of every chunk stacked before; a missing lake is restored first (`restore_lake`). |
| `avus_chunk` | Maps the chunk's taxa to NCBITaxon (rc 4 while OLS leaves taxa pending), then runs `avus.build_desired(site, [product], months, levels={'month', 'object'})`. Product, site, container, lake and side levels are built at finalize only, so there is no per-chunk churn. Variable counts come from the lake table `variables_files`. |
| `upload_chunk` | Uploads the chunk's raw files, then the site lake (`push_lake`, below): a new versioned catalog and its pointer, only while this VM holds the site's claim. |
| `apply_chunk` | `avu.bulk.apply_scope(site, only_units=<the chunk's month collections>)`. A desired AVU that is already in iRODS but that this journal does not own (another VM applied it before it died and this VM took the site over) is adopted: journaled as applied, so it is spooled and verified like the others. |
| `history_chunk` | `avu.history.record_scope(site)`: always the spool backend in stream runs (below). |
| `verify_chunk` | Checks four things: every chunk object has a good replica whose checksum equals `dl_md5`; its core AVUs are in iRODS; the journal rows are recorded or spooled; and the Data Store lake catalog is a recorded push that holds the chunk's stack snapshots, or its snapshot list holds them. The lake check runs under the host lock `lake-push-<SITE>`, which `push_lake` holds while it publishes, so verifying chunk k never races the push of chunk k+1; rc 4 when the Data Store catalog is being written. |
| `delete_chunk` | Deletes the chunk's local raw files, only after `verify_chunk` passed and re-checking each file, and marks `site_files.local_evicted=1`. It also deletes the superseded downloaded files of the chunk's months. |

How `stack_chunk` appends a chunk:

- Column types are merged from the lake and the chunk. An integer column is widened to DOUBLE when every
  value is numeric, else to VARCHAR.
- New columns are added.
- `uid` tables dedup against the rows already in the lake.
- Per-file tables (`file_stats`, `file_taxa`, `variables_files`, ...) are appended, and `table_catalog` is
  recomputed.
- Each unit is one DuckLake snapshot tagged `stream stack <SITE> <chunk> <unit>`. A re-run skips units that
  are already committed.

Return codes:

| rc | Meaning |
|---|---|
| 0 | Done, or already done. |
| 1 | Not ready: unknown chunk, an earlier stage missing, the chunk busy in another process, the local lake lacks earlier chunks, or the site's claim is lost. The chunk keeps its status. |
| 2 | The stage failed: status `failed`, `error` = `<stage>: <message>`. |
| 3 | Blocked: the working set stayed full, or a delete refused files. |
| 4 | `RC_RETRY`, retry later: a transient condition (OLS left taxa pending; the Data Store lake catalog is being written, or a push race could not be resolved). The chunk keeps its status. Site stages (`site restore`, `finalize`) use it too. |

## The site lake on the Data Store

Layout under `<IRODS_ROOT>/sites/<SITE>/ducklake/`:

| Path | Holds |
|---|---|
| `data/**` | the DuckLake data files; immutable, pushed before any catalog that refers to them |
| `catalogs/neon_<site>.<vkey>.ducklake` | one versioned catalog per push, written once and never overwritten; `<vkey>` = last snapshot id, a hash of the snapshot key and the md5, so a name pins both the lake history and the bytes; every version is kept |
| `CURRENT` | the pointer (JSON) to the current versioned catalog, with its md5 |
| `neon_<site>.ducklake` | the canonical copy, refreshed after the pointer (for readers that predate versioning) |

- `stream.push_lake(site) -> int` runs inside `upload_chunk` and again at finalize. It checkpoints and
  detaches the lake and pushes the new `data/**` files. It then publishes the catalog: the versioned copy
  (checksum-verified), then the pointer (written as `CURRENT.new-<id>`, read back and moved over `CURRENT`),
  then the canonical copy. Right before each of these writes it calls `coord.holds_site(site)`; when that is
  False it writes nothing more and returns 1 ("claim lost"). The push runs under the host lock
  `lake-push-<SITE>` and records the catalog's md5, time, last snapshot id and snapshot key in `lake_pushes`.
  It returns 0 (pushed), 1 (refused by the guard, or claim lost), 2 (an upload failed) or 4 (retry later:
  the Data Store catalog is being written).
- **Readers** (`upload.current_catalog`, `upload.fetch_current_catalog`, `restore_lake`, `bootstrap`) take
  the pointer's target when its good replica matches the pointer's md5; else the newest versioned catalog
  that verifies; else the canonical copy. A crash or a claim-loss kill at any point of a publish therefore
  leaves the previous version readable.
- **Catalog guard.** A new version is published only in one of these cases:
  - the Data Store has no catalog of the lake;
  - its current catalog equals the local catalog;
  - it equals the catalog this host last pushed or restored;
  - it holds only snapshots the local catalog also has.

  Anything else means another VM pushed a lake this host has not seen, so nothing is written
  (`restore_lake` brings it here first).
- `stream.restore_lake(site) -> int` brings the local lake up to the Data Store copy (the current version as
  above). It fetches missing `data/**` files (not for a cleaned site, whose Parquet stays on the Data Store
  unless a stage must write the lake again), and replaces the local catalog when it is missing, behind or
  diverged (the old one is kept as `.replaced-<time>`), recording `kind='restore'` in `lake_pushes`. It
  then **re-plans** the chunks the lake does not cover: every chunk marked at least `stacked` whose
  `stream stack` snapshots the lake lacks, and every chunk below `uploaded` whose raw files are missing or
  stale here (size and md5). It returns 0, 2 (the restore failed) or 4 (retry later: the catalog is being
  written and no copy verifies, or an uncovered chunk is busy in another process). The orchestrator's
  `restore` stage runs it before every site run (below).
- `stream.ensure_lake_data(site)`: after `cleanup_site` the lake's `data/**` exists only on the Data Store.
  A stage that writes the lake again (`site list`/`download`, stack, finalize) restores it first, with the
  data, and clears the site's `cleanup` step, so cleanup runs again afterwards.

## Site finalize and cleanup

`finalize_site(site)` runs after every chunk is deleted. Each step is journaled in `stream_sites` and
skipped once `ok`:

1. terms, cards (from lake tables), curate, validate (products, plus the site level);
2. `build_desired(levels={'site', 'container', 'product', 'lake', 'side'})`. Before this step, finalize
   returns 1 (not ready; the orchestrator retries later) while `validate.pending_generic(site)` lists
   products that another live VM is curating and whose generic result is not on the Data Store yet;
3. upload: `push_lake` first (a refused or claim-lost push uploads nothing else), then cards, curation and
   reports. The shared `ref/ontology_maps/` is not pushed from here: each VM publishes its own export;
4. apply;
5. history (spool);
6. site verify at metadata level (`verify_site.main(metadata_only=True)`; the raw files are gone).

It returns 0, 1 (not ready: chunks left, or another VM still curates), 2 (a step failed) or 4 (retry later:
the lake's catalog is being written). The orchestrator retries 1 and 4 later.

`cleanup_site(site)` then deletes the local lake `data/**`, cards and curation, after checking each file's
Data Store copy, plus any superseded raw file left.

- It keeps `reports/`, the lake catalog file and the journal, and writes `reports/cleanup.json`.
- It returns 0, 1 (not ready: finalize not finished) or 3 (some files kept). The orchestrator counts 3 as
  done, with a warning.
- After cleanup, the local catalog points at Parquet files that exist only on the Data Store.
  `ensure_lake_data` brings them back when a stage must write the lake again (a re-list after a NEON
  release), and drops the `cleanup` step, so the site is no longer finished until cleanup runs again.

## History backends (`avu/history.py`, `avu/recorder.py`)

`NEON_DUCKLAKE_HISTORY_MODE` selects the backend of `record_scope`:

- **`spool`** (every stream run). Each batch (at most 250k rows) is written under
  `<IRODS_ROOT>/.mesa/spool/<vm>/` as two files:
  - `<batch_id>.parquet`: the `AvuChange` columns plus `batch_id`, `scope`, `vm` and `journal_id`;
  - `<batch_id>.json`: a manifest with rows, sha256, md5 and createdAt, written after the Parquet.

  The journal rows become `spooled`. A batch interrupted mid-upload stays `spooling`, and the next call
  completes it, on this VM or on another one after a journal restore. The batch counts as spooled when
  `spool/<vm>/` or `spool-done/<vm>/` holds its Parquet with a matching md5. Its rows are released for
  re-spooling only when neither does.
- **`direct`** records into the MESA catalog and needs its single-writer lock. The `mesa-tag` lock is
  waited for up to `NEON_DUCKLAKE_MESA_LOCK_TIMEOUT_S` (default 1800 s). `direct` is the default of the
  whole-site (`--no-stream`) `record` stage.

**Stream runs always spool.**

- `run` exports `NEON_DUCKLAKE_HISTORY_MODE=spool` to every stage, and sets it in its own process for the
  run. `site chunk …` and `site finalize` run by hand also spool when the variable is unset.
- In stream mode, `run --history-mode direct`, or `direct` in the environment, is refused with a message.
  Per-chunk direct history would take the MESA lock for every chunk and spend at least one of the 900
  snapshots per chunk, and a site has at least one chunk per product (about 55 products per site).
- DuckDB 2.0 multi-writer catalogs are the switch. With `NEON_DUCKLAKE_MESA_MULTIWRITER=1`, `direct` is
  accepted. It must then also coalesce its snapshots, or the budget still runs out.

**Recorder: `neon-ducklake recorder [--watch] [--interval S] [--max-rows N] [--min-rows N] [--max-age-h H]
[--flush] [--dry-run]`.** It runs on the host that owns the MESA catalog (sparky-1). One pass:

1. Lists the spool, then takes the `mesa-tag` lock. It refuses while another process holds the MESA
   catalog.
2. Skips every batch id already in a committed snapshot note, including multi-batch notes.
3. **Coalesces.** The pending batches, in order, are cut into groups of at most `--max-rows` (250,000). A
   group is recorded only in one of these cases:
   - it holds `--min-rows` rows (default 200,000);
   - its oldest batch is older than `--max-age-h` hours (default 12);
   - `--flush` is given.

   The other groups wait for a later pass. A trickle of small batches therefore costs at most two
   snapshots a day. The snapshot budget guard runs first.
4. Checks each batch against its manifest (sha256, row count). A bad batch stays in the spool, and the
   later batches of that VM wait.
5. **Dedups by content.** A row is skipped when its key `sha1(irods_path, attribute, value, unit, op, ts)`
   was already recorded. That happens when a VM resumed from an older journal and spooled the same change
   again under another batch id. A VM that takes a site over gives an adopted AVU, or a reconciled intent,
   the time of the change another VM already spooled (not now), so its key matches and the change is
   recorded once.
6. Writes one snapshot per group, with the note `neon-ducklake spool batch=<id>[,<id>...] n=<k> rows=<n>`.
   It marks the local batches `recorded` and moves the files to `.mesa/spool-done/<vm>/`. It never deletes
   them.

`scripts/full_sweep.sh` starts the recorder on sparky-1 in the tmux session `neon-recorder`, unless one
already runs or `--no-recorder` is given. It runs `recorder --watch --interval 300 --min-rows 200000
--max-rows 250000 --max-age-h 12`. `--min-rows` must stay below `--max-rows`: a group never exceeds
`--max-rows`, so with both equal almost no group would ever be due and every snapshot would wait the full
12 hours. `recorder --flush` records the rest at once, for example at the end of a sweep.

## Coordination on the Data Store (`coord.py`, `journal_sync.py`)

**Site claims.**

- `<IRODS_ROOT>/sites/<SITE>/.claim` is created atomically with `data_objects.create(path, force=False)`;
  the server refuses an existing object. The claim JSON holds kind, key, vm, host, machine, boot, pid,
  token, claimedAt, heartbeatAt, expiresAt and ttlS.
- A claim is stale when `now > expiresAt`. A stale claim is broken under a second atomic lock,
  `.claim.break`: re-read, delete, create, settle, then re-read the token.
- A heartbeat renews the claim every 5 min (TTL 2 h) and re-reads it after each write; another token means
  the claim is lost. The holder also declares the claim lost itself when it could not renew by
  `expiresAt - 5 min`. A lost claim sets `claim.lost`.
- `coord.site_claim(site)` is a context manager for the orchestrator and the worker.
  - It yields the `Claim`.
  - It raises `SiteDone` when the site has `.done` (re-checked after winning), and `SiteClaimed` when
    another live VM holds the claim.
  - On exit it releases the claim, after checking the token.
- `<IRODS_ROOT>/sites/<SITE>/.done` (a JSON summary) marks a finished site.
- `coord.register_vm()` claims `<IRODS_ROOT>/.mesa/vms/<vm>.claim` at worker start, so two live workers
  with the same VM id are refused.
- **`coord.holds_site(site) -> bool`** is checked right before every write of a site's shared state: each
  step of a site-lake catalog publish, and the journal pointer. A False return aborts the write ("claim
  lost"; a chunk stage then returns 1). It reads `NEON_DUCKLAKE_SITE_CLAIMS`:
  - `on` (set by the worker, and by `run` for its stages and itself whenever it or its caller holds the
    claim): True only while the site's claim is live and this VM's, on this host, machine and boot. A
    missing, stale, foreign or pre-reboot claim is False.
  - `off` (`run --no-claims`) or unset (an operator's command): True unless another VM holds a live claim.

  `run` also exports `NEON_DUCKLAKE_REQUIRE_CLAIM` (1 with `on`, 0 with `off`), which the catalog guard of
  `site/upload.py` reads for the same fact.

**Curation.**

- `curation/claims/<DP>.claim` (TTL 6 h) guards a product's Opus replicates.
- Results are shared as `curation/generic/<DP>.validated.json`, and fetched before any curation runs.
- Which copy of a result is newer is decided by its content version (`validatedAt`, then `runId`), never by
  modify time.
- `NEON_DUCKLAKE_CURATION_SYNC=on|off|auto` switches the sharing. `auto` means on when a VM id is set or the
  history mode is `spool`, so it is on for every stream run.

**Journal backup.** Under `J = <IRODS_ROOT>/sites/<SITE>/.mesa-journal/`:

| Path | Holds |
|---|---|
| `J/v/<pushId>/<table>.parquet` + `J/v/<pushId>/manifest.json` | one immutable, versioned set per push |
| `J/CURRENT` | the pointer to the current set (pushId, pushedAt, vm, host, the manifest's path and sha256) plus `prev`, the two sets before it |
| `J/staged/`, `J/retired/` | pointers in the middle of a swap |

- `journal_sync.push_site_journal(site) -> dict` exports the site's rows of these tables inside one read
  transaction, so the set is one consistent snapshot:
  - by site: `site_files`, `listings`, `stream_chunks`, `stream_sites`, `fetch_superseded`, `lake_pushes`,
    `site_run_chunks`;
  - by scope: `avu_journal`, `batches`, `avu_desired`.

  Host-local tables (`fetch_workset`, `journal_sync_marks`, ...) never travel. It uploads the set
  (checksum-verified, manifest last), then publishes it by replacing the pointer last:
  1. write the new pointer to `J/staged/<pushId>.json`;
  2. check `coord.holds_site(site)` (False: raise `ClaimLost`; nothing is published);
  3. swap it in with renames, which refuse an existing target: move `CURRENT` to `J/retired/`, check it is
     the pointer this push was built on (else put it back and raise `JournalConflict`), move the staged
     pointer to `CURRENT`. Only one pusher can take `CURRENT` away, so the conflict check and the pointer
     write are one atomic step.

  A push is refused (`JournalConflict`) while the Data Store holds a journal this host has not seen, so a
  VM that lost the site never overwrites the new owner's backup. A failed push deletes its own set and
  leaves the pointer alone. The pointer's set and the two before it are kept; older sets are deleted only
  after a newer pointer landed.
- **Cadence.**
  - The orchestrator pushes after every chunk's `apply`, `history` and `delete` stages, and after
    `finalize` and `cleanup`. A failed push fails that stage. The push after `apply` means a VM that takes
    the site over sees the applied rows, instead of finding AVUs in iRODS its journal does not know.
  - The worker pushes again at the end of the site.
  - Finalize runs as one stage, so its nine internal steps are not pushed one by one. If a VM dies inside
    finalize, its `stream_sites` rows wait for the next push, and the next owner re-runs the steps (they
    are idempotent).
- `journal_sync.needs_restore(site) -> bool` is true when the pointer names a push this host has not
  pushed or restored last, and that push is newer or came from another VM.
- `journal_sync.restore_site_journal(site, *, force=False) -> bool` restores the backup:
  - it reads the pointer and downloads its set, checking every file against the manifest and the manifest
    against the pointer. A set that fails its checksums is skipped for the previous one in the chain (a
    transient iRODS error is raised instead);
  - every table gets its owner's schema; columns are added, never dropped;
  - the site's rows are replaced in one transaction. `applied` AVU rows whose batch is not in flight are
    released for spooling again (the recorder's dedup keeps the history single);
  - for a journal from another VM, chunks that never reached `uploaded` and whose raw files are not on this
    host go back to `planned`, and the finalize steps before an unfinished finalize upload are forgotten.
  - The flat layout of earlier versions (`J/manifest.json` plus `J/<table>.parquet`) is still read.

**Ontology mappings.** Each VM publishes its own export to `<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/`
(`bootstrap.publish_ontology`, which the orchestrator calls after each site's finalize). `bootstrap` merges
every VM's export; a row replaces a local one only when its status is better. No VM overwrites another
VM's mappings.

**Bootstrap: `neon-ducklake bootstrap [--what catalog,ontology,curation] [--force] [--json]`.**

- It fetches the catalog lake when it is missing or stale: data files first, the `.ducklake` catalog last.
  (The catalog lake under `<IRODS_ROOT>/ducklake/` is written by `mirror` on the catalog host only; the
  versioned layout above is the per-site lakes'.)
- It seeds the ontology cache from every VM's export.
- It syncs `curation/generic/` both ways, by content version.

## Orchestrator (`run.py`)

**Invocation:** `neon-ducklake run` streams by default; `--no-stream` gives the previous whole-site stages.

**Per site:** `restore → plan → list → chunk-plan → chunks → finalize → cleanup`.

- `restore` (`site restore <SITE>`) runs first whenever the site has any other stage to run.
  - It restores the journal backup when `needs_restore` says so. A newer backup that cannot be restored
    fails the stage (rc 2).
  - It then runs `stream.restore_lake(site)` and returns its rc (4: retry later).
- `chunks` is the chunk loop over `stream_chunks`, in plan order. It runs `site chunk <stage> <SITE> --chunk
  <ID>` for the eight chunk stages.

**Resource classes.** download is net-in (2 at once); stack and avus are cpu (1); upload is net-out (1);
apply is irods-meta (1); history, verify and delete are local. Spooled history never takes the `mesa`
class.

**Pipeline.** A chunk runs one stage at a time. Chunk k+1 runs a stage only after chunk k finished it, so
chunk k+1 downloads while chunk k stacks, uploads or applies. At most `--chunks-in-flight` (2) chunks of a
site are between download and delete.

**Working set.** A chunk download is dispatched only while the working set plus the bytes the chunk adds
fit the cap, and free disk covers 1.3 × the chunk. The working set is `fetch.local_workset_bytes()`: raw
files on disk (superseded ones included) plus the site lakes' `data/**`. When no other chunk holds raw
bytes, the chunk may start anyway: a chunk larger than the cap runs alone, and a lake alone never blocks.
The cap is exported to the stages.

**Retry later, not fail.**

- A task waits instead of failing while its condition is not met. The condition is checked again every
  minute. After `NEON_DUCKLAKE_WAIT_TIMEOUT_S` (default 6 h), the task fails with the reason.
  - `finalize` waits while `validate.pending_generic(site)` is not empty.
  - A task that writes MESA waits while another process holds the MESA catalog. That is the whole-site
    `record` stage, and, with the multi-writer switch, `direct` chunk history and finalize.
- A stage whose return code asks for a later retry (`RETRY_RC`) is dispatched again after a delay, instead
  of failing:
  - `RC_RETRY` (4) from any stream stage, chunk or site-level: a transient condition, such as OLS leaving
    taxa pending or the lake's Data Store catalog being written. A stage (or finalize step, or the lake
    restore) that raises a transient iRODS/network error (`tuning.transient_irods`: a refused handshake, a
    reset, a timeout, `SYS_MAX_CONNECT_COUNT_EXCEEDED`, a locked object) returns it too, instead of failing;
  - chunk `download` rc 3 (the working set stayed full);
  - `finalize` rc 1 (another VM still curates one of the site's products).

  The delays back off: 5, 10, 20, 40 and 60 minutes, then 60 again (capped at 1 h; about 3.3 h in all). After
  6 retries the stage fails with its reason ("gave up after 6 retries"), and the next invocation starts over.
  While a stage waits, the rest of the site's pipeline waits behind it in plan order; other sites go on.
- `cleanup` rc 3 (some local files kept) counts as done, with a warning.

**Claims.** Plain `run` takes `coord.site_claim(site)` for each site before its first stage, when the
site's stages include one that writes to the Data Store (`restore`, `chunks`, `finalize`, `cleanup`).

- A site with `.done`, or with a live claim of another VM, is skipped. Its first stage row becomes
  `skipped`, and it does not count as a failure.
- When `claim.lost` is set mid-run, that site stops: no new stages, its running children are terminated, no
  journal is pushed and no `.done` is written.
- A site that finishes (cleanup ok, not a sample run) gets `.done`, then its claim is released.
- **A finished site without its marker.** A site this host finished (its `cleanup` stage ok in a full run,
  every chunk deleted, and the stream journal's `cleanup` step ok or partial) plans nothing. Its `.done`
  may still be missing: the write failed, or the site finished under `--no-claims`. Such a site is claimed
  briefly when it gets a slot:
  - `SiteDone`: the marker exists; nothing to do (no claim is taken).
  - `SiteClaimed`: another VM works on the site now (for example after a NEON release); it is left alone.
  - Claimed: when `journal_sync.needs_restore(site)` says another VM pushed a newer journal backup since,
    this host's view is stale. The site then runs `restore`, `chunk-plan`, `chunks`, `finalize` and
    `cleanup` under the claim (the restored journal decides what is left), and gets `.done` when they finish.
    Otherwise `.done` is written now, and the claim is released.
  - A Data Store error, or a journal backup that cannot be checked, writes no marker and fails nothing; the
    next run tries again.
- Every stream stage learns whether a claim is in force: `NEON_DUCKLAKE_SITE_CLAIMS=on` (and
  `NEON_DUCKLAKE_REQUIRE_CLAIM=1`) when `run` or its caller holds it, `off` (0) for `--no-claims`. With
  `on`, `coord.holds_site` requires this VM's live claim before every lake-catalog and journal-pointer write.
- `run --no-claims` skips all of this. Use it only for single-host runs that no worker VM can touch.
  Whole-site mode takes no claims.

**After stages.** The journal is pushed after chunk `apply`, `history` and `delete`, and after `finalize`
and `cleanup` (above). The ontology export is published after `finalize`.

**Stop.** The first SIGINT/SIGTERM stops scheduling and lets running stages finish; a second terminates
them.

**Library entry point** (called by `worker.py`):

```python
run.run_sites(sites, *, stream=True, sample=None, stages=None, chunk_bytes=None, workset_bytes=None,
              history_mode=None, stop_event: threading.Event | None = None, claims=False) -> int
```

- It returns 0 when every site finished or had nothing to do, 1 on a failure, and 130 when stopped.
- Once `stop_event` is set, the run stops scheduling, terminates the running children at once, pushes no
  more journals and returns 130. The worker passes `claim.lost`.
- The worker holds the claim, so `run_sites` takes none (and checks no `.done` marker: the worker writes
  it). Its stages still run with `NEON_DUCKLAKE_SITE_CLAIMS=on`.

**Status.** `neon-ducklake status` shows:

- deleted/total chunks per site;
- running and failed chunk stages;
- raw bytes held, and the working set against its cap;
- the batches this host spooled and has not seen recorded.

A recorder on another host does not update a worker's journal, so the Data Store backlog is
`recorder --dry-run`.

## Worker (`worker.py`)

**Invocation:** `neon-ducklake worker [--sites …] [--habitat …] [--max-sites N] [--chunk-bytes N]
[--workset-bytes N] [--once] [--no-bootstrap]`.

**Loop:**

1. Check the history mode and the orchestrator configuration (a bad `NEON_DUCKLAKE_WORKER_HISTORY_MODE`
   stops the worker before it registers or claims anything), set `NEON_DUCKLAKE_SITE_CLAIMS=on`, register
   the VM id (`coord.register_vm`) and bootstrap (unless `--no-bootstrap`).
2. Pick the next site: aquatic first, then by size. Skip sites with `.done`, live claims of other VMs, and
   sites that failed in this process.
3. Claim it with `coord.site_claim(site)`. Restore the journal when `needs_restore`, then the site lake
   (`stream.restore_lake`).
4. Run `run.run_sites([site], stream=True, history_mode='spool', stop_event=claim.lost)`.
5. Push the journal, write `.done` when the run succeeded (cleanup rc 3 counts), and release the claim. A
   lost claim is neither released nor marked done, and its journal is not pushed.

SIGINT/SIGTERM stop the run; the journal is pushed and the claim released.

`NEON_DUCKLAKE_WORKER_HISTORY_MODE=direct` is the worker's switch for DuckDB 2.0 multi-writer MESA; `run`
also requires `NEON_DUCKLAKE_MESA_MULTIWRITER=1`.

**Secrets:** `scripts/with-secrets.sh`, with `NEON_DUCKLAKE_SECRETS=env|keyring|auto`. The default, `auto`,
decides per secret: the environment (or `~/.irods` from `iinit`) when present, else the keyring.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `NEON_DUCKLAKE_WORKSET_BYTES` | 50 GB | Cap on the local working set (raw files + site lakes); `run --workset-bytes`. |
| `NEON_DUCKLAKE_CHUNK_BYTES` | 8 GB | Largest chunk; `run --chunk-bytes`. |
| `NEON_DUCKLAKE_HISTORY_MODE` | spool (stream), direct (whole-site) | History backend. `direct` is refused in stream mode without the multi-writer switch. |
| `NEON_DUCKLAKE_MESA_MULTIWRITER` | off | `1`: the MESA catalog takes parallel writers (DuckDB 2.0), which allows `direct` in stream mode. |
| `NEON_DUCKLAKE_WORKER_HISTORY_MODE` | spool | The worker's backend; `direct` only with the multi-writer switch. |
| `NEON_DUCKLAKE_VM_ID` | short host name | Names the VM's claims, spool directory and ontology export. Must be unique per VM. |
| `NEON_DUCKLAKE_SECRETS` | auto | Where `with-secrets.sh` finds the secrets: `env`, `keyring` or `auto`. |
| `NEON_DUCKLAKE_CURATION_SYNC` | auto | Share curation claims and results on the Data Store. |
| `NEON_DUCKLAKE_WAIT_TIMEOUT_S` | 21600 | How long a task may wait for MESA or for another VM's curation. |
| `NEON_DUCKLAKE_WORKSET_TIMEOUT_S` | 14400 | How long a chunk download waits for the working set. |
| `NEON_DUCKLAKE_MESA_LOCK_TIMEOUT_S` | 1800 | How long a MESA writer waits for the `mesa-tag` lock. |
| `NEON_DUCKLAKE_CLAIM_TTL_S`, `NEON_DUCKLAKE_HEARTBEAT_S` | 7200, 300 | Site claim TTL and heartbeat interval. |
| `NEON_DUCKLAKE_SITE_CLAIMS` | set by `run` and the worker | `on`: a site claim is in force, and `coord.holds_site` requires this VM's live claim before every shared write of the site; `off` (`run --no-claims`) or unset: only another VM's live claim blocks. |
| `NEON_DUCKLAKE_REQUIRE_CLAIM` | set by `run` | `1` with `on`, `0` with `off`: the same fact for the lake-catalog guard of `site/upload.py`. |
| `NEON_DUCKLAKE_ROWS_PER_BATCH` | 250,000 | Rows per history batch and per recorder snapshot. |
| `NEON_DUCKLAKE_RECORDER_MIN_ROWS`, `NEON_DUCKLAKE_RECORDER_MAX_AGE_H` | 200,000, 12 | The recorder's coalescing defaults. |
