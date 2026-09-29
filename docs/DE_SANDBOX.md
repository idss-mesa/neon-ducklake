# Running workers on CyVerse DE VMs ("AI Sandboxes")

This guide covers running neon-ducklake on several CyVerse Discovery Environment VMs at once, each one
working on different NEON sites. The design is in `docs/STREAMING.md`; this page is the operator's view.

MESA projects keep a metadata database, not the data. Each worker VM fetches a bounded chunk of a site,
analyzes it, writes AVUs, pushes data and history to the CyVerse Data Store, verifies the Data Store copy
and deletes its local copy. No VM ever needs room for a whole site, let alone all 81 sites (about 4 TB). The
Data Store has no practical size limit and holds everything that is kept.

## 1. Roles

| Role | Where | What it runs |
|---|---|---|
| **Worker** | every DE VM (one or more) | `neon-ducklake worker`: claim a site, stream it, release it, repeat |
| **Catalog host** | exactly one machine (today sparky-1) | owns the MESA catalog `~/.mesa/ducklake/catalog.duckdb` and runs `neon-ducklake recorder --watch`; may also stream sites itself with `neon-ducklake run` (it claims them like a worker) |
| **Operator** | any machine with the code and credentials | `neon-ducklake claims`, `recorder --dry-run`, log reading |

The VMs never talk to each other. Everything they share lives on the Data Store under the project
collection `<IRODS_ROOT>` (default `/iplant/home/<user>/neon-ducklake`):

| Data Store path | Written by | Purpose |
|---|---|---|
| `sites/<SITE>/.claim` | worker or `run` | who is working on the site (JSON: vm, host, machine, boot, pid, claimedAt, heartbeatAt, expiresAt, token) |
| `sites/<SITE>/.done` | worker or `run` | the site is finished (JSON summary) |
| `sites/<SITE>/.mesa-journal/` | worker or `run` | Parquet backup of the site's journal rows, so another VM can resume the site: one immutable set per push (`v/<pushId>/`), after every chunk's apply, history and delete; the `CURRENT` pointer is replaced last and names the current set and the two before it |
| `sites/<SITE>/ducklake/` | worker or `run` | the site lake, pushed after every chunk: data files first, then a new versioned catalog (`catalogs/neon_<site>.<version>.ducklake`, never overwritten), the `CURRENT` pointer, and the canonical `neon_<site>.ducklake` |
| `sites/<SITE>/raw/`, `cards/`, `reports/`, ... | worker or `run` | the site's data, cards and reports (with AVUs) |
| `.mesa/vms/<vm>.claim` | worker | registers the VM id, so two live VMs never share one |
| `.mesa/spool/<vm>/<batch>.parquet` + `.json` | worker or `run` | AVU history batches waiting for the recorder |
| `.mesa/spool-done/<vm>/` | recorder | batches already recorded in MESA (kept, never deleted) |
| `curation/claims/<DP>.claim` | worker | a product's Opus curation is in progress |
| `curation/generic/<DP>.validated.json` | worker | validated generic curation, reused by every VM |
| `ref/ontology_maps/vm/<vm>/` | worker or `run` | each VM's ontology mappings, published after each site's finalize; bootstrap merges them all |
| `ducklake/` | catalog host | the catalog lake each VM bootstraps from |

```
  DE VM 1 (worker)      DE VM 2 (worker)      DE VM n (worker)
  sites A, D, ...       sites B, E, ...       sites C, F, ...
        \                     |                     /
         \   claims, data, AVUs, journals, spool   /
          v                   v                   v
  +--------------------------------------------------------------+
  | CyVerse Data Store  <IRODS_ROOT>                             |
  |   sites/<SITE>/{.claim,.done,.mesa-journal/,raw/,...}        |
  |   .mesa/spool/<vm>/  ->  .mesa/spool-done/<vm>/              |
  |   curation/{claims,generic}/   ducklake/   ref/              |
  +--------------------------------------------------------------+
                              |
                              |  neon-ducklake recorder --watch (one host)
                              v
              MESA catalog ~/.mesa/ducklake/catalog.duckdb
              (coalesced AVU history snapshots, <= 250,000 rows each;
               900-snapshot budget for the whole project)
```

## 2. Prepare a VM

1. **Start the VM** from the DE with enough local disk for the working set. A worker holds at most
   `NEON_DUCKLAKE_WORKSET_BYTES` (default 50 GB) of raw files and site-lake data, plus a few GB of
   temporary files. 100 GB of free disk is comfortable. On a smaller disk, lower the cap (for example
   `--workset-bytes 20G`) and the chunk size with it (`--chunk-bytes 4G`).
2. **Install the code:**

   ```bash
   git clone https://github.com/idss-mesa/neon-ducklake && cd neon-ducklake
   uv sync --locked --extra dev
   .venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('ducklake', 'spatial')]"
   ```

3. **Optional: the Claude CLI** (`claude`) for Opus curation of products no VM has curated yet. Without it,
   products that already have `curation/generic` results on the Data Store still get their curated AVUs,
   and the others get only the data-derived terms.

## 3. Secrets

A VM has no login keyring, so the secrets come from the environment. `scripts/with-secrets.sh` picks them up
according to `NEON_DUCKLAKE_SECRETS`:

| Mode | NEON token | CyVerse credentials |
|---|---|---|
| `env` | `NEON_TOKEN` | `MESA_MCP_IRODS__USER` + `MESA_MCP_IRODS__PASSWORD`, else `~/.irods/irods_environment.json` + `~/.irods/.irodsA` (from `iinit`) |
| `keyring` | the login keyring (sparky-1) | the login keyring |
| `auto` (default) | per secret: the environment when present, else the keyring | per secret: the environment or the iRODS environment file when present, else the keyring |

On a VM, use `env`, so a missing secret fails fast (exit 3 for the token, 4 for CyVerse) instead of
reaching for a keyring that is not there. The script never prints a secret.

Two ways to provide the CyVerse credentials:

- **iinit (recommended).** Run `iinit` once (host `data.cyverse.org`, port 1247, zone `iplant`, your user
  name). It writes `~/.irods/irods_environment.json` and the scrambled password in `~/.irods/.irodsA`. The
  wrapper reads both; it also sets `CYVERSE_USERNAME` from the file when it is unset.
- **Environment variables.** Type the password without echo, so it never lands in shell history:

  ```bash
  read -rsp "CyVerse password: " MESA_MCP_IRODS__PASSWORD; echo; export MESA_MCP_IRODS__PASSWORD
  export MESA_MCP_IRODS__USER=<your CyVerse user>
  ```

The NEON token, the same way:

```bash
read -rsp "NEON API token: " NEON_TOKEN; echo; export NEON_TOKEN
export NEON_DUCKLAKE_SECRETS=env
```

Several VMs sharing one NEON token also share NEON's rate limit. For many VMs, use one token per VM or
lower `NEON_MCP_NEON__MAX_CONCURRENCY`.

## 4. Name the VM and bootstrap it

Every worker needs a unique id. It names the VM's claims and its spool directory:

```bash
export NEON_DUCKLAKE_VM_ID=de-vm-01     # default: the short host name; must differ between VMs
```

Then bring the VM up to date from the Data Store:

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake preflight
scripts/with-secrets.sh .venv/bin/neon-ducklake bootstrap
```

`bootstrap` (all parts by default; `--what catalog,ontology,curation`):

- **catalog**: fetches the catalog lake `<IRODS_ROOT>/ducklake/` into `ducklake/` when it is missing here
  or older than the Data Store copy. Data files first, the `.ducklake` catalog file last.
- **ontology**: seeds `work/ontology_cache.duckdb` from every VM's export under
  `<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/` (and the older shared `ref/ontology_maps/*.parquet`). A row
  replaces one already present here only when its status is better.
- **curation**: syncs `curation/generic/` both ways and imports fetched results into `curation_avus`.

The worker runs `bootstrap` itself at start, so this step is for checking the VM before a long run.

## 5. Run the worker

Use tmux (or nohup), so the worker survives a lost connection:

```bash
tmux new -s worker
cd ~/neon-ducklake
scripts/with-secrets.sh .venv/bin/neon-ducklake worker 2>&1 | tee -a logs/worker.log
```

At start, the worker registers its VM id (`.mesa/vms/<vm>.claim`; a second live worker with the same id
is refused) and bootstraps. Then, again and again, it:

1. picks the next site: aquatic sites first, then by size, skipping sites that have `.done`, that another
   VM holds a live claim on, or that failed earlier in this process;
2. claims it on the Data Store (atomically: exactly one VM wins a race);
3. restores the site's journal from `.mesa-journal/` and its lake from `sites/<SITE>/ducklake/` when
   another VM worked on the site since this VM last saw it;
4. streams the site (`run.run_sites([site], stream=True, history_mode='spool', stop_event=claim.lost)`):
   bounded chunks of download, stack, AVUs, upload (raw files, then the site lake), apply, history spool,
   verify, delete; then finalize and cleanup. The journal is pushed after every chunk's apply, history
   and delete, and after finalize and cleanup;
5. pushes the journal, writes `.done` and releases the claim.

A heartbeat thread renews the claim every 5 minutes (TTL 2 hours). A claim that cannot be renewed in time
is declared lost: the run stops at once, its stages are terminated, and the VM moves on without pushing
anything over the new owner's work. The worker runs with `NEON_DUCKLAKE_SITE_CLAIMS=on`, so every stage also
re-checks the claim (`coord.holds_site`) right before it writes the site lake's catalog or the journal
pointer, and writes nothing once the claim is gone, even in the minutes before the heartbeat notices.

A stage that hits a transient condition (OLS unavailable while taxa are pending, the site lake's catalog
being written) exits 4 and is retried with backoff (5 minutes, then 10, 20, 40, 60 and 60), not failed.
Only after 6 retries does the site fail; the worker then moves on and the site is picked again later.

| Option | Effect |
|---|---|
| `--sites HARV SRER` | only these sites, in this order |
| `--habitat aquatic` | only aquatic sites (or `Terrestrial`, or one aquatic class) |
| `--max-sites N` / `--once` | stop after N sites / after one |
| `--chunk-bytes 8G` | largest chunk (`NEON_DUCKLAKE_CHUNK_BYTES`) |
| `--workset-bytes 50G` | cap on the local working set: raw files plus site lakes (`NEON_DUCKLAKE_WORKSET_BYTES`) |
| `--no-bootstrap` | skip the start-up bootstrap (the VM is already current) |

**Several VMs.** Start the same command on each VM, each with its own `NEON_DUCKLAKE_VM_ID`. No other
coordination is needed: claims keep them on different sites, and curation claims keep them from paying
for the same product's Opus runs twice. sparky-1's own `neon-ducklake run` (or `scripts/full_sweep.sh`)
takes the same site claims, so it can run next to the workers; it skips sites they hold or finished.

**The iRODS connection budget is per VM.** `NEON_DUCKLAKE_IRODS_CONNECTIONS` (default 8) is enforced with
lock files on each machine, but CyVerse's per-user connection limit is shared by all of them. On
2026-09-29 sparky-1 alone saw handshakes refused (`NetworkException`, "Read 0 bytes from socket") above
about 10 connections, so the default leaves room for the root sessions, the claim heartbeat and the journal
push, which hold connections outside the budget. With `n` VMs on one CyVerse user, lower it on each VM
(and close idle mesa-mcp sessions). A refused connection is retried: the listing and every put retry on a
fresh session, the journal push retries in place, and a stage that still fails with one returns rc 4
(retry later), so the orchestrator backs off instead of failing the site.

**Stopping.** Press Ctrl-C once, or `kill -INT` / `kill -TERM` the worker. The site in progress stops, its
journal is pushed and its claim is released, so any VM can continue it. Chunks already verified and
deleted are not repeated.

## 6. The recorder (catalog host only)

Workers never open the MESA catalog. They spool their AVU history to `<IRODS_ROOT>/.mesa/spool/<vm>/`, and
one recorder on the catalog host records it:

On sparky-1, `scripts/full_sweep.sh` starts it in the tmux session `neon-recorder`. By hand:

```bash
tmux new -s neon-recorder
cd ~/neon-ducklake
scripts/with-secrets.sh .venv/bin/neon-ducklake recorder --watch --interval 300 --min-rows 200000 \
    --max-rows 250000 --max-age-h 12 2>&1 | tee -a logs/recorder.log
```

- It takes the `mesa-tag` lock and refuses while another process holds the MESA catalog (usually a
  mesa-mcp session: close it). In `--watch` mode it logs that and retries at the next interval.
- It coalesces, because the MESA project has a 900-snapshot budget for all 81 sites. Spool batches are
  grouped into snapshots of at most 250,000 rows (`--max-rows`). A group is recorded only once it holds
  `--min-rows` rows (default 200,000) or its oldest batch is `--max-age-h` hours old (default 12);
  `--flush` records everything pending now. Keep `--min-rows` below `--max-rows`, or nearly every group
  waits the full 12 hours. Each snapshot is noted `neon-ducklake spool batch=<id>,<id>,...`, and the budget
  is checked first.
- It is idempotent: a batch already in a committed snapshot is skipped, even after a crash between the
  commit and the move, and a row already recorded under another batch id is skipped too.
- Recorded files move to `.mesa/spool-done/<vm>/`. Nothing is deleted.
- `recorder --dry-run` shows the backlog and how it would be grouped, without opening MESA.

A bad batch (checksum or row count differs from its manifest) is reported and left in the spool; the
others are recorded.

## 7. Monitoring

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake claims                    # every claim, live or STALE, and finished sites
scripts/with-secrets.sh .venv/bin/neon-ducklake recorder --dry-run        # spool backlog (any machine)
.venv/bin/neon-ducklake status                                            # this VM's sites, chunks and local spool counts
tail -f logs/worker.log logs/run/<SITE>/*.log
```

- `claims` prints one line per claim: site or product, VM, host, pid, claimed, last heartbeat, and
  whether it is live or STALE.
- A site's `.done` JSON records the VM, the finish time, the run time, the chunk statuses and the journal
  row counts.
- On a worker VM, spooled batches stay `spooled` in its local journal; the recorder's backlog is what is
  still in `.mesa/spool/`.
- `status` also shows the VM's working set on disk (raw files plus site lakes) against its cap.

## 8. Stale claims

A claim is stale when its heartbeat stopped for longer than the TTL (2 hours), for example because the VM
was deleted. Nothing needs to be done by hand:

- The next worker (or sparky-1's `run`) that reaches the site breaks the stale claim (under a second atomic
  lock, so only one VM can), restores the site's journal from `.mesa-journal/` and its lake from
  `sites/<SITE>/ducklake/`, and continues the site.
- A worker restarted on the same VM (same `NEON_DUCKLAKE_VM_ID`, host and machine) resumes its own claim.

To free a site sooner, when you know its VM is gone:

```bash
scripts/with-secrets.sh .venv/bin/neon-ducklake claims --release HARV            # only stale or this VM's claims
scripts/with-secrets.sh .venv/bin/neon-ducklake claims --release HARV --force    # a live claim of another VM
```

Never force-release a claim whose VM is still running: two VMs would then work on the same site.

What a VM that takes a site over gets: the journal (listed and uploaded files, chunk and finalize states,
applied and spooled AVUs) as of the last push, which is at most one chunk stage old, and the site lake as of
the last chunk upload. Only the dead VM's chunks in flight (raw files on its disk, not yet uploaded) are
redone. AVUs the dead VM applied after its last push are adopted, not applied twice, and their history is
recorded once. Both the lake catalog and the journal are versioned on the Data Store, so a VM that died in
the middle of writing one leaves the previous version readable; nothing needs repairing by hand.

## 9. Curation across VMs

With `NEON_DUCKLAKE_CURATION_SYNC=on` (the worker sets it; `auto` enables it whenever
`NEON_DUCKLAKE_VM_ID` is set or the history mode is `spool`):

1. Before curating, `site curate` fetches `curation/generic/<DP>.validated.json` for every product of the
   site that has one on the Data Store. Those products are skipped, and their curated terms are imported
   into this VM's `curation_avus`.
2. It claims `curation/claims/<DP>.claim` for each product it will curate (TTL 6 hours, heartbeated while
   the Opus replicates run). A product claimed by another live VM is skipped at this site.
3. `site validate` publishes each validated product to `curation/generic/` and releases the claim.

## 10. History backends: spool now, `direct` with DuckDB 2.0

| Setting | Meaning |
|---|---|
| `NEON_DUCKLAKE_HISTORY_MODE=spool` | write batches to the Data Store spool for the recorder: every stream run (workers and sparky-1's `run`) |
| `NEON_DUCKLAKE_HISTORY_MODE=direct` | record into the MESA catalog from the stage itself: whole-site (`--no-stream`) runs only, until DuckDB 2.0 |
| `NEON_DUCKLAKE_WORKER_HISTORY_MODE` | the backend a worker uses: `spool` (default) or `direct` |
| `NEON_DUCKLAKE_MESA_MULTIWRITER=1` | the MESA catalog takes parallel writers (DuckDB 2.0): only then does a stream run accept `direct` |

Today the MESA catalog is a single-writer DuckDB file on one host, so every stream run spools and one
recorder writes; `run` refuses `direct` in stream mode. When DuckDB 2.0 lets several processes write one
catalog, point every VM at the shared catalog (`MESA_MCP_DUCKLAKE__CATALOG_DSN`) and set
`NEON_DUCKLAKE_MESA_MULTIWRITER=1` and `NEON_DUCKLAKE_WORKER_HISTORY_MODE=direct`: each VM then records its
own snapshots and the recorder is no longer needed. Per-chunk direct history still spends one snapshot per
chunk, so it must coalesce before the 900-snapshot budget allows it. Drain the spool first (`recorder
--flush` until `--dry-run` shows nothing), so no batch is left behind.

## 11. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `with-secrets: NEON_DUCKLAKE_SECRETS=env but NEON_TOKEN is not set` (exit 3) | export `NEON_TOKEN` (section 3) |
| exit 4 mentioning `iinit` | run `iinit`, or export `MESA_MCP_IRODS__USER` and `MESA_MCP_IRODS__PASSWORD` |
| exit 2 mentioning `mesa-keyring-common.sh` | the keyring was needed: set `NEON_DUCKLAKE_SECRETS=env` on a VM |
| `bootstrap catalog: missing` and the worker exits 1 | the catalog lake is not on the Data Store yet: run `neon-ducklake mirror --what lake` on the catalog host |
| a site is never picked | it has `.done`, or a live claim of another VM (`claims`) |
| `MESA catalog is held by PID(s) ...` in the recorder log | close that process (usually a mesa-mcp session); the recorder retries |
| `SYS_MAX_CONNECT_COUNT_EXCEEDED` in upload logs | too many VMs for the per-user connection limit: lower `NEON_DUCKLAKE_IRODS_CONNECTIONS` |
| the worker refuses to start: another live worker uses this VM id | give each VM its own `NEON_DUCKLAKE_VM_ID` |
| `run` refuses `--history-mode direct` in stream mode | stream runs spool; `direct` needs `NEON_DUCKLAKE_MESA_MULTIWRITER=1` (section 10) |
| a site reports `claim lost` | its claim could not be renewed (an iRODS outage) and another VM may own it now: check `claims` before re-running it |
| a stage log ends in `claim lost` (exit 1) | the stage found the site's claim gone right before writing the lake catalog or the journal pointer, and wrote nothing |
| `retry n/6 in Ns: transient (exit 4)` in the worker log | OLS was unavailable with taxa pending, or the site lake's catalog was being written; the stage is retried with backoff and fails only after 6 retries |
| `marked done` for a site in a `run` summary | the site had finished earlier but its `.done` was missing; `run` wrote it under the site's claim |
| `restore` fails for a site | the Data Store holds a newer journal that could not be restored, or the lake lacks stacked chunks: `neon-ducklake journal restore <SITE>` shows why |
| `finalize` waits: "another VM is curating ..." | another VM holds a product's curation claim; finalize continues once its result is published (up to `NEON_DUCKLAKE_WAIT_TIMEOUT_S`, 6 h) |
| a spool batch reported as skipped by the recorder | its Parquet does not match its manifest; it stays in `.mesa/spool/<vm>/` for inspection |
