#!/usr/bin/env bash
# scripts/full_sweep.sh: run the neon-ducklake Phase-2 sweep over every NEON site (or a selection), safely.
#
#   scripts/full_sweep.sh --help
#   scripts/full_sweep.sh --dry-run          # plan + size/time estimate only; needs no secrets
#   scripts/full_sweep.sh --detach           # preflight, plan, confirm, then run in a tmux session
#   scripts/full_sweep.sh --worker --detach  # a CyVerse DE VM: claim sites on the Data Store one by one
#
# Steps: checks (repo root, .venv, no other orchestrator) -> preflight -> plan and estimate -> confirmation
# -> [--refresh-catalog] -> the MESA history recorder (stream runs spool their history; `neon-ducklake recorder
# --watch` in the tmux session neon-recorder, unless one runs already) -> `neon-ducklake run` (stream mode by
# default: bounded chunks, pushed to the Data Store and deleted locally, each site claimed on the Data Store;
# docs/STREAMING.md) -> `neon-ducklake status`. With --worker the last steps are `neon-ducklake worker`
# instead (it picks, claims and streams sites itself; the recorder runs on the MESA-catalog host). Every
# command that talks to NEON, iRODS or MESA runs through scripts/with-secrets.sh, which provides the secrets
# inside that command (login keyring, or the environment on a DE VM: NEON_DUCKLAKE_SECRETS=env). This script
# never reads, prints or passes a secret.
set -euo pipefail

readonly WHOLE_STAGES="plan list download stack terms cards curate validate upload apply record verify evict"
readonly STREAM_STAGES="restore plan list chunk-plan chunks finalize cleanup"

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly ROOT
readonly SELF="$ROOT/scripts/full_sweep.sh"
readonly ND="$ROOT/.venv/bin/neon-ducklake"
readonly PY="$ROOT/.venv/bin/python"
readonly SECRETS="$ROOT/scripts/with-secrets.sh"
readonly RECORDER_SESSION="neon-recorder"
# coalesced MESA snapshots (900-snapshot budget): groups of at most 250k spooled rows, each recorded once it holds
# 200k rows or its oldest batch is 12 h old. --min-rows must stay below --max-rows: a group never exceeds
# --max-rows, so with min == max nearly every group would wait the full 12 h (docs/STREAMING.md "Recorder").
readonly RECORDER_ARGS=(recorder --watch --interval 300 --min-rows 200000 --max-rows 250000 --max-age-h 12)

usage() {
  cat <<'EOF'
Usage: scripts/full_sweep.sh [OPTIONS] [-- EXTRA 'neon-ducklake run' OPTIONS]

Run the Phase-2 site pipeline (`neon-ducklake run`) over all NEON sites or a selection:
preflight -> plan and estimate -> confirmation -> run -> status. Safe to re-run at any time:
every site resumes at its first stage that is not ok.

Site selection (default: --all):
  --all                  every site in the catalog lake; aquatic sites first, then by size
  --sites 'A B ...'      these site codes, in this order (space- or comma-separated; repeatable)
  --habitat HABITAT      Terrestrial | aquatic | 'Aquatic Wadeable Stream' | 'Aquatic Lake' |
                         'Aquatic Non-wadeable Stream' (with --sites it filters those sites)

Run options:
  --stream               (default) per site: restore, plan, list, chunk-plan, then bounded chunks
                         (download, stack, AVUs, upload, apply, history, verify, delete), finalize,
                         cleanup; raw data never stays on local disk, each site is claimed on the Data
                         Store first (sites done or claimed by a worker VM are skipped) and its journal
                         is pushed after every chunk (docs/STREAMING.md)
  --no-stream            the previous whole-site stages (plan ... verify, evict)
  --chunk-bytes N        largest chunk (default 8 GB; 8G, 500M, ...)
  --workset-bytes N      cap on the local working set: raw files plus site lakes (default 50 GB)
  --history-mode MODE    stream: spool only (history batches go to the Data Store and the recorder
                         ingests them); direct needs NEON_DUCKLAKE_MESA_MULTIWRITER=1 (DuckDB 2.0
                         multi-writer MESA). --no-stream: direct (default) | spool
  --no-recorder          do not start `neon-ducklake recorder --watch` (tmux session neon-recorder)
                         for the spooled history; start it yourself on this host
  --sample N             probe mode: only the N most recent months per product (always the
                         latest released and latest PROVISIONAL month); --no-stream: never evicts
  --no-evict             --no-stream only: keep raw files on disk (the evict stage is skipped)
  --from-stage STAGE     re-run from STAGE even when later stages are ok; after a new NEON
                         release use --refresh-catalog --from-stage list
  --max-in-flight N      sites between their first and last stage at once (default 3)
  --refresh-catalog      first harvest a new catalog run: catalog fetch --new, catalog seal,
                         catalog build, verify catalog, diff, mirror --what api,lake (~1.5-2.5 h)
  -- ARGS                pass ARGS on to `neon-ducklake run` (e.g. -- --order size --threads 12)
                         or, with --worker, to `neon-ducklake worker`

Worker mode (CyVerse Discovery Environment VMs; several VMs at once, one site each):
  --worker               run `neon-ducklake worker` instead of `run`: bootstrap, then repeatedly
                         claim the next free site on the Data Store (aquatic first, then by size),
                         stream it with spooled history, write .done and release the claim.
                         Site selection: --sites / --habitat limit the queue (default: all).
                         Run `neon-ducklake recorder --watch` on the host that owns the MESA
                         catalog to ingest the spooled history (a local sweep there starts it).
  --max-sites N          stop after N sites
  --once                 process at most one site

Execution:
  --dry-run              print the plan and the size/time estimate, then exit; needs no secrets,
                         runs no preflight and starts nothing
  -y, --yes              do not ask for confirmation
  --detach[=tmux|nohup]  after preflight and confirmation, run in the background: in a tmux
                         session (neon-sweep-<UTC>) when tmux is available, else under nohup
  -h, --help             show this help

Each run writes logs/sweep-<UTC>.log; stage logs are in logs/run/<SITE>/<stage>.log and chunk logs
in logs/run/<SITE>/chunks/<ID>/<stage>.log. Monitor with scripts/sweep_status.sh. Stop cleanly with one
Ctrl-C, or `kill -INT` the orchestrator: running stages finish, a second signal terminates them. Re-run
the same command to resume (a worker releases its claim and the site resumes on any VM).

Exit status: 0 every selected site finished; 1 a check, step or stage failed; 2 usage error or
MESA local install missing; 3 NEON token / 4 CyVerse password not available from the keyring;
130 interrupted.
EOF
}

# ---------------------------------------------------------------- helpers

say() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
warn() { printf '%s WARNING: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() {
  local rc=1
  if [[ $1 =~ ^[0-9]+$ ]]; then rc=$1; shift; fi
  printf '%s full_sweep: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
  exit "$rc"
}
usage_error() { printf 'full_sweep: %s (see --help)\n' "$*" >&2; exit 2; }
need_value() { [[ $# -ge 2 && -n $2 ]] || usage_error "$1 needs a value"; }
nd() { "$ND" "$@"; }                       # commands that need no secrets (run --dry-run, status, diff)
nd_secret() { "$SECRETS" "$ND" "$@"; }     # everything that talks to NEON, iRODS or MESA

# ---------------------------------------------------------------- arguments

sites=() habitat="" all=0 sample="" no_evict=0 from_stage="" max_in_flight="" refresh=0
dry_run=0 yes=0 detach="" child_log="" extra=() no_recorder=0
stream=1 chunk_bytes="" workset_bytes="" history_mode="" worker=0 max_sites="" once=0
readonly SIZE_RE='^[0-9]+(\.[0-9]+)?([KkMmGgTt][Ii]?)?[Bb]?$'

while (($#)); do
  if [[ $1 == --*=* ]]; then set -- "${1%%=*}" "${1#*=}" "${@:2}"; fi
  case $1 in
    -h|--help) usage; exit 0 ;;
    --all) all=1 ;;
    --sites) need_value "$@"; read -r -a _codes <<<"${2//,/ }"; sites+=("${_codes[@]}"); shift ;;
    --habitat) need_value "$@"; habitat=$2; shift ;;
    --sample) need_value "$@"; sample=$2; shift ;;
    --no-evict) no_evict=1 ;;
    --from-stage) need_value "$@"; from_stage=$2; shift ;;
    --max-in-flight|--max-sites-in-flight) need_value "$@"; max_in_flight=$2; shift ;;
    --refresh-catalog) refresh=1 ;;
    --stream) stream=1 ;;
    --no-stream) stream=0 ;;
    --chunk-bytes) need_value "$@"; chunk_bytes=$2; shift ;;
    --workset-bytes) need_value "$@"; workset_bytes=$2; shift ;;
    --history-mode) need_value "$@"; history_mode=$2; shift ;;
    --no-recorder) no_recorder=1 ;;
    --worker) worker=1 ;;
    --max-sites) need_value "$@"; max_sites=$2; shift ;;
    --once) once=1 ;;
    --dry-run) dry_run=1 ;;
    -y|--yes) yes=1 ;;
    --detach)
      if [[ ${2:-} == tmux || ${2:-} == nohup ]]; then detach=$2; shift; else detach=auto; fi ;;
    --_child) need_value "$@"; child_log=$2; shift ;;   # internal: the detached run (preflight already done)
    --) shift; extra=("$@"); break ;;
    *) usage_error "unknown option: $1" ;;
  esac
  shift
done

for i in "${!sites[@]}"; do
  code=${sites[i]^^}
  [[ $code =~ ^[A-Z0-9]{4}$ ]] || usage_error "not a NEON site code: ${sites[i]}"
  sites[i]=$code
done
if ((all)) && ((${#sites[@]})); then usage_error "use either --all or --sites"; fi
[[ -z $sample || $sample =~ ^[1-9][0-9]*$ ]] || usage_error "--sample needs a positive integer"
[[ -z $max_in_flight || $max_in_flight =~ ^[1-9][0-9]*$ ]] || usage_error "--max-in-flight needs a positive integer"
[[ -z $max_sites || $max_sites =~ ^[1-9][0-9]*$ ]] || usage_error "--max-sites needs a positive integer"
[[ -z $chunk_bytes || $chunk_bytes =~ $SIZE_RE ]] || usage_error "--chunk-bytes needs a size (bytes, or e.g. 8G)"
[[ -z $workset_bytes || $workset_bytes =~ $SIZE_RE ]] || usage_error "--workset-bytes needs a size (bytes, or e.g. 50G)"
[[ -z $history_mode || $history_mode == direct || $history_mode == spool ]] \
  || usage_error "--history-mode must be direct or spool"
if ((stream)); then STAGES=$STREAM_STAGES; else STAGES=$WHOLE_STAGES; fi
readonly STAGES
multiwriter=0
case ${NEON_DUCKLAKE_MESA_MULTIWRITER:-} in 1|true|TRUE|yes|on) multiwriter=1 ;; esac
if ((stream && !multiwriter && !worker)) && [[ $history_mode == direct || ( -z $history_mode && ${NEON_DUCKLAKE_HISTORY_MODE:-} == direct ) ]]; then
  usage_error "stream mode spools its MESA history for the recorder; direct needs DuckDB 2.0 multi-writer (NEON_DUCKLAKE_MESA_MULTIWRITER=1). Drop --history-mode direct / unset NEON_DUCKLAKE_HISTORY_MODE, or add --no-stream"
fi
if ((worker && !multiwriter)) && [[ ${NEON_DUCKLAKE_WORKER_HISTORY_MODE:-} == direct ]]; then
  usage_error "NEON_DUCKLAKE_WORKER_HISTORY_MODE=direct needs DuckDB 2.0 multi-writer MESA (NEON_DUCKLAKE_MESA_MULTIWRITER=1); unset it to spool"
fi
# the history backend the run will use: stream spools (unless the multi-writer switch and direct), whole-site records directly
history_eff=${history_mode:-${NEON_DUCKLAKE_HISTORY_MODE:-}}
if [[ -z $history_eff ]]; then
  if ((stream)); then history_eff=spool; else history_eff=direct; fi
fi
if ((stream && no_evict)); then
  usage_error "--no-evict keeps raw files, but stream mode deletes every verified chunk: add --no-stream"
fi
if [[ -n $from_stage && " $STAGES " != *" $from_stage "* ]]; then
  usage_error "unknown stage '$from_stage'; $( ((stream)) && echo stream || echo whole-site) stages: $STAGES"
fi
if ((worker)); then
  for opt in sample from_stage max_in_flight history_mode; do
    [[ -z ${!opt} ]] || usage_error "--worker picks and streams sites itself: --${opt//_/-} does not apply"
  done
  ((!no_evict && !refresh && stream)) || usage_error "--worker always streams: drop --no-evict/--no-stream/--refresh-catalog"
else
  if [[ -n $max_sites ]] || ((once)); then usage_error "--max-sites and --once apply to --worker only"; fi
fi
if ((worker && no_recorder)); then usage_error "--no-recorder applies to local runs (a worker never starts the recorder)"; fi
if ((dry_run)) && [[ -n $detach ]]; then usage_error "--dry-run and --detach exclude each other"; fi
if [[ $detach == tmux ]] && ! command -v tmux >/dev/null 2>&1; then usage_error "--detach=tmux: tmux not found"; fi

run_args=()
if ((${#sites[@]})); then run_args+=("${sites[@]}"); elif [[ -z $habitat ]]; then run_args+=(--all); fi
if [[ -n $habitat ]]; then run_args+=(--habitat "$habitat"); fi
if [[ -n $sample ]]; then run_args+=(--sample "$sample"); fi
if ((no_evict)); then run_args+=(--no-evict); fi
if [[ -n $from_stage ]]; then run_args+=(--from-stage "$from_stage"); fi
if [[ -n $max_in_flight ]]; then run_args+=(--max-sites-in-flight "$max_in_flight"); fi
if ((!stream)); then run_args+=(--no-stream); fi
if [[ -n $chunk_bytes ]]; then run_args+=(--chunk-bytes "$chunk_bytes"); fi
if [[ -n $workset_bytes ]]; then run_args+=(--workset-bytes "$workset_bytes"); fi
if [[ -n $history_mode ]]; then run_args+=(--history-mode "$history_mode"); fi
run_args+=("${extra[@]}")
evict=1
if ((!stream)) && { [[ -n $sample ]] || ((no_evict)); }; then evict=0; fi

worker_args=(worker)
if ((${#sites[@]})); then worker_args+=(--sites "${sites[@]}"); fi
if [[ -n $habitat ]]; then worker_args+=(--habitat "$habitat"); fi
if [[ -n $max_sites ]]; then worker_args+=(--max-sites "$max_sites"); fi
if [[ -n $chunk_bytes ]]; then worker_args+=(--chunk-bytes "$chunk_bytes"); fi
if [[ -n $workset_bytes ]]; then worker_args+=(--workset-bytes "$workset_bytes"); fi
if ((once)); then worker_args+=(--once); fi
worker_args+=("${extra[@]}")
RECORDER_NOTE="history is spooled to the Data Store (<IRODS_ROOT>/.mesa/spool/<vm>/): run 'scripts/with-secrets.sh .venv/bin/neon-ducklake ${RECORDER_ARGS[*]}' on the host that owns the MESA catalog (~/.mesa/ducklake/catalog.duckdb) to ingest it"

# ---------------------------------------------------------------- environment

cd "$ROOT"
export NEON_DUCKLAKE_ROOT="${NEON_DUCKLAKE_ROOT:-$ROOT}"
DATA_ROOT=$NEON_DUCKLAKE_ROOT
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

if [[ ! -x $ND || ! -x $PY ]]; then
  die "$ROOT/.venv is missing or incomplete: install with 'uv sync --extra dev' (see README.md, Install)"
fi
if ! command -v uv >/dev/null 2>&1; then say "note: uv is not on PATH (it is only needed to install or update .venv)"; fi
"$PY" -c 'import neon_ducklake' 2>/dev/null || die "the neon_ducklake package does not import from .venv: run 'uv sync --extra dev'"
[[ -x $SECRETS ]] || die "$SECRETS is missing or not executable"

LOG=""
if ((!dry_run)); then
  LOG=${child_log:-"$DATA_ROOT/logs/sweep-$STAMP.log"}
  mkdir -p -- "$(dirname -- "$LOG")"
  # Copy all output to the log. tee ignores SIGINT/SIGTERM/SIGHUP so that a Ctrl-C reaches only the
  # orchestrator (which stops gracefully) and never breaks the pipe it writes to.
  exec > >(trap '' INT TERM HUP; exec tee -a -- "$LOG") 2>&1
fi

# ---------------------------------------------------------------- steps

orch_running=0 orch_pid="" snaps_used=0

read_status() {
  local out
  out=$(nd status --json 2>/dev/null | "$PY" -c '
import json, sys
r = json.load(sys.stdin)
print(int(bool(r.get("orchestratorRunning"))), int((r.get("allScopes") or {}).get("snapshots") or 0))
' 2>/dev/null) || out="0 0"
  read -r orch_running snaps_used <<<"$out"
  orch_pid=""
  if ((orch_running)) && [[ -r $DATA_ROOT/work/locks/run.lock ]]; then
    orch_pid=$(head -n 1 -- "$DATA_ROOT/work/locks/run.lock" 2>/dev/null || true)
  fi
}

run_preflight() {
  local rc=0 args=(preflight)
  if ((${#sites[@]} == 1)); then args+=(--site "${sites[0]}"); fi
  say "== preflight (read-only)"
  nd_secret "${args[@]}" || rc=$?
  case $rc in
    0) say "preflight ok" ;;
    2) die 2 "the MESA local install was not found (see the message above and README.md, Requirements)" ;;
    3) die 3 "the NEON token is not available from the keyring: unlock it, or store it with \$MESA_HOME/bin/neon-keyring-set" ;;
    4) die 4 "the CyVerse password is not available from the keyring: unlock the login keyring" ;;
    *) die "preflight reported hard failure(s) (exit $rc): fix them first (docs/RUNBOOK.md, section 1)" ;;
  esac
}

# Size and time estimate from the dry-run plan (stdin) and the catalog lake. Rates are the measured ones of
# docs/SCALING.md ("Measured", 2026-09-27/28); they are estimates, not promises.
ESTIMATE_PY=$(cat <<'PY'
import math
import re
import shutil
import sys
from collections import defaultdict

from neon_ducklake.avu.history import SNAPSHOT_BUDGET, rows_per_batch_default
from neon_ducklake.run import (DISK_FACTOR, ENV_WORKSET_BYTES, STAGES, STREAM_STAGES, WORKSET_BYTES_DEFAULT,
                               env_bytes, parse_size)
from neon_ducklake.site.sample import catalog_query, select_months

UPLOAD_FILES_PER_S = 10.0     # net-out, one site at a time: the long pole
APPLY_OBJECTS_PER_S = 60.0    # irods-meta, AVU apply at its configured ceiling
AVUS_PER_FILE = 20            # the full AVU profile
RECORD_ROWS_PER_S = 1100.0    # MESA history staging, under the MESA lock

sample, in_flight, evict, snaps_used, root = (int(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == "1",
                                              int(sys.argv[4]), sys.argv[5])
stream = sys.argv[6] == "1"
workset = parse_size(sys.argv[7]) if sys.argv[7] else env_bytes(ENV_WORKSET_BYTES, WORKSET_BYTES_DEFAULT)
row = re.compile(r"^\s*\d+ (\w{4,5})\s.*\s\d+(?:\.\d+)?  (\S+)\s+\S")
chunk_line = re.compile(r"^\s+chunks: (\d+)/(\d+) deleted")
work, left, last = {}, {}, None
for line in sys.stdin:
    m = row.match(line)
    if m and m.group(2) in (STREAM_STAGES if stream else STAGES):
        last = m.group(1)
        # stream: every stage up to the chunk loop still has bulk work; finalize/cleanup have none
        work[last] = (0 if STREAM_STAGES.index(m.group(2)) <= STREAM_STAGES.index("chunks") else len(STAGES)) \
            if stream else STAGES.index(m.group(2))
        continue
    c = chunk_line.match(line)
    if c and last and int(c.group(2)):
        left[last] = 1.0 - int(c.group(1)) / int(c.group(2))  # share of the site's chunks not deleted yet
if not work:
    print("estimate: nothing to do for the selected sites")
    sys.exit(0)

sites = sorted(work)
rows = catalog_query(
    "SELECT siteCode, productCode, month, isProvisional, isReleaseOnly, coalesce(fileCount, 0) AS fileCount, "
    "coalesce(totalBytes, 0) AS totalBytes FROM {cat}v_site_inventory WHERE siteCode IN ("
    + ", ".join("?" * len(sites)) + ")", sites)
by_site = defaultdict(list)
for r in rows:
    by_site[r["siteCode"]].append(r)

per_batch = rows_per_batch_default()
t = dict(files=0, bytes=0, dl_bytes=0, up=0, apply=0, rows=0, snaps=0)
largest = 0
for s in sites:
    rs = by_site.get(s, [])
    if sample:
        keep = select_months(rs, sample)
        rs = [r for r in rs if r["month"] in keep.get(r["productCode"], ())]
    files, nbytes = sum(int(r["fileCount"]) for r in rs), sum(int(r["totalBytes"]) for r in rs)
    if stream and s in left:
        files, nbytes = int(files * left[s]), int(nbytes * left[s])
    t["files"] += files
    t["bytes"] += nbytes
    if work[s] <= STAGES.index("download"):
        t["dl_bytes"] += nbytes
        largest = max(largest, nbytes)
    if work[s] <= STAGES.index("upload"):
        t["up"] += files
    if work[s] <= STAGES.index("apply"):
        t["apply"] += files
    if work[s] <= STAGES.index("record") and files:
        t["rows"] += files * AVUS_PER_FILE
        t["snaps"] += math.ceil(files * AVUS_PER_FILE / per_batch)


def hours(n: float, rate: float) -> float:
    return n / rate / 3600


def dur(h: float) -> str:
    return f"~{h * 60:.0f} min" if h * 60 < 59.5 else f"~{h:,.1f} h"


def gb(n: float) -> str:
    return f"{n / 1e9:,.1f} GB"


up_h, ap_h, rec_h = hours(t["up"], UPLOAD_FILES_PER_S), hours(t["apply"], APPLY_OBJECTS_PER_S), \
    hours(t["rows"], RECORD_ROWS_PER_S)
hi = up_h + ap_h + rec_h
lo = max(up_h, ap_h, rec_h) if len(sites) > 1 else hi
mode = f"sample:{sample}" if sample else "full"
print(f"estimate for {len(sites)} site(s) with work (mode {mode}; rates from docs/SCALING.md 'Measured'):")
print(f"  size      {t['files']:,} files, {gb(t['bytes'])} (non-AOP basic package, catalog v_site_inventory)")
print(f"  download  {gb(t['dl_bytes'])} still to download")
print(f"  upload    {t['up']:,} files: {dur(up_h)} at ~{UPLOAD_FILES_PER_S:.0f} files/s (the long pole)")
print(f"  apply     {t['apply']:,} objects: {dur(ap_h)} at ~{APPLY_OBJECTS_PER_S:.0f} calls/s")
if stream:
    print(f"  record    ~{t['rows']:,} AVU rows spooled per chunk; the recorder records them in ~{t['snaps']} "
          f"coalesced snapshot(s) ({dur(rec_h)} at ~{RECORD_ROWS_PER_S:,.0f} rows/s under the MESA lock)")
else:
    print(f"  record    ~{t['rows']:,} AVU rows: {dur(rec_h)} at ~{RECORD_ROWS_PER_S:,.0f} rows/s under the MESA lock, "
          f"~{t['snaps']} snapshot(s)")
if len(sites) > 1:
    days = f" ({lo / 24:.1f}-{hi / 24:.1f} days)" if hi >= 24 else ""
    print(f"  wall      {dur(lo)} to {dur(hi)}{days} plus the first site's download, stack,")
    print("            terms and curation; stages of different sites overlap, so the slowest resource class")
    print("            bounds the time from below and the sum from above")
else:
    print(f"  wall      {dur(hi)} for upload, apply and record, plus download, stack, terms and curation")
print("  reference one terrestrial site ~4-7 h, one aquatic site ~2-3 h, all 81 sites ~4-5 days")
if sample:
    print("  note      in sample mode fixed per-stage costs dominate (the 9-site probe took 4-7 min per site for "
          "the local stages)")
free = shutil.disk_usage(root).free
if stream:
    need = DISK_FACTOR * workset
    print(f"  disk      {gb(free)} free; stream mode holds at most {gb(workset)} locally (the working-set cap: raw "
          "files plus site lakes; a single chunk larger than the cap runs alone)")
else:
    need = DISK_FACTOR * (largest * min(in_flight, len(sites)) if evict else t["dl_bytes"])
    print(f"  disk      {gb(free)} free; needs ~{gb(need)} "
          + ("(1.3 x largest site x sites in flight; evict on)" if evict else "(1.3 x all downloads; evict off)"))
print(f"  MESA      {snaps_used} snapshot(s) recorded so far + ~{t['snaps']} planned, budget {SNAPSHOT_BUDGET}")
warn = []
if free < need:
    warn.append("free disk is below the estimate: the disk guard will hold downloads until "
                + ("chunks are deleted" if stream else "evictions free space"))
if snaps_used + t["snaps"] > SNAPSHOT_BUDGET:
    warn.append(f"the planned snapshots would pass the {SNAPSHOT_BUDGET}-snapshot budget: `record` refuses past it "
                "(raise NEON_DUCKLAKE_ROWS_PER_BATCH or compact the MESA history first)")
for w in warn:
    print(f"  WARNING   {w}")
PY
)

show_plan() {
  local plan rc=0
  plan=$(nd run "${run_args[@]}" --dry-run) || rc=$?
  printf '%s\n' "$plan"
  ((rc == 0)) || die "could not plan the sweep (neon-ducklake run --dry-run exited $rc)"
  printf '\n'
  "$PY" -c "$ESTIMATE_PY" "${sample:-0}" "${max_in_flight:-3}" "$evict" "$snaps_used" "$DATA_ROOT" "$stream" \
    "$workset_bytes" <<<"$plan" || warn "no estimate (see the message above); the plan itself is fine"
  printf '\n'
  PLAN_HAS_WORK=0
  if grep -Eq '^ *[0-9]+ [A-Z0-9]{4,5} .*[a-z]+\.\.[a-z]+ \([0-9]+\)$' <<<"$plan"; then PLAN_HAS_WORK=1; fi
}

confirm() {
  if ((yes)); then return 0; fi
  [[ -t 0 ]] || die "no terminal to confirm on: re-run with --yes"
  local ans="" what="the sweep"
  if ((refresh)); then what="the catalog refresh and then the sweep"; fi
  if ((worker)); then what="the worker (it claims sites on the Data Store)"; fi
  read -r -p "Start $what? [y/N] " ans || true
  [[ $ans =~ ^[Yy]([Ee][Ss])?$ ]] || die "not confirmed; nothing was started"
}

launch_detached() {
  local mode=$detach child
  if [[ $mode == auto ]]; then
    if command -v tmux >/dev/null 2>&1; then mode="tmux"; else mode="nohup"; fi
  fi
  child=("$SELF")
  if ((${#sites[@]})); then child+=(--sites "${sites[*]}"); fi
  if ((all)); then child+=(--all); fi
  if [[ -n $habitat ]]; then child+=(--habitat "$habitat"); fi
  if [[ -n $sample ]]; then child+=(--sample "$sample"); fi
  if ((no_evict)); then child+=(--no-evict); fi
  if [[ -n $from_stage ]]; then child+=(--from-stage "$from_stage"); fi
  if [[ -n $max_in_flight ]]; then child+=(--max-in-flight "$max_in_flight"); fi
  if ((refresh)); then child+=(--refresh-catalog); fi
  if ((!stream)); then child+=(--no-stream); fi
  if [[ -n $chunk_bytes ]]; then child+=(--chunk-bytes "$chunk_bytes"); fi
  if [[ -n $workset_bytes ]]; then child+=(--workset-bytes "$workset_bytes"); fi
  if [[ -n $history_mode ]]; then child+=(--history-mode "$history_mode"); fi
  if ((worker)); then child+=(--worker); fi
  if [[ -n $max_sites ]]; then child+=(--max-sites "$max_sites"); fi
  if ((once)); then child+=(--once); fi
  if ((no_recorder)); then child+=(--no-recorder); fi
  child+=(--yes --_child "$LOG")
  if ((${#extra[@]})); then child+=(-- "${extra[@]}"); fi
  forward_env
  if [[ $mode == tmux ]]; then
    local session="neon-sweep-$STAMP"
    if ((worker)); then session="neon-worker-$STAMP"; fi
    tmux new-session -d -s "$session" -c "$ROOT" env "${FWD[@]}" "${child[@]}"
    say "started tmux session $session (attach: tmux attach -t $session; detach again: Ctrl-b d)"
  else
    nohup env "${FWD[@]}" "${child[@]}" >/dev/null 2>&1 </dev/null &
    say "started the sweep under nohup as pid $!"
  fi
  say "log: $LOG"
  say "monitor: scripts/sweep_status.sh   stop cleanly: kill -INT <orchestrator pid> (scripts/sweep_status.sh shows it)"
  if ((worker)); then say "note: $RECORDER_NOTE"; fi
}

# The (non-secret) NEON_DUCKLAKE_* and MESA_HOME settings, as NAME=value words for env(1): a tmux server has
# its own environment.
forward_env() {
  local v
  FWD=()
  while IFS= read -r v; do
    if [[ $v == NEON_DUCKLAKE_* || $v == MESA_HOME || $v == CYVERSE_USERNAME ]]; then FWD+=("$v=${!v}"); fi
  done < <(compgen -e)
}

recorder_running() {
  pgrep -f 'neon[-_]ducklake(\.cli)? recorder' >/dev/null 2>&1 \
    || { command -v tmux >/dev/null 2>&1 && tmux has-session -t "$RECORDER_SESSION" 2>/dev/null; }
}

# Stream runs spool their MESA history to the Data Store; this host owns the MESA catalog, so it also runs the
# central recorder (coalesced snapshots: RECORDER_ARGS). Started once, in its own tmux session, and left
# running after the sweep so the spool keeps draining; `neon-ducklake recorder --flush` records the rest at once.
start_recorder() {
  if ((worker)) || [[ $history_eff != spool ]]; then return 0; fi
  if ((no_recorder)); then
    say "note: --no-recorder: $RECORDER_NOTE"
    return 0
  fi
  if recorder_running; then
    say "MESA history recorder: already running (tmux session $RECORDER_SESSION or another recorder process)"
    return 0
  fi
  if ((dry_run)); then
    say "dry run: would start the MESA history recorder in tmux session $RECORDER_SESSION: neon-ducklake ${RECORDER_ARGS[*]}"
    return 0
  fi
  if ! command -v tmux >/dev/null 2>&1; then
    warn "tmux not found, so the MESA history recorder was not started: $RECORDER_NOTE"
    return 0
  fi
  forward_env
  mkdir -p -- "$DATA_ROOT/logs"
  local rlog="$DATA_ROOT/logs/recorder-$STAMP.log"
  # bash -c SCRIPT NAME LOG CMD...: run CMD with its output appended to LOG (and shown in the tmux window)
  # shellcheck disable=SC2016
  if tmux new-session -d -s "$RECORDER_SESSION" -c "$ROOT" bash -c '"${@:2}" 2>&1 | tee -a -- "$1"' recorder \
      "$rlog" env "${FWD[@]}" "$SECRETS" "$ND" "${RECORDER_ARGS[@]}"; then
    say "started the MESA history recorder in tmux session $RECORDER_SESSION (neon-ducklake ${RECORDER_ARGS[*]}; log ${rlog#"$DATA_ROOT"/}; attach: tmux attach -t $RECORDER_SESSION)"
  else
    warn "could not start the recorder in tmux: $RECORDER_NOTE"
  fi
}

step() {
  local name=$1 rc=0
  shift
  say "== $name"
  "$@" || rc=$?
  ((rc == 0)) || die "$rc" "$name failed (exit $rc). An interrupted harvest resumes with 'catalog fetch' (without --new)."
}

refresh_catalog() {
  say "catalog refresh: fetch --new, seal, build, verify catalog, diff, mirror --what api,lake"
  step "catalog fetch --new" nd_secret catalog fetch --new
  step "catalog seal" nd_secret catalog seal
  step "catalog build" nd_secret catalog build
  step "verify catalog" nd_secret verify catalog
  step "diff (the last two catalog snapshots)" nd diff
  step "mirror --what api,lake" nd_secret mirror --what api,lake
  say "catalog refresh done. 'neon-ducklake tag catalog' writes the Phase-1 AVUs of the new run (not run here)."
}

run_worker() {
  say "== neon-ducklake ${worker_args[*]}"
  say "note: $RECORDER_NOTE"
  local rc=0
  nd_secret "${worker_args[@]}" || rc=$?
  case $rc in
    0) say "worker finished: no free site left (or --max-sites / --once reached)" ;;
    130) say "worker stopped by a signal: its claim was released; re-run to continue" ;;
    2|3|4) say "worker did not start: secrets unavailable (exit $rc, see the message above)" ;;
    *) say "worker finished with failures (exit $rc): see the failed stages below and docs/RUNBOOK.md, sections 6 and 11" ;;
  esac
  say "== status"
  nd status || warn "neon-ducklake status failed"
  exit "$rc"
}

# ---------------------------------------------------------------- main

say "neon-ducklake sweep: code $ROOT, data root $DATA_ROOT${LOG:+, log $LOG}"
if ((worker)); then
  say "worker command: neon-ducklake ${worker_args[*]}"
else
  say "sweep command: neon-ducklake run ${run_args[*]} (MESA history: $history_eff)"
fi

read_status
if ((orch_running)); then
  msg="an orchestrator is already running${orch_pid:+ (pid $orch_pid)}: monitor it with scripts/sweep_status.sh"
  if ((dry_run)); then warn "$msg"; else die "$msg"; fi
fi

if ((dry_run)) && ((worker)); then
  say "note: $RECORDER_NOTE"
  say "dry run: the worker chooses its sites from the Data Store claims at run time (see 'neon-ducklake claims');" \
      "nothing was started"
  exit 0
fi

if ((dry_run)); then
  show_plan
  start_recorder
  if ((refresh)); then
    say "with --refresh-catalog the sweep first runs: catalog fetch --new, seal, build, verify catalog, diff," \
        "mirror --what api,lake (the plan above uses the current catalog)"
  fi
  say "dry run: nothing was started"
  exit 0
fi

if [[ -z $child_log ]] && ((worker)); then
  run_preflight
  confirm
  if [[ -n $detach ]]; then
    launch_detached
    exit 0
  fi
elif [[ -z $child_log ]]; then
  run_preflight
  show_plan
  if ((!PLAN_HAS_WORK && !refresh)); then
    say "nothing to do for the selected sites (use --from-stage to re-run finished stages)"
    exit 0
  fi
  confirm
  if [[ -n $detach ]]; then
    launch_detached
    exit 0
  fi
else
  say "detached run (preflight and confirmation were done by the launching command)"
fi

if ((worker)); then run_worker; fi

if ((refresh)); then
  refresh_catalog
  say "plan after the catalog refresh:"
  show_plan
fi

start_recorder
say "== neon-ducklake run ${run_args[*]}"
rc=0
nd_secret run "${run_args[@]}" || rc=$?
case $rc in
  0) say "sweep finished: every selected site is done (or done / claimed by a worker VM: see the status)" ;;
  130) say "sweep stopped by a signal: re-run the same command to resume" ;;
  2|3|4) say "sweep did not start: secrets unavailable (exit $rc, see the message above)" ;;
  *) say "sweep finished with failures (exit $rc): see the failed stages below and docs/RUNBOOK.md, section 6" ;;
esac
say "== status"
nd status || warn "neon-ducklake status failed"
exit "$rc"
