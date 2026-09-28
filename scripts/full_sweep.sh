#!/usr/bin/env bash
# scripts/full_sweep.sh: run the neon-ducklake Phase-2 sweep over every NEON site (or a selection), safely.
#
#   scripts/full_sweep.sh --help
#   scripts/full_sweep.sh --dry-run          # plan + size/time estimate only; needs no secrets
#   scripts/full_sweep.sh --detach           # preflight, plan, confirm, then run in a tmux session
#
# Steps: checks (repo root, .venv, no other orchestrator) -> preflight -> plan and estimate -> confirmation
# -> [--refresh-catalog] -> `neon-ducklake run` -> `neon-ducklake status`. Every command that talks to NEON,
# iRODS or MESA runs through scripts/with-secrets.sh, which reads the secrets from the login keyring inside
# that command. This script never reads, prints or passes a secret.
set -euo pipefail

readonly STAGES="plan list download stack terms cards curate validate upload apply record verify evict"

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly ROOT
readonly SELF="$ROOT/scripts/full_sweep.sh"
readonly ND="$ROOT/.venv/bin/neon-ducklake"
readonly PY="$ROOT/.venv/bin/python"
readonly SECRETS="$ROOT/scripts/with-secrets.sh"

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
  --sample N             probe mode: only the N most recent months per product (always the
                         latest released and latest PROVISIONAL month); never evicts
  --no-evict             keep raw files on disk (the evict stage is skipped)
  --from-stage STAGE     re-run from STAGE even when later stages are ok; after a new NEON
                         release use --refresh-catalog --from-stage list
  --max-in-flight N      sites between their first and last stage at once (default 3)
  --refresh-catalog      first harvest a new catalog run: catalog fetch --new, catalog seal,
                         catalog build, verify catalog, diff, mirror --what api,lake (~1.5-2.5 h)
  -- ARGS                pass ARGS on to `neon-ducklake run` (e.g. -- --order size --threads 12)

Execution:
  --dry-run              print the plan and the size/time estimate, then exit; needs no secrets,
                         runs no preflight and starts nothing
  -y, --yes              do not ask for confirmation
  --detach[=tmux|nohup]  after preflight and confirmation, run in the background: in a tmux
                         session (neon-sweep-<UTC>) when tmux is available, else under nohup
  -h, --help             show this help

Each run writes logs/sweep-<UTC>.log; stage logs are in logs/run/<SITE>/<stage>.log.
Monitor with scripts/sweep_status.sh. Stop cleanly with one Ctrl-C, or `kill -INT` the orchestrator:
running stages finish, a second signal terminates them. Re-run the same command to resume.

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
dry_run=0 yes=0 detach="" child_log="" extra=()

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
if [[ -n $from_stage && " $STAGES " != *" $from_stage "* ]]; then
  usage_error "unknown stage '$from_stage'; stages: $STAGES"
fi
if ((dry_run)) && [[ -n $detach ]]; then usage_error "--dry-run and --detach exclude each other"; fi
if [[ $detach == tmux ]] && ! command -v tmux >/dev/null 2>&1; then usage_error "--detach=tmux: tmux not found"; fi

run_args=()
if ((${#sites[@]})); then run_args+=("${sites[@]}"); elif [[ -z $habitat ]]; then run_args+=(--all); fi
if [[ -n $habitat ]]; then run_args+=(--habitat "$habitat"); fi
if [[ -n $sample ]]; then run_args+=(--sample "$sample"); fi
if ((no_evict)); then run_args+=(--no-evict); fi
if [[ -n $from_stage ]]; then run_args+=(--from-stage "$from_stage"); fi
if [[ -n $max_in_flight ]]; then run_args+=(--max-sites-in-flight "$max_in_flight"); fi
run_args+=("${extra[@]}")
evict=1
if [[ -n $sample ]] || ((no_evict)); then evict=0; fi

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
from neon_ducklake.run import DISK_FACTOR, STAGES
from neon_ducklake.site.sample import catalog_query, select_months

UPLOAD_FILES_PER_S = 10.0     # net-out, one site at a time: the long pole
APPLY_OBJECTS_PER_S = 60.0    # irods-meta, AVU apply at its configured ceiling
AVUS_PER_FILE = 20            # the full AVU profile
RECORD_ROWS_PER_S = 1100.0    # MESA history staging, under the MESA lock

sample, in_flight, evict, snaps_used, root = (int(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == "1",
                                              int(sys.argv[4]), sys.argv[5])
row = re.compile(r"^\s*\d+ (\w{4,5})\s.*\s\d+(?:\.\d+)?  (\S+)\s+\S")
work = {}
for line in sys.stdin:
    m = row.match(line)
    if m and m.group(2) in STAGES:
        work[m.group(1)] = STAGES.index(m.group(2))
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
need = DISK_FACTOR * (largest * min(in_flight, len(sites)) if evict else t["dl_bytes"])
print(f"  disk      {gb(free)} free; needs ~{gb(need)} "
      + ("(1.3 x largest site x sites in flight; evict on)" if evict else "(1.3 x all downloads; evict off)"))
print(f"  MESA      {snaps_used} snapshot(s) recorded so far + ~{t['snaps']} planned, budget {SNAPSHOT_BUDGET}")
warn = []
if free < need:
    warn.append("free disk is below the estimate: the disk guard will hold downloads until evictions free space")
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
  "$PY" -c "$ESTIMATE_PY" "${sample:-0}" "${max_in_flight:-3}" "$evict" "$snaps_used" "$DATA_ROOT" <<<"$plan" \
    || warn "no estimate (see the message above); the plan itself is fine"
  printf '\n'
  PLAN_HAS_WORK=0
  if grep -Eq '^ *[0-9]+ [A-Z0-9]{4,5} .*[a-z]+\.\.[a-z]+ \([0-9]+\)$' <<<"$plan"; then PLAN_HAS_WORK=1; fi
}

confirm() {
  if ((yes)); then return 0; fi
  [[ -t 0 ]] || die "no terminal to confirm on: re-run with --yes"
  local ans="" what="the sweep"
  if ((refresh)); then what="the catalog refresh and then the sweep"; fi
  read -r -p "Start $what? [y/N] " ans || true
  [[ $ans =~ ^[Yy]([Ee][Ss])?$ ]] || die "not confirmed; nothing was started"
}

launch_detached() {
  local mode=$detach child fwd=() v
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
  child+=(--yes --_child "$LOG")
  if ((${#extra[@]})); then child+=(-- "${extra[@]}"); fi
  # Forward the (non-secret) NEON_DUCKLAKE_* and MESA_HOME settings; a tmux server has its own environment.
  while IFS= read -r v; do
    if [[ $v == NEON_DUCKLAKE_* || $v == MESA_HOME || $v == CYVERSE_USERNAME ]]; then fwd+=("$v=${!v}"); fi
  done < <(compgen -e)
  if [[ $mode == tmux ]]; then
    local session="neon-sweep-$STAMP"
    tmux new-session -d -s "$session" -c "$ROOT" env "${fwd[@]}" "${child[@]}"
    say "started tmux session $session (attach: tmux attach -t $session; detach again: Ctrl-b d)"
  else
    nohup env "${fwd[@]}" "${child[@]}" >/dev/null 2>&1 </dev/null &
    say "started the sweep under nohup as pid $!"
  fi
  say "log: $LOG"
  say "monitor: scripts/sweep_status.sh   stop cleanly: kill -INT <orchestrator pid> (scripts/sweep_status.sh shows it)"
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

# ---------------------------------------------------------------- main

say "neon-ducklake sweep: code $ROOT, data root $DATA_ROOT${LOG:+, log $LOG}"
say "sweep command: neon-ducklake run ${run_args[*]}"

read_status
if ((orch_running)); then
  msg="an orchestrator is already running${orch_pid:+ (pid $orch_pid)}: monitor it with scripts/sweep_status.sh"
  if ((dry_run)); then warn "$msg"; else die "$msg"; fi
fi

if ((dry_run)); then
  show_plan
  if ((refresh)); then
    say "with --refresh-catalog the sweep first runs: catalog fetch --new, seal, build, verify catalog, diff," \
        "mirror --what api,lake (the plan above uses the current catalog)"
  fi
  say "dry run: nothing was started"
  exit 0
fi

if [[ -z $child_log ]]; then
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

if ((refresh)); then
  refresh_catalog
  say "plan after the catalog refresh:"
  show_plan
fi

say "== neon-ducklake run ${run_args[*]}"
rc=0
nd_secret run "${run_args[@]}" || rc=$?
case $rc in
  0) say "sweep finished: every selected site is done" ;;
  130) say "sweep stopped by a signal: re-run the same command to resume" ;;
  2|3|4) say "sweep did not start: secrets unavailable (exit $rc, see the message above)" ;;
  *) say "sweep finished with failures (exit $rc): see the failed stages below and docs/RUNBOOK.md, section 6" ;;
esac
say "== status"
nd status || warn "neon-ducklake status failed"
exit "$rc"
