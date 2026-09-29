#!/usr/bin/env bash
# scripts/sweep_status.sh: one screen on a running or finished sweep or worker (scripts/full_sweep.sh).
#
# Prints the stage table (`neon-ducklake status`), the orchestrator and its running stages and chunk stages,
# chunk progress per site, the local working set (raw files + site lakes) against its cap, the spool backlog
# and whether the MESA history recorder runs, the tail of the latest logs/sweep-*.log, free disk and the MESA
# snapshot budget. Needs no secrets and changes nothing except logs/status.json, which `neon-ducklake status`
# rewrites.
#
#   scripts/sweep_status.sh [-n LINES] [SITE ...]
#   watch -n 60 scripts/sweep_status.sh -n 15      # refresh every minute
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly ROOT
readonly ND="$ROOT/.venv/bin/neon-ducklake"
readonly PY="$ROOT/.venv/bin/python"

usage() {
  cat <<'EOF'
Usage: scripts/sweep_status.sh [-n LINES] [SITE ...]

Show the progress of a sweep or worker: the site x stage table, the orchestrator and its running
stages and chunk stages (with the last line of each log), chunk progress (deleted/total chunks per
site), the local working set (raw files incl. superseded + site lakes) against its cap, the spool
backlog (history batches waiting for `neon-ducklake recorder`) and whether a recorder runs on this
host, the last LINES (default 25) lines of the latest logs/sweep-*.log, free disk, and the MESA
snapshot budget. SITE codes limit the table.
Needs no secrets; safe to run while a sweep is running. Data Store claims of other VMs:
scripts/with-secrets.sh .venv/bin/neon-ducklake claims
EOF
}

lines=25
sites=()
while (($#)); do
  case $1 in
    -h|--help) usage; exit 0 ;;
    -n|--lines)
      [[ ${2:-} =~ ^[0-9]+$ ]] || { echo "sweep_status: $1 needs a number" >&2; exit 2; }
      lines=$2; shift ;;
    -*) echo "sweep_status: unknown option $1 (see --help)" >&2; exit 2 ;;
    *) sites+=("${1^^}") ;;
  esac
  shift
done

cd "$ROOT"
export NEON_DUCKLAKE_ROOT="${NEON_DUCKLAKE_ROOT:-$ROOT}"
DATA_ROOT=$NEON_DUCKLAKE_ROOT
[[ -x $ND && -x $PY ]] || { echo "sweep_status: $ROOT/.venv is missing: run 'uv sync --extra dev'" >&2; exit 1; }

# 1. Stage table (also rewrites logs/status.json, read below).
"$ND" status "${sites[@]}"
echo

# 2. Orchestrator, running stages, failures and the MESA snapshot budget, from logs/status.json.
"$PY" - "$DATA_ROOT" <<'PY' || echo "sweep_status: could not read logs/status.json" >&2
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
rep = json.loads((root / "logs" / "status.json").read_text())


def last_line(rel: str | None) -> str:
    if not rel:
        return ""
    path = root / rel
    try:
        with path.open("rb") as fh:
            fh.seek(0, 2)
            fh.seek(max(0, fh.tell() - 8192))
            tail = fh.read().decode("utf-8", "replace").rstrip().splitlines()
    except OSError:
        return ""
    return tail[-1][:200] if tail else ""


pid = ""
lock = root / "work" / "locks" / "run.lock"
if rep.get("orchestratorRunning") and lock.exists():
    pid = (lock.read_text().split() or [""])[0]
state = "running" + (f" as pid {pid} (stop cleanly: kill -INT {pid})" if pid else "") \
    if rep.get("orchestratorRunning") else "not running"
print(f"orchestrator: {state}")

running, failed, finished, chunk_rows = [], 0, 0, []
for site, v in rep.get("sites", {}).items():
    stages = v.get("stages", {})
    for stage, info in stages.items():
        if info.get("status") == "running" and stage != "chunks":  # the loop itself: its chunk stages show below
            running.append((site, stage, info))
        elif info.get("status") == "failed":
            failed += 1
    for info in v.get("runningChunks", []):
        running.append((site, f"chunk {info.get('chunk_id')} {info.get('stage')}", info))
    failed += len(v.get("failedChunks", []))
    done_verify = (stages.get("verify") or {}).get("status") == "ok"
    done_evict = (stages.get("evict") or {}).get("status") in ("ok", "skipped", None)  # None: never planned (sample)
    done_stream = (stages.get("cleanup") or {}).get("status") == "ok"
    if (done_verify and done_evict) or done_stream:
        finished += 1
    c = v.get("chunks") or {}
    if c.get("total"):
        chunk_rows.append((site, c, int(v.get("localBytes") or 0)))
print(f"sites: {len(rep.get('sites', {}))} in the journal, {finished} finished (stream: cleanup ok; whole-site: "
      f"verify ok, nothing left to evict); {failed} failed stage(s)")
live = bool(rep.get("orchestratorRunning"))
if running and not live:
    print("  (no orchestrator is running: these stages were interrupted; the next run marks them failed and "
          "retries them)")
for site, stage, info in running:
    print(f"  {'running' if live else 'stale  '}  {site} {stage} (attempt {info.get('attempt')}, since "
          f"{(info.get('started_at') or '')[:19]}Z)  {info.get('log_path') or ''}")
    line = last_line(info.get("log_path"))
    if line:
        print(f"           {line}")
if chunk_rows:
    print("chunks (deleted/total; in progress; failed; raw GB on local disk):")
    for site, c, held in chunk_rows:
        print(f"  {site:5} {c.get('done', 0):>4}/{c.get('total', 0):<4} {c.get('inProgress', 0):>3} in progress  "
              f"{c.get('failed', 0):>3} failed  {held / 1e9:7.1f} GB")

a = rep.get("allScopes") or {}
used, budget = int(a.get("snapshots") or 0), int(a.get("snapshotBudget") or 900)
print(f"MESA history: {int(a.get('avusRecorded') or 0):,} AVU rows recorded in {used} snapshot(s); "
      f"budget {budget} ({used / budget:.0%} used)")
if used >= 0.8 * budget:
    print("  WARNING: over 80% of the snapshot budget is used; `record` refuses to pass it (docs/SCALING.md)")
t = rep.get("totals") or {}
print(f"files: {int(t.get('files') or 0):,} journaled, {int(t.get('uploaded') or 0):,} in iRODS, "
      f"{int(t.get('evictedFiles') or 0):,} evicted ({int(t.get('evictedBytes') or 0) / 1e9:,.1f} GB freed)")
held = int(rep.get("localBytesAllSites") or 0)
try:
    from neon_ducklake.run import ENV_WORKSET_BYTES, WORKSET_BYTES_DEFAULT, env_bytes

    cap = env_bytes(ENV_WORKSET_BYTES, WORKSET_BYTES_DEFAULT)
except (ImportError, SystemExit):
    cap = 50 * 10**9
ws = rep.get("worksetBytes")
cap = int(rep.get("worksetCap") or cap)
if ws is not None:
    print(f"local working set: {int(ws) / 1e9:,.1f} GB on disk (raw files incl. superseded + site lakes; "
          f"{held / 1e9:,.1f} GB of it raw files in the journal); cap {cap / 1e9:,.1f} GB (NEON_DUCKLAKE_WORKSET_BYTES)")
    if int(ws) > cap:
        print("  over the cap: new chunk downloads wait; raw files of earlier whole-site runs are freed by "
              "`neon-ducklake site evict SITE`")
else:
    print(f"local raw data: {held / 1e9:,.1f} GB on disk; working-set cap {cap / 1e9:,.1f} GB "
          f"(NEON_DUCKLAKE_WORKSET_BYTES; stream chunks only)")
sp = rep.get("spool") or {}
if sp:
    msg = (f"spool backlog: {int(sp.get('batches') or 0)} history batch(es), {int(sp.get('rows') or 0):,} row(s) "
           "spooled to the Data Store")
    if sp.get("spooling"):
        msg += f"; {int(sp['spooling'])} interrupted upload(s) (the next history stage completes them)"
    print(msg)
    if sp.get("batches"):
        print("  the recorder ingests them into MESA in coalesced snapshots (scripts/full_sweep.sh starts it in "
              "tmux session neon-recorder); the exact backlog on the Data Store: scripts/with-secrets.sh "
              ".venv/bin/neon-ducklake recorder --dry-run")
PY
if pgrep -f 'neon[-_]ducklake(\.cli)? recorder' >/dev/null 2>&1; then
  echo "MESA history recorder: running on this host (pid $(pgrep -f 'neon[-_]ducklake(\.cli)? recorder' | head -n 1))"
else
  echo "MESA history recorder: not running on this host (stream runs spool their history; start it with"
  echo "  scripts/with-secrets.sh .venv/bin/neon-ducklake recorder --watch --interval 300 --min-rows 200000 --max-rows 250000 --max-age-h 12)"
fi
echo

# 3. Tail of the latest sweep log (names sort by their UTC timestamp).
shopt -s nullglob
logs=("$DATA_ROOT"/logs/sweep-*.log)
shopt -u nullglob
if ((${#logs[@]})); then
  latest=${logs[${#logs[@]}-1]}
  echo "latest sweep log: ${latest#"$DATA_ROOT"/} (last $lines lines)"
  tail -n "$lines" -- "$latest"
else
  echo "no sweep log yet (logs/sweep-*.log is written by scripts/full_sweep.sh)"
fi
if command -v tmux >/dev/null 2>&1; then
  sessions=$(tmux list-sessions -F '#{session_name}' 2>/dev/null | grep -E '^neon-(sweep-|worker-|recorder)' || true)
  if [[ -n $sessions ]]; then echo "tmux: ${sessions//$'\n'/, } (tmux attach -t <name>)"; fi
fi
echo

# 4. Free disk where the raw files land.
df -h -- "$DATA_ROOT"
