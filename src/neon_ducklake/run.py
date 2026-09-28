"""`neon-ducklake run` and `neon-ducklake status`: the multi-site orchestrator (docs/SCALING.md, Orchestrator).

Every stage of every site runs as a SUBPROCESS of this interpreter (``python -m neon_ducklake.cli ...``),
inherits the environment (run the orchestrator through ``scripts/with-secrets.sh``) and logs to
``logs/run/<SITE>/<stage>.log``. Children start in their own session, so a terminal Ctrl-C reaches only
the orchestrator: the first SIGINT/SIGTERM stops scheduling and lets running stages finish, a second one
terminates the children (SIGTERM, then SIGKILL after a grace period). Every stage is idempotent.

Stages (in order) and resource classes. Each class is a semaphore over sites, so site B downloads while
site A uploads:

    plan local | list, download net-in (2) | stack, terms, cards, validate cpu (1) | curate llm (1)
    upload net-out (1) | apply irods-meta (1) | record mesa (1) | verify, evict local (unlimited)

``apply`` runs ``site build-avus`` then ``tag --apply-only``; ``record`` is ``tag --record-only``.

State: ``work/state.sqlite`` table ``site_runs(site, stage, status, attempt, started_at, finished_at, rc,
log_path, mode, info)``; status is pending | running | ok | failed | skipped. A site resumes at its first
stage not ``ok`` (in a compatible mode: a sample run never satisfies a full run); a failed stage stops that
site (the others continue) and is retried by the next invocation.

Guards:
- disk: a download is dispatched only when free disk >= 1.3 x the bytes it will write plus the
  reservations of downloads in flight; otherwise it waits for evictions (and fails if nothing is running
  that could free space);
- evicted inputs: the download stage restores evicted files of products that changed, and the stack stage
  refuses to run while a product would be re-stacked from a partial input set (see site/evict.py);
- MESA: ``record`` fails fast, naming the process, while another process holds the MESA catalog;
- curation: ``validate`` is ``ok`` without running when the site has no proposals (all products reuse
  ``curation/generic`` results or are external).
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import env, state

STAGES: tuple[str, ...] = ("plan", "list", "download", "stack", "terms", "cards", "curate", "validate", "upload",
                           "apply", "record", "verify", "evict")
STAGE_CLASS: dict[str, str] = {
    "plan": "local", "list": "net-in", "download": "net-in", "stack": "cpu", "terms": "cpu", "cards": "cpu",
    "curate": "llm", "validate": "cpu", "upload": "net-out", "apply": "irods-meta", "record": "mesa",
    "verify": "local", "evict": "local",
}
CLASS_LIMITS: dict[str, int | None] = {"net-in": 2, "cpu": 1, "llm": 1, "net-out": 1, "irods-meta": 1, "mesa": 1,
                                       "local": None}
STATUSES = ("pending", "running", "ok", "failed", "skipped")
SYMBOLS = {"ok": "+", "failed": "X", "running": ">", "pending": ".", "skipped": "-"}
SHORT = {"plan": "plan", "list": "list", "download": "dl", "stack": "stk", "terms": "trm", "cards": "crd",
         "curate": "cur", "validate": "val", "upload": "up", "apply": "app", "record": "rec", "verify": "ver",
         "evict": "evc"}
DISK_FACTOR = 1.3
SNAPSHOT_BUDGET = 900
KILL_GRACE_S = 15.0
AQUATIC = ("Aquatic Wadeable Stream", "Aquatic Lake", "Aquatic Non-wadeable Stream")
HABITATS: dict[str, tuple[str, ...]] = {"terrestrial": ("Terrestrial",), "aquatic": AQUATIC,
                                        **{h.lower(): (h,) for h in AQUATIC}}
ORDERS = ("aquatic-first", "size", "name", "given")

SCHEMA = """
CREATE TABLE IF NOT EXISTS site_runs (
  site TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
  attempt INTEGER NOT NULL DEFAULT 0, started_at TEXT, finished_at TEXT, rc INTEGER, log_path TEXT,
  mode TEXT, info TEXT, PRIMARY KEY (site, stage)
);
"""


def say(msg: str) -> None:
    print(f"{env.utcnow().strftime('%H:%M:%S')} {msg}", flush=True)


def _gb(n: float | None) -> str:
    return f"{(n or 0) / 1e9:.1f}"


# ---------------------------------------------------------------- config

@dataclass
class RunConfig:
    stages: tuple[str, ...] = STAGES
    from_stage: str | None = None
    sample: int | None = None
    evict: bool = True
    max_in_flight: int = 3
    restack: bool = False
    threads: int | None = None
    poll_s: float = 5.0
    disk_factor: float = DISK_FACTOR

    @property
    def mode(self) -> str:
        return f"sample:{self.sample}" if self.sample else "full"

    def selected(self) -> list[str]:
        """Stages this run may execute (evict dropped for --sample / --no-evict)."""
        return [s for s in STAGES if s in self.stages and not (s == "evict" and self.skips_evict)]

    @property
    def skips_evict(self) -> bool:
        return bool(self.sample) or not self.evict


def make_config(*, stages: str | Iterable[str] | None = None, from_stage: str | None = None,
                sample: int | None = None, evict: bool = True, max_in_flight: int = 3, restack: bool = False,
                threads: int | None = None, poll_s: float = 5.0) -> RunConfig:
    """Validated RunConfig (SystemExit with a clear message on a bad combination)."""
    if isinstance(stages, str):
        stages = [s.strip() for s in stages.split(",") if s.strip()]
    chosen = tuple(stages) if stages else STAGES
    bad = [s for s in chosen if s not in STAGES]
    if bad:
        raise SystemExit(f"unknown stage(s) {', '.join(bad)}; stages are: {', '.join(STAGES)}")
    if sample is not None and sample < 1:
        raise SystemExit("--sample must be >= 1")
    if max_in_flight < 1:
        raise SystemExit("--max-sites-in-flight must be >= 1")
    cfg = RunConfig(stages=tuple(s for s in STAGES if s in chosen), from_stage=from_stage, sample=sample,
                    evict=evict, max_in_flight=max_in_flight, restack=restack, threads=threads, poll_s=poll_s)
    sel = cfg.selected()
    if restack:
        if "download" not in sel or "stack" not in sel:
            raise SystemExit("--restack needs the download and stack stages (it re-downloads evicted inputs)")
        cfg.from_stage = from_stage or "download"
        if STAGES.index(cfg.from_stage) > STAGES.index("download"):
            raise SystemExit("--restack re-downloads evicted inputs: --from-stage must be download or earlier")
    if cfg.from_stage is not None and cfg.from_stage not in sel:
        raise SystemExit(f"--from-stage {cfg.from_stage} is not among the selected stages ({', '.join(sel)})")
    return cfg


def covers(have: str | None, want: str) -> bool:
    """Does a stage done in mode ``have`` satisfy mode ``want``? full covers all; sample:M covers sample:N<=M."""
    if have == "full" or have == want:
        return True
    if have and want and have.startswith("sample:") and want.startswith("sample:"):
        return int(have.split(":", 1)[1]) >= int(want.split(":", 1)[1])
    return False


# ---------------------------------------------------------------- persistence

class Store:
    """``site_runs`` in work/state.sqlite (one connection, serialised by a lock)."""

    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        self.lock = threading.Lock()
        con.executescript(SCHEMA)

    def rows(self, site: str | None = None) -> dict[str, dict[str, dict[str, Any]]]:
        with self.lock:
            q = "SELECT * FROM site_runs" + (" WHERE site=?" if site else "") + " ORDER BY site"
            out: dict[str, dict[str, dict[str, Any]]] = {}
            for r in self.con.execute(q, (site,) if site else ()):
                out.setdefault(r["site"], {})[r["stage"]] = dict(r)
            return out

    def _upsert(self, site: str, stage: str, **cols: Any) -> None:
        names = ", ".join(cols)
        marks = ", ".join("?" * len(cols))
        sets = ", ".join(f"{k}=excluded.{k}" for k in cols)
        with self.lock:
            self.con.execute(f"INSERT INTO site_runs(site, stage, {names}) VALUES (?, ?, {marks}) "
                             f"ON CONFLICT(site, stage) DO UPDATE SET {sets}", (site, stage, *cols.values()))

    def set_pending(self, site: str, stages: Iterable[str]) -> None:
        for s in stages:
            self._upsert(site, s, status="pending", rc=None, finished_at=None)

    def start(self, site: str, stage: str, log_path: str, mode: str) -> int:
        with self.lock:
            row = self.con.execute("SELECT attempt FROM site_runs WHERE site=? AND stage=?", (site, stage)).fetchone()
        attempt = (row["attempt"] if row else 0) + 1
        self._upsert(site, stage, status="running", attempt=attempt, started_at=env.utcnow().isoformat(),
                     finished_at=None, rc=None, log_path=log_path, mode=mode, info=None)
        return attempt

    def note_pid(self, site: str, stage: str, pid: int) -> None:
        with self.lock:
            row = self.con.execute("SELECT info FROM site_runs WHERE site=? AND stage=?", (site, stage)).fetchone()
        info = _json(row["info"] if row else None)
        info.setdefault("pids", []).append(pid)
        self._upsert(site, stage, info=json.dumps(info))

    def finish(self, site: str, stage: str, status: str, rc: int | None, info: str = "") -> None:
        with self.lock:
            row = self.con.execute("SELECT info FROM site_runs WHERE site=? AND stage=?", (site, stage)).fetchone()
        prev = _json(row["info"] if row else None)
        prev.pop("pids", None)
        if info:
            prev["message"] = info
        self._upsert(site, stage, status=status, rc=rc, finished_at=env.utcnow().isoformat(),
                     info=json.dumps(prev) if prev else None)

    def skip(self, site: str, stage: str, info: str) -> None:
        self._upsert(site, stage, status="skipped", rc=None, finished_at=env.utcnow().isoformat(),
                     info=json.dumps({"message": info}))

    def recover_stale(self) -> list[tuple[str, str]]:
        """'running' rows left by a dead orchestrator -> failed; refuse while one of their children lives."""
        with self.lock:
            rows = [dict(r) for r in self.con.execute("SELECT * FROM site_runs WHERE status='running'")]
        out = []
        for r in rows:
            live = [p for p in _json(r["info"]).get("pids", []) if _alive(int(p))]
            if live:
                raise SystemExit(f"stage {r['stage']} of {r['site']} is still running as pid {live[0]} (left by a "
                                 f"previous orchestrator): wait for it or kill it, then re-run")
            self.finish(r["site"], r["stage"], "failed", None, "interrupted: the orchestrator exited mid-stage")
            out.append((r["site"], r["stage"]))
        return out


def _json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        v = json.loads(value)
    except (TypeError, ValueError):
        return {"message": str(value)}
    return v if isinstance(v, dict) else {"message": str(v)}


def _alive(pid: int) -> bool:
    try:
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False
    return b"neon_ducklake" in cmd or b"neon-ducklake" in cmd


def log_path(site: str, stage: str) -> Path:
    return env.ROOT / "logs" / "run" / site / f"{stage}.log"


def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(env.ROOT))
    except ValueError:
        return str(p)


# ---------------------------------------------------------------- stage commands

def stage_commands(site: str, stage: str, *, sample: int | None = None, threads: int | None = None,
                   restack: bool = False, python: str | None = None) -> list[list[str]]:
    """argv list(s) of one stage (``apply`` is two commands)."""
    base = [python or sys.executable, "-m", "neon_ducklake.cli"]
    thr = ["--threads", str(threads)] if threads else []
    smp = ["--sample", str(sample)] if sample else []
    if stage == "plan":
        return [base + ["site", "plan", site]]
    if stage in ("list", "download"):
        return [base + ["site", stage, site, *smp, *thr]]
    if stage == "stack":
        return [base + ["site", "stack", site, *(["--force"] if restack else [])]]
    if stage in ("terms", "cards", "curate", "validate"):
        return [base + ["site", stage, site]]
    if stage == "upload":
        return [base + ["site", "upload", site, *thr]]
    if stage == "apply":
        return [base + ["site", "build-avus", site], base + ["tag", site, "--apply-only"]]
    if stage == "record":
        return [base + ["tag", site, "--record-only"]]
    if stage == "verify":
        return [base + ["verify", site]]
    if stage == "evict":
        return [base + ["site", "evict", site]]
    raise ValueError(f"unknown stage {stage}")


class SubprocessRunner:
    """Runs a stage's command(s) in their own session; output appended to logs/run/<SITE>/<stage>.log."""

    def __init__(self, cfg: RunConfig, *, on_spawn: Callable[[str, str, int], None] | None = None,
                 grace_s: float = KILL_GRACE_S) -> None:
        self.cfg = cfg
        self.on_spawn = on_spawn
        self.grace_s = grace_s
        self.killing = False
        self._procs: dict[tuple[str, str], subprocess.Popen] = {}
        self._lock = threading.Lock()

    def __call__(self, site: str, stage: str) -> int:
        path = log_path(site, stage)
        path.parent.mkdir(parents=True, exist_ok=True)
        child_env = dict(os.environ)
        child_env["PYTHONUNBUFFERED"] = "1"
        rc = 0
        with path.open("a", encoding="utf-8") as fh:
            for argv in stage_commands(site, stage, sample=self.cfg.sample, threads=self.cfg.threads,
                                       restack=self.cfg.restack):
                if self.killing:
                    return -int(signal.SIGTERM)
                shown = " ".join(["neon-ducklake", *argv[3:]]) if argv[1:3] == ["-m", "neon_ducklake.cli"] \
                    else " ".join(argv)
                fh.write(f"\n=== {env.utcnow().isoformat()} $ {shown}\n")
                fh.flush()
                t0 = time.monotonic()
                p = subprocess.Popen(argv, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                     cwd=str(env.ROOT), env=child_env, start_new_session=True)
                with self._lock:
                    self._procs[(site, stage)] = p
                if self.on_spawn:
                    self.on_spawn(site, stage, p.pid)
                try:
                    rc = p.wait()
                finally:
                    with self._lock:
                        self._procs.pop((site, stage), None)
                fh.write(f"=== exit {rc} after {time.monotonic() - t0:.1f}s\n")
                fh.flush()
                if rc != 0:
                    return rc
        return rc

    def kill(self) -> None:
        """Terminate every running child process group (SIGTERM now, SIGKILL after the grace period)."""
        self.killing = True
        with self._lock:
            procs = list(self._procs.values())
        for p in procs:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGTERM)

        def finish() -> None:
            deadline = time.monotonic() + self.grace_s
            for p in procs:
                with contextlib.suppress(subprocess.TimeoutExpired):
                    p.wait(timeout=max(deadline - time.monotonic(), 0.01))
                if p.poll() is None:
                    with contextlib.suppress(ProcessLookupError, PermissionError):
                        os.killpg(p.pid, signal.SIGKILL)

        threading.Thread(target=finish, name="run-kill", daemon=True).start()


# ---------------------------------------------------------------- guards

@dataclass
class Outcome:
    status: str  # ok | failed
    rc: int | None = None
    info: str = ""


class DiskGuard:
    """Free disk >= factor x (bytes the download will write) + reservations of downloads in flight."""

    def __init__(self, need_bytes: Callable[[str], int], free_bytes: Callable[[], int],
                 factor: float = DISK_FACTOR) -> None:
        self.need_bytes, self.free_bytes, self.factor = need_bytes, free_bytes, factor
        self.reserved: dict[str, int] = {}
        self._need: dict[str, int] = {}

    def try_reserve(self, site: str) -> tuple[bool, str]:
        if site not in self._need:
            self._need[site] = int(self.factor * max(int(self.need_bytes(site) or 0), 0))
        need = self._need[site]
        others = sum(v for k, v in self.reserved.items() if k != site)
        free = int(self.free_bytes())
        if need == 0 or free >= need + others:
            self.reserved[site] = need
            return True, ""
        return False, (f"disk guard: needs {_gb(need)} GB ({self.factor} x download) + {_gb(others)} GB reserved "
                       f"by downloads in flight, {_gb(free)} GB free")

    def release(self, site: str) -> None:
        self.reserved.pop(site, None)
        self._need.pop(site, None)


def free_disk() -> int:
    import shutil

    return shutil.disk_usage(env.ROOT).free


def download_bytes(site: str) -> int:
    """Bytes the site's next download writes (journal; plan.json before the first listing)."""
    from .site import evict

    con = state.connect(state.DB_PATH)
    try:
        n_rows = con.execute("SELECT count(*) FROM site_files WHERE site=?", (site,)).fetchone()[0]
        if n_rows:
            return evict.pending_bytes(con, site)
    finally:
        con.close()
    plan = env.ROOT / "sites" / site / "reports" / "plan.json"
    try:
        return int(json.loads(plan.read_text("utf-8"))["totals"]["bytes"])
    except (OSError, ValueError, KeyError, TypeError):
        return 0


def _note(site: str, stage: str, msg: str) -> None:
    p = log_path(site, stage)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(f"\n=== {env.utcnow().isoformat()} orchestrator: {msg}\n")


def make_prep(cfg: RunConfig) -> Callable[[str, str], Outcome | None]:
    """Per-stage pre-steps and short-circuits run in the worker thread before the subprocess."""

    def prep(site: str, stage: str) -> Outcome | None:
        from .site import evict

        if stage in ("download", "stack"):
            con = state.connect(state.DB_PATH)
            try:
                evict.ensure_schema(con)
                if stage == "download":
                    products = None if cfg.restack else sorted(evict.restore_candidates(con, site))
                    n, b = evict.mark_restore(con, site, products)
                    if n:
                        what = "all products" if products is None else ", ".join(products)
                        _note(site, stage, f"restoring {n} evicted file(s) ({_gb(b)} GB) of {what}")
                    return None
                st = evict.input_state(con, site)
                if cfg.restack and st["missing"]:
                    return Outcome("failed", None, f"--restack needs every input on disk: {st['missing']} downloaded "
                                                   "file(s) are missing (re-run from download)")
                if not cfg.restack:
                    cands = evict.restore_candidates(con, site)
                    if cands:
                        names = ", ".join(list(cands)[:5]) + (" ..." if len(cands) > 5 else "")
                        return Outcome("failed", None, f"{len(cands)} product(s) would be re-stacked from partly "
                                                       f"evicted inputs ({names}): re-run from download (it restores "
                                                       "them) or use --restack")
                if st["local"] == 0 and st["missing"]:
                    return Outcome("ok", 0, f"nothing to stack on disk: {st['missing']} input(s) evicted, lake "
                                            "unchanged")
                return None
            finally:
                con.close()
        if stage == "validate":
            from .site.validate import load_replicates

            if not load_replicates(site):
                return Outcome("ok", 0, "no site proposals: every product reuses curation/generic or is external")
        if stage == "record":
            holders = env.catalog_lock_holders()
            if holders:
                who = "; ".join(f"pid {h['pid']} ({h['cmd'][:80].strip()})" for h in holders[:3])
                return Outcome("failed", None, f"MESA catalog is open by another process: {who}")
        return None

    return prep


# ---------------------------------------------------------------- scheduler

@dataclass
class SitePlan:
    site: str
    stages: list[str]
    skip: list[str] = field(default_factory=list)  # marked 'skipped' when the site completes
    habitat: str | None = None
    bytes: int = 0


def plan_site(site: str, rows: dict[str, dict[str, Any]], cfg: RunConfig) -> SitePlan:
    """Stages to run: from --from-stage, else from the first selected stage not ok in a covering mode."""
    sel = cfg.selected()
    skip = ["evict"] if ("evict" in cfg.stages and cfg.skips_evict) else []
    if cfg.from_stage:
        return SitePlan(site, sel[sel.index(cfg.from_stage):], skip)
    for i, s in enumerate(sel):
        r = rows.get(s)
        if not (r and r.get("status") == "ok" and covers(r.get("mode"), cfg.mode)):
            return SitePlan(site, sel[i:], skip)
    return SitePlan(site, [], skip)


class Scheduler:
    """Dispatches site stages in priority order under the resource-class limits (one stage per site)."""

    def __init__(self, plans: list[SitePlan], runner: Callable[[str, str], int], store: Store | None = None, *,
                 mode: str = "full", limits: dict[str, int | None] | None = None, max_in_flight: int = 3,
                 disk_guard: DiskGuard | None = None, prep: Callable[[str, str], Outcome | None] | None = None,
                 poll_s: float = 5.0, out: Callable[[str], None] = say) -> None:
        self.plans, self.runner, self.store, self.mode = plans, runner, store, mode
        self.limits = dict(CLASS_LIMITS if limits is None else limits)
        self.max_in_flight = max_in_flight
        self.disk_guard, self.prep, self.poll_s, self.out = disk_guard, prep, poll_s, out
        self.stopping = False
        self.signals = 0
        self.events: list[tuple[str, str, str]] = []  # (site, stage, start|ok|failed) for tests/diagnostics

    # ------------------------------------------------------------ signals
    def request_stop(self) -> None:
        if not self.stopping:
            self.stopping = True
            self.out("stop requested: no new stages; running stages finish (signal again to kill them)")

    def on_signal(self, signum: int, frame: Any) -> None:
        self.signals += 1
        if self.signals == 1:
            self.request_stop()
            return
        self.out("second signal: terminating running stages")
        kill = getattr(self.runner, "kill", None)
        if callable(kill):
            kill()

    # ------------------------------------------------------------ run
    def _work(self, site: str, stage: str) -> Outcome:
        try:
            if self.prep is not None:
                pre = self.prep(site, stage)
                if pre is not None:
                    return pre
            rc = int(self.runner(site, stage))
        except Exception as exc:  # noqa: BLE001 - a stage error must not stop the scheduler
            return Outcome("failed", None, f"{type(exc).__name__}: {exc}"[:500])
        if rc == 0:
            return Outcome("ok", 0)
        return Outcome("failed", rc, f"killed by signal {-rc}" if rc < 0 else f"exit {rc}")

    def _complete_site(self, p: SitePlan) -> None:
        if not self.store:
            return
        rows = self.store.rows(p.site).get(p.site, {})
        for s in p.skip:
            if (rows.get(s) or {}).get("status") != "ok":
                self.store.skip(p.site, s, "skipped: --sample or --no-evict")

    def run(self) -> dict[str, str]:
        idx = {p.site: 0 for p in self.plans}
        done: dict[str, str] = {}
        for p in self.plans:
            if not p.stages:
                done[p.site] = "nothing to do"
                self._complete_site(p)
        running: dict[Future, tuple[str, str, str, float]] = {}
        busy: set[str] = set()
        in_flight: set[str] = set()
        used: Counter = Counter()
        waiting_disk: dict[str, str] = {}
        ex = ThreadPoolExecutor(max_workers=max(1, self.max_in_flight), thread_name_prefix="run-stage")
        try:
            while True:
                blocked: dict[str, str] = {}
                if not self.stopping:
                    for p in self.plans:
                        s = p.site
                        if s in done or s in busy:
                            continue
                        if s not in in_flight and len(in_flight) >= self.max_in_flight:
                            continue
                        stage = p.stages[idx[s]]
                        cls = STAGE_CLASS[stage]
                        lim = self.limits.get(cls)
                        if lim is not None and used[cls] >= lim:
                            continue
                        if stage == "download" and self.disk_guard is not None:
                            ok, why = self.disk_guard.try_reserve(s)
                            if not ok:
                                blocked[s] = why
                                if waiting_disk.get(s) is None:
                                    self.out(f"{s:5} download waits: {why}")
                                waiting_disk[s] = why
                                continue
                            waiting_disk.pop(s, None)
                        used[cls] += 1
                        busy.add(s)
                        in_flight.add(s)
                        attempt = 1
                        if self.store:
                            attempt = self.store.start(s, stage, _rel(log_path(s, stage)), self.mode)
                        self.events.append((s, stage, "start"))
                        self.out(f"{s:5} {stage:9} start (attempt {attempt}, {cls})")
                        running[ex.submit(self._work, s, stage)] = (s, stage, cls, time.monotonic())
                if not running:
                    if self.stopping or len(done) == len(self.plans):
                        break
                    if blocked:  # nothing runs, so nothing can free disk: fail the waiting downloads
                        for s, why in blocked.items():
                            self._finish(s, "download", Outcome("failed", None, why + "; nothing left to evict"),
                                         0.0)
                            done[s] = "failed"
                            in_flight.discard(s)
                            if self.disk_guard is not None:
                                self.disk_guard.release(s)
                        continue
                    for p in self.plans:  # safety net; not reachable with consistent limits
                        done.setdefault(p.site, "stopped")
                    break
                finished, _ = wait(list(running), timeout=self.poll_s, return_when=FIRST_COMPLETED)
                for fut in finished:
                    s, stage, cls, t0 = running.pop(fut)
                    used[cls] -= 1
                    busy.discard(s)
                    if stage == "download" and self.disk_guard is not None:
                        self.disk_guard.release(s)
                    out = fut.result()
                    self._finish(s, stage, out, time.monotonic() - t0)
                    p = next(q for q in self.plans if q.site == s)
                    if out.status == "ok":
                        idx[s] += 1
                        if idx[s] >= len(p.stages):
                            done[s] = "ok"
                            in_flight.discard(s)
                            self._complete_site(p)
                    else:
                        done[s] = "failed"
                        in_flight.discard(s)
        finally:
            ex.shutdown(wait=True)
        for p in self.plans:
            done.setdefault(p.site, "stopped")
        return done

    def _finish(self, site: str, stage: str, out: Outcome, seconds: float) -> None:
        if self.store:
            self.store.finish(site, stage, out.status, out.rc, out.info)
        self.events.append((site, stage, out.status))
        extra = f": {out.info}" if out.info else ""
        self.out(f"{site:5} {stage:9} {out.status} ({seconds:.0f}s){extra}")


# ---------------------------------------------------------------- site selection

def site_universe(*, con: Any | None = None) -> list[dict[str, Any]]:
    """Every site of the catalog with its habitat and non-AOP basic bytes."""
    from .site.sample import catalog_query

    return catalog_query(
        "SELECT s.siteCode AS site, s.siteProperties['HABITAT'] AS habitat, coalesce(i.bytes, 0) AS bytes "
        "FROM {cat}sites s LEFT JOIN (SELECT siteCode, sum(totalBytes) AS bytes FROM {cat}v_site_inventory "
        "GROUP BY siteCode) i ON i.siteCode = s.siteCode ORDER BY s.siteCode", con=con)


def order_sites(rows: list[dict[str, Any]], order: str) -> list[dict[str, Any]]:
    if order == "given":
        return list(rows)
    if order == "name":
        return sorted(rows, key=lambda r: r["site"])
    if order == "size":
        return sorted(rows, key=lambda r: (int(r.get("bytes") or 0), r["site"]))
    if order == "aquatic-first":
        return sorted(rows, key=lambda r: (not str(r.get("habitat") or "").startswith("Aquatic"),
                                           int(r.get("bytes") or 0), r["site"]))
    raise SystemExit(f"unknown order {order}; one of {', '.join(ORDERS)}")


def select_sites(sites: list[str] | None, *, all_sites: bool = False, habitat: str | None = None,
                 order: str | None = None, universe: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Ordered [{site, habitat, bytes}] for the requested sites / --all / --habitat."""
    codes = list(dict.fromkeys(s.strip().upper() for s in sites or [] if s.strip()))
    if not codes and not all_sites and not habitat:
        raise SystemExit("name one or more SITES, or use --all or --habitat")
    want: tuple[str, ...] | None = None
    if habitat:
        want = HABITATS.get(habitat.strip().lower())
        if want is None:
            raise SystemExit(f"unknown habitat {habitat!r}; one of Terrestrial, aquatic, " + ", ".join(AQUATIC))
    if universe is None:
        try:
            universe = site_universe()
        except (SystemExit, Exception):  # explicit site codes work without the catalog
            if all_sites or habitat:
                raise
            universe = None
    by_code = {r["site"]: r for r in universe or []}
    if codes:
        if by_code:
            unknown = [c for c in codes if c not in by_code]
            if unknown:
                raise SystemExit(f"unknown site(s): {', '.join(unknown)}")
        rows = [by_code.get(c, {"site": c, "habitat": None, "bytes": 0}) for c in codes]
    else:
        rows = list(by_code.values())
    if want is not None:
        rows = [r for r in rows if r.get("habitat") in want]
    return order_sites(rows, order or ("given" if codes else "aquatic-first"))


# ---------------------------------------------------------------- entry points

def build_plans(selected: list[dict[str, Any]], store: Store, cfg: RunConfig) -> list[SitePlan]:
    all_rows = store.rows()
    plans = []
    for r in selected:
        p = plan_site(r["site"], all_rows.get(r["site"], {}), cfg)
        p.habitat, p.bytes = r.get("habitat"), int(r.get("bytes") or 0)
        plans.append(p)
    return plans


def render_plan(plans: list[SitePlan], cfg: RunConfig) -> str:
    lines = [f"run plan: {len(plans)} site(s), mode {cfg.mode}, max {cfg.max_in_flight} in flight, "
             f"evict {'off' if cfg.skips_evict else 'on'}" + (", restack" if cfg.restack else ""),
             f"{'#':>3} {'site':5} {'habitat':28} {'GB':>6}  {'resume at':10} stages"]
    for i, p in enumerate(plans, 1):
        st = f"{p.stages[0]}..{p.stages[-1]} ({len(p.stages)})" if p.stages else "nothing to do"
        lines.append(f"{i:>3} {p.site:5} {str(p.habitat or '?')[:28]:28} {_gb(p.bytes):>6}  "
                     f"{(p.stages[0] if p.stages else '-'):10} {st}")
    first = next((p for p in plans if p.stages), None)
    if first:
        lines.append(f"commands for {first.site}:")
        for s in first.stages:
            for argv in stage_commands(first.site, s, sample=cfg.sample, threads=cfg.threads, restack=cfg.restack):
                lines.append(f"  {s:9} [{STAGE_CLASS[s]:10}] neon-ducklake {' '.join(argv[3:])}")
    return "\n".join(lines)


def main(sites: list[str] | None = None, *, all_sites: bool = False, habitat: str | None = None,
         sample: int | None = None, stages: str | None = None, from_stage: str | None = None, evict: bool = True,
         max_in_flight: int = 3, dry_run: bool = False, restack: bool = False, order: str | None = None,
         threads: int | None = None, poll_s: float = 5.0) -> int:
    cfg = make_config(stages=stages, from_stage=from_stage, sample=sample, evict=evict, max_in_flight=max_in_flight,
                      restack=restack, threads=threads, poll_s=poll_s)
    selected = select_sites(sites, all_sites=all_sites, habitat=habitat, order=order)
    if not selected:
        print("no sites selected")
        return 1
    con = state.connect(state.DB_PATH)
    try:
        store = Store(con)
        if dry_run:
            print(render_plan(build_plans(selected, store, cfg), cfg))
            return 0
        with env.file_lock("run"):
            for site, stage in store.recover_stale():
                say(f"{site:5} {stage:9} marked failed (interrupted by a previous orchestrator)")
            plans = build_plans(selected, store, cfg)
            for p in plans:
                store.set_pending(p.site, p.stages)
            print(render_plan(plans, cfg), flush=True)
            runner = SubprocessRunner(cfg, on_spawn=store.note_pid)
            guard = DiskGuard(download_bytes, free_disk, cfg.disk_factor)
            sched = Scheduler(plans, runner, store, mode=cfg.mode, max_in_flight=cfg.max_in_flight,
                              disk_guard=guard, prep=make_prep(cfg), poll_s=cfg.poll_s)
            old = {sig: signal.signal(sig, sched.on_signal) for sig in (signal.SIGINT, signal.SIGTERM)}
            try:
                results = sched.run()
            finally:
                for sig, handler in old.items():
                    signal.signal(sig, handler)
        counts = Counter(results.values())
        say("run done: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
        for s, r in results.items():
            if r not in ("ok", "nothing to do"):
                say(f"{s:5} {r}")
        with contextlib.suppress(Exception):  # the status file is a convenience
            write_status(collect_status(con, [r["site"] for r in selected]))
    finally:
        con.close()
    if sched.stopping:
        return 130
    return 0 if all(r in ("ok", "nothing to do") for r in results.values()) else 1


# ---------------------------------------------------------------- status

def _lock_held(name: str) -> bool:
    path = env.LOCK_DIR / f"{name}.lock"
    if not path.exists():
        return False
    with path.open("a+") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
    return False


def collect_status(con: sqlite3.Connection, sites: list[str] | None = None) -> dict[str, Any]:
    """Per-site stage statuses + journal totals (files, bytes, evicted, AVUs recorded, snapshots)."""
    from .site import evict

    store = Store(con)
    runs = store.rows()
    files: dict[str, dict[str, int]] = {}
    for r in con.execute(
            "SELECT site, count(*) AS n, coalesce(sum(coalesce(size, 0)), 0) AS bytes,"
            " coalesce(sum(dl_status = 'ok'), 0) AS dl_ok,"
            " coalesce(sum(CASE WHEN dl_status = 'ok' THEN coalesce(dl_bytes, size, 0) END), 0) AS dl_bytes,"
            " coalesce(sum(up_status IN ('uploaded', 'present')), 0) AS up_ok"
            " FROM site_files WHERE coalesce(dl_status, '') != 'superseded' GROUP BY site"):
        files[r["site"]] = {k: int(r[k]) for k in ("n", "bytes", "dl_ok", "dl_bytes", "up_ok")}
    evicted = evict.evicted_totals(con)
    avus = {r[0]: int(r[1]) for r in con.execute(
        "SELECT scope, count(*) FROM avu_journal WHERE status = 'recorded' GROUP BY scope")}
    snaps = {r[0]: int(r[1]) for r in con.execute(
        "SELECT scope, count(*) FROM batches WHERE status = 'recorded' GROUP BY scope")}
    names = sorted(set(runs) | set(files)) if not sites else [s.upper() for s in sites]
    out_sites: dict[str, Any] = {}
    for s in names:
        f = files.get(s, {})
        out_sites[s] = {
            "stages": {st: {k: (runs.get(s, {}).get(st) or {}).get(k) for k in
                            ("status", "attempt", "rc", "started_at", "finished_at", "log_path", "mode")}
                       | {"info": _json((runs.get(s, {}).get(st) or {}).get("info")).get("message")}
                       for st in STAGES if st in runs.get(s, {})},
            "files": f.get("n", 0), "bytes": f.get("bytes", 0), "downloaded": f.get("dl_ok", 0),
            "downloadedBytes": f.get("dl_bytes", 0), "uploaded": f.get("up_ok", 0),
            "evictedFiles": evicted.get(s, {}).get("files", 0), "evictedBytes": evicted.get(s, {}).get("bytes", 0),
            "avusRecorded": avus.get(s, 0), "snapshots": snaps.get(s, 0)}
    totals = {k: sum(v[k] for v in out_sites.values()) for k in
              ("files", "bytes", "downloaded", "downloadedBytes", "uploaded", "evictedFiles", "evictedBytes",
               "avusRecorded", "snapshots")}
    stage_counts = Counter(st["status"] for v in out_sites.values() for st in v["stages"].values())
    return {"generatedAt": env.utcnow().isoformat(), "orchestratorRunning": _lock_held("run"),
            "sites": out_sites, "totals": totals, "stageCounts": dict(stage_counts),
            "allScopes": {"avusRecorded": sum(avus.values()), "snapshots": sum(snaps.values()),
                          "snapshotBudget": SNAPSHOT_BUDGET}}


def render_status(rep: dict[str, Any]) -> str:
    head = f"{'site':5} " + " ".join(f"{SHORT[s]:>4}" for s in STAGES) + \
        f" {'files':>8} {'GB':>7} {'evictGB':>7} {'AVUs':>10} {'snaps':>5}"
    title = (f"neon-ducklake status ({rep['generatedAt'][:19]}Z; orchestrator "
             f"{'running' if rep['orchestratorRunning'] else 'not running'})")
    lines = [title, head, "-" * len(head)]
    problems = []
    for s, v in rep["sites"].items():
        cells = []
        for st in STAGES:
            info = v["stages"].get(st)
            status = (info or {}).get("status")
            if status == "running" and not rep["orchestratorRunning"]:
                status = "failed"  # a dead orchestrator's stage
            cells.append(f"{SYMBOLS.get(status, ' ') if status else ' ':>4}")
            if info and status == "failed":
                problems.append(f"{s} {st} failed (attempt {info.get('attempt')}, rc {info.get('rc')}): "
                                f"{info.get('info') or ''} [{info.get('log_path') or ''}]")
        lines.append(f"{s:5} " + " ".join(cells) + f" {v['files']:>8} {_gb(v['bytes']):>7} "
                     f"{_gb(v['evictedBytes']):>7} {v['avusRecorded']:>10} {v['snapshots']:>5}")
    t = rep["totals"]
    lines.append("-" * len(head))
    lines.append(f"{'total':5} " + " " * (5 * len(STAGES) - 1) + f" {t['files']:>8} {_gb(t['bytes']):>7} "
                 f"{_gb(t['evictedBytes']):>7} {t['avusRecorded']:>10} {t['snapshots']:>5}")
    a = rep["allScopes"]
    lines.append(f"all scopes: {a['avusRecorded']} AVU rows recorded, {a['snapshots']} MESA snapshots "
                 f"(budget {a['snapshotBudget']})")
    lines.append("legend: + ok  X failed  > running  . pending  - skipped  (blank) never run")
    lines += problems
    return "\n".join(lines)


def write_status(rep: dict[str, Any]) -> Path:
    path = env.ROOT / "logs" / "status.json"
    env.write_json(path, rep)
    return path


def status_main(sites: list[str] | None = None, *, as_json: bool = False) -> int:
    con = state.connect(state.DB_PATH)
    try:
        rep = collect_status(con, sites or None)
    finally:
        con.close()
    path = write_status(rep)
    print(json.dumps(rep, indent=1, default=str) if as_json else render_status(rep))
    if not as_json:
        print(f"\nwrote {_rel(path)}")
    return 0
