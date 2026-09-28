"""run.py scheduler: ordering, resource-class concurrency, resume, failures, SIGINT, disk guard (fake runners)."""

from __future__ import annotations

import signal
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path

import pytest

from neon_ducklake import run, state

from .test_site_common import site_env  # noqa: F401 - fixture


class FakeRunner:
    """Records a timeline of (site, stage, start, end); per-(site, stage) behaviour hooks."""

    def __init__(self, hooks: dict[tuple[str, str], Callable[[], int]] | None = None, dur: float = 0.005) -> None:
        self.hooks = hooks or {}
        self.dur = dur
        self.lock = threading.Lock()
        self.timeline: list[tuple[str, str, float, float]] = []
        self.started: list[tuple[str, str]] = []
        self.killed = threading.Event()

    def __call__(self, site: str, stage: str) -> int:
        t0 = time.monotonic()
        with self.lock:
            self.started.append((site, stage))
        hook = self.hooks.get((site, stage))
        rc = hook() if hook else 0
        if not hook:
            time.sleep(self.dur)
        with self.lock:
            self.timeline.append((site, stage, t0, time.monotonic()))
        return rc

    def kill(self) -> None:
        self.killed.set()

    def max_concurrency(self) -> dict[str, int]:
        """Max number of overlapping stages per resource class."""
        out: dict[str, int] = {}
        by_cls: dict[str, list[tuple[float, int]]] = defaultdict(list)
        for _site, stage, a, b in self.timeline:
            by_cls[run.STAGE_CLASS[stage]] += [(a, 1), (b, -1)]
        for cls, ev in by_cls.items():
            cur = best = 0
            for _t, d in sorted(ev, key=lambda x: (x[0], x[1])):
                cur += d
                best = max(best, cur)
            out[cls] = best
        return out


def _store() -> run.Store:
    return run.Store(state.connect(state.DB_PATH))


def _plans(sites: list[str], stages: list[str] | None = None) -> list[run.SitePlan]:
    return [run.SitePlan(s, list(stages or run.STAGES)) for s in sites]


def _sched(plans: list[run.SitePlan], runner: FakeRunner, store: run.Store | None = None, **kw) -> run.Scheduler:
    kw.setdefault("poll_s", 0.01)
    kw.setdefault("out", lambda _msg: None)
    return run.Scheduler(plans, runner, store, **kw)


# ---------------------------------------------------------------- ordering / concurrency

def test_every_site_runs_all_stages_in_order_within_class_limits(site_env: Path) -> None:  # noqa: F811
    store = _store()
    r = FakeRunner(dur=0.01)
    res = _sched(_plans(["A", "B", "C"]), r, store, max_in_flight=3).run()
    assert res == {"A": "ok", "B": "ok", "C": "ok"}
    for s in "ABC":
        assert [st for site, st in r.started if site == s] == list(run.STAGES)
    conc = r.max_concurrency()
    for cls, n in conc.items():
        lim = run.CLASS_LIMITS[cls]
        assert lim is None or n <= lim, (cls, n)
    rows = store.rows()
    assert all(rows[s][st]["status"] == "ok" and rows[s][st]["attempt"] == 1 for s in "ABC" for st in run.STAGES)
    assert rows["A"]["stack"]["log_path"] == "logs/run/A/stack.log"


def test_net_in_allows_two_sites_cpu_only_one(site_env: Path) -> None:  # noqa: F811
    barrier = threading.Barrier(2, timeout=5)

    def list_hook() -> int:  # two sites must be listing at the same time
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            return 9
        time.sleep(0.02)
        return 0

    hooks = {(s, "list"): list_hook for s in ("A", "B")}
    r = FakeRunner(hooks, dur=0.02)
    res = _sched(_plans(["A", "B", "C"], ["plan", "list", "stack", "terms"]), r, max_in_flight=3).run()
    assert res == {"A": "ok", "B": "ok", "C": "ok"}
    conc = r.max_concurrency()
    assert conc["net-in"] == 2
    assert conc["cpu"] == 1


def test_site_b_downloads_while_site_a_uploads(site_env: Path) -> None:  # noqa: F811
    a_up, b_dl = threading.Event(), threading.Event()

    def a_upload() -> int:
        a_up.set()
        return 0 if b_dl.wait(5) else 99

    def b_download() -> int:
        b_dl.set()
        return 0 if a_up.wait(5) else 99

    stages = ["plan", "list", "download", "stack", "upload", "verify"]
    r = FakeRunner({("A", "upload"): a_upload, ("B", "download"): b_download})
    res = _sched(_plans(["A", "B"], stages), r, max_in_flight=2).run()
    assert res == {"A": "ok", "B": "ok"}


def test_priority_order_with_one_site_in_flight(site_env: Path) -> None:  # noqa: F811
    r = FakeRunner()
    res = _sched(_plans(["C", "A", "B"], ["plan", "list", "stack"]), r, max_in_flight=1).run()
    assert res == {"C": "ok", "A": "ok", "B": "ok"}
    assert [s for s, _ in r.started] == ["C"] * 3 + ["A"] * 3 + ["B"] * 3


def test_single_class_slot_goes_to_the_earlier_site(site_env: Path) -> None:  # noqa: F811
    r = FakeRunner(dur=0.01)
    _sched(_plans(["A", "B", "C"], ["stack"]), r, max_in_flight=3).run()
    assert [s for s, _ in r.started] == ["A", "B", "C"]


# ---------------------------------------------------------------- failure + resume

def test_failed_stage_stops_that_site_only_and_resumes_next_time(site_env: Path) -> None:  # noqa: F811
    store = _store()
    cfg = run.make_config()
    sel = [{"site": "A"}, {"site": "B"}]
    r1 = FakeRunner({("A", "stack"): lambda: 2})
    res = _sched(run.build_plans(sel, store, cfg), r1, store, mode=cfg.mode).run()
    assert res == {"A": "failed", "B": "ok"}
    assert [st for s, st in r1.started if s == "A"] == ["plan", "list", "download", "stack"]
    rows = store.rows()
    assert rows["A"]["stack"]["status"] == "failed" and rows["A"]["stack"]["rc"] == 2
    assert "exit 2" in rows["A"]["stack"]["info"]
    assert "terms" not in rows["A"]  # never scheduled

    # next invocation: A resumes at stack (attempt 2), B has nothing to do
    plans = run.build_plans(sel, store, cfg)
    assert plans[0].stages[0] == "stack" and plans[1].stages == []
    for p in plans:
        store.set_pending(p.site, p.stages)
    r2 = FakeRunner()
    res = _sched(plans, r2, store, mode=cfg.mode).run()
    assert res == {"A": "ok", "B": "nothing to do"}
    assert [st for _s, st in r2.started] == list(run.STAGES[run.STAGES.index("stack"):])
    assert store.rows()["A"]["stack"]["attempt"] == 2
    assert all(v["status"] == "ok" for v in store.rows()["A"].values())


def test_runner_exception_is_a_failed_stage(site_env: Path) -> None:  # noqa: F811
    def boom() -> int:
        raise RuntimeError("no such thing")

    store = _store()
    res = _sched(_plans(["A"], ["plan", "list"]), FakeRunner({("A", "plan"): boom}), store).run()
    assert res == {"A": "failed"}
    assert "RuntimeError: no such thing" in store.rows()["A"]["plan"]["info"]


def test_plan_site_resume_rules() -> None:
    cfg = run.make_config()
    ok = {"status": "ok", "mode": "full"}
    rows = {s: dict(ok) for s in run.STAGES[:4]} | {"terms": {"status": "failed", "mode": "full"}}
    assert run.plan_site("A", rows, cfg).stages == list(run.STAGES[4:])
    done = {s: dict(ok) for s in run.STAGES}
    assert run.plan_site("A", done, cfg).stages == []
    # a sample run never satisfies a full run; full satisfies a sample; sample:3 satisfies sample:2
    sampled = {s: {"status": "ok", "mode": "sample:2"} for s in run.STAGES}
    assert run.plan_site("A", sampled, cfg).stages == list(run.STAGES)
    s2 = run.make_config(sample=2)
    assert run.plan_site("A", done, s2).stages == []
    assert run.plan_site("A", sampled, s2).stages == []
    assert run.plan_site("A", sampled, run.make_config(sample=3)).stages[0] == "plan"
    assert run.covers("sample:3", "sample:2") and not run.covers(None, "full")
    # sample / --no-evict drop evict (marked skipped at completion)
    p = run.plan_site("A", {}, s2)
    assert "evict" not in p.stages and p.skip == ["evict"]
    assert run.plan_site("A", {}, run.make_config(evict=False)).skip == ["evict"]
    # --from-stage re-runs from there even when everything is ok; --stages restricts
    assert run.plan_site("A", done, run.make_config(from_stage="upload")).stages == list(run.STAGES[8:])
    only = run.make_config(stages="plan,list,download")
    assert run.plan_site("A", {}, only).stages == ["plan", "list", "download"]
    # evict previously skipped -> a normal run does just evict
    no_ev = {s: dict(ok) for s in run.STAGES[:-1]} | {"evict": {"status": "skipped", "mode": None}}
    assert run.plan_site("A", no_ev, cfg).stages == ["evict"]


def test_skipped_evict_marked_at_completion_but_never_over_ok(site_env: Path) -> None:  # noqa: F811
    store = _store()
    cfg = run.make_config(evict=False)
    plans = run.build_plans([{"site": "A"}], store, cfg)
    _sched(plans, FakeRunner(), store, mode=cfg.mode).run()
    assert store.rows()["A"]["evict"]["status"] == "skipped"
    store.finish("A", "evict", "ok", 0)
    plans = run.build_plans([{"site": "A"}], store, cfg)
    _sched(plans, FakeRunner(), store, mode=cfg.mode).run()
    assert store.rows()["A"]["evict"]["status"] == "ok"


def test_make_config_rejects_bad_combinations() -> None:
    with pytest.raises(SystemExit, match="unknown stage"):
        run.make_config(stages="plan,bogus")
    with pytest.raises(SystemExit, match="from-stage"):
        run.make_config(stages="plan,list", from_stage="stack")
    with pytest.raises(SystemExit, match="restack"):
        run.make_config(restack=True, from_stage="stack")
    with pytest.raises(SystemExit, match="restack"):
        run.make_config(restack=True, stages="stack,terms")
    with pytest.raises(SystemExit, match="from-stage"):
        run.make_config(sample=2, from_stage="evict")
    assert run.make_config(restack=True).from_stage == "download"


# ---------------------------------------------------------------- SIGINT

def test_first_signal_stops_scheduling_and_lets_running_stage_finish(site_env: Path) -> None:  # noqa: F811
    store = _store()
    started, release = threading.Event(), threading.Event()

    def slow_list() -> int:
        started.set()
        release.wait(5)
        return 0

    r = FakeRunner({("A", "list"): slow_list})
    plans = _plans(["A", "B"], ["plan", "list", "download", "stack"])
    for p in plans:
        store.set_pending(p.site, p.stages)
    sched = _sched(plans, r, store, max_in_flight=1)
    out: dict = {}
    t = threading.Thread(target=lambda: out.update(sched.run()))
    t.start()
    assert started.wait(5)
    sched.on_signal(signal.SIGINT, None)
    release.set()
    t.join(5)
    assert not t.is_alive()
    assert out == {"A": "stopped", "B": "stopped"}
    assert r.started == [("A", "plan"), ("A", "list")]
    rows = store.rows()
    assert rows["A"]["list"]["status"] == "ok"
    assert rows["A"]["download"]["status"] == "pending"
    assert rows["B"]["plan"]["status"] == "pending"
    assert not r.killed.is_set()


def test_second_signal_kills_running_children(site_env: Path) -> None:  # noqa: F811
    store = _store()
    started = threading.Event()
    r = FakeRunner()

    def killable() -> int:
        started.set()
        return -15 if r.killed.wait(5) else 0

    r.hooks[("A", "upload")] = killable
    sched = _sched(_plans(["A"], ["upload", "apply"]), r, store)
    out: dict = {}
    t = threading.Thread(target=lambda: out.update(sched.run()))
    t.start()
    assert started.wait(5)
    sched.on_signal(signal.SIGINT, None)
    sched.on_signal(signal.SIGINT, None)
    t.join(5)
    assert out == {"A": "failed"}
    row = store.rows()["A"]["upload"]
    assert row["status"] == "failed" and row["rc"] == -15 and "killed by signal 15" in row["info"]
    assert ("A", "apply") not in r.started


def test_stale_running_rows_are_recovered_unless_a_child_lives(site_env: Path,  # noqa: F811
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store()
    store.start("A", "download", "logs/run/A/download.log", "full")
    store.note_pid("A", "download", 999999)
    monkeypatch.setattr(run, "_alive", lambda pid: True)
    with pytest.raises(SystemExit, match="still running as pid 999999"):
        store.recover_stale()
    monkeypatch.setattr(run, "_alive", lambda pid: False)
    assert store.recover_stale() == [("A", "download")]
    row = store.rows()["A"]["download"]
    assert row["status"] == "failed" and "interrupted" in row["info"] and "pids" not in row["info"]


# ---------------------------------------------------------------- disk guard

def test_disk_guard_arithmetic() -> None:
    need = {"A": 60, "B": 50, "Z": 0}
    free = {"v": 100}
    g = run.DiskGuard(need.__getitem__, lambda: free["v"], factor=1.3)
    assert g.try_reserve("A") == (True, "")
    ok, why = g.try_reserve("B")
    assert not ok and "disk guard" in why  # 65 + 78 reserved > 100
    assert g.try_reserve("Z")[0]  # nothing to download never waits
    g.release("A")
    free["v"] = 64
    assert not g.try_reserve("B")[0]
    free["v"] = 65
    assert g.try_reserve("B")[0]


def test_download_waits_for_evictions(site_env: Path) -> None:  # noqa: F811
    disk = {"used": 0}
    capacity = 100
    lock = threading.Lock()

    def use(delta: int) -> Callable[[], int]:
        def f() -> int:
            with lock:
                disk["used"] += delta
            time.sleep(0.02)
            return 0
        return f

    hooks = {("A", "download"): use(60), ("A", "evict"): use(-60), ("B", "download"): use(50)}
    r = FakeRunner(hooks)
    guard = run.DiskGuard({"A": 60, "B": 50}.__getitem__, lambda: capacity - disk["used"], factor=1.3)
    stages = ["plan", "download", "stack", "verify", "evict"]
    res = _sched(_plans(["A", "B"], stages), r, max_in_flight=2, disk_guard=guard).run()
    assert res == {"A": "ok", "B": "ok"}
    t = {(s, st): (a, b) for s, st, a, b in r.timeline}
    # B's download (65 needed) could start only once A's eviction had freed the space A downloaded
    assert t[("B", "download")][0] >= t[("A", "evict")][0]
    assert t[("B", "download")][0] >= t[("A", "download")][1]
    assert guard.reserved == {}


def test_download_fails_when_nothing_can_free_disk(site_env: Path) -> None:  # noqa: F811
    store = _store()
    guard = run.DiskGuard(lambda s: 50, lambda: 10)
    r = FakeRunner()
    res = _sched(_plans(["A", "B"], ["plan", "download", "stack"]), r, store, disk_guard=guard).run()
    assert res == {"A": "failed", "B": "failed"}
    assert all(st != "download" for _s, st in r.started)
    row = store.rows()["A"]["download"]
    assert row["status"] == "failed" and "disk guard" in row["info"] and "nothing left to evict" in row["info"]


def test_classes_counted_per_stage_not_per_site(site_env: Path) -> None:  # noqa: F811
    """A site never runs two stages at once; the number of running stages never exceeds sites in flight."""
    r = FakeRunner(dur=0.01)
    _sched(_plans(["A", "B", "C", "D"], ["plan", "verify", "evict"]), r, max_in_flight=2).run()
    events = sorted([(a, 1) for *_x, a, _b in r.timeline] + [(b, -1) for *_x, _a, b in r.timeline],
                    key=lambda e: (e[0], e[1]))
    cur = best = 0
    for _t, d in events:
        cur += d
        best = max(best, cur)
    assert best <= 2
    per_site = Counter(s for s, _ in r.started)
    assert per_site == {"A": 3, "B": 3, "C": 3, "D": 3}
