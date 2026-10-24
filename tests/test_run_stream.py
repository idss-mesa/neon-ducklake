"""run.py stream mode (docs/STREAMING.md "Orchestrator"): chunk pipeline scheduling with fake runners, resume
from the chunk journal, working-set guard, finalize/cleanup ordering, status, and CLI wiring of every new
command (lazy imports replaced by fake modules)."""

from __future__ import annotations

import contextlib
import json
import signal
import sys
import threading
import time
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import cli, env, run, state

from .test_site_common import site_env  # noqa: F401 - fixture

CS = run.CHUNK_STAGES


@pytest.fixture
def ds_coord(monkeypatch: pytest.MonkeyPatch) -> types.SimpleNamespace:
    """Fake Data Store coordination for run.main (docs/STREAMING.md "Coordination"): coord.site_claim,
    coord.write_done, journal_sync.push_site_journal and journal_sync.needs_restore, recorded instead of talking
    to iRODS. Like the real site_claim, a site whose .done was written raises coord.SiteDone; ``newer`` holds the
    sites whose Data Store journal is newer than this host's."""
    from neon_ducklake import bootstrap, coord, journal_sync

    rec = types.SimpleNamespace(claims=[], released=[], pushes=[], done=[], lost={}, raises={}, published=[],
                                newer=set(), restore_checks=[])

    @contextlib.contextmanager
    def site_claim(site: str) -> Any:
        exc = rec.raises.get(site)
        if exc is not None:
            raise exc
        if any(d[0] == site for d in rec.done):
            raise coord.SiteDone(site, f"site {site} is done (.done marker)")
        claim = types.SimpleNamespace(site=site, lost=threading.Event())
        rec.claims.append(site)
        rec.lost[site] = claim.lost
        try:
            yield claim
        finally:
            rec.released.append(site)

    def push(site: str) -> dict[str, Any]:
        rec.pushes.append(site)
        return {"site": site, "tables": {}}

    def needs_restore(site: str, **kw: Any) -> bool:
        rec.restore_checks.append(site)
        return site in rec.newer

    monkeypatch.setattr(coord, "site_claim", site_claim, raising=False)
    monkeypatch.setattr(coord, "write_done", lambda site, summary, **kw: rec.done.append((site, summary)) or "")
    monkeypatch.setattr(journal_sync, "push_site_journal", push)
    monkeypatch.setattr(journal_sync, "needs_restore", needs_restore)
    monkeypatch.setattr(bootstrap, "publish_ontology", lambda **kw: rec.published.append(kw) or
                        {"status": "published"}, raising=False)
    return rec


# ---------------------------------------------------------------- fakes

class FakeStreamRunner:
    """runner(site, stage, chunk) with per-(site, chunk, stage) hooks; records a timeline."""

    def __init__(self, hooks: dict[tuple[str, str | None, str], Callable[[], int]] | None = None,
                 dur: float = 0.003, journal: Callable[[str, str | None, str, int], None] | None = None) -> None:
        self.hooks = hooks or {}
        self.dur = dur
        self.journal = journal
        self.lock = threading.Lock()
        self.timeline: list[tuple[str, str | None, str, float, float]] = []
        self.started: list[tuple[str, str | None, str]] = []
        self.killed = threading.Event()

    def __call__(self, site: str, stage: str, chunk: str | None = None) -> int:
        t0 = time.monotonic()
        with self.lock:
            self.started.append((site, chunk, stage))
        hook = self.hooks.get((site, chunk, stage))
        rc = hook() if hook else 0
        if not hook:
            time.sleep(self.dur)
        if self.journal is not None:
            self.journal(site, chunk, stage, rc)
        with self.lock:
            self.timeline.append((site, chunk, stage, t0, time.monotonic()))
        return rc

    def kill(self) -> None:
        self.killed.set()

    def span(self, site: str, chunk: str | None, stage: str) -> tuple[float, float]:
        return next((a, b) for s, c, st, a, b in self.timeline if (s, c, st) == (site, chunk, stage))

    def stages_of(self, site: str, chunk: str) -> list[str]:
        return [st for s, c, st in self.started if s == site and c == chunk]


def _chunks(*ids: str, nbytes: int = 1, nxt: int = 0) -> list[run.ChunkRun]:
    return [run.ChunkRun(i, nbytes, nxt) for i in ids]


def _loader(table: dict[str, list[run.ChunkRun]]) -> Callable[[str], list[run.ChunkRun]]:
    return lambda site: [run.ChunkRun(c.chunk_id, c.bytes, c.next) for c in table.get(site, [])]


def _plans(sites: list[str], stages: list[str] | None = None) -> list[run.StreamSitePlan]:
    return [run.StreamSitePlan(s, list(stages or ["chunks", "finalize", "cleanup"])) for s in sites]


def _sched(plans: list[run.StreamSitePlan], runner: FakeStreamRunner, chunks: dict[str, list[run.ChunkRun]] | None,
           store: run.Store | None = None, **kw: Any) -> run.StreamScheduler:
    kw.setdefault("poll_s", 0.01)
    kw.setdefault("out", lambda _msg: None)
    kw.setdefault("load_chunks", _loader(chunks or {}))
    return run.StreamScheduler(plans, runner, store, **kw)


def _store() -> run.Store:
    return run.Store(state.connect(state.DB_PATH))


def _max_overlap(intervals: list[tuple[float, float]]) -> int:
    ev = sorted([(a, 1) for a, _ in intervals] + [(b, -1) for _, b in intervals], key=lambda e: (e[0], e[1]))
    cur = best = 0
    for _t, d in ev:
        cur += d
        best = max(best, cur)
    return best


# ---- a stream_chunks journal (contract schema, docs/STREAMING.md "Chunks") updated like site/stream.py would

JOURNAL_DDL = """CREATE TABLE IF NOT EXISTS stream_chunks (
  site TEXT NOT NULL, chunk_id TEXT NOT NULL, product TEXT, months TEXT, n_files INTEGER, bytes INTEGER,
  status TEXT, attempt INTEGER DEFAULT 0, started_at TEXT, finished_at TEXT, error TEXT,
  PRIMARY KEY (site, chunk_id))"""


def _journal(site: str, rows: list[tuple[str, int, str]], product: str = "DP1.00001.001") -> None:
    con = state.connect(state.DB_PATH)
    con.execute(JOURNAL_DDL)
    for cid, nbytes, status in rows:
        month = cid.rsplit("__", 1)[-1].split("_")[0] if "__" in cid else "2024-01"
        con.execute("INSERT OR REPLACE INTO stream_chunks(site, chunk_id, product, months, n_files, bytes, status) "
                    "VALUES (?,?,?,?,?,?,?)", (site, cid, product, json.dumps([month]), 1, nbytes, status))
    con.close()


def _journal_status(site: str) -> dict[str, str]:
    con = state.connect(state.DB_PATH)
    try:
        return dict(con.execute("SELECT chunk_id, status FROM stream_chunks WHERE site=?", (site,)).fetchall())
    finally:
        con.close()


def journal_writer(site_chunks: dict[str, list[tuple[str, int]]]) -> Callable[[str, str | None, str, int], None]:
    """Fake site/chunks.py + site/stream.py: chunk-plan inserts planned rows, a chunk stage advances the status."""

    def cb(site: str, chunk: str | None, stage: str, rc: int) -> None:
        con = state.connect(state.DB_PATH)
        try:
            con.execute(JOURNAL_DDL)
            if chunk is None:
                if stage == "chunk-plan" and rc == 0:
                    for cid, nbytes in site_chunks.get(site, []):
                        con.execute("INSERT OR IGNORE INTO stream_chunks(site, chunk_id, product, months, n_files, "
                                    "bytes, status) VALUES (?,?,?,?,?,?, 'planned')",
                                    (site, cid, "DP1.00001.001", '["2024-01"]', 1, nbytes))
                return
            new = run.CHUNK_STATUS_ORDER[CS.index(stage) + 1] if rc == 0 else "failed"
            con.execute("UPDATE stream_chunks SET status=? WHERE site=? AND chunk_id=?", (new, site, chunk))
        finally:
            con.close()

    return cb


# ---------------------------------------------------------------- pipeline order, overlap, finalize, cleanup

def test_chunk_pipeline_order_overlap_finalize_after_delete_and_cleanup_last(site_env: Path) -> None:  # noqa: F811
    c2_dl, c1_up = threading.Event(), threading.Event()

    def c1_upload() -> int:  # chunk 1 uploads while chunk 2 downloads (and vice versa)
        c1_up.set()
        return 0 if c2_dl.wait(5) else 91

    def c2_download() -> int:
        c2_dl.set()
        return 0 if c1_up.wait(5) else 92

    r = FakeStreamRunner({("A", "c1", "upload"): c1_upload, ("A", "c2", "download"): c2_download})
    store = _store()
    sched = _sched(_plans(["A"]), r, {"A": _chunks("c1", "c2", "c3")}, store)
    assert sched.run() == {"A": "ok"}
    for c in ("c1", "c2", "c3"):
        assert r.stages_of("A", c) == list(CS)
    # pipeline order: chunk k+1 starts a stage only after chunk k finished it
    for st in CS:
        for a, b in (("c1", "c2"), ("c2", "c3")):
            assert r.span("A", b, st)[0] >= r.span("A", a, st)[1], st
    # never more than chunk_depth (2) chunks between download and delete
    active = [(r.span("A", c, "download")[0], r.span("A", c, "delete")[1]) for c in ("c1", "c2", "c3")]
    assert _max_overlap(active) <= 2
    last_delete = max(r.span("A", c, "delete")[1] for c in ("c1", "c2", "c3"))
    fin = r.span("A", None, "finalize")
    assert fin[0] >= last_delete
    assert r.span("A", None, "cleanup")[0] >= fin[1]
    assert r.started[-1] == ("A", None, "cleanup")
    rows = store.rows()["A"]
    assert rows["chunks"]["status"] == "ok" and "3 chunk(s)" in rows["chunks"]["info"]
    assert rows["finalize"]["status"] == "ok" and rows["cleanup"]["status"] == "ok"
    crow = store.chunk_rows("A")
    assert all(crow[c][st]["status"] == "ok" for c in ("c1", "c2", "c3") for st in CS)
    assert crow["c2"]["upload"]["log_path"] == "logs/run/A/chunks/c2/upload.log"


def test_chunk_depth_one_runs_chunks_back_to_back(site_env: Path) -> None:  # noqa: F811
    r = FakeStreamRunner()
    assert _sched(_plans(["A"], ["chunks"]), r, {"A": _chunks("c1", "c2")}, chunk_depth=1).run() == {"A": "ok"}
    assert r.span("A", "c2", "download")[0] >= r.span("A", "c1", "delete")[1]


def test_class_limits_hold_across_sites_and_chunks(site_env: Path) -> None:  # noqa: F811
    r = FakeStreamRunner(dur=0.004)
    res = _sched(_plans(["A", "B", "C"]), r, {s: _chunks("c1", "c2", "c3") for s in "ABC"}, max_in_flight=3).run()
    assert res == {"A": "ok", "B": "ok", "C": "ok"}
    by_cls: dict[str, list[tuple[float, float]]] = {}
    for _s, c, st, a, b in r.timeline:
        for k in run.stream_classes(st, c):
            by_cls.setdefault(k, []).append((a, b))
    for k, iv in by_cls.items():
        lim = run.CLASS_LIMITS[k]
        assert lim is None or _max_overlap(iv) <= lim, k


# ---------------------------------------------------------------- working set

def _held_hooks(held: dict[tuple[str, str], int], site: str, chunks: list[str], nbytes: int,
                extra: dict[tuple[str, str | None, str], Callable[[], int]] | None = None
                ) -> dict[tuple[str, str | None, str], Callable[[], int]]:
    lock = threading.Lock()

    def dl(c: str) -> Callable[[], int]:
        def f() -> int:
            time.sleep(0.01)
            with lock:
                held[(site, c)] = nbytes
            return 0
        return f

    def rm(c: str) -> Callable[[], int]:
        def f() -> int:
            with lock:
                held.pop((site, c), None)
            time.sleep(0.005)
            return 0
        return f

    hooks = {(site, c, "download"): dl(c) for c in chunks} | {(site, c, "delete"): rm(c) for c in chunks}
    return hooks | (extra or {})


def test_working_set_cap_holds_the_next_download_until_a_delete(site_env: Path) -> None:  # noqa: F811
    held: dict[tuple[str, str], int] = {}
    r = FakeStreamRunner(_held_hooks(held, "A", ["c1", "c2"], 6))
    guard = run.WorksetGuard(10, lambda: dict(held))
    msgs: list[str] = []
    res = _sched(_plans(["A"], ["chunks"]), r, {"A": _chunks("c1", "c2", nbytes=6)}, guard=guard,
                 out=msgs.append).run()
    assert res == {"A": "ok"}
    assert r.span("A", "c2", "download")[0] >= r.span("A", "c1", "delete")[1]
    assert guard.reserved == {}
    assert any("c2 waits: working set" in m for m in msgs)


def test_working_set_lets_download_k1_overlap_chunk_k_when_both_fit(site_env: Path) -> None:  # noqa: F811
    held: dict[tuple[str, str], int] = {}
    c2_dl = threading.Event()

    def c1_apply() -> int:
        return 0 if c2_dl.wait(5) else 93

    def c2_download() -> int:
        c2_dl.set()
        held[("A", "c2")] = 6
        return 0

    hooks = _held_hooks(held, "A", ["c1", "c2"], 6, {("A", "c1", "apply"): c1_apply,
                                                     ("A", "c2", "download"): c2_download})
    r = FakeStreamRunner(hooks)
    guard = run.WorksetGuard(12, lambda: dict(held))
    assert _sched(_plans(["A"], ["chunks"]), r, {"A": _chunks("c1", "c2", nbytes=6)}, guard=guard).run() == {"A": "ok"}


def test_chunk_larger_than_the_cap_runs_alone(site_env: Path) -> None:  # noqa: F811
    held: dict[tuple[str, str], int] = {}
    r = FakeStreamRunner(_held_hooks(held, "A", ["c1", "c2"], 8))
    guard = run.WorksetGuard(5, lambda: dict(held))
    assert _sched(_plans(["A"], ["chunks"]), r, {"A": _chunks("c1", "c2", nbytes=8)}, guard=guard).run() == {"A": "ok"}
    assert r.span("A", "c2", "download")[0] >= r.span("A", "c1", "delete")[1]


def test_working_set_full_with_nothing_running_fails_the_download(site_env: Path) -> None:  # noqa: F811
    store = _store()
    guard = run.WorksetGuard(50, lambda: {("Z", "old"): 100})  # a failed site's chunk still on disk
    r = FakeStreamRunner()
    res = _sched(_plans(["A"]), r, {"A": _chunks("c1", nbytes=10)}, store, guard=guard).run()
    assert res == {"A": "failed"}
    assert r.started == []
    row = store.chunk_rows("A")["c1"]["download"]
    assert row["status"] == "failed" and "working set" in row["info"] and "nothing running can free it" in row["info"]
    assert store.rows()["A"]["chunks"]["status"] == "failed"
    assert "finalize" not in store.rows()["A"]


def test_workset_guard_arithmetic() -> None:
    held = {("A", "c1"): 30}
    free = {"v": 10**12}
    g = run.WorksetGuard(50, lambda: dict(held), lambda: free["v"], factor=1.3)
    assert g.try_reserve(("A", "c2"), 20) == (True, "")  # 30 + 20 <= 50
    ok, why = g.try_reserve(("B", "c1"), 1)  # 30 + 20 reserved + 1 > 50
    assert not ok and "working set" in why
    g.release(("A", "c2"))
    assert g.held_bytes() == 30
    assert g.held_bytes(exclude=("A", "c1")) == 0  # a retried chunk never counts against itself
    held.clear()
    assert g.try_reserve(("C", "big"), 80)[0]  # nothing held: a chunk larger than the cap runs alone
    g.release(("C", "big"))
    free["v"] = 100
    ok, why = g.try_reserve(("C", "c1"), 90)
    assert not ok and "disk guard" in why
    # a chunk's own files already on disk never count against it; only what its download adds is checked
    free["v"] = 10**12
    held.update({("D", "c1"): 40, run.OTHER_KEY: 10})
    assert g.try_reserve(("D", "c1"), 45)[0]  # adds 5: 10 + 5 <= 50
    assert g.reserved[("D", "c1")] == 45 and g.held_bytes() == 55
    g.release(("D", "c1"))
    assert not g.try_reserve(("E", "c1"), 45)[0]  # 50 held + 45 > 50


def test_blocked_download_is_rechecked_against_the_files_on_disk(site_env: Path) -> None:  # noqa: F811
    stale = {"v": True}

    def held(verify: bool = False) -> dict[Any, int]:
        # the journal still lists 100 GB of files that were deleted by hand; stat-verified, nothing is held
        return {} if verify else ({run.OTHER_KEY: 100} if stale["v"] else {})

    r = FakeStreamRunner()
    guard = run.WorksetGuard(50, held)
    assert _sched(_plans(["A"], ["chunks"]), r, {"A": _chunks("c1", nbytes=10)}, guard=guard).run() == {"A": "ok"}
    assert r.stages_of("A", "c1") == list(CS)


# ---------------------------------------------------------------- resume

def test_resume_mid_chunk_starts_at_the_stage_after_the_journal_status(site_env: Path) -> None:  # noqa: F811
    r = FakeStreamRunner()
    idx = run.CHUNK_STATUS_ORDER.index("uploaded")
    chunks = {"A": [run.ChunkRun("c1", 1, idx), run.ChunkRun("c2", 1, 0), run.ChunkRun("c0", 1, len(CS))]}
    assert _sched(_plans(["A"]), r, chunks).run() == {"A": "ok"}
    assert r.stages_of("A", "c1") == ["apply", "history", "verify", "delete"]
    assert r.stages_of("A", "c2") == list(CS)
    assert r.stages_of("A", "c0") == []


def test_chunk_resume_index_rules() -> None:
    assert run.chunk_resume_index("planned") == 0
    assert run.chunk_resume_index(None) == 0
    assert run.chunk_resume_index("downloaded") == 1
    assert run.chunk_resume_index("recorded") == CS.index("verify")
    assert run.chunk_resume_index("spooled") == CS.index("verify")
    assert run.chunk_resume_index("deleted") == len(CS)
    ok = {"status": "ok"}
    rows = {"download": ok, "stack": ok, "avus": {"status": "failed"}}
    assert run.chunk_resume_index("failed", rows) == CS.index("avus")
    assert run.chunk_resume_index("failed", None) == 0
    # a failed chunk is never complete, whatever the orchestrator saw before
    assert run.chunk_resume_index("failed", {s: ok for s in CS}) == len(CS) - 1
    # site/chunks.py's `done` (the last status reached) wins over the orchestrator's own rows
    assert run.chunk_resume_index("failed", rows, done="uploaded") == CS.index("apply")
    assert run.chunk_resume_index("failed", None, done="planned") == 0
    assert run.chunk_resume_index("failed", None, done="deleted") == len(CS) - 1
    assert run.chunk_resume_index("failed", rows, done="bogus") == CS.index("avus")
    assert run.chunk_resume_index("stacked", rows, done="uploaded") == CS.index("avus")  # status decides


def test_failed_chunk_stops_the_site_and_the_next_run_resumes_it(site_env: Path) -> None:  # noqa: F811
    _journal("A", [("DP1.1__2024-01_2024-01", 1, "planned"), ("DP1.1__2024-02_2024-02", 1, "planned")])
    _journal("B", [("DP1.1__2024-01_2024-01", 1, "planned")])
    store = _store()
    c1 = "DP1.1__2024-01_2024-01"
    cb = journal_writer({})
    r1 = FakeStreamRunner({("A", c1, "stack"): lambda: 2}, journal=cb)
    cfg = run.make_config(stream=True)
    sel = [{"site": "A"}, {"site": "B"}]
    plans = run.build_stream_plans(sel, store, cfg)
    assert [p.stages for p in plans] == [list(run.STREAM_STAGES)] * 2
    res = _sched(plans, r1, None, store, load_chunks=lambda s: run.load_chunk_runs(s, store)).run()
    assert res == {"A": "failed", "B": "ok"}
    assert ("A", None, "finalize") not in r1.started and ("B", None, "cleanup") in r1.started
    assert _journal_status("A")[c1] == "failed"
    crow = store.chunk_rows("A")[c1]
    assert crow["stack"]["status"] == "failed" and crow["stack"]["rc"] == 2 and "exit 2" in crow["stack"]["info"]
    assert f"chunk {c1} stack failed" in store.rows()["A"]["chunks"]["info"]

    # next invocation: plan/list/chunk-plan are ok, A resumes its chunk loop; the failed chunk restarts at stack
    plans = run.build_stream_plans(sel, store, cfg)
    assert plans[0].stages == ["restore", "chunks", "finalize", "cleanup"] and plans[1].stages == []
    assert plans[0].chunks == {"total": 2, "done": 0, "started": 2}
    r2 = FakeStreamRunner(journal=cb)
    res = _sched(plans, r2, None, store, load_chunks=lambda s: run.load_chunk_runs(s, store)).run()
    assert res == {"A": "ok", "B": "nothing to do"}
    assert r2.stages_of("A", c1) == list(CS[CS.index("stack"):])
    assert store.chunk_rows("A")[c1]["stack"]["attempt"] == 2
    assert set(_journal_status("A").values()) == {"deleted"}
    assert [st for s, c, st in r2.started if c is None] == ["restore", "finalize", "cleanup"]


def test_plan_stream_site_resume_rules() -> None:
    cfg = run.make_config(stream=True)
    ok = {"status": "ok", "mode": "full"}
    done = [run.ChunkRun("c1", 1, len(CS))]
    todo = [run.ChunkRun("c1", 1, len(CS)), run.ChunkRun("c2", 1, 3)]
    assert run.plan_stream_site("A", {}, cfg).stages == list(run.STREAM_STAGES)
    assert run.STREAM_STAGES[0] == "restore"
    pre_ok = {s: dict(ok) for s in run.PRE_STAGES}
    # restore (journal backup + site lake from the Data Store) runs first whenever the site has work
    assert run.plan_stream_site("A", pre_ok, cfg, todo).stages == ["restore", "chunks", "finalize", "cleanup"]
    # every chunk deleted but the loop's row never finished -> the loop (a no-op) then finalize, cleanup
    assert run.plan_stream_site("A", pre_ok, cfg, done).stages == ["restore", "chunks", "finalize", "cleanup"]
    all_ok = {s: dict(ok) for s in run.STREAM_STAGES}
    assert run.plan_stream_site("A", all_ok, cfg, done).stages == []
    assert run.plan_stream_site("A", {s: dict(ok) for s in run.STREAM_STAGES if s != "restore"}, cfg,
                                done).stages == []  # restore alone is never a reason to run
    # new chunks after a finished site (a re-list) -> the loop, finalize and cleanup run again
    assert run.plan_stream_site("A", all_ok, cfg, todo).stages == ["restore", "chunks", "finalize", "cleanup"]
    assert run.plan_stream_site("A", all_ok | {"cleanup": {"status": "failed", "mode": "full"}}, cfg,
                                done).stages == ["restore", "cleanup"]
    # a list done in sample mode never satisfies a full run
    sampled = pre_ok | {"list": {"status": "ok", "mode": "sample:2"}}
    assert run.plan_stream_site("A", sampled, cfg, done).stages[:2] == ["restore", "list"]
    # --from-stage and --stages
    assert run.plan_stream_site("A", all_ok, run.make_config(stream=True, from_stage="finalize"),
                                done).stages == ["restore", "finalize", "cleanup"]
    assert run.plan_stream_site("A", all_ok, run.make_config(stream=True, from_stage="restore"),
                                done).stages == list(run.STREAM_STAGES)
    only = run.make_config(stream=True, stages="plan,list,chunk-plan")
    assert run.plan_stream_site("A", {}, only).stages == ["plan", "list", "chunk-plan"]


def test_finalize_refuses_while_chunks_are_not_deleted(site_env: Path) -> None:  # noqa: F811
    store = _store()
    r = FakeStreamRunner()
    chunks = {"A": [run.ChunkRun("c1", 1, len(CS)), run.ChunkRun("c2", 1, 2)]}
    res = _sched(_plans(["A"], ["finalize", "cleanup"]), r, chunks, store).run()
    assert res == {"A": "failed"}
    assert r.started == []
    row = store.rows()["A"]["finalize"]
    assert row["status"] == "failed" and "1 of 2 chunk(s) are not deleted yet" in row["info"] and "c2" in row["info"]


def test_site_level_stages_run_in_order_and_a_site_without_chunks_goes_to_finalize(site_env: Path) -> None:  # noqa: F811
    store = _store()
    r = FakeStreamRunner()
    res = _sched(_plans(["A"], list(run.STREAM_STAGES)), r, {"A": []}, store).run()
    assert res == {"A": "ok"}
    assert r.started == [("A", None, s) for s in ("restore", "plan", "list", "chunk-plan", "finalize", "cleanup")]
    assert store.rows()["A"]["chunks"]["status"] == "ok"


def test_signal_stops_mid_loop_and_leaves_the_loop_pending(site_env: Path) -> None:  # noqa: F811
    store = _store()
    started, release = threading.Event(), threading.Event()

    def slow_stack() -> int:
        started.set()
        release.wait(5)
        return 0

    r = FakeStreamRunner({("A", "c1", "stack"): slow_stack})
    plans = _plans(["A"])
    for p in plans:
        store.set_pending(p.site, p.stages)
    sched = _sched(plans, r, {"A": _chunks("c1", "c2")}, store)
    out: dict[str, str] = {}
    t = threading.Thread(target=lambda: out.update(sched.run()))
    t.start()
    assert started.wait(5)
    sched.on_signal(signal.SIGINT, None)
    release.set()
    t.join(5)
    assert not t.is_alive()
    assert out == {"A": "stopped"}
    assert ("A", "c1", "upload") not in r.started
    row = store.rows()["A"]["chunks"]
    assert row["status"] == "pending" and "stopped" in row["info"]
    assert store.chunk_rows("A")["c1"]["stack"]["status"] == "ok"
    assert not r.killed.is_set()
    sched.on_signal(signal.SIGINT, None)  # a second signal kills children
    assert r.killed.is_set()


def test_direct_history_finalize_excludes_chunk_history_spool_does_not(site_env: Path) -> None:  # noqa: F811
    assert run.stream_classes("finalize", None, "direct") == ("finalize", "mesa")
    assert run.stream_classes("finalize", None, "spool") == ("finalize",)
    assert run.stream_classes("history", "c1", "direct") == ("mesa",)
    assert run.stream_classes("history", "c1") == ("local",)  # spool (the stream default) never opens MESA
    fin_started = threading.Event()

    def slow_finalize() -> int:
        fin_started.set()
        time.sleep(0.15)
        return 0

    r = FakeStreamRunner({("A", None, "finalize"): slow_finalize})
    plans = [run.StreamSitePlan("A", ["finalize"]), run.StreamSitePlan("B", ["chunks"])]
    res = _sched(plans, r, {"A": [], "B": _chunks("c1")}, history_mode="direct").run()
    assert res == {"A": "ok", "B": "ok"}
    fa, fb = r.span("A", None, "finalize")
    ha, hb = r.span("B", "c1", "history")
    assert hb <= fa or ha >= fb  # never overlapping under the direct backend


def test_stream_prep_checks_the_mesa_lock_for_direct_history_only(site_env: Path,  # noqa: F811
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda: [{"pid": "77", "cmd": "mesa-mcp"}])
    monkeypatch.setenv(run.ENV_MULTIWRITER, "1")  # direct is only allowed in stream mode with multi-writer MESA
    direct = run.make_stream_prep(run.make_config(stream=True, history_mode="direct"))
    out = direct("A", "history", "c1")
    assert out is not None and out.status == "retry" and "pid 77" in out.info  # retried later, not failed
    assert direct("A", "finalize", None) is not None
    assert direct("A", "upload", "c1") is None and direct("A", "cleanup", None) is None
    spool = run.make_stream_prep(run.make_config(stream=True, history_mode="spool"))
    assert spool("A", "history", "c1") is None and spool("A", "finalize", None) is None


def test_recover_stale_chunk_rows(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    store = _store()
    store.start("A", "upload", "logs/run/A/chunks/c1/upload.log", "full", chunk="c1")
    store.note_pid("A", "upload", 424242, "c1")
    monkeypatch.setattr(run, "_alive", lambda pid: True)
    with pytest.raises(SystemExit, match="chunk c1 upload of A is still running as pid 424242"):
        store.recover_stale()
    monkeypatch.setattr(run, "_alive", lambda pid: False)
    assert store.recover_stale() == [("A", "chunk c1 upload")]
    row = store.chunk_rows("A")["c1"]["upload"]
    assert row["status"] == "failed" and "interrupted" in row["info"] and "pids" not in row["info"]
    # a new pass from download forgets the later stages but keeps the download attempt count
    store.start("A", "download", "x", "full", chunk="c1")
    store.finish("A", "download", "failed", 1, chunk="c1")
    store.reset_chunk("A", "c1")
    assert set(store.chunk_rows("A")["c1"]) == {"download"}
    assert store.start("A", "download", "x", "full", chunk="c1") == 2


# ---------------------------------------------------------------- journal readers

def test_journal_chunks_load_chunk_runs_and_held_bytes(site_env: Path) -> None:  # noqa: F811
    store = _store()
    assert run.journal_chunks("SRER") == [] and run.held_chunk_bytes() == {}
    _journal("SRER", [("DP1.1__2024-02_2024-02", 20, "uploaded"), ("DP1.1__2024-01_2024-01", 10, "deleted"),
                      ("DP1.1__2024-03_2024-03", 30, "planned"), ("DP1.1__2024-04_2024-04", 40, "failed")])
    store.start("SRER", "download", "x", "full", chunk="DP1.1__2024-04_2024-04")
    store.finish("SRER", "download", "ok", 0, chunk="DP1.1__2024-04_2024-04")
    ids = [r["chunk_id"] for r in run.journal_chunks("SRER")]
    assert ids == sorted(ids)
    runs = {c.chunk_id: c for c in run.load_chunk_runs("SRER", store)}
    assert runs["DP1.1__2024-01_2024-01"].complete
    assert runs["DP1.1__2024-02_2024-02"].stage == "apply"
    assert runs["DP1.1__2024-03_2024-03"].stage == "download"
    assert runs["DP1.1__2024-04_2024-04"].stage == "stack"
    # held: chunks between download and delete; the journal's on-disk bytes when site_files has them
    con = state.connect(state.DB_PATH)
    from neon_ducklake.site import evict

    evict.ensure_schema(con)
    for name, month, size, ev in [("a", "2024-02", 7, 0), ("b", "2024-02", 5, 1)]:
        con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status, dl_bytes, local_evicted) "
                    "VALUES ('SRER','DP1.00001.001',?,?,?,'ok',?,?)", (month, name, size, size, ev))
    con.close()
    # by owner: the failed chunk has no file on disk; evicted files never count
    assert run.held_chunk_bytes() == {("SRER", "DP1.1__2024-02_2024-02"): 7}
    # raw files of no chunk (a whole-site download, a kept sample probe) count too, under OTHER_KEY
    con = state.connect(state.DB_PATH)
    rel = "sites/SRER/raw/DP1.00009.001/2023-01/z.csv"
    (site_env / rel).parent.mkdir(parents=True, exist_ok=True)
    (site_env / rel).write_bytes(b"z" * 11)
    con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status, dl_bytes, local_path) "
                "VALUES ('SRER','DP1.00009.001','2023-01','z.csv',11,'ok',11,?)", (rel,))
    con.close()
    assert run.held_chunk_bytes() == {("SRER", "DP1.1__2024-02_2024-02"): 7, run.OTHER_KEY: 11}
    # verify stats the files: journal rows whose file is gone do not count
    assert run.held_chunk_bytes(verify=True) == {run.OTHER_KEY: 11}
    # with site/chunks.py's `done` column a failed chunk resumes after the last status it reached
    con = state.connect(state.DB_PATH)
    con.execute("ALTER TABLE stream_chunks ADD COLUMN done TEXT NOT NULL DEFAULT 'planned'")
    con.execute("UPDATE stream_chunks SET done='avus' WHERE chunk_id='DP1.1__2024-04_2024-04'")
    con.close()
    runs = {c.chunk_id: c for c in run.load_chunk_runs("SRER", store)}
    assert runs["DP1.1__2024-04_2024-04"].stage == "upload"
    assert runs["DP1.1__2024-02_2024-02"].stage == "apply"


# ---------------------------------------------------------------- commands, runner, config

def test_stream_commands_map_to_cli_commands() -> None:
    cmd = {s: [a[3:] for a in run.stream_commands("SRER", s, python="py")] for s in run.STREAM_STAGES}
    assert cmd["plan"] == [["site", "plan", "SRER"]]
    assert cmd["list"] == [["site", "list", "SRER"]]
    assert cmd["chunk-plan"] == [["site", "chunk-plan", "SRER"]]
    assert cmd["chunks"] == []
    assert cmd["finalize"] == [["site", "finalize", "SRER"]]
    assert cmd["cleanup"] == [["site", "cleanup", "SRER"]]
    assert run.stream_commands("SRER", "list", sample=2, threads=4)[0][3:] == \
        ["site", "list", "SRER", "--sample", "2", "--threads", "4"]
    assert run.stream_commands("SRER", "upload", "DP1.1__2024-01_2024-03")[0][3:] == \
        ["site", "chunk", "upload", "SRER", "--chunk", "DP1.1__2024-01_2024-03"]
    # --threads reaches list, the chunk download/upload/apply stages and finalize
    assert run.stream_commands("SRER", "download", "C", threads=12)[0][-2:] == ["--threads", "12"]
    assert run.stream_commands("SRER", "apply", "C", threads=12)[0][-2:] == ["--threads", "12"]
    assert "--threads" not in run.stream_commands("SRER", "stack", "C", threads=12)[0]
    assert run.stream_commands("SRER", "finalize", threads=12)[0][3:] == ["site", "finalize", "SRER", "--threads", "12"]
    assert "--threads" not in run.stream_commands("SRER", "cleanup", threads=12)[0]
    with pytest.raises(ValueError):
        run.stream_commands("SRER", "evict", "c1")


def test_every_stream_command_parses_with_the_real_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    import neon_ducklake.site as site_pkg

    seen: list[tuple[Any, ...]] = []
    monkeypatch.setattr(site_pkg, "dispatch",
                        lambda a: seen.append((a.step, a.site, a.chunk_site, a.chunk, a.sample)) or 0)
    for s in run.STREAM_STAGES:
        for argv in run.stream_commands("XYZW", s, sample=2, threads=4):
            assert cli.main(argv[3:]) == 0, argv
    for s in CS:
        assert cli.main(run.stream_commands("XYZW", s, "DP1.1__2024-01_2024-02", threads=4)[0][3:]) == 0
    assert ("list", "XYZW", None, None, 2) in seen and ("chunk-plan", "XYZW", None, None, None) in seen
    assert ("chunk", "download", "XYZW", "DP1.1__2024-01_2024-02", None) in seen
    assert ("finalize", "XYZW", None, None, None) in seen and ("cleanup", "XYZW", None, None, None) in seen
    assert ("restore", "XYZW", None, None, None) in seen
    assert len(seen) == 6 + len(CS)


def test_subprocess_runner_passes_stream_env_and_logs_per_chunk(site_env: Path,  # noqa: F811
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    prog = ("import os; print('env', os.environ.get('NEON_DUCKLAKE_CHUNK_BYTES'), "
            "os.environ.get('NEON_DUCKLAKE_WORKSET_BYTES'), os.environ.get('NEON_DUCKLAKE_HISTORY_MODE'))")
    monkeypatch.setattr(run, "stream_commands", lambda site, stage, chunk=None, **kw: [[sys.executable, "-c", prog]])
    cfg = run.make_config(stream=True, chunk_bytes=4 * 10**9, workset_bytes=20 * 10**9, history_mode="spool")
    spawned: list[tuple[Any, ...]] = []
    r = run.SubprocessRunner(cfg, on_spawn=lambda *a: spawned.append(a))
    assert r("S1", "download", "DP1.1__2024-01_2024-01") == 0
    text = (site_env / "logs/run/S1/chunks/DP1.1__2024-01_2024-01/download.log").read_text()
    assert "env 4000000000 20000000000 spool" in text and "=== exit 0" in text
    assert spawned[0][:2] == ("S1", "download") and spawned[0][3] == "DP1.1__2024-01_2024-01"
    assert r("S1", "finalize") == 0
    assert (site_env / "logs/run/S1/finalize.log").exists() and len(spawned[1]) == 3


def test_make_config_stream_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(run.ENV_WORKSET_BYTES, raising=False)
    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    monkeypatch.delenv(run.ENV_MULTIWRITER, raising=False)
    cfg = run.make_config(stream=True)
    assert cfg.selected() == list(run.STREAM_STAGES) and cfg.workset == run.WORKSET_BYTES_DEFAULT
    assert cfg.history == "spool"
    assert cfg.child_env() == {run.ENV_WORKSET_BYTES: str(run.WORKSET_BYTES_DEFAULT), run.ENV_HISTORY_MODE: "spool",
                               run.ENV_SITE_CLAIMS: "off",  # no claim unless run takes one or its caller holds it
                               run.ENV_REQUIRE_CLAIM: "0"}
    # stream mode deletes chunks after their Data Store copy verifies: --no-evict / --restack are whole-site
    assert run.make_config(stream=True, sample=2).selected()[-1] == "cleanup"
    with pytest.raises(SystemExit, match="--no-stream --no-evict"):
        run.make_config(stream=True, evict=False)
    with pytest.raises(SystemExit, match="--no-stream --restack"):
        run.make_config(stream=True, restack=True)
    with pytest.raises(SystemExit, match="per-chunk stages"):
        run.make_config(stream=True, stages="plan,verify")
    with pytest.raises(SystemExit, match="--chunks-in-flight"):
        run.make_config(stream=True, chunk_depth=0)
    with pytest.raises(SystemExit, match="--history-mode"):
        run.make_config(stream=True, history_mode="bogus")
    monkeypatch.setenv(run.ENV_WORKSET_BYTES, "20G")
    monkeypatch.setenv(run.ENV_HISTORY_MODE, "spool")
    cfg = run.make_config(stream=True, chunk_bytes=10**9)
    assert cfg.workset == 20 * 10**9 and cfg.history == "spool"
    assert cfg.child_env() == {run.ENV_CHUNK_BYTES: str(10**9), run.ENV_WORKSET_BYTES: str(20 * 10**9),
                               run.ENV_HISTORY_MODE: "spool", run.ENV_SITE_CLAIMS: "off", run.ENV_REQUIRE_CLAIM: "0"}
    cfg.claims = True  # run takes the site claims ...
    assert cfg.child_env()[run.ENV_SITE_CLAIMS] == "on" and cfg.child_env()[run.ENV_REQUIRE_CLAIM] == "1"
    cfg.claims, cfg.claims_held = False, True  # ... or its caller (the worker) holds them
    assert cfg.child_env()[run.ENV_SITE_CLAIMS] == "on" and cfg.child_env()[run.ENV_REQUIRE_CLAIM] == "1"
    assert run.ENV_SITE_CLAIMS not in run.make_config().child_env()  # whole-site mode never claims
    monkeypatch.setenv(run.ENV_HISTORY_MODE, "sideways")
    with pytest.raises(SystemExit, match="NEON_DUCKLAKE_HISTORY_MODE"):
        run.make_config(stream=True)
    assert run.parse_size("8G") == 8 * 10**9 and run.parse_size("1GiB") == 2**30 and run.parse_size("123") == 123
    with pytest.raises(ValueError):
        run.parse_size("lots")


# ---------------------------------------------------------------- entry points

def test_main_stream_end_to_end_resumes_and_reports(site_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                                                    capsys: pytest.CaptureFixture[str],
                                                    ds_coord: types.SimpleNamespace) -> None:
    ids = ["DP1.1__2024-01_2024-01", "DP1.1__2024-02_2024-03"]
    cb = journal_writer({"SRER": [(ids[0], 10), (ids[1], 20)]})
    calls: list[tuple[str, str | None, str]] = []

    def fake_call(self: run.SubprocessRunner, site: str, stage: str, chunk: str | None = None) -> int:
        assert self.cfg.stream and self.cfg.child_env()[run.ENV_HISTORY_MODE] == "spool"
        calls.append((site, chunk, stage))
        cb(site, chunk, stage, 0)
        return 0

    monkeypatch.setattr(run.SubprocessRunner, "__call__", fake_call)
    monkeypatch.setattr(run, "free_disk", lambda: 10**15)
    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    assert run.run_sites(["srer"], history_mode="spool") == 0  # the worker's entry point
    assert calls[0] == ("SRER", None, "restore") and calls[1] == ("SRER", None, "plan")
    assert calls[-1] == ("SRER", None, "cleanup")
    assert len(calls) == 6 + 2 * len(CS)
    assert ds_coord.claims == [] and ds_coord.done == []  # the worker holds the claim and writes .done itself
    assert ds_coord.pushes == ["SRER"] * (3 * 2 + 2)  # apply + history + delete per chunk, finalize, cleanup
    assert len(ds_coord.published) == 1  # this VM's ontology export, after finalize
    calls.clear()
    # a second invocation (plain run, claims on) has nothing to do, but the site has no .done marker yet (say the
    # worker died before writing it): run claims the site, checks the journal backup and writes the marker
    assert run.main(["SRER"], poll_s=0.01, history_mode="spool") == 0
    assert calls == [] and ds_coord.claims == ["SRER"] and ds_coord.released == ["SRER"]
    assert ds_coord.restore_checks == ["SRER"] and [d[0] for d in ds_coord.done] == ["SRER"]
    assert "SRER  marked done" in capsys.readouterr().out
    # a third one finds the marker (SiteDone): nothing to do, no claim taken
    assert run.main(["SRER"], poll_s=0.01, history_mode="spool") == 0
    assert calls == [] and ds_coord.claims == ["SRER"] and len(ds_coord.done) == 1
    rows = run.Store(state.connect(state.DB_PATH)).rows()["SRER"]
    assert all(rows[s]["status"] == "ok" for s in run.STREAM_STAGES)
    rep = run.collect_status(state.connect(state.DB_PATH))
    assert rep["sites"]["SRER"]["chunks"]["done"] == 2 and rep["sites"]["SRER"]["chunks"]["total"] == 2
    out = capsys.readouterr().out
    assert "nothing to do" in out
    # dry run lists the chunk commands and changes nothing
    assert run.main(["SRER"], dry_run=True, from_stage="chunk-plan") == 0
    out = capsys.readouterr().out
    assert "stream (chunks <=" in out and "site chunk download SRER --chunk <ID>" in out
    assert "neon-ducklake site finalize SRER" in out and "[finalize  ]" in out and "history spool" in out
    assert "history  [local     ]" in out and "Data Store site claims on" in out


def test_main_stream_runs_every_stage_in_order(site_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                                               ds_coord: types.SimpleNamespace) -> None:
    ids = ["DP1.1__2024-01_2024-01", "DP1.1__2024-02_2024-03"]
    cb = journal_writer({"SRER": [(ids[0], 10), (ids[1], 20)]})
    calls: list[tuple[str, str | None, str]] = []

    def fake_call(self: run.SubprocessRunner, site: str, stage: str, chunk: str | None = None) -> int:
        calls.append((site, chunk, stage))
        cb(site, chunk, stage, 0)
        return 0

    monkeypatch.setattr(run.SubprocessRunner, "__call__", fake_call)
    monkeypatch.setattr(run, "free_disk", lambda: 10**15)
    assert run.main(["SRER"], poll_s=0.01) == 0
    site_level = [st for _s, c, st in calls if c is None]
    assert site_level == ["restore", "plan", "list", "chunk-plan", "finalize", "cleanup"]
    # plain `run` claims the site on the Data Store, marks it done and releases the claim
    assert ds_coord.claims == ["SRER"] and ds_coord.released == ["SRER"] and [d[0] for d in ds_coord.done] == ["SRER"]
    for cid in ids:
        assert [st for _s, c, st in calls if c == cid] == list(CS)
    assert calls[-1] == ("SRER", None, "cleanup")
    assert json.loads((site_env / "logs/status.json").read_text())["sites"]["SRER"]["chunks"]["done"] == 2


def test_run_sites_is_main_with_stream_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    got: dict[str, Any] = {}
    monkeypatch.setattr(run, "main", lambda sites, **kw: got.update(sites=sites, kw=kw) or 0)
    assert run.run_sites(("ARIK",), history_mode="spool", chunk_bytes=5, workset_bytes=7) == 0
    assert got["sites"] == ["ARIK"]
    assert got["kw"] == {"stream": True, "sample": None, "stages": None, "chunk_bytes": 5, "workset_bytes": 7,
                         "history_mode": "spool", "claims": False, "stop_event": None, "claims_held": True}


# ---------------------------------------------------------------- status

def test_status_shows_chunk_progress_local_bytes_and_spool_backlog(site_env: Path,  # noqa: F811
                                                                   capsys: pytest.CaptureFixture[str]) -> None:
    _journal("SRER", [("DP1.1__2024-01_2024-01", 1, "deleted"), ("DP1.1__2024-02_2024-02", 1, "deleted"),
                      ("DP1.1__2024-03_2024-03", 1, "uploaded"), ("DP1.1__2024-04_2024-04", 1, "failed"),
                      ("DP1.1__2024-05_2024-05", 1, "planned")])
    con = state.connect(state.DB_PATH)
    store = run.Store(con)
    for s in ("plan", "list", "chunk-plan"):
        store.start("SRER", s, f"logs/run/SRER/{s}.log", "full")
        store.finish("SRER", s, "ok", 0)
    store.start("SRER", "chunks", "logs/run/SRER/chunks", "full")
    store.start("SRER", "apply", "logs/run/SRER/chunks/DP1.1__2024-03_2024-03/apply.log", "full",
                chunk="DP1.1__2024-03_2024-03")
    store.start("SRER", "stack", "logs/run/SRER/chunks/DP1.1__2024-04_2024-04/stack.log", "full",
                chunk="DP1.1__2024-04_2024-04")
    store.finish("SRER", "stack", "failed", 3, "exit 3", chunk="DP1.1__2024-04_2024-04")
    con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status, dl_bytes) VALUES "
                "('SRER','DP1.00001.001','2024-03','x.csv',2000000000,'ok',2000000000)")
    con.execute("INSERT INTO batches(batch_id, scope, status, n_rows) VALUES ('b1','SRER','spooled', 100), "
                "('b2','SRER','spooled', 50), ('b3','SRER','recorded', 10), ('b4','SRER','spooling', 5)")
    rep = run.collect_status(con)
    con.close()
    s = rep["sites"]["SRER"]
    assert s["chunks"]["total"] == 5 and s["chunks"]["done"] == 2 and s["chunks"]["failed"] == 1
    assert s["chunks"]["inProgress"] == 1 and s["chunks"]["planned"] == 1
    assert s["localBytes"] == 2 * 10**9 and rep["localBytesAllSites"] == 2 * 10**9
    assert rep["spool"] == {"batches": 2, "rows": 150, "byScope": {"SRER": {"batches": 2, "rows": 150}},
                            "spooling": 1}
    assert s["spooledBatches"] == 2 and rep["totals"]["chunks"] == 5 and rep["totals"]["chunksDone"] == 2
    assert s["runningChunks"][0]["stage"] == "apply" and s["failedChunks"][0]["rc"] == 3
    assert s["stages"]["chunk-plan"]["status"] == "ok" and s["stages"]["chunks"]["status"] == "running"
    text = run.render_status(rep)
    assert " cpl" in text and " chk" in text and "2/5" in text and "heldGB" in text
    assert "spool backlog: 2 batch(es), 150 row(s)" in text and "(1 interrupted upload(s))" in text
    assert "SRER chunk DP1.1__2024-04_2024-04 stack failed (attempt 1, rc 3): exit 3" in text
    assert "stale (orchestrator not running; the next run retries it): SRER chunk DP1.1__2024-03_2024-03 apply" in text
    assert "running: SRER chunk DP1.1__2024-03_2024-03 apply" in run.render_status(rep | {"orchestratorRunning": True})
    assert " dl " not in text.splitlines()[1]  # no whole-site columns for a stream-only journal
    assert run.status_main(["SRER"], as_json=True) == 0
    assert json.loads(capsys.readouterr().out)["spool"]["batches"] == 2


# ---------------------------------------------------------------- CLI: site stream steps

def _fake_module(monkeypatch: pytest.MonkeyPatch, name: str, **attrs: Any) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, name, mod)
    return mod


def test_cli_site_chunk_finalize_cleanup_and_chunk_plan(monkeypatch: pytest.MonkeyPatch,
                                                        capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[Any, ...]] = []

    def stage_fn(stage: str) -> Callable[[str, str], int]:
        return lambda site, chunk_id: calls.append((stage, site, chunk_id)) or 0

    def download_chunk(site: str, chunk_id: str, *, threads: int = 8) -> int:
        calls.append(("download", site, chunk_id, threads))
        return 0

    def finalize_site(site: str, *, threads: int = 8) -> int:
        calls.append(("finalize", site, threads))
        return 0

    _fake_module(monkeypatch, "neon_ducklake.site.stream",
                 **{f"{s}_chunk": stage_fn(s) for s in CS} | {"download_chunk": download_chunk},
                 finalize_site=finalize_site,
                 cleanup_site=lambda site: calls.append(("cleanup", site)) or 3)
    chunk_t = types.SimpleNamespace

    def plan_chunks(site: str, **kw: Any) -> list[Any]:
        calls.append(("plan_chunks", site, kw))
        return [chunk_t(site=site, chunk_id="DP1.1__2024-01_2024-02", product="DP1.1", months=["2024-01", "2024-02"],
                        n_files=3, bytes=2 * 10**9)]

    _fake_module(monkeypatch, "neon_ducklake.site.chunks", plan_chunks=plan_chunks)
    assert cli.main(["site", "chunk", "download", "srer", "--chunk", "DP1.1__2024-01_2024-02", "--threads", "12"]) == 0
    assert calls[-1] == ("download", "SRER", "DP1.1__2024-01_2024-02", 12)
    assert cli.main(["site", "chunk", "history_chunk", "SRER", "--chunk", "X", "--threads", "12"]) == 0
    assert calls[-1] == ("history", "SRER", "X")  # a stage without `threads` never gets it
    assert cli.main(["site", "finalize", "srer"]) == 0 and calls[-1] == ("finalize", "SRER", 8)
    assert cli.main(["site", "cleanup", "SRER"]) == 3 and calls[-1] == ("cleanup", "SRER")
    assert cli.main(["site", "chunk-plan", "srer", "--chunk-bytes", "4G", "--products", "DP1.1"]) == 0
    assert calls[-1] == ("plan_chunks", "SRER", {"products": ["DP1.1"], "months": None, "max_bytes": 4 * 10**9})
    assert "SRER: 1 chunk(s) over 1 product(s), 3 file(s), 2.0 GB" in capsys.readouterr().out
    assert cli.main(["site", "chunk-plan", "SRER"]) == 0
    assert calls[-1] == ("plan_chunks", "SRER", {"products": None, "months": None})
    for argv, msg in [
        (["site", "chunk", "download", "SRER"], "--chunk ID"),
        (["site", "chunk", "download", "--chunk", "X"], "SITE is missing"),
        (["site", "chunk", "evict", "SRER", "--chunk", "X"], "unknown chunk stage"),
        (["site", "stack", "SRER", "--chunk", "X"], "--chunk applies"),
        (["site", "stack", "SRER", "EXTRA"], "unexpected argument"),
        (["site", "stack", "SRER", "--chunk-bytes", "1G"], "--chunk-bytes applies"),
        (["site", "chunk-plan", "SRER", "--sample", "2"], "--sample applies"),
    ]:
        with pytest.raises(SystemExit, match=msg):
            cli.main(argv)


# ---------------------------------------------------------------- CLI: run options and multi-VM commands

def test_cli_run_stream_options(monkeypatch: pytest.MonkeyPatch) -> None:
    got: dict[str, Any] = {}
    monkeypatch.setattr(run, "main", lambda sites, **kw: got.update(sites=sites, kw=kw) or 0)
    assert cli.main(["run", "SRER", "--chunk-bytes", "8G", "--workset-bytes", "50GB", "--history-mode", "spool",
                     "--chunks-in-flight", "3"]) == 0
    kw = got["kw"]
    assert kw["stream"] is True and kw["chunk_bytes"] == 8 * 10**9 and kw["workset_bytes"] == 50 * 10**9
    assert kw["history_mode"] == "spool" and kw["chunk_depth"] == 3
    assert cli.main(["run", "--all", "--no-stream", "--no-evict"]) == 0
    assert got["kw"]["stream"] is False and got["kw"]["evict"] is False and got["kw"]["chunk_bytes"] is None
    assert cli.main(["run", "--all", "--stream"]) == 0 and got["kw"]["stream"] is True
    with pytest.raises(SystemExit):
        cli.main(["run", "SRER", "--chunk-bytes", "lots"])
    with pytest.raises(SystemExit):
        cli.main(["run", "SRER", "--history-mode", "sideways"])


def test_cli_worker_recorder_claims_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    got: dict[str, Any] = {}

    def worker_main(sites: list[str] | None, habitat: str | None, max_sites: int | None, chunk_bytes: int | None,
                    workset_bytes: int | None, once: bool = False) -> int:
        got["worker"] = (sites, habitat, max_sites, chunk_bytes, workset_bytes, once)
        return 0

    def recorder_main(watch: bool = False, interval_s: int = 60, max_rows: int = 250_000) -> int:
        got["recorder"] = (watch, interval_s, max_rows)
        return 0

    def bootstrap_main(what: set[str]) -> int:
        got["bootstrap"] = what
        return 0

    _fake_module(monkeypatch, "neon_ducklake.worker", main=worker_main)
    _fake_module(monkeypatch, "neon_ducklake.avu.recorder", main=recorder_main)
    _fake_module(monkeypatch, "neon_ducklake.bootstrap", main=bootstrap_main)

    assert cli.main(["worker"]) == 0
    assert got["worker"] == (None, None, None, None, None, False)
    assert cli.main(["worker", "--sites", "srer,harv", "ARIK", "--habitat", "aquatic", "--max-sites", "2",
                     "--chunk-bytes", "4G", "--workset-bytes", "20GB", "--once"]) == 0
    assert got["worker"] == (["SRER", "HARV", "ARIK"], "aquatic", 2, 4 * 10**9, 20 * 10**9, True)
    with pytest.raises(SystemExit, match="--max-sites"):
        cli.main(["worker", "--max-sites", "0"])

    assert cli.main(["recorder"]) == 0 and got["recorder"] == (False, 60, 250_000)
    assert cli.main(["recorder", "--watch", "--interval", "30", "--max-rows", "1000"]) == 0
    assert got["recorder"] == (True, 30, 1000)
    with pytest.raises(SystemExit, match="--interval"):
        cli.main(["recorder", "--interval", "0"])
    with pytest.raises(SystemExit, match="no --dry-run"):
        cli.main(["recorder", "--dry-run"])

    def recorder_main2(*, watch: bool = False, interval: float = 60.0, max_rows: int | None = None,
                       dry_run: bool = False) -> int:  # the keyword spelling avu/recorder.py implements
        got["recorder2"] = (watch, interval, max_rows, dry_run)
        return 0

    _fake_module(monkeypatch, "neon_ducklake.avu.recorder", main=recorder_main2)
    assert cli.main(["recorder", "--watch", "--interval", "5", "--dry-run"]) == 0
    assert got["recorder2"] == (True, 5, None, True)  # max_rows left to the recorder's own default
    with pytest.raises(SystemExit, match="--max-rows"):
        cli.main(["recorder", "--max-rows", "0"])

    def claims_main2(action: str = "list", targets: list[str] | None = None, *, force: bool = False,
                     as_json: bool = False) -> int:  # the signature coord.py implements
        got["claims2"] = (action, targets, force, as_json)
        return 0

    _fake_module(monkeypatch, "neon_ducklake.coord", claims_main=claims_main2)
    assert cli.main(["claims"]) == 0 and got["claims2"] == ("list", None, False, False)
    with pytest.raises(SystemExit, match="--force applies"):
        cli.main(["claims", "--force"])
    assert cli.main(["claims", "--json"]) == 0 and got["claims2"] == ("list", None, False, True)
    assert cli.main(["claims", "--release", "srer", "--release", "dp1.10003.001", "--release", "/zone/x/.claim",
                     "--force"]) == 0
    assert got["claims2"] == ("release", ["SRER", "DP1.10003.001", "/zone/x/.claim"], True, False)

    assert cli.main(["bootstrap"]) == 0 and got["bootstrap"] == {"catalog", "ontology", "curation"}
    assert cli.main(["bootstrap", "--what", "catalog, Ontology"]) == 0 and got["bootstrap"] == {"catalog", "ontology"}
    with pytest.raises(SystemExit, match="unknown part"):
        cli.main(["bootstrap", "--what", "catalog,everything"])
    with pytest.raises(SystemExit, match="has no --force"):
        cli.main(["bootstrap", "--force"])

    def bootstrap_main2(what: set[str] | None = None, *, force: bool = False, as_json: bool = False) -> int:
        got["bootstrap2"] = (what, force, as_json)
        return 0

    _fake_module(monkeypatch, "neon_ducklake.bootstrap", main=bootstrap_main2)
    assert cli.main(["bootstrap", "--what", "curation", "--force", "--json"]) == 0
    assert got["bootstrap2"] == ({"curation"}, True, True)


# ---------------------------------------------------------------- integration with site/chunks.py

def test_chunk_plan_cli_feeds_the_orchestrator_journal_readers(site_env: Path,  # noqa: F811
                                                               capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("neon_ducklake.site.chunks")
    con = state.connect(state.DB_PATH)
    for m in ("2024-01", "2024-02", "2024-03"):
        for i in range(3):
            con.execute("INSERT INTO site_files(site, product, month, name, size) VALUES (?,?,?,?,?)",
                        ("TEST", "DP1.00001.001", m, f"f{i}.csv", 3 * 10**9))
    con.execute("INSERT INTO site_files(site, product, month, name, size) VALUES "
                "('TEST', 'DP1.00002.001', '2024-01', 'g.csv', 10)")
    con.close()
    assert cli.main(["site", "chunk-plan", "test", "--chunk-bytes", "10G"]) == 0
    assert "TEST: 4 chunk(s) over 2 product(s)" in capsys.readouterr().out
    store = _store()
    runs = run.load_chunk_runs("TEST", store)
    assert [c.chunk_id for c in runs] == ["DP1.00001.001__2024-01_2024-01", "DP1.00001.001__2024-02_2024-02",
                                          "DP1.00001.001__2024-03_2024-03", "DP1.00002.001__2024-01_2024-01"]
    assert all(c.next == 0 for c in runs) and runs[0].bytes == 9 * 10**9
    assert run.held_chunk_bytes() == {}
    plan = run.plan_stream_site("TEST", {}, run.make_config(stream=True), runs)
    assert plan.stages == list(run.STREAM_STAGES) and plan.chunks == {"total": 4, "done": 0, "started": 0}
