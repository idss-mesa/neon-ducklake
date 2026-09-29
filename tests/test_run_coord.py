"""run.py stream mode and the Data Store (docs/STREAMING.md "Orchestrator", "Coordination"): spool-only
history, the stop event of run_sites, journal pushes after every chunk and finalize step, site claims of a
plain `run`, lost claims, waiting instead of failing (MESA busy, another VM's curation, blocked downloads),
cleanup rc 3, and the working set measured by fetch.local_workset_bytes()."""

from __future__ import annotations

import contextlib
import sys
import threading
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import cli, env, run, state

from .test_run_stream import (  # noqa: F401 - ds_coord is a fixture
    CS,
    FakeStreamRunner,
    _chunks,
    _plans,
    _sched,
    _store,
    ds_coord,
    journal_writer,
)
from .test_site_common import site_env  # noqa: F401 - fixture

GB = 10**9


# ---------------------------------------------------------------- B3: stream history is always spooled

def test_stream_history_is_spool_and_direct_is_refused_without_multiwriter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    monkeypatch.delenv(run.ENV_MULTIWRITER, raising=False)
    cfg = run.make_config(stream=True)
    assert cfg.history == "spool" and cfg.child_env()[run.ENV_HISTORY_MODE] == "spool"
    with pytest.raises(SystemExit, match="NEON_DUCKLAKE_MESA_MULTIWRITER=1"):
        run.make_config(stream=True, history_mode="direct")
    monkeypatch.setenv(run.ENV_HISTORY_MODE, "direct")  # e.g. left over in the shell
    with pytest.raises(SystemExit, match="central recorder"):
        run.make_config(stream=True)
    # whole-site mode keeps direct as its default and exports nothing unless asked
    assert run.make_config().history == "direct"
    monkeypatch.delenv(run.ENV_HISTORY_MODE)
    assert run.make_config().history == "direct" and run.ENV_HISTORY_MODE not in run.make_config().child_env()
    # the DuckDB 2.0 multi-writer switch lets direct through (it stays a switch)
    monkeypatch.setenv(run.ENV_MULTIWRITER, "1")
    assert run.make_config(stream=True, history_mode="direct").child_env()[run.ENV_HISTORY_MODE] == "direct"


def test_cli_run_refuses_direct_history_in_stream_mode(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    monkeypatch.delenv(run.ENV_MULTIWRITER, raising=False)
    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    with pytest.raises(SystemExit, match="spend at least one of the 900 MESA snapshots per chunk"):
        cli.main(["run", "SRER", "--history-mode", "direct", "--dry-run"])
    assert cli.main(["run", "SRER", "--no-stream", "--history-mode", "direct", "--dry-run"]) == 0


def test_every_stream_child_gets_spool_even_when_nothing_is_set(site_env: Path,  # noqa: F811
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    prog = "import os; print('mode', os.environ.get('NEON_DUCKLAKE_HISTORY_MODE'))"
    monkeypatch.setattr(run, "stream_commands", lambda site, stage, chunk=None, **kw: [[sys.executable, "-c", prog]])
    r = run.SubprocessRunner(run.make_config(stream=True))
    assert r("S1", "history", "C1") == 0
    assert "mode spool" in (site_env / "logs/run/S1/chunks/C1/history.log").read_text()


def test_spooled_chunk_history_does_not_hold_the_mesa_class() -> None:
    assert run.stream_classes("history", "c1") == ("local",)
    assert run.stream_classes("finalize", None) == ("finalize",)
    sched = run.StreamScheduler([], lambda *a: 0, load_chunks=lambda s: [])
    assert sched.history_mode == "spool" and sched.classes("history", "c1") == ("local",)


# ---------------------------------------------------------------- B4: run_sites(stop_event=...)

def test_run_sites_stop_event_terminates_children_and_returns_130(site_env: Path,  # noqa: F811
                                                                  monkeypatch: pytest.MonkeyPatch,
                                                                  ds_coord: types.SimpleNamespace) -> None:  # noqa: F811
    ids = ["DP1.1__2024-01_2024-01", "DP1.1__2024-02_2024-02"]
    cb = journal_writer({"SRER": [(ids[0], 10), (ids[1], 10)]})
    started, killed = threading.Event(), threading.Event()
    calls: list[tuple[str, str | None, str]] = []

    def fake_call(self: run.SubprocessRunner, site: str, stage: str, chunk: str | None = None) -> int:
        calls.append((site, chunk, stage))
        if chunk == ids[0] and stage == "stack":
            started.set()
            return -15 if killed.wait(10) else 0  # a child terminated by the kill
        cb(site, chunk, stage, 0)
        return 0

    monkeypatch.setattr(run.SubprocessRunner, "__call__", fake_call)
    monkeypatch.setattr(run.SubprocessRunner, "kill", lambda self: killed.set())
    monkeypatch.setattr(run, "free_disk", lambda: 10**15)
    stop = threading.Event()
    out: dict[str, int] = {}
    t = threading.Thread(target=lambda: out.update(rc=run.run_sites(["SRER"], stop_event=stop)))
    t.start()
    assert started.wait(10)
    stop.set()  # the worker lost the site's claim
    t.join(15)
    assert not t.is_alive() and out["rc"] == 130
    assert killed.is_set()  # children terminated at once, not after they finish
    assert ("SRER", ids[0], "avus") not in calls and ("SRER", None, "finalize") not in calls
    assert ds_coord.pushes == [] and ds_coord.claims == []
    row = run.Store(state.connect(state.DB_PATH)).chunk_rows("SRER")[ids[0]]["stack"]
    assert row["status"] == "failed" and "signal 15" in row["info"]


def test_run_sites_with_the_stop_event_already_set_runs_nothing(site_env: Path,  # noqa: F811
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(run.SubprocessRunner, "__call__", lambda self, *a: calls.append(a) or 0)
    ev = threading.Event()
    ev.set()
    assert run.run_sites(["SRER"], stop_event=ev) == 130 and calls == []


# ---------------------------------------------------------------- M2: journal push cadence

def test_journal_pusher_stages_errors_and_stop() -> None:
    pushed: list[str] = []
    stop = threading.Event()
    hook = run.make_journal_pusher(stop, push=lambda s: pushed.append(s) or {"tables": {}})
    assert hook("A", "upload", "c1") is None and hook("A", "delete", None) is None and pushed == []
    for stage, chunk in (("history", "c1"), ("delete", "c1"), ("finalize", None), ("cleanup", None)):
        assert hook("A", stage, chunk) is None
    assert pushed == ["A"] * 4
    assert run.make_journal_pusher(push=lambda s: {"error": "iRODS down"})("A", "delete", "c1") == \
        "journal push failed: iRODS down"

    def boom(site: str) -> dict[str, Any]:
        raise ConnectionError("no route to host")

    assert "journal push failed: ConnectionError: no route to host" in run.make_journal_pusher(push=boom)(
        "A", "finalize", None)
    stop.set()  # the caller lost the claim: never overwrite the new owner's backup
    assert "not pushed" in (hook("A", "delete", "c1") or "") and pushed == ["A"] * 4


def test_journal_is_pushed_after_every_chunk_and_after_finalize_and_cleanup(site_env: Path) -> None:  # noqa: F811
    pushed_after: list[tuple[str, str | None]] = []
    lock = threading.Lock()
    hook = run.make_journal_pusher(push=lambda s: {})

    def after_ok(site: str, stage: str, chunk: str | None) -> str | None:
        if (stage, chunk is not None) in run.JOURNAL_PUSH_AFTER:
            with lock:
                pushed_after.append((stage, chunk))
        return hook(site, stage, chunk)

    r = FakeStreamRunner()
    assert _sched(_plans(["A"]), r, {"A": _chunks("c1", "c2")}, after_ok=after_ok).run() == {"A": "ok"}
    # N1: also after apply, so a VM taking the site over sees the applied rows (never re-applies blind)
    assert sorted(pushed_after[:-2], key=str) == sorted([(st, c) for c in ("c1", "c2")
                                                        for st in ("apply", "history", "delete")], key=str)
    assert pushed_after[-2:] == [("finalize", None), ("cleanup", None)]
    assert pushed_after.index(("apply", "c1")) < pushed_after.index(("history", "c1"))


def test_a_failed_journal_push_fails_the_chunk_stage(site_env: Path) -> None:  # noqa: F811
    store = _store()

    def push(site: str) -> dict[str, Any]:
        raise OSError("Data Store unreachable")

    r = FakeStreamRunner()
    res = _sched(_plans(["A"]), r, {"A": _chunks("c1", "c2")}, store,
                 after_ok=run.make_journal_pusher(push=push)).run()
    assert res == {"A": "failed"}
    row = store.chunk_rows("A")["c1"]["apply"]  # the first push is after apply
    assert row["status"] == "failed" and "journal push failed: OSError: Data Store unreachable" in row["info"]
    assert ("A", "c1", "history") not in r.started and ("A", None, "finalize") not in r.started


# ---------------------------------------------------------------- m6: site claims of a plain `run`

class SiteDone(Exception):
    """Stands in for coord.SiteDone (matched by class name)."""


class SiteClaimed(Exception):
    """Stands in for coord.SiteClaimed (matched by class name)."""


def test_run_claims_each_site_and_skips_done_and_claimed_sites(site_env: Path,  # noqa: F811
                                                               monkeypatch: pytest.MonkeyPatch,
                                                               ds_coord: types.SimpleNamespace) -> None:  # noqa: F811
    ds_coord.raises = {"BBBB": SiteDone("sites/BBBB/.done exists"), "CCCC": SiteClaimed("CCCC: vm-7 holds it")}
    cb = journal_writer({"AAAA": [("DP1.1__2024-01_2024-01", 10)], "DDDD": [("DP1.1__2024-01_2024-01", 10)]})
    calls: list[tuple[str, str | None, str]] = []

    def fake_call(self: run.SubprocessRunner, site: str, stage: str, chunk: str | None = None) -> int:
        calls.append((site, chunk, stage))
        cb(site, chunk, stage, 0)
        return 3 if (site, stage) == ("AAAA", "cleanup") else 0  # cleanup kept a few files: done with a warning

    guards: list[run.WorksetGuard] = []
    real_guard = run.WorksetGuard
    monkeypatch.setattr(run, "WorksetGuard", lambda *a, **kw: guards.append(real_guard(*a, **kw)) or guards[-1])
    monkeypatch.setattr(run.SubprocessRunner, "__call__", fake_call)
    monkeypatch.setattr(run, "free_disk", lambda: 10**15)
    assert run.main(["AAAA", "BBBB", "CCCC"], poll_s=0.01) == 0
    assert {c[0] for c in calls} == {"AAAA"}
    assert ds_coord.claims == ["AAAA"] and ds_coord.released == ["AAAA"]
    assert [d[0] for d in ds_coord.done] == ["AAAA"] and ds_coord.done[0][1]["by"] == "run"
    assert guards[0].total is run.local_workset_total  # the working set is fetch.local_workset_bytes()
    rows = run.Store(state.connect(state.DB_PATH)).rows()
    assert rows["AAAA"]["cleanup"]["status"] == "ok" and rows["AAAA"]["cleanup"]["rc"] == 3
    assert rows["BBBB"]["restore"]["status"] == "skipped" and ".done" in rows["BBBB"]["restore"]["info"]
    assert rows["CCCC"]["restore"]["status"] == "skipped" and "vm-7" in rows["CCCC"]["restore"]["info"]
    # --no-claims: no claim, no .done, but the journal is still pushed after every chunk
    n_push = len(ds_coord.pushes)
    assert run.main(["DDDD"], poll_s=0.01, claims=False) == 0
    assert ds_coord.claims == ["AAAA"] and len(ds_coord.done) == 1 and len(ds_coord.pushes) == n_push + 5
    # a sample run claims the site but never marks it done
    calls.clear()
    assert run.main(["EEEE"], poll_s=0.01, sample=1) == 0
    assert ds_coord.claims[-1] == "EEEE" and ds_coord.released[-1] == "EEEE" and len(ds_coord.done) == 1


def test_run_fails_a_site_whose_claim_cannot_be_taken(site_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                                                      ds_coord: types.SimpleNamespace) -> None:  # noqa: F811
    ds_coord.raises = {"AAAA": ConnectionError("iRODS unreachable")}
    calls: list[Any] = []
    monkeypatch.setattr(run.SubprocessRunner, "__call__", lambda self, *a: calls.append(a) or 0)
    assert run.main(["AAAA"], poll_s=0.01) == 1 and calls == []
    row = run.Store(state.connect(state.DB_PATH)).rows()["AAAA"]["restore"]
    assert row["status"] == "failed" and "cannot claim the site on the Data Store" in row["info"]


def test_a_lost_claim_stops_its_site_and_terminates_its_children(site_env: Path) -> None:  # noqa: F811
    store = _store()
    lost = threading.Event()
    held: dict[str, str] = {}

    @contextlib.contextmanager
    def claimer(site: str) -> Iterator[Any]:
        held[site] = "held"
        try:
            yield types.SimpleNamespace(lost=lost if site == "A" else threading.Event())
        finally:
            held[site] = "released"

    in_history, release = threading.Event(), threading.Event()

    def c2_history() -> int:  # still running when the claim is lost; finishes "ok" after the kill
        in_history.set()
        release.wait(5)
        return 0

    r = FakeStreamRunner({("A", "c2", "history"): c2_history})
    killed: list[str] = []
    r.kill_site = lambda site: (killed.append(site), release.set())  # type: ignore[attr-defined]
    pushes: list[str] = []
    marks: list[str] = []
    sched = _sched(_plans(["A", "B"]), r, {"A": _chunks("c1", "c2"), "B": _chunks("c1")}, store, chunk_depth=1,
                   claimer=claimer, after_ok=run.make_journal_pusher(push=lambda s: pushes.append(s) or {}),
                   on_site_done=marks.append)
    out: dict[str, str] = {}
    t = threading.Thread(target=lambda: out.update(sched.run()))
    t.start()
    assert in_history.wait(5)
    assert pushes.count("A") == 4  # c1 apply + history + delete, c2 apply
    lost.set()
    t.join(10)
    assert not t.is_alive()
    assert out == {"A": "claim lost", "B": "ok"}
    assert killed == ["A"]
    assert pushes.count("A") == 4  # nothing pushed over the new owner's backup
    assert not [st for s, c, st in r.started if s == "A" and c == "c2" and st in ("verify", "delete")]
    assert ("A", None, "finalize") not in r.started
    assert held == {"A": "released", "B": "released"} and marks == ["B"]
    row = store.chunk_rows("A")["c2"]["history"]
    assert row["status"] == "failed" and "claim was lost" in row["info"]


def test_subprocess_runner_kill_site_only_terminates_that_site(site_env: Path,  # noqa: F811
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    prog = "import time; time.sleep(30)"
    monkeypatch.setattr(run, "stream_commands", lambda site, stage, chunk=None, **kw: [[sys.executable, "-c", prog]])
    r = run.SubprocessRunner(run.make_config(stream=True), grace_s=1.0)
    rcs: dict[str, int] = {}
    threads = [threading.Thread(target=lambda s=s: rcs.update({s: r(s, "download", "c1")})) for s in ("A", "B")]
    for t in threads:
        t.start()
    for _ in range(200):
        if len(r._procs) == 2:
            break
        threading.Event().wait(0.02)
    r.kill_site("A")
    threads[0].join(10)
    assert rcs["A"] < 0 and threads[1].is_alive()
    assert r("A", "stack", "c1") < 0  # no new stage of a stopped site
    r.kill()
    threads[1].join(10)
    assert rcs["B"] < 0


# ---------------------------------------------------------------- retry later instead of failing

def test_whole_site_record_waits_while_mesa_is_busy_then_runs(site_env: Path,  # noqa: F811
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    busy = {"n": 3}

    def holders() -> list[dict[str, str]]:
        if busy["n"]:
            busy["n"] -= 1
            return [{"pid": "99", "cmd": "neon-ducklake recorder --watch"}]
        return []

    monkeypatch.setattr(env, "catalog_lock_holders", holders)
    store = _store()
    started: list[tuple[str, str]] = []
    msgs: list[str] = []
    gate = run.make_gate(run.make_config())
    sched = run.Scheduler([run.SitePlan("A", ["apply", "record", "verify"])],
                          lambda site, stage: started.append((site, stage)) or 0, store, poll_s=0.01,
                          out=msgs.append, gate=gate, gate_recheck_s=0.0)
    assert sched.run() == {"A": "ok"}
    assert started == [("A", "apply"), ("A", "record"), ("A", "verify")] and busy["n"] == 0
    assert any("record" in m and "waits: MESA catalog is open by another process: pid 99" in m for m in msgs)
    assert store.rows()["A"]["record"]["status"] == "ok" and store.rows()["A"]["record"]["attempt"] == 1
    # past the wait timeout the stage fails, naming the holder
    monkeypatch.setattr(env, "catalog_lock_holders", lambda: [{"pid": "98", "cmd": "mesa-mcp"}])
    sched = run.Scheduler([run.SitePlan("B", ["record"])], lambda site, stage: 0, store, poll_s=0.01,
                          out=msgs.append, gate=gate, gate_recheck_s=0.0, wait_timeout_s=0.05)
    assert sched.run() == {"B": "failed"}
    row = store.rows()["B"]["record"]
    assert row["status"] == "failed" and "pid 98" in row["info"] and "waited" in row["info"]


def test_mesa_busy_seen_by_prep_is_retried_not_failed(site_env: Path) -> None:  # noqa: F811
    store = _store()
    outcomes = iter([run.Outcome("retry", None, "MESA catalog is open by another process: pid 5")])
    ran: list[str] = []
    sched = run.Scheduler([run.SitePlan("A", ["record"])], lambda site, stage: ran.append(stage) or 0, store,
                          poll_s=0.01, out=lambda m: None, retry_delay_s=0.0,
                          prep=lambda site, stage: next(outcomes, None))
    assert sched.run() == {"A": "ok"} and ran == ["record"]
    assert store.rows()["A"]["record"]["attempt"] == 2


def test_finalize_waits_while_another_vm_curates_one_of_the_sites_products(site_env: Path,  # noqa: F811
                                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake.site import validate

    pend = {"n": 2}

    def pending(site: str) -> list[str]:
        if pend["n"]:
            pend["n"] -= 1
            return ["DP1.10003.001"]
        return []

    monkeypatch.setattr(validate, "pending_generic", pending, raising=False)
    msgs: list[str] = []
    r = FakeStreamRunner()
    res = _sched(_plans(["A"], ["finalize", "cleanup"]), r, {"A": []}, gate=run.make_gate(run.make_config(stream=True)),
                 gate_recheck_s=0.0, out=msgs.append).run()
    assert res == {"A": "ok"} and pend["n"] == 0
    assert [st for _s, _c, st in r.started] == ["finalize", "cleanup"]
    assert any("another VM is curating 1 of the site's product(s) (DP1.10003.001)" in m for m in msgs)

    def broken(site: str) -> list[str]:
        raise ConnectionError("iRODS down")

    monkeypatch.setattr(validate, "pending_generic", broken, raising=False)  # finalize's own step still checks
    assert run.make_gate(run.make_config(stream=True))("A", "finalize", None) is None


def test_blocked_download_rc3_is_retried_later_not_failed(site_env: Path) -> None:  # noqa: F811
    store = _store()
    n = {"dl": 0}

    def dl() -> int:
        n["dl"] += 1
        return 3 if n["dl"] == 1 else 0

    r = FakeStreamRunner({("A", "c1", "download"): dl})
    assert _sched(_plans(["A"], ["chunks"]), r, {"A": _chunks("c1")}, store, retry_delay_s=0.0).run() == {"A": "ok"}
    assert r.stages_of("A", "c1") == ["download", *CS]
    assert store.chunk_rows("A")["c1"]["download"]["attempt"] == 2
    # it gives up after max_retries
    r = FakeStreamRunner({("B", "c1", "download"): lambda: 3})
    res = _sched(_plans(["B"], ["chunks"]), r, {"B": _chunks("c1")}, store, retry_delay_s=0.0, max_retries=2).run()
    assert res == {"B": "failed"} and r.stages_of("B", "c1") == ["download"] * 3
    assert "gave up after 2 retries" in store.chunk_rows("B")["c1"]["download"]["info"]


def test_cleanup_rc3_is_done_with_a_warning(site_env: Path) -> None:  # noqa: F811
    store = _store()
    r = FakeStreamRunner({("A", None, "cleanup"): lambda: 3})
    assert _sched(_plans(["A"], ["finalize", "cleanup"]), r, {"A": []}, store).run() == {"A": "ok"}
    row = store.rows()["A"]["cleanup"]
    assert row["status"] == "ok" and row["rc"] == 3 and "warning" in row["info"]
    # rc 3 of another stage is still a failure
    r = FakeStreamRunner({("B", "c1", "delete"): lambda: 3})
    assert _sched(_plans(["B"], ["chunks"]), r, {"B": _chunks("c1")}, store).run() == {"B": "failed"}


# ---------------------------------------------------------------- working set = fetch.local_workset_bytes()

def test_workset_guard_counts_site_lakes_and_superseded_files() -> None:
    held = {("A", "c1"): 4 * GB}
    total = {"v": 9 * GB}  # 4 GB of chunk c1 + 5 GB of site lake / superseded files
    g = run.WorksetGuard(10 * GB, lambda: dict(held), total=lambda: total["v"])
    ok, why = g.try_reserve(("A", "c2"), 3 * GB)
    assert not ok and "4.0 GB of raw data on local disk + 5.0 GB of site lakes and other files" in why
    assert run.WorksetGuard(10 * GB, lambda: dict(held)).try_reserve(("A", "c2"), 3 * GB)[0]  # raw bytes alone fit
    # with no other chunk holding raw bytes, a lake alone never blocks the next chunk
    held.clear()
    total["v"] = 50 * GB
    assert g.try_reserve(("A", "c3"), 3 * GB)[0]
    # a measure that fails falls back to the tracked raw bytes

    def broken() -> int:
        raise OSError("stat failed")

    assert run.WorksetGuard(10 * GB, lambda: {("A", "c1"): 4 * GB}, total=broken).try_reserve(("A", "c2"), 3 * GB)[0]


def test_local_workset_total_and_status_use_fetch_local_workset_bytes(site_env: Path,  # noqa: F811
                                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake.site import fetch

    monkeypatch.setattr(fetch, "local_workset_bytes", lambda site=None: 12 * GB, raising=False)
    assert run.local_workset_total() == 12 * GB
    con = state.connect(state.DB_PATH)
    rep = run.collect_status(con)
    con.close()
    assert rep["worksetBytes"] == 12 * GB
    assert "working set on local disk: 12.0 GB (raw files incl. superseded + site lakes)" in run.render_status(rep)


# ---------------------------------------------------------------- the restore stage

def test_restore_runs_first_and_maps_to_site_restore() -> None:
    assert run.stream_commands("SRER", "restore", python="py")[0][3:] == ["site", "restore", "SRER"]
    assert run.STREAM_STAGE_CLASS["restore"] == "net-in" and run.SHORT["restore"] == "rst"
    plan = run.plan_stream_site("SRER", {}, run.make_config(stream=True))
    assert plan.stages[0] == "restore"
    text = run.render_stream_plan([plan], run.make_config(stream=True))
    assert "  plan       restore..cleanup (7)" in text  # resume shows the first real stage


def test_restored_running_rows_of_another_vm_never_block_the_orchestrator(site_env: Path,  # noqa: F811
                                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    import json as _json

    store = _store()
    store.start("A", "upload", "x", "full", chunk="c1")
    store.note_pid("A", "upload", 4242, "c1")
    assert _json.loads(store.chunk_rows("A")["c1"]["upload"]["info"])["host"] == run._host()
    # the same row as journal_sync restores it from another VM's backup
    store.con.execute("UPDATE site_run_chunks SET info=? WHERE site='A'", (_json.dumps({"pids": [4242],
                                                                                       "host": "other-vm"}),))
    monkeypatch.setattr(run, "_alive", lambda pid: True)  # a local neon_ducklake process happens to have that pid
    assert store.recover_stale() == [("A", "chunk c1 upload")]
    row = store.chunk_rows("A")["c1"]["upload"]
    assert row["status"] == "failed" and "host" not in row["info"]


def test_finalize_not_ready_rc1_is_retried_later(site_env: Path) -> None:  # noqa: F811
    n = {"fin": 0}

    def fin() -> int:
        n["fin"] += 1
        return 1 if n["fin"] == 1 else 0  # stream.finalize_site: another VM still curates a product

    r = FakeStreamRunner({("A", None, "finalize"): fin})
    assert _sched(_plans(["A"], ["finalize", "cleanup"]), r, {"A": []}, retry_delay_s=0.0).run() == {"A": "ok"}
    assert [st for _s, _c, st in r.started] == ["finalize", "finalize", "cleanup"]


def test_after_ok_publishes_the_ontology_export_after_finalize_only(site_env: Path) -> None:  # noqa: F811
    pushed: list[str] = []
    published: list[int] = []
    hook = run.make_after_ok(push=lambda s: pushed.append(s) or {}, publish=lambda: published.append(1) or None)
    assert hook("A", "delete", "c1") is None and published == []
    assert hook("A", "finalize", None) is None and published == [1] and pushed == ["A", "A"]
    # a failed publish is only a warning; a failed journal push fails the stage and skips the publish
    assert run.make_after_ok(push=lambda s: {}, publish=lambda: "iRODS down")("A", "finalize", None) is None
    bad = run.make_after_ok(push=lambda s: {"error": "refused"}, publish=lambda: published.append(2) or None)
    assert "journal push failed: refused" in (bad("A", "finalize", None) or "") and published == [1]
    stop = threading.Event()
    stop.set()
    assert "not pushed" in (run.make_after_ok(stop, push=lambda s: {}, publish=lambda: published.append(3) or None)(
        "A", "finalize", None) or "") and published == [1]


def test_the_orchestrator_process_sees_the_childrens_history_mode(site_env: Path,  # noqa: F811
                                                                  monkeypatch: pytest.MonkeyPatch,
                                                                  ds_coord: types.SimpleNamespace) -> None:  # noqa: F811
    """coord.curation_sync_enabled() (auto: on with spool) and the finalize gate run in the orchestrator itself."""
    import os

    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    seen: list[str | None] = []

    def pending(site: str) -> list[str]:
        seen.append(os.environ.get(run.ENV_HISTORY_MODE))
        return []

    from neon_ducklake.site import validate

    monkeypatch.setattr(validate, "pending_generic", pending, raising=False)
    monkeypatch.setattr(run.SubprocessRunner, "__call__", lambda self, site, stage, chunk=None: 0)
    assert run.main(["SRER"], poll_s=0.01) == 0
    assert seen == ["spool"] and os.environ.get(run.ENV_HISTORY_MODE) is None


def test_plan_only_runs_take_no_claim(site_env: Path) -> None:  # noqa: F811
    claimed: list[str] = []

    @contextlib.contextmanager
    def claimer(site: str) -> Iterator[Any]:
        claimed.append(site)
        yield types.SimpleNamespace(lost=threading.Event())

    r = FakeStreamRunner()
    assert _sched(_plans(["A"], ["plan", "list", "chunk-plan"]), r, {"A": []}, claimer=claimer).run() == {"A": "ok"}
    assert claimed == [] and [st for _s, _c, st in r.started] == ["plan", "list", "chunk-plan"]
    assert _sched(_plans(["B"], ["restore", "plan"]), FakeStreamRunner(), {"B": []}, claimer=claimer).run() == \
        {"B": "ok"}
    assert claimed == ["B"]
