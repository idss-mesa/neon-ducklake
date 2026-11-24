"""run.py, review round 3 (docs/STREAMING.md "Orchestrator"): RC_RETRY (4) from any stream stage is retried later
with a bounded backoff (N10); a site this host finished whose .done marker is missing gets it, under its claim,
unless another VM pushed a newer journal since, in which case the site runs again from restore first (N8); and
the stages learn whether a site claim is in force (NEON_DUCKLAKE_SITE_CLAIMS).

The .done tests drive run.main with the real coord (site_claim, write_done, .done checks) and the real
journal_sync (needs_restore, push/restore of another VM's journal) against the fake Data Store of
tests/test_coord_fakes.py; only the stage subprocesses are replaced."""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import cli, coord, journal_sync, run, state
from neon_ducklake.site import chunks as chunkmod

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds
from .test_run_stream import (  # noqa: F401 - ds_coord is a fixture
    CS,
    FakeStreamRunner,
    _chunks,
    _plans,
    _sched,
    _store,
    ds_coord,
)
from .test_site_common import site_env  # noqa: F401 - fixture

SITE = "SRER"
OLD = "DP1.00001.001__2024-01_2024-01"
NEW = "DP1.00001.001__2025-01_2025-01"


# ---------------------------------------------------------------- N10: RC_RETRY = 4 is "retry later"

def test_rc_retry_matches_stream_and_is_a_retry_code_of_every_stream_stage() -> None:
    from neon_ducklake.site import stream

    assert run.RC_RETRY == 4 and getattr(stream, "RC_RETRY", run.RC_RETRY) == run.RC_RETRY
    for stage in run.CHUNK_STAGES:
        assert run.RC_RETRY in run.RETRY_RC[(stage, True)]
    for stage in run.STREAM_STAGES:
        if stage != "chunks":
            assert run.RC_RETRY in run.RETRY_RC[(stage, False)]
    # the older retry codes stay: download 3 (working set), finalize 1 (another VM curates)
    assert 3 in run.RETRY_RC[("download", True)] and 1 in run.RETRY_RC[("finalize", False)]
    sched = _sched([], FakeStreamRunner(), {})
    assert sched._outcome("avus", "c1", 4).status == "retry" and "transient" in sched._outcome("avus", "c1", 4).info
    assert sched._outcome("avus", "c1", 3).status == "failed"  # rc 3 is a retry only for download
    assert sched._outcome("stack", "c1", 2).status == "failed"


def test_rc_retry_is_retried_with_backoff_for_chunk_and_site_stages(site_env: Path) -> None:  # noqa: F811
    store = _store()
    left = {("A", "c1", "avus"): 2, ("A", "c1", "upload"): 1, ("A", None, "restore"): 1, ("A", None, "finalize"): 1,
            ("A", "c1", "verify"): 1}

    def transient(key: tuple[str, str | None, str]) -> Any:
        def hook() -> int:
            if left[key]:
                left[key] -= 1
                return run.RC_RETRY  # e.g. OLS down with taxa pending, or a lake-push race
            return 0
        return hook

    r = FakeStreamRunner({k: transient(k) for k in left})
    msgs: list[str] = []
    sched = _sched(_plans(["A"], ["restore", "chunks", "finalize", "cleanup"]), r, {"A": _chunks("c1")}, store,
                   retry_delay_s=0.001, retry_backoff=2.0, retry_max_delay_s=0.003, out=msgs.append)
    assert sched.run() == {"A": "ok"}
    assert all(v == 0 for v in left.values())
    assert r.stages_of("A", "c1") == ["download", "stack", "avus", "avus", "avus", "upload", "upload", "apply",
                                      "history", "verify", "verify", "delete"]
    assert [st for s, c, st in r.started if c is None] == ["restore", "restore", "finalize", "finalize", "cleanup"]
    # backoff per task: the second retry of avus waits twice as long as the first
    delays = {}
    for key, d in sched.retry_delays:
        delays.setdefault(key, []).append(d)
    assert delays[("A", "c1", "avus")] == [0.001, 0.002]
    assert delays[("A", None, "restore")] == [0.001]
    assert store.chunk_rows("A")["c1"]["avus"]["attempt"] == 3 and store.rows()["A"]["finalize"]["status"] == "ok"
    assert any("retry 2/6" in m and "transient (exit 4)" in m for m in msgs)


def test_retry_backoff_is_capped_and_the_retries_are_bounded(site_env: Path) -> None:  # noqa: F811
    sched = _sched([], FakeStreamRunner(), {})
    assert [sched.retry_delay(n) for n in range(1, 7)] == [300, 600, 1200, 2400, 3600, 3600]
    assert sum(sched.retry_delay(n) for n in range(1, run.MAX_RETRIES + 1)) < run.WAIT_TIMEOUT_S
    store = _store()
    r = FakeStreamRunner({("B", "c1", "avus"): lambda: run.RC_RETRY})  # OLS stays down
    sched = _sched(_plans(["B"], ["chunks"]), r, {"B": _chunks("c1")}, store, retry_delay_s=0.0, max_retries=3)
    assert sched.run() == {"B": "failed"}
    assert r.stages_of("B", "c1") == ["download", "stack", "avus", "avus", "avus", "avus"]
    row = store.chunk_rows("B")["c1"]["avus"]
    assert row["status"] == "failed" and row["rc"] == 4 and "gave up after 3 retries" in row["info"]


def test_rc_retry_from_a_real_stage_subprocess(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """The exit code of a real child process reaches the scheduler: 4 twice, then 0."""
    counter = site_env / "tries"
    prog = (f"import pathlib, sys; p = pathlib.Path({str(counter)!r}); n = int(p.read_text() or 0) "
            "if p.exists() else 0; p.write_text(str(n + 1)); sys.exit(4 if n < 2 else 0)")
    monkeypatch.setattr(run, "stream_commands", lambda site, stage, chunk=None, **kw: [[sys.executable, "-c", prog]])
    runner = run.SubprocessRunner(run.make_config(stream=True))
    store = _store()
    sched = _sched(_plans(["A"], ["finalize"]), runner, {"A": []}, store, retry_delay_s=0.0)
    assert sched.run() == {"A": "ok"} and counter.read_text() == "3"
    log = (site_env / "logs/run/A/finalize.log").read_text()
    assert log.count("=== exit 4") == 2 and "=== exit 0" in log
    assert store.rows()["A"]["finalize"]["attempt"] == 3


def test_rc_retry_through_the_real_site_chunk_cli(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """`site chunk avus` returns the stage's RC_RETRY as its exit code (site/__init__.py), and run retries it."""
    left = {"avus": 1}

    def avus_chunk(site: str, chunk_id: str) -> int:
        if left["avus"]:
            left["avus"] -= 1
            return run.RC_RETRY
        return 0

    stages = {f"{s}_chunk": (lambda site, chunk_id: 0) for s in run.CHUNK_STAGES}
    stages["avus_chunk"] = avus_chunk
    mod = types.ModuleType("neon_ducklake.site.stream")
    for k, v in stages.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, "neon_ducklake.site.stream", mod)
    assert cli.main(["site", "chunk", "avus", SITE, "--chunk", OLD]) == run.RC_RETRY
    left["avus"] = 1
    calls: list[str] = []

    def in_process(site: str, stage: str, chunk: str | None = None) -> int:
        calls.append(stage)
        return cli.main(run.stream_commands(site, stage, chunk)[0][3:])

    sched = _sched(_plans([SITE], ["chunks"]), in_process, {SITE: _chunks(OLD)}, _store(),  # type: ignore[arg-type]
                   retry_delay_s=0.0)
    assert sched.run() == {SITE: "ok"} and calls == ["download", "stack", "avus", "avus", *CS[3:]]


# ---------------------------------------------------------------- the stages learn whether a claim is in force

def test_stages_learn_whether_a_site_claim_is_in_force(site_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                                                       ds_coord: types.SimpleNamespace) -> None:  # noqa: F811
    prog = "import os; print('claims', os.environ.get('NEON_DUCKLAKE_SITE_CLAIMS'))"
    monkeypatch.setattr(run, "stream_commands", lambda site, stage, chunk=None, **kw: [[sys.executable, "-c", prog]])
    monkeypatch.setattr(run, "free_disk", lambda: 10**15)
    monkeypatch.setenv(run.ENV_SITE_CLAIMS, "off")  # a stray value in the shell never wins
    log = site_env / "logs/run/AAAA/restore.log"
    assert run.run_sites(["AAAA"]) == 0  # the worker holds the claim
    assert "\nclaims on\n" in log.read_text() and ds_coord.claims == []
    monkeypatch.delenv(run.ENV_SITE_CLAIMS)
    assert run.main(["BBBB"], poll_s=0.01) == 0  # run takes it
    assert (site_env / "logs/run/BBBB/restore.log").read_text().count("claims on") == 1 and ds_coord.claims == ["BBBB"]
    assert run.main(["CCCC"], poll_s=0.01, claims=False) == 0  # --no-claims: no claim is in force
    assert "claims off" in (site_env / "logs/run/CCCC/restore.log").read_text()


def test_the_exported_claim_switch_drives_the_stages_claim_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    """coord.holds_site (journal pointer, catalog writes) and upload.claim_problem read what run exports."""
    from neon_ducklake.site import upload

    assert getattr(coord, "SITE_CLAIMS_ENV", run.ENV_SITE_CLAIMS) == run.ENV_SITE_CLAIMS
    assert getattr(upload, "REQUIRE_CLAIM_ENV", run.ENV_REQUIRE_CLAIM) == run.ENV_REQUIRE_CLAIM
    cfg = run.make_config(stream=True)
    for claims, held, want in ((True, False, True), (False, True, True), (False, False, False)):
        cfg.claims, cfg.claims_held = claims, held
        child = cfg.child_env()
        if hasattr(coord, "site_claims_required"):
            assert coord.site_claims_required(child) is want
        if hasattr(upload, "_require_claim"):
            assert upload._require_claim(child) is want


# ---------------------------------------------------------------- N8: a finished site's missing .done marker

@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    store = hermetic_ds(tmp_path, monkeypatch)
    s = DSSession(store)
    monkeypatch.setattr(coord, "get_session", lambda session=None: session if session is not None else s)
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.setattr(run, "free_disk", lambda: 10**15)
    monkeypatch.setattr(run, "publish_ontology_export", lambda: None)
    from neon_ducklake.site import validate

    monkeypatch.setattr(validate, "pending_generic", lambda site: [], raising=False)
    return store


def _finished_here(site: str = SITE) -> None:
    """This host's journal: every stream stage of the site ok in a full run, its only chunk deleted, and
    site/chunks.py's cleanup step ok."""
    con = state.connect(state.DB_PATH)
    chunkmod.ensure_schema(con)
    con.execute("INSERT INTO stream_chunks(site, chunk_id, product, months, n_files, bytes, status, done) "
                "VALUES (?, ?, 'DP1.00001.001', '[\"2024-01\"]', 1, 10, 'deleted', 'deleted')", (site, OLD))
    chunkmod.set_step(con, site, "cleanup", "ok")
    store = run.Store(con)
    for stage in run.STREAM_STAGES:
        store.start(site, stage, "x", "full")
        store.finish(site, stage, "ok", 0)
    con.close()


class Stages:
    """Stands in for the stage subprocesses: records them and advances stream_chunks like site/stream.py."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None, str]] = []

    def __call__(self, site: str, stage: str, chunk: str | None = None) -> int:  # set on the class: no self
        self.calls.append((site, chunk, stage))
        if stage == "restore":
            from neon_ducklake import site as site_pkg
            from neon_ducklake.site import stream

            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(stream, "restore_lake", lambda s: 0, raising=False)  # stream-core's; not under test
                return site_pkg._restore(site)  # the real `site restore`: journal backup first
        if chunk is not None:
            con = state.connect(state.DB_PATH)
            new = run.CHUNK_STATUS_ORDER[run.CHUNK_STAGES.index(stage) + 1]
            con.execute("UPDATE stream_chunks SET status=?, done=? WHERE site=? AND chunk_id=?", (new, new, site, chunk))
            con.close()
        return 0


def _done_doc(store: DSIrods, site: str = SITE) -> dict[str, Any] | None:
    path = f"{ROOT}/sites/{site}/.done"
    return json.loads(store.content(path)) if path in store.objs else None


def _claim_creates(store: DSIrods, site: str = SITE) -> int:
    return sum(1 for op, p in store.ops if op == "create" and p == f"{ROOT}/sites/{site}/.claim")


def test_a_finished_site_without_its_done_marker_gets_it_under_its_claim(ds: DSIrods, monkeypatch: pytest.MonkeyPatch,
                                                                         capsys: pytest.CaptureFixture[str]) -> None:
    _finished_here()
    stages = Stages()
    monkeypatch.setattr(run.SubprocessRunner, "__call__", stages)
    assert run.main([SITE], poll_s=0.01) == 0
    assert stages.calls == []  # nothing is run again
    doc = _done_doc(ds)
    assert doc is not None and doc["by"] == "run" and doc["chunks"] == {"deleted": 1}
    assert _claim_creates(ds) == 1 and f"{ROOT}/sites/{SITE}/.claim" not in ds.objs  # claimed, then released
    assert f"{SITE}  marked done" in capsys.readouterr().out
    # the next run finds the marker before claiming: nothing to do, no claim
    assert run.main([SITE], poll_s=0.01) == 0
    assert stages.calls == [] and _claim_creates(ds) == 1 and "nothing to do" in capsys.readouterr().out


def test_a_finished_site_claimed_by_another_live_vm_is_left_alone(ds: DSIrods,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    _finished_here()
    other = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-c")  # vm-c works on the site
    assert other.acquired
    monkeypatch.setattr(run.SubprocessRunner, "__call__", Stages())
    assert run.main([SITE], poll_s=0.01) == 0  # claimed elsewhere is not a failure
    assert _done_doc(ds) is None
    held = coord.read_claim(f"{ROOT}/sites/{SITE}/.claim", session=DSSession(ds))
    assert held is not None and held.vm == "vm-c" and held.token == other.claim.token


def test_no_done_check_without_claims_in_sample_runs_or_without_cleanup(ds: DSIrods,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    _finished_here()
    monkeypatch.setattr(run.SubprocessRunner, "__call__", Stages())
    assert run.main([SITE], poll_s=0.01, claims=False) == 0  # --no-claims: no claim, no .done
    assert run.main([SITE], poll_s=0.01, stages="plan,list,chunk-plan") == 0  # cleanup not selected
    assert _claim_creates(ds) == 0 and _done_doc(ds) is None
    assert run.main([SITE], poll_s=0.01, sample=2, stages="plan") == 0  # sample runs never mark a site done
    assert _claim_creates(ds) == 0 and _done_doc(ds) is None
    # the site lake was reopened for writing since (stream.restore_lake dropped site/chunks.py's cleanup row,
    # e.g. a `site list` by hand after a NEON release): not finished, so no marker
    con = state.connect(state.DB_PATH)
    assert chunkmod.clear_cleanup(con, SITE)
    con.close()
    assert run.main([SITE], poll_s=0.01) == 0
    assert _claim_creates(ds) == 0 and _done_doc(ds) is None


def test_the_done_check_never_fails_the_run_when_the_data_store_is_down(ds: DSIrods,
                                                                        monkeypatch: pytest.MonkeyPatch,
                                                                        capsys: pytest.CaptureFixture[str]) -> None:
    _finished_here()
    monkeypatch.setattr(run.SubprocessRunner, "__call__", Stages())

    def unreachable(session: Any = None) -> Any:
        raise ConnectionError("iRODS unreachable")

    monkeypatch.setattr(coord, "get_session", unreachable)
    assert run.main([SITE], poll_s=0.01) == 0
    assert "could not be checked" in capsys.readouterr().out and _done_doc(ds) is None


def test_the_journal_backup_cannot_be_checked_so_no_marker(ds: DSIrods, monkeypatch: pytest.MonkeyPatch) -> None:
    _finished_here()
    monkeypatch.setattr(run.SubprocessRunner, "__call__", Stages())

    def broken(site: str, **kw: Any) -> bool:
        raise OSError("manifest unreadable")

    monkeypatch.setattr(journal_sync, "needs_restore", broken)
    assert run.main([SITE], poll_s=0.01) == 0
    assert _done_doc(ds) is None and _claim_creates(ds) == 1 and f"{ROOT}/sites/{SITE}/.claim" not in ds.objs


def test_a_newer_journal_of_another_vm_reruns_the_site_before_marking_it_done(ds: DSIrods, tmp_path: Path,
                                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    """This host finished SRER long ago (no .done: say its write failed). Since then vm-b re-listed the site after a
    NEON release, planned a new chunk, pushed its journal and stopped. The plain run must not mark the site done
    over vm-b's work: it restores vm-b's journal (restore stage), runs the new chunk, finalize and cleanup, then
    writes .done."""
    _finished_here()
    s = DSSession(ds)
    b = state.connect(tmp_path / "vm-b.sqlite")
    chunkmod.ensure_schema(b)
    for cid, status in ((OLD, "deleted"), (NEW, "planned")):
        b.execute("INSERT INTO stream_chunks(site, chunk_id, product, months, n_files, bytes, status) VALUES "
                  "(?, ?, 'DP1.00001.001', ?, 1, 10, ?)", (SITE, cid, json.dumps([cid[-7:]]), status))
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-b")
    held = coord.claim_site(SITE, session=s, settle_s=0)  # vm-b pushes under its own claim ...
    journal_sync.push_site_journal(SITE, session=s, con=b)
    coord.release(held.claim, session=s)  # ... and releases the site without finishing it
    monkeypatch.delenv("NEON_DUCKLAKE_VM_ID")
    b.close()
    assert journal_sync.needs_restore(SITE)
    stages = Stages()
    monkeypatch.setattr(run.SubprocessRunner, "__call__", stages)
    pushes: list[str] = []
    monkeypatch.setattr(run, "_push_site_journal", lambda site: pushes.append(site) or {})
    assert run.main([SITE], poll_s=0.01) == 0
    assert stages.calls[:2] == [(SITE, None, "restore"), (SITE, None, "chunk-plan")]
    assert [st for _s, c, st in stages.calls if c == NEW] == list(CS)
    assert not [c for _s, c, _st in stages.calls if c == OLD]
    assert stages.calls[-2:] == [(SITE, None, "finalize"), (SITE, None, "cleanup")]
    doc = _done_doc(ds)
    assert doc is not None and doc["chunks"] == {"deleted": 2}
    assert _claim_creates(ds) == 2 and f"{ROOT}/sites/{SITE}/.claim" not in ds.objs  # vm-b's, then run's
    assert pushes == [SITE] * (3 + 2)  # the new chunk's apply, history, delete; finalize; cleanup
    rows = run.Store(state.connect(state.DB_PATH)).rows()[SITE]
    assert all(rows[st]["status"] == "ok" for st in run.RERUN_STAGES)


def test_a_stale_journal_is_never_rerun_without_the_restore_stage(site_env: Path) -> None:  # noqa: F811
    import contextlib
    import threading

    held: list[str] = []

    @contextlib.contextmanager
    def claimer(site: str) -> Any:
        held.append(site)
        yield types.SimpleNamespace(lost=threading.Event())
        held.append("released")

    marks: list[str] = []
    msgs: list[str] = []
    r = FakeStreamRunner()
    sched = _sched([run.StreamSitePlan("A", [])], r, {"A": []}, claimer=claimer, on_site_done=lambda s: marks.append(s) or True,
                   finished=lambda s: True, journal_newer=lambda s: True, out=msgs.append,
                   rerun_stages=["chunks", "finalize", "cleanup"])  # e.g. --stages without restore
    assert sched.run() == {"A": "nothing to do"}
    assert r.started == [] and marks == [] and held == ["A", "released"]
    assert any("run it with the restore stage selected" in m for m in msgs)
    # the same site with restore selected runs the rerun stages, then gets its marker
    sched = _sched([run.StreamSitePlan("A", [])], r, {"A": []}, claimer=claimer, on_site_done=lambda s: marks.append(s) or True,
                   finished=lambda s: True, journal_newer=lambda s: True)
    assert sched.run() == {"A": "ok"}
    assert [st for _s, _c, st in r.started] == ["restore", "chunk-plan", "finalize", "cleanup"] and marks == ["A"]
