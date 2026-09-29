"""Recorder: coalescing for the 900-snapshot budget (B3), content-key dedup across batch ids, in-order
recording per VM (m7), --watch robustness and work/spool-in cleanup (#13), mesa-tag lock waits (M10)."""

from __future__ import annotations

import json
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest

from neon_ducklake import env, state
from neon_ducklake.avu import history, recorder

from .test_avu_history import add_rows, status_of
from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds

SPOOL = f"{ROOT}/.mesa/spool"
DONE = f"{ROOT}/.mesa/spool-done"


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)


@pytest.fixture
def lake(ds: DSIrods, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from mesa_ducklake import DuckLakeClient

    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    client = DuckLakeClient(catalog_dsn=f"duckdb:///{tmp_path / 'catalog.duckdb'}", irods_session=None,
                            cache_dir=str(env.MESA_CACHE_DIR), cache_cap_bytes=0)
    project = client.register_project(ROOT, env.ACTOR, "iplant")
    yield client, project
    client.close()


def spool_as(ds: DSIrods, con, vm: str, monkeypatch, scope: str = "SRER", **kw) -> dict:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", vm)
    return history.record_scope(scope, con=con, mode="spool", session=DSSession(ds), **kw)


def ingest(ds: DSIrods, con, lake, **kw) -> dict:
    client, project = lake
    return recorder.ingest(session=DSSession(ds), con=con, client=client, project=project, **kw)


def snaps(lake) -> list:
    client, project = lake
    return history._catalog_snapshots(client, project.project_id)


# ---------------------------------------------------------------- coalescing (B3)

def test_small_backlogs_wait_for_min_rows_or_max_age(ds, lake, tmp_path, monkeypatch) -> None:
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=3, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch)
    rcon = state.connect(tmp_path / "r.sqlite")
    out = ingest(ds, rcon, lake, min_rows=10, max_age_h=12)
    assert out["snapshots"] == [] and out["waiting_rows"] == 3 and out["waiting_batches"] == 1
    assert snaps(lake) == [] and len([p for p in ds.objs if p.startswith(SPOOL + "/")]) == 2
    later = env.utcnow() + timedelta(hours=13)  # the oldest batch is now older than max_age_h
    out = ingest(ds, rcon, lake, min_rows=10, max_age_h=12, now=later)
    assert out["rows_recorded"] == 3 and len(out["snapshots"]) == 1


def test_min_rows_triggers_and_flush_records_everything(ds, lake, tmp_path, monkeypatch) -> None:
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=5, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch, rows_per_batch=1)  # 5 batches of 1 row
    rcon = state.connect(tmp_path / "r.sqlite")
    out = ingest(ds, rcon, lake, min_rows=2, max_rows=2, max_age_h=12)
    # groups [2, 2, 1]: the two full groups are due, the last one (1 row, fresh) waits
    assert len(out["snapshots"]) == 2 and out["rows_recorded"] == 4 and out["waiting_rows"] == 1
    assert all(n.note.count(",") == 1 for n in snaps(lake))  # each snapshot merged 2 batches (<= max_rows)
    out = ingest(ds, rcon, lake, min_rows=2, max_rows=2, max_age_h=12, flush=True)
    assert out["rows_recorded"] == 1 and len(snaps(lake)) == 3


def test_older_groups_ride_along_with_a_due_group(ds, lake, tmp_path, monkeypatch) -> None:
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=1, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch)  # an early, small batch
    time.sleep(0.01)
    add_rows(w, n_paths=4, per_path=1, scope="HARV", op="remove")  # other changes (content keys differ)
    spool_as(ds, w, "vm-a", monkeypatch, scope="HARV")  # a later batch that alone fills a group
    rcon = state.connect(tmp_path / "r.sqlite")
    out = ingest(ds, rcon, lake, min_rows=4, max_rows=4, max_age_h=12)
    assert out["rows_recorded"] == 5 and len(out["snapshots"]) == 2 and out["waiting_rows"] == 0  # in order


def test_budget_guard_counts_only_the_due_groups(ds, lake, tmp_path, monkeypatch) -> None:
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=3, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch, rows_per_batch=1)
    rcon = state.connect(tmp_path / "r.sqlite")
    with pytest.raises(history.SnapshotBudgetExceeded):
        ingest(ds, rcon, lake, max_rows=1, flush=True, snapshot_budget=2)
    assert snaps(lake) == []
    assert ingest(ds, rcon, lake, max_rows=1, min_rows=5, snapshot_budget=2)["snapshots"] == []  # nothing due


def test_dry_run_reports_due_and_waiting(ds, lake, tmp_path, monkeypatch) -> None:
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=3, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch, rows_per_batch=1)
    out = ingest(ds, state.connect(tmp_path / "r.sqlite"), lake, max_rows=2, min_rows=2, dry_run=True)
    assert [len(g) for g in out["groups"]] == [2] and out["waiting_rows"] == 1


def test_defaults_are_200k_rows_and_12_hours(monkeypatch) -> None:
    monkeypatch.delenv(recorder.MIN_ROWS_ENV, raising=False)
    monkeypatch.delenv(recorder.MAX_AGE_ENV, raising=False)
    assert recorder.min_rows_default() == 200_000 and recorder.max_age_h_default() == 12.0
    monkeypatch.setenv(recorder.MIN_ROWS_ENV, "50_000")
    assert recorder.min_rows_default() == 50_000


# ---------------------------------------------------------------- content dedup across batch ids

def test_a_respooled_batch_is_not_recorded_twice(ds, lake, tmp_path, monkeypatch) -> None:
    """VM A spooled rows and died before its journal push; VM B restored an older backup where the rows are
    still 'applied' and spooled them again under a new batch id."""
    a = state.connect(tmp_path / "a.sqlite")
    ids = add_rows(a, n_paths=2, per_path=2)
    first = spool_as(ds, a, "vm-a", monkeypatch)["batch_ids"]
    b = state.connect(tmp_path / "b.sqlite")
    add_rows(b, n_paths=2, per_path=2)  # the same changes (same path, attribute, value, unit, op, applied_at)
    add_rows(b, n_paths=1, per_path=1, scope="SRER", op="remove")  # and one change that is new
    second = spool_as(ds, b, "vm-b", monkeypatch)["batch_ids"]
    assert first != second
    rcon = state.connect(tmp_path / "r.sqlite")
    out = ingest(ds, rcon, lake, flush=True, max_rows=4)
    assert out["duplicates_skipped"] == 4 and out["rows_recorded"] == 5  # 4 + the 1 new row
    client, project = lake
    hist = client.get_history(project.project_id, f"{ROOT}/sites/SRER/raw/DP1.00002.001/2024-06/f0.csv")
    assert len([h for h in hist if h.op == "add"]) == 2  # neon.k0 and neon.k1 once each
    assert {r[0] for r in rcon.execute("SELECT status FROM batches")} == {"recorded"}
    assert not [p for p in ds.objs if p.startswith(SPOOL + "/")]
    assert len(ids) == 4


def test_a_batch_of_only_duplicates_records_no_snapshot(ds, lake, tmp_path, monkeypatch) -> None:
    a = state.connect(tmp_path / "a.sqlite")
    add_rows(a, n_paths=1, per_path=2)
    spool_as(ds, a, "vm-a", monkeypatch)
    rcon = state.connect(tmp_path / "r.sqlite")
    assert ingest(ds, rcon, lake, flush=True)["rows_recorded"] == 2
    b = state.connect(tmp_path / "b.sqlite")
    add_rows(b, n_paths=1, per_path=2)
    bid = spool_as(ds, b, "vm-b", monkeypatch)["batch_ids"][0]
    out = ingest(ds, rcon, lake, flush=True)
    assert out["snapshots"] == [] and out["duplicates_skipped"] == 2 and out["batches_recorded"] == 1
    assert len(snaps(lake)) == 1 and f"{DONE}/vm-b/{bid}.parquet" in ds.objs
    assert recorder.SeenIndex().count() == 2


def test_keys_of_a_batch_committed_before_a_crash_are_indexed_next_pass(ds, lake, tmp_path, monkeypatch) -> None:
    a = state.connect(tmp_path / "a.sqlite")
    add_rows(a, n_paths=1, per_path=2)
    spool_as(ds, a, "vm-a", monkeypatch)
    rcon = state.connect(tmp_path / "r.sqlite")
    real = recorder.SeenIndex.add
    monkeypatch.setattr(recorder.SeenIndex, "add", lambda self, k, s: (_ for _ in ()).throw(OSError("disk")))
    first = ingest(ds, rcon, lake, flush=True)
    assert len(first["snapshots"]) == 1 and "disk" in first["errors"][0]["error"]
    monkeypatch.setattr(recorder.SeenIndex, "add", real)
    again = ingest(ds, rcon, lake, flush=True)
    assert again["already_committed"] == 1 and recorder.SeenIndex().count() == 2


# ---------------------------------------------------------------- order per VM (m7), --watch, spool-in (#13)

def test_an_unfetchable_batch_holds_back_later_batches_of_its_vm(ds, lake, tmp_path, monkeypatch) -> None:
    a = state.connect(tmp_path / "a.sqlite")
    add_rows(a, n_paths=1, per_path=1)
    bad = spool_as(ds, a, "vm-a", monkeypatch)["batch_ids"][0]
    time.sleep(0.01)
    add_rows(a, n_paths=1, per_path=1, scope="HARV")
    later = spool_as(ds, a, "vm-a", monkeypatch, scope="HARV")["batch_ids"][0]
    b = state.connect(tmp_path / "b.sqlite")
    add_rows(b, n_paths=1, per_path=1, scope="KONZ")
    other = spool_as(ds, b, "vm-b", monkeypatch, scope="KONZ")["batch_ids"][0]
    doc = json.loads(ds.content(f"{SPOOL}/vm-a/{bad}.json")) | {"sha256": "0" * 64}
    ds.set_content(f"{SPOOL}/vm-a/{bad}.json", json.dumps(doc).encode())
    out = ingest(ds, state.connect(tmp_path / "r.sqlite"), lake, flush=True)
    assert [e["batch_id"] for e in out["errors"]] == [bad] and out["deferred"] == [later]
    assert f"{DONE}/vm-b/{other}.parquet" in ds.objs  # another VM's batches go on
    assert f"{SPOOL}/vm-a/{later}.parquet" in ds.objs  # recorded after `bad`, never before it


def test_watch_survives_any_pass_error(ds, monkeypatch) -> None:
    calls: list[int] = []

    def flaky(**kw):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("iRODS connection reset")
        return {"spooled": 0}

    monkeypatch.setattr(recorder, "ingest", flaky)
    rc = recorder.main(watch=True, interval=0, session=DSSession(ds), passes=3, sleep=lambda s: None)
    assert len(calls) == 3 and rc == 0
    with pytest.raises(RuntimeError):  # without --watch the error is the result
        calls.clear()
        recorder.main(watch=False, session=DSSession(ds))


def test_spool_in_is_emptied(ds, lake, tmp_path, monkeypatch) -> None:
    junk = env.WORK_DIR / "spool-in" / "vm-z" / "left-over.parquet"
    junk.parent.mkdir(parents=True)
    junk.write_bytes(b"x")
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=1, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch)
    ingest(ds, state.connect(tmp_path / "r.sqlite"), lake, flush=True)
    assert not junk.exists() and not any((env.WORK_DIR / "spool-in").rglob("*.parquet"))


# ---------------------------------------------------------------- mesa-tag waits (M10)

def _hold(name: str, seconds: float, ready: threading.Event) -> threading.Thread:
    def run() -> None:
        with env.file_lock(name):
            ready.set()
            time.sleep(seconds)

    t = threading.Thread(target=run)
    t.start()
    return t


def test_recorder_waits_for_mesa_tag(ds, lake, tmp_path, monkeypatch) -> None:
    w = state.connect(tmp_path / "w.sqlite")
    add_rows(w, n_paths=1, per_path=1)
    spool_as(ds, w, "vm-a", monkeypatch)
    monkeypatch.setenv(env.MESA_LOCK_TIMEOUT_ENV, "10")
    ready = threading.Event()
    t = _hold("mesa-tag", 0.4, ready)
    ready.wait(5)
    out = ingest(ds, state.connect(tmp_path / "r.sqlite"), lake, flush=True)
    t.join()
    assert out["rows_recorded"] == 1


def test_record_scope_direct_waits_for_mesa_tag_then_times_out(tmp_path, lake, monkeypatch) -> None:
    client, _project = lake

    class M:
        session = None

    monkeypatch.setattr(env, "mesa", lambda: M())
    monkeypatch.setattr(env, "mesa_ducklake_client", lambda m: client)
    monkeypatch.setattr(env, "close_mesa_ducklake_client", lambda: None)
    con = state.connect(tmp_path / "s.sqlite")
    add_rows(con, n_paths=1, per_path=1)
    monkeypatch.setenv(env.MESA_LOCK_TIMEOUT_ENV, "10")
    ready = threading.Event()
    t = _hold("mesa-tag", 0.4, ready)
    ready.wait(5)
    assert history.record_scope("SRER", con=con, mode="direct")["rows_recorded"] == 1  # waited, then recorded
    t.join()
    monkeypatch.setenv(env.MESA_LOCK_TIMEOUT_ENV, "0.2")
    ready = threading.Event()
    t = _hold("mesa-tag", 1.0, ready)
    ready.wait(5)
    with pytest.raises(SystemExit, match="after waiting"):
        history.record_scope("SRER", con=con, mode="direct")
    t.join()
    assert status_of(con, [1])[0][0] == "recorded"
