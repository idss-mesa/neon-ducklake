"""Spool history backend (workers) + the recorder (catalog host), against a fake Data Store and a real
local-only mesa-ducklake client in tmp_path."""

from __future__ import annotations

import json
from datetime import UTC
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from neon_ducklake import env, state
from neon_ducklake.avu import history, recorder

from .test_avu_history import T0, C, add_rows, status_of
from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)

SPOOL = f"{ROOT}/.mesa/spool/vm-a"
DONE = f"{ROOT}/.mesa/spool-done/vm-a"


@pytest.fixture
def worker(ds: DSIrods, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    return state.connect(tmp_path / "worker.sqlite")


@pytest.fixture
def lake(ds: DSIrods, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from mesa_ducklake import DuckLakeClient

    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    client = DuckLakeClient(catalog_dsn=f"duckdb:///{tmp_path / 'catalog.duckdb'}", irods_session=None,
                            cache_dir=str(env.MESA_CACHE_DIR), cache_cap_bytes=0)
    project = client.register_project(ROOT, env.ACTOR, "iplant")
    yield client, project
    client.close()


def spool(ds: DSIrods, con, **kw):
    return history.record_scope("SRER", con=con, mode="spool", session=DSSession(ds), **kw)


def ingest(ds: DSIrods, con, lake, **kw):
    client, project = lake
    kw.setdefault("flush", True)  # these tests record whatever is spooled (coalescing has its own tests)
    return recorder.ingest(session=DSSession(ds), con=con, client=client, project=project, **kw)


def remote(ds: DSIrods, prefix: str) -> list[str]:
    return sorted(p for p in ds.objs if p.startswith(prefix + "/"))


# ---------------------------------------------------------------- spool (worker side)

def test_spool_writes_parquet_and_manifest_and_marks_rows(ds, worker) -> None:
    ids = add_rows(worker, n_paths=3, per_path=2)
    rid = add_rows(worker, n_paths=1, per_path=1, op="remove")[0]
    out = spool(ds, worker, rows_per_batch=4)
    assert out["mode"] == "spool" and out["batches_spooled"] == 2 and out["rows_spooled"] == 7
    assert out["blocked_rows"] == 0 and "failed_batch" not in out and out["spool_dir"] == SPOOL
    st = status_of(worker, ids + [rid])
    assert {s for s, _, _ in st} == {"spooled"} and all(b for _, b, _ in st)
    batches = {r["batch_id"]: dict(r) for r in worker.execute("SELECT * FROM batches")}
    assert {b["status"] for b in batches.values()} == {"spooled"} and set(out["batch_ids"]) == set(batches)
    names = remote(ds, SPOOL)
    assert names == sorted([f"{SPOOL}/{b}.{ext}" for b in batches for ext in ("parquet", "json")])
    for bid, b in batches.items():
        doc = json.loads(ds.content(f"{SPOOL}/{bid}.json"))
        assert doc["batchId"] == bid and doc["rows"] == b["n_rows"] and doc["vm"] == "vm-a" and doc["scope"] == "SRER"
        assert doc["createdAt"] and len(doc["sha256"]) == 64 and doc["format"] == history.SPOOL_FORMAT
        rep = ds.objs[f"{SPOOL}/{bid}.parquet"]["replicas"][0]
        assert rep["checksum"] == doc["md5"] and rep["status"] == "1"  # checksum-verified upload
        assert "spool batch=" + bid in b["note"] and "spooled" in b["note"]
    # local copies are gone once the Data Store copy is verified
    assert not list((env.WORK_DIR / "spool" / "vm-a").glob("*"))
    # nothing left to spool; a second call is a no-op
    assert spool(ds, worker)["batches_planned"] == 0


def test_spool_parquet_columns(ds, worker, tmp_path) -> None:
    add_rows(worker, n_paths=1, per_path=2)
    out = spool(ds, worker)
    bid = out["batch_ids"][0]
    p = tmp_path / "b.parquet"
    p.write_bytes(ds.content(f"{SPOOL}/{bid}.parquet"))
    t = pq.read_table(p)
    assert t.column_names == ["irods_path", "target_type", "attribute", "value", "unit", "op", "actor", "ts", "source",
                              "batch_id", "scope", "vm", "journal_id"]
    rows = t.to_pylist()
    assert {r["batch_id"] for r in rows} == {bid} and {r["vm"] for r in rows} == {"vm-a"}
    assert rows[0]["ts"].astimezone(UTC) == T0 and rows[0]["source"] == "neon-ducklake:site.avus"


def test_history_mode_switch(ds, worker, monkeypatch) -> None:
    monkeypatch.setenv(history.HISTORY_MODE_ENV, "spool")
    assert history.history_mode() == "spool" and history.history_mode("direct") == "direct"
    add_rows(worker, n_paths=1, per_path=1)
    out = history.record_scope("SRER", con=worker, session=DSSession(ds))
    assert out["mode"] == "spool" and out["rows_spooled"] == 1
    monkeypatch.setenv(history.HISTORY_MODE_ENV, "multi")
    with pytest.raises(SystemExit, match="direct, spool"):
        history.history_mode()
    monkeypatch.delenv(history.HISTORY_MODE_ENV)
    assert history.history_mode() == "direct"


def test_failed_upload_is_completed_by_the_next_call(ds, worker) -> None:
    ids = add_rows(worker, n_paths=2, per_path=1)
    ds.put_errors = [RuntimeError("injected put failure")]
    out = spool(ds, worker)
    assert out["failed_batch"] and "injected" in out["error"]
    bid = out["failed_batch"]
    assert worker.execute("SELECT status FROM batches WHERE batch_id=?", (bid,)).fetchone()[0] == "spooling"
    assert {s for s, _, _ in status_of(worker, ids)} == {"applied"}  # batched but not spooled yet
    assert (env.WORK_DIR / "spool" / "vm-a" / f"{bid}.parquet").exists()
    again = spool(ds, worker)
    assert again["resumed"] == 1 and again["batches_planned"] == 0
    assert {(s, b) for s, b, _ in status_of(worker, ids)} == {("spooled", bid)}
    assert f"{SPOOL}/{bid}.json" in ds.objs


def test_interrupted_batch_trusts_a_verified_remote_copy(ds, worker) -> None:
    ids = add_rows(worker, n_paths=2, per_path=1)
    bid = spool(ds, worker)["batch_ids"][0]
    # crash after the upload, before the journal was marked (and the local files are lost)
    worker.execute("UPDATE batches SET status='spooling' WHERE batch_id=?", (bid,))
    worker.execute("UPDATE avu_journal SET status='applied' WHERE batch_id=?", (bid,))
    out = spool(ds, worker)
    assert out["resumed"] == 1 and out["released"] == 0 and out["batches_planned"] == 0
    assert {(s, b) for s, b, _ in status_of(worker, ids)} == {("spooled", bid)}


def test_lost_batch_is_released_and_respooled(ds, worker) -> None:
    ids = add_rows(worker, n_paths=2, per_path=1)
    ds.put_errors = [RuntimeError("injected")]
    bid = spool(ds, worker)["failed_batch"]
    for f in (env.WORK_DIR / "spool" / "vm-a").glob(f"{bid}.*"):
        f.unlink()
    out = spool(ds, worker)
    assert out["released"] == 1 and out["batches_spooled"] == 1 and out["batch_ids"][0] != bid
    assert worker.execute("SELECT status FROM batches WHERE batch_id=?", (bid,)).fetchone()[0] == "abandoned"
    assert {s for s, _, _ in status_of(worker, ids)} == {"spooled"}


# ---------------------------------------------------------------- recorder (catalog host)

def test_recorder_merges_batches_into_snapshots(ds, worker, lake, tmp_path) -> None:
    client, project = lake
    add_rows(worker, n_paths=3, per_path=2)
    ids = spool(ds, worker, rows_per_batch=2)["batch_ids"]
    assert len(ids) == 3
    rcon = state.connect(tmp_path / "recorder.sqlite")
    out = ingest(ds, rcon, lake, max_rows=4)
    assert out["batches_recorded"] == 3 and out["rows_recorded"] == 6 and len(out["snapshots"]) == 2
    snaps = history._catalog_snapshots(client, project.project_id)
    notes = sorted(s.note for s in snaps)
    assert all(n.startswith("neon-ducklake spool batch=") for n in notes)
    assert sorted(len(history.batch_ids_of(n)) for n in notes) == [1, 2]
    assert sorted(i for n in notes for i in history.batch_ids_of(n)) == sorted(ids)
    got = {(h.attribute, h.op, h.ts.astimezone(UTC), h.source) for h in client.get_history(project.project_id,
                                                                                              f"{C}/f0.csv")}
    assert ("neon.k0", "add", T0, "neon-ducklake:site.avus") in got
    # the spool is emptied into spool-done (never deleted); the recorder's journal knows the batches
    assert remote(ds, SPOOL) == [] and len(remote(ds, DONE)) == 6
    rec = {r["batch_id"]: r["status"] for r in rcon.execute("SELECT batch_id, status FROM batches")}
    assert rec == {i: "recorded" for i in ids}
    assert not list((env.WORK_DIR / "spool-in" / "vm-a").glob("*.parquet"))


def test_recorder_is_idempotent_after_a_crash_before_the_move(ds, worker, lake, tmp_path, monkeypatch) -> None:
    client, project = lake
    add_rows(worker, n_paths=2, per_path=2)
    ids = spool(ds, worker, rows_per_batch=2)["batch_ids"]
    rcon = state.connect(tmp_path / "recorder.sqlite")
    real_move = recorder.move_done
    monkeypatch.setattr(recorder, "move_done", lambda s, b: (_ for _ in ()).throw(RuntimeError("crash")))
    first = ingest(ds, rcon, lake)  # the group's error is reported; the pass (and --watch) goes on
    assert "crash" in first["errors"][0]["error"] and len(first["snapshots"]) == 1
    assert len(history._catalog_snapshots(client, project.project_id)) == 1 and len(remote(ds, SPOOL)) == 4
    monkeypatch.setattr(recorder, "move_done", real_move)
    again = ingest(ds, rcon, lake)
    assert again["already_committed"] == 2 and again["snapshots"] == [] and again["batches_recorded"] == 0
    assert len(history._catalog_snapshots(client, project.project_id)) == 1  # never recorded twice
    assert remote(ds, SPOOL) == [] and len(remote(ds, DONE)) == 4
    assert {r[0] for r in rcon.execute("SELECT status FROM batches")} == {"recorded"} and len(ids) == 2
    assert ingest(ds, rcon, lake)["spooled"] == 0


def test_recorder_on_a_shared_host_marks_the_journal_recorded(ds, worker, lake) -> None:
    ids = add_rows(worker, n_paths=2, per_path=1)
    spool(ds, worker)
    out = ingest(ds, worker, lake)
    assert out["rows_recorded"] == 2
    st = status_of(worker, ids)
    assert {s for s, _, _ in st} == {"recorded"} and all(snap == out["snapshots"][0] for _, _, snap in st)


def test_recorder_skips_a_corrupt_batch_and_keeps_it(ds, worker, lake, tmp_path) -> None:
    add_rows(worker, n_paths=2, per_path=1)
    good = spool(ds, worker)["batch_ids"][0]
    add_rows(worker, n_paths=1, per_path=1, scope="HARV")
    bad = history.record_scope("HARV", con=worker, mode="spool", session=DSSession(ds))["batch_ids"][0]
    doc = json.loads(ds.content(f"{SPOOL}/{bad}.json")) | {"sha256": "0" * 64}
    ds.set_content(f"{SPOOL}/{bad}.json", json.dumps(doc).encode())
    rcon = state.connect(tmp_path / "recorder.sqlite")
    out = ingest(ds, rcon, lake)
    assert out["batches_recorded"] == 1 and [e["batch_id"] for e in out["errors"]] == [bad]
    assert remote(ds, SPOOL) == [f"{SPOOL}/{bad}.json", f"{SPOOL}/{bad}.parquet"]
    assert f"{DONE}/{good}.parquet" in ds.objs


def test_recorder_leaves_incomplete_uploads_and_does_not_open_mesa(ds, worker, monkeypatch, tmp_path) -> None:
    ds.add_obj(f"{SPOOL}/11111111-2222-3333-4444-555555555555.parquet", content=b"partial")
    monkeypatch.setattr(history, "_open", lambda: (_ for _ in ()).throw(AssertionError("MESA opened")))
    out = recorder.ingest(session=DSSession(ds), con=state.connect(tmp_path / "r.sqlite"))
    assert out["spooled"] == 0 and out["incomplete"] == [f"{SPOOL}/11111111-2222-3333-4444-555555555555.parquet"]


def test_recorder_budget_guard_and_dry_run(ds, worker, lake, tmp_path) -> None:
    client, project = lake
    add_rows(worker, n_paths=3, per_path=1)
    spool(ds, worker, rows_per_batch=1)
    rcon = state.connect(tmp_path / "recorder.sqlite")
    dry = ingest(ds, rcon, lake, max_rows=2, dry_run=True)
    assert [len(g) for g in dry["groups"]] == [2, 1] and dry["snapshots"] == []
    with pytest.raises(history.SnapshotBudgetExceeded):
        ingest(ds, rcon, lake, max_rows=1, snapshot_budget=2)
    assert history._catalog_snapshots(client, project.project_id) == [] and len(remote(ds, SPOOL)) == 6


def test_recorder_refuses_while_the_catalog_is_held(ds, worker, monkeypatch, tmp_path) -> None:
    add_rows(worker, n_paths=1, per_path=1)
    spool(ds, worker)
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [{"pid": "4242", "cmd": "mesa-mcp"}])
    with pytest.raises(SystemExit, match="4242"):
        recorder.ingest(session=DSSession(ds), con=state.connect(tmp_path / "r.sqlite"))
    # --watch logs it and tries again at the next interval
    rc = recorder.main(watch=True, interval=0, session=DSSession(ds), passes=2, sleep=lambda s: None)
    assert rc == 1


# ---------------------------------------------------------------- recover: multi-batch notes, spool statuses

def test_batch_ids_of_multi_batch_notes() -> None:
    a, b = "0b1f7c1e-9a6e-4c1f-8a53-2a9b0c1d2e3f", "11111111-2222-3333-4444-555555555555"
    assert history.batch_ids_of(f"neon-ducklake spool batch={a},{b} n=2 rows=9") == [a, b]
    assert history.batch_id_of(f"neon-ducklake spool batch={a},{b} n=2") == a
    assert history.batch_ids_of(f"neon-ducklake SRER batch={a} 1/2 paths=1") == [a]
    assert history.batch_ids_of("ds_add_avus") == []


def test_recover_matches_multi_batch_pending_snapshots(ds, lake, tmp_path) -> None:
    from .test_avu_history import _fake_recover

    client, project = lake
    con = state.connect(tmp_path / "recorder.sqlite")
    a, b = "aaaaaaaa-2222-3333-4444-555555555555", "bbbbbbbb-2222-3333-4444-555555555555"
    rows_a = add_rows(con, n_paths=1, per_path=2)
    rows_b = add_rows(con, n_paths=1, per_path=1)
    con.execute("UPDATE avu_journal SET path=? WHERE id=?", (f"{C}/g.csv", rows_b[0]))
    note = f"neon-ducklake spool batch={a},{b} n=2 rows=3"
    for bid, rows in ((a, rows_a), (b, rows_b)):
        con.execute("INSERT INTO batches(batch_id, scope, note, n_rows, status) VALUES (?, 'SRER', ?, ?, 'recording')",
                    (bid, note, len(rows)))
        con.executemany("UPDATE avu_journal SET batch_id=?, status='spooled' WHERE id=?", [(bid, i) for i in rows])
    cat = client._get_catalog()
    snap = cat.create_snapshot(project.project_id, env.ACTOR, None, note, "pending")
    journal = [con.execute("SELECT * FROM avu_journal WHERE id=?", (i,)).fetchone() for i in rows_a + rows_b]
    client._get_lake(project).write_changes(project.project_id, snap.snapshot_id, history.to_changes(journal, "SRER"))
    local = env.MESA_CACHE_DIR / str(project.project_id) / f"snapshot_{snap.snapshot_id}.parquet"
    cat.insert_pending_push(snap.snapshot_id, str(local), f"{ROOT}/.mesa/ducklake/{local.name}")
    _fake_recover(client, project, commit=True)
    rec = history.recover(client, project, None, con)
    assert rec["quarantined"] == [] and rec["recorded"] == 3  # 2 + 1 rows: the expected count summed both ids
    assert {r[0] for r in con.execute("SELECT status FROM batches")} == {"recorded"}
    assert {s for s, _, _ in status_of(con, rows_a + rows_b)} == {"recorded"}


def test_recover_never_releases_spool_batches(ds, lake, tmp_path) -> None:
    client, project = lake
    con = state.connect(tmp_path / "w.sqlite")
    ids = add_rows(con, n_paths=1, per_path=2)
    for status in ("spooling", "spooled"):
        bid = f"{'c' if status == 'spooling' else 'd'}ccccccc-2222-3333-4444-555555555555"
        con.execute("INSERT INTO batches(batch_id, scope, n_rows, status) VALUES (?, 'SRER', 1, ?)", (bid, status))
        con.execute("UPDATE avu_journal SET batch_id=? WHERE id=?", (bid, ids.pop()))
    rec = history.recover(client, project, None, con)
    assert rec["released"] == 0
    assert {r[0] for r in con.execute("SELECT status FROM batches")} == {"spooling", "spooled"}
    assert all(r[0] for r in con.execute("SELECT batch_id FROM avu_journal"))
