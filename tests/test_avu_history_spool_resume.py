"""Spool batches left 'spooling' (M4): resolved in the batch's own VM spool and spool-done (``.dup-*`` included)
after a journal restore on another VM; released only when the batch is nowhere."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from neon_ducklake import coord, env, state
from neon_ducklake.avu import history

from .test_avu_history import add_rows, status_of
from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)


def spooled_by_a(ds: DSIrods, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[object, str, list[int]]:
    """VM A spooled a batch, then (as a restore on VM B would show it) the batch is back at 'spooling'."""
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    con = state.connect(tmp_path / "b.sqlite")
    ids = add_rows(con, n_paths=2, per_path=1)
    bid = history.record_scope("SRER", con=con, mode="spool", session=DSSession(ds))["batch_ids"][0]
    con.execute("UPDATE batches SET status='spooling' WHERE batch_id=?", (bid,))
    con.execute("UPDATE avu_journal SET status='applied' WHERE batch_id=?", (bid,))
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-b")  # the restoring VM
    return con, bid, ids


def resume(ds: DSIrods, con) -> dict:
    return history.spool_batches(con, "SRER", session=DSSession(ds))


def test_batches_record_their_vm(ds, tmp_path, monkeypatch) -> None:
    con, bid, _ = spooled_by_a(ds, tmp_path, monkeypatch)
    row = con.execute("SELECT vm, note FROM batches WHERE batch_id=?", (bid,)).fetchone()
    assert row["vm"] == "vm-a" and "vm=vm-a" in row["note"]
    con.execute("UPDATE batches SET vm=NULL WHERE batch_id=?", (bid,))  # an older row: the note still tells
    assert history.batch_vm(con.execute("SELECT * FROM batches WHERE batch_id=?", (bid,)).fetchone(), "x") == "vm-a"


def test_another_vms_verified_spool_copy_is_trusted(ds, tmp_path, monkeypatch) -> None:
    con, bid, ids = spooled_by_a(ds, tmp_path, monkeypatch)
    out = resume(ds, con)
    assert out["resumed"] == 1 and out["released"] == 0 and out["batches_planned"] == 0
    assert {(s, b) for s, b, _ in status_of(con, ids)} == {("spooled", bid)}


@pytest.mark.parametrize("dup", [False, True])
def test_a_batch_the_recorder_already_took_is_not_respooled(ds, tmp_path, monkeypatch, dup) -> None:
    con, bid, ids = spooled_by_a(ds, tmp_path, monkeypatch)
    for ext in ("parquet", "json"):  # the recorder moved it to spool-done (possibly as a .dup-* copy)
        src = f"{ROOT}/.mesa/spool/vm-a/{bid}.{ext}"
        dst = f"{ROOT}/.mesa/spool-done/vm-a/{bid}.{ext}" + (".dup-20260928T000000000000Z" if dup else "")
        DSSession(ds).data_objects.move(src, dst)
    out = resume(ds, con)
    assert out["resumed"] == 1 and out["released"] == 0 and out["batches_spooled"] == 0
    assert {s for s, _, _ in status_of(con, ids)} == {"spooled"}


def test_a_parquet_without_its_manifest_is_set_aside_then_released(ds, tmp_path, monkeypatch) -> None:
    con, bid, ids = spooled_by_a(ds, tmp_path, monkeypatch)
    DSSession(ds).data_objects.unlink(f"{ROOT}/.mesa/spool/vm-a/{bid}.json")
    out = resume(ds, con)
    assert out["released"] == 1 and out["set_aside"] == 1 and out["batches_spooled"] == 1
    assert f"{ROOT}/.mesa/spool-aside/vm-a/{bid}.parquet" in ds.objs
    assert f"{ROOT}/.mesa/spool/vm-a/{bid}.parquet" not in ds.objs  # the recorder never sees it
    new = out["batch_ids"][0]
    assert new != bid and f"{coord.spool_dir('vm-b')}/{new}.parquet" in ds.objs
    assert {(s, b) for s, b, _ in status_of(con, ids)} == {("spooled", new)}


def test_a_batch_that_is_nowhere_is_released(ds, tmp_path, monkeypatch) -> None:
    con, bid, ids = spooled_by_a(ds, tmp_path, monkeypatch)
    for ext in ("parquet", "json"):
        DSSession(ds).data_objects.unlink(f"{ROOT}/.mesa/spool/vm-a/{bid}.{ext}")
    out = resume(ds, con)
    assert out["released"] == 1 and out["set_aside"] == 0
    assert con.execute("SELECT status FROM batches WHERE batch_id=?", (bid,)).fetchone()[0] == "abandoned"
    assert {s for s, _, _ in status_of(con, ids)} == {"spooled"}  # re-spooled by vm-b


def test_a_mismatching_manifest_is_set_aside(ds, tmp_path, monkeypatch) -> None:
    con, bid, _ = spooled_by_a(ds, tmp_path, monkeypatch)
    m = f"{ROOT}/.mesa/spool/vm-a/{bid}.json"
    ds.set_content(m, json.dumps(json.loads(ds.content(m)) | {"md5": "0" * 32}).encode())
    out = resume(ds, con)
    assert out["set_aside"] == 1 and f"{ROOT}/.mesa/spool-aside/vm-a/{bid}.json" in ds.objs
    assert env.vm_id() == "vm-b"
