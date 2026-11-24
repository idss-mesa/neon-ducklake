"""bulk.apply_scope on a VM that took a site over mid-apply (N1: adoption) or mid-write (N11: reconciled intents):
the real apply, spool, journal backup and verify helpers against the fake Data Store."""

from __future__ import annotations

from pathlib import Path

import pytest

from neon_ducklake import coord, journal_sync, state, tuning
from neon_ducklake.avu import bulk, history, recorder
from neon_ducklake.site import evict

from .test_avu_fakes import FakeMeta
from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds

SITE = "SRER"
C = f"{ROOT}/sites/{SITE}/raw/DP1.00002.001/2024-06"
F1, F2 = f"{C}/a.csv", f"{C}/b.csv"
CLAIMS_ENV = "NEON_DUCKLAKE_SITE_CLAIMS"  # coord.SITE_CLAIMS_ENV
DESIRED = [
    (C, C, "collection", "neon.month", "2024-06", "", "month", "site.avus"),
    (C, C, "collection", "neon.fileCount", "2", "count", "month", "site.avus"),
    (C, F1, "data_object", "neon.productCode", "DP1.00002.001", "", "object", "site.avus"),
    (C, F1, "data_object", "neon.month", "2024-06", "", "object", "site.avus"),
    (C, F2, "data_object", "neon.productCode", "DP1.00002.001", "", "object", "site.avus"),
]


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    store = hermetic_ds(tmp_path, monkeypatch)
    monkeypatch.setattr(bulk.TokenBucket, "acquire", lambda self: 0.0)
    monkeypatch.setattr(tuning.AdaptiveRate, "acquire", lambda self: 0.0)
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.delenv(CLAIMS_ENV, raising=False)
    store.add_coll(C)
    store.add_obj(F1, (("ipc_UUID", "u-1", ""),))
    store.add_obj(F2, (("ipc_UUID", "u-2", ""),))
    return store


def as_vm(monkeypatch: pytest.MonkeyPatch, vm: str) -> None:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", vm)


def journal(con) -> list[dict]:
    return [dict(r) for r in con.execute("SELECT * FROM avu_journal WHERE scope=? ORDER BY id", (SITE,))]


def key(r: dict) -> tuple[str, str, str, str, str]:
    return r["path"], r["attribute"], r["value"], r["unit"] or "", r["op"]


def spooled_keys(ds: DSIrods, vm: str, batch_ids: list[str], tmp_path: Path) -> set[str]:
    """recorder.change_key of every row of these spool batches: what the recorder dedups on."""
    out: set[str] = set()
    for bid in batch_ids:
        local = tmp_path / f"{bid}.parquet"
        local.write_bytes(ds.content(f"{coord.spool_dir(vm)}/{bid}.parquet"))
        out |= {recorder.change_key(c) for c in recorder.read_changes(local)}
    return out


def spool(ds: DSIrods, con) -> list[str]:
    return history.record_scope(SITE, con=con, mode="spool", session=DSSession(ds))["batch_ids"]


def vm_a_backed_up_then_applied(ds: DSIrods, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> object:
    """VM A: builds the chunk's AVUs, pushes its journal (after chunk k-1), then applies chunk k."""
    as_vm(monkeypatch, "vm-a")
    a = state.connect(tmp_path / "a.sqlite")
    bulk.replace_desired(a, SITE, DESIRED)
    journal_sync.push_site_journal(SITE, session=DSSession(ds), con=a)
    stats = bulk.apply_scope(SITE, con=a, session=DSSession(ds), threads=1)
    assert stats["adds"] == len(DESIRED) and ("neon.productCode", "DP1.00002.001", "") in ds.avus(F2)
    return a


def vm_b_takes_over(ds: DSIrods, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> object:
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    assert journal_sync.restore_site_journal(SITE, session=DSSession(ds), con=b) is True
    assert not [r for r in journal(b) if r["status"] in bulk.OWNED_STATUSES]  # A's apply never reached the backup
    return b


def verify_journal(con) -> tuple[set[str], dict]:
    """The journal half of verify_chunk: core AVUs recorded or spooled, nothing pending."""
    return evict.recorded_paths(con, SITE), evict.journal_state(con, SITE, [F1, F2, C])


def test_a_site_taken_over_mid_apply_adopts_the_avus_and_verifies(ds, tmp_path, monkeypatch) -> None:
    """N1: A died after apply(k) and before spooling. B's restored journal has no rows for A's AVUs; the apply
    used to write nothing, so verify failed "core AVUs not recorded or spooled" forever."""
    vm_a_backed_up_then_applied(ds, tmp_path, monkeypatch)
    b = vm_b_takes_over(ds, tmp_path, monkeypatch)
    atomic = ds.calls["atomic"]
    stats = bulk.apply_scope(SITE, con=b, session=DSSession(ds), threads=1)
    assert ds.calls["atomic"] == atomic and stats["adds"] == 0  # nothing written to iRODS
    rows = journal(b)
    assert sorted(key(r) for r in rows) == sorted((p, a, v, u, "add") for _, p, _, a, v, u, _, _ in DESIRED)
    assert {(r["status"], r["op"], r["adopted"]) for r in rows} == {("applied", "add", 1)}
    assert stats["adopted"] == len(DESIRED) and stats["adopted_original_time"] == 0  # A never spooled them
    assert {r["generator"] for r in rows} == {"site.avus"}
    recorded, pending = verify_journal(b)
    assert F1 not in recorded and pending["pending"]  # adopted, not yet in history
    bids = spool(ds, b)
    recorded, pending = verify_journal(b)
    assert {F1, F2} <= recorded and pending == {"pending": [], "errors": []}  # verify now passes
    local = tmp_path / "y.parquet"
    local.write_bytes(ds.content(f"{coord.spool_dir('vm-b')}/{bids[0]}.parquet"))
    assert {c.source for c in recorder.read_changes(local)} == {"neon-ducklake:site.avus+adopted"}
    again = bulk.apply_scope(SITE, con=b, session=DSSession(ds), threads=1)
    assert again.get("adopted", 0) == 0 and len(journal(b)) == len(DESIRED)  # adopted once: now owned


def test_adopted_avus_keep_the_time_of_the_copy_already_spooled(ds, tmp_path, monkeypatch) -> None:
    """N1: A applied and spooled chunk k, then died before its next journal push. B adopts with A's times, so the
    recorder's content dedup keeps one copy in MESA history."""
    a = vm_a_backed_up_then_applied(ds, tmp_path, monkeypatch)
    a_batches = spool(ds, a)
    a_times = {key(r): r["applied_at"] for r in journal(a)}
    b = vm_b_takes_over(ds, tmp_path, monkeypatch)
    stats = bulk.apply_scope(SITE, con=b, session=DSSession(ds), threads=1)
    assert {key(r): r["applied_at"] for r in journal(b)} == a_times
    assert stats["adopted"] == stats["adopted_original_time"] == len(DESIRED)
    b_batches = spool(ds, b)
    assert spooled_keys(ds, "vm-b", b_batches, tmp_path) == spooled_keys(ds, "vm-a", a_batches, tmp_path)


def test_an_avu_owned_by_another_scope_is_not_adopted(ds, tmp_path, monkeypatch) -> None:
    as_vm(monkeypatch, "vm-a")
    con = state.connect(tmp_path / "a.sqlite")
    bulk.replace_desired(con, "catalog", [(C, C, "collection", "neon.month", "2024-06", "", "month", "catalog")])
    bulk.apply_scope("catalog", con=con, session=DSSession(ds), threads=1)
    bulk.replace_desired(con, SITE, DESIRED)
    stats = bulk.apply_scope(SITE, con=con, session=DSSession(ds), threads=1)
    assert not [r for r in journal(con) if r["attribute"] == "neon.month" and r["path"] == C]
    assert stats["adopted"] == 0 and stats["adds"] == len(DESIRED) - 1


def test_reconciled_intents_keep_the_spooled_time(ds, tmp_path, monkeypatch) -> None:
    """N11: A's journal was pushed while apply(k) had written its intents (the push after delete(k-1) runs
    alongside); A then applied and spooled them and died. B settles the intents as applied: with A's times,
    not now, so the recorder's dedup key matches A's spooled copy."""
    as_vm(monkeypatch, "vm-a")
    path = tmp_path / "a.sqlite"
    a = state.connect(path)
    bulk.replace_desired(a, SITE, DESIRED)
    pushed: dict = {}
    real = FakeMeta.apply_atomic_operations

    def journal_pushed_mid_apply(self, model, target, *ops):
        if not pushed:  # the orchestrator's push (another process) while the intents are written
            other = state.connect(path)
            pushed.update(journal_sync.push_site_journal(SITE, session=DSSession(ds), con=other))
            other.close()
        return real(self, model, target, *ops)

    monkeypatch.setattr(FakeMeta, "apply_atomic_operations", journal_pushed_mid_apply)
    bulk.apply_scope(SITE, con=a, session=DSSession(ds), threads=1)
    monkeypatch.setattr(FakeMeta, "apply_atomic_operations", real)
    assert pushed["tables"]["avu_journal"] == len(DESIRED)
    a_batches = spool(ds, a)
    a_times = {key(r): r["applied_at"] for r in journal(a)}
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    journal_sync.restore_site_journal(SITE, session=DSSession(ds), con=b)
    assert {r["status"] for r in journal(b)} == {"intent"}
    stats = bulk.apply_scope(SITE, con=b, session=DSSession(ds), threads=1)
    assert stats["intents_reconciled"] == len(DESIRED) and stats["adds"] == 0 and stats.get("adopted", 0) == 0
    rows = journal(b)
    assert {r["status"] for r in rows} == {"applied"}
    assert {key(r): r["applied_at"] for r in rows} == a_times
    b_batches = spool(ds, b)
    assert spooled_keys(ds, "vm-b", b_batches, tmp_path) == spooled_keys(ds, "vm-a", a_batches, tmp_path)


def test_a_reconciled_intent_nobody_spooled_gets_the_current_time(ds, tmp_path, monkeypatch) -> None:
    """The same VM crashed mid-write (no spooled copy anywhere): the intent is settled with the current time."""
    as_vm(monkeypatch, "vm-a")
    con = state.connect(tmp_path / "a.sqlite")
    bulk.replace_desired(con, SITE, DESIRED[:1])
    con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, generator,"
                " status) VALUES (?, ?, ?, 'collection', 'neon.month', '2024-06', '', 'add', 'site.avus', 'intent')",
                (SITE, C, C))
    ds.colls[C]["avus"].add(("neon.month", "2024-06", ""))  # the call went through before the crash
    stats = bulk.apply_scope(SITE, con=con, session=DSSession(ds), threads=1)
    (row,) = journal(con)
    assert stats["intents_reconciled"] == 1 and row["status"] == "applied" and row["applied_at"]
    assert history.parse_ts(row["applied_at"]).tzinfo is not None
