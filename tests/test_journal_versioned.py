"""journal_sync, round 3: immutable versioned journal sets behind a CURRENT pointer (N2), one read snapshot per
export and the repair of torn backups on restore (N6), and the claim re-check plus the compare-and-swap of the
pointer (N7)."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from neon_ducklake import coord, env, journal_sync, mirror, state
from neon_ducklake.avu import history

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds, put

SITE = "SRER"
J = f"{ROOT}/sites/{SITE}/.mesa-journal"
CURRENT = f"{J}/CURRENT"
CLAIM = f"{ROOT}/sites/{SITE}/.claim"
CLAIMS_ENV = "NEON_DUCKLAKE_SITE_CLAIMS"  # coord.SITE_CLAIMS_ENV


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    store = hermetic_ds(tmp_path, monkeypatch)
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    monkeypatch.delenv(CLAIMS_ENV, raising=False)
    monkeypatch.setattr(env, "utcnow", store.clock)  # one clock for the hosts and the Data Store (ds.tick)
    return store


def as_vm(monkeypatch: pytest.MonkeyPatch, vm: str) -> None:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", vm)


def add_file(con, name: str) -> None:
    con.execute("INSERT INTO site_files(site, product, month, name, dl_status) VALUES (?, 'DP1.00002.001',"
                " '2024-06', ?, 'ok')", (SITE, name))


def names(con) -> list[str]:
    return sorted(r[0] for r in con.execute("SELECT name FROM site_files WHERE site=?", (SITE,)))


def pointer(ds: DSIrods) -> dict:
    return json.loads(ds.content(CURRENT))


def set_dir(entry: dict) -> str:
    return f"{J}/" + entry["manifest"].rsplit("/", 1)[0]


def set_ids(ds: DSIrods) -> set[str]:
    return {p[len(J) + 3:].split("/", 1)[0] for p in ds.objs if p.startswith(f"{J}/v/")}


def corrupt(ds: DSIrods, entry: dict, table: str = "site_files") -> None:
    """Replace a table file of a set; iRODS registers the new checksum, so only the manifest can tell."""
    ds.set_content(f"{set_dir(entry)}/{table}.parquet", b"garbage", register=True)


# ---------------------------------------------------------------- N2: versioned sets, fallback, pruning

def test_an_interrupted_push_leaves_the_previous_backup_restorable(ds, tmp_path, monkeypatch) -> None:
    """The reviewer's scenario: an iRODS outage cuts a push after some tables were written. The old layout
    replaced <table>.parquet in place and left the old manifest, so every later restore failed its sha256."""
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "a.csv")
    first = journal_sync.push_site_journal(SITE, session=s, con=a)
    add_file(a, "b.csv")
    real = mirror.push_tree

    def outage(items, **kw):
        real(items[:3], **kw)  # a few tables (site_files among them) made it to the Data Store ...
        raise RuntimeError("SYS_SOCK_READ_TIMEDOUT")  # ... then the connection died

    monkeypatch.setattr(mirror, "push_tree", outage)
    with pytest.raises(RuntimeError, match="TIMEDOUT"):
        journal_sync.push_site_journal(SITE, session=s, con=a)
    monkeypatch.setattr(mirror, "push_tree", real)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    assert journal_sync.restore_site_journal(SITE, session=s, con=b) is True
    assert names(b) == ["a.csv"]
    assert pointer(ds)["pushId"] == first["pushId"]  # nothing was published
    assert set_ids(ds) == {first["pushId"]}  # the failed push removed its partial set


def test_restore_falls_back_along_the_chain_and_may_then_push(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    pushes = []
    for name in ("a.csv", "b.csv", "c.csv"):
        add_file(a, name)
        pushes.append(journal_sync.push_site_journal(SITE, session=s, con=a)["pushId"])
    ptr = pointer(ds)
    assert ptr["pushId"] == pushes[2] and [e["pushId"] for e in ptr["prev"]] == [pushes[1], pushes[0]]
    corrupt(ds, ptr)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    out = journal_sync.restore_detail(SITE, session=s, con=b)
    assert out["restored"] and out["fallback"] == pushes[1] and out["pushId"] == pushes[1]
    assert [e["pushId"] for e in out["skipped"]] == [pushes[2]] and "manifest" in out["skipped"][0]["error"]
    assert names(b) == ["a.csv", "b.csv"]
    # the restore is marked with the pointer it saw: B continues and replaces the broken head
    assert not journal_sync.needs_restore(SITE, session=s, con=b)
    add_file(b, "d.csv")
    mine = journal_sync.push_site_journal(SITE, session=s, con=b)
    assert pointer(ds)["pushId"] == mine["pushId"] and mine["prev"] == [pushes[2], pushes[1]]


def test_a_broken_manifest_or_missing_file_also_falls_back_but_all_broken_raises(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    for name in ("a.csv", "b.csv", "c.csv"):
        add_file(a, name)
        journal_sync.push_site_journal(SITE, session=s, con=a)
    ptr = pointer(ds)
    head, mid, first = ptr, ptr["prev"][0], ptr["prev"][1]
    ds.set_content(f"{J}/{head['manifest']}", b'{"tables": {}}', register=True)  # not the pointer's manifest
    DSSession(ds).data_objects.unlink(f"{set_dir(mid)}/site_files.parquet")
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    out = journal_sync.restore_detail(SITE, session=s, con=b)
    assert out["fallback"] == first["pushId"] and names(b) == ["a.csv"]
    assert "pointer" in out["skipped"][0]["error"] and "missing" in out["skipped"][1]["error"]
    corrupt(ds, first)
    c = state.connect(tmp_path / "c.sqlite")
    with pytest.raises(journal_sync.JournalSetBroken, match="no journal set of SRER"):
        journal_sync.restore_detail(SITE, session=s, con=c)
    assert journal_sync.local_rows(SITE, con=c) == 0


def test_a_transient_error_is_raised_not_fallen_back_from(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    for name in ("a.csv", "b.csv"):
        add_file(a, name)
        journal_sync.push_site_journal(SITE, session=s, con=a)
    real = coord.fetch

    def flaky(session, remote, local, **kw):
        if remote.endswith("site_files.parquet"):
            raise ConnectionResetError("iRODS agent went away")
        return real(session, remote, local, **kw)

    monkeypatch.setattr(coord, "fetch", flaky)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    with pytest.raises(ConnectionResetError):
        journal_sync.restore_detail(SITE, session=s, con=b)
    assert journal_sync.local_rows(SITE, con=b) == 0 and journal_sync.needs_restore(SITE, session=s, con=b)


def test_only_the_pointer_chain_of_three_sets_is_kept(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    ids = []
    for i in range(3):
        add_file(a, f"f{i}.csv")
        ids.append(journal_sync.push_site_journal(SITE, session=s, con=a)["pushId"])
    add_file(a, "young.csv")
    ids.append(journal_sync.push_site_journal(SITE, session=s, con=a)["pushId"])
    assert set_ids(ds) == set(ids)  # the set that left the chain is younger than PRUNE_GRACE_S: kept for now
    ds.tick(journal_sync.PRUNE_GRACE_S + 1)
    add_file(a, "last.csv")
    out = journal_sync.push_site_journal(SITE, session=s, con=a)
    ids.append(out["pushId"])
    assert set_ids(ds) == set(ids[-3:]) and out["pruned"] > 0
    assert [e["pushId"] for e in pointer(ds)["prev"]] == ids[-2:-4:-1]
    assert not [p for p in ds.objs if p.startswith((f"{J}/staged/", f"{J}/retired/"))]


def test_a_flat_layout_backup_is_restored_then_replaced(ds, tmp_path, monkeypatch) -> None:
    """Backups pushed before this change (J/<table>.parquet + J/manifest.json) are still read."""
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "old.csv")
    work = tmp_path / "legacy"
    tables = journal_sync.export_tables(a, SITE, work)
    for t, info in tables.items():
        if info is not None:
            put(ds, f"{J}/{t}.parquet", (work / f"{t}.parquet").read_bytes())
    put(ds, f"{J}/manifest.json", json.dumps({"site": SITE, "vm": "vm-old", "host": "h",
                                              "pushedAt": "2026-09-01T00:00:00+00:00", "tables": tables}).encode())
    s = DSSession(ds)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    assert journal_sync.read_pointer(SITE, session=s)["state"] == "legacy"
    assert journal_sync.restore_site_journal(SITE, session=s, con=b) is True and names(b) == ["old.csv"]
    ds.tick(journal_sync.PRUNE_GRACE_S + 1)
    add_file(b, "new.csv")
    out = journal_sync.push_site_journal(SITE, session=s, con=b)
    assert pointer(ds)["prev"][0]["manifest"] == "manifest.json"  # the flat set stays in the chain
    for _ in range(2):
        out = journal_sync.push_site_journal(SITE, session=s, con=b)
        ds.tick(journal_sync.PRUNE_GRACE_S + 1)
    out = journal_sync.push_site_journal(SITE, session=s, con=b)
    assert f"{J}/manifest.json" not in ds.objs and f"{J}/site_files.parquet" not in ds.objs  # left the chain
    assert out["pushId"] in set_ids(ds)


# ---------------------------------------------------------------- N6: one snapshot; torn backups repaired

def spooling_journal(con) -> list[int]:
    """Two applied rows of batch b-1, which history is spooling right now."""
    history.ensure_batches_vm(con)
    ids = []
    for i in range(2):
        cur = con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op,"
                          " generator, status, applied_at, batch_id) VALUES (?, 'u', ?, 'data_object', 'neon.k',"
                          " 'v', '', 'add', 'g', 'applied', '2026-09-28T10:00:00+00:00', 'b-1')",
                          (SITE, f"{ROOT}/sites/{SITE}/raw/f{i}"))
        ids.append(cur.lastrowid)
    con.execute("INSERT INTO batches(batch_id, scope, n_rows, status, created_at, vm) VALUES ('b-1', ?, 2,"
                " 'spooling', '2026-09-28T10:00:00+00:00', 'vm-a')", (SITE,))
    return ids


def test_the_export_is_one_snapshot_of_the_journal(ds, tmp_path, monkeypatch) -> None:
    """history(k+1) marks its batch spooled while the push after delete(k) exports: the backup must hold the
    rows and the batch from the same moment (the old per-table autocommit SELECTs mixed them)."""
    s = DSSession(ds)
    path = tmp_path / "a.sqlite"
    a = state.connect(path)
    spooling_journal(a)
    real = journal_sync.export_table

    def export_then_history_commits(con, table, site, out):
        info = real(con, table, site, out)
        if table == "avu_journal":  # the history stage (another process) finishes its batch right now
            other = state.connect(path)
            history._mark_spooled(other, "b-1", " | spooled")
            other.close()
        return info

    monkeypatch.setattr(journal_sync, "export_table", export_then_history_commits)
    journal_sync.push_site_journal(SITE, session=s, con=a)
    monkeypatch.setattr(journal_sync, "export_table", real)
    assert {r[0] for r in a.execute("SELECT status FROM avu_journal")} == {"spooled"}  # it did commit
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    journal_sync.restore_site_journal(SITE, session=s, con=b)
    rows = {(r["status"], r["batch_id"]) for r in b.execute("SELECT status, batch_id FROM avu_journal")}
    batch = b.execute("SELECT status FROM batches WHERE batch_id='b-1'").fetchone()["status"]
    assert (rows, batch) == ({("applied", "b-1")}, "spooling")  # one consistent moment: before the commit


def test_restore_releases_applied_rows_of_a_finished_or_unknown_batch(ds, tmp_path, monkeypatch) -> None:
    """A backup torn by the old export: rows 'applied' with batch_id set, the batch already 'spooled' (or gone).
    They must be spooled again, not stuck forever outside plan_batches."""
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    history.ensure_batches_vm(a)
    ins = ("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, generator, status,"
           " applied_at, batch_id) VALUES (?, 'u', ?, 'data_object', 'neon.k', 'v', '', 'add', 'g', 'applied',"
           " '2026-09-28T10:00:00+00:00', ?)")
    for i, batch in enumerate(("b-spooled", "b-spooled", "b-gone", "b-flight", "b-direct")):
        a.execute(ins, (SITE, f"{ROOT}/sites/{SITE}/raw/f{i}", batch))
    for batch, status, snap in (("b-spooled", "spooled", None), ("b-flight", "spooling", None),
                                ("b-direct", "recorded", 42)):
        a.execute("INSERT INTO batches(batch_id, scope, n_rows, status, snapshot_id, created_at, vm) VALUES"
                  " (?, ?, 1, ?, ?, '2026-09-28T10:00:00+00:00', 'vm-a')", (batch, SITE, status, snap))
    journal_sync.push_site_journal(SITE, session=s, con=a)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    out = journal_sync.restore_detail(SITE, session=s, con=b)
    planned = history.plan_batches(b, SITE)
    assert sorted(r["path"].rsplit("/", 1)[1] for batch in planned for r in batch) == ["f0", "f1", "f2"]
    got = {r["path"].rsplit("/", 1)[1]: (r["status"], r["batch_id"], r["snapshot_id"])
           for r in b.execute("SELECT * FROM avu_journal")}
    assert got == {"f0": ("applied", None, None), "f1": ("applied", None, None), "f2": ("applied", None, None),
                   "f3": ("applied", "b-flight", None), "f4": ("recorded", "b-direct", 42)}
    assert out["batches"] == {"released": 3, "recorded": 1}


# ---------------------------------------------------------------- N7: claim re-check, compare-and-swap

def steal(ds: DSIrods) -> None:
    doc = json.loads(ds.content(CLAIM)) | {"vm": "vm-b", "host": "elsewhere", "token": "theirs"}
    ds.set_content(CLAIM, json.dumps(doc).encode())


def test_a_push_rechecks_the_claim_right_before_the_pointer(ds, tmp_path, monkeypatch) -> None:
    """A VM suspended past its claim resumes mid-push: its set is uploaded, but the pointer is not replaced."""
    s = DSSession(ds)
    monkeypatch.setenv(CLAIMS_ENV, "on")
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "a.csv")
    real = mirror.push_tree
    with coord.site_claim(SITE, session=s, vm="vm-a", heartbeat_s=3600):
        first = journal_sync.push_site_journal(SITE, session=s, con=a)
        add_file(a, "b.csv")

        def upload_then_lose_the_claim(items, **kw):
            out = real(items, **kw)
            if items[0][1].endswith("/manifest.json"):
                steal(ds)  # the new owner broke the stale claim while this VM was uploading
            return out

        monkeypatch.setattr(mirror, "push_tree", upload_then_lose_the_claim)
        with pytest.raises(journal_sync.JournalConflict, match="claim lost"):  # ClaimLost
            journal_sync.push_site_journal(SITE, session=s, con=a)
    assert pointer(ds)["pushId"] == first["pushId"]
    assert set_ids(ds) == {first["pushId"]}
    assert not [p for p in ds.objs if p.startswith((f"{J}/staged/", f"{J}/retired/"))]
    assert journal_sync.last_mark(SITE, con=a)["push_id"] == first["pushId"]


def test_a_push_without_the_claim_in_force_is_refused(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    monkeypatch.setenv(CLAIMS_ENV, "on")
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "a.csv")
    with pytest.raises(journal_sync.JournalConflict, match="claim lost"):  # ClaimLost
        journal_sync.push_site_journal(SITE, session=s, con=a)  # the claim is gone (released or deleted)
    assert CURRENT not in ds.objs and not set_ids(ds)
    assert journal_sync.main("push", SITE, session=s) == 1  # the CLI entry point: rc 1, nothing raised


def test_two_racing_pushes_one_wins_the_other_publishes_nothing(ds, tmp_path, monkeypatch) -> None:
    """Both pushers checked the same pointer; the second one's swap must see that the first replaced it (the old
    conflict check and the manifest write were two separate steps)."""
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "a.csv")
    base = journal_sync.push_site_journal(SITE, session=s, con=a)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    journal_sync.restore_site_journal(SITE, session=s, con=b)  # B continued from the same backup
    add_file(b, "theirs.csv")
    as_vm(monkeypatch, "vm-a")
    add_file(a, "mine.csv")
    real = mirror.push_tree
    here = (env.LOCK_DIR, env.WORK_DIR)
    won: dict = {}

    def b_pushes_meanwhile(items, **kw):
        out = real(items, **kw)
        if items[0][1].endswith("/manifest.json") and not won:
            monkeypatch.setattr(mirror, "push_tree", real)
            monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "b" / "locks")  # another host
            monkeypatch.setattr(env, "WORK_DIR", tmp_path / "b" / "work")
            as_vm(monkeypatch, "vm-b")
            won.update(journal_sync.push_site_journal(SITE, session=s, con=b))
            as_vm(monkeypatch, "vm-a")
            monkeypatch.setattr(env, "LOCK_DIR", here[0])
            monkeypatch.setattr(env, "WORK_DIR", here[1])
        return out

    monkeypatch.setattr(mirror, "push_tree", b_pushes_meanwhile)
    with pytest.raises(journal_sync.JournalConflict, match="replaced by vm-b"):
        journal_sync.push_site_journal(SITE, session=s, con=a)
    assert pointer(ds)["pushId"] == won["pushId"] and won["prev"] == [base["pushId"]]
    assert set_ids(ds) == {base["pushId"], won["pushId"]}  # A's set was deleted, B's is published
    assert not [p for p in ds.objs if p.startswith((f"{J}/staged/", f"{J}/retired/"))]
    c = state.connect(tmp_path / "c.sqlite")
    journal_sync.restore_site_journal(SITE, session=s, con=c, force=True)
    assert names(c) == ["a.csv", "theirs.csv"]


def swap_interrupted(ds: DSIrods, s, tmp_path) -> dict:
    """A pusher died between its two renames: CURRENT moved to retired/<X>, its new pointer still staged/<X>."""
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "a.csv")
    first = journal_sync.push_site_journal(SITE, session=s, con=a)
    new = dict(pointer(ds), pushId="X", prev=[])
    coord.put_json(s, f"{J}/staged/X.json", new)
    assert coord.rename(s, CURRENT, f"{J}/retired/X.json") == "ok"
    return first


def test_readers_see_the_committed_pointer_during_a_swap(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    first = swap_interrupted(ds, s, tmp_path)
    ptr = journal_sync.read_pointer(SITE, session=s)
    assert ptr["state"] == "swapping" and ptr["pushId"] == first["pushId"] and ptr["swapId"] == "X"
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    assert journal_sync.needs_restore(SITE, session=s, con=b)
    assert journal_sync.restore_site_journal(SITE, session=s, con=b) and names(b) == ["a.csv"]
    add_file(b, "b.csv")
    with pytest.raises(journal_sync.JournalConflict, match="swapping"):
        journal_sync.push_site_journal(SITE, session=s, con=b)  # a swap in flight: never pushed over


def test_this_hosts_own_interrupted_swap_is_rolled_back_at_once(ds, tmp_path, monkeypatch) -> None:
    """The push that died was this VM's (a restarted orchestrator): the host lock proves it is not running."""
    s = DSSession(ds)
    first = swap_interrupted(ds, s, tmp_path)
    a = state.connect(tmp_path / "a.sqlite")
    add_file(a, "b.csv")
    out = journal_sync.push_site_journal(SITE, session=s, con=a)
    assert pointer(ds)["pushId"] == out["pushId"] and out["prev"] == [first["pushId"]]


def test_an_abandoned_swap_is_rolled_back_by_the_next_push(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    first = swap_interrupted(ds, s, tmp_path)
    ds.mtimes[f"{J}/staged/X.json"] -= timedelta(seconds=journal_sync.SWAP_GRACE_S + 1)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    journal_sync.restore_site_journal(SITE, session=s, con=b)
    add_file(b, "b.csv")
    out = journal_sync.push_site_journal(SITE, session=s, con=b)
    assert pointer(ds)["pushId"] == out["pushId"] and out["prev"] == [first["pushId"]]
    assert f"{J}/staged/X.json" not in ds.objs and f"{J}/retired/X.json" not in ds.objs
