"""journal_sync on a VM that takes a site over: full schema (B1), the extra tables (B2), needs_restore marks
(M2/M3), the push guard, and re-planning chunks whose raw files stayed on the other VM."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from neon_ducklake import env, journal_sync, state
from neon_ducklake.avu import bulk, history
from neon_ducklake.site import chunks, evict
from neon_ducklake.site.fetch import SUPERSEDED_SCHEMA

from .test_coord_fakes import DSIrods, DSSession, hermetic_ds

SITE = "SRER"
P = "DP1.00002.001"
RUN_CHUNKS_DDL = """CREATE TABLE IF NOT EXISTS site_run_chunks (
  site TEXT NOT NULL, chunk_id TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
  attempt INTEGER NOT NULL DEFAULT 0, started_at TEXT, finished_at TEXT, rc INTEGER, log_path TEXT,
  mode TEXT, info TEXT, PRIMARY KEY (site, chunk_id, stage)
)"""


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)


def as_vm(monkeypatch: pytest.MonkeyPatch, vm: str) -> None:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", vm)


def full_journal(con, *, local_files: bool = True) -> None:
    """VM A's journal with every table the streaming path writes, plus a column only A has."""
    chunks.ensure_schema(con)
    evict.ensure_schema(con)
    con.executescript(SUPERSEDED_SCHEMA)
    bulk.ensure_desired(con)
    history.ensure_batches_vm(con)
    con.execute(RUN_CHUNKS_DDL)
    con.execute("ALTER TABLE site_files ADD COLUMN future_col TEXT")
    rows = [  # (month, name, dl_status, local_evicted)
        ("2024-01", "a.csv", "ok", 1), ("2024-02", "b.csv", "ok", 0), ("2024-03", "c.csv", "ok", 0),
        ("2024-04", "d.csv", "ok", 0)]
    for month, name, dl, ev in rows:
        rel = f"sites/{SITE}/raw/{P}/{month}/{name}"
        con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status, dl_bytes, local_path,"
                    " up_status, local_evicted, future_col) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (SITE, P, month, name, 3, dl, 3, rel, "uploaded", ev, f"x-{name}"))
        if local_files and not ev:
            f = env.ROOT / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"abc")
    con.execute("INSERT INTO fetch_superseded(site, product, month, name, was_downloaded, superseded_at) VALUES"
                " (?, ?, '2024-01', 'old.csv', 1, '2026-09-01')", (SITE, P))
    for cid, month, status, info in (
            (f"{P}__2024-01_2024-01", "2024-01", "deleted", {"delete": {"files": 1}}),
            (f"{P}__2024-02_2024-02", "2024-02", "verified", {"verify": {"ok": True, "at": "2026-09-27"}}),
            (f"{P}__2024-03_2024-03", "2024-03", "avus", {"avus": {"rows": 12}}),
            (f"{P}__2024-04_2024-04", "2024-04", "downloaded", {})):
        con.execute("INSERT INTO stream_chunks(site, chunk_id, product, months, n_files, bytes, status, done, info,"
                    " planned_at) VALUES (?,?,?,?,1,3,?,?,?, '2026-09-26')",
                    (SITE, cid, P, json.dumps([month]), status, status, json.dumps(info)))
    for step, status in (("terms", "ok"), ("cards", "ok")):
        chunks.set_step(con, SITE, step, status)
    for scope in (SITE, "HARV"):
        con.execute("INSERT INTO avu_desired(scope, unit_id, path, target_type, attribute, value, unit, level,"
                    " generator) VALUES (?, 'u', ?, 'data_object', 'neon.k', 'v', '', 'object', 'g')",
                    (scope, f"/p/{scope}/f"))
    con.execute("INSERT INTO lake_pushes(site, catalog_md5, pushed_at, snapshot_id) VALUES (?, 'abc', 't', 7)",
                (SITE,))  # chunks.ensure_schema (stream-core) owns this table; its id is host-local
    con.execute("INSERT INTO site_run_chunks(site, chunk_id, stage, status, rc) VALUES (?, ?, 'delete', 'ok', 0)",
                (SITE, f"{P}__2024-01_2024-01"))
    con.execute("INSERT INTO batches(batch_id, scope, n_rows, status, note, vm) VALUES ('b-1', ?, 2, 'spooling',"
                " 'neon-ducklake SRER spool batch=b-1 vm=vm-a', 'vm-a')", (SITE,))


def table(con, name: str, key: str = "site") -> list[dict]:
    return [dict(r) for r in con.execute(f"SELECT * FROM {name} WHERE {key}=? ORDER BY rowid", (SITE,))]


def test_restore_into_a_fresh_state_keeps_every_column_and_table(ds, tmp_path, monkeypatch) -> None:
    """B1 + B2: a fresh state.sqlite on VM B gets done/info/planned_at, local_evicted, A's extra column and the
    stream_sites, fetch_superseded, avu_desired (site rows only), lake_pushes and site_run_chunks rows."""
    as_vm(monkeypatch, "vm-a")
    a = state.connect(tmp_path / "a.sqlite")
    full_journal(a)
    chunks.set_step(a, SITE, "upload", "ok")
    out = journal_sync.push_site_journal(SITE, session=DSSession(ds), con=a)
    assert out["tables"]["avu_desired"] == 1 and out["tables"]["lake_pushes"] == 1
    assert out["tables"]["site_run_chunks"] == 1 and out["tables"]["fetch_superseded"] == 1
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")  # a VM that never ran a chunk: none of the module tables exist
    chunks.ensure_schema(b)
    b.execute("INSERT INTO lake_pushes(site, catalog_md5, pushed_at) VALUES ('HARV', 'h', 't')")  # id 1 is taken
    b.close()
    b = state.connect(tmp_path / "b.sqlite")
    assert journal_sync.restore_site_journal(SITE, session=DSSession(ds), con=b) is True
    got = {r["chunk_id"]: r for r in table(b, "stream_chunks")}
    want = {r["chunk_id"]: r for r in table(a, "stream_chunks")}
    for cid in (f"{P}__2024-01_2024-01", f"{P}__2024-02_2024-02", f"{P}__2024-03_2024-03"):
        assert got[cid] == want[cid], cid  # done, info (info.verify!) and planned_at survive
    assert json.loads(got[f"{P}__2024-02_2024-02"]["info"])["verify"]["ok"] is True
    sf = {r["name"]: r for r in table(b, "site_files")}
    assert sf["a.csv"]["local_evicted"] == 1 and sf["b.csv"]["local_evicted"] == 0  # not dropped
    assert sf["c.csv"]["future_col"] == "x-c.csv"  # a column only A had was ADDED here
    for name in ("fetch_superseded", "stream_sites", "site_run_chunks"):
        assert table(b, name) == table(a, name), name
    drop_id = [{k: v for k, v in r.items() if k != "id"} for r in table(b, "lake_pushes")]
    assert drop_id == [{k: v for k, v in r.items() if k != "id"} for r in table(a, "lake_pushes")]
    assert [r["path"] for r in table(b, "avu_desired", "scope")] == ["/p/SRER/f"]  # the site's rows only
    assert table(b, "batches", "scope")[0]["vm"] == "vm-a"


def test_foreign_restore_replans_chunks_whose_raw_files_are_elsewhere(ds, tmp_path, monkeypatch) -> None:
    as_vm(monkeypatch, "vm-a")
    a = state.connect(tmp_path / "a.sqlite")
    full_journal(a, local_files=False)  # A's raw files are on A's disk, not here
    journal_sync.push_site_journal(SITE, session=DSSession(ds), con=a)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    out = journal_sync.restore_detail(SITE, session=DSSession(ds), con=b)
    st = {r["chunk_id"]: (r["status"], r["done"]) for r in table(b, "stream_chunks")}
    assert st[f"{P}__2024-01_2024-01"] == ("deleted", "deleted")
    assert st[f"{P}__2024-02_2024-02"] == ("verified", "verified")  # uploaded already: raw files not needed
    assert st[f"{P}__2024-03_2024-03"] == ("planned", "planned")  # stacked, but its raw files are not here
    assert st[f"{P}__2024-04_2024-04"] == ("planned", "planned")
    assert sorted(out["chunks"]) == [f"{P}__2024-03_2024-03", f"{P}__2024-04_2024-04"]
    assert out["steps"] == ["cards", "terms"]  # their cards/terms were on A's disk; upload never ran


def test_same_vm_restore_keeps_chunks_with_files_on_disk(ds, tmp_path, monkeypatch) -> None:
    as_vm(monkeypatch, "vm-a")
    a = state.connect(tmp_path / "a.sqlite")
    full_journal(a, local_files=True)
    chunks.set_step(a, SITE, "upload", "ok")
    journal_sync.push_site_journal(SITE, session=DSSession(ds), con=a)
    as_vm(monkeypatch, "vm-b")
    b = state.connect(tmp_path / "b.sqlite")
    out = journal_sync.restore_detail(SITE, session=DSSession(ds), con=b)
    st = {r["chunk_id"]: r["status"] for r in table(b, "stream_chunks")}
    assert st[f"{P}__2024-03_2024-03"] == "avus" and st[f"{P}__2024-04_2024-04"] == "downloaded"
    assert out["chunks"] == [] and out["steps"] == []
    assert chunks.step_status(SITE, "terms", con=b) == "ok"  # upload ok: the finalize outputs are on the DS


def test_needs_restore_follows_the_marks(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    b = state.connect(tmp_path / "b.sqlite")
    as_vm(monkeypatch, "vm-a")
    assert not journal_sync.needs_restore(SITE, session=s, con=a)  # nothing on the Data Store
    a.execute("INSERT INTO site_files(site, product, month, name) VALUES (?, ?, '2024-01', 'a.csv')", (SITE, P))
    journal_sync.push_site_journal(SITE, session=s, con=a)
    assert not journal_sync.needs_restore(SITE, session=s, con=a)  # our own latest push
    assert journal_sync.last_mark(SITE, con=a)["direction"] == "push"

    as_vm(monkeypatch, "vm-b")
    assert journal_sync.needs_restore(SITE, session=s, con=b)  # never seen, pushed by another VM
    assert journal_sync.restore_site_journal(SITE, session=s, con=b) is True
    assert not journal_sync.needs_restore(SITE, session=s, con=b)
    assert journal_sync.restore_site_journal(SITE, session=s, con=b) is False  # current: nothing to do
    b.execute("INSERT INTO site_files(site, product, month, name) VALUES (?, ?, '2024-02', 'b.csv')", (SITE, P))
    journal_sync.push_site_journal(SITE, session=s, con=b)

    as_vm(monkeypatch, "vm-a")  # M3: A has rows and marks of its own, but B pushed later
    assert journal_sync.local_rows(SITE, con=a) > 0 and journal_sync.needs_restore(SITE, session=s, con=a)
    current = f"{journal_sync.coord.journal_dir(SITE)}/CURRENT"
    before, sets = ds.content(current), {p for p in ds.objs if "/.mesa-journal/v/" in p}
    with pytest.raises(journal_sync.JournalConflict, match="vm-b"):
        journal_sync.push_site_journal(SITE, session=s, con=a)  # never over the newer backup
    assert ds.content(current) == before and {p for p in ds.objs if "/.mesa-journal/v/" in p} == sets
    assert journal_sync.restore_site_journal(SITE, session=s, con=a) is True
    assert {r["name"] for r in a.execute("SELECT name FROM site_files WHERE site=?", (SITE,))} == {"a.csv", "b.csv"}
    journal_sync.push_site_journal(SITE, session=s, con=a)  # allowed again
    assert journal_sync.restore_site_journal(SITE, session=s, con=a, force=True) is True  # force: always


def test_a_push_by_another_vm_with_an_older_clock_still_needs_restore(ds, tmp_path, monkeypatch) -> None:
    s = DSSession(ds)
    a = state.connect(tmp_path / "a.sqlite")
    b = state.connect(tmp_path / "b.sqlite")
    as_vm(monkeypatch, "vm-a")
    a.execute("INSERT INTO site_files(site, product, month, name) VALUES (?, ?, '2024-01', 'a.csv')", (SITE, P))
    journal_sync.push_site_journal(SITE, session=s, con=a)
    as_vm(monkeypatch, "vm-b")
    journal_sync.restore_site_journal(SITE, session=s, con=b)
    real = env.utcnow
    monkeypatch.setattr(env, "utcnow", lambda: real() - timedelta(hours=1))  # B's clock is behind
    journal_sync.push_site_journal(SITE, session=s, con=b)
    monkeypatch.setattr(env, "utcnow", real)
    as_vm(monkeypatch, "vm-a")
    assert journal_sync.needs_restore(SITE, session=s, con=a)


def test_host_local_tables_never_travel() -> None:
    assert not set(journal_sync.HOST_LOCAL) & set(journal_sync.TABLES)
    assert {"stream_sites", "fetch_superseded", "avu_desired", "lake_pushes", "site_run_chunks"} <= set(
        journal_sync.TABLES)
