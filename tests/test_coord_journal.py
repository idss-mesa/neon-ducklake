"""journal_sync: push a site's journal rows to the (fake) Data Store and restore them on another VM."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from neon_ducklake import env, journal_sync, state
from neon_ducklake.site import chunks

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)

JDIR = f"{ROOT}/sites/SRER/.mesa-journal"


def seed(con, site: str = "SRER", n: int = 3) -> None:
    chunks.ensure_schema(con)
    for i in range(n):
        con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status, up_status, irods_checksum)"
                    " VALUES (?,?,?,?,?,?,?,?)", (site, "DP1.00002.001", "2024-06", f"f{i}.csv", 100 + i, "ok",
                                                  "uploaded", f"md5-{i}"))
        con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, op, status,"
                    " applied_at, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (site, "u", f"/p/{site}/f{i}", "data_object", "neon.k", f"v{i}", "add", "spooled",
                     "2026-09-27T15:00:00+00:00", "b-1"))
    con.execute("INSERT INTO listings(site, product, part, n_files, status) VALUES (?, 'DP1.00002.001', 'all', ?, 'ok')",
                (site, n))
    con.execute("INSERT INTO batches(batch_id, scope, n_rows, status) VALUES (?, ?, ?, 'spooled')", (f"b-{site}", site, n))
    con.execute("INSERT INTO stream_chunks(site, chunk_id, product, months, n_files, bytes, status) VALUES "
                "(?, 'DP1.00002.001__2024-06_2024-06', 'DP1.00002.001', '[\"2024-06\"]', ?, 1000, 'deleted')",
                (site, n))


def rows(con, table: str, where: str, site: str, drop: tuple[str, ...] = (), only: set[str] | None = None
         ) -> list[dict]:
    out = [dict(r) for r in con.execute(f"SELECT * FROM {table} WHERE {where}=? ORDER BY rowid", (site,))]
    return [{k: v for k, v in r.items() if k not in drop and (only is None or k in only)} for r in out]


def pointer(ds: DSIrods) -> dict:
    return json.loads(ds.content(f"{JDIR}/CURRENT"))


def test_push_writes_a_versioned_set_and_the_pointer_last(ds: DSIrods, tmp_path: Path) -> None:
    con = state.connect(tmp_path / "a.sqlite")
    seed(con)
    seed(con, "HARV", 2)
    out = journal_sync.push_site_journal("srer", session=DSSession(ds), con=con)
    assert {t: n for t, n in out["tables"].items() if n} == {"site_files": 3, "listings": 1, "stream_chunks": 1,
                                                            "avu_journal": 3, "batches": 1}
    present = {t for t in journal_sync.TABLES if journal_sync._exists(con, t)}
    assert set(out["tables"]) == present and "stream_sites" in present
    ptr = pointer(ds)
    setdir = f"{JDIR}/v/{out['pushId']}"
    assert ptr["pushId"] == out["pushId"] and ptr["manifest"] == f"v/{out['pushId']}/manifest.json"
    assert ptr["prev"] == [] and ptr["vm"] == env.vm_id() and ptr["pushedAt"] == out["pushedAt"]
    names = sorted(p.rsplit("/", 1)[1] for p in ds.objs if p.startswith(setdir + "/"))
    assert names == sorted([f"{t}.parquet" for t in present] + ["manifest.json"])
    assert {p for p in ds.objs if p.startswith(JDIR + "/") and not p.startswith(setdir + "/")} == {f"{JDIR}/CURRENT"}
    raw = ds.content(f"{setdir}/manifest.json")
    assert ptr["manifestSha256"] == hashlib.sha256(raw).hexdigest()
    doc = json.loads(raw)
    assert doc["site"] == "SRER" and doc["vm"] == env.vm_id() and doc["tables"]["avu_journal"]["rows"] == 3
    assert doc["pushId"] == out["pushId"]
    assert doc["tables"]["avu_desired"] is None  # not on this host: skipped
    assert doc["tables"]["stream_chunks"]["ddl"].startswith("CREATE TABLE")
    assert {c["name"] for c in doc["tables"]["stream_chunks"]["columns"]} >= {"done", "info", "planned_at"}
    assert all(ds.objs[f"{setdir}/{t}.parquet"]["replicas"][0]["checksum"] for t in out["tables"])
    assert [p for p, _, _ in ds.put_log][-1] == f"{setdir}/manifest.json"  # the manifest goes last in the set
    moves = [m for op, m in ds.ops if op == "move"]
    assert moves == [f"{JDIR}/staged/{out['pushId']}.json -> {JDIR}/CURRENT"]  # then the pointer: write, move
    assert not (env.WORK_DIR / "journal-push" / "SRER").exists()
    # the next push is a new set; the pointer keeps the previous one in its chain
    again = journal_sync.push_site_journal("SRER", session=DSSession(ds), con=con)
    assert again["pushId"] != out["pushId"] and again["prev"] == [out["pushId"]]
    assert pointer(ds)["pushId"] == again["pushId"] and f"{setdir}/manifest.json" in ds.objs
    assert ds.content(f"{setdir}/manifest.json") == raw  # never overwritten in place


def test_restore_rebuilds_the_site_on_another_vm(ds: DSIrods, tmp_path: Path) -> None:
    a = state.connect(tmp_path / "a.sqlite")
    seed(a)
    journal_sync.push_site_journal("SRER", session=DSSession(ds), con=a)
    b = state.connect(tmp_path / "b.sqlite")  # the VM taking over: its own rows for another site
    seed(b, "HARV", 2)
    b.execute("INSERT INTO site_files(site, product, month, name) VALUES ('SRER', 'X', '2020-01', 'stale.csv')")
    assert journal_sync.local_rows("SRER", con=b) == 1
    out = journal_sync.restore_detail("SRER", session=DSSession(ds), con=b)
    assert out["restored"] and out["tables"]["site_files"] == 3 and out["from_vm"] == env.vm_id()
    for table, key in journal_sync.TABLES.items():
        if not journal_sync._exists(a, table):
            continue
        drop = journal_sync.DROP_ON_RESTORE.get(table, ())
        cols = {c for c, _ in journal_sync._columns(a, table)}  # b may have more (e.g. site_files.local_evicted)
        assert rows(b, table, key, "SRER", drop, cols) == rows(a, table, key, "SRER", drop, cols), table
    assert len(rows(b, "site_files", "site", "HARV")) == 2  # other sites untouched
    ids = [r["id"] for r in b.execute("SELECT id FROM avu_journal")]
    assert len(ids) == len(set(ids)) == 5  # re-assigned ids never collide with the local ones
    assert not (env.WORK_DIR / "journal-restore" / "SRER").exists()


def test_restore_without_a_backup_or_with_a_bad_file(ds: DSIrods, tmp_path: Path) -> None:
    con = state.connect(tmp_path / "a.sqlite")
    assert journal_sync.restore_site_journal("SRER", session=DSSession(ds), con=con, force=True) is False
    assert journal_sync.restore_detail("SRER", session=DSSession(ds), con=con)["restored"] is False
    seed(con)
    journal_sync.push_site_journal("SRER", session=DSSession(ds), con=con)
    b = state.connect(tmp_path / "b.sqlite")
    setdir = f"{JDIR}/" + pointer(ds)["manifest"].rsplit("/", 1)[0]
    ds.set_content(f"{setdir}/site_files.parquet", b"not the pushed file", register=True)  # iRODS agrees with it
    with pytest.raises(journal_sync.JournalSetBroken, match="does not match its manifest"):
        journal_sync.restore_site_journal("SRER", session=DSSession(ds), con=b)  # b has no rows: needs restore
    assert journal_sync.local_rows("SRER", con=b) == 0  # nothing half-restored


def test_mixed_types_survive_the_round_trip(ds: DSIrods, tmp_path: Path) -> None:
    a = state.connect(tmp_path / "a.sqlite")
    a.execute("INSERT INTO site_files(site, product, month, name, size) VALUES ('SRER', 'P', 'm', 'x', 'n/a')")
    a.execute("INSERT INTO site_files(site, product, month, name, size) VALUES ('SRER', 'P', 'm', 'y', 7)")
    journal_sync.push_site_journal("SRER", session=DSSession(ds), con=a)
    b = state.connect(tmp_path / "b.sqlite")
    assert journal_sync.restore_site_journal("SRER", session=DSSession(ds), con=b) is True
    got = {r["name"]: (r["size"], r["t"]) for r in b.execute("SELECT name, size, typeof(size) AS t FROM site_files")}
    assert got == {"x": ("n/a", "text"), "y": (7, "integer")}
