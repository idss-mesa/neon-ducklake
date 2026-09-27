from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from irods.exception import LOCKED_DATA_OBJECT_ACCESS, SYS_COPY_LEN_ERR

from neon_ducklake import env, mirror, state
from neon_ducklake.mirror import push_tree, report_orphans

from .test_avu_fakes import SYS_MAX_CONNECT_COUNT_EXCEEDED, FakeIrods, FakeSession

R = "/iplant/home/tswetnam/neon-ducklake"


@pytest.fixture
def con(tmp_path):
    return state.connect(tmp_path / "state.sqlite")


@pytest.fixture
def tree(tmp_path):
    root = tmp_path / "root"
    files = {
        "ducklake/data/main/products/a.parquet": b"parquet-a",
        "ducklake/data/main/sites/b.parquet": b"parquet-bb",
        "ducklake/neon_catalog.ducklake": b"catalog",
        "README.md": b"# readme",
    }
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return root, [(root / rel, f"{R}/{rel}", None) for rel in files]


def md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def uploads(con):
    return {r["irods_path"]: dict(r) for r in con.execute("SELECT * FROM uploads")}


def test_upload_verify_and_catalog_last(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    stats = push_tree(items, threads=3, session=FakeSession(s), con=con, sleep=lambda _: None)
    assert stats["uploaded"] == 4 and stats["errors"] == 0 and stats["present"] == 0
    assert [p for p, _, _ in s.put_log][-1] == f"{R}/ducklake/neon_catalog.ducklake"
    for p, nthreads, opts in s.put_log:
        assert nthreads == 1 and "regChksum" in opts and "forceFlag" not in opts
    up = uploads(con)
    assert up[f"{R}/README.md"]["status"] == "uploaded"
    assert up[f"{R}/README.md"]["irods_checksum"] == md5(b"# readme")
    assert f"{R}/ducklake/data/main/products" in s.colls
    assert s.calls["cleanup"] == s.calls["clone"]


def test_second_run_skips_everything_and_caches_md5(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    push_tree(items, session=FakeSession(s), con=con, sleep=lambda _: None)
    puts = s.calls["put"]
    stats = push_tree(items, session=FakeSession(s), con=con, sleep=lambda _: None)
    assert s.calls["put"] == puts and stats["present"] == 4 and stats["uploaded"] == 0
    assert stats.get("md5_computed", 0) == 0  # (path, size, mtime) cache hit
    assert con.execute("SELECT count(*) FROM mirror_md5").fetchone()[0] == 4


def test_replica_aware_skip_and_forced_reupload(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    a = f"{R}/ducklake/data/main/products/a.parquet"
    b = f"{R}/ducklake/data/main/sites/b.parquet"
    # a: replica 0 stale, replica 1 good -> present
    s.add_obj(a, content=b"parquet-a", replicas=[
        {"number": 0, "size": 9, "checksum": md5(b"parquet-a"), "status": "0"},
        {"number": 1, "size": 9, "checksum": md5(b"parquet-a"), "status": "1"}])
    # b: good replica but different content -> re-put with force
    s.add_obj(b, content=b"old", replicas=[{"number": 0, "size": 10, "checksum": md5(b"old!"), "status": "1"}])
    stats = push_tree(items, session=FakeSession(s), con=con, sleep=lambda _: None)
    assert stats["present"] == 1 and stats["uploaded"] == 3
    (bput,) = [o for p, _, o in s.put_log if p == b]
    assert "forceFlag" in bput
    assert a not in [p for p, _, _ in s.put_log]


def test_failure_blocks_the_catalog_file(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    s.put_errors = [SYS_COPY_LEN_ERR("boom")]
    stats = push_tree([i for i in items if "README" not in i[1]], threads=1, session=FakeSession(s), con=con,
                      sleep=lambda _: None)
    assert stats["errors"] == 1 and stats["blocked"] == 1
    assert f"{R}/ducklake/neon_catalog.ducklake" not in [p for p, _, _ in s.put_log]
    assert uploads(con)[f"{R}/ducklake/neon_catalog.ducklake"]["status"] == "blocked"


def test_locked_object_is_retried_after_60s(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    s.put_errors = [LOCKED_DATA_OBJECT_ACCESS("locked")]
    slept: list[float] = []
    stats = push_tree(items[:1], session=FakeSession(s), con=con, sleep=slept.append)
    assert slept == [60.0] and stats["uploaded"] == 1 and stats["retries"] == 1
    assert uploads(con)[items[0][1]]["attempts"] == 2


def test_connection_budget_exceeded_halves_concurrency(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    s.put_errors = [SYS_MAX_CONNECT_COUNT_EXCEEDED("SYS_MAX_CONNECT_COUNT_EXCEEDED")]
    stats = push_tree(items, threads=8, session=FakeSession(s), con=con, sleep=lambda _: None)
    assert stats["uploaded"] == 4 and stats["threads_start"] == 8 and stats["threads_final"] == 4
    assert stats["conn_backoffs"] == 1


def test_missing_checksum_is_computed_on_verify(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    s.register_checksum = False
    stats = push_tree(items[:2], session=FakeSession(s), con=con, sleep=lambda _: None)
    assert stats["uploaded"] == 2 and s.calls["chksum"] == 2


def test_big_files_use_four_streams(con, tree, monkeypatch) -> None:
    _root, items = tree
    monkeypatch.setattr(mirror, "BIG_BYTES", 9)
    s = FakeIrods()
    push_tree(items, threads=16, session=FakeSession(s), con=con, sleep=lambda _: None)
    by = {p: n for p, n, _ in s.put_log}
    assert by[f"{R}/ducklake/data/main/sites/b.parquet"] == 4  # 10 bytes >= 9
    assert by[f"{R}/README.md"] == 1


def test_expected_md5_mismatch_is_an_error(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    local, ipath, _ = items[0]
    stats = push_tree([(local, ipath, "0" * 32)], session=FakeSession(s), con=con, sleep=lambda _: None)
    assert stats["errors"] == 1 and s.calls["put"] == 2  # verify failed -> one re-upload -> still wrong
    assert "checksum mismatch" in uploads(con)[ipath]["error"]


def test_record_site_updates_site_files(con, tree, monkeypatch) -> None:
    root, items = tree
    monkeypatch.setattr(env, "ROOT", root)
    local, ipath, _ = items[0]
    rel = str(local.relative_to(root))
    con.execute("INSERT INTO site_files(site, product, month, name, local_path) VALUES ('SRER','DP','2024-06',"
                "'a.parquet', ?)", (rel,))
    push_tree([items[0]], record_site="SRER", session=FakeSession(FakeIrods()), con=con, sleep=lambda _: None)
    row = con.execute("SELECT up_status, irods_path, irods_checksum, irods_size FROM site_files").fetchone()
    assert tuple(row) == ("uploaded", ipath, md5(b"parquet-a"), 9)


def test_duplicate_targets_rejected(con, tree) -> None:
    _root, items = tree
    with pytest.raises(ValueError, match="two local files"):
        push_tree([items[0], (items[1][0], items[0][1], None)], session=FakeSession(FakeIrods()), con=con)


def test_report_orphans_never_deletes(tree) -> None:
    _root, items = tree
    s = FakeIrods()
    s.add_obj(f"{R}/ducklake/data/main/products/old.parquet")
    s.add_obj(f"{R}/ducklake/data/main/products/a.parquet")
    s.add_obj(f"{R}/unrelated/x")
    rep = report_orphans(FakeSession(s), items, [(f"{R}/ducklake", True)])
    assert rep == {"count": 1, "sample": [f"{R}/ducklake/data/main/products/old.parquet"]}
    assert f"{R}/ducklake/data/main/products/old.parquet" in s.objs


def test_lake_items_refuses_wal_and_orders_catalog_last(tmp_path, monkeypatch) -> None:
    lake = tmp_path / "ducklake"
    (lake / "data/main/t").mkdir(parents=True)
    (lake / "data/main/t/x.parquet").write_bytes(b"1")
    (lake / "neon_catalog.ducklake").write_bytes(b"c")
    monkeypatch.setattr(env, "ROOT", tmp_path)
    monkeypatch.setattr(env, "LAKE_DIR", lake)
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    items, _roots = mirror.lake_items()
    assert items[-1][1].endswith("/ducklake/neon_catalog.ducklake") and len(items) == 2
    (lake / "neon_catalog.ducklake.wal").write_bytes(b"w")
    with pytest.raises(SystemExit, match="wal"):
        mirror.lake_items()


def test_code_items_skip_caches(tmp_path, monkeypatch) -> None:
    for rel in ("README.md", "src/neon_ducklake/a.py", "src/neon_ducklake/__pycache__/a.pyc", "tests/t.py",
                "curation/prompt.md", "curation/generic/DP1.json", "work/state.sqlite"):
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    monkeypatch.setattr(env, "ROOT", tmp_path)
    items, _ = mirror.code_items()
    rels = sorted(i[1].removeprefix(env.IRODS_ROOT + "/") for i in items)
    assert rels == ["README.md", "curation/prompt.md", "src/neon_ducklake/a.py", "tests/t.py"]


def test_export_ledger_redacts_signed_urls(tmp_path, monkeypatch, con) -> None:
    monkeypatch.setattr(env, "RUNS_DIR", tmp_path / "runs")
    (tmp_path / "runs/R1").mkdir(parents=True)
    con.execute("INSERT INTO requests(run_id, family, key, method, path, params, status) VALUES "
                "('R1','manifests','x','GET','GCS m.json', ?, 'ok')",
                (json.dumps({"u": "https://s/x?X-Goog-Signature=abc&X-Goog-Date=1"}),))
    out = mirror.export_ledger("R1", con)
    text = out.read_text()
    assert "abc" not in text and "X-Goog-REDACTED" in text and len(text.splitlines()) == 1
    assert not Path(str(out) + ".part").exists()
