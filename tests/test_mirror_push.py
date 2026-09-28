from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from irods.exception import LOCKED_DATA_OBJECT_ACCESS, SYS_COPY_LEN_ERR

from neon_ducklake import env, mirror, state
from neon_ducklake.mirror import push_tree, report_orphans
from neon_ducklake.tuning import ConnectionBudget

from .test_avu_fakes import SYS_MAX_CONNECT_COUNT_EXCEEDED, FakeDataObjects, FakeIrods, FakeSession

R = "/iplant/home/tswetnam/neon-ducklake"


@pytest.fixture(autouse=True)
def _hermetic_locks(tmp_path, monkeypatch) -> None:
    """The default iRODS connection budget lives in env.LOCK_DIR: never the real work/locks."""
    monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "locks")


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
    assert stats["conn_backoffs"] == 1 and stats["threads_min"] == 4 and stats["threads_max"] == 8
    assert s.calls["cleanup"] == s.calls["clone"]  # the failed session was discarded, the rest closed


# ---------------------------------------------------------------- adaptive concurrency + connection budget

class Clock:
    """Shared fake monotonic clock; each put advances it by ``latency``."""

    def __init__(self) -> None:
        import threading

        self.t = 0.0
        self.latency = 0.0
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.t

    def advance(self, dt: float) -> None:
        with self._lock:
            self.t += dt


@pytest.fixture
def clock(monkeypatch) -> Clock:
    import time

    c = Clock()
    orig = FakeDataObjects.put

    def put(self, local, irods_path, num_threads=0, **opts):
        time.sleep(0.005)  # real overlap, so callers queue at the gate (the limit is binding)
        c.advance(c.latency)
        return orig(self, local, irods_path, num_threads, **opts)

    monkeypatch.setattr(FakeDataObjects, "put", put)
    return c


def many_files(tmp_path, n: int, sub: str = "many") -> list:
    items = []
    for i in range(n):
        p = tmp_path / sub / f"d{i % 7}" / f"f{i:04d}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(f"row {i}\n".encode())
        items.append((p, f"{R}/{sub}/d{i % 7}/f{i:04d}.csv", None))
    return items


def test_concurrency_grows_while_uploads_are_fast(con, tmp_path, clock) -> None:
    clock.latency = 0.05  # far below the 3 s small-file target (even with 32 puts overlapping)
    items = many_files(tmp_path, 300)
    s = FakeIrods()
    stats = push_tree(items, threads=4, session=FakeSession(s), con=con, sleep=lambda _: None, clock=clock)
    assert stats["uploaded"] == 300 and stats["errors"] == 0
    assert stats["threads_start"] == 4 and stats["threads_min"] == 4
    assert stats["threads_max"] > 8 and stats["threads_final"] == stats["threads_max"] <= mirror.MAX_THREADS
    assert stats["peak_in_flight"] <= stats["threads_max"]
    assert all("< target" in c["reason"] for c in stats["concurrency_changes"])
    assert stats["files_per_s"] > 0 and stats["mb_per_s"] >= 0 and "uploads_per_s" in stats
    assert stats["sessions_created"] <= stats["threads_max"]
    assert s.calls["cleanup"] == s.calls["clone"]


def test_concurrency_halves_to_the_floor_when_uploads_are_slow(con, tmp_path, clock, monkeypatch) -> None:
    monkeypatch.setattr(mirror, "COOLDOWN_S", 0.0)
    clock.latency = 12.0  # 4x the 3 s target -> p95 ratio above the 3.0 ceiling
    items = many_files(tmp_path, 60)
    stats = push_tree(items, threads=16, session=FakeSession(FakeIrods()), con=con, sleep=lambda _: None,
                      clock=clock)
    assert stats["uploaded"] == 60
    assert stats["threads_start"] == 16 and stats["threads_final"] == 4 and stats["threads_min"] == 4
    reasons = [c["reason"] for c in stats["concurrency_changes"]]
    assert reasons and all("> ceiling" in r for r in reasons)


def test_target_scales_with_size() -> None:
    assert mirror.target_seconds(0) == mirror.SMALL_TARGET_S == 3.0
    assert mirror.target_seconds(100_000_000) == pytest.approx(13.0)
    lim = mirror.upload_limit(1)
    assert (lim.limit, lim.lo, lim.hi) == (1, 1, 32)
    lim = mirror.upload_limit(64)
    assert (lim.limit, lim.lo) == (32, 4)


def test_connection_reset_is_retried_on_a_fresh_session(con, tree) -> None:
    _root, items = tree
    s = FakeIrods()
    s.put_errors = [ConnectionResetError("Connection reset by peer")]
    slept: list[float] = []
    stats = push_tree(items, threads=8, session=FakeSession(s), con=con, sleep=slept.append)
    assert stats["uploaded"] == 4 and stats["errors"] == 0 and stats["net_retries"] == 1
    assert slept == [mirror.NET_WAIT_S] and stats["threads_final"] == 4
    assert stats["sessions_discarded"] == 1 and s.calls["cleanup"] == s.calls["clone"]
    first_path, _n, first_opts = s.put_log[0]
    assert "forceFlag" not in first_opts
    retried = [o for p, _, o in s.put_log if p == first_path]
    assert len(retried) == 2 and "forceFlag" in retried[1]  # the reset put may have created the object


def test_worker_sessions_are_capped_by_the_shared_budget(con, tmp_path) -> None:
    lock_dir = tmp_path / "budget"
    other = ConnectionBudget(3, lock_dir=lock_dir)  # "another process" (flock is per open file)
    held = [other.try_acquire(), other.try_acquire()]
    assert None not in held
    budget = ConnectionBudget(3, lock_dir=lock_dir, poll_s=0.001)
    items = many_files(tmp_path, 25)
    s = FakeIrods()
    stats = push_tree(items, threads=8, session=FakeSession(s), con=con, sleep=lambda _: None, budget=budget)
    assert stats["uploaded"] == 25
    assert stats["sessions_created"] == 1 and budget.peak_held == 1  # one free slot -> one connection
    assert budget.held() == 0 and other.held() == 2 and budget.in_use() == 2
    assert s.calls["cleanup"] == s.calls["clone"] == 1


def test_big_put_holds_extra_budget_slots_on_a_dedicated_session(con, tree, tmp_path, monkeypatch) -> None:
    _root, items = tree
    monkeypatch.setattr(mirror, "BIG_BYTES", 9)
    budget = ConnectionBudget(8, lock_dir=tmp_path / "b8", poll_s=0.001)
    s = FakeIrods()
    stats = push_tree(items, threads=4, session=FakeSession(s), con=con, sleep=lambda _: None, budget=budget)
    assert stats["uploaded"] == 4
    assert {p: n for p, n, _ in s.put_log}[f"{R}/ducklake/data/main/sites/b.parquet"] == mirror.BIG_THREADS
    assert budget.peak_held >= mirror.BIG_THREADS + 1 and budget.held() == 0
    assert s.calls["cleanup"] == s.calls["clone"]


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
