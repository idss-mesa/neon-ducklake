"""History recording + recovery against a real (local-only) mesa-ducklake client in tmp_path."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from neon_ducklake import env, state
from neon_ducklake.avu import history

R = env.IRODS_ROOT
C = f"{R}/sites/SRER/raw/DP1.00002.001/2024-06"
T0 = datetime(2026, 9, 27, 15, 0, 0, tzinfo=UTC)


@pytest.fixture
def con(tmp_path):
    return state.connect(tmp_path / "state.sqlite")


@pytest.fixture
def lake(tmp_path, monkeypatch):
    from mesa_ducklake import DuckLakeClient

    cache = tmp_path / "mesa-cache"
    monkeypatch.setattr(env, "MESA_CACHE_DIR", cache)
    monkeypatch.setattr(env, "QUARANTINE_DIR", tmp_path / "quarantine")
    monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "locks")
    client = DuckLakeClient(catalog_dsn=f"duckdb:///{tmp_path / 'catalog.duckdb'}", irods_session=None,
                            cache_dir=str(cache), cache_cap_bytes=0)
    project = client.register_project(R, env.ACTOR, "iplant")
    yield client, project
    client.close()


def add_rows(con, n_paths: int = 3, per_path: int = 2, *, scope: str = "SRER", op: str = "add",
             naive: bool = False) -> list[int]:
    ids = []
    for i in range(n_paths):
        for j in range(per_path):
            ts = T0 + timedelta(seconds=i * 10 + j)
            at = ts.replace(tzinfo=None).isoformat() if naive else ts.isoformat()
            cur = con.execute(
                "INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, generator,"
                " status, applied_at) VALUES (?,?,?,?,?,?,?,?,?, 'applied', ?)",
                (scope, C, f"{C}/f{i}.csv", "data_object", f"neon.k{j}", f"v{j}", "", op, "site.avus", at))
            ids.append(cur.lastrowid)
    return ids


def status_of(con, ids):
    q = f"SELECT status, batch_id, snapshot_id FROM avu_journal WHERE id IN ({','.join('?' * len(ids))})"
    return [tuple(r) for r in con.execute(q, ids)]


def test_parse_ts_is_utc_aware() -> None:
    assert history.parse_ts("2026-09-27T15:00:00").tzinfo is not None
    assert history.parse_ts("2026-09-27T09:00:00-06:00") == T0
    assert history.batch_id_of("neon-ducklake SRER batch=0b1f7c1e-9a6e-4c1f-8a53-2a9b0c1d2e3f 1/2 paths=1") == \
        "0b1f7c1e-9a6e-4c1f-8a53-2a9b0c1d2e3f"
    assert history.batch_id_of("ds_add_avus") is None


def test_plan_batches_never_splits_a_path(con) -> None:
    add_rows(con, n_paths=5, per_path=2)
    batches = history.plan_batches(con, "SRER", rows_per_batch=3)
    assert [len(b) for b in batches] == [2, 2, 2, 2, 2]
    paths = [[r["path"] for r in b] for b in batches]
    assert all(len(set(p)) == 1 for p in paths)
    assert [p[0] for p in paths] == sorted(p[0] for p in paths)
    assert [len(b) for b in history.plan_batches(con, "SRER", rows_per_batch=4)] == [4, 4, 2]
    assert history.plan_batches(con, "OTHER") == []


def test_record_batches_writes_snapshots_with_exact_ts(con, lake) -> None:
    client, project = lake
    ids = add_rows(con, n_paths=3, per_path=2)
    rid = add_rows(con, n_paths=1, per_path=1, op="remove")[0]
    out = history.record_batches(client, project.project_id, None, con, "SRER", rows_per_batch=4)
    assert out["batches_recorded"] == 2 and out["rows_recorded"] == 7
    st = status_of(con, ids + [rid])
    assert {s for s, _, _ in st} == {"recorded"} and all(snap for _, _, snap in st)
    snaps = history._catalog_snapshots(client, project.project_id)
    assert len(snaps) == 2 and all(s.parquet_file.endswith(".parquet") for s in snaps)
    assert all("batch=" in s.note and "neon-ducklake SRER" in s.note for s in snaps)
    b = [dict(r) for r in con.execute("SELECT * FROM batches ORDER BY seq")]
    assert [x["status"] for x in b] == ["recorded", "recorded"] and [x["n_rows"] for x in b] == [3, 4]  # f0 has 3
    hist = client.get_history(project.project_id, f"{C}/f0.csv")
    got = {(h.attribute, h.op, h.ts.astimezone(UTC), h.source, h.actor) for h in hist}
    assert ("neon.k0", "add", T0, "neon-ducklake:site.avus", "tswetnam") in got
    assert ("neon.k0", "delete", T0, "neon-ducklake:site.avus", "tswetnam") in got
    # nothing left to record
    assert history.plan_batches(con, "SRER") == []


def test_naive_applied_at_is_taken_as_utc(con, lake) -> None:
    client, project = lake
    add_rows(con, n_paths=1, per_path=1, naive=True)
    history.record_batches(client, project.project_id, None, con, "SRER")
    (h,) = client.get_history(project.project_id, f"{C}/f0.csv")
    assert h.ts.astimezone(UTC) == T0


def _pending_snapshot(client, project, con, rows, *, n_parquet_rows=None, wal=True, failed=False):
    """Simulate a crash inside record_changes: batch row + pending snapshot (+ WAL row + local parquet)."""
    batch_id = "11111111-2222-3333-4444-555555555555" if not failed else "99999999-2222-3333-4444-555555555555"
    note = f"neon-ducklake SRER batch={batch_id} 1/1 paths=1 avus={len(rows)}"
    con.execute("INSERT INTO batches(batch_id, scope, seq, total, note, n_rows, status) VALUES (?,?,?,?,?,?,"
                " 'recording')", (batch_id, "SRER", 1, 1, note, len(rows)))
    con.executemany("UPDATE avu_journal SET batch_id=? WHERE id=?", [(batch_id, i) for i in rows])
    cat = client._get_catalog()
    snap = cat.create_snapshot(project.project_id, env.ACTOR, None, note, "pending")
    changes = history.to_changes([con.execute("SELECT * FROM avu_journal WHERE id=?", (i,)).fetchone()
                                  for i in rows], "SRER")
    lake_store = client._get_lake(project)
    lake_store.write_changes(project.project_id, snap.snapshot_id, changes[: n_parquet_rows or len(changes)])
    local = env.MESA_CACHE_DIR / str(project.project_id) / f"snapshot_{snap.snapshot_id}.parquet"
    assert local.exists()
    if wal:
        cat.insert_pending_push(snap.snapshot_id, str(local), f"{R}/.mesa/ducklake/{local.name}")
    if failed:
        cat.update_snapshot_parquet_file(snap.snapshot_id, "failed")
    return batch_id, snap.snapshot_id, local


def _fake_recover(client, project, *, commit: bool):
    calls = []

    def recover_pending_pushes(session=None, limit=100):
        calls.append(limit)
        cat = client._get_catalog()
        out = {"pushed": 0, "missing_local": 0}
        for p in cat.list_pending_pushes(limit=limit):
            if not Path(p.local_path).exists():
                cat.bump_pending_push_attempt(p.snapshot_id, "local parquet missing")
                out["missing_local"] += 1
            elif commit:
                cat.update_snapshot_parquet_file(p.snapshot_id, Path(p.local_path).name)
                cat.delete_pending_push(p.snapshot_id)
                out["pushed"] += 1
        return out

    client.recover_pending_pushes = recover_pending_pushes
    return calls


def test_recover_commits_a_valid_pending_batch(con, lake) -> None:
    client, project = lake
    ids = add_rows(con, n_paths=2, per_path=2)
    batch_id, sid, _ = _pending_snapshot(client, project, con, ids)
    calls = _fake_recover(client, project, commit=True)
    rec = history.recover(client, project, None, con)
    assert calls == [1000] and rec["recorded"] == 4 and rec["quarantined"] == []
    assert {s for s, _, _ in status_of(con, ids)} == {"recorded"}
    assert tuple(con.execute("SELECT status, snapshot_id FROM batches WHERE batch_id=?", (batch_id,)).fetchone()) \
        == ("recorded", sid)
    assert history.plan_batches(con, "SRER") == []  # never recorded twice


def test_recover_marks_a_committed_batch_after_a_crash(con, lake) -> None:
    client, project = lake
    ids = add_rows(con, n_paths=1, per_path=3)
    batch_id = "abcdefab-2222-3333-4444-555555555555"
    note = f"neon-ducklake SRER batch={batch_id} 1/1 paths=1 avus=3"
    con.execute("INSERT INTO batches(batch_id, scope, n_rows, status) VALUES (?, 'SRER', 3, 'recording')",
                (batch_id,))
    con.executemany("UPDATE avu_journal SET batch_id=? WHERE id=?", [(batch_id, i) for i in ids])
    rows = [con.execute("SELECT * FROM avu_journal WHERE id=?", (i,)).fetchone() for i in ids]
    snap = client.record_changes(project.project_id, env.ACTOR, history.to_changes(rows, "SRER"), note=note)
    rec = history.recover(client, project, None, con)
    assert rec["recorded"] == 3
    assert {x for x in status_of(con, ids)} == {("recorded", batch_id, snap.snapshot_id)}


def test_recover_quarantines_bad_and_failed_parquet_and_releases_rows(con, lake) -> None:
    client, project = lake
    good = add_rows(con, n_paths=1, per_path=3)
    bad_batch, _bad_sid, bad_local = _pending_snapshot(client, project, con, good, n_parquet_rows=2)
    more = add_rows(con, n_paths=1, per_path=2, scope="SRER")
    con.execute("UPDATE avu_journal SET path=? WHERE id IN (?, ?)", (f"{C}/g.csv", *more))
    failed_batch, _failed_sid, failed_local = _pending_snapshot(client, project, con, more, wal=False, failed=True)
    # an orphan pending snapshot (no WAL row) and a stray file with no catalog row
    cat = client._get_catalog()
    orphan = cat.create_snapshot(project.project_id, env.ACTOR, None, "manual", "pending")
    orphan_local = env.MESA_CACHE_DIR / str(project.project_id) / f"snapshot_{orphan.snapshot_id}.parquet"
    orphan_local.write_bytes(b"not parquet")
    stray = env.MESA_CACHE_DIR / str(project.project_id) / "snapshot_999999.parquet"
    stray.write_bytes(b"x")
    _fake_recover(client, project, commit=True)
    rec = history.recover(client, project, None, con)
    reasons = {q["file"]: q["reason"] for q in rec["quarantined"]}
    assert reasons[bad_local.name] == "row count 2 != batch 3"
    assert reasons[failed_local.name] == "snapshot failed"
    assert reasons[orphan_local.name].startswith("orphan pending")
    assert reasons[stray.name] == "no catalog snapshot"
    assert not bad_local.exists() and not failed_local.exists() and not stray.exists()
    assert len(list((env.QUARANTINE_DIR / str(project.project_id)).iterdir())) == 4
    assert rec["released"] == 5 and rec["blocked"] == []
    # the local cache only holds committed snapshots now; the rows are re-recorded exactly once
    out = history.record_batches(client, project.project_id, None, con, "SRER")
    assert out["rows_recorded"] == 5
    assert {s for s, _, _ in status_of(con, good + more)} == {"recorded"}
    assert history.plan_batches(con, "SRER") == []
    status = dict(con.execute("SELECT batch_id, status FROM batches").fetchall())
    assert status[bad_batch] == "abandoned" and status[failed_batch] == "abandoned"


def test_recover_blocks_a_batch_that_is_still_pending(con, lake) -> None:
    client, project = lake
    ids = add_rows(con, n_paths=1, per_path=2)
    batch_id, sid, local = _pending_snapshot(client, project, con, ids)
    _fake_recover(client, project, commit=False)  # iRODS still unreachable
    rec = history.recover(client, project, None, con)
    assert rec["blocked"] == [{"batch_id": batch_id, "snapshot_id": sid}] and local.exists()
    assert history.plan_batches(con, "SRER") == []  # held back, not re-recorded


def test_record_scope_refuses_when_catalog_is_held(con, lake, monkeypatch) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [{"pid": "4242", "cmd": "mesa-mcp"}])
    with pytest.raises(SystemExit, match="4242"):
        history.record_scope("SRER", con=con)


def test_record_scope_end_to_end(con, lake, monkeypatch) -> None:
    client, _project = lake
    closed = []

    class M:
        session = None

    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    monkeypatch.setattr(env, "mesa", lambda: M())
    monkeypatch.setattr(env, "mesa_ducklake_client", lambda m: client)
    monkeypatch.setattr(env, "close_mesa_ducklake_client", lambda: closed.append(1))
    add_rows(con, n_paths=4, per_path=2)
    out = history.record_scope("SRER", rows_per_batch=4, con=con)
    assert out["batches_recorded"] == 2 and out["rows_recorded"] == 8 and out["blocked_rows"] == 0
    assert closed == [1]
    again = history.record_scope("SRER", con=con)
    assert again["batches_planned"] == 0


def test_record_scope_requires_enrolled_project(con, tmp_path, monkeypatch) -> None:
    from mesa_ducklake import DuckLakeClient

    monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "locks")
    client = DuckLakeClient(catalog_dsn=f"duckdb:///{tmp_path / 'c2.duckdb'}", cache_dir=str(tmp_path / "c"))
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    monkeypatch.setattr(env, "mesa", lambda: type("M", (), {"session": None})())
    monkeypatch.setattr(env, "mesa_ducklake_client", lambda m: client)
    monkeypatch.setattr(env, "close_mesa_ducklake_client", lambda: None)
    with pytest.raises(SystemExit, match="enroll"):
        history.record_scope("SRER", con=con)
    client.close()
