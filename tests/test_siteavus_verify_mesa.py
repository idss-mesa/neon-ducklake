"""verify_mesa.py with a fake mesa-ducklake client, local snapshot Parquet and the iRODS fake."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from neon_ducklake import env, state, verify_mesa
from neon_ducklake.avu import bulk
from neon_ducklake.verify_mesa import MesaVerifier

from .test_avu_fakes import FakeIrods, FakeSession
from .test_site_common import site_env  # noqa: F401 - fixture
from .test_siteavus_common import detach_site_avus

detach_site_avus()  # see test_siteavus_common.detach_site_avus

R = "/zone/home/u/proj"
COLL = f"{R}/sites/TEST"
OBJ = f"{R}/sites/TEST/raw/DP1.00002.001/2024-06/a.csv"
PID = uuid.UUID("8a83b1f3-e50f-4530-92ed-4135985a4761")
B1, B2 = str(uuid.uuid4()), str(uuid.uuid4())
A1 = ("neon.siteCode", "TEST", "")
A2 = ("neon.fileCount", "10", "count")
A3 = ("neon.productCode", "DP1.00002.001", "")


class FakeClient:
    def __init__(self, snaps, pushes, mesa_avus) -> None:
        self.snaps, self.pushes, self.mesa_avus = snaps, pushes, mesa_avus
        self.calls: list[str] = []
        self.project = SimpleNamespace(project_id=PID, irods_path=R)

    def find_project_by_path(self, path):
        self.calls.append("find")
        return self.project if path == R else None

    def _get_catalog(self):
        client = self

        class Cat:
            def list_snapshots(self, pid, limit=100, *, include_pending=False):
                client.calls.append(f"snapshots:{limit}:{include_pending}")
                return list(client.snaps)

            def list_pending_pushes(self, limit=100):
                return list(client.pushes)

        return Cat()

    def get_avus(self, pid, path, *, session=None):
        assert session is None  # local cache only
        self.calls.append(f"get_avus:{path}")
        return [SimpleNamespace(attribute=a, value=v, unit=u) for a, v, u in self.mesa_avus.get(path, set())]


def snap(sid: int, note: str, actor: str = "tswetnam", parquet: str | None = None):
    return SimpleNamespace(snapshot_id=sid, actor=actor, note=note, parquet_file=parquet or f"snapshot_{sid}.parquet")


def write_parquet(cache: Path, sid: int, rows: list[tuple], source: str = "neon-ducklake:site.avus",
                  actor: str = "tswetnam") -> None:
    d = cache / str(PID)
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"project_id": [str(PID)] * len(rows), "snapshot_id": [sid] * len(rows),
                             "irods_path": [r[0] for r in rows], "attribute": [r[1][0] for r in rows],
                             "value": [r[1][1] for r in rows], "unit": [r[1][2] for r in rows],
                             "op": ["add"] * len(rows), "actor": [actor] * len(rows), "source": [source] * len(rows)}),
                   d / f"snapshot_{sid}.parquet")


@pytest.fixture
def world(site_env: Path):  # noqa: F811
    con = state.connect(state.DB_PATH)
    bulk.ensure_desired(con)
    j = [(COLL, "collection", A1, B1, 11), (COLL, "collection", A2, B1, 11), (OBJ, "data_object", A3, B2, 12)]
    for path, ttype, t, bid, sid in j:
        con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, generator,"
                    " status, applied_at, batch_id, snapshot_id) VALUES ('TEST', ?, ?, ?, ?, ?, ?, 'add', 'site.avus',"
                    " 'recorded', '2026-09-27T12:00:00+00:00', ?, ?)", (path, path, ttype, *t, bid, sid))
        con.execute("INSERT OR IGNORE INTO avu_desired(scope, unit_id, path, target_type, attribute, value, unit, level)"
                    " VALUES ('TEST', ?, ?, ?, ?, ?, ?, ?)", (path, path, ttype, *t,
                                                              "site" if ttype == "collection" else "object"))
    for bid, sid in ((B1, 11), (B2, 12)):
        con.execute("INSERT INTO batches(batch_id, scope, status, snapshot_id) VALUES (?, 'TEST', 'recorded', ?)",
                    (bid, sid))
    write_parquet(env.MESA_CACHE_DIR, 11, [(COLL, A1), (COLL, A2)])
    write_parquet(env.MESA_CACHE_DIR, 12, [(OBJ, A3)])
    store = FakeIrods()
    store.add_coll(COLL, (A1, A2, ("ipc_UUID", "x", ""), ("foreign.attr", "keep", "")))
    store.add_obj(OBJ, (A3, ("ipc_UUID", "y", "")))
    snaps = [snap(10, "mesa-mcp init project"), snap(11, f"neon-ducklake TEST batch={B1} 1/2"),
             snap(12, f"neon-ducklake TEST batch={B2} 2/2")]
    client = FakeClient(snaps, [], {COLL: {A1, A2, ("mesa.enabled", "true", "")}, OBJ: {A3}})
    yield con, store, client
    con.close()


def checks(rep) -> dict[str, dict]:
    return {c["name"]: c for c in rep["checks"]}


def test_consistent_history_passes(world) -> None:
    con, store, client = world
    rep = MesaVerifier(client, FakeSession(store), con, project_path=R, seed=1).run()
    c = checks(rep)
    assert {k: v["status"] for k, v in c.items()} == {"project": "pass", "pending": "pass", "snapshots": "pass",
                                                       "journal": "pass", "sources": "pass", "get_avus": "pass"}, \
        {k: v["summary"] for k, v in c.items()}
    assert rep["ok"] and "journal recorded 3 vs Σ parquet rows 3 over 2 batch snapshots" in c["journal"]["summary"]
    assert "other project snapshots 1" in c["journal"]["summary"]
    assert "snapshots:1000:True" in client.calls
    # only read calls on iRODS
    assert store.calls["atomic"] == 0 and store.calls["meta_add"] == 0 and store.calls["put"] == 0


def test_detects_every_failure(world) -> None:
    con, store, client = world
    client.snaps.append(snap(13, "neon-ducklake TEST batch=x", parquet="failed"))
    client.snaps.append(snap(14, "someone else", actor="alice"))
    client.pushes.append(SimpleNamespace(snapshot_id=13))
    con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, status, "
                "snapshot_id) VALUES ('TEST', ?, ?, 'collection', 'neon.extra', '1', '', 'add', 'recorded', 11)",
                (COLL, COLL))
    write_parquet(env.MESA_CACHE_DIR, 12, [(OBJ, A3)], source="mesa-mcp")
    store.objs[OBJ]["avus"].discard(A3)  # removed in iRODS behind the journal's back
    c = checks(MesaVerifier(client, FakeSession(store), con, project_path=R).run())
    assert c["pending"]["status"] == "fail" and "pending pushes 1; failed snapshots 1" in c["pending"]["summary"]
    assert c["snapshots"]["status"] == "fail" and c["snapshots"]["details"]["foreign_actors"] == {"alice": 1}
    assert c["journal"]["status"] == "fail"
    assert c["journal"]["details"]["per_snapshot_diffs"] == {11: {"journal": 3, "parquet": 2}}
    assert c["sources"]["status"] == "fail" and c["sources"]["details"]["bad_sources"] == {"mesa-mcp": 1}
    assert c["get_avus"]["status"] == "fail" and any(OBJ in m for m in c["get_avus"]["details"]["mismatches"])


def test_missing_project_and_missing_parquet(world) -> None:
    con, store, client = world
    rep = MesaVerifier(client, FakeSession(store), con, project_path="/zone/elsewhere").run()
    assert not rep["ok"] and [c["name"] for c in rep["checks"]] == ["project"]
    (env.MESA_CACHE_DIR / str(PID) / "snapshot_12.parquet").unlink()
    c = checks(MesaVerifier(client, FakeSession(store), con, project_path=R).run())
    assert c["journal"]["status"] == "fail" and c["journal"]["details"]["missing_parquet"] == [12]


def test_sampling_is_bounded_per_level(world) -> None:
    con, store, client = world
    for i in range(7):
        p = f"{R}/sites/TEST/raw/DP1.00002.001/2024-06/f{i}.csv"
        store.add_obj(p, (A3,))
        client.mesa_avus[p] = {A3}
        con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, status,"
                    " snapshot_id) VALUES ('TEST', ?, ?, 'data_object', ?, ?, ?, 'add', 'recorded', 12)", (p, p, *A3))
    v = MesaVerifier(client, FakeSession(store), con, project_path=R, per_level=3, seed=0)
    picks = v.sample_paths()
    assert all(len(items) <= 3 for items in picks.values()) and set(picks) == {"TEST:site", "TEST:object",
                                                                             "TEST:data_object"}


def test_main_respects_the_catalog_lock(world, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys) -> None:
    con, store, client = world
    con.commit()
    opened: list[str] = []
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [{"pid": "42", "cmd": "mesa-mcp"}])
    monkeypatch.setattr(env, "mesa", lambda: opened.append("mesa") or SimpleNamespace(session=FakeSession(store)))
    assert verify_mesa.main() == 2 and opened == []
    assert "held by PID(s) 42" in capsys.readouterr().out
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    monkeypatch.setattr(env, "IRODS_ROOT", R)
    monkeypatch.setattr(env, "mesa_ducklake_client", lambda m: opened.append("client") or client)
    monkeypatch.setattr(env, "close_mesa_ducklake_client", lambda: opened.append("closed"))
    out = tmp_path / "vm.json"
    assert verify_mesa.main(out=out) == 0
    assert opened == ["mesa", "client", "closed"]
    assert json.loads(out.read_text())["ok"] is True
