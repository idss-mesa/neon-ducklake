"""bootstrap: catalog lake, ontology cache seeding and curation/generic sync from a fake Data Store."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from neon_ducklake import bootstrap, env
from neon_ducklake.site import ontology_store

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds, put


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)

LAKE = f"{ROOT}/ducklake"
T1 = datetime(2026, 9, 1, tzinfo=UTC)


def seed_lake(store: DSIrods, catalog: bytes = b"catalog-v1", *, mtime: datetime = T1) -> None:
    put(store, f"{LAKE}/data/main/products/a.parquet", b"aaa", mtime=mtime)
    put(store, f"{LAKE}/data/main/sites/b.parquet", b"bbbb", mtime=mtime)
    put(store, f"{LAKE}/{env.CATALOG_LAKE}", catalog, mtime=mtime)


def test_lake_fetched_when_missing_then_current(ds: DSIrods, monkeypatch) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    seed_lake(ds)
    out = bootstrap.sync_lake(session=DSSession(ds))
    assert out["status"] == "fetched" and out["data_fetched"] == 2
    cat = env.LAKE_DIR / env.CATALOG_LAKE
    assert cat.read_bytes() == b"catalog-v1" and (env.LAKE_DIR / "data/main/sites/b.parquet").read_bytes() == b"bbbb"
    assert cat.stat().st_mtime == pytest.approx(T1.timestamp())
    assert [p for op, p in ds.ops if op == "download"][-1] == f"{LAKE}/{env.CATALOG_LAKE}"  # catalog last
    assert bootstrap.sync_lake(session=DSSession(ds))["status"] == "current"


def test_lake_refreshed_when_the_data_store_copy_is_newer(ds: DSIrods, monkeypatch) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    seed_lake(ds)
    bootstrap.sync_lake(session=DSSession(ds))
    put(ds, f"{LAKE}/data/main/sites/c.parquet", b"ccccc", mtime=T1 + timedelta(days=1))
    ds.set_content(f"{LAKE}/{env.CATALOG_LAKE}", b"catalog-v2", register=True)
    ds.mtimes[f"{LAKE}/{env.CATALOG_LAKE}"] = T1 + timedelta(days=1)
    out = bootstrap.sync_lake(session=DSSession(ds))
    assert out == {"status": "fetched", "data_fetched": 1, "data_present": 2, "bytes": 5 + len(b"catalog-v2")}
    assert (env.LAKE_DIR / env.CATALOG_LAKE).read_bytes() == b"catalog-v2"


def test_lake_local_newer_kept_unless_forced_and_guards(ds: DSIrods, monkeypatch) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    seed_lake(ds)
    cat = env.LAKE_DIR / env.CATALOG_LAKE
    cat.parent.mkdir(parents=True)
    cat.write_bytes(b"local-build")
    assert bootstrap.sync_lake(session=DSSession(ds))["status"] == "local-newer"
    os.utime(cat, (T1.timestamp() - 60, T1.timestamp() - 60))
    (env.LAKE_DIR / "x.wal").write_bytes(b"")
    res = bootstrap.run("lake", session=DSSession(ds))
    assert res["catalog"]["status"] == "error" and "wal" in res["catalog"]["error"]
    (env.LAKE_DIR / "x.wal").unlink()
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [{"pid": "1", "cmd": "duckdb"}])
    assert "open by another process" in bootstrap.run("lake", session=DSSession(ds))["catalog"]["error"]
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    assert bootstrap.sync_lake(session=DSSession(ds))["status"] == "fetched" and cat.read_bytes() == b"catalog-v1"


def test_lake_missing_everywhere_fails_main(ds: DSIrods, capsys) -> None:
    assert bootstrap.main("lake", session=DSSession(ds)) == 1
    assert "bootstrap catalog: missing" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="unknown --what"):
        bootstrap.main("lake,nope", session=DSSession(ds))


def _parquet(tmp: Path, name: str, sql: str) -> bytes:
    path = tmp / name
    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    con.close()
    return path.read_bytes()


def test_ontology_seeding_keeps_local_rows(ds: DSIrods, tmp_path: Path) -> None:
    maps = f"{ROOT}/ref/ontology_maps"
    put(ds, f"{maps}/ref_nlcd_envo.parquet", _parquet(tmp_path, "n.parquet",
        "SELECT * FROM (VALUES ('11', 'ENVO:1', 'remote water'), ('21', 'ENVO:2', 'remote developed')) "
        "t(nlcdClass, envoCurie, envoLabel)"))
    put(ds, f"{maps}/ref_unit_map.parquet", _parquet(tmp_path, "u.parquet",
        "SELECT 'celsius' AS neonUnit, 'UO:0000027' AS uoCurie, 'degree Celsius' AS uoLabel, 'seed' AS method, "
        "'ok' AS status, NULL::TIMESTAMPTZ AS validatedAt, 'extra' AS notAColumn"))
    put(ds, f"{maps}/unrelated.parquet", b"not a store table")
    with ontology_store.store() as con:
        con.execute("INSERT INTO ref_nlcd_envo VALUES ('11', 'ENVO:1', 'local label')")
    out = bootstrap.seed_ontology(session=DSSession(ds))
    assert out["status"] == "seeded" and out["inserted"]["ref_nlcd_envo"] == 1 and out["inserted"]["ref_unit_map"] == 1
    with ontology_store.store(read_only=True) as con:
        assert dict(con.execute("SELECT nlcdClass, envoLabel FROM ref_nlcd_envo").fetchall()) == {
            "11": "local label", "21": "remote developed"}
    assert (env.ROOT / "ref" / "ontology_maps" / "ref_nlcd_envo.parquet").exists()  # re-exported
    assert bootstrap.seed_ontology(session=DSSession(ds))["inserted"]["ref_nlcd_envo"] == 0


def test_curation_generic_synced_both_ways_and_imported(ds: DSIrods) -> None:
    item = {"proposalId": "abcdef0123456789", "productCode": "DP1.00001.001", "status": "accepted",
            "attribute": "pato.temperature", "value": "temperature", "unit": "PATO:0000146",
            "curie": "PATO:0000146", "label": "temperature", "ontologyId": "pato", "aspect": "measured_property",
            "column": None, "rationale": "r", "model": "m", "reps": 2, "grounded": True, "reason": None}
    remote_doc = {"productCode": "DP1.00001.001", "accepted": [item], "proposed": [], "rejected": []}
    put(ds, f"{ROOT}/curation/generic/DP1.00001.001.validated.json", json.dumps(remote_doc).encode())
    local_only = env.ROOT / "curation" / "generic" / "DP1.00002.001.validated.json"
    env.write_json(local_only, {"productCode": "DP1.00002.001", "accepted": [], "validatedAt": "2026-09-01T00:00:00",
                                "validatedBy": {"vm": env.vm_id()}})
    git_copy = env.ROOT / "curation" / "generic" / "DP1.00003.001.validated.json"  # from a checkout: not ours
    env.write_json(git_copy, {"productCode": "DP1.00003.001", "accepted": [], "validatedAt": "2026-08-01T00:00:00"})
    out = bootstrap.sync_curation(session=DSSession(ds))
    assert out["fetched"] == ["DP1.00001.001"] and out["published"] == ["DP1.00002.001"]
    assert out["notValidatedHere"] == ["DP1.00003.001"]
    assert f"{ROOT}/curation/generic/DP1.00003.001.validated.json" not in ds.objs
    assert out["imported"] == {"DP1.00001.001": 1}
    assert json.loads(ds.content(f"{ROOT}/curation/generic/DP1.00002.001.validated.json"))["accepted"] == []
    with ontology_store.store(read_only=True) as con:
        assert con.execute("SELECT productCode, status, curie FROM curation_avus").fetchall() == [
            ("DP1.00001.001", "accepted", "PATO:0000146")]
    again = bootstrap.sync_curation(session=DSSession(ds))
    assert again["fetched"] == [] and again["published"] == [] and again["same"] == 2


def test_run_reports_each_step(ds: DSIrods, monkeypatch, capsys) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [])
    seed_lake(ds)
    assert bootstrap.main(None, session=DSSession(ds)) == 0
    out = capsys.readouterr().out
    assert "bootstrap catalog: fetched" in out and "bootstrap ontology: missing" in out and "bootstrap curation: synced" in out
