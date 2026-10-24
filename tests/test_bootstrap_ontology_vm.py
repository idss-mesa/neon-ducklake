"""Per-VM ontology exports (M9): each VM publishes ref/ontology_maps/vm/<vm>/, bootstrap merges all of them
(new keys inserted, a better status replaces a worse one, local rows otherwise win)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from neon_ducklake import bootstrap, env
from neon_ducklake.site import ontology_store

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds, put

MAPS = f"{ROOT}/ref/ontology_maps"


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    store = hermetic_ds(tmp_path, monkeypatch)
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    return store


def parquet(tmp: Path, name: str, sql: str) -> bytes:
    path = tmp / name
    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    con.close()
    return path.read_bytes()


def unit_rows(*rows: tuple[str, str, str]) -> str:
    vals = ", ".join(f"('{u}', '{c}', '{u} label', 'exact', '{s}', NULL::TIMESTAMPTZ)" for u, c, s in rows)
    return f"SELECT * FROM (VALUES {vals}) t(neonUnit, uoCurie, uoLabel, method, status, validatedAt)"


def test_publish_ontology_uploads_only_this_vms_export(ds: DSIrods) -> None:
    with ontology_store.store() as con:
        con.execute("INSERT INTO ref_nlcd_envo VALUES ('11', 'ENVO:1', 'open water')")
    out = bootstrap.publish_ontology(session=DSSession(ds))
    assert out["status"] == "published" and out["remote"] == f"{MAPS}/vm/vm-a"
    assert Path(out["local"]) == ontology_store.vm_export_dir() == env.WORK_DIR / "ontology_export" / "vm" / "vm-a"
    names = sorted(p.rsplit("/", 1)[1] for p in ds.objs if p.startswith(f"{MAPS}/vm/vm-a/"))
    assert names == sorted(f"{t}.parquet" for t in ontology_store.EXPORT_TABLES)
    assert not any(p.startswith(MAPS + "/") and p.count("/") == MAPS.count("/") + 1 for p in ds.objs)  # no shared
    again = bootstrap.publish_ontology(session=DSSession(ds))
    assert again["uploaded"] == 0 and again["present"] == len(ontology_store.EXPORT_TABLES)  # idempotent


def test_export_parquet_vm_scoped_returns_the_directory(ds: DSIrods) -> None:
    d = ontology_store.export_parquet(vm_scoped=True)
    assert isinstance(d, Path) and (d / "ref_unit_map.parquet").exists()
    assert isinstance(ontology_store.export_parquet(), dict)  # the shared ref/ontology_maps export is unchanged


def test_bootstrap_merges_every_vm_export_keeping_the_best_status(ds: DSIrods, tmp_path: Path) -> None:
    put(ds, f"{MAPS}/ref_unit_map.parquet", parquet(tmp_path, "shared.parquet", unit_rows(
        ("m", "UO:0000008", "pending"))))
    put(ds, f"{MAPS}/vm/vm-b/ref_unit_map.parquet", parquet(tmp_path, "b.parquet", unit_rows(
        ("m", "UO:0000008", "ok"), ("kg", "UO:0000009", "ok"), ("s", "UO:0000010", "pending"))))
    put(ds, f"{MAPS}/vm/vm-c/ref_unit_map.parquet", parquet(tmp_path, "c.parquet", unit_rows(
        ("km", "UO:0010066", "ok"), ("kg", "UO:9999999", "pending"))))
    with ontology_store.store() as con:
        con.execute("INSERT INTO ref_unit_map VALUES ('s', 'UO:0000010', 'second', 'seed', 'ok', NULL)")
    out = bootstrap.seed_ontology(session=DSSession(ds))
    assert out["status"] == "seeded" and out["sources"] == ["shared", "vm/vm-b", "vm/vm-c"]
    with ontology_store.store(read_only=True) as con:
        got = {r[0]: (r[1], r[2]) for r in con.execute("SELECT neonUnit, uoCurie, status FROM ref_unit_map").fetchall()}
    assert got == {"m": ("UO:0000008", "ok"),  # pending (shared) upgraded by vm-b's ok
                   "kg": ("UO:0000009", "ok"),  # vm-c's pending never replaces vm-b's ok
                   "s": ("UO:0000010", "ok"),  # the local ok row wins over vm-b's pending
                   "km": ("UO:0010066", "ok")}  # vm-c only: inserted
    assert out["upgraded"]["ref_unit_map"] == 1
    assert bootstrap.seed_ontology(session=DSSession(ds))["inserted"]["ref_unit_map"] == 0


def test_merge_parquet_without_status_is_insert_or_ignore(ds: DSIrods, tmp_path: Path) -> None:
    f = tmp_path / "n.parquet"
    f.write_bytes(parquet(tmp_path, "x.parquet", "SELECT * FROM (VALUES ('11', 'ENVO:9', 'remote')) "
                                                "t(nlcdClass, envoCurie, envoLabel)"))
    with ontology_store.store() as con:
        con.execute("INSERT INTO ref_nlcd_envo VALUES ('11', 'ENVO:1', 'local')")
        assert ontology_store.merge_parquet(con, "ref_nlcd_envo", f) == {"inserted": 0, "upgraded": 0}
        assert con.execute("SELECT envoLabel FROM ref_nlcd_envo").fetchall() == [("local",)]
