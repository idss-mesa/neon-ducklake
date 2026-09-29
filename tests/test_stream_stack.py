"""site/stack.py stack_chunk: incremental appends across chunks == one whole-product stack; conflicts; idempotence."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import state
from neon_ducklake.site import site_lake, stack

from .test_site_common import SITE, site_env  # noqa: F401 - site_env is a fixture
from .test_site_stack import EC, IS, OS, Site, _add_h5, _csv, _q, _types, build_fixture

SAAT = "dp1_00002_001__saat_30min"
BIRD = "dp1_10003_001__brd_countdata"
FLUX = "dp4_00200_001__nsae_dp04"
CHUNKS = [(IS, ["2019-05"]), (IS, ["2024-06"]), (OS, ["2023-05"]), (OS, ["2024-05"]), (EC, ["2024-06"]),
          (EC, ["2024-07"])]


def _rows(table: str, where: str = "") -> list[tuple]:
    cols = sorted(_types(table))
    sel = ", ".join(site_lake.ident(c) for c in cols)
    return _q(f"SELECT {sel} FROM lake.{site_lake.ident(table)} {where} ORDER BY ALL")


def _snapshot(tables: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {t: _rows(t) for t in tables}
    out["file_stats"] = _q("SELECT sourceFile, rowCount, timeMin, timeMax, qfFailPct, plotIDs, taxonCount "
                           "FROM lake.file_stats ORDER BY sourceFile")
    out["catalog"] = _q("SELECT tableName, nRows, nFiles, rowsBeforeDedup, dedupRemoved, firstMonth, lastMonth "
                        "FROM lake.table_catalog WHERE kind IN ('data', 'lab') ORDER BY tableName")
    out["variables"] = _q("SELECT productCode, tableName, fieldName, dataType, sourceFile FROM lake.variables "
                          "ORDER BY ALL")
    out["history"] = _q("SELECT productCode, tableName, fieldName, dataType, nFiles, firstMonth, lastMonth "
                        "FROM lake.variables_history ORDER BY ALL")
    out["codes"] = _q("SELECT productCode, pubCode, nRecords, nFiles FROM lake.categorical_codes ORDER BY ALL")
    out["taxa"] = _q("SELECT sourceFile, taxonID, nRecords FROM lake.file_taxa ORDER BY ALL")
    out["variables_files"] = _q("SELECT sourceFile, nVariables FROM lake.variables_files ORDER BY ALL")
    out["h5"] = _q("SELECT sourceFile, count(*) FROM lake.h5_attrs GROUP BY 1 ORDER BY 1")
    return out


def _stack_chunks(chunks: list[tuple[str, list[str]]]) -> list[dict[str, Any]]:
    out = []
    for product, months in chunks:
        res = stack.stack_chunk(SITE, product, months, chunk_id=f"{product}__{months[0]}_{months[-1]}")
        assert res["status"] == "ok", res["errors"]
        out.append(res)
    return out


def _drop_lake() -> None:
    shutil.rmtree(site_lake.site_paths(SITE).lake_dir)


def test_chunks_equal_whole_product_stack(site_env: Path) -> None:  # noqa: F811
    s = build_fixture(site_env)
    _add_h5(s, site_env)
    assert stack.main(SITE) == 0
    whole = _snapshot([SAAT, BIRD, "sensor_positions", "validation", FLUX])
    whole_types = {t: _types(t) for t in (SAAT, BIRD)}
    _drop_lake()
    _stack_chunks(CHUNKS)
    streamed = _snapshot([SAAT, BIRD, "sensor_positions", "validation", FLUX])
    for key in whole:
        assert streamed[key] == whole[key], key
    # uid dedup across chunks: u1 of the newer publication (2024-05 chunk) wins over the lake's older row
    assert _q(f"SELECT uid, clusterSize, samplingProtocolVersion FROM lake.{BIRD} ORDER BY uid") == [
        ("u1", 5, "v2"), ("u2", 2, "v1"), ("u3", 1, "v2")]
    # schema growth: newCol (2024-06 only) was added by ALTER; the 2019-05 rows have NULL there
    assert _q(f"SELECT month, count(newCol) FROM lake.{SAAT} GROUP BY 1 ORDER BY 1") == [("2019-05", 0),
                                                                                         ("2024-06", 2000)]
    # types: the lake keeps what the first chunk decided when later values still fit (integer kept for
    # tempSingleNumPts although the newest variables say real); everything else types exactly like the whole stack
    got = _types(SAAT)
    assert got["tempSingleNumPts"] == "BIGINT" and whole_types[SAAT]["tempSingleNumPts"] == "DOUBLE"
    assert {c: t for c, t in got.items() if c != "tempSingleNumPts"} == {
        c: t for c, t in whole_types[SAAT].items() if c != "tempSingleNumPts"}
    assert _types(BIRD) == whole_types[BIRD]
    # verify's invariant: Σ file_stats.rowCount == table_catalog.rowsBeforeDedup per lake table
    for t, before in _q("SELECT tableName, rowsBeforeDedup FROM lake.table_catalog WHERE kind='data'"):
        assert _q("SELECT sum(rowCount) FROM lake.file_stats WHERE lakeTable=?", [t]) == [(before,)]
    assert all(r[0].startswith("stream:") for r in _q("SELECT inputSha256 FROM lake.table_catalog"))


def test_chunk_never_rereads_earlier_chunks_and_is_idempotent(site_env: Path) -> None:  # noqa: F811
    s = build_fixture(site_env)
    first = _stack_chunks([CHUNKS[2]])[0]
    assert first["unitsBuilt"] >= 2
    # the 2023-05 raw files are gone (streamed away): the next chunk still stacks
    for (rel,) in s.con.execute("SELECT local_path FROM site_files WHERE product=? AND month='2023-05'", (OS,)):
        (site_env / rel).unlink()
    s.con.execute("ALTER TABLE site_files ADD COLUMN local_evicted INTEGER DEFAULT 0")
    s.con.execute("UPDATE site_files SET local_evicted=1 WHERE product=? AND month='2023-05'", (OS,))
    _stack_chunks([CHUNKS[3]])
    assert _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]
    n = _q("SELECT count(*) FROM lake.snapshots()")[0][0]
    again = stack.stack_chunk(SITE, OS, ["2024-05"], chunk_id=f"{OS}__2024-05_2024-05")
    assert again["status"] == "ok" and again["unitsBuilt"] == 0 and again["unitsSkipped"] >= 2
    assert _q("SELECT count(*) FROM lake.snapshots()")[0][0] == n  # nothing written again
    # a chunk whose inputs were all evicted has nothing on disk to stack: its rows stay in the lake untouched
    gone = stack.stack_chunk(SITE, OS, ["2023-05"], chunk_id=f"{OS}__2023-05_2023-05")
    assert gone["status"] == "empty" and _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]
    # ... but a downloaded input that is neither on disk nor evicted fails the chunk
    s.con.execute("UPDATE site_files SET local_evicted=0 WHERE product=? AND month='2023-05' AND kind='data'", (OS,))
    lost = stack.stack_chunk(SITE, OS, ["2023-05"], chunk_id=f"{OS}__2023-05_2023-05")
    assert lost["status"] == "failed" and "not on disk" in lost["errors"][0]["error"]
    s.con.execute("UPDATE site_files SET local_evicted=1 WHERE product=? AND month='2023-05'", (OS,))
    # forcing a committed unit re-stacks it without duplicating rows (its source files' rows are replaced)
    forced = stack.stack_chunk(SITE, OS, ["2024-05"], chunk_id=f"{OS}__2024-05_2024-05", force=True)
    assert forced["status"] == "ok" and forced["unitsBuilt"] >= 2
    assert _q(f"SELECT uid, clusterSize FROM lake.{BIRD} ORDER BY uid") == [("u1", 5), ("u2", 2), ("u3", 1)]
    assert _q("SELECT count(*) FROM lake.file_stats WHERE productCode=?", [OS]) == [(2,)]
    assert _q("SELECT nFiles FROM lake.table_catalog WHERE tableName=?", [BIRD]) == [(2,)]


LAB_T = "inv_labQC"
LAB_LAKE = "dp1_20086_002__lab_inv_labqc"
MCT = "DP1.20086.002"
VH = ["table", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat", "primaryKey",
      "categoricalCodeName"]


def test_type_conflicts_keep_cast_or_widen(site_env: Path) -> None:  # noqa: F811
    s = Site(site_env)
    cols = ["sampleCode", "count", "flag"]
    # chunk 1: count and flag are integral -> BIGINT (auto-typed lab table)
    s.add(MCT, "2024-01", f"NEON.BMI_LAB.{LAB_T}.20250101T000000Z.csv", _csv(cols, [["S1", "3", "1"],
                                                                                    ["S2", "4", "2"]]), "lab")
    # data table typed from variables: amount is real in the first chunk
    tbl = "mct_amounts"
    v1 = _csv(VH, [[tbl, "uid", "d", "string", "NA", "basic", "asIs", "N", ""],
                   [tbl, "amount", "d", "real", "NA", "basic", "*.##", "N", ""]])
    s.add(MCT, "2024-01", f"NEON.D10.{SITE}.{MCT}.variables.20250101T000000Z.csv", v1, "variables")
    rows1 = [[f"a{i}", f"{i}.5"] for i in range(10)]
    s.add(MCT, "2024-01", f"NEON.D10.{SITE}.{MCT}.{tbl}.2024-01.basic.20250101T000000Z.csv",
          _csv(["uid", "amount"], rows1), "data")
    _stack_chunks([(MCT, ["2024-01"])])
    assert _types(LAB_LAKE)["count"] == "BIGINT" and _types(LAB_LAKE)["flag"] == "BIGINT"
    lake_t = f"{MCT.replace('.', '_').lower()}__{tbl.lower()}"
    assert _types(lake_t)["amount"] == "DOUBLE"
    # chunk 2: count gets decimals (fits DOUBLE -> widened), flag gets text (-> widened to VARCHAR);
    # the newer variables call amount a string, but 1 in 2000 values fails DOUBLE: the lake keeps DOUBLE
    s.add(MCT, "2024-02", f"NEON.BMI_LAB.{LAB_T}.20250201T000000Z.csv", _csv(cols, [["S3", "2.5", "x"]]), "lab")
    v2 = _csv(VH, [[tbl, "uid", "d", "string", "NA", "basic", "asIs", "N", ""],
                   [tbl, "amount", "d", "string", "NA", "basic", "asIs", "N", ""]])
    s.add(MCT, "2024-02", f"NEON.D10.{SITE}.{MCT}.variables.20250201T000000Z.csv", v2, "variables")
    rows2 = [[f"b{i}", "oops" if i == 5 else f"{i}.25"] for i in range(2000)]
    s.add(MCT, "2024-02", f"NEON.D10.{SITE}.{MCT}.{tbl}.2024-02.basic.20250201T000000Z.csv",
          _csv(["uid", "amount"], rows2), "data")
    res = _stack_chunks([(MCT, ["2024-02"])])[0]
    t = _types(LAB_LAKE)
    assert t["count"] == "DOUBLE" and t["flag"] == "VARCHAR"
    assert _q(f"SELECT sampleCode, count, flag FROM lake.{LAB_LAKE} ORDER BY sampleCode") == [
        ("S1", 3.0, "1"), ("S2", 4.0, "2"), ("S3", 2.5, "x")]
    assert _types(lake_t)["amount"] == "DOUBLE"
    assert _q(f"SELECT count(*), count(amount) FROM lake.{lake_t}") == [(2010, 2009)]
    errs = {(r[0], r[1]): r[2:] for r in _q("SELECT tableName, columnName, action, failures, nonNull "
                                            "FROM lake.cast_errors ORDER BY 1, 2")}
    assert errs[(lake_t, "amount")] == ("cast to lake type DOUBLE; failures set NULL", 1, 2000)
    assert errs[(LAB_LAKE, "count")][0] == "widened lake column BIGINT -> DOUBLE"
    assert errs[(LAB_LAKE, "flag")][0] == "widened lake column BIGINT -> VARCHAR"
    widened = {r["table"]: r["widened"] for r in res["results"] if r.get("table")}
    assert widened[LAB_LAKE] == {"count": "DOUBLE", "flag": "VARCHAR"}
    # the merged variables: the newest definition wins
    assert _q("SELECT dataType FROM lake.variables WHERE fieldName='amount'") == [("string",)]


def test_whole_stack_never_prunes_tables_of_evicted_products(site_env: Path) -> None:  # noqa: F811
    s = build_fixture(site_env)
    assert stack.main(SITE) == 0
    # stream eviction of every OS data file (variables stay on disk): the whole-product path must not drop tables
    con = state.connect(state.DB_PATH)
    con.execute("ALTER TABLE site_files ADD COLUMN local_evicted INTEGER DEFAULT 0")
    for (rel,) in con.execute("SELECT local_path FROM site_files WHERE product=? AND kind='data'", (OS,)).fetchall():
        (site_env / rel).unlink()
    con.execute("UPDATE site_files SET local_evicted=1 WHERE product=? AND kind='data'", (OS,))
    con.close()
    assert stack.main(SITE) == 0
    assert _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]
    assert s is not None


@pytest.mark.parametrize("empty_months", [["2030-01"]])
def test_empty_chunk(site_env: Path, empty_months: list[str]) -> None:  # noqa: F811
    build_fixture(site_env)
    res = stack.stack_chunk(SITE, OS, empty_months, chunk_id="x")
    assert res["status"] == "empty" and res["files"] == 0


# ---------------------------------------------------------------- per-file counts (review M13 / #7)

def _figures() -> dict[str, Any]:
    """Every figure the stream path used to sum onto the lake's value."""
    return {
        "codes": _q("SELECT pubCode, nRecords, nFiles FROM lake.categorical_codes ORDER BY ALL"),
        "validation": _q("SELECT fieldName, nRecords, nFiles FROM lake.validation ORDER BY ALL"),
        "history": _q("SELECT tableName, fieldName, dataType, nFiles FROM lake.variables_history ORDER BY ALL"),
        "drift": _q("SELECT lakeTable, headerSha1, nFiles FROM lake.schema_drift ORDER BY ALL"),
        "catalog": _q("SELECT tableName, nFiles, rowsBeforeDedup FROM lake.table_catalog "
                      "WHERE tableName IN ('categorical_codes', 'validation', 'variables_history', ?) ORDER BY 1",
                      [BIRD]),
    }


def test_restacking_a_chunk_never_double_counts(site_env: Path) -> None:  # noqa: F811
    build_fixture(site_env)
    _stack_chunks([CHUNKS[2], CHUNKS[3]])
    before = _figures()
    assert before["codes"] == [("F", 2, 2), ("M", 2, 2)]
    assert ("categorical_codes", 2, None) in before["catalog"]
    for _ in range(2):  # --force, twice: the chunk's files replace their own per-file rows
        forced = stack.stack_chunk(SITE, OS, ["2024-05"], chunk_id=f"{OS}__2024-05_2024-05", force=True)
        assert forced["status"] == "ok" and forced["unitsBuilt"] >= 2
        assert _figures() == before


def test_stream_chunk_over_a_whole_product_stack_counts_each_file_once(site_env: Path) -> None:  # noqa: F811
    build_fixture(site_env)
    assert stack.main(SITE, [OS]) == 0
    whole = _figures()
    # the lake was stacked before the per-file counts existed: the stream chunk seeds them from the stored
    # figures minus its own files, which the lake already held
    res = stack.stack_chunk(SITE, OS, ["2024-05"], chunk_id=f"{OS}__2024-05_2024-05")
    assert res["status"] == "ok" and res["unitsBuilt"] >= 2
    assert _figures() == whole
    assert _q("SELECT count(*) FROM lake.stack_file_counts WHERE sourceFile = '<lake>'")[0][0] > 0
    # a whole-product re-stack drops the per-file counts (they are seeded again by the next stream chunk)
    assert stack.main(SITE, [OS], force=True) == 0
    assert _q("SELECT count(*) FROM lake.stack_file_counts WHERE productCode = ?", [OS]) == [(0,)]
    assert _figures() == whole


def test_lab_and_cast_figures_survive_a_forced_restack(site_env: Path) -> None:  # noqa: F811
    s = Site(site_env)
    cols = ["sampleCode", "count", "flag"]
    s.add(MCT, "2024-01", f"NEON.BMI_LAB.{LAB_T}.20250101T000000Z.csv", _csv(cols, [["S1", "3", "1"],
                                                                                    ["S2", "4", "2"]]), "lab")
    tbl = "mct_amounts"
    v1 = _csv(VH, [[tbl, "uid", "d", "string", "NA", "basic", "asIs", "N", ""],
                   [tbl, "amount", "d", "real", "NA", "basic", "*.##", "N", ""]])
    s.add(MCT, "2024-01", f"NEON.D10.{SITE}.{MCT}.variables.20250101T000000Z.csv", v1, "variables")
    rows = [[f"a{i}", "oops" if i == 3 else f"{i}.5"] for i in range(2000)]
    s.add(MCT, "2024-01", f"NEON.D10.{SITE}.{MCT}.{tbl}.2024-01.basic.20250101T000000Z.csv",
          _csv(["uid", "amount"], rows), "data")
    s.add(MCT, "2024-02", f"NEON.BMI_LAB.{LAB_T}.20250201T000000Z.csv", _csv(cols, [["S3", "5", "1"]]), "lab")
    _stack_chunks([(MCT, ["2024-01"]), (MCT, ["2024-02"])])
    lake_t = f"{MCT.replace('.', '_').lower()}__{tbl.lower()}"

    def figures() -> tuple:
        return (_q("SELECT nFiles, rowsBeforeDedup, nRows FROM lake.table_catalog WHERE tableName = ?", [LAB_LAKE]),
                _q("SELECT columnName, nonNull, failures FROM lake.cast_errors WHERE tableName = ?", [lake_t]),
                _q("SELECT nFiles FROM lake.schema_drift WHERE lakeTable = ? ORDER BY ALL", [LAB_LAKE]))

    before = figures()
    assert before[0] == [(2, 3, 3)] and before[1] == [("amount", 2000, 1)] and before[2] == [(2,)]
    for months in (["2024-01"], ["2024-02"], ["2024-01"]):
        res = stack.stack_chunk(SITE, MCT, months, chunk_id=f"{MCT}__{months[0]}_{months[0]}", force=True)
        assert res["status"] == "ok"
        assert figures() == before


def test_h5_objdesc_is_kept_in_the_lake_for_the_cards(site_env: Path) -> None:  # noqa: F811
    import h5py
    import numpy as np

    from neon_ducklake.site import cards

    from .test_site_stack import make_h5

    s = Site(site_env)
    name = f"NEON.D99.{SITE}.{EC}.nsae.2024-06.basic.20260115T171049Z.h5"
    tmp = site_env / "tmp_h5" / name
    make_h5(tmp)
    with h5py.File(tmp, "a") as f:
        f.create_dataset("objDesc", data=np.array(
            [(b"fluxCo2", b"CO2 flux | turbulent"), (b"veloFric", b"friction velocity")],
            dtype=[("Object", "S16"), ("Description", "S32")]))
    path = s.add(EC, "2024-06", name, tmp.read_bytes(), "data")
    _stack_chunks([(EC, ["2024-06"])])
    assert _q("SELECT term, description FROM lake.h5_objdesc ORDER BY term") == [
        ("fluxCo2", "CO2 flux | turbulent"), ("veloFric", "friction velocity")]
    path.unlink()  # streamed: the .h5 file is gone once uploaded; the card still describes the dp04 columns
    tmp.unlink()
    con = site_lake.open_site_lake(SITE, read_only=True)
    try:
        desc = cards.h5_objdesc(con, EC)
    finally:
        con.close()
    assert desc["veloFric"] == "friction velocity" and desc["fluxCo2"].startswith("CO2 flux")
