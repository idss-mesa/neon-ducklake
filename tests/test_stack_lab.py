"""F8: NEON.<Lab>.<table>.<ts>.csv files stacked per product into <dp>__lab_<table> (distinct rows)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from neon_ducklake.site import stack

from .test_site_common import SITE, site_env  # noqa: F401 - site_env is a fixture
from .test_site_stack import Site, _csv, _q, _types

MCT = "DP1.20086.002"
LAB_T = "mct_taxonTableLabSummary"
LAKE_T = "dp1_20086_002__lab_mct_taxontablelabsummary"
COLS = ["uid", "targetGene", "clusterSimilarity", "labSpecificStartDate", "laboratoryName", "remarks"]
A = ["6f9ee00d", "16S rRNA", "97.000", "2024-08-14", "NEON HQ", ""]
B = ["56e87be8", "ITS", "97.000", "2024-08-14", "NEON HQ", "r"]
C = ["0a1b2c3d", "ITS", "99.000", "2025-01-02", "NEON HQ", ""]


def _variables() -> bytes:
    h = ["table", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat", "primaryKey",
         "categoricalCodeName"]
    rows = [[LAB_T, "uid", "d", "string", "NA", "basic", "asIs", "N", ""],
            [LAB_T, "targetGene", "d", "string", "NA", "basic", "LOV", "N", ""],
            [LAB_T, "clusterSimilarity", "d", "real", "percent", "basic", "*.###(round)", "N", ""],
            [LAB_T, "labSpecificStartDate", "d", "dateTime", "NA", "basic", "yyyy-MM-dd(floor)", "N", ""],
            [LAB_T, "laboratoryName", "d", "string", "NA", "basic", "asIs", "N", ""],
            [LAB_T, "remarks", "d", "string", "NA", "basic", "asIs", "N", ""],
            ["mct_benthicSampleMetadata_16S", "uid", "d", "string", "NA", "basic", "asIs", "N", ""],
            ["mct_benthicSampleMetadata_16S", "collectDate", "d", "dateTime", "NA", "basic",
             "yyyy-MM-dd'T'HH:mm'Z'(floor)", "N", ""]]
    return _csv(h, rows)


def _fixture(root: Path) -> Site:
    s = Site(root)
    p1, p2 = "20251206T221502Z", "20260101T000000Z"
    s.add(MCT, "2024-03", f"NEON.D10.{SITE}.{MCT}.variables.{p1}.csv", _variables(), "variables")
    s.add(MCT, "2024-03", f"NEON.D10.{SITE}.{MCT}.mct_benthicSampleMetadata_16S.2024-03.basic.{p1}.csv",
          _csv(["uid", "collectDate"], [["m1", "2024-03-26T15:00Z"]]), "data")
    # an older journal classified lab files as 'other'; the stacker re-classifies them
    s.add(MCT, "2024-03", f"NEON.NEON_HQ.{LAB_T}.{p1}.csv", _csv(COLS, [A, B]), "other")
    s.add(MCT, "2024-07", f"NEON.NEON_HQ.{LAB_T}.{p2}.csv", _csv(COLS, [A, C]), "lab")
    # a lab table the variables do not describe: auto-typed; two labs keep their own rows
    auto_cols = ["sampleCode", "count", "value", "measured", "measuredAt", "note"]
    s.add(MCT, "2024-03", "NEON.BMI_LAB.inv_labQC.20250101T000000Z.csv", _csv(auto_cols, [
        ["S1", "3", "1.5", "2024-01-02", "2024-01-02T10:00:00Z", "ok"],
        ["S2", "-4", "2", "2024-01-03", "2024-01-03T10:00Z", ""]]), "lab")
    s.add(MCT, "2024-07", "NEON.OTHER_LAB.inv_labQC.20250201T000000Z.csv", _csv(auto_cols, [
        ["S1", "3", "1.5", "2024-01-02", "2024-01-02T10:00:00Z", "ok"]]), "lab")
    return s


def test_input_files_reclassify_lab(site_env: Path) -> None:  # noqa: F811
    _fixture(site_env)
    files = stack.input_files(SITE)[MCT]
    labs = sorted((f.name, f.kind, f.table, f.lab_name, f.pub_ts.year) for f in files if f.kind == "lab")
    assert [x[1:] for x in labs] == [("lab", "inv_labQC", "BMI_LAB", 2025), ("lab", LAB_T, "NEON_HQ", 2025),
                                     ("lab", LAB_T, "NEON_HQ", 2026), ("lab", "inv_labQC", "OTHER_LAB", 2025)]


def test_lab_tables(site_env: Path) -> None:  # noqa: F811
    _fixture(site_env)
    assert stack.main(SITE) == 0
    t = _types(LAKE_T)
    assert list(t) == [*COLS, "labName", "publicationDate", "sourceFile"]
    assert t["clusterSimilarity"] == "DOUBLE" and t["labSpecificStartDate"] == "DATE"
    assert t["uid"] == "VARCHAR" and t["publicationDate"] == "TIMESTAMP WITH TIME ZONE"
    rows = _q(f"SELECT uid, clusterSimilarity, labSpecificStartDate, labName, year(publicationDate), sourceFile "
              f"FROM lake.{LAKE_T} ORDER BY uid")
    assert [r[0] for r in rows] == ["0a1b2c3d", "56e87be8", "6f9ee00d"]  # A appears once
    a = rows[2]
    assert a[1] == 97.0 and a[2] == dt.date(2024, 8, 14) and a[3] == "NEON_HQ"
    assert a[4] == 2026 and a[5] == f"NEON.NEON_HQ.{LAB_T}.20260101T000000Z.csv"  # newest publication kept
    assert _q("SELECT kind, nFiles, nRows, rowsBeforeDedup, dedupRemoved, neonTable FROM lake.table_catalog "
              "WHERE tableName = ?", [LAKE_T]) == [("lab", 2, 3, 4, 1, LAB_T)]
    assert _q("SELECT count(*), max(nFiles) FROM lake.schema_drift WHERE lakeTable = ?", [LAKE_T]) == [(1, 2)]
    # auto-typed lab table: two labs, the identical row of each lab is kept
    auto = "dp1_20086_002__lab_inv_labqc"
    t = _types(auto)
    assert (t["count"], t["value"], t["measured"], t["measuredAt"], t["note"], t["sampleCode"]) == (
        "BIGINT", "DOUBLE", "DATE", "TIMESTAMP WITH TIME ZONE", "VARCHAR", "VARCHAR")
    assert _q(f"SELECT labName, sampleCode, count FROM lake.{auto} ORDER BY 1, 2") == [
        ("BMI_LAB", "S1", 3), ("BMI_LAB", "S2", -4), ("OTHER_LAB", "S1", 3)]
    # the product's data table is untouched by the lab files, and lab files add no file_stats rows
    assert _q("SELECT count(*) FROM lake.dp1_20086_002__mct_benthicsamplemetadata_16s") == [(1,)]
    assert _q("SELECT count(*) FROM lake.file_stats WHERE sourceFile LIKE 'NEON.%LAB%' OR "
              "sourceFile LIKE 'NEON.NEON_HQ.%'") == [(0,)]
    assert _q("SELECT count(*) FROM lake.cast_errors") == [(0,)]
    # unchanged inputs -> no new snapshot
    n = _q("SELECT count(*) FROM lake.snapshots()")[0][0]
    assert stack.main(SITE) == 0
    assert _q("SELECT count(*) FROM lake.snapshots()")[0][0] == n
    msgs = [r[0] for r in _q("SELECT commit_message FROM lake.snapshots()")]
    assert f"stack {SITE} {MCT} lab {LAB_T}" in msgs and f"stack {SITE} {MCT} lab inv_labQC" in msgs


def test_lab_cast_errors_follow_variables(site_env: Path) -> None:  # noqa: F811
    s = _fixture(site_env)
    bad = [[f"u{i}", "ITS", "n/a" if i < 5 else "97.0", "2024-08-14", "NEON HQ", ""] for i in range(50)]
    s.add(MCT, "2024-08", f"NEON.NEON_HQ.{LAB_T}.20260201T000000Z.csv", _csv(COLS, bad), "lab")
    assert stack.main(SITE) == 0
    assert _types(LAKE_T)["clusterSimilarity"] == "VARCHAR"
    assert _q("SELECT tableName, neonTable, columnName, failures, action FROM lake.cast_errors") == [
        (LAKE_T, LAB_T, "clusterSimilarity", 5, "kept VARCHAR")]
