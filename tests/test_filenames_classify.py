"""filenames.classify: the one NEON file-name classifier (catalog flatten, site fetch/stack/AVUs)."""

from __future__ import annotations

import pytest

from neon_ducklake import filenames
from neon_ducklake.filenames import Classified, classify


def test_instrument_data_file() -> None:
    c = classify("NEON.D14.SRER.DP1.00001.001.000.010.030.2DWSD_30min.2020-01.basic.20250128T000000Z.csv")
    assert c == Classified(kind="data", table="2DWSD_30min", hor="000", ver="010", tmi="030", domain="D14",
                           site="SRER", product_code="DP1.00001.001", month="2020-01", package="basic",
                           timestamp="20250128T000000Z", ext="csv")


def test_observational_and_support_files() -> None:
    os_ = classify("NEON.D10.ARIK.DP1.10111.001.sim_locationvisitsandattributes.2024-07.basic.20250101T000000Z.csv")
    assert (os_.kind, os_.table, os_.hor, os_.month) == ("data", "sim_locationvisitsandattributes", None, "2024-07")
    kinds = {
        "NEON.D10.ARIK.DP1.00044.001.variables.20250904T031049Z.csv": "variables",
        "NEON.D10.ARIK.DP1.00044.001.sensor_positions.20250904T031049Z.csv": "sensor_positions",
        "NEON.D10.ARIK.DP1.00044.001.readme.20250904T031049Z.txt": "readme",
        "NEON.D10.ARIK.DP1.00044.001.EML.20250601-20250630.20250904T031049Z.xml": "eml",
        "NEON.D10.ARIK.DP1.20288.001.science_review_flags.20250904T031049Z.csv": "science_review_flags",
        "NEON.D10.ARIK.DP1.10003.001.categoricalCodes_10003.20250101T000000Z.csv": "categorical_codes",
        "NEON.D10.ARIK.DP0.10003.001.validation.20250101T000000Z.csv": "validation",
        "NEON.D10.ARIK.DP1.00044.001.2025-06.basic.20250904T031049Z.zip": "package",
        "something.else.csv": "other",
        "": "other",
    }
    for name, kind in kinds.items():
        assert classify(name).kind == kind, name
    assert set(kinds.values()) <= filenames.KINDS


def test_dp0_support_files_take_the_listed_product() -> None:
    name = "NEON.D10.ARIK.DP0.10003.001.validation.20250101T000000Z.csv"
    assert classify(name).product_code == "DP0.10003.001"
    hinted = classify(name, product_hint="DP1.10003.001")
    assert (hinted.kind, hinted.product_code, hinted.table) == ("validation", "DP1.10003.001", "validation")
    # a DP1 file keeps its own code whatever the hint
    assert classify("NEON.D10.ARIK.DP1.00044.001.variables.20250904T031049Z.csv", "DP1.99999.001").product_code == \
        "DP1.00044.001"


@pytest.mark.parametrize(("name", "lab", "table", "ts"), [
    ("NEON.NEON_HQ.mct_taxonTableLabSummary.20251206T221502Z.csv", "NEON_HQ", "mct_taxonTableLabSummary",
     "20251206T221502Z"),
    ("NEON.Jonah_Ventures.inv_metabarcodeSummary.20251205T051406Z.csv", "Jonah_Ventures", "inv_metabarcodeSummary",
     "20251205T051406Z"),
    (("NEON.Colorado_Department_of_Public_Health_and_Environment_Air_Resources_Laboratory.dpm_filterBlank."
      "20260106T175906Z.csv"), "Colorado_Department_of_Public_Health_and_Environment_Air_Resources_Laboratory",
     "dpm_filterBlank", "20260106T175906Z"),
])
def test_lab_files(name: str, lab: str, table: str, ts: str) -> None:
    c = classify(name)
    assert (c.kind, c.lab_name, c.table, c.timestamp, c.ext) == ("lab", lab, table, ts, "csv")
    assert (c.domain, c.site, c.month, c.product_code) == (None, None, None, None)
    assert classify(name, product_hint="DP1.20126.001").product_code == "DP1.20126.001"
    assert classify("some/dir/" + name).kind == "lab"


def test_regenerated_kinds() -> None:
    assert filenames.REGENERATED_KINDS == {"variables", "sensor_positions", "readme", "eml", "categorical_codes",
                                           "validation", "science_review_flags"}
    assert filenames.is_regenerated("variables") and not filenames.is_regenerated("data")
    assert not filenames.is_regenerated("lab") and not filenames.is_regenerated(None)


def test_flatten_uses_the_shared_classifier() -> None:
    from neon_ducklake import flatten

    lab = flatten.parse_file_name("NEON.NEON_HQ.mct_taxonTableLabSummary.20251206T221502Z.csv")
    assert (lab.kind, lab.lab, lab.table) == ("lab", "NEON_HQ", "mct_taxonTableLabSummary")
    assert flatten.LAB_FILE_RE is filenames.LAB_FILE_RE


def test_alphanumeric_temporal_index_keeps_the_table_name() -> None:
    """DP4.00001.001 uses TMI codes like 01D/01M; the table must not collapse to '000'."""
    from neon_ducklake.filenames import classify

    for tmi, table in (("01D", "wss_daily_wind"), ("01M", "wss_monthly_temp")):
        c = classify(f"NEON.D14.SRER.DP4.00001.001.000.040.{tmi}.{table}.2025-06.basic.20260108T180323Z.csv")
        assert (c.kind, c.table, c.hor, c.ver, c.tmi, c.month, c.package) == (
            "data", table, "000", "040", tmi, "2025-06", "basic")
    c = classify("NEON.D14.SRER.DP1.00002.001.000.010.030.SAAT_30min.2024-06.basic.20240706T162340Z.csv")
    assert (c.table, c.tmi) == ("SAAT_30min", "030")
