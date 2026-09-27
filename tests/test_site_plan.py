"""site plan: harvest-landing fallback and the catalog-lake (v_site_inventory) path."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from neon_ducklake import env
from neon_ducklake.site import plan

from .test_site_common import SITE, query_payload, site_env, write_gz  # noqa: F401 - site_env is a fixture


def _fallback_run(root: Path) -> None:
    old = root / "api" / "runs" / "20260101T000000Z"
    new = root / "api" / "runs" / "20260201T000000Z"
    pairs = [
        {"product": "DP1.00002.001", "site": SITE, "start": "2024-05", "end": "2024-06", "release": None,
         "months": ["2024-05", "2024-06"], "scope": "current"},
        {"product": "DP1.00006.001", "site": SITE, "start": "2019-01", "end": "2019-01", "release": "RELEASE-2025",
         "months": ["2019-01"], "scope": "release-only"},
        {"product": "DP1.00033.001", "site": SITE, "start": "2024-05", "end": "2024-06", "release": None,
         "months": ["2024-05", "2024-06"], "scope": "current"},
        {"product": "DP1.10003.001", "site": SITE, "start": "2024-06", "end": "2024-06", "release": None,
         "months": ["2024-06"], "scope": "current"},
    ]
    write_gz(new / "stage" / "pairs.json.gz", pairs)
    write_gz(new / "raw" / "products" / "products.json.gz", [
        {"productCode": "DP1.00002.001", "productName": "Air temp", "productScienceTeamAbbr": "TIS",
         "siteCodes": [{"siteCode": SITE, "availableMonths": ["2024-05", "2024-06"], "availableReleases": [
             {"release": "RELEASE-2026", "availableMonths": ["2024-05"]},
             {"release": "PROVISIONAL", "availableMonths": ["2024-06"]}]}]},
        {"productCode": "DP3.30010.001", "productName": "Camera mosaic", "productScienceTeamAbbr": "AOP",
         "siteCodes": [{"siteCode": SITE, "availableMonths": ["2019-05", "2021-05"]}]}])
    f = [{"name": "a.csv", "size": 100}, {"name": "b.csv", "size": 50}]
    # bisected landings in the new run; an older landing of the same pair must be ignored
    write_gz(new / "raw" / "data_query" / f"DP1.00002.001__{SITE}__2024-05_2024-05.json.gz", {
        **query_payload("DP1.00002.001", [("RELEASE-2026", None, [("2024-05", f)])]),
        "_request": {"pair": {"product": "DP1.00002.001", "site": SITE, "release": None, "scope": "current"}}})
    write_gz(new / "raw" / "data_query" / f"DP1.00002.001__{SITE}__2024-06_2024-06.json.gz", {
        **query_payload("DP1.00002.001", [("PROVISIONAL", None, [("2024-06", f[:1])])]),
        "_request": {"pair": {"product": "DP1.00002.001", "site": SITE, "release": None, "scope": "current"}}})
    write_gz(old / "raw" / "data_query" / f"DP1.00002.001__{SITE}__2024-05_2024-06.json.gz",
             query_payload("DP1.00002.001", [("RELEASE-2026", None, [("2024-05", f * 10)])]))
    write_gz(old / "raw" / "data_query" / f"DP1.00006.001__{SITE}__2019-01_2019-01__RELEASE-2025.json.gz", {
        **query_payload("DP1.00006.001", [("RELEASE-2025", None, [("2019-01", f)])]),
        "_request": {"pair": {"product": "DP1.00006.001", "site": SITE, "release": "RELEASE-2025",
                              "scope": "release-only"}}})
    # a partial landing is never read
    (new / "raw" / "data_query" / f"DP1.10003.001__{SITE}__2024-06_2024-06.json.gz.part").write_bytes(b"junk")


def test_fallback_plan(site_env: Path, capsys) -> None:  # noqa: F811
    _fallback_run(site_env)
    assert plan.main(SITE) == 0
    out = json.loads((site_env / "sites" / SITE / "reports" / "plan.json").read_text())
    assert out["source"] == "stage:20260201T000000Z"
    by = {p["productCode"]: p for p in out["products"]}
    assert by["DP1.00002.001"]["files"] == 3 and by["DP1.00002.001"]["bytes"] == 250
    assert by["DP1.00002.001"]["provisionalMonths"] == 1 and by["DP1.00002.001"]["releases"] == [
        "PROVISIONAL", "RELEASE-2026"]
    assert by["DP1.00006.001"]["scope"] == "release-only" and by["DP1.00006.001"]["files"] == 2
    assert by["DP1.00033.001"]["external"] and by["DP1.00033.001"]["listed"]
    assert not by["DP1.10003.001"]["listed"]
    t = out["totals"]
    assert (t["products"], t["siteMonths"], t["releaseOnlySiteMonths"], t["files"]) == (4, 6, 1, 5)
    assert t["productsNotYetListed"] == ["DP1.10003.001"]
    assert out["excludedAop"] == [{"productCode": "DP3.30010.001", "productName": "Camera mosaic", "months": 2}]
    printed = capsys.readouterr().out
    assert "excluded AOP products (1): DP3.30010.001(2m)" in printed and "release-only" in printed


def test_lake_plan_and_pairs(site_env: Path) -> None:  # noqa: F811
    lake = env.LAKE_DIR
    lake.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{lake / env.CATALOG_LAKE}' AS neon (DATA_PATH '{lake / 'data'}/')")
    con.execute("""CREATE TABLE neon.inv AS SELECT * FROM (VALUES
        ('DP1.00002.001', 'TEST', '2024-05', 'Air temp', 'TIS', 'RELEASE-2026', false, false, false, 3, 300),
        ('DP1.00002.001', 'TEST', '2024-06', 'Air temp', 'TIS', 'PROVISIONAL', true, false, false, 3, 310),
        ('DP1.00006.001', 'TEST', '2019-01', 'Precip', 'TIS', 'RELEASE-2025', false, true, false, 5, 50),
        ('DP1.00006.001', 'TEST', '2019-02', 'Precip', 'TIS', 'RELEASE-2025', false, true, false, 5, 50),
        ('DP1.00033.001', 'TEST', '2024-06', 'Phenocam', 'TIS', 'PROVISIONAL', true, false, true, 0, 0),
        ('DP1.00002.001', 'OTHR', '2024-06', 'Air temp', 'TIS', 'PROVISIONAL', true, false, false, 3, 310))
        t(productCode, siteCode, month, productName, productScienceTeamAbbr, release, isProvisional, isReleaseOnly,
          isExternal, fileCount, totalBytes)""")
    con.execute("CREATE VIEW neon.v_site_inventory AS SELECT * FROM neon.inv")
    con.execute("CREATE TABLE neon.products AS SELECT * FROM (VALUES ('DP1.30001.001', 'Lidar', true), "
                "('DP1.00002.001', 'Air temp', false)) t(productCode, productName, isAop)")
    con.execute("CREATE TABLE neon.availability_months AS SELECT * FROM (VALUES ('DP1.30001.001', 'TEST', '2021-05'), "
                "('DP1.00002.001', 'TEST', '2024-05')) t(productCode, siteCode, month)")
    con.execute("DETACH neon")
    con.close()
    p = plan.build_plan(SITE)
    assert p["source"] == "lake:v_site_inventory"
    t = p["totals"]
    assert (t["products"], t["siteMonths"], t["files"], t["bytes"], t["externalProducts"]) == (3, 5, 16, 710, 1)
    assert p["excludedAop"] == [{"productCode": "DP1.30001.001", "productName": "Lidar", "months": 1}]
    pairs, source = plan.site_pairs(SITE)
    assert source == "lake"
    assert pairs[2]["monthReleases"] == {"2024-06": "PROVISIONAL"}
    assert [(q["product"], q["scope"], q["release"], q["months"], q["external"]) for q in pairs] == [
        ("DP1.00002.001", "current", None, ["2024-05", "2024-06"], False),
        ("DP1.00006.001", "release-only", "RELEASE-2025", ["2019-01", "2019-02"], False),
        ("DP1.00033.001", "current", None, ["2024-06"], True)]
