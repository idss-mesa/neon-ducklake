"""flatten.py: payloads -> typed Arrow tables (pure functions, hermetic)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from neon_ducklake import flatten, seal
from tests.test_catalog_fakes import build_payloads, dq_file, dq_payload, make_run, package


@pytest.fixture(scope="module")
def payloads() -> dict:
    return build_payloads()


def naive(*args: int) -> dt.datetime:
    return dt.datetime(*args, tzinfo=dt.UTC).replace(tzinfo=None)


def by(tbl: pa.Table, *keys: str) -> dict:
    return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r for r in tbl.to_pylist()}


def test_every_spec_is_consistent() -> None:
    for name, spec in flatten.TABLES.items():
        assert spec.name == name
        assert set(spec.key) <= set(spec.names), name
        assert len(set(spec.names)) == len(spec.names), name
        assert flatten.empty_table(name).schema == spec.arrow_schema()


def test_products(payloads: dict) -> None:
    out = flatten.flatten_products(payloads["products"]["products"])
    p = by(out["products"], "productCode")
    assert set(p) == {"DP1.00001.001", "DP1.10003.001", "DP1.00006.001", "DP3.30006.001"}
    wind = p["DP1.00001.001"]
    assert wind["isAop"] is False and p["DP3.30006.001"]["isAop"] is True
    assert (wind["siteCount"], wind["cellCount"], wind["provisionalCellCount"]) == (2, 4, 1)
    assert (wind["firstMonth"], wind["lastMonth"]) == ("2020-01", "2020-03")
    assert wind["latestRelease"] == "RELEASE-2025" and wind["latestReleaseDoi"] == "10.48443/s9ya-zc81"
    assert wind["themes"] == ["Atmosphere"] and wind["keywords"] == ["wind", "tower"]
    assert p["DP1.00006.001"]["siteCount"] == 0 and p["DP1.00006.001"]["firstMonth"] is None
    rel = by(out["product_releases"], "productCode", "release")
    r = rel[("DP1.00001.001", "RELEASE-2024")]
    assert r["productDoi"] == "10.48443/old1-0001" and r["productDoiUrl"].startswith("https://doi.org/")
    assert r["generationDate"] == dt.datetime(2024, 1, 26, tzinfo=dt.UTC)
    assert out["product_specs"].num_rows == 8
    assert out["product_biorepository"].to_pylist()[0]["collectionCode"] == "BRD"
    cl = out["product_change_logs"].to_pylist()[0]
    assert isinstance(cl["id"], int) and cl["dateRangeEnd"] is None
    assert out["product_themes"].num_rows == 4 and out["product_keywords"].num_rows == 8


def test_documents(payloads: dict) -> None:
    heads = list(payloads["documents_head"].values())
    docs = by(flatten.flatten_documents(payloads["products"]["products"], heads), "specNumber")
    atbd = docs["NEON.DOC.000780vD"]
    assert atbd["productCount"] == 4 and atbd["headStatus"] == 200 and atbd["fileName"].endswith(".pdf")
    qsg = docs["NEON.QSG.DP1.00001.001v1"]
    assert qsg["headStatus"] == 404 and "404" in qsg["headError"] and qsg["productCodes"] == ["DP1.00001.001"]


def test_releases(payloads: dict) -> None:
    out = flatten.flatten_releases(payloads["releases"]["releases"], payloads["manifests"],
                                   payloads["release_products"], payloads["release_sites"])
    rel = by(out["releases"], "release")
    assert rel["RELEASE-2025"]["manifestCellCount"] == 6  # duplicate month counted once
    assert rel["RELEASE-2025"]["manifestName"] == "manifest-available-2025.json"
    assert rel["RELEASE-2024"]["siteCount"] == 2
    rp = by(out["release_products"], "release", "productCode")
    assert rp[("RELEASE-2024", "DP1.00006.001")]["productDoi"] == "10.48443/3cp7-7s80"
    assert ("RELEASE-2024", "DP1.10003.001") not in rp
    rec = by(out["release_product_records"], "release", "productCode")
    assert ("RELEASE-2024", "DP1.10003.001") not in rec  # not part of that release
    assert rec[("RELEASE-2025", "DP1.00006.001")]["cellCount"] == 1
    mm = out["release_manifest_months"]
    assert mm.num_rows == 4 + 6
    assert ("RELEASE-2025", "DP1.10003.001", "HARV", "2020-06") in by(mm, "release", "productCode", "siteCode", "month")
    rsp = by(out["release_site_products"], "release", "siteCode", "productCode")
    assert rsp[("RELEASE-2024", "SRER", "DP1.00006.001")]["availableMonths"] == ["2020-01", "2020-02"]
    assert out["release_artifacts"].to_pylist()[0]["url"].startswith("https://storage.googleapis.com/")
    assert out["release_product_change_logs"].num_rows == 3 + 4


def test_sites(payloads: dict) -> None:
    out = flatten.flatten_sites(payloads["sites"]["sites"], payloads["locations_sites"]["locations_sites"])
    s = by(out["sites"], "siteCode")
    assert s["HARV"]["siteElevation"] == 348.1 and s["HARV"]["siteUtmZone"] == 18
    props = dict(s["SRER"]["siteProperties"])
    assert props == {"TypeEco": "Desert shrubland", "UTM Zone": "12.0"}  # 'Value for ' removed, values text
    assert s["SRER"]["productCount"] == 2
    assert out["site_locations"].num_rows == 3  # every SITE record, incl. retired MD01
    sp = by(out["site_products"], "siteCode", "productCode")
    wind = sp[("SRER", "DP1.00001.001")]
    assert wind["monthCount"] == 3 and wind["provisionalMonthCount"] == 1
    assert wind["releases"] == ["PROVISIONAL", "RELEASE-2025"]
    assert by(out["domains"], "domainCode")["D14"]["siteCodes"] == ["SRER"]
    assert by(out["site_locations"], "locationName")["SRER"]["locationChildrenCount"] == 1


def test_locations_and_history(payloads: dict) -> None:
    out = flatten.flatten_locations(list(payloads["locations"].values()), payloads["_stage"]["location_nodes"])
    loc = by(out["locations"], "locationName")
    assert set(loc) == {"D14", "SRER", "TOWER1", "SRER_001.basePlot.all"}  # GHOST was not returned
    assert loc["TOWER1"]["offsetLocationName"] == "SRER" and loc["TOWER1"]["hierarchyDepth"] == 1
    assert loc["TOWER1"]["hierarchySite"] == "SRER" and loc["TOWER1"]["locationPolygon"] is None
    assert '"Value for TypeEco"' in loc["SRER"]["locationProperties"]
    plot = loc["SRER_001.basePlot.all"]
    assert plot["hasPolygon"] is True and '"coordinates"' in plot["locationPolygon"]
    poly = out["location_polygons"].to_pylist()
    assert len(poly) == 1 and poly[0]["vertexCount"] == 3
    assert poly[0]["wkt"] == "POLYGON((-110.0 31.0, -110.1 31.0, -110.1 31.1, -110.0 31.0))"
    hist = flatten.flatten_location_history(list(payloads["location_history"].values())).to_pylist()
    assert [h["historyIndex"] for h in hist] == [0, 1]
    assert hist[0]["isCurrent"] is False and hist[1]["isCurrent"] is True
    assert hist[1]["offsetLocationName"] == "SRER" and hist[1]["zOffset"] == 8.5
    assert hist[0]["locationEndDate"] == dt.datetime(2019, 6, 1, tzinfo=dt.UTC)
    assert flatten.polygon_wkt([{"latitude": 1, "longitude": 2}]) is None


def test_availability_current_and_release_only(payloads: dict) -> None:
    am = by(flatten.flatten_availability(payloads["products"]["products"], payloads["manifests"]),
            "productCode", "siteCode", "month")
    cur = am[("DP1.00001.001", "SRER", "2020-03")]
    assert cur["release"] == "PROVISIONAL" and cur["isProvisional"] and not cur["isReleaseOnly"]
    assert cur["availableDataUrl"].endswith("/DP1.00001.001/SRER/2020-03")
    assert am[("DP1.00001.001", "SRER", "2020-01")]["release"] == "RELEASE-2025"
    ro1 = am[("DP1.00006.001", "SRER", "2020-01")]
    assert ro1["isReleaseOnly"] and ro1["release"] == "RELEASE-2025" and ro1["availableDataUrl"] is None
    assert am[("DP1.00006.001", "SRER", "2020-02")]["release"] == "RELEASE-2024"  # newest manifest with it
    assert am[("DP3.30006.001", "SRER", "2019-07")]["isReleaseOnly"]
    assert len(am) == 4 + 1 + 1 + 2 + 1


def _dq_lines(payloads: dict, product: str) -> list[dict]:
    return [{"key": k, "fetchedAt": "2026-01-01T00:00:00+00:00", "request": {}, "payload": v}
            for k, v in payloads["data_query"].items() if k.startswith(product)]


def test_data_query_files(payloads: dict) -> None:
    res = flatten.flatten_data_query(_dq_lines(payloads, "DP1.10003.001"), "DP1.10003.001")
    files = res.files.to_pylist()
    assert {f["productCode"] for f in files} == {"DP1.10003.001"}  # DP0 support files attributed to DP1
    kinds = {f["name"].split(".")[6]: f["kind"] for f in files}
    assert kinds == {"brd_countdata": "data", "categoricalCodes": "categorical_codes", "validation": "validation",
                     "variables": "variables"}
    assert all(f["parseConsistent"] for f in files)
    data = next(f for f in files if f["kind"] == "data")
    assert data["tableName"] == "brd_countdata" and data["publicationTimestamp"] == naive(2025, 1, 6, 22, 31, 27)
    assert data["releaseGenerationDate"] == dt.datetime(2025, 1, 29, 0, 7, 30, tzinfo=dt.UTC)
    assert data["objectUrl"].startswith("https://storage.googleapis.com/") and "?" not in data["objectUrl"]
    assert res.pair_counts == {"HARV|CURRENT": {"files": 4, "bytes": 770, "months": 1}}
    assert res.packages.to_pylist()[0]["fileCount"] == 4


def test_data_query_is_flags_and_dedupe(payloads: dict) -> None:
    res = flatten.flatten_data_query(_dq_lines(payloads, "DP1.00001.001"), "DP1.00001.001")
    files = res.files.to_pylist()
    assert len(files) == 15
    prov = [f for f in files if f["month"] == "2020-03"]
    assert all(f["isProvisional"] and f["release"] == "PROVISIONAL" and not f["isReleaseOnly"] for f in prov)
    is_data = [f for f in files if f["kind"] == "data"]
    assert {(f["hor"], f["ver"], f["tmi"]) for f in is_data} == {("000", "010", "030"), ("000", "010", "002")}
    eml = next(f for f in files if f["kind"] == "eml")
    assert eml["size"] is None and eml["parseConsistent"]
    assert {ld["requestRelease"] for ld in res.landings} == {"CURRENT"}
    ro = flatten.flatten_data_query(_dq_lines(payloads, "DP1.00006.001"), "DP1.00006.001")
    assert all(f["isReleaseOnly"] for f in ro.files.to_pylist())
    assert set(ro.pair_counts) == {"SRER|RELEASE-2024", "SRER|RELEASE-2025"}
    # an overlapping later landing replaces the earlier copy of the same file
    feb = next(f for f in ro.files.to_pylist() if f["month"] == "2020-02")
    newer = dq_payload("DP1.00006.001", "SRER", ("2020-02", "2020-02"), [
        {"release": "RELEASE-2024", "generationDate": "2024-01-26T00:00:00Z", "packages": [
            package("D14", "SRER", "2020-02", "2024-01-26T00:00:00Z", [
                {**dq_file(feb["name"], 4242, "SRER", "DP1.00006.001", "2020-02")}])]}],
        "RELEASE-2024")
    lines = _dq_lines(payloads, "DP1.00006.001") + [{"key": "DP1.00006.001__SRER__2020-02_2020-02__RELEASE-2024x",
                                                     "fetchedAt": "2026-02-01T00:00:00+00:00", "payload": newer}]
    again = flatten.flatten_data_query(lines, "DP1.00006.001").files.to_pylist()
    assert len(again) == ro.files.num_rows
    assert 4242 in {f["size"] for f in again}


def test_lab_files_are_recognised() -> None:
    lab = flatten.parse_file_name("NEON.Jonah_Ventures.inv_metabarcodeSummary.20251205T051406Z.csv")
    assert (lab.kind, lab.table, lab.timestamp, lab.ext, lab.lab) == (
        "lab", "inv_metabarcodeSummary", "20251205T051406Z", "csv", "Jonah_Ventures")
    assert flatten.parse_consistent(lab, "DP1.20126.001", "ARIK", "D10", "2018-07")
    long = flatten.parse_file_name("NEON.Colorado_Department_of_Public_Health_and_Environment_Air_Resources_"
                                   "Laboratory.dpm_filterBlank.20260106T175906Z.csv")
    assert long.kind == "lab" and long.table == "dpm_filterBlank"
    assert flatten.parse_file_name("something.else.csv").kind == "other"
    std = flatten.parse_file_name("NEON.D14.SRER.DP1.00001.001.readme.20250128T000000Z.txt")
    assert std.kind == "readme"
    pkg = dq_payload("DP1.20126.001", "ARIK", ("2018-07", "2018-07"), [
        {"release": "RELEASE-2025", "generationDate": "2025-01-29T00:07:30Z", "packages": [
            package("D10", "ARIK", "2018-07", "2025-01-29T00:07:30Z", [
                dq_file("NEON.NEON_HQ.inv_dnaExtractionStandard.20251205T051406Z.csv", 10, "ARIK", "DP1.20126.001",
                        "2018-07")])]}], None)
    row = flatten.flatten_data_query([{"key": "k", "payload": pkg}], "DP1.20126.001").files.to_pylist()[0]
    assert (row["kind"], row["tableName"], row["parseConsistent"]) == ("lab", "inv_dnaExtractionStandard", True)
    assert row["publicationTimestamp"] == naive(2025, 12, 5, 5, 14, 6) and row["ext"] == "csv"


def test_parse_consistent_detects_mismatch() -> None:
    from neon_mcp.neon.filenames import parse_neon_filename

    good = parse_neon_filename("NEON.D14.SRER.DP1.00001.001.000.010.030.2DWSD_30min.2020-01.basic.20250128T000000Z.csv")
    assert flatten.parse_consistent(good, "DP1.00001.001", "SRER", "D14", "2020-01")
    assert not flatten.parse_consistent(good, "DP1.00001.001", "HARV", "D14", "2020-01")
    assert not flatten.parse_consistent(good, "DP1.00001.001", "SRER", "D14", "2020-02")
    assert not flatten.parse_consistent(parse_neon_filename("random.csv"), "DP1.00001.001", "SRER", "D14", "2020-01")


def test_product_site_status(payloads: dict) -> None:
    pairs = [{"productCode": "DP1.00001.001", "siteCode": "SRER", "status": "ok"},
             {"productCode": "DP1.00001.001", "siteCode": "HARV", "status": "ok"},
             {"productCode": "DP1.10003.001", "siteCode": "HARV", "status": "error"},
             {"productCode": "DP1.00006.001", "siteCode": "SRER", "status": "ok"}]
    ext = [{"productCode": "DP1.00001.001", "siteCode": "HARV"}]
    ps = by(flatten.flatten_product_site(payloads["products"]["products"], payloads["manifests"], pairs, ext,
                                         {("DP1.00001.001", "SRER"): (15, 999), ("DP1.00006.001", "SRER"): (5, 1)}),
            "productCode", "siteCode")
    assert ps[("DP1.00001.001", "SRER")]["inventoryStatus"] == "ok"
    assert ps[("DP1.00001.001", "HARV")]["inventoryStatus"] == "external"
    assert ps[("DP1.10003.001", "HARV")]["inventoryStatus"] == "error"
    ro = ps[("DP1.00006.001", "SRER")]
    assert ro["inventoryScope"] == "release-only" and ro["releaseOnlyMonthCount"] == 2
    assert ro["releaseOnlyReleases"] == ["RELEASE-2024", "RELEASE-2025"]
    aop = ps[("DP3.30006.001", "SRER")]
    assert aop["inventoryScope"] == "excluded:AOP" and aop["inventoryStatus"] is None and aop["fileCount"] is None
    assert aop["currentMonthCount"] == 1 and aop["releaseOnlyMonthCount"] == 1
    empty = flatten.flatten_product_site(payloads["products"]["products"], {}, [
        {"productCode": "DP1.00001.001", "siteCode": "HARV", "status": "ok"}], [], {})
    assert by(empty, "productCode", "siteCode")[("DP1.00001.001", "HARV")]["inventoryStatus"] == "empty"
    assert by(empty, "productCode", "siteCode")[("DP1.00001.001", "SRER")]["inventoryStatus"] == "missing"


def test_data_query_pairs_from_plan_landings_ledger() -> None:
    plan = [{"product": "DP1.00001.001", "site": "SRER", "start": "2020-01", "end": "2020-03", "release": None,
             "months": ["2020-01", "2020-02", "2020-03"], "scope": "current"},
            {"product": "DP1.00001.001", "site": "HARV", "start": "2020-01", "end": "2020-01", "release": None,
             "months": ["2020-01"], "scope": "current"},
            {"product": "DP1.00006.001", "site": "SRER", "start": "2020-02", "end": "2020-02",
             "release": "RELEASE-2024", "months": ["2020-02"], "scope": "release-only"}]
    landings = [{"productCode": "DP1.00001.001", "siteCode": "SRER", "requestRelease": "CURRENT", "scope": "current",
                 "window": ["2020-01", "2020-01"], "fetchedAt": "2026-01-01T00:00:00+00:00"},
                {"productCode": "DP1.00001.001", "siteCode": "SRER", "requestRelease": "CURRENT", "scope": "current",
                 "window": ["2020-02", "2020-03"], "fetchedAt": "2026-01-02T00:00:00+00:00"}]
    ledger = [{"family": "data_query", "key": "DP1.00001.001__SRER__2020-01_2020-03", "status": "split"},
              {"family": "data_query", "key": "DP1.00001.001__HARV__2020-01_2020-01", "status": "error",
               "error": "NeonApiError: 500"}]
    out = by(flatten.flatten_data_query_pairs(plan, landings, {("DP1.00001.001", "SRER", "CURRENT"):
                                                              {"files": 3, "bytes": 30, "months": 3}}, ledger),
             "productCode", "siteCode", "requestRelease")
    srer = out[("DP1.00001.001", "SRER", "CURRENT")]
    assert (srer["status"], srer["landingCount"], srer["splitCount"], srer["fileCount"]) == ("ok", 2, 1, 3)
    assert srer["lastFetchedAt"] == dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
    assert out[("DP1.00001.001", "HARV", "CURRENT")]["status"] == "error"
    assert out[("DP1.00006.001", "SRER", "RELEASE-2024")]["status"] == "missing"


def test_taxonomy_samples_prototypes_graphql(payloads: dict) -> None:
    tax = by(flatten.flatten_taxonomy(list(payloads["taxonomy"].values())), "taxonID")
    robin = tax["AMRO"]
    assert robin["scientificName"] == "Turdus migratorius" and robin["order"] == "Passeriformes"
    assert '"dwc:' not in robin["record"] and '"scientificName":"Turdus migratorius"' in robin["record"]
    assert robin["updateDate"] == dt.datetime(2022, 4, 5, 14, 10, 34, tzinfo=dt.UTC)
    sc = flatten.flatten_sample_classes(payloads["samples"]["supportedClasses"]).to_pylist()
    assert [r["sampleClass"] for r in sc] == ["brd.sampleID", "wdp_collection_in.sampleID"]
    pr = flatten.flatten_prototypes(payloads["prototype"]["datasets"], list(payloads["prototype_data"].values()))
    ds = pr["prototype_datasets"].to_pylist()[0]
    assert ds["doi"] == "10.48443/7ay8-e395" and ds["fileCount"] == 1 and ds["relatedDataProducts"] == ["DP1.20190.001"]
    assert ds["fileTypes"] == ["CSV"] and ds["relatedVersions"] == "[]"
    assert pr["prototype_files"].to_pylist()[0]["typeName"] == "EML"
    assert pr["prototype_locations"].to_pylist()[0]["siteCode"] == "BLUE"
    gq = flatten.flatten_graphql(payloads["graphql"]["schema"])
    types = by(gq["graphql_types"], "name")
    assert types["Query"]["rootOperation"] == "query" and types["Kind"]["enumValueCount"] == 1
    f = by(gq["graphql_fields"], "typeName", "memberKind", "name")[("Query", "field", "findLocations")]
    assert f["type"] == "[Location]!" and '"type":"LocationQuery!"' in f["args"]


def test_external_and_requests(payloads: dict) -> None:
    lines = [{"key": k, "payload": v} for k, v in payloads["external"].items()]
    ext = flatten.flatten_external(lines).to_pylist()
    assert ext == [{"productCode": "DP1.00001.001", "siteCode": "HARV", "monthProbed": "2020-01", "name": "Phenocam",
                    "type": "Phenocam Website",
                    "url": "https://phenocam.nau.edu/webcam/sites/NEON.D01.HARV.DP1.00033/"}]
    req = flatten.flatten_requests([{"run_id": "r", "family": "manifests", "key": "RELEASE-2025", "params": "null",
                                     "status": "error", "fetched_at": "2026-01-01T00:00:00Z",
                                     "error": "GET https://storage.googleapis.com/a.json?X-Goog-Signature=zz failed"}])
    row = req.to_pylist()[0]
    assert "X-Goog" not in row["error"] and row["error"].endswith("a.json failed")
    assert row["fetchedAt"] == dt.datetime(2026, 1, 1, tzinfo=dt.UTC) and row["params"] is None


def test_value_helpers() -> None:
    assert flatten.tstz("2021-01-23T02:30:02Z") == dt.datetime(2021, 1, 23, 2, 30, 2, tzinfo=dt.UTC)
    assert flatten.tstz("2021-01-23T02:30:02") == dt.datetime(2021, 1, 23, 2, 30, 2, tzinfo=dt.UTC)
    assert flatten.tstz("not a date") is None and flatten.tstz("") is None
    assert flatten.neon_ts("20250128T000000Z") == naive(2025, 1, 28)
    assert flatten._int("12.0") == 12 and flatten._int(None) is None
    assert flatten.strip_query("https://x/y?X-Goog-Signature=1") == "https://x/y"
    assert flatten.is_aop("DP1.30003.001") and flatten.is_aop("DP1.00001.001", "AOP")
    assert not flatten.is_aop("DP1.00001.001", "TIS")


def test_flatten_run_with_worker_pool_and_cache(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    ledger = seal.read_ledger(run.run_id, run.ledger_db)
    seal.seal_run(run.run_dir, ledger, workers=1)
    fl = flatten.flatten_run(run.run_dir, ledger, workers=2)
    assert fl.stats["products"] == 3 and fl.stats["cachedProducts"] == 0
    assert fl.stats["fileRows"] == 15 + 4 + 5
    assert set(fl.tables) | set(flatten.PARQUET_TABLES) == set(flatten.TABLES) - {"catalog_builds", "table_catalog"}
    assert pq.read_table(fl.parquet["files"][0]).schema == flatten.TABLES["files"].arrow_schema()
    again = flatten.flatten_run(run.run_dir, ledger, workers=1)
    assert again.stats["cachedProducts"] == 3
    ps = by(fl.tables["product_site"], "productCode", "siteCode")
    assert {k: v["inventoryStatus"] for k, v in ps.items()} == {
        ("DP1.00001.001", "SRER"): "ok", ("DP1.00001.001", "HARV"): "external", ("DP1.10003.001", "HARV"): "ok",
        ("DP1.00006.001", "SRER"): "ok", ("DP3.30006.001", "SRER"): None}
    dq = by(fl.tables["data_query_pairs"], "productCode", "siteCode", "requestRelease")
    assert len(dq) == 5 and {r["status"] for r in dq.values()} == {"ok"}
    assert fl.tables["harvest_runs"].to_pylist()[0]["sealedManifestMd5"] == fl.stats["sealedManifestMd5"]


def test_flatten_run_requires_sealed(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    with pytest.raises(SystemExit, match="seal"):
        flatten.flatten_run(run.run_dir, [], workers=1)
