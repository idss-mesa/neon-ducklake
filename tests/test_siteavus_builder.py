"""site/avus.py Builder: levels, vocabulary, units and every data-object profile (synthetic facts)."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pytest

from neon_ducklake.avu import datacite
from neon_ducklake.avu.normalize import normalize
from neon_ducklake.site import avus
from neon_ducklake.site.avus import Builder, Ontology

from .test_site_common import site_env  # noqa: F401 - fixture
from .test_siteavus_common import (
    P_EXT,
    P_H5,
    P_IS,
    P_OS,
    P_RO,
    SITE,
    detach_site_avus,
    make_facts,
    write_local_files,
)

detach_site_avus()  # see test_siteavus_common.detach_site_avus

NEON_ATTR = re.compile(r"^neon\.[a-z][A-Za-z0-9]*$")
UNITS = {"", "URL", "ISO8601", "ISO8601 interval", "WGS84 lat lon", "meter", "byte", "percent", "count",
         "relative path"}
UNIT_RE = re.compile(r"^(doi:10\.\d+/\S+|ducklake:sites/\S+|[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_]+|NEON:[A-Z]+:\S+)$")


def build(facts=None, products=None, months=None) -> tuple[Builder, dict[str, dict]]:
    b = Builder(facts or make_facts(), products, months)
    rows = b.build()
    by_path: dict[str, dict] = defaultdict(lambda: {"level": None, "avus": set(), "units": set()})
    for unit_id, path, _t, a, v, u, level, _g in rows:
        d = by_path[path]
        d["level"], d["unit_id"] = level, unit_id
        d["avus"].add((a, v, u))
    return b, by_path


def attrs(d: dict) -> dict[str, list[tuple[str, str]]]:
    out: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for a, v, u in d["avus"]:
        out[a].append((v, u))
    return out


def one(d: dict, a: str) -> tuple[str, str]:
    got = attrs(d)[a]
    assert len(got) == 1, (a, got)
    return got[0]


@pytest.fixture
def built(site_env: Path):  # noqa: F811
    f = make_facts()
    write_local_files(f)
    b, by = build(f)
    return b, by, f"/zone/home/u/proj/sites/{SITE}"


def test_vocabulary_casing_units_and_levels(built) -> None:
    _b, by, _S = built
    levels = {d["level"] for d in by.values()}
    assert {"site", "product", "month", "object"} <= levels
    for path, d in by.items():
        for a, v, u in d["avus"]:
            if a.startswith("neon."):
                assert NEON_ATTR.match(a), a
            assert u in UNITS or UNIT_RE.match(u), (path, a, u)
            assert normalize(a, v, u) is not None
    # unit_id: collection = itself, object = its parent collection
    for path, d in by.items():
        if d["level"] == "object":
            assert d["unit_id"] == path.rsplit("/", 1)[0]
        if d["level"] in ("site", "product", "month"):
            assert d["unit_id"] == path


def test_site_level(built) -> None:
    b, by, S = built
    site = by[S]
    assert one(site, "neon.sitePoint") == ("31.91068 -110.83549", "WGS84 lat lon")
    assert one(site, "neon.siteElevation") == ("997.476", "meter")
    assert one(site, "neon.deimsId") == ("https://deims.org/test", "URL")
    assert one(site, "neon.package") == ("basic", "")
    assert one(site, "neon.excludedTeam") == ("AOP", "")
    assert one(site, "neon.productCount") == ("5", "count")
    assert one(site, "neon.externalProductCount") == ("1", "count")
    assert one(site, "neon.siteMonths") == ("6", "count")
    assert one(site, "neon.provisionalSiteMonths") == ("2", "count")
    assert one(site, "neon.releaseOnlySiteMonths") == ("1", "count")
    assert one(site, "neon.fileCount")[0] == str(len(b.f.files))
    assert one(site, "neon.downloadedWith") == ("neon-mcp 0.1.0", "")
    assert one(site, "neon.downloadDate") == ("2026-09-27", "ISO8601")
    assert one(site, "mesa.ducklake.catalog") == (f"sites/{SITE}/ducklake/neon_test.ducklake", "relative path")
    cov = one(site, "neon.temporalCoverage")
    assert cov == ("2019-05-01T00:00:00Z/2026-08-28T23:30:00Z", "ISO8601 interval")
    envo = attrs(site)
    # curated biome (value = typeEco) + NLCD classes >= 1 % (evergreenForest at 0.5 % is dropped)
    assert envo["envo.xeric_shrubland_biome"] == [("Desert shrubland", "ENVO:01000218")]
    assert envo["envo.area_of_scrub"] == [("NLCD 2006 shrubScrub 98.00 km2 (98.0%)", "ENVO:01000869")]
    assert envo["envo.area_of_grassland"] == [("NLCD 2006 grasslandHerbaceous 1.50 km2 (1.5%)", "ENVO:01000888")]
    assert "envo.area_of_evergreen_forest" not in envo
    dc = {a: v for a, v, _u in site["avus"] if a.startswith("datacite.")}
    assert dc["datacite.identifier"] == f"https://de.cyverse.org/data/ds{S}"
    assert dc["datacite.resourceTypeGeneral"] == "Collection"
    assert dc["datacite.creator.1.nameIdentifier"] == "https://ror.org/04j43p132"
    assert dc["datacite.contributor.1.name"] == "testuser" and dc["datacite.contributor.1.contributorType"] == \
        "DataCurator"
    assert dc["datacite.publicationYear"] == "2026"
    assert "https://www.neonscience.org/field-sites/test" in dc.values()
    assert "https://deims.org/test" in dc.values()
    assert dc["datacite.geoLocation.1.point"] == "31.91068 -110.83549"
    # the NEON siteDescription only repeats the name -> a composed abstract
    assert dc["datacite.description.1.value"].startswith("All non-AOP NEON data")
    datacite.roundtrip(sorted((a, v, u) for a, v, u in site["avus"] if a.startswith("datacite.")))


def test_containers_and_lake_tree(built, site_env: Path) -> None:  # noqa: F811
    root = site_env / "sites" / SITE
    (root / "ducklake" / "data" / "main" / "dp1_00002_001__saat_30min").mkdir(parents=True)
    (root / "ducklake" / "neon_test.ducklake").write_bytes(b"duck")
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"productCode": [P_IS, P_IS], "x": [1, 2]}),
                   root / "ducklake" / "data" / "main" / "dp1_00002_001__saat_30min" / "ducklake-a.parquet")
    (root / "ducklake" / "data" / "main" / "variables").mkdir()
    pq.write_table(pa.table({"productCode": [P_IS, P_OS]}), root / "ducklake" / "data" / "main" / "variables" / "v.parquet")
    (root / "reports").mkdir(exist_ok=True)
    (root / "reports" / "plan.json").write_text("{}")
    (root / "cards").mkdir()
    (root / "cards" / f"{P_IS}.md").write_text("# card")
    (root / "tmp").mkdir(exist_ok=True)  # never uploaded -> never tagged
    _b, by = build(make_facts())
    S = f"/zone/home/u/proj/sites/{SITE}"
    for c in ("raw", "ducklake", "reports", "cards"):
        d = by[f"{S}/{c}"]
        assert d["level"] == "container"
        assert {a for a, _v, _u in d["avus"]} == {"dc.description", "dc.type", "provenance.generator", "neon.siteCode"}
    assert f"{S}/tmp" not in by and f"{S}/curation" not in by
    t = by[f"{S}/ducklake/data/main/dp1_00002_001__saat_30min/ducklake-a.parquet"]
    assert t["level"] == "lake-file" and t["unit_id"] == f"{S}/ducklake/data/main/dp1_00002_001__saat_30min"
    assert one(t, "mesa.ducklake.table") == ("dp1_00002_001__saat_30min", f"ducklake:sites/{SITE}/ducklake/neon_test.ducklake")
    assert one(t, "neon.rowCount") == ("2", "count") and one(t, "neon.productCode") == (P_IS, "")
    assert one(t, "dc.format") == ("application/vnd.apache.parquet", "")
    v = by[f"{S}/ducklake/data/main/variables/v.parquet"]
    assert "neon.productCode" not in attrs(v)  # mixed products: no single code
    cat = by[f"{S}/ducklake/neon_test.ducklake"]
    assert one(cat, "mesa.ducklake.catalog") == (f"sites/{SITE}/ducklake/neon_test.ducklake", "relative path")
    tbl_dir = by[f"{S}/ducklake/data/main/dp1_00002_001__saat_30min"]
    assert tbl_dir["level"] == "lake-dir" and one(tbl_dir, "neon.siteCode") == (SITE, "")
    assert one(by[f"{S}/reports/plan.json"], "provenance.generator") == ("neon-ducklake site plan", "")
    assert one(by[f"{S}/cards/{P_IS}.md"], "neon.productCode") == (P_IS, "")


def test_versioned_catalogs_collection_only_after_a_lake_push(site_env: Path) -> None:  # noqa: F811
    """A streamed lake is published under ducklake/catalogs/ (push_lake): that collection gets a lake-dir row, so
    the verify rule (every collection carries a neon.*/mesa.* AVU) holds; a never-pushed lake has none."""
    from neon_ducklake import state
    from neon_ducklake.site import chunks

    root = site_env / "sites" / SITE
    (root / "ducklake").mkdir(parents=True, exist_ok=True)
    (root / "ducklake" / "neon_test.ducklake").write_bytes(b"duck")
    S = f"/zone/home/u/proj/sites/{SITE}"
    _b, by = build(make_facts())
    assert f"{S}/ducklake/catalogs" not in by
    con = state.connect(state.DB_PATH)
    try:
        chunks.ensure_schema(con)
        chunks.record_lake_push(con, SITE, "0" * 32, 7, kind="push", snapshot_key="k7")
        con.commit()
    finally:
        con.close()
    _b, by = build(make_facts())
    d = by[f"{S}/ducklake/catalogs"]
    assert d["level"] == "lake-dir" and d["unit_id"] == f"{S}/ducklake/catalogs"
    assert one(d, "neon.siteCode") == (SITE, "") and "dc.description" in attrs(d)


def test_product_level(built) -> None:
    _b, by, S = built
    p = by[f"{S}/raw/{P_IS}"]
    a = attrs(p)
    assert a["neon.release"] == [("RELEASE-2026", "doi:10.48443/p69b-5e50")]  # RELEASE-2025 contributes nothing
    assert one(p, "neon.months") == ("2024-06/2024-06,2026-08/2026-08", "ISO8601 interval")
    assert one(p, "neon.provisionalMonths") == ("2026-08/2026-08", "ISO8601 interval")
    assert "neon.releaseOnlyMonths" not in a and "neon.releaseOnly" not in a
    assert sorted(a["neon.table"]) == [("SAAT_30min", "")]
    assert (("dp1_00002_001__saat_30min", f"ducklake:sites/{SITE}/ducklake/neon_test.ducklake")
            in a["mesa.ducklake.table"])
    assert a["neon.theme"] == [("Atmosphere", "")] and sorted(a["neon.keyword"]) == [("air temperature", ""),
                                                                                    ("tower", "")]
    # uo: mapped basic-package variables only (expanded + unmapped units are skipped)
    assert a["uo.degree_celsius"] == [("SAAT_30min.tempSingleMean", "UO:0000027")]
    assert "uo.percent" not in a
    assert a["envo.air"] == [("SAAT_30min.tempSingleMean", "ENVO:00002005")]  # accepted curated term as stored
    dc = {x: v for x, v, _u in p["avus"] if x.startswith("datacite.")}
    assert dc["datacite.titles.1.value" if "datacite.titles.1.value" in dc else "datacite.title.1.value"] == \
        "NEON Single aspirated air temperature (DP1.00002.001) at Test Site NEON (TEST), 2024-2026"
    rel = {(dc[f"datacite.relatedIdentifier.{i}.value"], dc[f"datacite.relatedIdentifier.{i}.relationType"])
           for i in range(1, 10) if f"datacite.relatedIdentifier.{i}.value" in dc}
    assert ("10.48443/p69b-5e50", "IsDerivedFrom") in rel
    assert ("https://data.neonscience.org/data-products/DP1.00002.001", "IsDescribedBy") in rel
    assert ("https://data.neonscience.org/api/v0/documents/NEON.DOC.000646vD", "IsDocumentedBy") in rel
    assert ("https://www.neonscience.org/field-sites/test", "References") in rel
    others = [dc[k.replace("descriptionType", "value")] for k, v in dc.items()
              if k.endswith("descriptionType") and v == "Other"]
    assert any("RELEASE-2026. https://doi.org/10.48443/p69b-5e50" in o for o in others)
    assert any("provisional data" in o for o in others)
    assert dc["datacite.version"] == "RELEASE-2026 + PROVISIONAL"
    assert "air temperature" in dc.values() and "air (ENVO:00002005)" in dc.values()
    assert len(dc["datacite.description.1.value"].encode()) <= 2000
    datacite.roundtrip(sorted((x, v, u) for x, v, u in p["avus"] if x.startswith("datacite.")))
    # OS product: union of resolved taxa only; no neon.taxon at product level
    o = attrs(by[f"{S}/raw/{P_OS}"])
    assert o["ncbitaxon.amphispiza_bilineata"] == [("Black-throated Sparrow", "NCBITaxon:198939")]
    assert "neon.taxon" not in o and o["neon.taxaCount"] == [("2", "count")]
    # external + release-only products
    e = attrs(by[f"{S}/raw/{P_EXT}"])
    assert e["neon.externalDataUrl"] == [("https://phenocam.nau.edu/webcam/sites/NEON.D99.TEST/", "URL")]
    assert e["neon.externalHost"] == [("phenocam.nau.edu", "")] and e["neon.fileCount"] == [("0", "count")]
    r = attrs(by[f"{S}/raw/{P_RO}"])
    assert r["neon.releaseOnly"] == [("true", "")]
    assert r["neon.release"] == [("RELEASE-2024", "doi:10.48443/rr7n-7d52")]
    assert r["neon.releaseOnlyMonths"] == [("2019-05/2019-05", "ISO8601 interval")]


def test_month_level(built) -> None:
    _b, by, S = built
    m = attrs(by[f"{S}/raw/{P_IS}/2024-06"])
    assert m["neon.release"] == [("RELEASE-2026", "doi:10.48443/p69b-5e50")]
    assert m["neon.generationDate"] == [("2025-01-29T00:07:30Z", "ISO8601")]
    assert m["neon.sensorCount"] == [("2", "count")] and m["neon.rowCount"] == [("2880", "count")]
    assert m["neon.fileCount"] == [("6", "count")]
    p = attrs(by[f"{S}/raw/{P_IS}/2026-08"])
    assert p["neon.release"] == [("PROVISIONAL", "")]
    o = attrs(by[f"{S}/raw/{P_OS}/2024-05"])
    assert o["neon.plotCount"] == [("60", "count")] and o["neon.taxaCount"] == [("2", "count")]
    assert sorted(o["neon.protocolVersion"]) == [("NEON.DOC.014041vK", ""), ("NEON.DOC.014041vL", "")]
    assert "neon.sensorCount" not in o
    r = attrs(by[f"{S}/raw/{P_RO}/2019-05"])
    assert r["neon.releaseOnly"] == [("true", "")] and r["neon.release"] == [("RELEASE-2024", "doi:10.48443/rr7n-7d52")]


def _obj(by: dict, S: str, product: str, month: str, pred) -> tuple[str, dict]:
    hits = [(p, d) for p, d in by.items() if d["level"] == "object" and p.startswith(f"{S}/raw/{product}/{month}/")
            and pred(p)]
    assert len(hits) == 1, [h[0] for h in hits]
    return hits[0]


def test_object_core_and_is_profile(built) -> None:
    b, by, S = built
    path, d = _obj(by, S, P_IS, "2024-06", lambda p: ".000.010.030.SAAT_30min." in p)
    name = path.rsplit("/", 1)[1]
    a = attrs(d)
    for core in avus.CORE_ATTRS:
        assert core in a, core
    assert a["dc.source"] == [(f"https://data.neonscience.org/api/v0/data/{P_IS}/{SITE}/2024-06/{name}", "URL")]
    assert a["neon.publicationDate"] == [("2024-07-06T16:23:40Z", "ISO8601")]
    assert a["neon.release"] == [("RELEASE-2026", "doi:10.48443/p69b-5e50")]
    assert b.targets[path] == ("object", "is")
    assert a["neon.hor"] == [("000", "")] and a["neon.ver"] == [("010", "")] and a["neon.tmi"] == [("030", "")]
    # the position active in the month wins over a retired one
    assert a["neon.namedLocation"] == [("CFGLOC104458", "")]
    assert a["neon.sensorPoint"] == [("31.9106751 -110.8354251", "WGS84 lat lon")]
    assert a["neon.sensorElevation"] == [("997.98", "meter")] and a["neon.zOffset"] == [("0.25", "meter")]
    assert a["neon.finalQFFailPercent"] == [("0.4861", "percent")]
    assert a["neon.rowCount"] == [("1440", "count")]
    assert a["mesa.ducklake.table"] == [("dp1_00002_001__saat_30min", f"ducklake:sites/{SITE}/ducklake/neon_test.ducklake")]
    _p, prov = _obj(by, S, P_IS, "2026-08", lambda p: ".000.020.030." in p)
    assert attrs(prov)["neon.release"] == [("PROVISIONAL", "")]


def test_os_profile_taxa_only_from_the_file(built) -> None:
    b, by, S = built
    path, d = _obj(by, S, P_OS, "2024-05", lambda p: "brd_countdata" in p)
    a = attrs(d)
    assert b.targets[path][1] == "os"
    assert a["ncbitaxon.amphispiza_bilineata"] == [("Black-throated Sparrow", "NCBITaxon:198939")]
    assert a["neon.taxon"] == [("Unknownus avis", "NEON:BIRD:UNKN")]
    assert a["neon.taxaCount"] == [("2", "count")] and a["neon.plotCount"] == [("5", "count")]
    assert a["neon.protocolVersion"] == [("NEON.DOC.014041vL", "")]
    _p, pp = _obj(by, S, P_OS, "2024-05", lambda p: "brd_perpoint" in p)
    pa_ = attrs(pp)
    assert len(pa_["neon.plotID"]) == 50 and pa_["neon.plotCount"] == [("60", "count")]
    assert not any(k.startswith("ncbitaxon.") for k in pa_) and "neon.taxaCount" not in pa_


def test_hdf5_and_supporting_profiles(built) -> None:
    b, by, S = built
    path, d = _obj(by, S, P_H5, "2024-06", lambda p: p.endswith(".h5"))
    a = attrs(d)
    assert b.targets[path][1] == "hdf5"
    assert a["neon.table"] == [("nsae", "")] and a["neon.towerPoint"] == [("31.910711 -110.83547", "WGS84 lat lon")]
    assert a["neon.rowCount"] == [("1440", "count")] and "neon.temporalCoverage" in a
    _p, eml = _obj(by, S, P_IS, "2024-06", lambda p: ".EML." in p)
    e = attrs(eml)
    assert e["neon.temporalCoverage"] == [("2024-06-01/2024-06-28", "ISO8601 interval")]
    assert e["neon.md5Verified"] == [("false", "")]  # flagged soft EML
    _p, eml2 = _obj(by, S, P_IS, "2026-08", lambda p: ".EML." in p)
    assert "neon.md5Verified" not in attrs(eml2)
    _p, var = _obj(by, S, P_IS, "2024-06", lambda p: ".variables." in p)
    assert attrs(var)["neon.variableCount"] == [("3", "count")]  # quoted newline is one record
    _p, sp = _obj(by, S, P_IS, "2024-06", lambda p: ".sensor_positions." in p)
    assert attrs(sp)["neon.sensorCount"] == [("2", "count")]
    _p, cc = _obj(by, S, P_OS, "2024-05", lambda p: "categoricalCodes" in p)
    c = attrs(cc)
    assert c["neon.table"] == [("categoricalCodes", "")] and c["neon.productCode"] == [(P_OS, "")]
    assert c["dc.source"][0][0].startswith(f"https://data.neonscience.org/api/v0/data/{P_OS}/{SITE}/2024-05/NEON.D99")
    _p, rd = _obj(by, S, P_IS, "2024-06", lambda p: ".readme." in p)
    assert set(attrs(rd)) == set(avus.CORE_ATTRS)
    _ro_path, ro = _obj(by, S, P_RO, "2019-05", lambda p: True)
    assert attrs(ro)["neon.releaseOnly"] == [("true", "")]
    assert attrs(ro)["neon.release"] == [("RELEASE-2024", "doi:10.48443/rr7n-7d52")]


def test_missing_ontology_tables_skip_generators(site_env: Path) -> None:  # noqa: F811
    f = make_facts(Ontology(warnings=["ontology store missing"]))
    b, by = build(f)
    all_attrs = {a for d in by.values() for a, _v, _u in d["avus"]}
    assert not any(a.startswith(("ncbitaxon.", "uo.", "envo.")) for a in all_attrs)
    assert "neon.taxon" not in all_attrs
    assert "neon.siteCode" in all_attrs and b.info["skipped"] == ["ontology store missing"]


def test_filters_select_units(site_env: Path) -> None:  # noqa: F811
    b, by = build(make_facts(), [P_IS], ["2024-06"])
    S = f"/zone/home/u/proj/sites/{SITE}"
    months = {p for p, d in by.items() if d["level"] == "month"}
    assert months == {f"{S}/raw/{P_IS}/2024-06"}
    assert {p for p, d in by.items() if d["level"] == "product"} == {f"{S}/raw/{P_IS}"}
    assert S in by  # site-level units are always rebuilt
    assert b.rebuilds(S) and b.rebuilds(f"{S}/raw") and b.rebuilds(f"{S}/raw/{P_IS}")
    assert b.rebuilds(f"{S}/raw/{P_IS}/2024-06") and not b.rebuilds(f"{S}/raw/{P_IS}/2026-08")
    assert not b.rebuilds(f"{S}/raw/{P_OS}") and not b.rebuilds(f"{S}/raw/{P_OS}/2024-05")


def test_formatting_helpers() -> None:
    assert avus.num(997.476, 3) == "997.476" and avus.num(0.0) == "0" and avus.num(-0.00000001) == "0"
    assert avus.point(31.9, -110.8) == "31.9 -110.8" and avus.point(None, 1) is None
    assert avus.eml_coverage("X.EML.20190501-20190601.20260123T000749Z.xml") == "2019-05-01/2019-06-01"
    assert avus.month_ranges(["2024-01", "2024-02", "2024-04"]) == "2024-01/2024-02,2024-04/2024-04"
    assert avus.iso("2024-06-01T00:00:00Z") == "2024-06-01T00:00:00Z"
    assert avus.lake_product("dp1_00002_001__saat_30min") == "DP1.00002.001" and avus.lake_product("files") is None
    assert avus.ontology_avu("ncbitaxon", "Amphispiza bilineata", "Black-throated Sparrow", "NCBITaxon:198939") == \
        ("ncbitaxon.amphispiza_bilineata", "Black-throated Sparrow", "NCBITaxon:198939")
    assert avus.ontology_avu("uo", "", "x", "UO:1") is None


def _aquatic_facts():
    f = make_facts()
    f.site_info.update(siteName="Test Creek NEON", habitat="Aquatic Wadeable Stream",
                       siteProperties={"HABITAT": "Aquatic Wadeable Stream"},
                       typeEco="lightly grazed North American natural shortgrass steppe")  # NEON's copied value
    o = f.ontology
    o.tables.add("ref_site_envo_feature")
    o.site_envo = {"siteCode": SITE, "envoCurie": "ENVO:03605008", "envoLabel": "freshwater stream biome",
                   "typeEco": None, "method": "curated", "status": "curated"}
    o.site_feature = {"siteCode": SITE, "envoCurie": "ENVO:00000023", "envoLabel": "stream",
                      "habitat": "Aquatic Wadeable Stream", "method": "curated", "status": "curated"}
    return f


def test_aquatic_site_habitat_feature_and_no_type_eco(site_env: Path) -> None:  # noqa: F811
    f = _aquatic_facts()
    write_local_files(f)
    _b, by = build(f)
    site = by[f"/zone/home/u/proj/sites/{SITE}"]
    a = attrs(site)
    assert a["neon.habitat"] == [("Aquatic Wadeable Stream", "")]
    assert "neon.typeEco" not in a  # NEON's aquatic TypeEco is a copy-paste: never used
    assert a["envo.freshwater_stream_biome"] == [("Aquatic Wadeable Stream", "ENVO:03605008")]
    assert a["envo.stream"] == [("Test Creek NEON", "ENVO:00000023")]
    assert not any(k.startswith("envo.area_of") for k in a)  # NLCD AVUs: terrestrial sites only
    assert not any("grassland" in k or "steppe" in v for k, vs in a.items() for v, _u in vs)
    subjects = {v for k, v, _u in site["avus"] if k.startswith("datacite.subject.") and k.endswith(".value")}
    assert "Aquatic Wadeable Stream" in subjects and "stream (ENVO:00000023)" in subjects
    assert not any("steppe" in s for s in subjects)


def test_site_feature_only_when_curated_and_aquatic(site_env: Path) -> None:  # noqa: F811
    f = _aquatic_facts()
    f.ontology.site_feature = dict(f.ontology.site_feature, method="heuristic", status="proposed")
    f.ontology.site_envo = dict(f.ontology.site_envo, method="heuristic", status="proposed")
    write_local_files(f)
    _b, by = build(f)
    a = attrs(by[f"/zone/home/u/proj/sites/{SITE}"])
    assert not any(k.startswith("envo.") for k in a)  # proposals are never AVUs
    t = make_facts()  # terrestrial: a stray feature row is ignored; the biome value stays TypeEco
    t.ontology.site_feature = _aquatic_facts().ontology.site_feature
    t.site_info["habitat"] = "Terrestrial"
    write_local_files(t)
    _b, by = build(t)
    a = attrs(by[f"/zone/home/u/proj/sites/{SITE}"])
    assert "envo.stream" not in a and a["neon.habitat"] == [("Terrestrial", "")]
    assert a["envo.xeric_shrubland_biome"] == [("Desert shrubland", "ENVO:01000218")]
    assert a["neon.typeEco"] == [("Desert shrubland", "")] and "envo.area_of_scrub" in a


def test_neon_md5_mismatch_and_lab_objects(site_env: Path) -> None:  # noqa: F811
    f = make_facts()
    var = next(r for r in f.files if r["kind"] == "variables" and r["month"] == "2024-06")
    var.update(md5Verified=False, transferVerified=True, neonMd5="d9cab7b4d9cab7b4d9cab7b4d9cab7b4",
               storageMd5=var["dlMd5"])
    readme = next(r for r in f.files if r["kind"] == "readme" and r["month"] == "2026-08")
    readme.update(md5Verified=False)  # an old lake without the contract (b) columns: the journal fills in
    f.downloads[(readme["productCode"], readme["month"], readme["name"])].update(
        transfer_verified=1, neon_md5="0123456789abcdef0123456789abcdef")
    lab = dict(next(r for r in f.files if r["productCode"] == P_OS), kind="other", tableName=None, hor=None,
               name="NEON.Univ_of_Wyoming.swc_externalLabSummaryData.20250102T000000Z.csv")
    lab["localRelPath"] = lab["localRelPath"].rsplit("/", 1)[0] + "/" + lab["name"]
    f.files.append(lab)
    write_local_files(f)
    _b, by = build(f)
    S = f"/zone/home/u/proj/sites/{SITE}"
    a = attrs(by[f"{S}/raw/{P_IS}/2024-06/{var['name']}"])
    assert a["neon.md5Verified"] == [("false", "")]
    assert a["neon.neonMd5Mismatch"] == [("d9cab7b4d9cab7b4d9cab7b4d9cab7b4", "")]
    a = attrs(by[f"{S}/raw/{P_IS}/2026-08/{readme['name']}"])
    assert a["neon.neonMd5Mismatch"] == [("0123456789abcdef0123456789abcdef", "")]
    _p, eml = _obj(by, S, P_IS, "2024-06", lambda p: ".EML." in p)
    assert "neon.neonMd5Mismatch" not in attrs(eml)  # md5 false without a verified transfer: flag only
    a = attrs(by[f"{S}/raw/{P_OS}/2024-05/{lab['name']}"])
    assert a["neon.fileKind"] == [("lab", "")] and a["neon.labName"] == [("Univ_of_Wyoming", "")]
    assert a["neon.table"] == [("swc_externalLabSummaryData", "")]
    assert avus.md5_state({"md5Verified": None}, {"md5_verified": 0, "transfer_verified": 1, "neon_md5": "n"}) == \
        (False, True, "n")
