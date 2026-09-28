"""site/terms.py units (UO), NLCD -> ENVO and the site-biome proposal."""

from __future__ import annotations

import csv

import duckdb
import pytest

from neon_ducklake.site import ontology_store as store
from neon_ducklake.site import site_lake, terms

from .test_site_common import SITE, site_env  # noqa: F401 - fixture
from .test_terms_fakes import FakeOLS, ols_env  # noqa: F401 - fixture

BIOME = "ENVO:00000428"


@pytest.mark.parametrize(("unit", "words", "variants"), [
    ("kilometersPerHour", ["kilometer", "per", "hour"], ["kilometer per hour"]),
    ("metersPerSecond", ["meter", "per", "second"], ["meter per second"]),
    ("microsiemensPerCentimeter", ["microsiemen", "per", "centimeter"], ["microsiemen per centimeter"]),
    ("celsiusSquared", ["celsius", "squared"], ["celsius squared", "square celsius"]),
    ("metersSquared", ["meter", "squared"], ["meter squared", "square meter"]),
    ("inches", ["inch"], ["inch"]),
    ("percent", ["percent"], ["percent"]),
    ("pH", ["p", "h"], ["p h"]),  # seeded; the split alone cannot recover it
])
def test_unit_words_and_variants(unit, words, variants):
    assert terms.unit_words(unit) == words
    assert terms.unit_label_variants(unit) == variants


def _uo(ols: FakeOLS) -> None:
    ols.add("UO:0000027", "degree Celsius")
    ols.add("UO:1000027", "degree Celsius")  # the 'based unit' twin with the same label must lose
    ols.add("UO:0000189", "count unit")
    ols.add("UO:0010008", "kilometer per hour")
    ols.add("UO:0000187", "percent")
    ols.add("UO:0000008", "meter")


def test_resolve_units(ols_env: FakeOLS):  # noqa: F811
    _uo(ols_env)
    res = terms.resolve_units(["celsius", "number", "kilometersPerHour", "percent", "celsiusSquared", "meter",
                               "furlongsPerFortnight"])
    assert res["celsius"] == {"uoCurie": "UO:0000027", "uoLabel": "degree Celsius", "method": "seed", "status": "ok"}
    assert res["number"]["uoCurie"] == "UO:0000189" and res["number"]["method"] == "seed"
    assert res["kilometersPerHour"] == {"uoCurie": "UO:0010008", "uoLabel": "kilometer per hour", "method": "exact",
                                        "status": "ok"}
    assert res["celsiusSquared"] == {"uoCurie": None, "uoLabel": None, "method": "not_applicable",
                                     "status": "not_applicable"}
    assert res["furlongsPerFortnight"] == {"uoCurie": None, "uoLabel": None, "method": "unmapped",
                                           "status": "unmapped"}
    assert all(c[1] == "uo" for c in ols_env.search_calls)
    assert not any(c["q"].startswith("celsius") for c in ols_env.exact_calls)  # variance: never looked up


@pytest.mark.parametrize(("unit", "variance"), [
    ("celsiusSquared", True), ("degreesSquared", True), ("percentSquared", True), ("kilopascalsSquared", True),
    ("metersPerSecondSquared", True),  # NEON: windSpeedVariance only
    ("wattsPerSquareMeterSquared", True), ("partsPerMillionSquared", True),
    ("micromolesPerSquareMeterPerSecondSquared", True), ("cubicCentimetersPerCubicCentimeterSquared", True),
    ("metersSquared", False), ("centimetersSquared", False), ("wattsPerMeterSquared", False),
    ("voltMetersSquaredPerWatt", False), ("celsius", False), ("Squared", False),
])
def test_is_variance_unit(unit, variance):
    assert terms.is_variance_unit(unit) is variance


def test_resolve_units_synonyms_spelling_and_seed_alternatives(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("UO:0000064", "micromolar", synonyms=("uM",))
    ols_env.add("UO:0010003", "micromole per litre")
    ols_env.add("UO:0000077", "meter per second per second", synonyms=("metre per second per second",))
    ols_env.add("UO:0000168", "parts per thousand", synonyms=("ppth",))
    ols_env.add("UO:0000063", "millimolar", synonyms=("mM",))
    ols_env.add("UO:0000075", "normal", synonyms=("N",))
    ols_env.add("UO:0000500", "gram per kilogram", synonyms=("gram per kilogramme", "g/kg"))
    ols_env.add("UO:0000501", "fluffy unit", synonyms=("widget per litre",))
    res = terms.resolve_units(["micromolesPerLiter", "millimolesPerLiter", "permill", "equivalentsPerLiter",
                               "micromolePerLiter", "widgetsPerLiter", "cubicMicrometer", "metersPerSecondSquared"])
    got = {u: (r["uoCurie"], r["method"], r["status"]) for u, r in res.items()}
    assert got == {
        "micromolesPerLiter": ("UO:0000064", "seed", "ok"),  # seed 'micromolar'
        "millimolesPerLiter": ("UO:0000063", "seed", "ok"),
        "permill": ("UO:0000168", "seed", "ok"),  # the first of the seed alternatives
        "equivalentsPerLiter": ("UO:0000075", "seed", "ok"),
        "micromolePerLiter": ("UO:0010003", "exact", "ok"),  # label 'micromole per litre': US/UK spelling
        "widgetsPerLiter": ("UO:0000501", "synonym", "ok"),  # only a (UK-spelled) synonym matches
        "cubicMicrometer": (None, "unmapped", "unmapped"),  # UO has no such term: stays unmapped
        "metersPerSecondSquared": (None, "not_applicable", "not_applicable"),  # variance, not acceleration
    }


def test_unit_spelling_helpers():
    assert terms.spellings("micromole per liter") == ["micromole per liter", "micromole per litre"]
    assert terms.spellings("cubic centimeter") == ["cubic centimeter", "cubic centimetre"]
    assert terms.unit_fold(" Millilitre  per Metre ") == "milliliter per meter"
    assert terms.seed_labels("permill") == ["parts per thousand", "permil"]
    assert terms.seed_labels("celsius") == ["degree Celsius"] and terms.seed_labels("nope") == []


def test_resolve_units_seed_label_missing_falls_to_exact_then_unmapped(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("UO:0000010", "second")
    res = terms.resolve_units(["micromolesPerSquareMeterPerSecond", "second"])
    assert res["micromolesPerSquareMeterPerSecond"]["status"] == "unmapped"
    assert res["second"]["method"] == "seed"


def test_resolve_units_pending_on_outage(ols_env: FakeOLS):  # noqa: F811
    _uo(ols_env)
    ols_env.fail_queries.add("furlong per fortnight")
    res = terms.resolve_units(["furlongsPerFortnight", "celsius", "celsiusSquared"])
    assert res["furlongsPerFortnight"]["status"] == "pending"
    assert res["celsius"]["status"] == "ok"
    assert res["celsiusSquared"]["status"] == "not_applicable"
    ols_env.fail_queries.clear()
    ols_env.fail_queries.add("furlong per fortnite")  # only the exact (synonym) search of another unit fails
    res = terms.resolve_units(["furlongsPerFortnite", "celsius"])
    assert res["furlongsPerFortnite"]["status"] == "pending" and res["celsius"]["status"] == "ok"


def test_run_units_keeps_curated_rows(ols_env: FakeOLS):  # noqa: F811
    _uo(ols_env)
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lake")
    con.execute("CREATE TABLE lake.variables (productCode VARCHAR, tableName VARCHAR, fieldName VARCHAR, "
                "units VARCHAR)")
    con.executemany("INSERT INTO lake.variables VALUES (?, ?, ?, ?)", [
        ("P", "t", "a", "celsius"), ("P", "t", "b", "celsiusSquared"), ("P", "t", "c", "NA"), ("P", "t", "d", None),
        ("P", "t", "e", " "), ("P", "t2", "f", "celsiusSquared"), ("P", "t", "g", "furlongsPerFortnight")])
    with store.store() as sc:
        sc.execute("INSERT INTO ref_unit_map VALUES ('furlongsPerFortnight', 'UO:0000999', 'furlong per fortnight', "
                   "'curated', 'ok', now())")
    con.execute("INSERT INTO lake.variables VALUES ('P', 't', 'h', 'hogsheadsPerHour'), "
                "('P', 't', 'specCondVariance', 'microsiemensPerCentimeterSquared'), "
                "('P', 't', 'flux', 'wattsPerMeterSquared')")
    ols_env.add("UO:0000155", "watt per square meter")
    out = terms.run_units(con, SITE)
    assert out["distinct"] == 6 and out["mapped"] == 3 and out["unmapped"] == 1 and out["notApplicable"] == 2
    assert out["counts"]["curated"] == 1 and out["counts"]["seed"] == 2 and out["counts"]["not_applicable"] == 2
    with store.store(read_only=True) as sc:
        rows = dict(sc.execute("SELECT neonUnit, method FROM ref_unit_map").fetchall())
    assert rows == {"celsius": "seed", "celsiusSquared": "not_applicable", "furlongsPerFortnight": "curated",
                    "hogsheadsPerHour": "unmapped",
                    "microsiemensPerCentimeterSquared": "not_applicable",  # its only field is a variance
                    "wattsPerMeterSquared": "seed"}  # per-area, not a variance
    report = site_lake.site_paths(SITE).reports_dir / "unmapped_units.csv"
    got = list(csv.DictReader(report.open()))
    assert [(r["neonUnit"], r["nFields"], r["fields"]) for r in got] == [("hogsheadsPerHour", "1", "P/t.h")]


def test_resolve_nlcd_verifies_and_replaces(ols_env: FakeOLS):  # noqa: F811
    for curie, label in terms.NLCD_ENVO.values():
        ols_env.add(curie, label)
    ols_env.terms["ENVO:01000869"]["label"] = "something else"  # label drift -> search for the expected label
    ols_env.add("ENVO:09999999", "area of scrub")
    ols_env.terms["ENVO:01000746"]["obsolete"] = True  # obsolete, no replacement -> not stored
    ols_env.terms["ENVO:01000746"]["label"] = "obsolete area of perennial ice or snow"
    rows, issues = terms.resolve_nlcd()
    assert rows["shrubScrub"] == ("ENVO:09999999", "area of scrub")
    assert rows["deciduousForest"] == ("ENVO:01000816", "area of deciduous forest")
    assert "perennialIceSnow" not in rows and len(rows) == 19
    by = {i["nlcdClass"]: i for i in issues}
    assert by["shrubScrub"]["replacement"] == "ENVO:09999999"
    assert "obsolete" in by["perennialIceSnow"]["issue"]
    out = terms.run_nlcd()
    assert out["verified"] == 19
    with store.store(read_only=True) as sc:
        assert sc.execute("SELECT count(*) FROM ref_nlcd_envo").fetchone()[0] == 19


@pytest.mark.parametrize(("eco", "nlcd", "state", "habitat", "name", "rule", "first"), [
    ("Desert shrubland dominated by Creosote bush", "shrubScrub", "AZ", "Terrestrial", "Santa Rita",
     "desert+shrub", "xeric shrubland biome"),
    ("Desert grassland", "grasslandHerbaceous", "NM", "Terrestrial", "Jornada", "desert", "desert biome"),
    ("Acidic tussock tundra", "dwarfScrub", "AK", "Terrestrial", "Toolik", "tundra", "tundra biome"),
    ("Spruce forest on permafrost", "evergreenForest", "AK", "Terrestrial", "x", "boreal forest",
     "boreal forest biome"),
    ("Ponderosa Pine forest", "evergreenForest", "CO", "Terrestrial", "x", "coniferous forest",
     "temperate coniferous forest biome"),
    ("Mixed longleaf pine/hardwood forest", "mixedForest", "AL", "Terrestrial", "x", "mixed forest",
     "temperate mixed forest biome"),
    ("Northern hardwood forest dominated by sugar maple", "deciduousForest", "MA", "Terrestrial", "x",
     "broadleaf forest", "temperate broadleaf forest biome"),
    ("Oak-dominated woodland savanna", "grasslandHerbaceous", "CA", "Terrestrial", "x", "savanna",
     "temperate savanna biome"),
    ("Tallgrass prairie", "grasslandHerbaceous", "KS", "Terrestrial", "x", "grassland", "temperate grassland biome"),
    ("Subtropical dry seasonal forest, dense closed canopy", "evergreenForest", "PR", "Terrestrial", "x",
     "tropical dry forest", "tropical dry broadleaf forest biome"),
    ("Argonomic field", "cultivatedCrops", "IA", "Terrestrial", "x", "cropland", "cropland biome"),
    (None, "deciduousForest", "TN", "Terrestrial", "x", "nlcd:deciduousForest", "temperate broadleaf forest biome"),
    (None, None, "TN", "Aquatic", "Lake Suggs NEON", "aquatic:lake", "freshwater lake biome"),
    (None, None, "VA", "Aquatic", "Posey Creek NEON", "aquatic:stream", "freshwater stream biome"),
    # HABITAT decides for aquatic sites; the copied shortgrass-steppe TypeEco is never used
    ("lightly grazed North American natural shortgrass steppe", None, "CO", "Aquatic Wadeable Stream",
     "Arikaree River NEON", "aquatic:wadeable stream", "freshwater stream biome"),
    ("lightly grazed North American natural shortgrass steppe", None, "GA", "Aquatic Non-wadeable Stream",
     "Flint River NEON", "aquatic:non-wadeable stream", "freshwater river biome"),
    ("lightly grazed North American natural shortgrass steppe", None, "FL", "Aquatic Lake", "Barco Lake NEON",
     "aquatic:lake", "freshwater lake biome"),
])
def test_biome_candidates(eco, nlcd, state, habitat, name, rule, first):
    got_rule, cands = terms.biome_candidates(eco, nlcd, state, habitat, name)
    assert got_rule == rule and cands[0] == first


@pytest.mark.parametrize(("eco", "nlcd", "state", "habitat", "name"), [
    (None, None, "AZ", "Terrestrial", "x"),  # nothing to go on
    ("Tallgrass prairie", "grasslandHerbaceous", "HI", "Terrestrial", "x"),  # no tropical grassland guess
    (None, "evergreenForest", "PR", "Terrestrial", "x"),  # NLCD-only temperate guesses are not made in the tropics
    (None, None, "AK", "Aquatic", "Mystery Site"),
])
def test_biome_candidates_no_guess(eco, nlcd, state, habitat, name):
    assert terms.biome_candidates(eco, nlcd, state, habitat, name)[1] == []


@pytest.mark.parametrize(("habitat", "name", "rule", "labels"), [
    ("Aquatic Wadeable Stream", "Arikaree River NEON", "aquatic:wadeable stream", ["stream"]),
    ("Aquatic Non-wadeable Stream", "Flint River NEON", "aquatic:non-wadeable stream", ["river", "stream"]),
    ("Aquatic Lake", "Toolik Lake NEON", "aquatic:lake", ["lake"]),
    ("Aquatic", "Posey Creek NEON", "aquatic:stream", ["stream"]),
    ("Terrestrial", "Konza Prairie", "terrestrial", []),
    (None, "x", "terrestrial", []),
])
def test_feature_candidates(habitat, name, rule, labels):
    assert terms.feature_candidates(habitat, name) == (rule, labels)


def _ctx(eco: str | None, nlcd: str | None) -> dict:
    return {"typeEco": eco, "stateCode": "AZ", "habitat": "Terrestrial", "siteName": "Santa Rita",
            "nlcdYear": "2006", "nlcd": [{"nlcdClass": nlcd, "areaKm2": 10.0, "share": 0.9}] if nlcd else []}


def test_propose_biome_within_biome_root(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("ENVO:01000218", "xeric shrubland biome", ancestors=(BIOME,))
    ols_env.add("ENVO:01000179", "desert biome", ancestors=(BIOME,))
    p = terms.propose_biome(_ctx("Desert shrubland dominated by Creosote bush", "shrubScrub"))
    assert (p["envoCurie"], p["envoLabel"], p["method"], p["status"]) == \
        ("ENVO:01000218", "xeric shrubland biome", "heuristic", "proposed")
    assert all(c[3] == store.iri_for(BIOME) for c in ols_env.search_calls)
    ols_env.terms["ENVO:01000218"]["ancestors"] = set()  # not a biome descendant -> next candidate
    store._TERM_MISSES.clear()
    with store.store() as sc:
        sc.execute("DELETE FROM ols_search_cache")
    p = terms.propose_biome(_ctx("Desert shrubland", "shrubScrub"))
    assert p["envoCurie"] == "ENVO:01000179"
    p = terms.propose_biome(_ctx(None, None))
    assert (p["envoCurie"], p["method"], p["status"]) == (None, "pending", "pending")


def test_run_biome_keeps_curated(ols_env: FakeOLS, monkeypatch):  # noqa: F811
    ols_env.add("ENVO:01000218", "xeric shrubland biome", ancestors=(BIOME,))
    monkeypatch.setattr(terms, "site_context", lambda con, site, has: _ctx("Desert shrubland", "shrubScrub"))
    out = terms.run_biome(None, SITE, True)
    assert out["status"] == "proposed"
    with store.store() as sc:
        sc.execute("UPDATE ref_site_envo SET envoCurie = 'ENVO:01000179', envoLabel = 'desert biome', "
                   "method = 'curated', status = 'accepted'")
    out = terms.run_biome(None, SITE, True)
    assert out["kept"] and out["envoCurie"] == "ENVO:01000179"
    with store.store(read_only=True) as sc:
        assert sc.execute("SELECT envoCurie, method FROM ref_site_envo").fetchall() == [("ENVO:01000179", "curated")]


def test_site_context_from_catalog(site_env):  # noqa: F811
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS neon")
    con.execute("CREATE TABLE neon.sites (siteCode VARCHAR, siteName VARCHAR, stateCode VARCHAR, "
                "siteProperties MAP(VARCHAR, VARCHAR))")
    con.execute("INSERT INTO neon.sites VALUES ('TEST', 'Test Site NEON', 'AZ', "
                "MAP {'TypeEco': 'Desert shrubland', 'HABITAT': 'Terrestrial'})")
    con.execute("CREATE TABLE neon.site_locations (siteCode VARCHAR, locationType VARCHAR, locationProperties JSON)")
    con.execute("CREATE TABLE neon.neon_tos_nlcdcoverbysite (siteID VARCHAR, nlcdYear VARCHAR, nlcdClass VARCHAR, "
                "area_km2 DOUBLE)")
    con.executemany("INSERT INTO neon.neon_tos_nlcdcoverbysite VALUES (?, ?, ?, ?)", [
        ("TEST", "2006", "shrubScrub", 9.0), ("TEST", "2006", "barrenLand", 1.0), ("TEST", "2001", "openWater", 50.0)])
    ctx = terms.site_context(con, "TEST", True)
    assert (ctx["typeEco"], ctx["stateCode"], ctx["habitat"], ctx["nlcdYear"]) == \
        ("Desert shrubland", "AZ", "Terrestrial", "2006")
    assert [(x["nlcdClass"], round(x["share"], 2)) for x in ctx["nlcd"]] == [("shrubScrub", 0.9), ("barrenLand", 0.1)]


WATER = "ENVO:00000063"


def _aquatic_terms(ols: FakeOLS) -> None:
    ols.add("ENVO:03605008", "freshwater stream biome", ancestors=(BIOME, WATER))  # biomes are water bodies too
    ols.add("ENVO:01000253", "freshwater river biome", ancestors=(BIOME, WATER))
    ols.add("ENVO:01000193", "temperate grassland biome", ancestors=(BIOME,))
    ols.add("ENVO:00000023", "stream", ancestors=(WATER,))
    ols.add("ENVO:00000022", "river", ancestors=(WATER,))


def test_run_biome_aquatic_feature_and_proposals(ols_env: FakeOLS, monkeypatch):  # noqa: F811
    _aquatic_terms(ols_env)
    ctx = {"typeEco": None, "stateCode": "CO", "habitat": "Aquatic Wadeable Stream", "siteName": "Arikaree River NEON",
           "nlcdYear": None, "nlcd": []}
    monkeypatch.setattr(terms, "site_context", lambda con, site, has: dict(ctx))
    out = terms.run_biome(None, SITE, True)
    assert (out["envoCurie"], out["rule"], out["status"]) == ("ENVO:03605008", "aquatic:wadeable stream", "proposed")
    assert (out["feature"]["envoCurie"], out["feature"]["status"]) == ("ENVO:00000023", "proposed")
    assert any(c[3] == store.iri_for(WATER) for c in ols_env.search_calls)  # features: water-body descendants
    with store.store(read_only=True) as sc:
        assert sc.execute("SELECT envoCurie, typeEco, method, status FROM ref_site_envo").fetchall() == [
            ("ENVO:03605008", None, "heuristic", "proposed")]
        assert sc.execute("SELECT envoCurie, habitat, method, status FROM ref_site_envo_feature").fetchall() == [
            ("ENVO:00000023", "Aquatic Wadeable Stream", "heuristic", "proposed")]
        props = sc.execute("SELECT role, source, envoCurie, rule FROM site_envo_proposals ORDER BY role").fetchall()
    assert props == [("biome", "heuristic", "ENVO:03605008", "aquatic:wadeable stream"),
                     ("feature", "heuristic", "ENVO:00000023", "aquatic:wadeable stream")]
    # a curated feature is kept; the heuristic is still recorded
    with store.store() as sc:
        sc.execute("UPDATE ref_site_envo_feature SET envoCurie = 'ENVO:00000022', envoLabel = 'river', "
                   "method = 'curated', status = 'curated'")
    out = terms.run_biome(None, SITE, True)
    assert out["feature"]["kept"] and out["feature"]["envoCurie"] == "ENVO:00000022"
    assert out["feature"]["heuristic"]["envoCurie"] == "ENVO:00000023"
    text = terms.render(SITE, {"taxa": {"distinct": 0, "counts": dict.fromkeys(terms.TAXON_COUNT_KEYS, 0),
                                        "resolvedNow": 0, "keptOk": 0, "typeChoices": []},
                               "units": {"distinct": 0, "mapped": 0, "unmapped": 0,
                                         "counts": dict.fromkeys(terms.UNIT_COUNT_KEYS, 0), "map": {}},
                               "nlcd": {"verified": 0, "issues": []}, "biome": out})
    assert "HABITAT: 'Aquatic Wadeable Stream'" in text and "site feature: ENVO:00000022 river" in text
    # a terrestrial site has no feature row
    monkeypatch.setattr(terms, "site_context", lambda con, site, has: {**_ctx("Tallgrass prairie", None)})
    terms.run_biome(None, "KONZ", True)
    with store.store(read_only=True) as sc:
        assert sc.execute("SELECT count(*) FROM ref_site_envo_feature WHERE siteCode = 'KONZ'").fetchone()[0] == 0


def test_site_context_aquatic_ignores_type_eco(site_env):  # noqa: F811
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS neon")
    con.execute("CREATE TABLE neon.sites (siteCode VARCHAR, siteName VARCHAR, stateCode VARCHAR, "
                "siteLatitude DOUBLE, siteProperties MAP(VARCHAR, VARCHAR))")
    con.execute("INSERT INTO neon.sites VALUES ('ARIK', 'Arikaree River NEON', 'CO', 39.75, "
                "MAP {'TypeEco': 'lightly grazed North American natural shortgrass steppe', "
                "'HABITAT': 'Aquatic Wadeable Stream'})")
    con.execute("CREATE TABLE neon.neon_tos_nlcdcoverbysite (siteID VARCHAR, nlcdYear VARCHAR, nlcdClass VARCHAR, "
                "area_km2 DOUBLE)")
    con.execute("INSERT INTO neon.neon_tos_nlcdcoverbysite VALUES ('ARIK', '2019', 'grasslandHerbaceous', 9.0)")
    ctx = terms.site_context(con, "ARIK", True)
    assert ctx["habitat"] == "Aquatic Wadeable Stream" and ctx["typeEco"] is None and ctx["nlcd"] == []
    assert ctx["typeEcoIgnored"].startswith("lightly grazed") and ctx["siteLatitude"] == 39.75
    assert terms.propose_biome.__name__  # the proposal never sees the TypeEco
    rule, cands = terms.biome_candidates(ctx["typeEco"], None, ctx["stateCode"], ctx["habitat"], ctx["siteName"])
    assert "grassland" not in " ".join(cands) and rule == "aquatic:wadeable stream"
