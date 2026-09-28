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
    res = terms.resolve_units(["celsius", "number", "kilometersPerHour", "percent", "celsiusSquared", "meter"])
    assert res["celsius"] == {"uoCurie": "UO:0000027", "uoLabel": "degree Celsius", "method": "seed", "status": "ok"}
    assert res["number"]["uoCurie"] == "UO:0000189" and res["number"]["method"] == "seed"
    assert res["kilometersPerHour"] == {"uoCurie": "UO:0010008", "uoLabel": "kilometer per hour", "method": "exact",
                                        "status": "ok"}
    assert res["celsiusSquared"] == {"uoCurie": None, "uoLabel": None, "method": "unmapped", "status": "unmapped"}
    assert all(c[1] == "uo" for c in ols_env.search_calls)


def test_resolve_units_seed_label_missing_falls_to_exact_then_unmapped(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("UO:0000010", "second")
    res = terms.resolve_units(["micromolesPerSquareMeterPerSecond", "second"])
    assert res["micromolesPerSquareMeterPerSecond"]["status"] == "unmapped"
    assert res["second"]["method"] == "seed"


def test_resolve_units_pending_on_outage(ols_env: FakeOLS):  # noqa: F811
    _uo(ols_env)
    ols_env.fail_queries.add("square celsius")
    res = terms.resolve_units(["celsiusSquared", "celsius"])
    assert res["celsiusSquared"]["status"] == "pending"
    assert res["celsius"]["status"] == "ok"


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
    out = terms.run_units(con, SITE)
    assert out["distinct"] == 3 and out["mapped"] == 2 and out["unmapped"] == 1
    assert out["counts"]["curated"] == 1 and out["counts"]["seed"] == 1
    with store.store(read_only=True) as sc:
        rows = dict(sc.execute("SELECT neonUnit, method FROM ref_unit_map").fetchall())
    assert rows == {"celsius": "seed", "celsiusSquared": "unmapped", "furlongsPerFortnight": "curated"}
    report = site_lake.site_paths(SITE).reports_dir / "unmapped_units.csv"
    got = list(csv.DictReader(report.open()))
    assert [(r["neonUnit"], r["nFields"], r["fields"]) for r in got] == [
        ("celsiusSquared", "2", "P/t.b; P/t2.f")]


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
