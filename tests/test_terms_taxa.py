"""site/terms.py taxa: name normalisation, taxonTypeCode choice, NCBITaxon resolution levels, run_taxa."""

from __future__ import annotations

import csv

import duckdb
import pytest

from neon_ducklake.site import ontology_store as store
from neon_ducklake.site import site_lake, terms

from .test_site_common import SITE, site_env  # noqa: F401 - fixture
from .test_terms_fakes import FakeOLS, ols_env  # noqa: F401 - fixture

TAXONOMY_COLS = ("taxonTypeCode", "taxonID", "acceptedTaxonID", "scientificName", "scientificNameAuthorship",
                 "taxonRank", "vernacularName", "kingdom", "family", "genus")


def fake_lakes(file_taxa: list[tuple], taxonomy: list[tuple]) -> duckdb.DuckDBPyConnection:
    """In-memory stand-ins for the site lake (alias lake) and the catalog lake (alias neon)."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lake")
    con.execute("ATTACH ':memory:' AS neon")
    cols = ", ".join(f'"{c}" {t}' for c, t in site_lake.STACK_SCHEMAS["file_taxa"].items())
    con.execute(f"CREATE TABLE lake.file_taxa ({cols})")
    con.executemany("INSERT INTO lake.file_taxa VALUES (?, ?, ?, ?, ?, ?, ?, ?)", file_taxa)
    con.execute("CREATE TABLE neon.taxonomy (" + ", ".join(f'"{c}" VARCHAR' for c in TAXONOMY_COLS) + ")")
    con.executemany(f"INSERT INTO neon.taxonomy VALUES ({', '.join('?' * len(TAXONOMY_COLS))})", taxonomy)
    return con


@pytest.mark.parametrize(("name", "auth", "want"), [
    ("Larrea tridentata (DC.) Coville", "(DC.) Coville", "Larrea tridentata"),
    ("Larrea tridentata (DC.) Coville", None, "Larrea tridentata"),
    ("Opuntia engelmannii Salm-Dyck ex Engelm. var. linguiformis (Griffiths) B.D. Parfitt & Pinkava", None,
     "Opuntia engelmannii var. linguiformis"),
    ("Toxostoma cf. curvirostre", None, "Toxostoma curvirostre"),
    ("Empidonax aff. traillii", None, "Empidonax traillii"),
    ("Carex sp.", None, "Carex"),
    ("Carex spp.", None, "Carex"),
    ("Carex sp. 1", None, "Carex"),
    ("Bouteloua curtipendula ssp. caespitosa", None, "Bouteloua curtipendula subsp. caespitosa"),
    ("Setophaga coronata auduboni", None, "Setophaga coronata auduboni"),
    ("Tyrannidae", None, "Tyrannidae"),
    ("  Amphispiza   bilineata ", None, "Amphispiza bilineata"),
    ("", None, None),
    (None, None, None),
])
def test_query_name(name, auth, want):
    assert terms.query_name(name, auth) == want


def test_taxon_levels_skip_duplicates():
    t = terms.Taxon("BIRD", "X", "Carex sp.", "Carex", family="Cyperaceae", genus="Carex")
    assert [t.name_for(m) for m in terms.TAXON_LEVELS] == ["Carex", None, "Cyperaceae"]
    t = terms.Taxon("BIRD", "Y", "Tyrannidae", "Tyrannidae", family="Tyrannidae")
    assert [t.name_for(m) for m in terms.TAXON_LEVELS] == ["Tyrannidae", None, None]
    t = terms.Taxon("BIRD", "Z", "Picoides scalaris", "Picoides scalaris")  # genus from the name
    assert t.name_for("genus_fallback") == "Picoides" and t.name_for("family_fallback") is None
    t = terms.Taxon("BIRD", "W", "Aimophila/Peucaea sp.", "Aimophila/Peucaea", family="Passerellidae")
    assert t.name_for("genus_fallback") is None and t.name_for("family_fallback") == "Passerellidae"


def _ft(table: str, tid: str, name: str, n: int, product: str = "DP1.10003.001") -> tuple:
    return (f"NEON.{table}.2024-05.csv", product, table, tid, name, "species", None, n)


def test_choose_type_codes_best_rate_and_ties():
    con = fake_lakes(
        [_ft("brd_countdata", "AMRO", "Turdus migratorius", 5), _ft("brd_countdata", "CACW", "Campylorhynchus x", 3),
         _ft("tbl_b", "ACRU", "Acer rubrum", 2), _ft("tbl_c", "NONE1", "Nothing", 1)],
        [("BIRD", "AMRO", "AMRO", "Turdus migratorius", None, "species", None, "Animalia", "Turdidae", "Turdus"),
         ("BIRD", "CACW", "CACW", "Campylorhynchus x", None, "species", None, "Animalia", "Troglodytidae", None),
         ("PLANT", "AMRO", "AMRO", "Amaranthus robustus", None, "species", None, "Plantae", "Amaranthaceae", None),
         ("PLANT", "ACRU", "ACRU", "Acer rubrum", "L.", "species", None, "Plantae", "Sapindaceae", "Acer"),
         ("TICK", "ACRU", "ACRU", "Acer rubrum", None, "species", None, "Plantae", None, None)])
    raw = terms.site_taxa(con)
    ch = terms.choose_type_codes(con, raw)
    a = ch[("DP1.10003.001", "brd_countdata")]
    assert (a.taxonTypeCode, a.nTaxa, a.nMatched, a.matchRate, a.tiedWith) == ("BIRD", 2, 2, 1.0, [])
    b = ch[("DP1.10003.001", "tbl_b")]  # PLANT and TICK both match 1/1 with the same name: alphabetical, reported
    assert (b.taxonTypeCode, b.tiedWith) == ("PLANT", ["TICK"])
    c = ch[("DP1.10003.001", "tbl_c")]
    assert (c.taxonTypeCode, c.matchRate) == (terms.UNKNOWN_TYPE, 0.0)
    assert terms.choose_type_codes(con, raw, has_catalog=False)[("DP1.10003.001", "brd_countdata")].taxonTypeCode \
        == terms.UNKNOWN_TYPE


def test_build_taxa_follows_accepted_and_merges_uses():
    con = fake_lakes(
        [_ft("t1", "OLD", "Oldus nameus", 4), _ft("t2", "OLD", "Oldus nameus", 6),
         _ft("t1", "PLSP", "Carex sp.", 1)],
        [("PLANT", "OLD", "NEW", "Oldus nameus", "Auth.", "species", None, "Plantae", "Oldaceae", "Oldus"),
         ("PLANT", "NEW", "NEW", "Newus nameus (L.) Smith", "(L.) Smith", "species", None, "Plantae", "Newaceae",
          "Newus"),
         ("PLANT", "PLSP", "PLSP", "Carex sp.", None, "genus", None, "Plantae", "Cyperaceae", "Carex")])
    raw = terms.site_taxa(con)
    choices = terms.choose_type_codes(con, raw)
    taxa = {t.taxonID: t for t in terms.build_taxa(con, raw, choices)}
    old = taxa["OLD"]
    assert (old.typeCode, old.neonName, old.queryName, old.family, old.genus) == \
        ("PLANT", "Newus nameus (L.) Smith", "Newus nameus", "Newaceae", "Newus")
    assert old.nRecords == 10 and sorted(old.uses) == ["DP1.10003.001/t1", "DP1.10003.001/t2"]
    assert taxa["PLSP"].queryName == "Carex"


def _taxon(tid: str, query: str, family: str | None = None, genus: str | None = None,
           kingdom: str | None = "Animalia") -> terms.Taxon:
    return terms.Taxon("BIRD", tid, query, query, family=family, genus=genus, kingdom=kingdom)


def test_resolve_levels(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("NCBITaxon:198939", "Amphispiza bilineata", ancestors=("NCBITaxon:33208",))
    ols_env.add("NCBITaxon:55418", "Toxostoma")
    ols_env.add("NCBITaxon:9999", "Parulidae")
    ols_env.add("NCBITaxon:111", "Obsoletus avis", obsolete=True)
    ols_env.add("NCBITaxon:112", "Obsoletus")
    ols_env.add("NCBITaxon:500", "Picidae", obsolete=True)
    taxa = [_taxon("BTSP", "Amphispiza bilineata"),
            _taxon("XTHR", "Toxostoma nonexistens"),  # genus fallback
            _taxon("LUWA", "Oreothlypis luciae", family="Parulidae"),  # no genus in NCBI -> family fallback
            _taxon("OBSO", "Obsoletus avis"),  # exact is obsolete -> genus
            _taxon("PICI", "Nothingus", family="Picidae"),  # only an obsolete family -> status obsolete
            _taxon("NONE", "Zzz yyy")]
    res = terms.resolve_taxa(taxa)
    got = {k[1]: (r.curie, r.label, r.method, r.status) for k, r in res.items()}
    assert got == {
        "BTSP": ("NCBITaxon:198939", "Amphispiza bilineata", "exact", "ok"),
        "XTHR": ("NCBITaxon:55418", "Toxostoma", "genus_fallback", "ok"),
        "LUWA": ("NCBITaxon:9999", "Parulidae", "family_fallback", "ok"),
        "OBSO": ("NCBITaxon:112", "Obsoletus", "genus_fallback", "ok"),
        "PICI": ("NCBITaxon:500", "Picidae", "family_fallback", "obsolete"),
        "NONE": (None, None, "unresolved", "unresolved"),
    }
    assert all(q[1] == "ncbitaxon" and q[2] == terms.SEARCH_SIZE for q in ols_env.search_calls)


def test_resolve_homonyms_by_kingdom(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("NCBITaxon:3497", "Morus", ancestors=("NCBITaxon:33090",))  # the mulberry
    ols_env.add("NCBITaxon:37577", "Morus", ancestors=("NCBITaxon:33208",))  # the gannet
    ols_env.add("NCBITaxon:8", "Sulidae")
    bird = _taxon("NOGA", "Morus", family="Sulidae", kingdom="Animalia")
    plant = terms.Taxon("PLANT", "MORUS", "Morus", "Morus", kingdom="Plantae")
    unknown = terms.Taxon("ALGAE", "MORUS", "Morus", "Morus", family="Sulidae", kingdom=None)
    res = terms.resolve_taxa([bird, plant, unknown])
    assert res[bird.key].curie == "NCBITaxon:37577"
    assert res[plant.key].curie == "NCBITaxon:3497"
    assert (res[unknown.key].curie, res[unknown.key].method) == ("NCBITaxon:8", "family_fallback")  # ambiguous


def test_resolve_pending_on_outage(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("NCBITaxon:1", "Aa bb")
    ols_env.add("NCBITaxon:2", "Cc dd")
    ols_env.add("NCBITaxon:3", "Cc")
    ols_env.fail_queries.add("aa bb")
    ols_env.fail_terms.add("NCBITaxon:2")
    res = terms.resolve_taxa([_taxon("A", "Aa bb"), _taxon("C", "Cc dd")])
    assert {k[1]: (r.method, r.status) for k, r in res.items()} == {"A": ("exact", "pending"),
                                                                    "C": ("exact", "pending")}


def test_run_taxa_end_to_end_and_skip_ok(ols_env: FakeOLS):  # noqa: F811
    con = fake_lakes(
        [_ft("brd_countdata", "BTSP", "Amphispiza bilineata", 42), _ft("brd_countdata", "LBWO", "Picoides scalaris", 9),
         _ft("brd_countdata", "ZZZZ", "Unknownus bird", 1)],
        [("BIRD", "BTSP", "BTSP", "Amphispiza bilineata", "(Cassin)", "species", None, "Animalia", "Emberizidae",
          "Amphispiza"),
         ("BIRD", "LBWO", "LBWO", "Picoides scalaris", None, "species", None, "Animalia", "Picidae", "Picoides"),
         ("BIRD", "ZZZZ", "ZZZZ", "Unknownus bird", None, "species", None, "Animalia", "Nullidae", "Unknownus")])
    ols_env.add("NCBITaxon:198939", "Amphispiza bilineata")
    ols_env.add("NCBITaxon:36302", "Picoides")
    ols_env.add("NCBITaxon:165749", "Dryobates scalaris")
    ols_env.extra_hits["picoides scalaris"] = ["NCBITaxon:165749"]  # OLS matches the synonym
    out = terms.run_taxa(con, SITE, True)
    assert out["counts"] == {"exact": 1, "genus_fallback": 1, "family_fallback": 0, "unresolved": 1,
                             "obsolete": 0, "pending": 0}
    assert out["typeChoices"][0]["taxonTypeCode"] == "BIRD"
    with store.store(read_only=True) as sc:
        rows = sc.execute("SELECT taxonID, curie, method, status FROM ref_ncbitaxon_map ORDER BY 1").fetchall()
        tt = sc.execute("SELECT siteCode, productCode, tableName, taxonTypeCode, matchRate FROM terms_taxon_types"
                        ).fetchall()
    assert rows == [("BTSP", "NCBITaxon:198939", "exact", "ok"), ("LBWO", "NCBITaxon:36302", "genus_fallback", "ok"),
                    ("ZZZZ", None, "unresolved", "unresolved")]
    assert tt == [(SITE, "DP1.10003.001", "brd_countdata", "BIRD", 1.0)]
    report = site_lake.site_paths(SITE).reports_dir / "unresolved_taxa.csv"
    got = {r["taxonID"]: r for r in csv.DictReader(report.open())}
    assert set(got) == {"LBWO", "ZZZZ"}  # every non-exact row is listed for review
    assert got["LBWO"]["searchHits"] == "NCBITaxon:165749 Dryobates scalaris"
    assert got["ZZZZ"]["status"] == "unresolved" and got["ZZZZ"]["nRecords"] == "1"
    n_search = len(ols_env.search_calls)
    again = terms.run_taxa(con, SITE, True)
    assert again["keptOk"] == 2 and again["resolvedNow"] == 1  # only the unresolved row is retried
    assert len(ols_env.search_calls) == n_search  # ... and its searches come from the cache


@pytest.mark.parametrize(("name", "want"), [
    ("Opuntia engelmannii Salm-Dyck ex Engelm.", "Opuntia engelmannii"),
    ("Ambrosia confertiflora DC. subsp. x", "Ambrosia confertiflora subsp. x"),
    ("Abies lasiocarpa (Hook.) Nutt. var. arizonica (Merriam) Lemmon", "Abies lasiocarpa var. arizonica"),
    ("Quercus alba L. 1753 et al", "Quercus alba"),
])
def test_query_name_inline_authorship(name, want):
    assert terms.query_name(name) == want
