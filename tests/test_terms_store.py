"""site/ontology_store.py: schema, CURIE helpers, cached term/search lookups, OBO registry, export."""

from __future__ import annotations

import json

import duckdb
import pytest

from neon_ducklake import env
from neon_ducklake.site import ontology_store as store

from .test_site_common import site_env  # noqa: F401 - fixture
from .test_terms_fakes import FakeOLS, ols_env  # noqa: F401 - fixture

CONTRACT = {
    "ols_term_cache": ["curie", "iri", "label", "ontologyId", "isObsolete", "oboFoundry", "fetchedAt"],
    "ref_ncbitaxon_map": ["taxonTypeCode", "taxonID", "neonName", "queryName", "curie", "label", "method",
                          "status", "resolvedAt"],
    "ref_unit_map": ["neonUnit", "uoCurie", "uoLabel", "method", "status", "validatedAt"],
    "ref_nlcd_envo": ["nlcdClass", "envoCurie", "envoLabel"],
    "ref_site_envo": ["siteCode", "envoCurie", "envoLabel", "typeEco", "method", "status"],
    "curation_avus": ["proposalId", "productCode", "attribute", "value", "unit", "curie", "label", "ontologyId",
                      "aspect", "columnName", "rationale", "model", "reps", "grounded", "status", "reason",
                      "createdAt"],
}


def test_open_store_creates_contract_tables(site_env):  # noqa: F811
    with store.store() as con:
        for table, cols in CONTRACT.items():
            got = [r[0] for r in con.execute(
                "SELECT column_name FROM duckdb_columns() WHERE table_name = ? ORDER BY column_index",
                [table]).fetchall()]
            assert got == cols, table
        con.execute("INSERT INTO ref_ncbitaxon_map (taxonTypeCode, taxonID) VALUES ('BIRD', 'X')")
        with pytest.raises(duckdb.ConstraintException):
            con.execute("INSERT INTO ref_ncbitaxon_map (taxonTypeCode, taxonID) VALUES ('BIRD', 'X')")
    assert store.store_path() == env.WORK_DIR / "ontology_cache.duckdb"
    with store.store(read_only=True) as con:  # re-open read-only; tables persist
        assert con.execute("SELECT count(*) FROM ref_ncbitaxon_map").fetchone()[0] == 1


def test_open_store_read_only_creates_missing_file(site_env):  # noqa: F811
    assert not store.store_path().exists()
    with store.store(read_only=True) as con:
        assert con.execute("SELECT count(*) FROM ols_term_cache").fetchone()[0] == 0


def test_lock_error_detection():
    assert store._is_lock_error(duckdb.IOException(
        'IO Error: Could not set lock on file "x": Conflicting lock is held in python (PID 1)'))
    assert not store._is_lock_error(duckdb.IOException("IO Error: No such file"))


@pytest.mark.parametrize(("raw", "want"), [
    ("ncbitaxon:9606", "NCBITaxon:9606"),
    ("NCBITAXON:9606", "NCBITaxon:9606"),
    ("envo:01000816", "ENVO:01000816"),
    (" uo:0000027 ", "UO:0000027"),
    ("http://purl.obolibrary.org/obo/ENVO_01000816", "ENVO:01000816"),
    ("http://purl.obolibrary.org/obo/NCBITaxon_198939", "NCBITaxon:198939"),
    ("ENVO_01000816", "ENVO:01000816"),
    ({"iri": "http://purl.obolibrary.org/obo/UO_0000187"}, "UO:0000187"),
    ({"unit": "pato:0000146"}, "PATO:0000146"),
    ("https://example.org/thing", None),
    ("not a curie", None),
    ("", None),
    (None, None),
])
def test_norm_curie(raw, want):
    assert store.norm_curie(raw) == want


def test_iri_for():
    assert store.iri_for("NCBITaxon:198939") == "http://purl.obolibrary.org/obo/NCBITaxon_198939"


def test_ols_term_cached_and_flags(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("ENVO:01000816", "area of deciduous forest")
    ols_env.add("ENVO:00000001", "obsolete old thing")
    ols_env.add("GONE:1", "a term of an obsolete ontology")
    ols_env.add("UO:0000001", "retired unit", obsolete=True)
    t = store.ols_term("envo:01000816")
    assert t == {"curie": "ENVO:01000816", "iri": "http://purl.obolibrary.org/obo/ENVO_01000816",
                 "label": "area of deciduous forest", "ontologyId": "envo", "isObsolete": False,
                 "oboFoundry": True}
    assert store.ols_term("ENVO:01000816") == t
    assert ols_env.term_calls == ["ENVO:01000816"]  # second call came from the store
    assert store.ols_term("ENVO:00000001")["isObsolete"] is True  # 'obsolete ...' label
    assert store.ols_term("UO:0000001")["isObsolete"] is True  # is_obsolete flag
    assert store.ols_term("GONE:1")["oboFoundry"] is False  # obsolete ontology in the registry
    with store.store(read_only=True) as con:
        assert con.execute("SELECT count(*) FROM ols_term_cache").fetchone()[0] == 4
        assert con.execute("SELECT fetchedAt IS NOT NULL FROM ols_term_cache LIMIT 1").fetchone()[0]


def test_ols_term_missing_and_unavailable(ols_env: FakeOLS):  # noqa: F811
    assert store.ols_term("UO:9999999") is None
    assert store.ols_term("UO:9999999") is None
    assert ols_env.term_calls == ["UO:9999999"]  # misses are remembered in-process
    assert store.ols_term("not-a-curie") is None
    ols_env.add("UO:0000187", "percent")
    ols_env.fail_terms.add("UO:0000187")
    with pytest.raises(store.OLSUnavailable):
        store.ols_term("UO:0000187")
    assert "UO:0000187" not in store.ols_terms(["UO:0000187"])  # batch form omits failures
    ols_env.fail_terms.clear()
    assert store.ols_term("UO:0000187")["label"] == "percent"  # failures are never cached
    with store.store(read_only=True) as con:
        assert con.execute("SELECT curie FROM ols_term_cache").fetchall() == [("UO:0000187",)]


def test_ols_terms_batch_and_refresh(ols_env: FakeOLS):  # noqa: F811
    for i in range(6):
        ols_env.add(f"UO:000000{i}", f"unit {i}")
    got = store.ols_terms([f"uo:000000{i}" for i in range(6)] + ["UO:0000001"])
    assert sorted(got) == [f"UO:000000{i}" for i in range(6)]
    assert len(ols_env.term_calls) == 6
    ols_env.terms["UO:0000001"]["label"] = "renamed unit"
    assert store.ols_term("UO:0000001")["label"] == "unit 1"
    assert store.ols_term("UO:0000001", refresh=True)["label"] == "renamed unit"
    assert store.ols_term("UO:0000001")["label"] == "renamed unit"


def test_search_cached_normalised_and_within(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("ENVO:01000218", "xeric shrubland biome", ancestors=("ENVO:00000428",))
    ols_env.add("ENVO:01000869", "area of scrub")
    ols_env.add("ENVO:00000300", "scrubland area")
    hits = store.search_ols("xeric shrubland biome", "envo")
    assert hits == [{"curie": "ENVO:01000218", "label": "xeric shrubland biome",
                     "iri": "http://purl.obolibrary.org/obo/ENVO_01000218", "ontologyId": "envo"}]
    assert store.search_ols("  Xeric   Shrubland biome ", "ENVO") == hits  # same normalised key
    assert len(ols_env.search_calls) == 1
    assert store.search_ols("scrub", "envo", within="ENVO:00000428") == []
    assert ols_env.search_calls[-1][3] == "http://purl.obolibrary.org/obo/ENVO_00000428"
    assert len(store.search_ols("scrub", "envo")) == 2  # a different cache key than the 'within' search
    many = store.search_many(["scrub", "xeric", "Scrub"], "envo")
    assert set(many) == {"scrub", "xeric", "Scrub"} and many["Scrub"] == many["scrub"]
    ols_env.fail_queries.add("nothing here")
    with pytest.raises(store.OLSUnavailable):
        store.search_ols("nothing here", "envo")
    assert store.search_ols("", "envo") == []
    with store.store(read_only=True) as con:
        rows = con.execute("SELECT ontologyId, query, size, within, results FROM ols_search_cache "
                           "ORDER BY query, within").fetchall()
    assert [(r[1], r[3]) for r in rows] == [("scrub", ""), ("scrub", "http://purl.obolibrary.org/obo/ENVO_00000428"),
                                            ("xeric", ""), ("xeric shrubland biome", "")]
    assert json.loads(rows[-1][4])[0]["curie"] == "ENVO:01000218"


def test_search_dedupes_and_normalises_curies(monkeypatch):
    raw = [{"curie": "ncbitaxon:1", "label": "A", "iri": "", "ontologyId": "ncbitaxon"},
           {"curie": "NCBITaxon:1", "label": "A", "iri": "", "ontologyId": "ncbitaxon"},
           {"curie": "", "label": "B", "iri": "http://purl.obolibrary.org/obo/NCBITaxon_2", "ontologyId": "x"},
           {"curie": "", "label": "junk", "iri": "", "ontologyId": ""}]
    got = store._clean_hits(raw)
    assert [h["curie"] for h in got] == ["NCBITaxon:1", "NCBITaxon:2"]
    assert got[0]["iri"] == "http://purl.obolibrary.org/obo/NCBITaxon_1"


def test_exact_matches():
    hits = [{"curie": "UO:0000027", "label": "degree Celsius"}, {"curie": "UO:1000027", "label": "degree Celsius"},
            {"curie": "UO:0000027", "label": "degree Celsius"}, {"curie": "X:1", "label": "Degree  celsius"},
            {"curie": "UO:0000195", "label": "degree Fahrenheit"}]
    assert [h["curie"] for h in store.exact_matches(hits, "DEGREE celsius")] == ["UO:0000027", "UO:1000027", "X:1"]
    assert [h["curie"] for h in store.exact_matches(hits, "degree celsius", "uo")] == ["UO:0000027", "UO:1000027"]


def test_obo_foundry_prefixes_from_cached_registry(ols_env):  # noqa: F811
    assert store.obo_foundry_prefixes() == {"ncbitaxon", "uo", "envo"}
    assert store.registry_path() == env.ROOT / "ref" / "obofoundry_ontologies.jsonld"


def test_obo_registry_download_when_missing(site_env, monkeypatch):  # noqa: F811
    calls = []

    def fake_get(url):
        calls.append(url)
        return 200, json.dumps({"ontologies": [{"id": "PATO", "activity_status": "active"}]}).encode()

    monkeypatch.setattr(store, "_get", fake_get)
    monkeypatch.setattr(store, "_OBO_MEMO", {})
    assert store.obo_foundry_prefixes() == {"pato"}
    assert store.obo_foundry_prefixes() == {"pato"}
    assert calls == [store.OBO_REGISTRY_URL]  # downloaded once, then read from ref/
    assert store.registry_path().exists()


def test_export_parquet_ordered_and_stable(ols_env: FakeOLS):  # noqa: F811
    ols_env.add("UO:0000187", "percent")
    ols_env.add("ENVO:01000816", "area of deciduous forest")
    store.ols_terms(["UO:0000187", "ENVO:01000816"])
    with store.store() as con:
        con.execute("INSERT INTO ref_unit_map VALUES ('percent', 'UO:0000187', 'percent', 'seed', 'ok', now())")
    counts = store.export_parquet()
    assert counts["ols_term_cache"] == 2 and counts["ref_unit_map"] == 1 and counts["curation_avus"] == 0
    assert "ols_search_cache" not in counts
    out = store.maps_dir()
    assert sorted(p.name for p in out.glob("*.parquet")) == sorted(f"{t}.parquet" for t in store.EXPORT_TABLES)
    rows = duckdb.sql(f"SELECT curie FROM '{out / 'ols_term_cache.parquet'}'").fetchall()
    assert rows == [("ENVO:01000816",), ("UO:0000187",)]
    before = {p.name: p.stat().st_mtime_ns for p in out.glob("*.parquet")}
    store.export_parquet()
    assert {p.name: p.stat().st_mtime_ns for p in out.glob("*.parquet")} == before  # unchanged -> not rewritten
    assert not list(out.glob("*.part"))
