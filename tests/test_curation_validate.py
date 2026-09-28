"""site/validate.py: every rule, the replicate outcome, canonical rebuild, outputs and human acceptance."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import env
from neon_ducklake.site import curate, ontology_store, validate

from .test_site_common import SITE, site_env  # noqa: F401 - fixture

P = "DP1.00002.001"
OBO = {"pato", "envo", "ncbitaxon", "obi", "iao", "uo", "gaz", "go"}
TERMS: dict[str, dict[str, Any] | None] = {
    "PATO:0000146": {"label": "temperature", "isObsolete": False},
    "ENVO:00002005": {"label": "air", "isObsolete": False},
    "OBI:0000070": {"label": "assay", "isObsolete": False},
    "IAO:0000027": {"label": "data item", "isObsolete": False},
    "IAO:0000100": {"label": "data set", "isObsolete": False},
    "NCBITaxon:9606": {"label": "Homo sapiens", "isObsolete": False},
    "NCBITaxon:8782": {"label": "Aves", "isObsolete": False},
    "NCBITaxon:50557": {"label": "Insecta", "isObsolete": False},
    "ENVO:01000174": {"label": "forest biome", "isObsolete": False},
    "UO:0000027": {"label": "degree Celsius", "isObsolete": False},
    "GO:0000001": {"label": "obsolete mitochondrion inheritance", "isObsolete": True},
    "PATO:0000001": {"label": "quality", "isObsolete": False},
    "GO:0015979": {"label": "photosynthesis", "isObsolete": False},
    "OBI:9999999": None,
}
FACTS = {
    "NCBITaxon:9606": {"speciesOrBelow": True, "isBiome": False},
    "NCBITaxon:8782": {"speciesOrBelow": False, "isBiome": False},
    "NCBITaxon:50557": {"speciesOrBelow": False, "isBiome": False},
    "ENVO:00002005": {"speciesOrBelow": False, "isBiome": False},
    "ENVO:01000174": {"speciesOrBelow": False, "isBiome": True},
}


def avu(curie: str, label: str, value: str | None = None, *, column: str | None = None,
        aspect: str = "measured_property", attribute: str = "bogus.attr") -> dict[str, Any]:
    return {"attribute": attribute, "value": label if value is None else value, "unit": curie,
            "ontology_id": curie.split(":")[0].lower(), "curie": curie, "iri": "", "label": label,
            "aspect": aspect, "column": column, "rationale": f"because {label}"}


@pytest.fixture
def fake_ols(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:  # noqa: F811
    """Fake OLS terms/facts/registry; the real DuckDB store under the tmp root. Counts fact fetches."""
    fetched = {"facts": 0}

    def ols_terms(curies: list[str], **kw: Any) -> dict[str, Any]:
        return {c: ({"curie": c, **TERMS[c]} if TERMS[c] else None) for c in curies if c in TERMS}

    def fetch_facts(curie: str) -> dict[str, Any]:
        fetched["facts"] += 1
        return {"curie": curie, "ontologyId": curie.split(":")[0].lower(), "rank": None, "ancestors": [],
                **FACTS[curie]}

    monkeypatch.setattr(ontology_store, "ols_terms", ols_terms)
    monkeypatch.setattr(ontology_store, "obo_foundry_prefixes", lambda refresh=False: set(OBO))
    monkeypatch.setattr(validate, "fetch_facts", fetch_facts)
    monkeypatch.setattr(validate, "product_columns",
                        lambda site, products: {p: {"tempsinglemean", "saat_30min.tempsinglemean"} for p in products})
    with ontology_store.store() as con:
        con.execute("INSERT INTO ref_ncbitaxon_map VALUES ('BIRD', 'X', 'x', 'x', 'NCBITaxon:50557', 'Insecta', "
                    "'family_fallback', 'ok', NULL)")
    return fetched


def write_proposal(rep: int, avus: list[dict[str, Any]], seen: list[str], product: str = P) -> Path:
    p = curate.curation_paths(SITE)
    card = p.card(product)
    card.parent.mkdir(parents=True, exist_ok=True)
    card.write_text(f"# {product}: Single aspirated air temperature\n")
    env.write_json(p.proposal(product, rep), {
        "kind": "product", "key": product, "rep": rep, "productCode": product, "model": "claude-opus-5-5",
        "card_sha256": "c", "prompt_sha256": "p", "seconds": 1.0, "tool_calls": 3, "usage": {"input_tokens": 1},
        "tool_call_counts": {}, "seen_curies": seen, "avus": avus, "trace": "t", "parse_ok": True})
    return p.proposal(product, rep)


def standard_proposals() -> None:
    seen = ["PATO:0000146", "ENVO:00002005", "OBI:0000070", "NCBITaxon:9606", "NCBITaxon:8782",
            "ENVO:01000174", "UO:0000027", "GO:0000001", "NCBITaxon:50557", "PATO:0000001", "GO:0015979"]
    rep1 = [
        avu("PATO:0000146", "temperature", column="tempSingleMean"),  # accepted
        avu("ENVO:00002005", "air", aspect="environmental_material"),  # proposed (rep 1 only)
        avu("OBI:0000070", "assay", column="nope", aspect="method"),  # rep1 unknown column
        avu("NCBITaxon:9606", "Homo sapiens", aspect="organism_group"),  # species
        avu("NCBITaxon:8782", "aves", "Aves", aspect="organism_group"),  # accepted (label case-insensitive)
        avu("ENVO:01000174", "forest biome"),  # biome
        avu("UO:0000027", "degree Celsius"),  # unit
        avu("PATO:0000001", "Quality", "quality at TEST"),  # site-specific value
        avu("GO:0000001", "obsolete mitochondrion inheritance"),  # obsolete
        avu("NCBITaxon:50557", "Insecta"),  # duplicate deterministic
        avu("IAO:0000100", "data set", aspect="data_type"),  # valid both, ungrounded -> proposed
        avu("OBI:9999999", "nothing"),  # unresolved
        avu("ECSO:00000001", "ecso thing"),  # not OBO Foundry
        avu("GO:0015979", "photosynthesis process", aspect="process"),  # label mismatch
        {"value": "free text", "attribute": "x.y"},  # no CURIE
    ]
    rep2 = [
        avu("pato:0000146", "Temperature", "temperature"),
        avu("OBI:0000070", "assay", column=None, aspect="method"),  # rep2 valid -> proposed
        avu("NCBITaxon:8782", "Aves", aspect="organism_group"),
        avu("IAO:0000100", "data set", aspect="data_type"),
        avu("IAO:0000027", "data item", "x" * 2001),  # too long
    ]
    write_proposal(1, rep1, seen)
    write_proposal(2, rep2, seen)


def by_curie(items: list[dict[str, Any]]) -> dict[str | None, dict[str, Any]]:
    return {it["curie"]: it for it in items}


def test_rules_outcomes_and_outputs(fake_ols: dict[str, int]) -> None:
    standard_proposals()
    assert validate.main(SITE) == 0
    doc = json.loads(curate.generic_path(P).read_text())
    acc, prop, rej = by_curie(doc["accepted"]), by_curie(doc["proposed"]), by_curie(doc["rejected"])
    assert set(acc) == {"PATO:0000146", "NCBITaxon:8782"}
    assert set(prop) == {"ENVO:00002005", "OBI:0000070", "IAO:0000100"}
    assert prop["ENVO:00002005"]["reason"] == "single_replicate" and prop["IAO:0000100"]["reason"] == "ungrounded"
    reasons = {c: it["reason"] for c, it in rej.items()}
    assert reasons == {
        "NCBITaxon:9606": "species_or_below", "ENVO:01000174": "biome_term", "UO:0000027": "forbidden_unit_term",
        "PATO:0000001": "site_specific_value", "GO:0000001": "obsolete",
        "NCBITaxon:50557": "duplicate_deterministic", "OBI:9999999": "unresolved",
        "ECSO:00000001": "not_active_obo_foundry", "GO:0015979": "label_mismatch", None: "no_curie",
        "IAO:0000027": "value_too_long"}
    t = acc["PATO:0000146"]
    # the model's attribute is discarded; canonical <prefix>.<snake(OLS label)> | value | CURIE
    assert (t["attribute"], t["value"], t["unit"], t["label"]) == ("pato.temperature", "temperature",
                                                                  "PATO:0000146", "temperature")
    assert t["reps"] == 2 and t["grounded"] and t["column"] == "tempSingleMean"
    assert acc["NCBITaxon:8782"]["attribute"] == "ncbitaxon.aves" and acc["NCBITaxon:8782"]["value"] == "Aves"
    assert doc["counts"] == {"accepted": 2, "human_accepted": 0, "proposed": 3, "rejected": 11}
    assert t["proposalId"] == validate.proposal_id(P, "PATO:0000146", "temperature")
    with ontology_store.store(read_only=True) as con:
        rows = con.execute("SELECT status, count(*) FROM curation_avus WHERE productCode = ? GROUP BY 1 ORDER BY 1",
                           [P]).fetchall()
        facts = con.execute("SELECT count(*) FROM curation_term_facts").fetchone()[0]
    assert rows == [("accepted", 2), ("proposed", 3), ("rejected", 11)]
    assert facts == 5  # the NCBITaxon/ENVO terms that resolved, cached
    assert (env.ROOT / "ref" / "ontology_maps" / "curation_avus.parquet").exists()
    review = curate.curation_paths(SITE).review_file.read_text()
    for it in doc["proposed"]:
        assert f"- [ ] `{it['proposalId']}`" in review
    assert f"`{t['proposalId']}` pato.temperature = \"temperature\"" in review
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "validate_summary.json").read_text())
    assert summary["counts"][P]["accepted"] == 2 and summary["rejectReasons"]["obsolete"] == 1

    # a second run takes facts from the cache and leaves unchanged outputs byte-identical
    n = fake_ols["facts"]
    before = (curate.generic_path(P).read_bytes(), curate.curation_paths(SITE).review_file.read_bytes())
    assert validate.main(SITE) == 0 and fake_ols["facts"] == n
    assert (curate.generic_path(P).read_bytes(), curate.curation_paths(SITE).review_file.read_bytes()) == before


def test_human_acceptance_by_flag_tick_and_persistence(fake_ols: dict[str, int]) -> None:
    standard_proposals()
    assert validate.main(SITE) == 0
    doc = json.loads(curate.generic_path(P).read_text())
    ids = {it["curie"]: it["proposalId"] for it in doc["proposed"]}
    assert validate.main(SITE, accept=f"{ids['ENVO:00002005']},deadbeefdeadbeef") == 0
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "validate_summary.json").read_text())
    assert summary["unknownAcceptIds"] == ["deadbeefdeadbeef"]
    # the earlier acceptance persists without the flag
    assert validate.main(SITE) == 0
    doc = json.loads(curate.generic_path(P).read_text())
    assert {it["curie"] for it in doc["accepted"] if it["status"] == "human_accepted"} == {"ENVO:00002005"}
    # tick a box in review.md
    review = curate.curation_paths(SITE).review_file
    review.write_text(review.read_text().replace(f"- [ ] `{ids['OBI:0000070']}`", f"- [x] `{ids['OBI:0000070']}`"))
    assert validate.main(SITE) == 0
    with ontology_store.store(read_only=True) as con:
        human = {r[0] for r in con.execute("SELECT curie FROM curation_avus WHERE status = 'human_accepted'").fetchall()}
    assert human == {"ENVO:00002005", "OBI:0000070"}
    assert f"- [x] `{ids['OBI:0000070']}`" in review.read_text()
    # --accept proposed takes the rest
    assert validate.main(SITE, accept="proposed") == 0
    doc = json.loads(curate.generic_path(P).read_text())
    assert doc["counts"]["proposed"] == 0 and doc["counts"]["human_accepted"] == 3


def test_incomplete_replicates_and_ols_outage_write_nothing(fake_ols: dict[str, int],
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    write_proposal(1, [avu("PATO:0000146", "temperature")], ["PATO:0000146"])
    assert validate.main(SITE) == 2
    assert not curate.generic_path(P).exists()
    write_proposal(2, [avu("PATO:0000146", "temperature")], ["PATO:0000146"])
    monkeypatch.setattr(ontology_store, "ols_terms", lambda curies, **kw: {})  # OLS unreachable
    assert validate.main(SITE) == 2
    assert not curate.generic_path(P).exists()


def test_product_cap_keeps_both_replicate_terms_first() -> None:
    labels = [f"quality {i}" for i in range(30)]
    terms = {f"PATO:{i:07d}": {"label": lab, "isObsolete": False} for i, lab in enumerate(labels)}
    ctx = validate.Context(terms=terms, facts={}, obo={"pato"}, deterministic=set(), site_codes={"TEST"},
                           site_names=set())
    both = [avu(c, t["label"]) for c, t in list(terms.items())[20:]]  # 10 in both replicates
    only1 = [avu(c, t["label"]) for c, t in list(terms.items())[:20]]  # 20 in rep 1 only
    seen = {c.casefold() for c in terms}
    reps = [validate.Replicate(1, Path("r1"), {"avus": only1 + both, "seen_curies": sorted(seen)}),
            validate.Replicate(2, Path("r2"), {"avus": both, "seen_curies": sorted(seen)})]
    items = validate.evaluate_product(P, reps, ctx)
    status = [it["status"] for it in items]
    assert status.count("accepted") == 10 and status.count("proposed") == 15
    assert status.count("rejected") == 5 and all(it["reason"] == "over_product_cap"
                                                  for it in items if it["status"] == "rejected")


def test_unit_proposal_validation(fake_ols: dict[str, int]) -> None:
    with ontology_store.store() as con:
        con.executemany("INSERT INTO ref_unit_map VALUES (?, ?, ?, ?, ?, ?)",
                        [[u, None, None, "unmapped", "unmapped", None] for u in ("a", "b", "c", "d")])
    proposal = {"key": "units.x", "seen_curies": ["UO:0000027", "PATO:0000146"], "units": [
        {"neonUnit": "a", "curie": "uo:0000027", "label": "Degree Celsius"},
        {"neonUnit": "b", "curie": "PATO:0000146", "label": "temperature"},
        {"neonUnit": "c", "curie": "UO:0000027", "label": "kelvin"},
        {"neonUnit": "zzz", "curie": "UO:0000027", "label": "degree Celsius"}]}
    out = tmp = curate.curation_paths(SITE).proposals_dir / "units.x.validated.json"
    res = validate.apply_unit_proposal(SITE, proposal, {"a", "b", "c", "d"}, out)
    assert res["mapped"] == 1 and res["unmappedAfter"] == ["b", "c", "d"]
    assert {r["neonUnit"]: r["reason"] for r in res["rejected"]} == {"b": "not_uo", "c": "label_mismatch",
                                                                     "zzz": "not_requested"}
    assert json.loads(tmp.read_text())["mapped"] == 1
    with ontology_store.store(read_only=True) as con:
        rows = con.execute("SELECT neonUnit, uoCurie, method, status FROM ref_unit_map ORDER BY 1").fetchall()
    assert rows[0] == ("a", "UO:0000027", "curated", "ok") and rows[1] == ("b", None, "unmapped", "unmapped")
    # ungrounded mappings are refused (a units pass has no second replicate)
    res = validate.apply_unit_proposal(SITE, {"units": [{"neonUnit": "d", "curie": "UO:0000027",
                                                         "label": "degree Celsius"}], "seen_curies": []}, {"d"}, out)
    assert res["mapped"] == 0 and res["rejected"][0]["reason"] == "ungrounded"


def test_helpers() -> None:
    assert validate.proposal_id(P, "PATO:1", "v") == validate.proposal_id(P, "PATO:1", "v")
    assert validate.proposal_id(P, "PATO:1", "v") != validate.proposal_id(P, "PATO:1", "w")
    assert validate.clean_column(" null ") is None and validate.clean_column("x") == "x"
    assert validate.is_reserved("datacite") and validate.is_reserved("DC") and not validate.is_reserved("envo")
    assert validate.canonical_avu("ENVO", "Surface Water", "surface water", "ENVO:00002042") == {
        "attribute": "envo.surface_water", "value": "surface water", "unit": "ENVO:00002042"}
    ctx = validate.Context(terms={}, facts={}, obo=set(), deterministic=set(), site_codes={"SRER", "HARV"},
                           site_names={"santa rita experimental range"})
    assert validate.mentions_site("air at SRER", ctx) and validate.mentions_site("Santa Rita Experimental Range", ctx)
    assert not validate.mentions_site("air temperature", ctx)
    assert validate._ranks({"has_rank": ["http://purl.obolibrary.org/obo/TAXRANK_0000006"]}) == {"TAXRANK:0000006"}
