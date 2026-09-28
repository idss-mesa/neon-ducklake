"""The site-level curation pass: site/curate.py (two replicates on cards/_site.md) and site/validate.py
(agreement rule, HABITAT rules, review ticks, ref_site_envo / ref_site_envo_feature)."""

from __future__ import annotations

import json
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import env
from neon_ducklake.site import curate, ontology_store, validate

from .test_curation_curate import fake_mesa_mcp
from .test_site_common import SITE, site_env  # noqa: F401 - fixture

REPO_CURATION = Path(__file__).resolve().parents[1] / "curation"
BIOME, AQUATIC, TERRESTRIAL, WATER = "ENVO:00000428", "ENVO:00002030", "ENVO:00000446", "ENVO:00000063"

FAKE_CLAUDE = r'''
import json, os, sys
from pathlib import Path

here = Path(__file__).resolve().parent
cfg = json.loads((here / "fake_config.json").read_text())
argv = sys.argv[1:]
stdin = sys.stdin.read()
with (here / "calls.jsonl").open("a") as log:
    log.write(json.dumps({"argv": argv, "env": sorted(os.environ), "site": "SITE CARD" in stdin,
                          "card": "# Site:" in stdin, "resume": "--resume" in argv}) + "\n")

def emit(ev):
    print(json.dumps(ev), flush=True)

sid = f"sess-site-{os.getpid()}"
emit({"type": "system", "subtype": "init", "session_id": sid, "model": "claude-opus-5-5",
      "claude_code_version": "0.0-fake", "mcp_servers": [{"name": "mesa-ols", "status": "connected"}],
      "tools": ["mcp__mesa-ols__mesa_ols_get_term", "mcp__mesa-ols__mesa_ols_search_terms"]})
emit({"type": "assistant", "message": {"content": [
    {"type": "tool_use", "id": "s1", "name": "mcp__mesa-ols__mesa_ols_search_terms",
     "input": {"query": "freshwater stream biome", "ontology_id": "envo"}}]}})
emit({"type": "user", "message": {"content": [
    {"type": "tool_result", "tool_use_id": "s1", "content": [{"type": "text", "text": json.dumps(
        {"results": [{"curie": c, "label": l} for c, l in cfg["seen"]]})}]}]}})
emit({"type": "result", "subtype": "success", "is_error": False, "session_id": sid,
      "result": json.dumps({"terms": cfg["terms"]}), "usage": {"input_tokens": 3, "output_tokens": 2},
      "total_cost_usd": 0.05, "num_turns": 2, "duration_ms": 5})
'''

STREAM_TERMS = [{"role": "biome", "curie": "ENVO:03605008", "label": "freshwater stream biome", "rationale": "r"},
                {"role": "feature", "curie": "ENVO:00000023", "label": "stream", "rationale": "r"}]
SEEN = [["ENVO:03605008", "freshwater stream biome"], ["ENVO:00000023", "stream"]]


@pytest.fixture
def fake_claude(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:  # noqa: F811
    shutil.copytree(REPO_CURATION, env.ROOT / "curation", ignore=shutil.ignore_patterns("generic"))
    (env.ROOT / "curation" / "generic").mkdir()
    bindir = site_env / "bin"
    bindir.mkdir()
    script = bindir / "claude"
    script.write_text(f"#!{sys.executable}\n{FAKE_CLAUDE}")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    (bindir / "fake_config.json").write_text(json.dumps({"terms": STREAM_TERMS, "seen": SEEN}))
    monkeypatch.setattr(curate, "CLAUDE_BIN", script)
    monkeypatch.setattr(env, "MESA_HOME", site_env / "mesa-home")
    fake_mesa_mcp(env.MESA_HOME)
    monkeypatch.setattr(curate, "unmapped_units", list)
    monkeypatch.setenv("NEON_TOKEN", "must-not-leak")
    return bindir


def calls(bindir: Path) -> list[dict]:
    f = bindir / "calls.jsonl"
    return [json.loads(line) for line in f.read_text().splitlines()] if f.exists() else []


def write_site_card() -> Path:
    p = curate.curation_paths(SITE).site_card
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f"# Site: {SITE} — Test Creek NEON\n\nHABITAT: Aquatic Wadeable Stream\n")
    return p


# ---------------------------------------------------------------- curate


def test_prompt_site_carries_the_contract() -> None:
    text = (REPO_CURATION / "prompt_site.md").read_text()
    for needle in ("mesa_ols_search_terms", "mesa_ols_get_term", "ENVO:00000428", "ENVO:00000063", "HABITAT",
                   '{"terms": [{"role": "biome"', '"role": "feature"', "12 tool calls", "never a terrestrial"):
        assert needle in text, needle
    assert text.rstrip().endswith("SITE CARD")


def test_site_pass_two_scrubbed_replicates(fake_claude: Path) -> None:
    p = curate.curation_paths(SITE)
    assert curate.main(SITE, site_level=True) == 1  # no site card yet
    assert json.loads((p.site_dir / "reports" / "curate_site_summary.json").read_text())["status"] == "missing_card"
    card = write_site_card()
    assert curate.main(SITE, site_level=True) == 0
    got = calls(fake_claude)
    assert len(got) == 2 and all(c["site"] and c["card"] and not c["resume"] for c in got)
    for c in got:
        argv = c["argv"]
        assert argv[argv.index("--allowedTools") + 1] == \
            "mcp__mesa-ols__mesa_ols_search_terms,mcp__mesa-ols__mesa_ols_get_term"
        assert "mcp__mesa-ols__mesa_avu_from_term" in argv[argv.index("--disallowedTools") + 1]
        assert argv[argv.index("--max-turns") + 1] == str(curate.SITE_MAX_TURNS)
        assert argv[argv.index("--model") + 1] == "claude-opus-5-5"
        assert "NEON_TOKEN" not in c["env"]
    for rep in (1, 2):
        prop = json.loads(p.site_proposal(rep).read_text())
        assert prop["kind"] == "site" and prop["key"] == curate.SITE_KEY and prop["rep"] == rep
        assert prop["terms"] == STREAM_TERMS and prop["seen_curies"] == ["ENVO:00000023", "ENVO:03605008"]
        assert prop["card_sha256"] == curate.sha256_bytes(card.read_bytes())
        assert prop["trace"] == f"sites/{SITE}/curation/site/_site.rep{rep}.jsonl"
    assert curate.has_site_proposals(SITE)
    assert not list(p.proposals_dir.glob("*.json")) if p.proposals_dir.exists() else True  # kept apart
    summary = json.loads((p.site_dir / "reports" / "curate_site_summary.json").read_text())
    assert summary["status"] == "ok" and summary["total_cost_usd"] == pytest.approx(0.1)
    assert summary["terms"]["rep1"][0] == ["biome", "ENVO:03605008", "freshwater stream biome"]
    # existing proposals: no new child; force re-runs both
    assert curate.main(SITE, site_level=True) == 0 and len(calls(fake_claude)) == 2
    assert curate.main(SITE, site_level=True, force=True) == 0 and len(calls(fake_claude)) == 4


def test_default_main_adds_the_site_pass_once(fake_claude: Path) -> None:
    write_site_card()
    p = curate.curation_paths(SITE)
    (p.cards_dir / "DP1.00004.001.md").write_text("# DP1.00004.001 Barometric pressure\n")
    env.write_json(curate.generic_path("DP1.00004.001"), {"done": True})  # product already validated
    codes, skipped = curate.select_products(SITE, None, False)
    assert codes == [] and "_site" not in skipped  # the site card is never a product
    assert curate.main(SITE, products=["DP1.00004.001"]) == 0 and calls(fake_claude) == []  # filter: no site pass
    assert curate.main(SITE, site_level=False) == 0 and calls(fake_claude) == []
    assert curate.main(SITE) == 0 and len(calls(fake_claude)) == 2
    summary = json.loads((p.site_dir / "reports" / "curate_summary.json").read_text())
    assert len(summary["siteRuns"]) == 2 and summary["runs"] == []
    assert curate.main(SITE) == 0 and len(calls(fake_claude)) == 2  # proposals exist: not again


# ---------------------------------------------------------------- validate

LABELS = {"ENVO:03605008": "freshwater stream biome", "ENVO:01000253": "freshwater river biome",
          "ENVO:01000193": "temperate grassland biome", "ENVO:00000023": "stream", "ENVO:00000022": "river",
          "ENVO:01000174": "forest biome", "ENVO:00002005": "air", "PATO:0000146": "temperature"}
ANCESTORS = {"ENVO:03605008": [BIOME, AQUATIC, WATER, "ENVO:00000023"], "ENVO:01000253": [BIOME, AQUATIC, WATER],
             "ENVO:01000193": [BIOME, TERRESTRIAL], "ENVO:00000023": [WATER], "ENVO:00000022": [WATER, "ENVO:00000023"],
             "ENVO:01000174": [BIOME, TERRESTRIAL], "ENVO:00002005": []}


@pytest.fixture
def site_ols(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:  # noqa: F811
    cfg: dict[str, Any] = {"habitat": "Aquatic Wadeable Stream"}

    def ols_terms(curies: list[str], **kw: Any) -> dict[str, Any]:
        return {c: {"curie": c, "label": LABELS[c], "isObsolete": False} for c in curies if c in LABELS}

    def fetch_facts(curie: str) -> dict[str, Any]:
        anc = ANCESTORS[curie]
        return {"curie": curie, "ontologyId": "envo", "rank": None, "speciesOrBelow": False,
                "isBiome": BIOME in anc, "ancestors": anc}

    monkeypatch.setattr(ontology_store, "ols_terms", ols_terms)
    monkeypatch.setattr(validate, "fetch_facts", fetch_facts)
    monkeypatch.setattr(validate, "site_habitat", lambda site: cfg["habitat"])
    with ontology_store.store() as con:  # what `site terms` left: the heuristic rows
        con.execute(f"INSERT INTO ref_site_envo VALUES ('{SITE}', 'ENVO:03605008', 'freshwater stream biome', NULL, "
                    "'heuristic', 'proposed')")
        con.execute(f"INSERT INTO ref_site_envo_feature VALUES ('{SITE}', 'ENVO:00000023', 'stream', "
                    "'Aquatic Wadeable Stream', 'heuristic', 'proposed')")
        con.execute(f"INSERT INTO site_envo_proposals (siteCode, role, source, envoCurie, envoLabel, rule) VALUES "
                    f"('{SITE}', 'biome', 'heuristic', 'ENVO:03605008', 'freshwater stream biome', 'x'), "
                    f"('{SITE}', 'feature', 'heuristic', 'ENVO:00000023', 'stream', 'x')")
    return cfg


def site_prop(rep: int, terms: list[dict[str, Any]], seen: list[str] | None = None) -> None:
    p = curate.curation_paths(SITE)
    env.write_json(p.site_proposal(rep), {
        "kind": "site", "key": curate.SITE_KEY, "rep": rep, "model": "claude-opus-5-5", "card_sha256": "c",
        "prompt_sha256": "p", "seconds": 1.0, "tool_call_counts": {}, "total_cost_usd": 0.05,
        "seen_curies": [t["curie"] for t in terms] if seen is None else seen, "terms": terms, "trace": "t"})


def term(role: str, curie: str, label: str | None = None) -> dict[str, Any]:
    return {"role": role, "curie": curie, "label": LABELS.get(curie) if label is None else label, "rationale": "why"}


def refs() -> tuple[list, list]:
    with ontology_store.store(read_only=True) as con:
        return (con.execute("SELECT envoCurie, method, status FROM ref_site_envo").fetchall(),
                con.execute("SELECT envoCurie, habitat, method, status FROM ref_site_envo_feature").fetchall())


def test_site_agreement_is_curated(site_ols) -> None:
    for rep in (1, 2):
        site_prop(rep, [term("biome", "ENVO:01000253"), term("feature", "envo:00000022", "river")])
    assert validate.has_proposals(SITE) and not validate.load_replicates(SITE)
    assert validate.main(SITE) == 0  # no product proposals: the site pass alone decides
    biome, feature = refs()
    assert biome == [("ENVO:01000253", "curated", "curated")]
    assert feature == [("ENVO:00000022", "Aquatic Wadeable Stream", "curated", "curated")]
    p = curate.curation_paths(SITE)
    doc = json.loads((p.site_dir_curation / "_site.validated.json").read_text())
    assert doc["items"]["biome"]["status"] == "curated" and doc["habitat"] == "Aquatic Wadeable Stream"
    with ontology_store.store(read_only=True) as con:
        props = con.execute("SELECT role, source, envoCurie FROM site_envo_proposals ORDER BY role, source").fetchall()
    assert props == [("biome", "heuristic", "ENVO:03605008"), ("biome", "rep1", "ENVO:01000253"),
                     ("biome", "rep2", "ENVO:01000253"), ("feature", "heuristic", "ENVO:00000023"),
                     ("feature", "rep1", "ENVO:00000022"), ("feature", "rep2", "ENVO:00000022")]
    assert "## biome: curated" in (p.curation_dir / "review_site.md").read_text()
    # the curated biome counts as deterministic for product curation; the site's water body does not
    # (product terms are generic across sites: an aquatic product may use 'river')
    det = validate.deterministic_curies()
    assert "ENVO:01000253" in det and "ENVO:00000022" not in det


def test_site_rules_disagreement_review_and_tick(site_ols) -> None:
    site_prop(1, [term("biome", "ENVO:01000193"), term("feature", "ENVO:00000023"), term("feature", "ENVO:00000022")])
    site_prop(2, [term("biome", "ENVO:03605008"), term("feature", "ENVO:00000023"), term("biome", "ENVO:00002005")])
    res = validate.validate_site(SITE)
    b, f = res["items"]["biome"], res["items"]["feature"]
    assert (b["status"], b["reason"]) == ("proposed", "single_replicate")  # grassland: not an aquatic biome
    assert [c["reason"] for c in b["choices"]] == ["not_an_aquatic_biome", None]
    assert (f["status"], f["curie"]) == ("curated", "ENVO:00000023")  # first feature of each replicate
    assert refs()[0] == [("ENVO:03605008", "heuristic", "proposed")]  # the heuristic row stays
    review = curate.curation_paths(SITE).curation_dir / "review_site.md"
    opt = b["options"][0]
    assert f"- [ ] `{opt['proposalId']}` ENVO:03605008 freshwater stream biome" in review.read_text()
    review.write_text(review.read_text().replace(f"- [ ] `{opt['proposalId']}`", f"- [x] `{opt['proposalId']}`"))
    res = validate.validate_site(SITE)
    assert res["items"]["biome"]["status"] == "curated" and res["matched"] == [opt["proposalId"]]
    assert refs()[0] == [("ENVO:03605008", "curated", "curated")]
    with ontology_store.store(read_only=True) as con:
        assert con.execute("SELECT envoCurie FROM site_envo_proposals WHERE source = 'human'").fetchall() == [
            ("ENVO:03605008",)]


def test_site_disagreement_accept_flag_and_revert(site_ols) -> None:
    site_prop(1, [term("biome", "ENVO:03605008"), term("feature", "ENVO:00000023")])
    site_prop(2, [term("biome", "ENVO:01000253"), term("feature", "ENVO:00000023")])
    res = validate.validate_site(SITE)
    b = res["items"]["biome"]
    assert (b["status"], b["reason"], len(b["options"])) == ("proposed", "replicates_disagree", 2)
    assert validate.main(SITE) == 2  # something left to review
    river = next(o for o in b["options"] if o["curie"] == "ENVO:01000253")
    assert validate.main(SITE, accept=river["proposalId"]) == 0
    assert refs()[0] == [("ENVO:01000253", "curated", "curated")]
    # later replicates that are ungrounded: not curated any more -> back to the heuristic
    for rep in (1, 2):
        site_prop(rep, [term("biome", "ENVO:03605008"), term("feature", "ENVO:00000023")], seen=[])
    res = validate.validate_site(SITE)
    assert res["items"]["biome"]["reason"] == "ungrounded"
    assert refs()[0] == [("ENVO:03605008", "heuristic", "proposed")]


def test_site_terrestrial_rules_and_manual_rows(site_ols) -> None:
    site_ols["habitat"] = "Terrestrial"
    site_prop(1, [term("biome", "ENVO:01000174"), term("feature", "ENVO:00000023")])
    site_prop(2, [term("biome", "ENVO:01000174", "Forest Biome")])
    res = validate.validate_site(SITE)
    assert set(res["items"]) == {"biome"}  # a terrestrial site has no feature
    assert res["items"]["biome"]["status"] == "curated"
    site_prop(2, [term("biome", "ENVO:03605008")])  # an aquatic biome at a terrestrial site
    res = validate.validate_site(SITE)
    assert res["items"]["biome"]["choices"][1]["reason"] == "aquatic_biome_at_terrestrial_site"
    with ontology_store.store() as con:  # a row set by hand (RUNBOOK) is never replaced
        con.execute("UPDATE ref_site_envo SET envoCurie = 'ENVO:01000193', method = 'curated', status = 'ok'")
    site_prop(2, [term("biome", "ENVO:01000174")])
    res = validate.validate_site(SITE)
    assert res["written"]["biome"] == {"kept": "manual", "curie": "ENVO:01000193"}
    assert refs()[0] == [("ENVO:01000193", "curated", "ok")]


@pytest.mark.parametrize(("t", "aquatic", "reason"), [
    (term("biome", "PATO:0000146"), True, "not_envo"),
    (term("biome", "ENVO:00000023"), True, "not_a_biome"),
    (term("feature", "ENVO:03605008"), True, "biome_as_feature"),
    (term("feature", "ENVO:00002005"), True, "not_a_water_body"),
    (term("feature", "ENVO:00000023"), False, "feature_for_terrestrial_site"),
    (term("biome", "ENVO:03605008", "stream biome"), True, "label_mismatch"),
    (term("biome", "ENVO:99999999"), True, "ols_unavailable"),
    ({"role": "biome", "label": "x"}, True, "no_curie"),
])
def test_check_site_term_reasons(t, aquatic, reason) -> None:
    terms = {c: {"curie": c, "label": lb, "isObsolete": False} for c, lb in LABELS.items()}
    facts = {c: {"isBiome": BIOME in a, "ancestors": a} for c, a in ANCESTORS.items()}
    ch = validate.check_site_term(1, t["role"], t, set(), aquatic, terms, facts)
    assert (ch.valid, ch.reason) == (False, reason)


def test_site_incomplete_and_none(site_ols) -> None:
    assert validate.validate_site(SITE)["status"] == "none"
    assert validate.main(SITE, site_level=True) == 1
    site_prop(1, [term("biome", "ENVO:03605008")])
    assert validate.validate_site(SITE)["status"] == "incomplete"
    assert validate.main(SITE, site_level=True) == 2
    assert refs()[0] == [("ENVO:03605008", "heuristic", "proposed")]  # nothing written
