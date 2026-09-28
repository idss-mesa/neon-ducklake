"""site/curate.py: stream parsing, the scrubbed claude child (a fake CLI), replicates, retry, skips, units pass."""

from __future__ import annotations

import json
import shutil
import stat
import sys
from pathlib import Path

import pytest

from neon_ducklake import env
from neon_ducklake.site import curate, ontology_store

from .test_site_common import SITE, site_env  # noqa: F401 - fixture

REPO_CURATION = Path(__file__).resolve().parents[1] / "curation"
ALLOWED = [f"mcp__mesa-ols__{t}" for t in curate.PRODUCT_TOOLS]
REAL_UNMAPPED = curate.unmapped_units

FAKE_CLAUDE = r'''
import json, os, sys
from pathlib import Path

here = Path(__file__).resolve().parent
cfg = json.loads((here / "fake_config.json").read_text())
argv = sys.argv[1:]
stdin = sys.stdin.read()
resume = argv[argv.index("--resume") + 1] if "--resume" in argv else None
product = next((w.strip("#* ") for w in stdin.split() if w.strip("#* ").startswith("DP")), "units")
if resume:
    product = resume.split("-")[1]
with (here / "calls.jsonl").open("a") as log:
    log.write(json.dumps({"argv": argv, "env": sorted(os.environ), "cwd": os.getcwd(), "product": product,
                          "resume": resume, "units": "NEON UNITS" in stdin}) + "\n")

def emit(ev):
    print(json.dumps(ev), flush=True)

if product in cfg.get("garbage", []):
    print("not json at all", flush=True)
    sys.exit(1)

sid = resume or f"sess-{product}-{os.getpid()}"
emit({"type": "system", "subtype": "init", "session_id": sid, "model": "claude-opus-5-5",
      "claude_code_version": "0.0-fake", "mcp_servers": [{"name": "mesa-ols", "status": "connected"}],
      "tools": ["mcp__mesa-ols__mesa_avu_from_term", "mcp__mesa-ols__mesa_ols_get_term",
                "mcp__mesa-ols__mesa_ols_search_terms"]})
usage = {"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 100,
         "cache_creation_input_tokens": 50, "output_tokens_details": {"thinking_tokens": 2}}
if "NEON UNITS" in stdin and not resume:
    emit({"type": "assistant", "message": {"content": [
        {"type": "tool_use", "id": "u1", "name": "mcp__mesa-ols__mesa_ols_search_terms",
         "input": {"query": "degree Celsius squared", "ontology_id": "uo"}}]}})
    emit({"type": "user", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "u1", "content": [{"type": "text", "text": json.dumps(
            {"results": [{"curie": "UO:0000027", "label": "degree Celsius",
                          "iri": "http://purl.obolibrary.org/obo/UO_0000027"}]})}]}]}})
    body = {"units": [{"neonUnit": "fooUnit", "curie": "UO:0000027", "label": "degree Celsius",
                       "rationale": "same"}]}
    emit({"type": "result", "subtype": "success", "is_error": False, "session_id": sid,
          "result": json.dumps(body), "usage": usage, "total_cost_usd": 0.1, "num_turns": 2, "duration_ms": 5})
    sys.exit(0)
if not resume:
    emit({"type": "assistant", "message": {"content": [
        {"type": "text", "text": "searching"},
        {"type": "tool_use", "id": "t1", "name": "mcp__mesa-ols__mesa_ols_search_terms",
         "input": {"query": "temperature", "ontology_id": "pato", "size": 5}},
        {"type": "tool_use", "id": "t2", "name": "mcp__mesa-ols__mesa_avu_from_term",
         "input": {"ontology_id": "envo", "value": "x", "curie": "ENVO:99999999"}}]}})
    emit({"type": "user", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "t1", "content": [{"type": "text", "text": json.dumps(
            {"results": [{"curie": "PATO:0000146", "label": "temperature",
                          "iri": "http://purl.obolibrary.org/obo/PATO_0000146"},
                         {"label": "air", "iri": "http://purl.obolibrary.org/obo/ENVO_00002005"}]})}]},
        {"type": "tool_result", "tool_use_id": "t2", "content": [{"type": "text", "text": json.dumps(
            {"avu": {"attribute": "envo.x", "value": "x", "unit": "ENVO:99999999"},
             "term": {"curie": "ENVO:99999999"}})}]}]}})
avus = {"avus": [{"attribute": "pato.temperature", "value": "temperature", "unit": "PATO:0000146",
                  "ontology_id": "pato", "curie": "PATO:0000146", "label": "temperature",
                  "aspect": "measured_property", "column": None, "rationale": "r"}]}
if product in cfg.get("bad_first", []) and not resume:
    text = "Here are the AVUs I found, in prose only."
else:
    text = "```json\n" + json.dumps(avus) + "\n```"
emit({"type": "result", "subtype": "success", "is_error": False, "session_id": sid, "result": text,
      "usage": usage, "total_cost_usd": 0.25, "num_turns": 3, "duration_ms": 10,
      "permission_denials": []})
'''


@pytest.fixture
def fake_claude(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:  # noqa: F811
    """Copy the real prompts/config into the tmp root and install a fake ``claude`` executable."""
    shutil.copytree(REPO_CURATION, env.ROOT / "curation", ignore=shutil.ignore_patterns("generic"))
    (env.ROOT / "curation" / "generic").mkdir()
    bindir = site_env / "bin"
    bindir.mkdir()
    script = bindir / "claude"
    script.write_text(f"#!{sys.executable}\n{FAKE_CLAUDE}")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    (bindir / "fake_config.json").write_text(json.dumps({}))
    monkeypatch.setattr(curate, "CLAUDE_BIN", script)
    monkeypatch.setenv("NEON_TOKEN", "must-not-leak")
    monkeypatch.setenv("MESA_MCP_IRODS__PASSWORD", "must-not-leak")
    monkeypatch.setenv("MESA_MCP_DUCKLAKE__CATALOG_DSN", "duckdb:///nope")
    monkeypatch.setattr(curate, "unmapped_units", list)
    return bindir


def write_card(code: str, extra: str = "") -> Path:
    p = curate.curation_paths(SITE).card(code)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f"# {code} Some product\n\nColumns: tempSingleMean\n{extra}")
    return p


def calls(bindir: Path) -> list[dict]:
    f = bindir / "calls.jsonl"
    return [json.loads(line) for line in f.read_text().splitlines()] if f.exists() else []


# ---------------------------------------------------------------- pure helpers

def test_curies_in_json_iri_and_text() -> None:
    content = [{"type": "text", "text": json.dumps({"results": [
        {"curie": "pato:0000146"}, {"iri": "http://purl.obolibrary.org/obo/ENVO_00002005"},
        {"obo_id": "NCBITaxon:8782"}]})}]
    assert curate.curies_in(content) == {"PATO:0000146", "ENVO:00002005", "NCBITaxon:8782"}
    text = 'partial "curie": "OBI:0000070", see http://purl.obolibrary.org/obo/IAO_0000027 ok'
    assert curate.curies_in(text) == {"OBI:0000070", "IAO:0000027"}


def test_extract_json_variants() -> None:
    body = {"avus": [{"curie": "PATO:1"}]}
    assert curate.extract_json(json.dumps(body), "avus") == body
    assert curate.extract_json("```json\n" + json.dumps(body) + "\n```", "avus") == body
    assert curate.extract_json("Sure! " + json.dumps(body) + " done", "avus") == body
    assert curate.extract_json('{"other": 1} then ' + json.dumps(body), "avus") == body
    assert curate.extract_json("no json", "avus") is None
    assert curate.extract_json('{"avus": "x"}', "avus") is None
    assert curate.extract_json(None, "avus") is None


def test_parse_stream_counts_grounding_and_usage() -> None:
    evs = [
        {"type": "system", "subtype": "init", "session_id": "s1", "model": "m", "tools": ["b", "a"]},
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "1", "name": "mcp__mesa-ols__mesa_ols_search_terms", "input": {}},
            {"type": "tool_use", "id": "2", "name": "mcp__mesa-ols__mesa_avu_from_term", "input": {}},
            {"type": "tool_use", "id": "3", "name": "mcp__mesa-ols__mesa_ols_get_term", "input": {}}]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "1", "content": '{"results": [{"curie": "PATO:0000146"}]}'},
            {"type": "tool_result", "tool_use_id": "2", "content": '{"avu": {"unit": "ENVO:1"}, "term": {"curie": "ENVO:1"}}'},
            {"type": "tool_result", "tool_use_id": "3", "is_error": True, "content": '{"curie": "OBI:1"}'}]}},
        {"type": "result", "session_id": "s1", "result": "x", "subtype": "success", "num_turns": 2,
         "usage": {"input_tokens": 3, "output_tokens": 4}, "total_cost_usd": 0.5},
        {"type": "result", "session_id": "s1", "result": "y", "subtype": "success", "num_turns": 1,
         "usage": {"input_tokens": 1, "output_tokens": 1}, "total_cost_usd": 0.25},
    ]
    s = curate.parse_stream([json.dumps(e) for e in evs] + ["not json", ""])
    assert s.session_id == "s1" and s.tools_visible == ["a", "b"]
    assert s.tool_call_counts == {"mesa_ols_search_terms": 1, "mesa_avu_from_term": 1, "mesa_ols_get_term": 1}
    assert s.seen_curies == {"PATO:0000146"}  # avu_from_term echoes and errors do not ground
    assert s.result_text == "y" and s.num_turns == 3 and s.n_results == 2
    assert s.usage == {"input_tokens": 4, "output_tokens": 5} and s.total_cost_usd == pytest.approx(0.75)


def test_argv_and_child_env(site_env: Path) -> None:  # noqa: F811
    argv = curate.claude_argv(curate.PRODUCT_TOOLS)
    assert argv[argv.index("--model") + 1] == "claude-opus-5-5"
    assert "--strict-mcp-config" in argv and argv[argv.index("--tools") + 1] == ""
    assert argv[argv.index("--mcp-config") + 1] == str(env.ROOT / "curation" / "mcp-mesa-ols.json")
    assert argv[argv.index("--allowedTools") + 1].split(",") == ALLOWED
    hidden = argv[argv.index("--disallowedTools") + 1].split(",")
    assert "mcp__mesa-ols__ds_add_avu" in hidden and not set(hidden) & set(ALLOWED)
    assert argv[argv.index("--permission-mode") + 1] == "dontAsk"
    assert argv[argv.index("--output-format") + 1] == "stream-json" and "--verbose" in argv
    assert argv[argv.index("--max-turns") + 1] == "40"
    units = curate.claude_argv(curate.UNIT_TOOLS, resume="abc", max_turns=5)
    assert "mcp__mesa-ols__mesa_avu_from_term" in units[units.index("--disallowedTools") + 1]
    assert units[units.index("--resume") + 1] == "abc"
    src = {"HOME": "/h", "PATH": "/p", "NEON_TOKEN": "t", "MESA_MCP_IRODS__PASSWORD": "pw",
           "DBUS_SESSION_BUS_ADDRESS": "unix:x", "ANTHROPIC_API_KEY": "k"}
    assert curate.child_env(src) == {"HOME": "/h", "PATH": "/p", "DBUS_SESSION_BUS_ADDRESS": "unix:x"}


def test_mcp_config_is_scrubbed() -> None:
    cfg = json.loads((REPO_CURATION / "mcp-mesa-ols.json").read_text())
    srv = cfg["mcpServers"]["mesa-ols"]
    assert list(cfg["mcpServers"]) == ["mesa-ols"]
    assert srv["command"] == "/home/tswetnam/.mesa/.venv/bin/mesa-mcp" and srv["args"] == ["--transport", "stdio"]
    assert srv["env"]["MESA_MCP_DUCKLAKE__CATALOG_DSN"] == "" and srv["env"]["MESA_MCP_IRODS__USER"] == "anonymous"
    assert not any("PASSWORD" in k or "TOKEN" in k for k in srv["env"])


def test_prompts_carry_the_contract() -> None:
    product = (REPO_CURATION / "prompt_product.md").read_text()
    for needle in ("mesa_ols_search_terms", "mesa_avu_from_term", "OBO Foundry", "30 tool calls",
                   '{"avus": [{"attribute"', "DATASET CARD", "organism_group", "UO"):
        assert needle in product
    assert product.rstrip().endswith("DATASET CARD")
    units = (REPO_CURATION / "prompt_units.md").read_text()
    assert '{"units": [{"neonUnit"' in units and units.rstrip().endswith("NEON UNITS")


# ---------------------------------------------------------------- main with the fake CLI

def test_main_runs_replicates_scrubbed_and_skips(fake_claude: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_card("DP1.00002.001")
    write_card("DP1.10003.001")
    write_card("DP1.00033.001")  # external-only (built-in list; no site lake here)
    write_card("DP1.00004.001")
    env.write_json(curate.generic_path("DP1.00004.001"), {"done": True})
    (fake_claude / "fake_config.json").write_text(json.dumps({"bad_first": ["DP1.10003.001"]}))

    assert curate.main(SITE) == 0
    p = curate.curation_paths(SITE)
    got = calls(fake_claude)
    assert {c["product"] for c in got} == {"DP1.00002.001", "DP1.10003.001"}
    assert len([c for c in got if not c["resume"]]) == 4  # 2 products x 2 replicates
    assert len([c for c in got if c["resume"]]) == 2  # the repair resumes for the non-JSON product
    for c in got:
        assert set(c["env"]) <= {"HOME", "PATH", "DBUS_SESSION_BUS_ADDRESS", "PWD", "SHLVL", "_", "LC_CTYPE"}
        assert "NEON_TOKEN" not in c["env"] and "MESA_MCP_IRODS__PASSWORD" not in c["env"]
        assert c["cwd"] == str(env.WORK_DIR / "curate-cwd")
        assert "--strict-mcp-config" in c["argv"]
    for code in ("DP1.00002.001", "DP1.10003.001"):
        for rep in (1, 2):
            prop = json.loads(p.proposal(code, rep).read_text())
            assert prop["productCode"] == code and prop["rep"] == rep and prop["parse_ok"]
            assert prop["avus"][0]["curie"] == "PATO:0000146"
            assert prop["seen_curies"] == ["ENVO:00002005", "PATO:0000146"]
            assert prop["tool_call_counts"] == {"mesa_ols_search_terms": 1, "mesa_avu_from_term": 1}
            assert prop["card_sha256"] == curate.sha256_bytes(p.card(code).read_bytes())
            assert prop["modelReported"] == "claude-opus-5-5" and prop["model"] == "claude-opus-5-5"
            assert prop["trace"] == f"sites/{SITE}/curation/traces/{code}.rep{rep}.jsonl"
            assert p.trace(code, rep).exists()
            assert prop["attempts"] == (2 if code == "DP1.10003.001" else 1)
    repaired = json.loads(p.proposal("DP1.10003.001", 1).read_text())
    assert repaired["total_cost_usd"] == pytest.approx(0.5) and repaired["usage"]["input_tokens"] == 20
    summary = json.loads((p.site_dir / "reports" / "curate_summary.json").read_text())
    assert summary["skipped"] == {"DP1.00033.001": "external-only",
                                  "DP1.00004.001": "already validated (curation/generic)"}
    assert summary["status"] == "ok" and len(summary["runs"]) == 4

    # a re-run starts no child; --force re-runs just the selected product
    n = len(got)
    assert curate.main(SITE) == 0 and len(calls(fake_claude)) == n
    assert curate.main(SITE, products=["DP1.00002.001"], force=True) == 0
    assert len(calls(fake_claude)) == n + 2


def test_failed_run_is_recorded_and_retried_later(fake_claude: Path) -> None:
    write_card("DP1.99999.001")
    (fake_claude / "fake_config.json").write_text(json.dumps({"garbage": ["DP1.99999.001"]}))
    assert curate.main(SITE) == 2
    p = curate.curation_paths(SITE)
    got = calls(fake_claude)
    assert len(got) == 4 and not any(c["resume"] for c in got)  # 2 reps x (run + one fresh retry)
    for rep in (1, 2):
        assert not p.proposal("DP1.99999.001", rep).exists()
        failed = json.loads(p.failed("DP1.99999.001", rep).read_text())
        assert failed["error"] == "no result event" and failed["attempts"] == 2
    (fake_claude / "fake_config.json").write_text(json.dumps({}))
    assert curate.main(SITE) == 0
    assert p.proposal("DP1.99999.001", 1).exists() and not p.failed("DP1.99999.001", 1).exists()


def test_units_pass_writes_curated_rows(fake_claude: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with ontology_store.store() as con:
        con.executemany("INSERT INTO ref_unit_map VALUES (?, ?, ?, ?, ?, ?)",
                        [["fooUnit", None, None, "unmapped", "unmapped", None],
                         ["celsius", "UO:0000027", "degree Celsius", "seed", "ok", None]])
    monkeypatch.setattr(curate, "unmapped_units", REAL_UNMAPPED)
    monkeypatch.setattr(ontology_store, "ols_terms", lambda curies, **kw: {
        c: {"curie": c, "label": "degree Celsius", "isObsolete": False} for c in curies})
    assert curate.main(SITE) == 0
    p = curate.curation_paths(SITE)
    key = curate.units_key(["fooUnit"])
    prop = json.loads(p.proposal(key).read_text())
    assert prop["units"][0]["neonUnit"] == "fooUnit" and prop["seen_curies"] == ["UO:0000027"]
    assert [c for c in calls(fake_claude) if c["units"]]
    with ontology_store.store(read_only=True) as con:
        rows = con.execute("SELECT neonUnit, uoCurie, uoLabel, method, status FROM ref_unit_map "
                           "ORDER BY 1").fetchall()
    assert rows == [("celsius", "UO:0000027", "degree Celsius", "seed", "ok"),
                    ("fooUnit", "UO:0000027", "degree Celsius", "curated", "ok")]
    validated = json.loads((p.proposals_dir / f"{key}.validated.json").read_text())
    assert validated["mapped"] == 1 and validated["unmappedAfter"] == []
    # nothing unmapped any more: no second units run
    n = len(calls(fake_claude))
    assert curate.main(SITE) == 0 and len(calls(fake_claude)) == n
