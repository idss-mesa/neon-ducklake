"""`site curate <SITE>` — product-level ontology AVUs proposed by Claude Opus, two replicates per product.

This is the neon-avu-eval protocol (Opus 5.5 was the best model there). Each run is a fresh
``claude -p`` child process:

* **Scrubbed environment.** The child gets only ``HOME``, ``PATH`` and ``DBUS_SESSION_BUS_ADDRESS``, as
  with ``env -i``. So it sees no NEON token, no iRODS password and no MESA catalog DSN.
* **One MCP server.** ``--strict-mcp-config`` loads only the config rendered from the template
  ``curation/mcp-mesa-ols.json`` into ``work/mcp-mesa-ols.json`` (``${MESA_HOME}`` filled in, default ``~/.mesa``).
  That config runs the MESA install's mesa-mcp directly, with anonymous iRODS and
  ``MESA_MCP_DUCKLAKE__CATALOG_DSN=''``.
* **Three tools.** Built-in tools are off (``--tools ""``). The three OLS tools are allowed, the rest of
  mesa-mcp's tools are hidden, and ``--permission-mode dontAsk`` denies anything else.
* **Kept outputs.** The full ``stream-json`` trace goes to ``sites/<SITE>/curation/traces/<code>.rep<N>.jsonl``.
  The final JSON goes to ``sites/<SITE>/curation/proposals/<code>.rep<N>.json``, with ``seen_curies``:
  every CURIE in the run's own search and get-term results, which is what groundedness is checked against.
* **Retry.** A reply that is not JSON gets one retry: the session is resumed with a repair prompt, or a
  fresh run is started when there is no session to resume.

Deciding whether terms are valid, and what is accepted, proposed or rejected, is :mod:`site.validate`'s job.

The units pass: when ``ref_unit_map`` has ``unmapped`` NEON units, one run of ``prompt_units.md`` proposes UO
terms. :func:`site.validate.apply_unit_proposal` checks them, and the ones that pass are written back with
``method='curated'``.

The site pass (docs/SCALING.md, "Site biome"): two replicates of ``prompt_site.md`` on the site card
``sites/<SITE>/cards/_site.md`` (written by ``site cards``) each choose one ENVO biome and, for an aquatic
site, one ENVO water-body feature. Same isolated child, tools ``mesa_ols_search_terms`` + ``mesa_ols_get_term``.
Traces and proposals go to ``sites/<SITE>/curation/site/`` (``_site.rep<N>.jsonl`` / ``.json``), apart from the
product proposals. :func:`site.validate.validate_site` applies the agreement rule.

Several VMs (docs/STREAMING.md, "Curation claims"): when ``coord.curation_sync_enabled()`` (workers), the
product pass first fetches ``<IRODS_ROOT>/curation/generic/<code>.validated.json`` for every card that has
one on the Data Store (a product curated by any VM is then skipped, and its result is imported into
``curation_avus``), then claims ``<IRODS_ROOT>/curation/claims/<code>.claim`` for each product it will
curate. A product claimed by another live VM is skipped. Claims are heartbeated while the replicates run,
released at once for a product whose run failed, and otherwise released by ``site validate`` after it
publishes the generic result.

``main(site, products=None, force=False, site_level=None)``:
- ``site_level=None`` (the default, and the ``site curate`` stage): the products as before, plus the site pass
  when no product filter is given, the site card exists and the site has no proposals yet;
- ``site_level=True``: only the site pass (``curate_site``); ``site_level=False``: only the products.

**Without the claude CLI** (:func:`curator_unavailable`; optional on DE worker VMs, docs/DE_SANDBOX.md) the
product pass still reuses what exists: the Data Store prefetch and import (with curation sync) and the import
of the local ``curation/generic`` results of the site's products. No Opus run starts and no curation claim is
taken; the products without a generic result are left uncurated (logged, and listed as ``uncurated`` in
``reports/curate_summary.json``, status ``no_curator``), the units and site passes are skipped, and ``main``
returns 0 so the site's finalize goes on (it still waits for products another VM is curating). The site pass
alone (``site_level=True``) returns 1, as does any pass with ``NEON_DUCKLAKE_CURATION_REQUIRE_CLI=1``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .. import env, state
from . import site_lake
from .ontology_store import norm_curie

log = logging.getLogger("neon_ducklake.site.curate")



def _default_claude() -> Path:
    """``~/.local/bin/claude`` (the native installer's location), else ``claude`` on PATH."""
    local = Path.home() / ".local" / "bin" / "claude"
    found = None if local.exists() else shutil.which("claude")
    return Path(found) if found else local


CLAUDE_BIN = Path(os.environ.get("NEON_DUCKLAKE_CLAUDE") or _default_claude()).expanduser()
REQUIRE_CLI_ENV = "NEON_DUCKLAKE_CURATION_REQUIRE_CLI"  # 1: a missing claude CLI fails `site curate` (rc 1)
NO_CURATOR = "no_curator"  # curate_summary.json status of a pass without the claude CLI (reuse only)
MODEL = "claude-opus-5-5"
REPLICATES = 2
PARALLEL = 4
MAX_TURNS = 40
REPAIR_TURNS = 10
RUN_TIMEOUT_S = 1800
CHILD_ENV_KEYS = ("HOME", "PATH", "DBUS_SESSION_BUS_ADDRESS")

SERVER = "mesa-ols"
PRODUCT_TOOLS = ("mesa_ols_search_terms", "mesa_ols_get_term", "mesa_avu_from_term")
UNIT_TOOLS = ("mesa_ols_search_terms", "mesa_ols_get_term")
# Groundedness: only these tools *retrieve* terms. mesa_avu_from_term echoes whatever CURIE it is given.
RETRIEVAL_TOOLS = frozenset({"mesa_ols_search_terms", "mesa_ols_get_term"})
# Every other mesa-mcp tool (registry as of mesa-mcp 8fbaedf, unchanged in 0.2.0 / 0e2e561). They are hidden from the model, and
# dontAsk denies anything not listed here that a newer mesa-mcp might add.
HIDDEN_TOOLS = (
    "ds_add_avu", "ds_add_avus", "ds_copy_file", "ds_create_ticket", "ds_delete_avu", "ds_delete_file",
    "ds_delete_ticket", "ds_directory_tree", "ds_download_file", "ds_execute_rule", "ds_get_file_info",
    "ds_get_metadata", "ds_get_policy_config", "ds_get_rule_definition", "ds_get_ticket_info",
    "ds_list_allowed_directories", "ds_list_avus", "ds_list_directory", "ds_list_directory_details",
    "ds_list_policies", "ds_list_rules", "ds_list_tickets", "ds_make_directory", "ds_modify_access",
    "ds_modify_access_inheritance", "ds_modify_ticket", "ds_move_file", "ds_ping", "ds_read_file",
    "ds_search_files", "ds_search_files_by_avu", "ds_search_metadata", "ds_upload_file", "ds_use_ticket",
    "ds_write_file", "mesa_avu_apply_datacite", "mesa_avu_apply_term", "mesa_datacite_export",
    "mesa_datacite_template", "mesa_datacite_validate", "mesa_ducklake_init_project",
    "mesa_ols_generate_template", "mesa_ols_get_ontology", "mesa_ols_get_term_hierarchy",
    "mesa_ols_list_ontologies", "mesa_policy_disable", "mesa_policy_enable",
)

PRODUCT_REPAIR = ("Your previous reply was not the required JSON object. Reply now with ONLY the JSON object "
                  '{"avus": [...]} described in the task, built from the AVUs you already made with '
                  "mesa_avu_from_term. No prose and no code fence.")
UNITS_REPAIR = ("Your previous reply was not the required JSON object. Reply now with ONLY the JSON object "
                '{"units": [...]} described in the task. No prose and no code fence.')
SITE_REPAIR = ("Your previous reply was not the required JSON object. Reply now with ONLY the JSON object "
               '{"terms": [...]} described in the task (one biome, plus one feature for an aquatic site). '
               "No prose and no code fence.")
SITE_KEY = "_site"
SITE_TOOLS = ("mesa_ols_search_terms", "mesa_ols_get_term")
SITE_MAX_TURNS = 20

_OBO_IRI = re.compile(r"/obo/([A-Za-z][A-Za-z0-9]*)_([A-Za-z0-9]+)\b")
_CURIE_FIELD = re.compile(r'"(?:curie|obo_id|oboId)"\s*:\s*"([A-Za-z][A-Za-z0-9]*:[A-Za-z0-9_]+)"')


# ---------------------------------------------------------------- paths

def curation_root() -> Path:
    """``<ROOT>/curation``: prompts, the MCP config and the generic per-product results."""
    return env.ROOT / "curation"


def generic_path(product: str) -> Path:
    return curation_root() / "generic" / f"{product}.validated.json"


@dataclass(frozen=True)
class CurationPaths:
    site: str
    site_dir: Path

    @property
    def cards_dir(self) -> Path:
        return self.site_dir / "cards"

    @property
    def curation_dir(self) -> Path:
        return self.site_dir / "curation"

    @property
    def traces_dir(self) -> Path:
        return self.curation_dir / "traces"

    @property
    def proposals_dir(self) -> Path:
        return self.curation_dir / "proposals"

    @property
    def review_file(self) -> Path:
        return self.curation_dir / "review.md"

    @property
    def run_cwd(self) -> Path:
        """An empty working directory for the child, so it finds no project CLAUDE.md or settings."""
        return env.WORK_DIR / "curate-cwd"

    @property
    def site_dir_curation(self) -> Path:
        """``curation/site/``: the site-level pass (kept apart from the product proposals)."""
        return self.curation_dir / "site"

    @property
    def site_card(self) -> Path:
        return self.cards_dir / f"{SITE_KEY}.md"

    def site_proposal(self, rep: int) -> Path:
        return self.site_dir_curation / f"{SITE_KEY}.rep{rep}.json"

    def site_failed(self, rep: int) -> Path:
        return self.site_dir_curation / f"{SITE_KEY}.rep{rep}.failed.json"

    def site_trace(self, rep: int) -> Path:
        return self.site_dir_curation / f"{SITE_KEY}.rep{rep}.jsonl"

    def card(self, product: str) -> Path:
        return self.cards_dir / f"{product}.md"

    def trace(self, key: str, rep: int | None = None) -> Path:
        return self.traces_dir / (f"{key}.rep{rep}.jsonl" if rep is not None else f"{key}.jsonl")

    def proposal(self, key: str, rep: int | None = None) -> Path:
        return self.proposals_dir / (f"{key}.rep{rep}.json" if rep is not None else f"{key}.json")

    def failed(self, key: str, rep: int | None = None) -> Path:
        return self.proposals_dir / (f"{key}.rep{rep}.failed.json" if rep is not None else f"{key}.failed.json")


def curation_paths(site: str) -> CurationPaths:
    site = site.upper()
    return CurationPaths(site=site, site_dir=env.SITES_DIR / site)


def rel(path: Path) -> str:
    """Path relative to the project root when it is inside it (for provenance fields)."""
    try:
        return str(path.relative_to(env.ROOT))
    except ValueError:
        return str(path)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- CURIEs

def curies_in(obj: Any) -> set[str]:
    """Every CURIE in a tool result: ``curie``/``obo_id`` fields and OBO PURLs, JSON or plain text."""
    out: set[str] = set()

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for k, v in x.items():
                if k in ("curie", "obo_id", "oboId") and isinstance(v, str):
                    c = norm_curie(v)
                    if c:
                        out.add(c)
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        elif isinstance(x, str):
            try:
                parsed = json.loads(x)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict | list):
                walk(parsed)
            else:
                for m in _CURIE_FIELD.finditer(x):
                    c = norm_curie(m.group(1))
                    if c:
                        out.add(c)
            for m in _OBO_IRI.finditer(x):
                c = norm_curie(f"{m.group(1)}:{m.group(2)}")
                if c:
                    out.add(c)

    walk(obj)
    return out


# ---------------------------------------------------------------- the claude child

def child_env(source: dict[str, str] | None = None) -> dict[str, str]:
    """``env -i HOME=… PATH=… DBUS_SESSION_BUS_ADDRESS=…``: nothing else crosses into the child."""
    src = os.environ if source is None else source
    return {k: src[k] for k in CHILD_ENV_KEYS if src.get(k)}


MCP_CONFIG = "mcp-mesa-ols.json"
MCP_PLACEHOLDERS = ("${MESA_HOME}",)


def mcp_template_path() -> Path:
    """The tracked template (``curation/mcp-mesa-ols.json``): no absolute paths, ``${MESA_HOME}`` placeholders."""
    return curation_root() / MCP_CONFIG


def mcp_config_path() -> Path:
    """The rendered config the child loads (``work/``, git-ignored); written by :func:`render_mcp_config`."""
    return env.WORK_DIR / MCP_CONFIG


def render_mcp_config() -> Path:
    """Fill ``${MESA_HOME}`` into the template, write :func:`mcp_config_path` and check the server command.

    Raises ``SystemExit`` with a hint when the template is missing, holds an unknown placeholder, or its
    command (the MESA install's ``mesa-mcp``) does not exist.
    """
    tpl = mcp_template_path()
    if not tpl.is_file():
        raise SystemExit(f"MCP config template missing: {tpl}")
    values = {"${MESA_HOME}": str(env.MESA_HOME)}

    def fill(v: Any) -> Any:
        if isinstance(v, str):
            for k, val in values.items():
                v = v.replace(k, val)
            return v
        if isinstance(v, list):
            return [fill(x) for x in v]
        if isinstance(v, dict):
            return {k: fill(x) for k, x in v.items()}
        return v

    cfg = fill(json.loads(tpl.read_text()))
    left = sorted(set(re.findall(r"\$\{[^}]*\}", json.dumps(cfg))))
    if left:
        raise SystemExit(f"{tpl}: unknown placeholder(s) {', '.join(left)} (known: {', '.join(MCP_PLACEHOLDERS)})")
    command = Path(cfg["mcpServers"][SERVER]["command"])
    if not (command.is_file() and os.access(command, os.X_OK)):
        raise SystemExit(f"mesa-mcp not found at {command}: it is part of the MESA local install "
                         f"(set MESA_HOME if that is not {env.MESA_HOME})")
    out = mcp_config_path()
    env.write_json(out, cfg)
    return out


def tool_names(tools: Iterable[str]) -> list[str]:
    return [f"mcp__{SERVER}__{t}" for t in tools]


def claude_argv(tools: Iterable[str], *, max_turns: int = MAX_TURNS, resume: str | None = None,
                model: str = MODEL) -> list[str]:
    tools = list(tools)
    hidden = list(dict.fromkeys(t for t in (*HIDDEN_TOOLS, *PRODUCT_TOOLS) if t not in tools))
    argv = [str(CLAUDE_BIN), "-p", "--model", model,
            "--strict-mcp-config", "--mcp-config", str(mcp_config_path()),
            "--tools", "",
            "--allowedTools", ",".join(tool_names(tools)),
            "--disallowedTools", ",".join(tool_names(hidden)),
            "--permission-mode", "dontAsk",
            "--disable-slash-commands",
            "--output-format", "stream-json", "--verbose",
            "--max-turns", str(max_turns)]
    if resume:
        argv += ["--resume", resume]
    return argv


@dataclass
class Attempt:
    returncode: int | None
    seconds: float
    stderr_tail: str
    timed_out: bool = False


def run_claude(prompt: str, trace: Path, tools: Iterable[str], cwd: Path, *, max_turns: int = MAX_TURNS,
               resume: str | None = None, append: bool = False, timeout_s: float = RUN_TIMEOUT_S) -> Attempt:
    """One ``claude -p`` child; its stdout (stream-json) goes straight into ``trace``."""
    trace.parent.mkdir(parents=True, exist_ok=True)
    cwd.mkdir(parents=True, exist_ok=True)
    argv = claude_argv(tools, max_turns=max_turns, resume=resume)
    t0 = time.monotonic()
    with trace.open("ab" if append else "wb") as out:
        try:
            proc = subprocess.run(argv, input=prompt.encode("utf-8"), stdout=out, stderr=subprocess.PIPE,
                                  cwd=cwd, env=child_env(), timeout=timeout_s, check=False)
        except subprocess.TimeoutExpired as exc:
            tail = (exc.stderr or b"")[-2000:].decode("utf-8", "replace")
            return Attempt(None, time.monotonic() - t0, tail, timed_out=True)
    return Attempt(proc.returncode, time.monotonic() - t0, proc.stderr[-2000:].decode("utf-8", "replace"))


# ---------------------------------------------------------------- stream-json parsing

@dataclass
class StreamSummary:
    session_id: str | None = None
    model: str | None = None
    claude_version: str | None = None
    tools_visible: list[str] = field(default_factory=list)
    mcp_servers: list[dict[str, Any]] = field(default_factory=list)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    seen_curies: set[str] = field(default_factory=set)
    result_text: str | None = None
    subtype: str | None = None
    is_error: bool | None = None
    num_turns: int = 0
    usage: dict[str, Any] = field(default_factory=dict)
    total_cost_usd: float = 0.0
    duration_ms: int = 0
    permission_denials: list[Any] = field(default_factory=list)
    n_results: int = 0

    @property
    def tool_call_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for c in self.tool_calls:
            counts[c["tool"]] = counts.get(c["tool"], 0) + 1
        return counts


def short_tool(name: str) -> str:
    prefix = f"mcp__{SERVER}__"
    return name.removeprefix(prefix)


def _add_usage(total: dict[str, Any], usage: dict[str, Any]) -> None:
    for k in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
        if isinstance(usage.get(k), int):
            total[k] = total.get(k, 0) + usage[k]
    thinking = (usage.get("output_tokens_details") or {}).get("thinking_tokens")
    if isinstance(thinking, int):
        total["thinking_tokens"] = total.get("thinking_tokens", 0) + thinking


def parse_stream(lines: Iterable[str]) -> StreamSummary:
    """Summarise a ``stream-json`` trace (several resumed invocations may be appended in one file)."""
    s = StreamSummary()
    pending: dict[str, str] = {}  # tool_use id -> short tool name
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        typ = ev.get("type")
        if typ == "system" and ev.get("subtype") == "init":
            s.session_id = ev.get("session_id") or s.session_id
            s.model = ev.get("model") or s.model
            s.claude_version = ev.get("claude_code_version") or s.claude_version
            s.tools_visible = sorted(ev.get("tools") or [])
            s.mcp_servers = ev.get("mcp_servers") or []
        elif typ == "assistant":
            for block in (ev.get("message") or {}).get("content") or []:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    tool = short_tool(block.get("name") or "")
                    pending[block.get("id") or ""] = tool
                    s.tool_calls.append({"tool": tool, "input": block.get("input")})
        elif typ == "user":
            content = (ev.get("message") or {}).get("content")
            for block in content if isinstance(content, list) else []:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    tool = pending.get(block.get("tool_use_id") or "")
                    if tool in RETRIEVAL_TOOLS and not block.get("is_error"):
                        s.seen_curies |= curies_in(block.get("content"))
        elif typ == "result":
            s.n_results += 1
            s.session_id = ev.get("session_id") or s.session_id
            s.result_text = ev.get("result") if isinstance(ev.get("result"), str) else s.result_text
            s.subtype = ev.get("subtype")
            s.is_error = bool(ev.get("is_error"))
            s.num_turns += int(ev.get("num_turns") or 0)
            s.duration_ms += int(ev.get("duration_ms") or 0)
            s.total_cost_usd += float(ev.get("total_cost_usd") or 0.0)
            _add_usage(s.usage, ev.get("usage") or {})
            s.permission_denials += ev.get("permission_denials") or []
    return s


def extract_json(text: str | None, key: str) -> dict[str, Any] | None:
    """The first JSON object in ``text`` that has a list under ``key`` (a code fence or prose around it is ok)."""
    if not text:
        return None
    t = text.strip()
    fence = re.match(r"^```[a-zA-Z]*\s*(.*?)\s*```$", t, re.DOTALL)
    if fence:
        t = fence.group(1)
    dec = json.JSONDecoder()
    for start in [0] + [m.start() for m in re.finditer(r"\{", t)]:
        try:
            obj, _ = dec.raw_decode(t[start:])
        except ValueError:
            continue
        if isinstance(obj, dict) and isinstance(obj.get(key), list):
            return obj
    return None


# ---------------------------------------------------------------- one replicate

@dataclass
class Job:
    kind: str  # 'product' | 'units' | 'site'
    key: str  # product code, 'units.<sha12>' or '_site'
    rep: int | None
    prompt: str
    prompt_sha256: str
    input_sha256: str  # card sha (product, site) or the unit list sha (units)
    trace: Path
    proposal: Path
    failed: Path
    tools: tuple[str, ...]
    result_key: str  # 'avus' | 'units' | 'terms'
    repair: str
    extra: dict[str, Any] = field(default_factory=dict)
    max_turns: int = MAX_TURNS

    @property
    def label(self) -> str:
        return self.key if self.rep is None else f"{self.key} rep{self.rep}"


def run_job(job: Job, cwd: Path) -> dict[str, Any]:
    """Run one replicate (plus at most one retry) and write its proposal, or a ``.failed.json``."""
    t0 = time.monotonic()
    attempts = [run_claude(job.prompt, job.trace, job.tools, cwd, max_turns=job.max_turns)]
    summary = parse_stream(job.trace.read_text("utf-8", "replace").splitlines())
    parsed = extract_json(summary.result_text, job.result_key)
    if parsed is None:
        if summary.session_id:
            log.warning("%s: reply was not JSON (%s); resuming once to repair", job.label,
                        summary.subtype)
            attempts.append(run_claude(job.repair, job.trace, job.tools, cwd, max_turns=REPAIR_TURNS,
                                       resume=summary.session_id, append=True))
        else:
            log.warning("%s: no session (rc=%s); retrying once from scratch", job.label,
                        attempts[0].returncode)
            attempts.append(run_claude(job.prompt, job.trace, job.tools, cwd, max_turns=job.max_turns, append=True))
        summary = parse_stream(job.trace.read_text("utf-8", "replace").splitlines())
        parsed = extract_json(summary.result_text, job.result_key)
    seconds = round(time.monotonic() - t0, 1)
    record: dict[str, Any] = {
        "kind": job.kind, "key": job.key, "rep": job.rep, **job.extra,
        "model": MODEL, "modelReported": summary.model, "claudeVersion": summary.claude_version,
        "prompt_sha256": job.prompt_sha256, "input_sha256": job.input_sha256,
        "seconds": seconds, "attempts": len(attempts),
        "returncodes": [a.returncode for a in attempts], "timedOut": any(a.timed_out for a in attempts),
        "usage": summary.usage, "total_cost_usd": round(summary.total_cost_usd, 4),
        "num_turns": summary.num_turns, "tool_calls": len(summary.tool_calls),
        "tool_call_counts": summary.tool_call_counts, "seen_curies": sorted(summary.seen_curies),
        "tools_visible": summary.tools_visible, "mcp_servers": summary.mcp_servers,
        "permission_denials": summary.permission_denials, "stop": summary.subtype,
        "session_id": summary.session_id, "trace": rel(job.trace), "createdAt": env.utcnow().isoformat(),
    }
    extra_tools = [t for t in summary.tools_visible if short_tool(t) not in job.tools]
    if extra_tools:
        log.warning("%s: tools outside the allowed set were visible: %s", job.label, extra_tools)
        record["extra_tools_visible"] = extra_tools
    if parsed is None:
        record["error"] = "no JSON reply" if summary.result_text is not None else "no result event"
        record["result_tail"] = (summary.result_text or "")[-500:]
        record["stderr_tail"] = attempts[-1].stderr_tail[-500:]
        env.write_json(job.failed, record)
        log.error("%s failed after %d attempt(s): %s (see %s)", job.label, len(attempts),
                  record["error"], rel(job.failed))
        return record
    record[job.result_key] = parsed[job.result_key]
    record["parse_ok"] = True
    env.write_json(job.proposal, record)
    job.failed.unlink(missing_ok=True)
    log.info("%s: %d %s, %d tool calls %s, %.0fs, $%.2f", job.label, len(parsed[job.result_key]),
             job.result_key, len(summary.tool_calls), summary.tool_call_counts, seconds, summary.total_cost_usd)
    return record


# ---------------------------------------------------------------- product selection

def read_prompt(name: str) -> tuple[str, str]:
    path = curation_root() / name
    text = path.read_text("utf-8")
    return text, sha256_bytes(text.encode("utf-8"))


def external_only_products(site: str) -> set[str]:
    """Products whose every site-month is external (no NEON files), from the site lake when readable."""
    from .plan import EXTERNAL_PRODUCTS

    out = set(EXTERNAL_PRODUCTS)
    try:
        con = site_lake.open_site_lake(site, read_only=True)
    except Exception as exc:  # noqa: BLE001 - missing lake or a writer holding it
        log.info("site lake not readable (%s); using the built-in external product list",
                 str(exc).splitlines()[0][:120])
        return out
    try:
        if site_lake.table_exists(con, "site_months"):
            rows = con.execute(f"SELECT productCode, bool_and(coalesce(isExternal, false)) "
                               f"FROM {site_lake.ALIAS}.site_months GROUP BY 1").fetchall()
            out |= {r[0] for r in rows if r[1]}
    finally:
        con.close()
    return out


def select_products(site: str, products: list[str] | None, force: bool) -> tuple[list[str], dict[str, str]]:
    """(product codes to curate, {skipped code: reason}) from the site's cards."""
    p = curation_paths(site)
    cards = sorted(c.stem for c in p.cards_dir.glob("*.md") if not c.name.startswith("_")) \
        if p.cards_dir.exists() else []
    if products:
        missing = [c for c in products if c not in cards]
        for c in missing:
            log.warning("%s: no card %s", c, rel(p.card(c)))
        cards = [c for c in cards if c in products]
    external = external_only_products(site)
    todo: list[str] = []
    skipped: dict[str, str] = {}
    for code in cards:
        if code in external:
            skipped[code] = "external-only"
        elif generic_path(code).exists() and not force:
            skipped[code] = "already validated (curation/generic)"
        else:
            todo.append(code)
    return todo, skipped


def datastore_prefetch(site: str, products: list[str] | None) -> dict[str, str]:
    """Fetch the Data Store's generic results for this site's cards; import new ones. ``{code: status}``."""
    from .. import coord
    from . import validate

    p = curation_paths(site)
    cards = sorted(c.stem for c in p.cards_dir.glob("*.md") if not c.name.startswith("_")) \
        if p.cards_dir.exists() else []
    if products:
        cards = [c for c in cards if c in products]
    out: dict[str, str] = {}
    try:
        session = coord.get_session()
        for code in cards:
            out[code] = coord.fetch_generic(code, generic_path(code), session=session)
    except (SystemExit, Exception) as exc:  # noqa: BLE001 - curate on without the shared results
        log.warning("Data Store generic curation not fetched: %s", exc)
        return out
    fetched = [c for c, st in out.items() if st == "fetched"]
    if fetched:
        log.info("fetched %d generic curation result(s) from the Data Store: %s", len(fetched), ", ".join(fetched))
        try:
            validate.import_generic(fetched)
        except Exception as exc:  # noqa: BLE001 - the files are in place; `site validate` can redo it
            log.warning("importing fetched generic curation failed: %s", exc)
    return out


def datastore_claim(codes: list[str], skipped: dict[str, str]) -> tuple[list[str], list[Any]]:
    """Claim each product on the Data Store; returns (codes to curate, acquired claims). Claimed-elsewhere
    products move to ``skipped``. Without a Data Store session every product is curated (unclaimed)."""
    from .. import coord

    try:
        session = coord.get_session()
    except (SystemExit, Exception) as exc:  # noqa: BLE001
        log.warning("curation claims unavailable (%s); curating without them", exc)
        return codes, []
    todo: list[str] = []
    claims: list[Any] = []
    for code in codes:
        try:
            res = coord.claim_curation(code, session=session)
        except Exception as exc:  # noqa: BLE001 - a duplicate curation only costs a run
            log.warning("%s: curation claim failed (%s); curating anyway", code, exc)
            todo.append(code)
            continue
        if res.acquired:
            todo.append(code)
            claims.append(res.claim)
        else:
            who = res.claim.vm if res.claim is not None and res.claim.valid else "another VM"
            skipped[code] = f"curation claimed by {who} on the Data Store"
    return todo, claims


def product_jobs(site: str, codes: list[str], force: bool) -> tuple[list[Job], list[str]]:
    p = curation_paths(site)
    template, prompt_sha = read_prompt("prompt_product.md")
    jobs: list[Job] = []
    existing: list[str] = []
    for code in codes:
        card = p.card(code).read_bytes()
        prompt = template.rstrip("\n") + "\n\n" + card.decode("utf-8", "replace")
        for rep in range(1, REPLICATES + 1):
            if p.proposal(code, rep).exists() and not force:
                existing.append(f"{code}.rep{rep}")
                continue
            jobs.append(Job(kind="product", key=code, rep=rep, prompt=prompt, prompt_sha256=prompt_sha,
                            input_sha256=sha256_bytes(card), trace=p.trace(code, rep),
                            proposal=p.proposal(code, rep), failed=p.failed(code, rep), tools=PRODUCT_TOOLS,
                            result_key="avus", repair=PRODUCT_REPAIR,
                            extra={"productCode": code, "site": site, "card": rel(p.card(code)),
                                   "card_sha256": sha256_bytes(card)}))
    return jobs, existing


# ---------------------------------------------------------------- units pass

def unmapped_units() -> list[str]:
    """NEON unit strings that ``ref_unit_map`` marks unmapped ([] when the store or table is absent)."""
    from . import ontology_store

    try:
        con = ontology_store.open_store(read_only=True)
    except Exception as exc:  # noqa: BLE001
        log.warning("units pass: ontology store not readable (%s); skipped", str(exc).splitlines()[0][:160])
        return []
    try:
        have = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
        if "ref_unit_map" not in have:
            return []
        rows = con.execute("SELECT neonUnit FROM ref_unit_map WHERE status = 'unmapped' "
                           "ORDER BY neonUnit").fetchall()
    finally:
        con.close()
    return [r[0] for r in rows if r[0]]


def unit_context(site: str, units: list[str]) -> dict[str, str]:
    """One example ``table.field: description`` per unit from the site lake's ``variables`` (best effort)."""
    try:
        con = site_lake.open_site_lake(site, read_only=True)
    except Exception:  # noqa: BLE001
        return {}
    try:
        if not site_lake.table_exists(con, "variables"):
            return {}
        rows = con.execute(
            f"SELECT units, any_value(tableName || '.' || fieldName || ': ' || coalesce(description, '')) "
            f"FROM {site_lake.ALIAS}.variables WHERE units IN (SELECT unnest(?::VARCHAR[])) GROUP BY 1",
            [units]).fetchall()
    finally:
        con.close()
    return {r[0]: r[1][:160] for r in rows}


def units_key(units: list[str]) -> str:
    """``units.<sha12>`` of the sorted unit list: a new list of unmapped units gets a new run."""
    return f"units.{sha256_bytes(chr(10).join(sorted(units)).encode('utf-8'))[:12]}"


def product_units(site: str, products: list[str]) -> set[str] | None:
    """Units used by these products' ``variables`` in the site lake (None when the lake is unreadable)."""
    try:
        con = site_lake.open_site_lake(site, read_only=True)
    except Exception:  # noqa: BLE001
        return None
    try:
        if not site_lake.table_exists(con, "variables"):
            return set()
        rows = con.execute(f"SELECT DISTINCT units FROM {site_lake.ALIAS}.variables WHERE units IS NOT NULL "
                           f"AND productCode IN (SELECT unnest(?::VARCHAR[]))", [products]).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows}


def units_job(site: str, units: list[str], force: bool) -> Job | None:
    if not units:
        return None
    p = curation_paths(site)
    template, prompt_sha = read_prompt("prompt_units.md")
    units = sorted(units)
    listing = "\n".join(units)
    key = units_key(units)
    if p.proposal(key).exists() and not force:
        return None
    ctx = unit_context(site, units)
    lines = [f"- {u}" + (f"   (for example {ctx[u]})" if u in ctx else "") for u in units]
    prompt = template.rstrip("\n") + "\n\n" + "\n".join(lines) + "\n"
    return Job(kind="units", key=key, rep=None, prompt=prompt, prompt_sha256=prompt_sha,
               input_sha256=sha256_bytes(listing.encode("utf-8")), trace=p.trace(key), proposal=p.proposal(key),
               failed=p.failed(key), tools=UNIT_TOOLS, result_key="units", repair=UNITS_REPAIR,
               extra={"site": site, "neonUnits": units})


def apply_units(site: str, units: list[str]) -> dict[str, Any]:
    """Validate the units proposal for this exact unit list and write passing rows to ``ref_unit_map``."""
    if not units:
        return {"units": 0}
    p = curation_paths(site)
    key = units_key(units)
    if not p.proposal(key).exists():
        return {"units": len(units), "proposal": None}
    from . import validate

    proposal = json.loads(p.proposal(key).read_text("utf-8"))
    return validate.apply_unit_proposal(site, proposal, set(units), p.proposals_dir / f"{key}.validated.json")


# ---------------------------------------------------------------- site pass

def site_jobs(site: str, force: bool) -> tuple[list[Job], list[str]]:
    """The two replicate jobs of the site pass ([] when the card is missing or proposals exist)."""
    p = curation_paths(site)
    if not p.site_card.exists():
        log.warning("site pass %s: no site card %s (run `site cards %s` first)", site, rel(p.site_card), site)
        return [], []
    template, prompt_sha = read_prompt("prompt_site.md")
    card = p.site_card.read_bytes()
    prompt = template.rstrip("\n") + "\n\n" + card.decode("utf-8", "replace")
    jobs: list[Job] = []
    existing: list[str] = []
    for rep in range(1, REPLICATES + 1):
        if p.site_proposal(rep).exists() and not force:
            existing.append(f"{SITE_KEY}.rep{rep}")
            continue
        jobs.append(Job(kind="site", key=SITE_KEY, rep=rep, prompt=prompt, prompt_sha256=prompt_sha,
                        input_sha256=sha256_bytes(card), trace=p.site_trace(rep), proposal=p.site_proposal(rep),
                        failed=p.site_failed(rep), tools=SITE_TOOLS, result_key="terms", repair=SITE_REPAIR,
                        extra={"site": site, "card": rel(p.site_card), "card_sha256": sha256_bytes(card)},
                        max_turns=SITE_MAX_TURNS))
    return jobs, existing


def has_site_proposals(site: str) -> bool:
    p = curation_paths(site)
    return all(p.site_proposal(rep).exists() for rep in range(1, REPLICATES + 1))


def _run_summary(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: r.get(k) for k in ("key", "rep", "seconds", "attempts", "tool_calls", "tool_call_counts",
                                   "total_cost_usd", "usage", "stop", "error")}
            | {"n": len(r.get("avus") or r.get("units") or r.get("terms") or [])} for r in results]


def curate_site(site: str, force: bool = False) -> dict[str, Any]:
    """Run the site pass (2 replicates of ``prompt_site.md`` on ``cards/_site.md``); returns its summary.

    Writes ``reports/curate_site_summary.json``. ``status`` is ok, partial (a replicate failed) or
    missing_card; existing proposals are kept unless ``force``.
    """
    site = site.upper()
    t0 = time.monotonic()
    p = curation_paths(site)
    jobs, existing = site_jobs(site, force)
    if not jobs and not existing:
        summary = {"site": site, "status": "missing_card", "card": rel(p.site_card), "runs": []}
        env.write_json(p.site_dir / "reports" / "curate_site_summary.json", summary)
        return summary
    results = run_jobs(jobs, p.run_cwd, parallel=REPLICATES)
    failed = [r for r in results if r.get("error")]
    summary = {"site": site, "status": "ok" if not failed else "partial", "model": MODEL, "replicates": REPLICATES,
               "card": rel(p.site_card), "existing": existing, "runs": _run_summary(results),
               "seconds": round(time.monotonic() - t0, 1),
               "total_cost_usd": round(sum(r.get("total_cost_usd") or 0 for r in results), 4),
               "terms": {f"rep{r.get('rep')}": [(t.get("role"), t.get("curie"), t.get("label"))
                                                for t in r.get("terms") or [] if isinstance(t, dict)]
                         for r in results if not r.get("error")}}
    env.write_json(p.site_dir / "reports" / "curate_site_summary.json", summary)
    log.info("site pass %s: %d run(s), %d failed, $%.2f", site, len(results), len(failed), summary["total_cost_usd"])
    return summary


# ---------------------------------------------------------------- main

def run_jobs(jobs: list[Job], cwd: Path, parallel: int = PARALLEL) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not jobs:
        return results
    with ThreadPoolExecutor(max_workers=max(1, parallel), thread_name_prefix="curate") as pool:
        futures = {pool.submit(run_job, j, cwd): j for j in jobs}
        for fut in as_completed(futures):
            j = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:  # one failed child must not stop the others
                log.exception("%s crashed", j.label)
                results.append({"key": j.key, "rep": j.rep, "error": f"{type(exc).__name__}: {exc}"})
    return results


def curator_unavailable() -> str | None:
    """Why this host cannot run the Opus curation children (None when it can): the claude CLI (``CLAUDE_BIN``,
    ``$NEON_DUCKLAKE_CLAUDE``) is missing or not executable."""
    if not CLAUDE_BIN.exists():
        return f"claude CLI not found at {CLAUDE_BIN}"
    if not os.access(CLAUDE_BIN, os.X_OK):
        return f"claude CLI at {CLAUDE_BIN} is not executable"
    return None


def cli_required(environ: dict[str, str] | None = None) -> bool:
    """``$NEON_DUCKLAKE_CURATION_REQUIRE_CLI``: a host where a missing claude CLI is an error, not a reuse-only
    curation pass."""
    raw = ((environ if environ is not None else os.environ).get(REQUIRE_CLI_ENV) or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def reuse_only(site: str, products: list[str] | None, codes: list[str], skipped: dict[str, str],
               prefetch: dict[str, str], *, why: str, force: bool = False, site_level: bool | None = None,
               t0: float | None = None) -> int:
    """The product pass of a host that cannot run Opus (``why``): import the site's local ``curation/generic``
    results (``validate.import_generic``, a no-op for products with rows here), start no run and take no claim,
    log ``codes`` (products with no generic result) as left uncurated, except those another live VM is curating
    (``validate.pending_generic``: finalize waits for them), write ``reports/curate_summary.json`` (status
    ``no_curator``, ``uncurated``, ``pendingElsewhere``) and return 0. The Data Store prefetch has already run
    (``main``)."""
    from . import validate

    t0 = time.monotonic() if t0 is None else t0
    p = curation_paths(site)
    validated = sorted(c for c, r in skipped.items() if r.startswith("already validated"))
    imported: dict[str, int] = {}
    errors: list[str] = []
    if validated:
        try:
            imported = validate.import_generic(validated)
        except Exception as exc:  # noqa: BLE001 - the files stay; the next pass (or `site validate`) imports them
            log.warning("curate %s: importing generic curation failed: %s", site, exc)
            errors.append(f"import_generic: {type(exc).__name__}: {exc}"[:300])
    try:
        units = unmapped_units()
        if products and units:
            used = product_units(site, products)
            units = [u for u in units if used is None or u in used]
    except Exception as exc:  # noqa: BLE001 - informational only
        log.info("curate %s: unmapped units unknown (%s)", site, exc)
        units = []
    site_pass = site_level is None and not products and p.site_card.exists() and \
        (force or not has_site_proposals(site))
    pending: list[str] = []
    if codes:
        try:  # products another live VM is curating: finalize waits for their result (stream._generic_pending)
            pending = validate.pending_generic(site)
        except Exception as exc:  # noqa: BLE001 - informational: finalize asks again before the avus step
            log.info("curate %s: curation claims unknown (%s)", site, exc)
    pending = [c for c in codes if c in set(pending)]
    uncurated = [c for c in codes if c not in pending]
    if pending:
        log.info("curate %s: %s: %d product(s) are being curated by other VMs: %s", site, why, len(pending),
                 ", ".join(pending))
    if uncurated:
        log.warning("curate %s: %s: %d product(s) left uncurated (no curation/generic result here or on the Data "
                    "Store; their product AVUs carry no curated terms): %s", site, why, len(uncurated),
                    ", ".join(uncurated))
    elif not pending:
        log.info("curate %s: %s: every product reuses curation/generic or is external", site, why)
    if units:
        log.warning("curate %s: units pass skipped (%s): %d unmapped unit(s)", site, why, len(units))
    if site_pass:
        log.warning("curate %s: site pass skipped (%s): no biome/feature proposals for the site", site, why)
    rid = env.run_id()
    summary: dict[str, Any] = {
        "site": site, "runId": rid, "status": NO_CURATOR, "curator": why, "model": MODEL, "replicates": REPLICATES,
        "products": [], "uncurated": uncurated, "skipped": skipped, "existing": [], "runs": [],
        "prefetch": prefetch, "imported": imported, "errors": errors, "unitsUncurated": units,
        "sitePassSkipped": site_pass, "pendingElsewhere": pending, "units": {},
        "seconds": round(time.monotonic() - t0, 1), "total_cost_usd": 0.0}
    env.write_json(p.site_dir / "reports" / "curate_summary.json", summary)
    scon = state.connect(state.DB_PATH)
    try:
        state.start_run(scon, rid, "site-curate", site, [site, ",".join(products or []), "no-curator"])
        state.finish_run(scon, rid, "site-curate", site, NO_CURATOR,
                         {"runs": 0, "uncurated": len(uncurated), "pendingElsewhere": len(pending), "curator": why})
    finally:
        scon.close()
    return 0


def main(site: str, products: list[str] | None = None, force: bool = False,
         site_level: bool | None = None) -> int:
    """``site curate <SITE>``: product replicates + the units pass, and the site pass (see the module docstring).

    Without the claude CLI (:func:`curator_unavailable`) the product pass is :func:`reuse_only` (rc 0); the site
    pass alone, or ``NEON_DUCKLAKE_CURATION_REQUIRE_CLI=1``, returns 1."""
    site = site.upper()
    t0 = time.monotonic()
    p = curation_paths(site)
    no_cli = curator_unavailable()
    if no_cli and (site_level or cli_required()):
        log.error("%s%s", no_cli, "" if site_level else f" ({REQUIRE_CLI_ENV}=1)")
        return 1
    if no_cli is None:
        try:
            render_mcp_config()
        except SystemExit as exc:
            log.error("%s", exc)
            return 1
    if site_level:
        rid = env.run_id()
        scon = state.connect(state.DB_PATH)
        state.start_run(scon, rid, "site-curate", site, [site, "site-level", "force" if force else ""])
        summary: dict[str, Any] = {"status": "failed"}
        try:
            summary = curate_site(site, force)
        finally:
            state.finish_run(scon, rid, "site-curate", site, summary.get("status", "failed"),
                             {"runs": len(summary.get("runs") or []),
                              "errors": sum(1 for r in summary.get("runs") or [] if r.get("error"))})
            scon.close()
        return {"ok": 0, "missing_card": 1}.get(summary["status"], 2)
    from .. import coord

    sync = coord.curation_sync_enabled()
    prefetch: dict[str, str] = {}
    if sync:
        prefetch = datastore_prefetch(site, products)
    codes, skipped = select_products(site, products, force)
    if no_cli is not None:  # reuse what exists; never claim a product this host cannot curate
        return reuse_only(site, products, codes, skipped, prefetch, why=no_cli, force=force, site_level=site_level,
                          t0=t0)
    claims: list[Any] = []
    if sync and codes:
        codes, claims = datastore_claim(codes, skipped)
    for code, why in sorted(skipped.items()):
        log.info("%s: skipped (%s)", code, why)
    jobs, existing = product_jobs(site, codes, force)
    if existing:
        log.info("%d replicate(s) already have proposals: %s", len(existing), ", ".join(existing))
    units = unmapped_units()
    if products and units:
        used = product_units(site, products)
        units = [u for u in units if used is None or u in used]
    uj = units_job(site, units, force)
    if uj:
        jobs.append(uj)
    site_pass = site_level is None and not products and p.site_card.exists() and (force or not has_site_proposals(site))
    if site_pass:
        sj, _existing = site_jobs(site, force)
        jobs += sj
    rid = env.run_id()
    scon = state.connect(state.DB_PATH)
    state.start_run(scon, rid, "site-curate", site, [site, ",".join(products or []), "force" if force else ""])
    status = "failed"
    results: list[dict[str, Any]] = []
    units_result: dict[str, Any] = {}
    try:
        log.info("curate %s: %d product(s), %d run(s) (%s, %d replicates, %d in parallel)%s%s", site, len(codes),
                 len(jobs), MODEL, REPLICATES, PARALLEL, f", units pass for {len(units)} unit(s)" if uj else "",
                 ", site pass" if site_pass else "")
        heartbeat = coord.Heartbeat(claims).start() if claims else None
        try:
            results = run_jobs(jobs, p.run_cwd)
        finally:
            if heartbeat is not None:
                heartbeat.stop()
        if claims:
            failed_codes = {r.get("key") for r in results if r.get("error")}
            for c in claims:
                if c.key in failed_codes:
                    try:
                        coord.release(c)
                    except Exception as exc:  # noqa: BLE001 - it expires on its own
                        log.warning("%s: curation claim not released: %s", c.key, exc)
        if units:
            units_result = apply_units(site, units)
        failed = [r for r in results if r.get("error")]
        status = "ok" if not failed else "partial"
    finally:
        product_results = [r for r in results if r.get("key") != SITE_KEY]
        site_results = [r for r in results if r.get("key") == SITE_KEY]
        summary = {"site": site, "runId": rid, "status": status, "model": MODEL, "replicates": REPLICATES,
                   "products": codes, "skipped": skipped, "existing": existing, "runs": _run_summary(product_results),
                   "pendingElsewhere": sorted(c for c, why in skipped.items() if why.startswith("curation claimed by")),
                   "units": units_result, "seconds": round(time.monotonic() - t0, 1),
                   "total_cost_usd": round(sum(r.get("total_cost_usd") or 0 for r in results), 4)}
        if site_pass:
            summary["siteRuns"] = _run_summary(site_results)
        env.write_json(p.site_dir / "reports" / "curate_summary.json", summary)
        state.finish_run(scon, rid, "site-curate", site, status,
                         {"runs": len(results), "errors": sum(1 for r in results if r.get("error"))})
        scon.close()
    log.info("curate %s done in %.0fs: %d run(s), %d failed, $%.2f", site, time.monotonic() - t0, len(results),
             sum(1 for r in results if r.get("error")), summary["total_cost_usd"])
    return 0 if status == "ok" else 2
