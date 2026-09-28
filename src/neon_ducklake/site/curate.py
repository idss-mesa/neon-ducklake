"""`site curate <SITE>` — product-level ontology AVUs proposed by Claude Opus, two replicates per product.

This is the neon-avu-eval protocol (Opus 5.5 was the best model there). Each run is a fresh
``claude -p`` child process:

* **Scrubbed environment.** The child gets only ``HOME``, ``PATH`` and ``DBUS_SESSION_BUS_ADDRESS``, as
  with ``env -i``. So it sees no NEON token, no iRODS password and no MESA catalog DSN.
* **One MCP server.** ``--strict-mcp-config`` loads only ``curation/mcp-mesa-ols.json``. That config runs
  mesa-mcp directly, with anonymous iRODS and ``MESA_MCP_DUCKLAKE__CATALOG_DSN=''``.
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
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
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

CLAUDE_BIN = Path(os.environ.get("NEON_DUCKLAKE_CLAUDE", "/home/tswetnam/.local/bin/claude"))
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
# Every other mesa-mcp tool (registry as of mesa-mcp 8fbaedf). They are hidden from the model, and
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


def mcp_config_path() -> Path:
    return curation_root() / "mcp-mesa-ols.json"


def tool_names(tools: Iterable[str]) -> list[str]:
    return [f"mcp__{SERVER}__{t}" for t in tools]


def claude_argv(tools: Iterable[str], *, max_turns: int = MAX_TURNS, resume: str | None = None,
                model: str = MODEL) -> list[str]:
    tools = list(tools)
    hidden = [t for t in (*HIDDEN_TOOLS, *PRODUCT_TOOLS) if t not in tools]
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
    kind: str  # 'product' | 'units'
    key: str  # product code, or 'units.<sha12>'
    rep: int | None
    prompt: str
    prompt_sha256: str
    input_sha256: str  # card sha (product) or the unit list sha (units)
    trace: Path
    proposal: Path
    failed: Path
    tools: tuple[str, ...]
    result_key: str  # 'avus' | 'units'
    repair: str
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.key if self.rep is None else f"{self.key} rep{self.rep}"


def run_job(job: Job, cwd: Path) -> dict[str, Any]:
    """Run one replicate (plus at most one retry) and write its proposal, or a ``.failed.json``."""
    t0 = time.monotonic()
    attempts = [run_claude(job.prompt, job.trace, job.tools, cwd)]
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
            attempts.append(run_claude(job.prompt, job.trace, job.tools, cwd, append=True))
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
    cards = sorted(c.stem for c in p.cards_dir.glob("*.md")) if p.cards_dir.exists() else []
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


def main(site: str, products: list[str] | None = None, force: bool = False) -> int:
    site = site.upper()
    t0 = time.monotonic()
    p = curation_paths(site)
    if not CLAUDE_BIN.exists():
        log.error("claude CLI not found at %s", CLAUDE_BIN)
        return 1
    if not mcp_config_path().exists():
        log.error("MCP config missing: %s", mcp_config_path())
        return 1
    codes, skipped = select_products(site, products, force)
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
    rid = env.run_id()
    scon = state.connect(state.DB_PATH)
    state.start_run(scon, rid, "site-curate", site, [site, ",".join(products or []), "force" if force else ""])
    status = "failed"
    results: list[dict[str, Any]] = []
    units_result: dict[str, Any] = {}
    try:
        log.info("curate %s: %d product(s), %d run(s) (%s, %d replicates, %d in parallel)%s", site, len(codes),
                 len(jobs), MODEL, REPLICATES, PARALLEL, f", units pass for {len(units)} unit(s)" if uj else "")
        results = run_jobs(jobs, p.run_cwd)
        if units:
            units_result = apply_units(site, units)
        failed = [r for r in results if r.get("error")]
        status = "ok" if not failed else "partial"
    finally:
        runs = [{k: r.get(k) for k in ("key", "rep", "seconds", "attempts", "tool_calls", "tool_call_counts",
                                         "total_cost_usd", "usage", "stop", "error")}
                | {"n": len(r.get("avus") or r.get("units") or [])} for r in results]
        summary = {"site": site, "runId": rid, "status": status, "model": MODEL, "replicates": REPLICATES,
                   "products": codes, "skipped": skipped, "existing": existing, "runs": runs,
                   "units": units_result, "seconds": round(time.monotonic() - t0, 1),
                   "total_cost_usd": round(sum(r.get("total_cost_usd") or 0 for r in results), 4)}
        env.write_json(p.site_dir / "reports" / "curate_summary.json", summary)
        state.finish_run(scon, rid, "site-curate", site, status,
                         {"runs": len(results), "errors": sum(1 for r in results if r.get("error"))})
        scon.close()
    log.info("curate %s done in %.0fs: %d run(s), %d failed, $%.2f", site, time.monotonic() - t0, len(results),
             sum(1 for r in results if r.get("error")), summary["total_cost_usd"])
    return 0 if status == "ok" else 2
