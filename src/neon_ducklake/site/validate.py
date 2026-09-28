"""`site validate <SITE>` — check curated ontology AVUs and decide which are accepted, proposed or rejected.

This generalises neon-avu-eval's ``validate_load.py``. Each AVU in each replicate proposal from
:mod:`site.curate` is checked like this:

1. **CURIE.** The CURIE is normalised (``ontology_store.norm_curie``). Its prefix must not be reserved
   (``mesa_mcp.ols.transform.RESERVED_PREFIXES``). It must be an active OBO Foundry ontology, and it must
   not be UO or GAZ: units and places come from the data.
2. **Term.** The term must resolve in OLS through ``ontology_store.ols_term``, must not be obsolete, and
   its label must match the model's label, ignoring case.
3. **Not deterministic.** It must not duplicate what the data already gives:
   - no NCBITaxon term at species rank or below;
   - no ENVO term that is ``biome`` (ENVO:00000428) or one of its descendants;
   - no CURIE already in ``ref_ncbitaxon_map``, ``ref_unit_map``, ``ref_nlcd_envo`` or ``ref_site_envo``.
   Taxon rank and ENVO ancestry are fetched from the OLS v1 API and cached in the store table
   ``curation_term_facts``.
4. **Canonical rebuild.** The model's attribute is discarded. The AVU is rebuilt with
   ``ontology_annotations_to_avus(prefix, [{'key': _label_to_snake(ols_label), 'value': v, 'curie': curie}])``.
   The value must be non-empty, at most 2,000 UTF-8 bytes, and must not name a NEON site.
5. **Column.** A ``column`` that is given must exist for the product, in the site lake's ``variables``
   or a stacked table.

**Grounded** means the CURIE appeared in that run's own search or get-term results (``seen_curies``).

**Outcome per CURIE:**
- **accepted:** valid in every replicate and grounded in at least one.
- **proposed:** valid in one replicate, or valid everywhere but never grounded.
- **rejected:** everything else, with its reasons.

At most 25 terms per product are kept, taking the both-replicate ones first. The rest are rejected as
``over_product_cap``.

**Outputs:**
- ``curation_avus`` rows in ``work/ontology_cache.duckdb``. ``proposalId`` is a stable hash of
  product, CURIE and value.
- ``curation/generic/<code>.validated.json``.
- ``sites/<SITE>/curation/review.md``, with one checkbox per proposed item.

**Accepting proposed items.** Items become ``human_accepted`` in three ways:
- ``--accept <id>[,<id>…]``, or ``--accept proposed``;
- a ticked box (``- [x]``) in the existing ``review.md``;
- an earlier ``human_accepted`` status, which is kept.

**The units pass.** :func:`apply_unit_proposal` checks the UO terms proposed by
``site.curate``'s units pass:
- the prefix must be UO;
- the term must resolve and not be obsolete;
- the label must match;
- the CURIE must be grounded.
Passing units are written to ``ref_unit_map`` with ``method='curated'``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .. import env, state
from . import ontology_store, site_lake
from .curate import MODEL, REPLICATES, curation_paths, generic_path, rel

log = logging.getLogger("neon_ducklake.site.validate")

MAX_PER_PRODUCT = 25
MAX_VALUE_BYTES = 2000
BIOME = "ENVO:00000428"
SPECIES_RANKS = frozenset({"TAXRANK:0000006", "NCBITaxon:species"})
FORBIDDEN_PREFIXES = {"uo": "forbidden_unit_term", "gaz": "forbidden_place_term"}
ASPECTS = frozenset({"organism_group", "measured_property", "environmental_material", "method", "data_type",
                     "process"})
FACT_PREFIXES = frozenset({"ncbitaxon", "envo"})
FACTS_TABLE = "curation_term_facts"
FACTS_SCHEMA = ('CREATE TABLE IF NOT EXISTS curation_term_facts (curie VARCHAR PRIMARY KEY, ontologyId VARCHAR, '
                'rank VARCHAR, speciesOrBelow BOOLEAN, isBiome BOOLEAN, ancestors VARCHAR[], '
                'fetchedAt TIMESTAMPTZ)')
_PROPOSAL_RE = re.compile(r"^(?P<code>.+)\.rep(?P<rep>\d+)\.json$")
_TICKED_RE = re.compile(r"^\s*[-*]\s*\[[xX]\]\s*`?([0-9a-f]{16})`?")
_NULLISH = {"", "null", "none", "n/a", "na", "-"}


# ---------------------------------------------------------------- small helpers

def write_json_stable(path: Path, doc: dict[str, Any], volatile: tuple[str, ...] = ("validatedAt",)) -> bool:
    """Write ``doc`` unless the file already holds the same content apart from ``volatile`` keys."""
    try:
        old = json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):
        old = None
    def strip(d: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in d.items() if k not in volatile}

    if isinstance(old, dict) and strip(old) == json.loads(json.dumps(strip(doc), default=str)):
        return False
    env.write_json(path, doc)
    return True


def proposal_id(product: str, curie: str | None, value: str) -> str:
    return hashlib.sha256(f"{product}|{curie or ''}|{value}".encode()).hexdigest()[:16]


def fold(s: Any) -> str:
    return " ".join(str(s or "").split()).casefold()


def clean_column(value: Any) -> str | None:
    s = str(value or "").strip()
    return None if s.casefold() in _NULLISH else s


def canonical_avu(ontology_id: str, label: str, value: str, curie: str) -> dict[str, str] | None:
    """mesa-mcp's canonical triple: ``<prefix>.<snake(label)>`` | value | CURIE."""
    from mesa_mcp.ols.client import _label_to_snake
    from mesa_mcp.ols.transform import ontology_annotations_to_avus

    key = _label_to_snake(label)
    if not key:
        return None
    avus = ontology_annotations_to_avus(ontology_id, [{"key": key, "value": value, "curie": curie}])
    return avus[0] if avus else None


def is_reserved(prefix: str) -> bool:
    from mesa_mcp.ols.transform import RESERVED_PREFIXES

    p = prefix.lower()
    return p in RESERVED_PREFIXES or any(p.startswith(r) for r in RESERVED_PREFIXES if not r.isalnum())


# ---------------------------------------------------------------- OLS facts (rank, ENVO ancestry)

_HTTP: Any = None


def _http() -> Any:
    global _HTTP
    if _HTTP is None:
        import httpx

        _HTTP = httpx.Client(timeout=httpx.Timeout(30.0, connect=15.0), follow_redirects=True,
                             headers={"User-Agent": ontology_store.USER_AGENT, "Accept": "application/json"})
    return _HTTP


def _get_json(url: str) -> dict[str, Any] | None:
    """GET JSON with retries (429/5xx/transport); None on another 4xx."""
    import httpx

    last = ""
    for attempt in range(ontology_store.RETRIES):
        try:
            r = _http().get(url)
        except httpx.HTTPError as exc:
            last = type(exc).__name__
        else:
            if r.status_code == 200:
                return r.json()
            if r.status_code != 429 and r.status_code < 500:
                return None
            last = f"HTTP {r.status_code}"
        time.sleep(min(2.0 ** attempt, 8.0))
    raise ontology_store.OLSUnavailable(f"GET {url.split('?')[0]} failed ({last})")


def _ranks(annotation: dict[str, Any] | None) -> set[str]:
    out: set[str] = set()
    for key in ("has_rank", "obsolete has_rank"):
        for v in (annotation or {}).get(key) or []:
            c = ontology_store.norm_curie(v)
            if c:
                out.add(c)
    return out


def fetch_facts(curie: str) -> dict[str, Any]:
    """Rank and ancestry facts from the OLS v1 API (network; cached by :func:`term_facts`)."""
    prefix = curie.split(":", 1)[0].lower()
    enc = urllib.parse.quote(urllib.parse.quote(ontology_store.iri_for(curie), safe=""), safe="")
    base = f"{ontology_store.OLS_API}/ontologies/{prefix}/terms/{enc}"
    term = _get_json(base) or {}
    anc = _get_json(f"{base}/hierarchicalAncestors?size=1000") or {}
    ancestors = (anc.get("_embedded") or {}).get("terms") or []
    anc_curies = sorted({c for t in ancestors if (c := ontology_store.norm_curie(t.get("obo_id") or t.get("iri")))})
    own = _ranks(term.get("annotation"))
    anc_ranks = set().union(*(_ranks(t.get("annotation")) for t in ancestors)) if ancestors else set()
    rank = next((r for r in sorted(own) if r.startswith("TAXRANK")), next(iter(sorted(own)), None))
    return {"curie": curie, "ontologyId": prefix, "rank": rank,
            "speciesOrBelow": bool(own & SPECIES_RANKS or anc_ranks & SPECIES_RANKS),
            "isBiome": curie == BIOME or BIOME in anc_curies, "ancestors": anc_curies}


def term_facts(curies: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Cached facts for NCBITaxon and ENVO CURIEs (others need none). Unreachable ones are omitted."""
    wanted = sorted({c for c in curies if c.split(":", 1)[0].lower() in FACT_PREFIXES})
    out: dict[str, dict[str, Any]] = {}
    if not wanted:
        return out
    with ontology_store.store() as con:
        con.execute(FACTS_SCHEMA)
        rows = con.execute("SELECT curie, ontologyId, rank, speciesOrBelow, isBiome, ancestors FROM "
                           "curation_term_facts WHERE curie IN (SELECT unnest(?::VARCHAR[]))", [wanted]).fetchall()
    for r in rows:
        out[r[0]] = {"curie": r[0], "ontologyId": r[1], "rank": r[2], "speciesOrBelow": r[3], "isBiome": r[4],
                     "ancestors": list(r[5] or [])}
    new: list[dict[str, Any]] = []
    for c in wanted:
        if c in out:
            continue
        try:
            f = fetch_facts(c)
        except ontology_store.OLSUnavailable as exc:
            log.warning("OLS facts for %s unavailable: %s", c, exc)
            continue
        out[c] = f
        new.append(f)
    if new:
        now = env.utcnow()
        with ontology_store.store() as con:
            con.execute(FACTS_SCHEMA)
            con.executemany("INSERT OR REPLACE INTO curation_term_facts VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [[f["curie"], f["ontologyId"], f["rank"], f["speciesOrBelow"], f["isBiome"],
                              f["ancestors"], now] for f in new])
    return out


# ---------------------------------------------------------------- context

@dataclass
class Context:
    """Everything a check needs besides the AVU itself (built once per validate run)."""

    terms: dict[str, dict[str, Any] | None]
    facts: dict[str, dict[str, Any]]
    obo: set[str]
    deterministic: set[str]
    site_codes: set[str]
    site_names: set[str]
    columns: dict[str, set[str]] = field(default_factory=dict)  # product -> casefolded column names


def deterministic_curies() -> set[str]:
    """CURIEs the data-driven maps already produce (taxa, units, land cover, site biome)."""
    queries = {"ref_ncbitaxon_map": "SELECT curie FROM ref_ncbitaxon_map WHERE status = 'ok'",
               "ref_unit_map": "SELECT uoCurie FROM ref_unit_map",
               "ref_nlcd_envo": "SELECT envoCurie FROM ref_nlcd_envo",
               "ref_site_envo": "SELECT envoCurie FROM ref_site_envo"}
    out: set[str] = set()
    with ontology_store.store(read_only=True) as con:
        have = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
        for table, sql in queries.items():
            if table in have:
                out |= {c for (v,) in con.execute(sql).fetchall() if (c := ontology_store.norm_curie(v))}
    return out


def site_identity(site: str) -> tuple[set[str], set[str]]:
    """(site codes, site names) of every NEON site from the catalog lake; the current code at least."""
    codes, names = {site.upper()}, set()
    con = site_lake.connect(site)
    try:
        if site_lake.attach_catalog(con):
            for code, name in con.execute(f"SELECT siteCode, siteName FROM {site_lake.CATALOG_ALIAS}.sites").fetchall():
                if code:
                    codes.add(code.upper())
                if name:
                    names.add(re.sub(r"\s+NEON$", "", name.strip()).casefold())
    finally:
        con.close()
    return codes, names


def product_columns(site: str, products: Iterable[str]) -> dict[str, set[str]]:
    """product -> casefolded column names (``field`` and ``table.field``) from variables + stacked tables."""
    products = list(products)
    out: dict[str, set[str]] = {p: set() for p in products}
    try:
        con = site_lake.open_site_lake(site, read_only=True)
    except Exception as exc:  # noqa: BLE001 - missing lake or a writer holding it
        log.warning("site lake not readable (%s); column checks are skipped", str(exc).splitlines()[0][:120])
        return {}
    try:
        if site_lake.table_exists(con, "variables"):
            for p, t, f in con.execute(f"SELECT productCode, tableName, fieldName FROM {site_lake.ALIAS}.variables "
                                       f"WHERE productCode IN (SELECT unnest(?::VARCHAR[]))", [products]).fetchall():
                if f:
                    out[p] |= {f.casefold(), f"{t}.{f}".casefold()}
        for p in products:
            stem = p.lower().replace(".", "_") + "__"
            for t, c in con.execute("SELECT table_name, column_name FROM duckdb_columns() WHERE database_name = ? "
                                    "AND schema_name = 'main' AND starts_with(table_name, ?)",
                                    [site_lake.ALIAS, stem]).fetchall():
                out[p] |= {c.casefold(), f"{t[len(stem):]}.{c}".casefold(), f"{t}.{c}".casefold()}
    finally:
        con.close()
    return out


# ---------------------------------------------------------------- one AVU

@dataclass
class Check:
    rep: int
    index: int
    raw: dict[str, Any]
    curie: str | None = None
    valid: bool = False
    reason: str | None = None
    label: str | None = None
    ontology_id: str | None = None
    canonical: dict[str, str] | None = None
    column: str | None = None
    grounded: bool | None = None

    def fail(self, reason: str) -> Check:
        self.valid, self.reason = False, reason
        return self


def mentions_site(value: str, ctx: Context) -> bool:
    words = set(re.findall(r"\b[A-Z]{4}\b", value))
    if words & ctx.site_codes:
        return True
    v = value.casefold()
    return any(n and n in v for n in ctx.site_names)


def check_avu(product: str, rep: int, index: int, a: dict[str, Any], seen: set[str] | None, ctx: Context) -> Check:
    """Validate one proposed AVU (see the module docstring for the rules, in order)."""
    ch = Check(rep=rep, index=index, raw=a)
    ch.curie = ontology_store.norm_curie(a) if isinstance(a, dict) else None
    ch.column = clean_column(a.get("column")) if isinstance(a, dict) else None
    if not ch.curie:
        return ch.fail("no_curie")
    ch.grounded = (ch.curie.casefold() in seen) if seen is not None else None
    prefix = ch.curie.split(":", 1)[0].lower()
    ch.ontology_id = prefix
    if is_reserved(prefix):
        return ch.fail("reserved_prefix")
    if prefix in FORBIDDEN_PREFIXES:
        return ch.fail(FORBIDDEN_PREFIXES[prefix])
    if prefix not in ctx.obo:
        return ch.fail("not_active_obo_foundry")
    if ch.curie not in ctx.terms:
        return ch.fail("ols_unavailable")
    term = ctx.terms[ch.curie]
    if term is None or not term.get("label"):
        return ch.fail("unresolved")
    ch.label = str(term["label"]).strip()
    if term.get("isObsolete"):
        return ch.fail("obsolete")
    if fold(a.get("label")) != fold(ch.label):
        return ch.fail("label_mismatch")
    if prefix in FACT_PREFIXES:
        facts = ctx.facts.get(ch.curie)
        if facts is None:
            return ch.fail("ols_unavailable")
        if prefix == "ncbitaxon" and facts.get("speciesOrBelow"):
            return ch.fail("species_or_below")
        if prefix == "envo" and facts.get("isBiome"):
            return ch.fail("biome_term")
    if ch.curie in ctx.deterministic:
        return ch.fail("duplicate_deterministic")
    value = str(a.get("value") or "").strip() or ch.label
    if len(value.encode("utf-8")) > MAX_VALUE_BYTES:
        return ch.fail("value_too_long")
    if mentions_site(value, ctx):
        return ch.fail("site_specific_value")
    if ch.column and product in ctx.columns:
        parts = [p.strip() for p in re.split(r"[,;]", ch.column) if p.strip()]
        if not parts or any(p.casefold() not in ctx.columns[product] for p in parts):
            return ch.fail("unknown_column")
    ch.canonical = canonical_avu(prefix, ch.label, value, ch.curie)
    if ch.canonical is None:
        return ch.fail("no_canonical_attribute")
    ch.valid = True
    return ch


# ---------------------------------------------------------------- one product

@dataclass
class Replicate:
    rep: int
    path: Path
    data: dict[str, Any]

    @property
    def seen(self) -> set[str] | None:
        s = self.data.get("seen_curies")
        return {str(c).casefold() for c in s} if isinstance(s, list) else None


def load_replicates(site: str) -> dict[str, list[Replicate]]:
    """product -> its replicate proposals (``<code>.rep<N>.json``), sorted by rep."""
    d = curation_paths(site).proposals_dir
    out: dict[str, list[Replicate]] = {}
    for f in sorted(d.glob("*.rep*.json")) if d.exists() else []:
        m = _PROPOSAL_RE.match(f.name)
        if not m or f.name.endswith(".failed.json"):
            continue
        try:
            data = json.loads(f.read_text("utf-8"))
        except ValueError:
            log.warning("unreadable proposal %s", rel(f))
            continue
        out.setdefault(m.group("code"), []).append(Replicate(int(m.group("rep")), f, data))
    for reps in out.values():
        reps.sort(key=lambda r: r.rep)
    return out


def _pick_value(checks: list[Check], label: str) -> str:
    vals = [c.canonical["value"] for c in checks if c.canonical]
    if len({fold(v) for v in vals}) == 1:
        return vals[0]
    for v in vals:
        if fold(v) == fold(label):
            return v
    return vals[0]


def _item(product: str, status: str, reason: str | None, checks: list[Check], model: str,
          n_reps: int) -> dict[str, Any]:
    valid = [c for c in checks if c.valid]
    lead = valid[0] if valid else checks[0]
    raw = lead.raw if isinstance(lead.raw, dict) else {}
    if valid:
        value = _pick_value(valid, lead.label or "")
        canon = canonical_avu(lead.ontology_id or "", lead.label or "", value, lead.curie or "") or {}
    else:
        value = str(raw.get("value") or "").strip()
        canon = {}
    aspect = str(raw.get("aspect") or "").strip() or None
    return {
        "proposalId": proposal_id(product, lead.curie, value),
        "productCode": product, "status": status, "reason": reason,
        "attribute": canon.get("attribute", ""), "value": value, "unit": canon.get("unit", ""),
        "curie": lead.curie, "label": lead.label or raw.get("label"), "ontologyId": lead.ontology_id,
        "aspect": aspect, "aspectKnown": aspect in ASPECTS, "column": lead.column,
        "rationale": str(raw.get("rationale") or "").strip() or None, "model": model,
        "reps": len({c.rep for c in valid}), "replicates": n_reps,
        "grounded": any(c.grounded for c in valid) if valid else any(c.grounded for c in checks),
        "checks": [{"rep": c.rep, "valid": c.valid, "reason": c.reason, "grounded": c.grounded,
                    "modelAttribute": (c.raw or {}).get("attribute") if isinstance(c.raw, dict) else None,
                    "modelValue": (c.raw or {}).get("value") if isinstance(c.raw, dict) else None,
                    "modelLabel": (c.raw or {}).get("label") if isinstance(c.raw, dict) else None,
                    "column": c.column} for c in checks],
    }


def evaluate_product(product: str, reps: list[Replicate], ctx: Context) -> list[dict[str, Any]]:
    """All checks of all replicates → one item per CURIE with its outcome (capped at MAX_PER_PRODUCT)."""
    n_reps = len(reps)
    model = next((r.data.get("model") for r in reps if r.data.get("model")), MODEL)
    groups: dict[str, list[Check]] = {}
    order: list[str] = []
    for r in reps:
        for i, a in enumerate(r.data.get("avus") or []):
            a = a if isinstance(a, dict) else {"value": str(a)}
            ch = check_avu(product, r.rep, i, a, r.seen, ctx)
            key = ch.curie.casefold() if ch.curie else f"~{fold(a.get('value'))}"
            if key not in groups:
                groups[key] = []
                order.append(key)
            groups[key].append(ch)
    accepted: list[dict[str, Any]] = []
    proposed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for key in order:
        checks = groups[key]
        valid_reps = {c.rep for c in checks if c.valid}
        if valid_reps and len(valid_reps) >= n_reps and n_reps >= REPLICATES:
            if any(c.grounded for c in checks if c.valid):
                accepted.append(_item(product, "accepted", None, checks, model, n_reps))
            else:
                proposed.append(_item(product, "proposed", "ungrounded", checks, model, n_reps))
        elif valid_reps:
            proposed.append(_item(product, "proposed", "single_replicate", checks, model, n_reps))
        else:
            reasons = sorted({c.reason or "invalid" for c in checks})
            rejected.append(_item(product, "rejected", ",".join(reasons), checks, model, n_reps))
    kept = accepted + proposed
    for item in kept[MAX_PER_PRODUCT:]:
        item["status"], item["reason"] = "rejected", "over_product_cap"
    return kept[:MAX_PER_PRODUCT] + kept[MAX_PER_PRODUCT:] + rejected


# ---------------------------------------------------------------- human acceptance

def ticked_ids(review: Path) -> set[str]:
    if not review.exists():
        return set()
    return {m.group(1) for line in review.read_text("utf-8").splitlines() if (m := _TICKED_RE.match(line))}


def prior_human_accepted(products: Iterable[str]) -> set[str]:
    with ontology_store.store(read_only=True) as con:
        rows = con.execute("SELECT proposalId FROM curation_avus WHERE status = 'human_accepted' AND productCode IN "
                           "(SELECT unnest(?::VARCHAR[]))", [list(products)]).fetchall()
    return {r[0] for r in rows}


def apply_acceptance(items: list[dict[str, Any]], accept: str, ticked: set[str], prior: set[str]) -> list[str]:
    """Promote proposed items to human_accepted; returns the requested ids that matched no proposed item."""
    tokens = {t.strip() for t in (accept or "").split(",") if t.strip()}
    all_proposed = "proposed" in tokens
    wanted = (tokens - {"proposed"}) | ticked | prior
    matched: set[str] = set()
    for it in items:
        if it["status"] != "proposed":
            continue
        if all_proposed or it["proposalId"] in wanted:
            it["status"] = "human_accepted"
            it["reason"] = (it["reason"] or "") + ("; " if it["reason"] else "") + "accepted by reviewer"
            matched.add(it["proposalId"])
    return sorted((tokens - {"proposed"}) - matched)


# ---------------------------------------------------------------- outputs

STORE_COLUMNS = ("proposalId", "productCode", "attribute", "value", "unit", "curie", "label", "ontologyId",
                 "aspect", "columnName", "rationale", "model", "reps", "grounded", "status", "reason", "createdAt")


def store_rows(by_product: dict[str, list[dict[str, Any]]]) -> int:
    """Replace each product's ``curation_avus`` rows (one transaction)."""
    now = env.utcnow()
    rows = [[it["proposalId"], it["productCode"], it["attribute"], it["value"], it["unit"], it["curie"],
             it["label"], it["ontologyId"], it["aspect"], it["column"], it["rationale"], it["model"], it["reps"],
             it["grounded"], it["status"], it["reason"], now]
            for items in by_product.values() for it in items]
    with ontology_store.store() as con:
        con.execute("BEGIN")
        try:
            con.execute("DELETE FROM curation_avus WHERE productCode IN (SELECT unnest(?::VARCHAR[]))",
                        [list(by_product)])
            if rows:
                con.executemany(f"INSERT OR REPLACE INTO curation_avus ({', '.join(STORE_COLUMNS)}) "
                                f"VALUES ({', '.join('?' * len(STORE_COLUMNS))})", rows)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        ontology_store.export_parquet(con)
    return len(rows)


def write_generic(site: str, product: str, reps: list[Replicate], items: list[dict[str, Any]]) -> Path:
    def pick(status: set[str]) -> list[dict[str, Any]]:
        return [it for it in items if it["status"] in status]

    doc = {
        "productCode": product, "site": site, "validatedAt": env.utcnow().isoformat(),
        "model": next((r.data.get("model") for r in reps), MODEL),
        "rules": {"maxPerProduct": MAX_PER_PRODUCT, "replicates": REPLICATES,
                  "accepted": "valid in every replicate and grounded in at least one"},
        "replicates": [{"rep": r.rep, "proposal": rel(r.path), "trace": r.data.get("trace"),
                        "card_sha256": r.data.get("card_sha256"), "prompt_sha256": r.data.get("prompt_sha256"),
                        "n_avus": len(r.data.get("avus") or []), "seconds": r.data.get("seconds"),
                        "tool_calls": r.data.get("tool_calls"), "tool_call_counts": r.data.get("tool_call_counts"),
                        "usage": r.data.get("usage"), "total_cost_usd": r.data.get("total_cost_usd")} for r in reps],
        "counts": {s: sum(1 for it in items if it["status"] == s)
                   for s in ("accepted", "human_accepted", "proposed", "rejected")},
        "accepted": pick({"accepted", "human_accepted"}),
        "proposed": pick({"proposed"}),
        "rejected": pick({"rejected"}),
    }
    path = generic_path(product)
    write_json_stable(path, doc)
    return path


def card_title(site: str, product: str) -> str:
    card = curation_paths(site).card(product)
    try:
        for line in card.read_text("utf-8").splitlines():
            if line.startswith("#"):
                title = line.lstrip("#").strip()
                return re.sub(rf"^{re.escape(product)}\s*[—:-]\s*", "", title) or product
    except OSError:
        pass
    return product


def _line(it: dict[str, Any], box: str | None) -> str:
    where = ", ".join(x for x in (it["curie"], it["aspect"], f"column {it['column']}" if it["column"] else None) if x)
    reps = f"{it['reps']}/{it['replicates']} replicates" + ("" if it["grounded"] else ", ungrounded")
    head = f"- [{box}] " if box is not None else "- "
    why = f" {it['rationale']}" if it["rationale"] else ""
    return f"{head}`{it['proposalId']}` {it['attribute']} = \"{it['value']}\" ({where}); {reps}.{why}"


def _without_stamp(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.startswith("Generated "))


def write_review(site: str, by_product: dict[str, list[dict[str, Any]]]) -> Path:
    p = curation_paths(site)
    out = [f"# Curation review: {site}", "",
           f"Generated {env.utcnow().isoformat(timespec='seconds')} by `neon-ducklake site validate {site}`.",
           "",
           "Terms valid in both Opus replicates were accepted automatically and are listed for reference.",
           f"To accept a proposed term, tick its box (`- [x]`) and run `neon-ducklake site validate {site}` again,",
           "or pass `--accept <proposalId>[,…]` (or `--accept proposed` for all of them).", ""]
    n_open = 0
    for product in sorted(by_product):
        items = by_product[product]
        prop = [it for it in items if it["status"] in ("proposed", "human_accepted")]
        acc = [it for it in items if it["status"] == "accepted"]
        rej = [it for it in items if it["status"] == "rejected"]
        out += [f"## {product}: {card_title(site, product)}", "",
                f"{len(acc)} accepted, {len(prop)} to review, {len(rej)} rejected.", ""]
        if prop:
            out += ["### To review", ""]
            for it in prop:
                out.append(_line(it, "x" if it["status"] == "human_accepted" else " "))
                n_open += it["status"] == "proposed"
            out.append("")
        if acc:
            out += ["### Accepted (both replicates)", ""] + [_line(it, None) for it in acc] + [""]
        if rej:
            out += ["### Rejected", ""]
            out += [f"- {it['curie'] or '(no CURIE)'} \"{it['value']}\": {it['reason']}" for it in rej]
            out.append("")
    text = "\n".join(out).rstrip() + "\n"
    try:
        old = p.review_file.read_text("utf-8")
    except OSError:
        old = ""
    if _without_stamp(old) != _without_stamp(text):  # only the "Generated" line changed: keep the file
        p.review_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.review_file.with_name(p.review_file.name + ".part")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(p.review_file)
    log.info("review: %d proposed item(s) open in %s", n_open, rel(p.review_file))
    return p.review_file


# ---------------------------------------------------------------- the units pass

def apply_unit_proposal(site: str, proposal: dict[str, Any], requested: set[str], out_path: Path) -> dict[str, Any]:
    """Validate a units-pass proposal (UO only, grounded) and write passing units to ``ref_unit_map``."""
    seen = {str(c).casefold() for c in proposal.get("seen_curies") or []}
    items = [u for u in proposal.get("units") or [] if isinstance(u, dict)]
    curies = [c for u in items if (c := ontology_store.norm_curie(u.get("curie")))]
    terms = ontology_store.ols_terms(curies) if curies else {}
    results: list[dict[str, Any]] = []
    for u in items:
        unit = str(u.get("neonUnit") or "").strip()
        curie = ontology_store.norm_curie(u.get("curie"))
        rec = {"neonUnit": unit, "curie": curie, "modelLabel": u.get("label"), "rationale": u.get("rationale"),
               "grounded": bool(curie and curie.casefold() in seen), "valid": False, "reason": None}
        term = terms.get(curie) if curie else None
        if unit not in requested:
            rec["reason"] = "not_requested"
        elif not curie:
            rec["reason"] = "no_curie"
        elif not curie.startswith("UO:"):
            rec["reason"] = "not_uo"
        elif curie not in terms:
            rec["reason"] = "ols_unavailable"
        elif term is None or not term.get("label"):
            rec["reason"] = "unresolved"
        elif term.get("isObsolete"):
            rec["reason"] = "obsolete"
        elif fold(u.get("label")) != fold(term["label"]):
            rec["reason"] = "label_mismatch"
        elif not rec["grounded"]:
            rec["reason"] = "ungrounded"
        else:
            rec["valid"], rec["label"] = True, term["label"]
        results.append(rec)
    good = {}
    for r in results:
        if r["valid"] and r["neonUnit"] not in good:
            good[r["neonUnit"]] = r
    written = 0
    if good:
        now = env.utcnow()
        with ontology_store.store() as con:
            for r in good.values():
                n = con.execute("UPDATE ref_unit_map SET uoCurie = ?, uoLabel = ?, method = 'curated', status = 'ok', "
                                "validatedAt = ? WHERE neonUnit = ? AND status = 'unmapped' "
                                "RETURNING neonUnit", [r["curie"], r["label"], now, r["neonUnit"]]).fetchall()
                written += len(n)
            ontology_store.export_parquet(con)
    summary = {"site": site, "validatedAt": env.utcnow().isoformat(), "proposal": proposal.get("key"),
               "requested": sorted(requested), "mapped": written,
               "unmappedAfter": sorted(requested - set(good)), "results": results}
    write_json_stable(out_path, summary)
    log.info("units pass: %d of %d unit(s) mapped to UO (%d rejected)", written, len(requested),
             sum(1 for r in results if not r["valid"]))
    return {k: summary[k] for k in ("requested", "mapped", "unmappedAfter")} | {
        "rejected": [{k: r[k] for k in ("neonUnit", "curie", "reason")} for r in results if not r["valid"]]}


# ---------------------------------------------------------------- main

def build_context(site: str, reps: dict[str, list[Replicate]]) -> Context:
    curies = sorted({c for rs in reps.values() for r in rs for a in r.data.get("avus") or []
                     if isinstance(a, dict) and (c := ontology_store.norm_curie(a))})
    obo = ontology_store.obo_foundry_prefixes()
    lookup = [c for c in curies if c.split(":", 1)[0].lower() in obo and not is_reserved(c.split(":", 1)[0])]
    terms = ontology_store.ols_terms(lookup) if lookup else {}
    live = [c for c in lookup if terms.get(c)]
    codes, names = site_identity(site)
    return Context(terms=terms, facts=term_facts(live), obo=obo, deterministic=deterministic_curies(),
                   site_codes=codes, site_names=names, columns=product_columns(site, reps))


def main(site: str, accept: str = "") -> int:
    site = site.upper()
    t0 = time.monotonic()
    p = curation_paths(site)
    reps = load_replicates(site)
    if not reps:
        log.warning("validate %s: no proposals in %s (run `site curate %s` first)", site, rel(p.proposals_dir), site)
        return 1
    incomplete = {code: [r.rep for r in rs] for code, rs in reps.items() if len(rs) < REPLICATES}
    for code, have in incomplete.items():
        log.warning("%s: only replicate(s) %s have proposals; skipped until `site curate` completes it", code, have)
    reps = {code: rs for code, rs in reps.items() if code not in incomplete}
    if not reps:
        return 2
    rid = env.run_id()
    scon = state.connect(state.DB_PATH)
    state.start_run(scon, rid, "site-validate", site, [site, accept])
    status = "failed"
    by_product: dict[str, list[dict[str, Any]]] = {}
    unknown: list[str] = []
    try:
        ctx = build_context(site, reps)
        for code, rs in sorted(reps.items()):
            its = evaluate_product(code, rs, ctx)
            if any("ols_unavailable" in (it["reason"] or "") for it in its):
                # A term that could not be looked up is not a real rejection: write nothing for this product.
                log.error("%s: OLS lookups failed for some terms; not written (run validate again)", code)
                incomplete[code] = [r.rep for r in rs]
                continue
            by_product[code] = its
        items = [it for its in by_product.values() for it in its]
        unknown = apply_acceptance(items, accept, ticked_ids(p.review_file), prior_human_accepted(by_product))
        for u in unknown:
            log.warning("--accept %s matches no proposed item", u)
        n_rows = store_rows(by_product) if by_product else 0
        for code in sorted(by_product):
            write_generic(site, code, reps[code], by_product[code])
        if incomplete:
            # Rewriting review.md now would drop the boxes (and any ticks) of the skipped products.
            log.warning("review.md not rewritten while product(s) %s are incomplete", sorted(incomplete))
        else:
            write_review(site, by_product)
        status = "ok" if not incomplete else "partial"
        log.info("validate %s: %d curation_avus row(s) for %d product(s)", site, n_rows, len(by_product))
    finally:
        counts = {code: {s: sum(1 for it in its if it["status"] == s)
                         for s in ("accepted", "human_accepted", "proposed", "rejected")}
                  for code, its in by_product.items()}
        summary = {"site": site, "runId": rid, "status": status, "seconds": round(time.monotonic() - t0, 1),
                   "counts": counts, "incomplete": incomplete, "unknownAcceptIds": unknown,
                   "rejectReasons": _reason_counts(by_product)}
        env.write_json(p.site_dir / "reports" / "validate_summary.json", summary)
        state.finish_run(scon, rid, "site-validate", site, status, {"products": len(by_product)})
        scon.close()
    for code, c in sorted(counts.items()):
        log.info("%s: %d accepted, %d human-accepted, %d proposed, %d rejected", code, c["accepted"],
                 c["human_accepted"], c["proposed"], c["rejected"])
    return 0 if status == "ok" else 2


def _reason_counts(by_product: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for its in by_product.values():
        for it in its:
            if it["status"] == "rejected":
                for r in (it["reason"] or "invalid").split(","):
                    out[r] = out.get(r, 0) + 1
    return dict(sorted(out.items()))
