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

**The site pass.** :func:`validate_site` checks the two replicates of ``site.curate``'s site pass
(``sites/<SITE>/curation/site/_site.rep<N>.json``). Per role (``biome``; ``feature`` for aquatic sites only):
- the CURIE must be ENVO, resolve in OLS, not be obsolete, and carry the model's label;
- a biome must descend from ENVO:00000428 'biome'; at an aquatic site (catalog HABITAT 'Aquatic …') it must
  descend from ENVO:00002030 'aquatic biome', at a terrestrial site it must not;
- a feature must descend from ENVO:00000063 'water body' and not be a biome; a terrestrial site gets none.
Agreement rule (as for products): both replicates valid with the same CURIE, grounded in at least one →
**curated**: ``ref_site_envo`` (biome) / ``ref_site_envo_feature`` (feature) get ``method='curated'``,
``status='curated'``. Otherwise the choices are **proposed** for review in
``sites/<SITE>/curation/review_site.md`` (tick a box, or ``--accept <id>`` / ``--accept proposed``, then
validate again → curated) and the heuristic row stays. Rows set by hand (status ``ok``/``human_accepted``)
are never replaced. Every replicate choice is kept in ``site_envo_proposals`` (source ``rep1``/``rep2``/
``human``), and ``curation/site/_site.validated.json`` records the outcome.

``main(site, accept='', site_level=None)``: ``None`` validates the site pass (when it has proposals) and the
products (when they have proposals); ``True`` only the site pass; ``False`` only the products.
:func:`has_proposals` says whether there is anything to validate (for the orchestrator).
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
from .curate import MODEL, REPLICATES, SITE_KEY, curation_paths, generic_path, rel

log = logging.getLogger("neon_ducklake.site.validate")

MAX_PER_PRODUCT = 25
MAX_VALUE_BYTES = 2000
BIOME = "ENVO:00000428"
AQUATIC_BIOME = "ENVO:00002030"
WATER_BODY = "ENVO:00000063"
SITE_ROLES = ("biome", "feature")
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
    # ref_site_envo_feature is left out on purpose: a site's water body (stream, lake …) is site-specific,
    # while product terms are generic across sites, so a product may legitimately use 'stream'.
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


# ---------------------------------------------------------------- the site pass

_SITE_PROPOSAL_RE = re.compile(rf"^{re.escape(SITE_KEY)}\.rep(?P<rep>\d+)\.json$")


def load_site_replicates(site: str) -> list[Replicate]:
    """The site pass proposals (``curation/site/_site.rep<N>.json``), sorted by rep."""
    d = curation_paths(site).site_dir_curation
    out: list[Replicate] = []
    for f in sorted(d.glob(f"{SITE_KEY}.rep*.json")) if d.exists() else []:
        m = _SITE_PROPOSAL_RE.match(f.name)
        if not m:
            continue
        try:
            out.append(Replicate(int(m.group("rep")), f, json.loads(f.read_text("utf-8"))))
        except ValueError:
            log.warning("unreadable site proposal %s", rel(f))
    return sorted(out, key=lambda r: r.rep)


def has_proposals(site: str) -> bool:
    """Whether ``site validate`` has anything to check (product or site-pass proposals)."""
    return bool(load_replicates(site)) or bool(load_site_replicates(site))


def site_habitat(site: str) -> str | None:
    """The catalog ``sites.siteProperties['HABITAT']`` of the site (None when unavailable)."""
    con = site_lake.connect(site)
    try:
        if not site_lake.attach_catalog(con):
            return None
        row = con.execute(f"SELECT siteProperties['HABITAT'] FROM {site_lake.CATALOG_ALIAS}.sites WHERE siteCode = ?",
                          [site.upper()]).fetchone()
        return row[0] if row and row[0] else None
    except Exception as exc:  # noqa: BLE001 - schema surprise: validate as terrestrial is unsafe, report it
        log.warning("HABITAT of %s unreadable: %s", site, str(exc).splitlines()[0][:160])
        return None
    finally:
        con.close()


def is_aquatic(habitat: str | None) -> bool:
    return (habitat or "").strip().lower().startswith("aquatic")


def site_proposal_id(site: str, role: str, curie: str | None) -> str:
    return hashlib.sha256(f"site|{site}|{role}|{curie or ''}".encode()).hexdigest()[:16]


@dataclass
class SiteCheck:
    rep: int
    role: str
    raw: dict[str, Any]
    curie: str | None = None
    label: str | None = None
    valid: bool = False
    reason: str | None = None
    grounded: bool | None = None

    def fail(self, reason: str) -> SiteCheck:
        self.valid, self.reason = False, reason
        return self


def check_site_term(rep: int, role: str, t: dict[str, Any], seen: set[str] | None, aquatic: bool,
                    terms: dict[str, dict[str, Any] | None], facts: dict[str, dict[str, Any]]) -> SiteCheck:
    """Validate one replicate's biome or feature choice (rules in the module docstring)."""
    ch = SiteCheck(rep=rep, role=role, raw=t)
    ch.curie = ontology_store.norm_curie(t) if isinstance(t, dict) else None
    if not ch.curie:
        return ch.fail("no_curie")
    ch.grounded = (ch.curie.casefold() in seen) if seen is not None else None
    if not ch.curie.startswith("ENVO:"):
        return ch.fail("not_envo")
    if role == "feature" and not aquatic:
        return ch.fail("feature_for_terrestrial_site")
    if ch.curie not in terms:
        return ch.fail("ols_unavailable")
    term = terms[ch.curie]
    if term is None or not term.get("label"):
        return ch.fail("unresolved")
    ch.label = str(term["label"]).strip()
    if term.get("isObsolete"):
        return ch.fail("obsolete")
    if fold(t.get("label")) != fold(ch.label):
        return ch.fail("label_mismatch")
    f = facts.get(ch.curie)
    if f is None:
        return ch.fail("ols_unavailable")
    anc = set(f.get("ancestors") or []) | {ch.curie}
    if role == "biome":
        if not f.get("isBiome"):
            return ch.fail("not_a_biome")
        if aquatic and AQUATIC_BIOME not in anc:
            return ch.fail("not_an_aquatic_biome")
        if not aquatic and AQUATIC_BIOME in anc:
            return ch.fail("aquatic_biome_at_terrestrial_site")
    else:
        if f.get("isBiome"):
            return ch.fail("biome_as_feature")
        if WATER_BODY not in anc:
            return ch.fail("not_a_water_body")
    ch.valid = True
    return ch


def _role_terms(rep: Replicate, role: str) -> list[dict[str, Any]]:
    return [t for t in rep.data.get("terms") or [] if isinstance(t, dict)
            and str(t.get("role") or "").strip().lower() == role]


def evaluate_site(site: str, reps: list[Replicate], aquatic: bool, terms: dict[str, dict[str, Any] | None],
                  facts: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """role -> {status curated|proposed|rejected, reason, curie, label, choices[…], options[…]}."""
    out: dict[str, dict[str, Any]] = {}
    for role in SITE_ROLES:
        if role == "feature" and not aquatic:
            extra = [t for r in reps for t in _role_terms(r, role)]
            if extra:
                log.warning("site %s: feature terms ignored at a terrestrial site: %s", site,
                            [t.get("curie") for t in extra])
            continue
        checks: list[SiteCheck] = []
        for r in reps:
            ts = _role_terms(r, role)
            if not ts:
                checks.append(SiteCheck(rep=r.rep, role=role, raw={}).fail("missing"))
                continue
            if len(ts) > 1:
                log.warning("site %s rep%d: %d %s terms; the first is used", site, r.rep, len(ts), role)
            checks.append(check_site_term(r.rep, role, ts[0], r.seen, aquatic, terms, facts))
        valid = [c for c in checks if c.valid]
        curies = {c.curie for c in valid}
        item: dict[str, Any] = {"role": role, "curie": None, "label": None,
                                "choices": [{"rep": c.rep, "curie": c.curie, "label": c.label or c.raw.get("label"),
                                             "valid": c.valid, "reason": c.reason, "grounded": c.grounded,
                                             "rationale": c.raw.get("rationale")} for c in checks]}
        if len(valid) == len(reps) >= REPLICATES and len(curies) == 1 and any(c.grounded for c in valid):
            item.update(status="curated", reason=None, curie=valid[0].curie, label=valid[0].label)
        elif valid:
            why = ("replicates_disagree" if len(curies) > 1 else
                   "single_replicate" if len(valid) < len(reps) or len(reps) < REPLICATES else "ungrounded")
            item.update(status="proposed", reason=why)
        else:
            item.update(status="rejected", reason=",".join(sorted({c.reason or "invalid" for c in checks})))
        opts: dict[str, dict[str, Any]] = {}
        for c in valid:
            o = opts.setdefault(c.curie, {"proposalId": site_proposal_id(site, role, c.curie), "curie": c.curie,
                                          "label": c.label, "reps": [], "grounded": False})
            o["reps"].append(c.rep)
            o["grounded"] = o["grounded"] or bool(c.grounded)
        item["options"] = list(opts.values())
        item["proposalId"] = site_proposal_id(site, role, item["curie"]) if item["curie"] else None
        out[role] = item
    return out


def apply_site_acceptance(items: dict[str, dict[str, Any]], accept: str, ticked: set[str]) -> set[str]:
    """Promote a proposed role to curated when one of its options was accepted; returns the matched ids."""
    tokens = {t.strip() for t in (accept or "").split(",") if t.strip()}
    matched: set[str] = set()
    for it in items.values():
        if it["status"] != "proposed":
            continue
        chosen = [o for o in it["options"] if o["proposalId"] in (tokens | ticked)]
        if not chosen and "proposed" in tokens and len(it["options"]) == 1:
            chosen = it["options"]
        if len(chosen) == 1:
            o = chosen[0]
            it.update(status="curated", curie=o["curie"], label=o["label"], proposalId=o["proposalId"],
                      reason=(it["reason"] or "") + "; accepted by reviewer", human=True)
            matched.add(o["proposalId"])
        elif len(chosen) > 1:
            log.warning("site %s: %d options accepted; choose one", it["role"], len(chosen))
    return matched


_MANUAL_STATES = ("ok", "human_accepted", "accepted")


def store_site(site: str, items: dict[str, dict[str, Any]], reps: list[Replicate], habitat: str | None) -> dict[str, Any]:
    """Write curated roles to ref_site_envo / ref_site_envo_feature and every replicate choice to site_envo_proposals."""
    now = env.utcnow()
    written: dict[str, Any] = {}
    with ontology_store.store() as con:
        con.execute("BEGIN")
        try:
            for r in reps:
                for role in SITE_ROLES:
                    ts = _role_terms(r, role)
                    t = ts[0] if ts else {}
                    item = items.get(role) or {}
                    choice = next((c for c in item.get("choices", []) if c["rep"] == r.rep), {})
                    con.execute("INSERT OR REPLACE INTO site_envo_proposals (siteCode, role, source, envoCurie, "
                                "envoLabel, rule, detail, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                [site, role, f"rep{r.rep}", ontology_store.norm_curie(t) if t else None,
                                 t.get("label"), r.data.get("model") or MODEL,
                                 json.dumps({k: choice.get(k) for k in ("valid", "reason", "grounded", "rationale")}),
                                 now])
            for role, it in items.items():
                table = "ref_site_envo" if role == "biome" else "ref_site_envo_feature"
                cur = con.execute(f"SELECT envoCurie, method, status FROM {table} WHERE siteCode = ?",
                                  [site]).fetchone()
                if cur and (cur[2] or "") in _MANUAL_STATES:
                    written[role] = {"kept": "manual", "curie": cur[0]}
                    continue
                if it["status"] == "curated":
                    if it.get("human"):
                        con.execute("INSERT OR REPLACE INTO site_envo_proposals (siteCode, role, source, envoCurie, "
                                    "envoLabel, rule, detail, createdAt) VALUES (?, ?, 'human', ?, ?, 'review', "
                                    "?, ?)", [site, role, it["curie"], it["label"],
                                              json.dumps({"proposalId": it["proposalId"]}), now])
                    if role == "biome":
                        type_eco = con.execute("SELECT typeEco FROM ref_site_envo WHERE siteCode = ?",
                                               [site]).fetchone()
                        con.execute("INSERT OR REPLACE INTO ref_site_envo (siteCode, envoCurie, envoLabel, typeEco, "
                                    "method, status) VALUES (?, ?, ?, ?, 'curated', 'curated')",
                                    [site, it["curie"], it["label"],
                                     None if is_aquatic(habitat) else (type_eco[0] if type_eco else None)])
                    else:
                        con.execute("INSERT OR REPLACE INTO ref_site_envo_feature (siteCode, envoCurie, envoLabel, "
                                    "habitat, method, status) VALUES (?, ?, ?, ?, 'curated', 'curated')",
                                    [site, it["curie"], it["label"], habitat])
                    written[role] = {"curated": it["curie"]}
                elif cur and cur[1] == "curated":
                    # an earlier agreement no longer holds: back to the heuristic proposal
                    h = con.execute("SELECT envoCurie, envoLabel FROM site_envo_proposals WHERE siteCode = ? AND "
                                    "role = ? AND source = 'heuristic'", [site, role]).fetchone()
                    con.execute(f"UPDATE {table} SET envoCurie = ?, envoLabel = ?, method = ?, status = ? "
                                f"WHERE siteCode = ?", [h[0] if h else None, h[1] if h else None,
                                                        "heuristic" if h and h[0] else "pending",
                                                        "proposed" if h and h[0] else "pending", site])
                    written[role] = {"reverted": True}
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        ontology_store.export_parquet(con)
    return written


def write_site_review(site: str, items: dict[str, dict[str, Any]], habitat: str | None) -> Path:
    p = curation_paths(site)
    path = p.curation_dir / "review_site.md"
    out = [f"# Site curation review: {site}", "",
           f"Generated {env.utcnow().isoformat(timespec='seconds')} by `neon-ducklake site validate {site}`.", "",
           f"HABITAT: {habitat or 'not recorded'}. Terms both Opus replicates chose are curated automatically.",
           f"To accept a proposed term, tick its box (`- [x]`) and run `neon-ducklake site validate {site}` again,",
           "or pass `--accept <proposalId>`.", ""]
    for role, it in items.items():
        out += [f"## {role}: {it['status']}" + (f" ({it['reason']})" if it.get("reason") else ""), ""]
        if it["status"] == "curated":
            out.append(f"- `{it['proposalId']}` {it['curie']} {it['label']}")
        for o in it["options"] if it["status"] != "curated" else []:
            out.append(f"- [ ] `{o['proposalId']}` {o['curie']} {o['label']} (replicate(s) "
                       f"{', '.join(str(r) for r in o['reps'])}{'' if o['grounded'] else ', ungrounded'})")
        for c in it["choices"]:
            why = f": {c['reason']}" if c["reason"] else ""
            out.append(f"  - rep{c['rep']}: {c['curie'] or '-'} {c['label'] or ''}{why}. {c.get('rationale') or ''}"
                       .rstrip())
        out.append("")
    text = "\n".join(out).rstrip() + "\n"
    try:
        old = path.read_text("utf-8")
    except OSError:
        old = ""
    if _without_stamp(old) != _without_stamp(text):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".part")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    return path


def validate_site(site: str, accept: str = "") -> dict[str, Any]:
    """Validate the site pass and store the result (see the module docstring). ``status``: ok, partial,
    incomplete (fewer than two replicates), none (no proposals) or ols_unavailable."""
    site = site.upper()
    p = curation_paths(site)
    reps = load_site_replicates(site)
    if not reps:
        return {"site": site, "status": "none"}
    if len(reps) < REPLICATES:
        log.warning("site %s: only replicate(s) %s of the site pass exist; run `site curate %s` again", site,
                    [r.rep for r in reps], site)
        return {"site": site, "status": "incomplete", "replicates": [r.rep for r in reps]}
    habitat = site_habitat(site)
    aquatic = is_aquatic(habitat)
    curies = sorted({c for r in reps for t in r.data.get("terms") or []
                     if isinstance(t, dict) and (c := ontology_store.norm_curie(t)) and c.startswith("ENVO:")})
    terms = ontology_store.ols_terms(curies) if curies else {}
    facts = term_facts([c for c in curies if terms.get(c)])
    items = evaluate_site(site, reps, aquatic, terms, facts)
    if any("ols_unavailable" in (it.get("reason") or "") for it in items.values()):
        log.error("site %s: OLS lookups failed; nothing written (run validate again)", site)
        return {"site": site, "status": "ols_unavailable", "items": items}
    review = p.curation_dir / "review_site.md"
    matched = apply_site_acceptance(items, accept, ticked_ids(review))
    written = store_site(site, items, reps, habitat)
    write_site_review(site, items, habitat)
    doc = {"site": site, "validatedAt": env.utcnow().isoformat(), "habitat": habitat, "model": MODEL,
           "rules": {"replicates": REPLICATES, "curated": "both replicates valid with the same CURIE, grounded in one",
                     "biome": "descendant of ENVO:00000428; aquatic site: of ENVO:00002030, terrestrial: not",
                     "feature": "aquatic sites: descendant of ENVO:00000063 water body, not a biome"},
           "replicates": [{"rep": r.rep, "proposal": rel(r.path), "trace": r.data.get("trace"),
                           "card_sha256": r.data.get("card_sha256"), "prompt_sha256": r.data.get("prompt_sha256"),
                           "seconds": r.data.get("seconds"), "tool_call_counts": r.data.get("tool_call_counts"),
                           "total_cost_usd": r.data.get("total_cost_usd")} for r in reps],
           "items": items, "written": written, "acceptedIds": sorted(matched)}
    write_json_stable(p.site_dir_curation / f"{SITE_KEY}.validated.json", doc)
    status = "ok" if all(it["status"] == "curated" for it in items.values()) else "partial"
    for role, it in items.items():
        log.info("site %s %s: %s %s %s", site, role, it["status"], it.get("curie") or "-", it.get("reason") or "")
    return {"site": site, "status": status, "habitat": habitat, "items": items, "written": written,
            "matched": sorted(matched)}


def main_site(site: str, accept: str = "") -> int:
    """``site validate`` for the site pass alone (0 curated, 2 proposed/incomplete/outage, 1 no proposals)."""
    rid = env.run_id()
    scon = state.connect(state.DB_PATH)
    state.start_run(scon, rid, "site-validate", site, [site, accept, "site-level"])
    res: dict[str, Any] = {"status": "failed"}
    try:
        res = validate_site(site, accept)
    finally:
        env.write_json(curation_paths(site).site_dir / "reports" / "validate_site_summary.json",
                       {**res, "runId": rid})
        state.finish_run(scon, rid, "site-validate", site, res.get("status", "failed"),
                         {k: (it.get("status"), it.get("curie")) for k, it in (res.get("items") or {}).items()})
        scon.close()
    if res["status"] == "none":
        log.warning("validate %s: no site-pass proposals (run `site curate %s` first)", site, site)
        return 1
    return 0 if res["status"] == "ok" else 2


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


def main(site: str, accept: str = "", site_level: bool | None = None) -> int:
    """``site validate <SITE>``: the site pass and/or the products (see the module docstring)."""
    site = site.upper()
    rc_site = None
    if site_level is not False and (site_level or load_site_replicates(site)):
        rc_site = main_site(site, accept)
    if site_level:
        return rc_site if rc_site is not None else 1
    site_ids = _site_option_ids(site) if rc_site is not None else set()
    rc = _main_products(site, accept, site_ids, quiet=rc_site is not None)
    if rc is None:
        return rc_site if rc_site is not None else 1
    return rc if rc_site is None else max(rc, rc_site)


def _site_option_ids(site: str) -> set[str]:
    try:
        doc = json.loads((curation_paths(site).site_dir_curation / f"{SITE_KEY}.validated.json").read_text("utf-8"))
    except (OSError, ValueError):
        return set()
    return {o["proposalId"] for it in (doc.get("items") or {}).values() for o in it.get("options") or []}


def _main_products(site: str, accept: str, site_ids: set[str], quiet: bool) -> int | None:
    """The product validation (None when there are no product proposals)."""
    t0 = time.monotonic()
    p = curation_paths(site)
    reps = load_replicates(site)
    if not reps:
        if not quiet:
            log.warning("validate %s: no proposals in %s (run `site curate %s` first)", site, rel(p.proposals_dir),
                        site)
        return None
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
        unknown = [u for u in apply_acceptance(items, accept, ticked_ids(p.review_file),
                                               prior_human_accepted(by_product)) if u not in site_ids]
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
