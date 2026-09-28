"""`site terms <SITE>`: deterministic ontology terms for one site, kept in the shared ontology store.

Every CURIE comes from an OLS lookup and is re-checked with ``ontology_store.ols_term``, which
confirms the term exists and is not obsolete. Results are cached across sites in
``work/ontology_cache.duckdb`` and exported to ``ref/ontology_maps/*.parquet``.

(a) **Taxa → NCBITaxon**, for every distinct (taxonTypeCode, taxonID) in the site lake's ``file_taxa``.
  - taxonTypeCode is chosen per (product, table): the catalog ``taxonomy`` type with the best
    taxonID match rate. Ties go to name agreement, then alphabetical order, and are reported.
  - The name used is the accepted one: ``acceptedTaxonID`` is followed, the authorship is removed,
    and qualifiers are normalised: ``sp.``/``spp.``/``cf.``/``aff.`` are dropped (so ``Carex sp.`` is the
    genus ``Carex``), ``ssp.`` becomes ``subsp.``, ``fo.``/``forma`` become ``f.``, and the hybrid sign
    ``×`` becomes NCBI's ``x`` (``Abies ×phanerolepis`` → ``Abies x phanerolepis``, ``×Achnella caduca`` →
    ``x Achnella caduca``). A rank marker keeps its epithet only when that epithet is not an author word
    (``Hook. f. ex A. Gray`` stays authorship).
  - Levels, each searched and verified in one batch; the first that yields exactly one live term wins:
    1. ``exact``: an OLS ``ncbitaxon`` search (size 25) with a case-insensitive exact label match.
    2. ``synonym``: an exact OLS v1 search on label *and* synonym. It picks NCBI's homonym label
       (``Aedes <genus>``, whose exact synonym is ``Aedes``) or a term that lists the NEON name as a
       synonym (``Acacia farnesiana`` → ``Vachellia farnesiana``). The synonym's OBO type is checked on
       the term record: ``in-part``, ``misnomer``, ``type material``, acronyms and common names do not count.
    3. ``species_fallback``: for infraspecific names only (``var.``/``subsp.``/``f.`` or a trinomial), the
       binomial, by label then synonym.
    4. ``genus_fallback`` and 5. ``family_fallback`` (catalog family), by label then synonym.
    Otherwise the taxon is ``unresolved``.
  - Homonyms are disambiguated by rank (``<genus>``/``<family>`` labels), then inside the catalog kingdom.
  - Rows already ``ok`` in ``ref_ncbitaxon_map`` with method ``exact`` or ``synonym`` are kept as they are;
    fallback, unresolved and pending rows are resolved again on every run (their searches are cached).
  - Output: ``reports/unresolved_taxa.csv``. It lists every taxon that is not an ``ok`` exact match
    (fallbacks and synonyms included), with the top OLS hits of the exact-name search.

(b) **Units → UO**, for every distinct ``variables.units``.
  - Variance units (``celsiusSquared``, ``metersPerSecondSquared``, ``wattsPerSquareMeterSquared`` …: a
    unit followed by ``Squared`` that is not an area such as ``metersSquared``, or a ``…Squared`` unit whose
    every field is a ``…Variance`` field, such as ``microsiemensPerCentimeterSquared``) get method and status
    ``not_applicable``. They are not failures: they are left out of the unmapped counts, the report and
    the ``uo.*`` AVUs. (NEON uses ``metersPerSecondSquared`` only for wind-speed variance.)
  - First a seed dictionary, NEON unit → UO *label(s)*. The CURIE always comes from an OLS search in
    ``uo``, preferring ``UO:0000xxx`` over ``UO:1xxxxxx`` (the "… based unit" terms). Method ``seed``.
  - Otherwise the camelCase name is split into words and singularised, then matched exactly.
    Method ``exact``.
  - A label matches a term when it equals the term's label or one of its synonyms, ignoring case and
    US/UK spelling (liter/litre, meter/metre): the synonyms come from an exact OLS v1 search on label
    and synonym. A camelCase variant that only matches that way gets method ``synonym``.
  - What is left goes to ``reports/unmapped_units.csv``. Rows with method ``curated`` are never
    touched.

(c) **NLCD → ENVO**, a fixed dictionary.
  - Each CURIE is verified. When its label differs or the term is obsolete, ENVO is searched for
    the expected label and the difference is reported.

(d) **Site biome and environmental feature**, conservative proposals (a hint for the site-level Opus
    curation, ``site curate`` + ``site validate``).
  - Keyed on the catalog ``sites.siteProperties['HABITAT']``:
    - ``Terrestrial``: the biome from TypeEco and the dominant NLCD class through ordered keyword rules.
    - ``Aquatic Wadeable Stream``: feature ``stream``, biome ``freshwater stream biome``.
    - ``Aquatic Non-wadeable Stream``: feature ``river``, biome ``freshwater river biome``.
    - ``Aquatic Lake``: feature ``lake``, biome ``freshwater lake biome``.
    TypeEco is never used for an aquatic site: NEON copied one shortgrass-steppe TypeEco to all of them.
  - A biome label has to exist as a descendant of ENVO:00000428 'biome', a feature label as a
    descendant of ENVO:00000063 'water body' (OLS ``allChildrenOf``).
  - Stored in ``ref_site_envo`` (biome) and ``ref_site_envo_feature`` (feature), method ``heuristic``,
    status ``proposed``; with no candidate, method and status are both ``pending``. Every proposal is
    also kept in ``site_envo_proposals`` (source ``heuristic``).
  - A ``curated`` row is never overwritten.
"""

from __future__ import annotations

import csv
import json
import logging
import re
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from .. import env
from . import ontology_store as store
from . import site_lake

log = logging.getLogger("neon_ducklake.site.terms")

SEARCH_SIZE = 25
UNKNOWN_TYPE = "UNKNOWN"

# ---------------------------------------------------------------- taxa

TAXON_LEVELS = ("exact", "synonym", "species_fallback", "genus_fallback", "family_fallback")
TAXON_COUNT_KEYS = (*TAXON_LEVELS, "unresolved", "obsolete", "pending")
KEEP_METHODS = ("exact", "synonym")  # an 'ok' row with one of these methods is never re-resolved
# The rank a fallback level looks for (NCBI labels homonyms as 'Aedes <genus>').
LEVEL_RANK = {"species_fallback": "species", "genus_fallback": "genus", "family_fallback": "family"}
# Catalog kingdom -> NCBITaxon root used to disambiguate homonyms (e.g. Morus the plant vs the bird).
KINGDOM_ROOTS = {"animalia": "NCBITaxon:33208", "metazoa": "NCBITaxon:33208", "plantae": "NCBITaxon:33090",
                 "viridiplantae": "NCBITaxon:33090", "fungi": "NCBITaxon:4751", "bacteria": "NCBITaxon:2",
                 "archaea": "NCBITaxon:2157"}
RANK_MARKERS = {"subsp.", "var.", "f.", "subvar."}
MARKER_ALIASES = {"ssp.": "subsp.", "ssp": "subsp.", "subsp": "subsp.", "var": "var.", "fo.": "f.", "forma": "f.",
                  "fma.": "f."}
DROP_QUALIFIERS = {"sp.", "spp.", "cf.", "aff.", "sp", "spp", "cf", "aff"}
AUTHOR_PARTICLES = {"ex", "et", "in", "de", "van", "von", "der", "den", "du", "and", "emend.", "nom.", "al", "al."}
HYBRID = "x"
# OBO synonym types that do not name the same taxon (NCBI 'in-part', 'misnomer', …) or are not scientific names.
BAD_SYNONYM_TYPES = frozenset({"in-part", "misnomer", "type material", "acronym", "genbank acronym", "blast name",
                               "common name", "genbank common name", "layperson synonym"})
_NAME_WORD = re.compile(r"^[A-Z][a-z]+(?:-[a-z]+)?$")
_EPITHET = re.compile(r"^[a-z][a-z-]*$")
# NEON placeholder names ('Unknown plant', 'Unidentified beetle'): NCBI's 'unidentified' nodes list 'unknown'
# as a synonym, so these never go through the synonym, species or genus levels (the catalog family still may).
_PLACEHOLDER = re.compile(r"^(unknown|unidentified|unid\.?|undetermined|other|none|mixed|no)\b", re.IGNORECASE)


def is_placeholder(name: str | None) -> bool:
    return bool(_PLACEHOLDER.match((name or "").strip()))


def query_name(name: str | None, authorship: str | None = None) -> str | None:
    """NEON scientificName -> NCBITaxon query.

    - The authorship is removed.
    - ' sp.', ' spp.', ' cf.' and ' aff.' are dropped, 'ssp.' becomes 'subsp.' and 'fo.' becomes 'f.'.
    - The hybrid sign '×' becomes NCBI's separate 'x' ('Abies ×phanerolepis' -> 'Abies x phanerolepis').
    - What is kept is the genus, the lower-case epithets that follow it directly, and marker+epithet
      pairs such as 'var. x', even when authors come between them. Authors, numbers and 'sp. 1'
      are dropped; a marker whose next word is an author particle ('Hook. f. ex A. Gray') is authorship.

    >>> query_name("Larrea tridentata (DC.) Coville", "(DC.) Coville")
    'Larrea tridentata'
    >>> query_name("Toxostoma cf. curvirostre")
    'Toxostoma curvirostre'
    >>> query_name("Carex sp.")
    'Carex'
    >>> query_name("Abies ×phanerolepis (Fernald) Liu")
    'Abies x phanerolepis'
    """
    if not name or not name.strip():
        return None
    text = " ".join(name.split())
    if authorship and authorship.strip():
        auth = " ".join(authorship.split())
        if text.endswith(auth):
            text = text[: -len(auth)].strip()
        else:
            text = text.replace(auth, " ")
    text = re.sub(r"\s*×\s*", f" {HYBRID} ", text).strip()
    tokens = [MARKER_ALIASES.get(t, t) for t in text.split()]
    tokens = [t for t in tokens if t not in DROP_QUALIFIERS]
    if not tokens:
        return None
    out = [tokens[0]]
    i = 1
    if tokens[0] == HYBRID and len(tokens) > 1:  # a nothogenus: 'x Achnella caduca'
        out, i = [HYBRID, tokens[1]], 2
    broken = False  # an authorship token was skipped since the last kept epithet
    while i < len(tokens):
        t = tokens[i]
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        epithet_next = bool(_EPITHET.match(nxt)) and nxt not in AUTHOR_PARTICLES
        if (t in RANK_MARKERS and epithet_next) or (t == HYBRID and not broken and epithet_next and nxt != HYBRID):
            out += [t, nxt]
            i, broken = i + 2, False
            continue
        if not broken and _EPITHET.match(t) and t not in AUTHOR_PARTICLES and t != HYBRID:
            out.append(t)
        else:
            broken = True  # authors, years, '&', '(Griffiths)', 'sp. 1' ... are skipped
        i += 1
    return " ".join(out)


def _genus_of(name: str | None) -> str | None:
    if not name:
        return None
    first = name.split()[0]
    return first if _NAME_WORD.match(first) else None


def species_of(name: str | None) -> str | None:
    """The binomial of an infraspecific query ('A b var. c' / 'A b c' -> 'A b'); None for other names."""
    if not name:
        return None
    words = name.split()
    head = 1
    if words and words[0] == HYBRID:
        head = 2
    if len(words) < head + 1 or not _NAME_WORD.match(words[head - 1]):
        return None
    body = words[head:]
    if body and body[0] == HYBRID:  # nothospecies 'Abies x phanerolepis ...'
        if len(body) < 2 or not _EPITHET.match(body[1]):
            return None
        binomial, rest = words[: head + 2], body[2:]
    else:
        if not body or not _EPITHET.match(body[0]):
            return None
        binomial, rest = words[: head + 1], body[1:]
    return " ".join(binomial) if rest else None


@dataclass
class Taxon:
    """One distinct (taxonTypeCode, taxonID) of the site with everything needed to resolve it."""

    typeCode: str
    taxonID: str
    neonName: str | None
    queryName: str | None
    rank: str | None = None
    kingdom: str | None = None
    family: str | None = None
    genus: str | None = None
    nRecords: int = 0
    uses: list[str] = field(default_factory=list)  # "<product>/<table>"

    @property
    def key(self) -> tuple[str, str]:
        return self.typeCode, self.taxonID

    def name_for(self, method: str) -> str | None:
        """The query of one resolution level (None when the level does not apply)."""
        if method == "exact":
            return self.queryName
        if method in ("synonym", "species_fallback", "genus_fallback") and \
                (is_placeholder(self.queryName) or is_placeholder(self.genus)):
            return None
        if method == "synonym":
            return self.queryName
        if method == "species_fallback":
            return species_of(self.queryName)
        if method == "genus_fallback":
            g = self.genus or _genus_of(self.queryName)
            if not g or not _NAME_WORD.match(g) or (self.queryName or "").lower() == g.lower():
                return None
            return g
        f = self.family
        if not f or not _NAME_WORD.match(f):
            return None
        done = {(self.queryName or "").lower(), (self.name_for("genus_fallback") or "").lower()}
        return None if f.lower() in done else f

    def rank_for(self, method: str) -> str | None:
        """The rank the level's name has (for NCBI's '<rank>' homonym labels)."""
        if method in LEVEL_RANK:
            return LEVEL_RANK[method]
        r = (self.rank or "").strip().lower()
        return r or None


@dataclass
class TypeChoice:
    taxonTypeCode: str
    nTaxa: int
    nMatched: int
    matchRate: float
    tiedWith: list[str]


@dataclass
class Resolution:
    curie: str | None
    label: str | None
    method: str
    status: str
    queryName: str | None


def site_taxa(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Distinct (product, table, taxonID) of the site lake with the published name and record count."""
    if not site_lake.table_exists(con, "file_taxa"):
        return []
    rows = con.execute(
        "SELECT productCode, tableName, taxonID, arg_max(scientificName, nRecords) AS scientificName, "
        "arg_max(taxonRank, nRecords) AS taxonRank, arg_max(vernacularName, nRecords) AS vernacularName, "
        "sum(nRecords)::BIGINT AS nRecords FROM lake.file_taxa WHERE taxonID IS NOT NULL AND trim(taxonID) <> '' "
        "GROUP BY ALL ORDER BY ALL").fetchall()
    cols = ("productCode", "tableName", "taxonID", "scientificName", "taxonRank", "vernacularName", "nRecords")
    return [dict(zip(cols, r, strict=True)) for r in rows]


def choose_type_codes(con: duckdb.DuckDBPyConnection, taxa: list[dict[str, Any]],
                      has_catalog: bool = True) -> dict[tuple[str, str], TypeChoice]:
    """Per (product, table): the catalog taxonTypeCode with the best taxonID match rate.

    A tie on match rate is broken first by how many scientificNames agree with the catalog, then
    alphabetically. The other tied codes are reported in ``tiedWith``.
    """
    groups: dict[tuple[str, str], dict[str, str | None]] = defaultdict(dict)
    for t in taxa:
        groups[(t["productCode"], t["tableName"])][t["taxonID"]] = t.get("scientificName")
    out: dict[tuple[str, str], TypeChoice] = {}
    for key, ids in sorted(groups.items()):
        rates: list[tuple[str, int, int]] = []
        if has_catalog:
            rows = con.execute(
                "SELECT x.taxonTypeCode, count(DISTINCT x.taxonID), "
                "count(DISTINCT CASE WHEN lower(x.scientificName) = lower(s.name) THEN x.taxonID END) "
                "FROM neon.taxonomy x JOIN (SELECT unnest(?::VARCHAR[]) AS id, unnest(?::VARCHAR[]) AS name) s "
                "ON x.taxonID = s.id GROUP BY 1", [list(ids), [ids[i] or "" for i in ids]]).fetchall()
            rates = [(r[0], int(r[1]), int(r[2])) for r in rows]
        if not rates:
            out[key] = TypeChoice(UNKNOWN_TYPE, len(ids), 0, 0.0, [])
            continue
        best = max(r[1] for r in rates)
        tied = sorted([r for r in rates if r[1] == best], key=lambda r: (-r[2], r[0]))
        out[key] = TypeChoice(tied[0][0], len(ids), best, best / len(ids), [r[0] for r in tied[1:]])
    return out


def catalog_rows(con: duckdb.DuckDBPyConnection, type_code: str, ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    cols = ("taxonID", "acceptedTaxonID", "scientificName", "scientificNameAuthorship", "taxonRank", "kingdom",
            "family", "genus")
    ids = sorted(set(ids))
    if not ids or type_code == UNKNOWN_TYPE:
        return {}
    rows = con.execute(f"SELECT {', '.join(cols)} FROM neon.taxonomy WHERE taxonTypeCode = ? AND taxonID IN "
                       "(SELECT unnest(?::VARCHAR[]))", [type_code, ids]).fetchall()
    return {r[0]: dict(zip(cols, r, strict=True)) for r in rows}


def build_taxa(con: duckdb.DuckDBPyConnection, taxa: list[dict[str, Any]], choices: dict[tuple[str, str], TypeChoice],
               has_catalog: bool = True) -> list[Taxon]:
    """Merge the site taxa into distinct (type, taxonID) records; follow acceptedTaxonID in the catalog."""
    by_key: dict[tuple[str, str], Taxon] = {}
    raw_by_type: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for t in taxa:
        code = choices[(t["productCode"], t["tableName"])].taxonTypeCode
        raw_by_type[code].setdefault(t["taxonID"], t)
    catalog: dict[str, dict[str, dict[str, Any]]] = {}
    if has_catalog:
        for code, ids in raw_by_type.items():
            rows = catalog_rows(con, code, ids)
            accepted = {r["acceptedTaxonID"] for r in rows.values()
                        if r["acceptedTaxonID"] and r["acceptedTaxonID"] not in rows}
            rows.update(catalog_rows(con, code, accepted))
            catalog[code] = rows
    for t in taxa:
        code = choices[(t["productCode"], t["tableName"])].taxonTypeCode
        key = (code, t["taxonID"])
        rec = by_key.get(key)
        if rec is None:
            rows = catalog.get(code, {})
            row = rows.get(t["taxonID"])
            if row and row.get("acceptedTaxonID") and row["acceptedTaxonID"] != t["taxonID"]:
                acc = rows.get(row["acceptedTaxonID"])
                if acc:
                    log.info("taxon %s/%s -> accepted %s", code, t["taxonID"], row["acceptedTaxonID"])
                    row = acc
            name = (row or {}).get("scientificName") or t.get("scientificName")
            auth = (row or {}).get("scientificNameAuthorship")
            rec = Taxon(typeCode=code, taxonID=t["taxonID"], neonName=name, queryName=query_name(name, auth),
                        rank=(row or {}).get("taxonRank") or t.get("taxonRank"),
                        kingdom=(row or {}).get("kingdom"), family=(row or {}).get("family"),
                        genus=(row or {}).get("genus"))
            by_key[key] = rec
        rec.nRecords += int(t.get("nRecords") or 0)
        rec.uses.append(f"{t['productCode']}/{t['tableName']}")
    return sorted(by_key.values(), key=lambda r: r.key)


def _disambiguate(t: Taxon, name: str, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """More than one NCBITaxon term with the same label: keep those under the catalog kingdom."""
    root = KINGDOM_ROOTS.get((t.kingdom or "").strip().lower())
    if not root:
        return hits
    try:
        inside = store.exact_matches(store.search_ols(name, "ncbitaxon", SEARCH_SIZE, within=root), name, "NCBITaxon")
    except store.OLSUnavailable:
        return hits
    keep = {h["curie"] for h in inside}
    return [h for h in hits if h["curie"] in keep] or hits


def _fold(text: Any) -> str:
    return " ".join(str(text or "").split()).casefold()


_HOMONYM = re.compile(r"^(?P<name>.+?) <(?P<rank>[^>]+)>$")


def _label_names(label: Any) -> set[str]:
    """The folded names a label stands for: itself, and 'Aedes' for NCBI's homonym label 'Aedes <genus>'."""
    f = _fold(label)
    m = _HOMONYM.match(f)
    return {f, m.group("name")} if m else {f}


def pick_synonym(name: str, docs: list[dict[str, Any]], rank: str | None
                 ) -> tuple[list[dict[str, Any]], str | None]:
    """(candidates, via) from exact label/synonym hits of ``name``.

    ``via`` is 'label' (the label is the name, or NCBI's homonym label 'name <rank>' whose exact synonym
    is the name) or 'synonym'. One candidate means a pick; several mean an ambiguity for the caller.
    """
    want = _fold(name)
    docs = [d for d in docs if (d.get("curie") or "").startswith("NCBITaxon:")]
    label_eq = [d for d in docs if _fold(d.get("label")) == want]
    homonyms = [d for d in docs if (m := _HOMONYM.match(_fold(d.get("label")))) and m.group("name") == want]
    if len(label_eq) == 1:
        return label_eq, "label"
    by_rank = [d for d in homonyms if rank and _HOMONYM.match(_fold(d["label"])).group("rank") == rank.lower()]
    if not label_eq and len(by_rank) == 1:
        return by_rank, "label"
    if label_eq or homonyms:
        return label_eq + homonyms, "label"
    syn = store.synonym_matches(docs, name, "NCBITaxon")
    return syn, ("synonym" if syn else None)


def synonym_ok(synonyms: list[dict[str, Any]], name: str) -> bool:
    """Whether ``name`` is a synonym of an acceptable OBO type (same taxon, scientific) on the term record."""
    want = _fold(name)
    hits = [s for s in synonyms if _fold(s.get("name")) == want]
    return any((s.get("type") or "").strip().lower() not in BAD_SYNONYM_TYPES for s in hits)


def _kingdom_filter(pending_keys: set[tuple[str, str]], amb: dict[tuple[str, str], tuple[Taxon, str, list]]
                    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Ambiguous exact-search picks restricted to the catalog kingdom (one batch per kingdom root)."""
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_root: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for key, (t, _name, _cands) in amb.items():
        root = KINGDOM_ROOTS.get((t.kingdom or "").strip().lower())
        if root:
            by_root[root].append(key)
    for root, keys in by_root.items():
        names = sorted({amb[k][1] for k in keys})
        inside = store.search_exact_many(names, "ncbitaxon", SEARCH_SIZE, within=root)
        for k in keys:
            t, name, cands = amb[k]
            if name not in inside:
                pending_keys.add(k)
                continue
            keep = {d["curie"] for d in inside[name]}
            out[k] = [c for c in cands if c["curie"] in keep]
    return out


def _level_candidates(method: str, batch: list[tuple[Taxon, str]]
                      ) -> tuple[dict[tuple[str, str], tuple[list[dict[str, Any]], str]], set[tuple[str, str]]]:
    """Per taxon key: (candidate hits, via) of one level, and the keys whose lookups failed (pending)."""
    cands: dict[tuple[str, str], tuple[list[dict[str, Any]], str]] = {}
    pending: set[tuple[str, str]] = set()
    if method != "synonym":  # label search first (the v2 search, as before)
        hits = store.search_many(sorted({n for _t, n in batch}), "ncbitaxon", SEARCH_SIZE)
        for t, name in batch:
            if name not in hits:
                pending.add(t.key)
                continue
            ex = store.exact_matches(hits[name], name, "NCBITaxon")
            if len(ex) > 1:
                ex = _disambiguate(t, name, ex)
                if len(ex) > 1:
                    log.warning("ambiguous NCBITaxon label %r for %s/%s: %s (level skipped)", name, *t.key,
                                ", ".join(h["curie"] for h in ex))
                    ex = []
            if ex:
                cands[t.key] = (ex, "label")
    if method == "exact":
        return cands, pending
    todo = [(t, n) for t, n in batch if t.key not in cands and t.key not in pending]
    if not todo:
        return cands, pending
    docs = store.search_exact_many(sorted({n for _t, n in todo}), "ncbitaxon", SEARCH_SIZE)
    amb: dict[tuple[str, str], tuple[Taxon, str, list[dict[str, Any]]]] = {}
    for t, name in todo:
        if name not in docs:
            pending.add(t.key)
            continue
        found, via = pick_synonym(name, docs[name], t.rank_for(method))
        if len(found) == 1 and via:
            cands[t.key] = (found, via)
        elif len(found) > 1:
            amb[t.key] = (t, name, found)
    if amb:
        narrowed = _kingdom_filter(pending, amb)
        for key, (t, name, found) in amb.items():
            keep = narrowed.get(key, [])
            if len(keep) == 1:
                cands[key] = (keep, "label" if _fold(name) in _label_names(keep[0]["label"]) else "synonym")
            elif key not in pending:
                log.warning("ambiguous NCBITaxon name %r for %s/%s: %s (level skipped)", name, *key,
                            ", ".join(h["curie"] for h in found))
    syn_curies = {h["curie"] for hs, via in cands.values() if via == "synonym" for h in hs}
    if syn_curies:
        syns = store.term_synonyms(syn_curies)
        names = dict(batch_names(batch))
        for key, (hs, via) in list(cands.items()):
            if via != "synonym":
                continue
            c = hs[0]["curie"]
            if c not in syns:
                pending.add(key)
                del cands[key]
            elif not synonym_ok(syns[c], names[key]):
                log.info("%s/%s: %r is only a %s synonym of %s (not used)", *key, names[key],
                         ",".join(sorted({str(s.get('type')) for s in syns[c] if _fold(s.get('name'))
                                          == _fold(names[key])})), c)
                del cands[key]
    return cands, pending


def batch_names(batch: list[tuple[Taxon, str]]) -> list[tuple[tuple[str, str], str]]:
    return [(t.key, n) for t, n in batch]


def resolve_taxa(taxa: list[Taxon]) -> dict[tuple[str, str], Resolution]:
    """Level by level (exact → synonym → species → genus → family), each level searched and verified in a batch."""
    results: dict[tuple[str, str], Resolution] = {}
    obsolete: dict[tuple[str, str], Resolution] = {}
    for method in TAXON_LEVELS:
        batch = [(t, n) for t in taxa if t.key not in results and (n := t.name_for(method))]
        if not batch:
            continue
        cands, pending = _level_candidates(method, batch)
        for t, _name in batch:
            if t.key in pending:
                results[t.key] = Resolution(None, None, method, "pending", t.queryName)
        terms = store.ols_terms({h["curie"] for ex, _via in cands.values() for h in ex})
        for t, _name in batch:
            if t.key in results:
                continue
            ex = (cands.get(t.key) or ([], None))[0]
            if not ex:
                continue
            good = [h for h in ex if terms.get(h["curie"]) and not terms[h["curie"]]["isObsolete"]]
            if good:
                term = terms[good[0]["curie"]]
                results[t.key] = Resolution(term["curie"], term["label"], method, "ok", t.queryName)
            elif any(h["curie"] not in terms for h in ex):
                results[t.key] = Resolution(None, None, method, "pending", t.queryName)
            else:
                dead = next((terms[h["curie"]] for h in ex if terms.get(h["curie"])), None)
                if dead and t.key not in obsolete:
                    obsolete[t.key] = Resolution(dead["curie"], dead["label"], method, "obsolete", t.queryName)
    for t in taxa:
        if t.key not in results:
            results[t.key] = obsolete.get(t.key) or Resolution(None, None, "unresolved", "unresolved", t.queryName)
    return results


def _search_hints(name: str | None, n: int = 3) -> str:
    """Top OLS hits of the exact-name search (cached), e.g. a current name when NEON uses a synonym."""
    if not name:
        return ""
    try:
        hits = store.search_ols(name, "ncbitaxon", SEARCH_SIZE)
    except store.OLSUnavailable:
        return ""
    return "; ".join(f"{h['curie']} {h['label']}" for h in hits[:n])


def run_taxa(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> dict[str, Any]:
    raw = site_taxa(con)
    choices = choose_type_codes(con, raw, has_catalog)
    for (product, table), ch in choices.items():
        if ch.tiedWith:
            log.warning("taxonTypeCode tie for %s/%s: %s chosen over %s (match %.1f%%)", product, table,
                        ch.taxonTypeCode, ", ".join(ch.tiedWith), 100 * ch.matchRate)
    taxa = build_taxa(con, raw, choices, has_catalog)
    with store.store(read_only=True) as sc:
        existing = {(r[0], r[1]): r for r in sc.execute(
            "SELECT taxonTypeCode, taxonID, neonName, queryName, curie, label, method, status "
            "FROM ref_ncbitaxon_map").fetchall()}
    # 'ok' exact/synonym rows are final; fallbacks and failures are retried (searches come from the cache)
    todo = [t for t in taxa if (row := existing.get(t.key) or (None,) * 8)[7] != "ok" or row[6] not in KEEP_METHODS
            or (row[6] != "exact" and is_placeholder(row[3]))]
    res = resolve_taxa(todo) if todo else {}
    now = env.utcnow()
    with store.store() as sc:
        if res:
            sc.executemany(
                "INSERT OR REPLACE INTO ref_ncbitaxon_map (taxonTypeCode, taxonID, neonName, queryName, curie, "
                "label, method, status, resolvedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [[t.typeCode, t.taxonID, t.neonName, r.queryName, r.curie, r.label, r.method, r.status, now]
                 for t in todo for r in [res[t.key]]])
        sc.execute("DELETE FROM terms_taxon_types WHERE siteCode = ?", [site])
        if choices:
            sc.executemany("INSERT INTO terms_taxon_types VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           [[site, p, tb, c.taxonTypeCode, c.nTaxa, c.nMatched, c.matchRate, c.tiedWith, now]
                            for (p, tb), c in choices.items()])
        final = {(r[0], r[1]): r for r in sc.execute(
            "SELECT taxonTypeCode, taxonID, neonName, queryName, curie, label, method, status "
            "FROM ref_ncbitaxon_map").fetchall()}
    counts: Counter[str] = Counter()
    review: list[dict[str, Any]] = []
    for t in taxa:
        row = final.get(t.key)
        method, status = (row[6], row[7]) if row else ("unresolved", "unresolved")
        counts[method if status == "ok" else status] += 1
        if status != "ok" or method != "exact":
            review.append({"taxonTypeCode": t.typeCode, "taxonID": t.taxonID, "neonName": t.neonName,
                           "queryName": row[3] if row else t.queryName, "taxonRank": t.rank,
                           "family": t.family, "nRecords": t.nRecords, "status": status, "method": method,
                           "curie": row[4] if row else None, "label": row[5] if row else None,
                           "searchHits": _search_hints(row[3] if row else t.queryName),
                           "usedIn": ";".join(sorted(set(t.uses)))})
    path = site_lake.site_paths(site).reports_dir / "unresolved_taxa.csv"
    _write_csv(path, review, ["taxonTypeCode", "taxonID", "neonName", "queryName", "taxonRank", "family",
                              "nRecords", "status", "method", "curie", "label", "searchHits", "usedIn"])
    return {"distinct": len(taxa), "resolvedNow": len(res), "keptOk": len(taxa) - len(todo),
            "counts": {k: counts.get(k, 0) for k in TAXON_COUNT_KEYS},
            "typeChoices": [{"productCode": p, "tableName": tb, **vars(c)} for (p, tb), c in choices.items()],
            "report": str(path.relative_to(env.ROOT))}


# ---------------------------------------------------------------- units

# NEON unit -> UO *label* or labels in order (never a CURIE: the CURIE always comes from an OLS search).
UNIT_SEED: dict[str, str | tuple[str, ...]] = {
    "celsius": "degree Celsius", "kelvin": "kelvin", "percent": "percent", "meter": "meter",
    "millimeter": "millimeter", "centimeter": "centimeter", "kilometer": "kilometer", "micrometer": "micrometer",
    "nanometer": "nanometer", "second": "second", "minute": "minute", "hour": "hour", "day": "day",
    "week": "week", "year": "year", "decimalDegree": "degree", "degree": "degree",
    "metersPerSecond": "meter per second",
    "wattsPerSquareMeter": "watt per square meter", "wattsPerMeterSquared": "watt per square meter",
    "kilopascal": "kilopascal", "pascal": "pascal", "hectopascal": "hectopascal", "gram": "gram",
    "kilogram": "kilogram", "milligram": "milligram", "microgram": "microgram",
    "gramsPerSquareMeter": "gram per square meter", "milligramsPerLiter": "milligram per liter",
    "microgramsPerLiter": "microgram per liter",
    "micromolesPerSquareMeterPerSecond": "microeinstein per square meter per second",
    "partsPerMillion": "parts per million", "partsPerBillion": "parts per billion", "number": "count unit",
    "liter": "liter", "milliliter": "milliliter", "microliter": "microliter", "squareMeter": "square meter",
    "squareCentimeter": "square centimeter", "squareKilometer": "square kilometer", "hectare": "hectare",
    "cubicMeter": "cubic meter", "cubicCentimeter": "cubic centimeter", "pH": "pH",
    "unitless": "dimensionless unit", "dimensionless": "dimensionless unit",
    # found in NEON variables files (neon-downloads birds/beetles): a nominal day of sampling effort
    "nominalDay": "day",
    # probe round 2026-09-27 (ARIK … OSBS): nominal durations, concentrations, isotope deltas, volumes
    "nominalHour": "hour", "nominalYear": "year",
    "millimolesPerLiter": "millimolar", "micromolesPerLiter": "micromolar",
    "equivalentsPerLiter": "normal",  # normality: gram equivalents per liter (UO 'normal', N)
    "milliequivalentsPerLiter": "millinormal",
    "permill": ("parts per thousand", "permil"),  # the per-mil sign of delta values
    "partsPerMillionVolumetric": "parts per million",
    "litersPerSecond": "liter per second", "millilitersPerMinute": "milliliter per minute",
    "cubicMicrometer": "cubic micrometer",
    "numberPerMilliliter": "count per milliliter",
    "microsiemensPerCentimeter": "microsiemens per centimeter",
    "arcMinute": ("arcminute", "minute of arc"), "arcSecond": ("arcsecond", "second of arc"),
}
_KEEP_S = {"celsius", "siemens", "lux", "series", "hertz", "gauss"}
_IRREGULAR = {"inches": "inch", "feet": "foot"}
LENGTH_WORDS = frozenset({"meter", "decimeter", "centimeter", "millimeter", "micrometer", "nanometer", "kilometer",
                          "foot", "inch", "yard", "mile"})
_SPELLING = (("litre", "liter"), ("metre", "meter"))  # UK -> US, applied inside words (millilitre, centimetre)


def unit_words(neon_unit: str) -> list[str]:
    """'kilometersPerHour' -> ['kilometer', 'per', 'hour'] (camelCase split, singularised)."""
    words = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", neon_unit)
    out = []
    for w in (x.lower() for x in words):
        if w in _IRREGULAR:
            w = _IRREGULAR[w]
        elif w.endswith("s") and len(w) > 3 and w not in _KEEP_S and not w.endswith(("ss", "us")):
            w = w[:-1]
        out.append(w)
    return out


def is_variance_unit(neon_unit: str) -> bool:
    """A NEON '<unit>Squared' that is a variance, not an area.

    'celsiusSquared', 'metersPerSecondSquared' (NEON: wind-speed variance only), 'wattsPerSquareMeterSquared'
    and 'cubicCentimetersPerCubicCentimeterSquared' are variances; 'metersSquared' and 'wattsPerMeterSquared'
    are areas (a bare length unit squared).
    """
    if not neon_unit.endswith("Squared") or neon_unit == "Squared":
        return False
    words = unit_words(neon_unit[: -len("Squared")])
    if not words:
        return False
    return not (words[-1] in LENGTH_WORDS and (len(words) < 2 or words[-2] not in ("square", "cubic")))


def unit_label_variants(neon_unit: str) -> list[str]:
    """Candidate UO labels for the 'exact' method (plain words, then 'X squared'->'square X' etc.)."""
    words = unit_words(neon_unit)
    if not words:
        return []
    out = [" ".join(words)]
    for suffix, prefix in (("squared", "square"), ("cubed", "cubic")):
        if suffix in words:
            i = words.index(suffix)
            if i > 0:
                moved = words[: i - 1] + [prefix, words[i - 1]] + words[i + 1:]
                out.append(" ".join(moved))
    return list(dict.fromkeys(out))


def seed_labels(neon_unit: str) -> list[str]:
    seed = UNIT_SEED.get(neon_unit)
    if seed is None:
        return []
    return [seed] if isinstance(seed, str) else list(seed)


def unit_fold(label: Any) -> str:
    """Case-, whitespace- and US/UK-spelling-insensitive form of a unit label ('Litre' == 'liter')."""
    s = " ".join(str(label or "").split()).casefold()
    for uk, us in _SPELLING:
        s = s.replace(uk, us)
    return s


def spellings(label: str) -> list[str]:
    """The label and its UK spelling ('micromole per liter' -> also 'micromole per litre')."""
    uk = label
    for u, a in _SPELLING:
        uk = uk.replace(a, u)
    return list(dict.fromkeys([label, uk]))


def _uo_rank(curie: str) -> tuple[int, str]:
    """Prefer UO:0000xxx units over UO:1xxxxxx 'based unit' classes."""
    local = curie.split(":", 1)[1]
    return (1 if local.startswith("1") else 0, local)


def _uo_label_hits(hits: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    want = unit_fold(label)
    return sorted({h["curie"]: h for h in hits if (h.get("curie") or "").startswith("UO:")
                   and unit_fold(h.get("label")) == want}.values(), key=lambda h: _uo_rank(h["curie"]))


def _uo_synonym_hits(hits: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    want = unit_fold(label)
    return sorted({h["curie"]: h for h in hits if (h.get("curie") or "").startswith("UO:")
                   and any(unit_fold(x) == want for x in h.get("synonyms") or [])}.values(),
                  key=lambda h: _uo_rank(h["curie"]))


def _live(cands: list[dict[str, Any]], terms: dict[str, dict[str, Any] | None]) -> dict[str, Any] | None:
    for h in cands:
        t = terms.get(h["curie"])
        if t and not t["isObsolete"]:
            return t
    return None


def _pick_uo(hits: list[dict[str, Any]], label: str, terms: dict[str, dict[str, Any] | None]) -> dict[str, Any] | None:
    return _live(_uo_label_hits(hits, label), terms)


def site_units(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, Any]]:
    if not site_lake.table_exists(con, "variables"):
        return {}
    rows = con.execute(
        "SELECT trim(units) AS u, count(*)::BIGINT, list(DISTINCT productCode || '/' || tableName || '.' || fieldName "
        "ORDER BY productCode || '/' || tableName || '.' || fieldName) FROM lake.variables "
        "WHERE units IS NOT NULL AND trim(units) <> '' AND upper(trim(units)) NOT IN ('NA', 'N/A', 'NONE', 'NULL') "
        "GROUP BY 1 ORDER BY 1").fetchall()
    return {r[0]: {"nFields": int(r[1]), "fields": list(r[2])} for r in rows}


NOT_APPLICABLE = {"uoCurie": None, "uoLabel": None, "method": "not_applicable", "status": "not_applicable"}


def variance_by_fields(units: dict[str, dict[str, Any]]) -> set[str]:
    """'…Squared' units whose every field is a '…Variance' field ('microsiemensPerCentimeterSquared' is
    groundwaterSpecCondVariance, although its name reads like a per-area unit)."""
    return {u for u, info in units.items() if u.endswith("Squared") and info.get("fields")
            and all("variance" in f.rsplit(".", 1)[-1].lower() for f in info["fields"])}


def resolve_units(units: Iterable[str], variance: Iterable[str] = ()) -> dict[str, dict[str, Any]]:
    """NEON unit -> {uoCurie, uoLabel, method, status}.

    Variance units (by name, or listed in ``variance`` from their fields) are ``not_applicable``. Then
    seed labels, then camelCase variants, matched first on the label (OLS search), then on the label or
    a synonym (exact OLS v1 search, both spellings); else unmapped (or pending when OLS could not answer).
    """
    units = sorted(set(units))
    variance = set(variance)
    out: dict[str, dict[str, Any]] = {u: dict(NOT_APPLICABLE) for u in units
                                      if is_variance_unit(u) or u in variance}
    todo = [u for u in units if u not in out]
    seeds = {u: seed_labels(u) for u in todo}
    variants = {u: unit_label_variants(u) for u in todo}
    queries = {q for u in todo for q in (*seeds[u], *variants[u])}
    hits = store.search_many(sorted(queries), "uo", SEARCH_SIZE)
    curies = {h["curie"] for q in queries if q in hits for h in _uo_label_hits(hits[q], q)}
    terms = store.ols_terms(curies)
    unavailable = {q for q in queries if q not in hits}
    picked: dict[str, tuple[dict[str, Any], str]] = {}
    for u in todo:
        for method, labels in (("seed", seeds[u]), ("exact", variants[u])):
            got = next((t for q in labels if q in hits and (t := _pick_uo(hits[q], q, terms))), None)
            if got:
                picked[u] = (got, method)
                break
    # second pass: label or synonym, US and UK spelling (exact OLS v1 search)
    rest = [u for u in todo if u not in picked]
    ex_queries = {v for u in rest for q in (*seeds[u], *variants[u]) for v in spellings(q)}
    ex_hits = store.search_exact_many(sorted(ex_queries), "uo", SEARCH_SIZE) if ex_queries else {}
    unavailable |= {q for q in ex_queries if q not in ex_hits}
    ex_cands: dict[str, list[tuple[str, list[dict[str, Any]]]]] = {}
    for u in rest:
        # a seed label (label or synonym) -> 'seed'; a camelCase variant: label (any spelling) -> 'exact',
        # synonym only -> 'synonym'
        for label_method, syn_method, labels in (("seed", "seed", seeds[u]), ("exact", "synonym", variants[u])):
            for q in labels:
                docs = [d for v in spellings(q) for d in ex_hits.get(v) or []]
                for method, found in ((label_method, _uo_label_hits(docs, q)),
                                      (syn_method, _uo_synonym_hits(docs, q))):
                    if found:
                        ex_cands.setdefault(u, []).append((method, found))
    ex_terms = store.ols_terms({h["curie"] for cs in ex_cands.values() for _m, hs in cs for h in hs})
    terms.update(ex_terms)
    for u in rest:
        for method, hs in ex_cands.get(u, []):
            got = _live(hs, terms)
            if got:
                picked[u] = (got, method)
                break
    missing_terms = any(c not in terms for c in curies) or any(
        h["curie"] not in terms for cs in ex_cands.values() for _m, hs in cs for h in hs)
    for u in todo:
        if u in picked:
            t, method = picked[u]
            out[u] = {"uoCurie": t["curie"], "uoLabel": t["label"], "method": method, "status": "ok"}
            if method == "synonym" or (method == "seed" and u in rest):
                log.info("NEON unit %r -> %s %r via a UO synonym or spelling variant", u, t["curie"], t["label"])
        else:
            wanted = {v for q in (*seeds[u], *variants[u]) for v in (q, *spellings(q))}
            if wanted & unavailable or missing_terms:
                out[u] = {"uoCurie": None, "uoLabel": None, "method": "unmapped", "status": "pending"}
            else:
                if seeds[u]:
                    log.warning("seed label(s) %s for NEON unit %r have no live UO match (label or synonym)",
                                seeds[u], u)
                out[u] = {"uoCurie": None, "uoLabel": None, "method": "unmapped", "status": "unmapped"}
    return out


UNIT_COUNT_KEYS = ("seed", "exact", "synonym", "curated", "unmapped", "pending", "not_applicable")


def run_units(con: duckdb.DuckDBPyConnection, site: str) -> dict[str, Any]:
    units = site_units(con)
    with store.store(read_only=True) as sc:
        curated = {r[0] for r in sc.execute("SELECT neonUnit FROM ref_unit_map WHERE method = 'curated'").fetchall()}
    todo = [u for u in units if u not in curated]
    res = resolve_units(todo, variance_by_fields(units)) if todo else {}
    now = env.utcnow()
    with store.store() as sc:
        if res:
            sc.executemany("INSERT OR REPLACE INTO ref_unit_map (neonUnit, uoCurie, uoLabel, method, status, "
                           "validatedAt) VALUES (?, ?, ?, ?, ?, ?)",
                           [[u, r["uoCurie"], r["uoLabel"], r["method"], r["status"], now] for u, r in res.items()])
        final = {r[0]: r for r in sc.execute(
            "SELECT neonUnit, uoCurie, uoLabel, method, status FROM ref_unit_map").fetchall()}
    counts: Counter[str] = Counter()
    unmapped: list[dict[str, Any]] = []
    for u, info in units.items():
        row = final.get(u)
        status = row[4] if row else "unmapped"
        counts[row[3] if row and status == "ok" else status] += 1
        if status not in ("ok", "not_applicable"):
            unmapped.append({"neonUnit": u, "status": status, "nFields": info["nFields"],
                             "candidateLabels": "; ".join([*seed_labels(u), *unit_label_variants(u)]),
                             "fields": "; ".join(info["fields"][:10])})
    path = site_lake.site_paths(site).reports_dir / "unmapped_units.csv"
    _write_csv(path, unmapped, ["neonUnit", "status", "nFields", "candidateLabels", "fields"])
    mapped = sum(v for k, v in counts.items() if k in ("seed", "exact", "synonym", "curated"))
    return {"distinct": len(units), "mapped": mapped, "unmapped": len(unmapped),
            "notApplicable": counts.get("not_applicable", 0),
            "counts": {k: counts.get(k, 0) for k in UNIT_COUNT_KEYS},
            "map": {u: {"uoCurie": final[u][1], "uoLabel": final[u][2], "method": final[u][3],
                        "status": final[u][4]} for u in units if u in final},
            "report": str(path.relative_to(env.ROOT))}


# ---------------------------------------------------------------- NLCD -> ENVO

# NLCD class -> (ENVO CURIE, expected OLS label); every row checked live in OLS (2026-09-27).
NLCD_ENVO: dict[str, tuple[str, str]] = {
    "deciduousForest": ("ENVO:01000816", "area of deciduous forest"),
    "evergreenForest": ("ENVO:01000843", "area of evergreen forest"),
    "mixedForest": ("ENVO:01000855", "area of mixed forest"),
    "woodyWetlands": ("ENVO:01000893", "area of woody wetland"),
    "shrubScrub": ("ENVO:01000869", "area of scrub"),
    "dwarfScrub": ("ENVO:01000861", "area of dwarf scrub"),
    "openWater": ("ENVO:01000666", "area of open water"),
    "barrenLand": ("ENVO:01000752", "area of barren land"),
    "developedOpenSpace": ("ENVO:01000883", "area of developed open space"),
    "developedLowIntensity": ("ENVO:01000884", "area of developed space with low usage intensity"),
    "developedMediumIntensity": ("ENVO:01000885", "area of developed space with medium usage intensity"),
    "developedHighIntensity": ("ENVO:01000886", "area of developed space with high usage intensity"),
    "perennialIceSnow": ("ENVO:01000746", "area of perennial ice or snow"),
    "cultivatedCrops": ("ENVO:01000892", "area of cropland"),
    "pastureHay": ("ENVO:01000891", "area of pastureland or hayfields"),
    "emergentHerbaceousWetlands": ("ENVO:01000894", "area of emergent herbaceous wetland"),
    "grasslandHerbaceous": ("ENVO:01000888", "area of gramanoid or herbaceous vegetation"),
    "lichens": ("ENVO:01000889", "area of lichen-dominated vegetation"),
    "moss": ("ENVO:01000890", "area of moss-dominated vegetation"),
    "sedgeHerbaceous": ("ENVO:01000887", "area of sedge- and forb-dominated herbaceous vegetation"),
}


def resolve_nlcd() -> tuple[dict[str, tuple[str, str]], list[dict[str, Any]]]:
    """Verified NLCD rows {class: (curie, label)} and a list of issues (label change, obsolete, replaced)."""
    terms = store.ols_terms(c for c, _l in NLCD_ENVO.values())
    rows: dict[str, tuple[str, str]] = {}
    issues: list[dict[str, Any]] = []
    for cls, (curie, expected) in NLCD_ENVO.items():
        if curie not in terms:
            issues.append({"nlcdClass": cls, "curie": curie, "issue": "OLS unavailable (row left unchanged)"})
            continue
        t = terms[curie]
        if t and not t["isObsolete"] and (t["label"] or "").strip().lower() == expected.lower():
            rows[cls] = (curie, t["label"])
            continue
        why = "not found" if t is None else ("obsolete" if t["isObsolete"] else f"label is {t['label']!r}")
        repl = None
        try:
            ex = store.exact_matches(store.search_ols(expected, "envo", SEARCH_SIZE), expected, "ENVO")
            got = store.ols_terms(h["curie"] for h in ex)
            repl = next((got[h["curie"]] for h in ex if got.get(h["curie"]) and not got[h["curie"]]["isObsolete"]),
                        None)
        except store.OLSUnavailable:
            pass
        if repl:
            rows[cls] = (repl["curie"], repl["label"])
            issues.append({"nlcdClass": cls, "curie": curie, "issue": why, "replacement": repl["curie"]})
        elif t and not t["isObsolete"]:
            rows[cls] = (curie, t["label"])
            issues.append({"nlcdClass": cls, "curie": curie, "issue": why + "; kept (no exact ENVO replacement)"})
        else:
            issues.append({"nlcdClass": cls, "curie": curie, "issue": why + "; no replacement (row not stored)"})
    return rows, issues


def run_nlcd() -> dict[str, Any]:
    rows, issues = resolve_nlcd()
    with store.store() as sc:
        if rows:
            sc.executemany("INSERT OR REPLACE INTO ref_nlcd_envo (nlcdClass, envoCurie, envoLabel) VALUES (?, ?, ?)",
                           [[k, c, lb] for k, (c, lb) in sorted(rows.items())])
    for i in issues:
        log.warning("NLCD %s (%s): %s", i["nlcdClass"], i["curie"], i["issue"])
    return {"verified": len(rows), "issues": issues}


# ---------------------------------------------------------------- site biome + environmental feature

BIOME_ROOT = "ENVO:00000428"
FEATURE_ROOT = "ENVO:00000063"  # 'water body': stream, river, lake, pond …
AQUATIC_BIOME = "ENVO:00002030"
TERRESTRIAL = "Terrestrial"
# HABITAT -> (rule, biome labels, feature labels); every label is checked live in OLS.
AQUATIC_RULES: dict[str, tuple[str, list[str], list[str]]] = {
    "aquatic wadeable stream": ("aquatic:wadeable stream", ["freshwater stream biome", "freshwater river biome"],
                                ["stream"]),
    "aquatic non-wadeable stream": ("aquatic:non-wadeable stream", ["freshwater river biome",
                                                                    "freshwater stream biome"], ["river", "stream"]),
    "aquatic lake": ("aquatic:lake", ["freshwater lake biome"], ["lake"]),
}
_CONIFER = ("pine", "fir", "spruce", "hemlock", "cedar", "conifer", "juniper", "larch")
_BROADLEAF = ("deciduous", "hardwood", "oak", "maple", "aspen", "tulip", "hickory", "beech", "birch", "elm", "ash")


def is_aquatic(habitat: str | None) -> bool:
    """HABITAT is one of the 'Aquatic …' classes (or the bare 'Aquatic')."""
    return (habitat or "").strip().lower().startswith("aquatic")


def _aquatic_by_name(site_name: str | None) -> tuple[str, list[str], list[str]]:
    """A bare 'Aquatic' HABITAT: guess the water body from the site name."""
    name = (site_name or "").lower()
    if "lake" in name or "pond" in name:
        return "aquatic:lake", ["freshwater lake biome"], ["lake"]
    if "river" in name:
        return "aquatic:river", ["freshwater river biome", "freshwater stream biome"], ["river"]
    if any(w in name for w in ("creek", "run", "brook", "stream", "branch")):
        return "aquatic:stream", ["freshwater stream biome", "freshwater river biome"], ["stream"]
    return "aquatic:none", [], []


def aquatic_candidates(habitat: str | None, site_name: str | None) -> tuple[str, list[str], list[str]]:
    """(rule, biome labels, feature labels) of an aquatic site, from HABITAT (never TypeEco)."""
    return AQUATIC_RULES.get(" ".join((habitat or "").lower().split())) or _aquatic_by_name(site_name)


def biome_candidates(type_eco: str | None, nlcd: str | None, state: str | None, habitat: str | None,
                     site_name: str | None) -> tuple[str, list[str]]:
    """(rule, ordered candidate ENVO biome labels) from HABITAT, TypeEco, the dominant NLCD class and state.

    Aquatic sites use HABITAT (then the site name) only. The rules are keyword based, deterministic and
    deliberately coarse. An empty list means no guess, and the row stays pending.
    """
    if is_aquatic(habitat):
        rule, biomes, _features = aquatic_candidates(habitat, site_name)
        return rule, biomes
    t = " ".join((type_eco or "").lower().split())
    tropical = (state or "").upper() in ("PR", "HI")
    forest = any(w in t for w in ("forest", "woodland"))
    if "tundra" in t:
        return "tundra", ["tundra biome"]
    if "desert" in t and ("shrub" in t or "scrub" in t or nlcd in ("shrubScrub", "dwarfScrub")):
        return "desert+shrub", ["xeric shrubland biome", "desert biome"]
    if "desert" in t:
        return "desert", ["desert biome"]
    if "savanna" in t:
        return "savanna", ["tropical savanna biome"] if tropical else ["temperate savanna biome", "savanna biome"]
    if forest and tropical:
        if "dry" in t:
            return "tropical dry forest", ["tropical dry broadleaf forest biome"]
        return "tropical moist forest", ["tropical moist broadleaf forest biome"]
    if "rainforest" in t:
        return "rainforest", ["tropical moist broadleaf forest biome"]
    if forest and ("permafrost" in t or ("spruce" in t and (state or "").upper() == "AK")):
        return "boreal forest", ["boreal forest biome", "taiga"]
    if forest:
        conifer = any(w in t for w in _CONIFER)
        broad = any(re.search(rf"\b{w}", t) for w in _BROADLEAF)
        if (conifer and broad) or ("mixed" in t and not conifer and not broad):
            return "mixed forest", ["temperate mixed forest biome"]
        if conifer:
            return "coniferous forest", ["temperate coniferous forest biome"]
        if broad:
            return "broadleaf forest", ["temperate broadleaf forest biome"]
    if any(w in t for w in ("prairie", "grassland", "steppe", "grass")):
        return "grassland", [] if tropical else ["temperate grassland biome", "grassland biome"]
    if "shrub" in t:
        return "shrubland", ["shrubland biome"]
    if any(w in t for w in ("agricultur", "agronomic", "argonomic", "arable", "crop", "farm field")):
        return "cropland", ["cropland biome"]
    by_nlcd = {"shrubScrub": ["shrubland biome"], "deciduousForest": ["temperate broadleaf forest biome"],
               "evergreenForest": ["temperate coniferous forest biome"], "mixedForest": ["temperate mixed forest biome"],
               "grasslandHerbaceous": ["temperate grassland biome"], "cultivatedCrops": ["cropland biome"],
               "dwarfScrub": ["tundra biome"], "sedgeHerbaceous": ["tundra biome"]}
    cands = by_nlcd.get(nlcd or "", [])
    if tropical:
        cands = [c for c in cands if not c.startswith("temperate")]
    return (f"nlcd:{nlcd}" if cands else "none"), cands


def feature_candidates(habitat: str | None, site_name: str | None) -> tuple[str, list[str]]:
    """(rule, ENVO water-body labels) of an aquatic site; terrestrial sites get no feature proposal."""
    if not is_aquatic(habitat):
        return "terrestrial", []
    rule, _biomes, features = aquatic_candidates(habitat, site_name)
    return rule, features


def site_context(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> dict[str, Any]:
    """HABITAT, TypeEco (terrestrial only), identity, location and the NLCD composition (latest year) of a site."""
    ctx: dict[str, Any] = {"typeEco": None, "stateCode": None, "habitat": None, "siteName": None,
                           "nlcdYear": None, "nlcd": []}
    if not has_catalog:
        return ctx
    cols = {"siteName": "siteName", "stateCode": "stateCode", "habitat": "siteProperties['HABITAT']",
            "typeEco": "siteProperties['TypeEco']", "stateName": "stateName", "domainCode": "domainCode",
            "domainName": "domainName", "siteType": "siteType", "siteDescription": "siteDescription",
            "siteLatitude": "siteLatitude", "siteLongitude": "siteLongitude", "siteElevation": "siteElevation"}
    have = site_lake.table_columns(con, "sites", alias="neon") if site_lake.table_exists(con, "sites", alias="neon") \
        else {}
    sel = {k: v for k, v in cols.items() if v.split("[")[0] in have}
    if sel:
        row = con.execute(f"SELECT {', '.join(sel.values())} FROM neon.sites WHERE siteCode = ?", [site]).fetchone()
        if row:
            ctx.update(dict(zip(sel, row, strict=True)))
    aquatic = is_aquatic(ctx["habitat"])
    if not ctx["typeEco"] and not aquatic and site_lake.table_exists(con, "site_locations", alias="neon"):
        loc = con.execute("SELECT je.value->>'locationPropertyValue' FROM neon.site_locations sl, "
                          "json_each(sl.locationProperties) je WHERE sl.siteCode = ? AND sl.locationType = 'SITE' "
                          "AND je.value->>'locationPropertyName' = 'Value for TypeEco'", [site]).fetchone()
        ctx["typeEco"] = loc[0] if loc else None
    if aquatic:
        # NEON copied one shortgrass-steppe TypeEco to every aquatic site: never use it (docs/SCALING.md).
        ctx["typeEcoIgnored"], ctx["typeEco"] = ctx["typeEco"], None
    elif site_lake.table_exists(con, "neon_tos_nlcdcoverbysite", alias="neon"):
        rows = con.execute(
            "WITH y AS (SELECT max(nlcdYear) AS yr FROM neon.neon_tos_nlcdcoverbysite WHERE siteID = ?) "
            "SELECT nlcdClass, sum(area_km2) FROM neon.neon_tos_nlcdcoverbysite, y WHERE siteID = ? AND nlcdYear = y.yr "
            "GROUP BY 1 ORDER BY 2 DESC, 1", [site, site]).fetchall()
        total = sum(r[1] or 0 for r in rows) or 0
        ctx["nlcdYear"] = con.execute("SELECT max(nlcdYear) FROM neon.neon_tos_nlcdcoverbysite WHERE siteID = ?",
                                      [site]).fetchone()[0] if rows else None
        ctx["nlcd"] = [{"nlcdClass": r[0], "areaKm2": r[1], "share": (r[1] or 0) / total if total else None}
                       for r in rows]
    return ctx


def _propose(cands: list[str], root: str, note_empty: str) -> dict[str, Any]:
    """The first candidate label that is a live ENVO term under ``root``."""
    out: dict[str, Any] = {"candidates": cands, "envoCurie": None, "envoLabel": None, "method": "pending",
                           "status": "pending"}
    if not cands:
        return out
    hits = store.search_many(cands, "envo", SEARCH_SIZE, within=root)
    if any(c not in hits for c in cands):
        out["note"] = "OLS unavailable"
        return out
    exact = {c: store.exact_matches(hits[c], c, "ENVO") for c in cands}
    terms = store.ols_terms(h["curie"] for ex in exact.values() for h in ex)
    for c in cands:
        good = [terms[h["curie"]] for h in exact[c] if terms.get(h["curie"]) and not terms[h["curie"]]["isObsolete"]]
        if good:
            out.update(envoCurie=good[0]["curie"], envoLabel=good[0]["label"], method="heuristic", status="proposed")
            return out
    out["note"] = note_empty
    return out


def propose_biome(ctx: dict[str, Any]) -> dict[str, Any]:
    dominant = ctx["nlcd"][0]["nlcdClass"] if ctx.get("nlcd") else None
    typ = None if is_aquatic(ctx.get("habitat")) else ctx.get("typeEco")
    rule, cands = biome_candidates(typ, dominant, ctx.get("stateCode"), ctx.get("habitat"), ctx.get("siteName"))
    out = _propose(cands, BIOME_ROOT, "no candidate label is a live ENVO biome")
    return {"rule": rule, "nlcdDominant": dominant, **out}


def propose_feature(ctx: dict[str, Any]) -> dict[str, Any]:
    rule, cands = feature_candidates(ctx.get("habitat"), ctx.get("siteName"))
    out = _propose(cands, FEATURE_ROOT, "no candidate label is a live ENVO water body")
    return {"rule": rule, **out}


CURATED_STATES = ("curated", "accepted", "human_accepted")


def _is_curated(row: tuple | None) -> bool:
    """(envoCurie, envoLabel, method, status) of a stored row is curated (never overwritten)."""
    return bool(row) and (row[2] == "curated" or row[3] in CURATED_STATES)


def run_biome(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> dict[str, Any]:
    """Heuristic biome (all sites) and water-body feature (aquatic sites) proposals; curated rows are kept."""
    ctx = site_context(con, site, has_catalog)
    with store.store(read_only=True) as sc:
        cur = sc.execute("SELECT envoCurie, envoLabel, method, status FROM ref_site_envo WHERE siteCode = ?",
                         [site]).fetchone()
        cur_f = sc.execute("SELECT envoCurie, envoLabel, method, status FROM ref_site_envo_feature "
                           "WHERE siteCode = ?", [site]).fetchone()
    prop = propose_biome(ctx)
    feat = propose_feature(ctx)
    now = env.utcnow()
    type_eco = None if is_aquatic(ctx["habitat"]) else ctx["typeEco"]
    with store.store() as sc:
        for role, pr in (("biome", prop), ("feature", feat)):
            sc.execute("INSERT OR REPLACE INTO site_envo_proposals (siteCode, role, source, envoCurie, envoLabel, "
                       "rule, detail, createdAt) VALUES (?, ?, 'heuristic', ?, ?, ?, ?, ?)",
                       [site, role, pr["envoCurie"], pr["envoLabel"], pr["rule"],
                        json.dumps({"candidates": pr["candidates"], "habitat": ctx["habitat"],
                                    "note": pr.get("note")}), now])
        if not _is_curated(cur):
            sc.execute("INSERT OR REPLACE INTO ref_site_envo (siteCode, envoCurie, envoLabel, typeEco, method, status) "
                       "VALUES (?, ?, ?, ?, ?, ?)",
                       [site, prop["envoCurie"], prop["envoLabel"], type_eco, prop["method"], prop["status"]])
        if not _is_curated(cur_f):
            if is_aquatic(ctx["habitat"]):
                sc.execute("INSERT OR REPLACE INTO ref_site_envo_feature (siteCode, envoCurie, envoLabel, habitat, "
                           "method, status) VALUES (?, ?, ?, ?, ?, ?)",
                           [site, feat["envoCurie"], feat["envoLabel"], ctx["habitat"], feat["method"],
                            feat["status"]])
            else:
                sc.execute("DELETE FROM ref_site_envo_feature WHERE siteCode = ?", [site])
    biome = ({"kept": True, "envoCurie": cur[0], "envoLabel": cur[1], "method": cur[2], "status": cur[3],
              "heuristic": prop} if _is_curated(cur) else {"kept": False, **prop})
    feature = ({"kept": True, "envoCurie": cur_f[0], "envoLabel": cur_f[1], "method": cur_f[2], "status": cur_f[3],
                "heuristic": feat} if _is_curated(cur_f) else {"kept": False, **feat})
    return {**biome, **ctx, "feature": feature}


# ---------------------------------------------------------------- main

def _write_csv(path: Path, rows: list[dict[str, Any]], cols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def render(site: str, summary: dict[str, Any]) -> str:
    tx, un, nl, bi = summary["taxa"], summary["units"], summary["nlcd"], summary["biome"]
    c = tx["counts"]
    lines = [f"terms {site}",
             (f"  taxa: {tx['distinct']} distinct (exact {c['exact']}, synonym {c['synonym']}, "
              f"species {c['species_fallback']}, genus {c['genus_fallback']}, family {c['family_fallback']}, "
              f"unresolved {c['unresolved']}, obsolete {c['obsolete']}, pending {c['pending']}); "
              f"resolved now {tx['resolvedNow']}, kept ok {tx['keptOk']}")]
    for ch in tx["typeChoices"]:
        tie = f"; TIE with {', '.join(ch['tiedWith'])}" if ch["tiedWith"] else ""
        lines.append(f"    {ch['productCode']}/{ch['tableName']}: {ch['taxonTypeCode']} "
                     f"({ch['nMatched']}/{ch['nTaxa']} = {100 * ch['matchRate']:.1f}%){tie}")
    uc = un["counts"]
    lines.append(f"  units: {un['distinct']} distinct; mapped {un['mapped']} (seed {uc['seed']}, exact {uc['exact']}, "
                 f"synonym {uc['synonym']}, curated {uc['curated']}), unmapped {un['unmapped']}, "
                 f"not applicable (variance) {uc['not_applicable']}")
    for u, m in sorted(un["map"].items()):
        lines.append(f"    {u:<36} -> {m['uoCurie'] or '-'} {m['uoLabel'] or ''} [{m['method']}, {m['status']}]")
    lines.append(f"  nlcd->envo: {nl['verified']} verified, {len(nl['issues'])} issues")
    share = next((x["share"] for x in bi.get("nlcd", []) if x["nlcdClass"] == bi.get("nlcdDominant")), None)
    heur = bi.get("heuristic") or bi
    lines.append(f"  HABITAT: {bi.get('habitat')!r}")
    lines.append(f"  site biome: {bi.get('envoCurie') or '-'} {bi.get('envoLabel') or ''} "
                 f"[{bi.get('method')}, {bi.get('status')}; rule {heur.get('rule', 'kept')}; "
                 f"TypeEco={bi.get('typeEco')!r}; NLCD {bi.get('nlcdDominant', heur.get('nlcdDominant'))} "
                 f"{'' if share is None else f'{100 * share:.1f}%'}]")
    if bi.get("kept"):
        lines.append(f"    (heuristic: {heur.get('envoCurie') or '-'} {heur.get('envoLabel') or ''})")
    fe = bi.get("feature") or {}
    if fe and (fe.get("envoCurie") or fe.get("candidates")):
        fh = fe.get("heuristic") or fe
        lines.append(f"  site feature: {fe.get('envoCurie') or '-'} {fe.get('envoLabel') or ''} "
                     f"[{fe.get('method')}, {fe.get('status')}; rule {fh.get('rule', 'kept')}]")
    return "\n".join(lines)


def main(site: str) -> int:
    site = site.upper()
    paths = site_lake.site_paths(site)
    con = site_lake.open_site_lake(site, read_only=True)
    try:
        has_catalog = site_lake.attach_catalog(con)
        if not has_catalog:
            log.warning("catalog lake not attached: taxonTypeCode, accepted names, families and TypeEco unavailable")
        store.open_store().close()  # create the store + contract tables
        summary = {"site": site, "at": env.utcnow().isoformat(), "catalog": has_catalog,
                   "taxa": run_taxa(con, site, has_catalog), "units": run_units(con, site),
                   "nlcd": run_nlcd(), "biome": run_biome(con, site, has_catalog)}
    finally:
        con.close()
    summary["exported"] = store.export_parquet()
    env.write_json(paths.reports_dir / "terms.json", summary)
    print(render(site, summary))
    print(f"\nwrote {paths.reports_dir / 'unresolved_taxa.csv'}, {paths.reports_dir / 'unmapped_units.csv'}, "
          f"{paths.reports_dir / 'terms.json'} and {store.maps_dir()}/*.parquet")
    pending = summary["taxa"]["counts"]["pending"] + summary["units"]["counts"]["pending"]
    if pending:
        print(f"{pending} rows are pending (OLS unavailable); re-run `neon-ducklake site terms {site}`")
        return 1
    return 0
