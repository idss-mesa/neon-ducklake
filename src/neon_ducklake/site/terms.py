"""`site terms <SITE>`: deterministic ontology terms for one site, kept in the shared ontology store.

Every CURIE comes from an OLS lookup and is re-checked with ``ontology_store.ols_term``, which
confirms the term exists and is not obsolete. Results are cached across sites in
``work/ontology_cache.duckdb`` and exported to ``ref/ontology_maps/*.parquet``.

(a) **Taxa → NCBITaxon**, for every distinct (taxonTypeCode, taxonID) in the site lake's ``file_taxa``.
  - taxonTypeCode is chosen per (product, table): the catalog ``taxonomy`` type with the best
    taxonID match rate. Ties go to name agreement, then alphabetical order, and are reported.
  - The name used is the accepted one: ``acceptedTaxonID`` is followed, the authorship is removed,
    and qualifiers are normalised.
  - Lookup is an OLS ``ncbitaxon`` search (size 25) with a case-insensitive exact label match →
    ``exact``. If that fails, the genus is tried → ``genus_fallback``, then the catalog family →
    ``family_fallback``, and otherwise the taxon is ``unresolved``.
  - Homonyms are disambiguated inside the catalog kingdom.
  - Rows already ``ok`` in ``ref_ncbitaxon_map`` are kept as they are.
  - Output: ``reports/unresolved_taxa.csv``. It lists every taxon that is not an ``ok`` exact match
    (fallbacks included), with the top OLS hits of the exact-name search. NEON sometimes uses a
    name that NCBI keeps only as a synonym, so those hits help curation.

(b) **Units → UO**, for every distinct ``variables.units``.
  - First a seed dictionary, NEON unit → UO *label*. The CURIE always comes from an exact-label
    OLS search in ``uo``, preferring ``UO:0000xxx`` over ``UO:1xxxxxx`` (the "… based unit"
    terms). Method ``seed``.
  - Otherwise the camelCase name is split into words and singularised, then matched exactly.
    Method ``exact``.
  - What is left goes to ``reports/unmapped_units.csv``. Rows with method ``curated`` are never
    touched.

(c) **NLCD → ENVO**, a fixed dictionary.
  - Each CURIE is verified. When its label differs or the term is obsolete, ENVO is searched for
    the expected label and the difference is reported.

(d) **Site biome**, a conservative proposal for ``ref_site_envo``.
  - Built from TypeEco and the dominant NLCD class through ordered keyword rules.
  - Each candidate label has to exist as a descendant of ENVO:00000428 'biome'
    (OLS ``allChildrenOf``).
  - Result: method ``heuristic``, status ``proposed``. With no candidate, method and status are
    both ``pending``.
  - A ``curated`` row is never overwritten.
"""

from __future__ import annotations

import csv
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

TAXON_LEVELS = ("exact", "genus_fallback", "family_fallback")
# Catalog kingdom -> NCBITaxon root used to disambiguate homonyms (e.g. Morus the plant vs the bird).
KINGDOM_ROOTS = {"animalia": "NCBITaxon:33208", "metazoa": "NCBITaxon:33208", "plantae": "NCBITaxon:33090",
                 "viridiplantae": "NCBITaxon:33090", "fungi": "NCBITaxon:4751", "bacteria": "NCBITaxon:2",
                 "archaea": "NCBITaxon:2157"}
RANK_MARKERS = {"subsp.", "var.", "f.", "subvar."}
DROP_QUALIFIERS = {"sp.", "spp.", "cf.", "aff.", "sp", "spp", "cf", "aff"}
AUTHOR_PARTICLES = {"ex", "et", "in", "de", "van", "von", "der", "den", "du", "and", "emend.", "nom."}
_NAME_WORD = re.compile(r"^[A-Z][a-z]+(?:-[a-z]+)?$")
_EPITHET = re.compile(r"^[a-z][a-z-]*$")


def query_name(name: str | None, authorship: str | None = None) -> str | None:
    """NEON scientificName -> NCBITaxon query.

    - The authorship is removed.
    - ' sp.', ' spp.', ' cf.' and ' aff.' are dropped, and 'ssp.' becomes 'subsp.'.
    - What is kept is the genus, the lower-case epithets that follow it directly, and marker+epithet
      pairs such as 'var. x', even when authors come between them. Authors, numbers and 'sp. 1'
      are dropped.

    >>> query_name("Larrea tridentata (DC.) Coville", "(DC.) Coville")
    'Larrea tridentata'
    >>> query_name("Toxostoma cf. curvirostre")
    'Toxostoma curvirostre'
    >>> query_name("Carex sp.")
    'Carex'
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
    tokens = [("subsp." if t in ("ssp.", "ssp") else t) for t in text.split()]
    tokens = [t for t in tokens if t not in DROP_QUALIFIERS]
    if not tokens:
        return None
    out = [tokens[0]]
    i, broken = 1, False  # broken: an authorship token was skipped since the last kept epithet
    while i < len(tokens):
        t = tokens[i]
        if t in RANK_MARKERS and i + 1 < len(tokens) and _EPITHET.match(tokens[i + 1]):
            out += [t, tokens[i + 1]]
            i, broken = i + 2, False
            continue
        if not broken and _EPITHET.match(t) and t not in AUTHOR_PARTICLES:
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


def resolve_taxa(taxa: list[Taxon]) -> dict[tuple[str, str], Resolution]:
    """Level by level (exact → genus → family), each level searched and verified in one batch."""
    results: dict[tuple[str, str], Resolution] = {}
    obsolete: dict[tuple[str, str], Resolution] = {}
    for method in TAXON_LEVELS:
        batch = [(t, n) for t in taxa if t.key not in results and (n := t.name_for(method))]
        if not batch:
            continue
        hits = store.search_many(sorted({n for _t, n in batch}), "ncbitaxon", SEARCH_SIZE)
        cands: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for t, name in batch:
            if name not in hits:
                results[t.key] = Resolution(None, None, method, "pending", t.queryName)
                continue
            ex = store.exact_matches(hits[name], name, "NCBITaxon")
            if len(ex) > 1:
                ex = _disambiguate(t, name, ex)
                if len(ex) > 1:
                    log.warning("ambiguous NCBITaxon label %r for %s/%s: %s (level skipped)", name, *t.key,
                                ", ".join(h["curie"] for h in ex))
                    ex = []
            cands[t.key] = ex
        terms = store.ols_terms({h["curie"] for ex in cands.values() for h in ex})
        for t, name in batch:
            ex = cands.get(t.key)
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
    todo = [t for t in taxa if (existing.get(t.key) or (None,) * 8)[7] != "ok"]
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
            "counts": {k: counts.get(k, 0) for k in (*TAXON_LEVELS, "unresolved", "obsolete", "pending")},
            "typeChoices": [{"productCode": p, "tableName": tb, **vars(c)} for (p, tb), c in choices.items()],
            "report": str(path.relative_to(env.ROOT))}


# ---------------------------------------------------------------- units

# NEON unit -> UO *label* (never a CURIE: the CURIE always comes from an exact-label OLS search).
UNIT_SEED: dict[str, str] = {
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
}
_KEEP_S = {"celsius", "siemens", "lux", "series", "hertz", "gauss"}
_IRREGULAR = {"inches": "inch", "feet": "foot"}


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


def _uo_rank(curie: str) -> tuple[int, str]:
    """Prefer UO:0000xxx units over UO:1xxxxxx 'based unit' classes."""
    local = curie.split(":", 1)[1]
    return (1 if local.startswith("1") else 0, local)


def _pick_uo(hits: list[dict[str, Any]], label: str, terms: dict[str, dict[str, Any] | None]) -> dict[str, Any] | None:
    ex = sorted(store.exact_matches(hits, label, "UO"), key=lambda h: _uo_rank(h["curie"]))
    for h in ex:
        t = terms.get(h["curie"])
        if t and not t["isObsolete"]:
            return t
    return None


def site_units(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, Any]]:
    if not site_lake.table_exists(con, "variables"):
        return {}
    rows = con.execute(
        "SELECT trim(units) AS u, count(*)::BIGINT, list(DISTINCT productCode || '/' || tableName || '.' || fieldName "
        "ORDER BY productCode || '/' || tableName || '.' || fieldName) FROM lake.variables "
        "WHERE units IS NOT NULL AND trim(units) <> '' AND upper(trim(units)) NOT IN ('NA', 'N/A', 'NONE', 'NULL') "
        "GROUP BY 1 ORDER BY 1").fetchall()
    return {r[0]: {"nFields": int(r[1]), "fields": list(r[2])} for r in rows}


def resolve_units(units: Iterable[str]) -> dict[str, dict[str, Any]]:
    """NEON unit -> {uoCurie, uoLabel, method, status} (seed, then exact, else unmapped/pending)."""
    units = sorted(set(units))
    out: dict[str, dict[str, Any]] = {}
    seeds = {u: UNIT_SEED[u] for u in units if u in UNIT_SEED}
    variants = {u: unit_label_variants(u) for u in units}
    queries = set(seeds.values()) | {v for vs in variants.values() for v in vs}
    hits = store.search_many(sorted(queries), "uo", SEARCH_SIZE)
    curies = {h["curie"] for q in queries if q in hits for h in store.exact_matches(hits[q], q, "UO")}
    terms = store.ols_terms(curies)
    unavailable = any(q not in hits for q in queries) or any(c not in terms for c in curies)
    for u in units:
        picked, method = None, None
        if u in seeds and seeds[u] in hits:
            picked = _pick_uo(hits[seeds[u]], seeds[u], terms)
            method = "seed" if picked else None
            if not picked:
                log.warning("seed label %r for NEON unit %r has no live UO exact match", seeds[u], u)
        if picked is None:
            for v in variants[u]:
                if v in hits and (picked := _pick_uo(hits[v], v, terms)):
                    method = "exact"
                    break
        if picked:
            out[u] = {"uoCurie": picked["curie"], "uoLabel": picked["label"], "method": method, "status": "ok"}
        elif unavailable:
            out[u] = {"uoCurie": None, "uoLabel": None, "method": "unmapped", "status": "pending"}
        else:
            out[u] = {"uoCurie": None, "uoLabel": None, "method": "unmapped", "status": "unmapped"}
    return out


def run_units(con: duckdb.DuckDBPyConnection, site: str) -> dict[str, Any]:
    units = site_units(con)
    with store.store(read_only=True) as sc:
        curated = {r[0] for r in sc.execute("SELECT neonUnit FROM ref_unit_map WHERE method = 'curated'").fetchall()}
    todo = [u for u in units if u not in curated]
    res = resolve_units(todo) if todo else {}
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
        if status != "ok":
            unmapped.append({"neonUnit": u, "status": status, "nFields": info["nFields"],
                             "candidateLabels": "; ".join(unit_label_variants(u)),
                             "fields": "; ".join(info["fields"][:10])})
    path = site_lake.site_paths(site).reports_dir / "unmapped_units.csv"
    _write_csv(path, unmapped, ["neonUnit", "status", "nFields", "candidateLabels", "fields"])
    mapped = sum(v for k, v in counts.items() if k in ("seed", "exact", "curated"))
    return {"distinct": len(units), "mapped": mapped, "unmapped": len(unmapped),
            "counts": {k: counts.get(k, 0) for k in ("seed", "exact", "curated", "unmapped", "pending")},
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


# ---------------------------------------------------------------- site biome

BIOME_ROOT = "ENVO:00000428"
_CONIFER = ("pine", "fir", "spruce", "hemlock", "cedar", "conifer", "juniper", "larch")
_BROADLEAF = ("deciduous", "hardwood", "oak", "maple", "aspen", "tulip", "hickory", "beech", "birch", "elm", "ash")


def biome_candidates(type_eco: str | None, nlcd: str | None, state: str | None, habitat: str | None,
                     site_name: str | None) -> tuple[str, list[str]]:
    """(rule, ordered candidate ENVO biome labels) from TypeEco, the dominant NLCD class, state and habitat.

    The rules are keyword based, deterministic and deliberately coarse. An empty list means no
    guess, and the row stays pending.
    """
    t = " ".join((type_eco or "").lower().split())
    name = (site_name or "").lower()
    tropical = (state or "").upper() in ("PR", "HI")
    forest = any(w in t for w in ("forest", "woodland"))
    if (habitat or "").lower() == "aquatic":
        if "lake" in name or "pond" in name:
            return "aquatic:lake", ["freshwater lake biome"]
        if "river" in name:
            return "aquatic:river", ["freshwater river biome", "freshwater stream biome"]
        if any(w in name for w in ("creek", "run", "brook", "stream", "branch")):
            return "aquatic:stream", ["freshwater stream biome", "freshwater river biome"]
        return "aquatic:none", []
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


def site_context(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> dict[str, Any]:
    """TypeEco, state, habitat, name and the NLCD composition (latest year) of the site from the catalog."""
    ctx: dict[str, Any] = {"typeEco": None, "stateCode": None, "habitat": None, "siteName": None,
                           "nlcdYear": None, "nlcd": []}
    if not has_catalog:
        return ctx
    row = con.execute("SELECT siteProperties['TypeEco'], stateCode, siteProperties['HABITAT'], siteName "
                      "FROM neon.sites WHERE siteCode = ?", [site]).fetchone()
    if row:
        ctx.update(typeEco=row[0], stateCode=row[1], habitat=row[2], siteName=row[3])
    if not ctx["typeEco"]:
        loc = con.execute("SELECT je.value->>'locationPropertyValue' FROM neon.site_locations sl, "
                          "json_each(sl.locationProperties) je WHERE sl.siteCode = ? AND sl.locationType = 'SITE' "
                          "AND je.value->>'locationPropertyName' = 'Value for TypeEco'", [site]).fetchone()
        ctx["typeEco"] = loc[0] if loc else None
    if site_lake.table_exists(con, "neon_tos_nlcdcoverbysite", alias="neon"):
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


def propose_biome(ctx: dict[str, Any]) -> dict[str, Any]:
    dominant = ctx["nlcd"][0]["nlcdClass"] if ctx["nlcd"] else None
    rule, cands = biome_candidates(ctx["typeEco"], dominant, ctx["stateCode"], ctx["habitat"], ctx["siteName"])
    out = {"rule": rule, "candidates": cands, "nlcdDominant": dominant, "envoCurie": None, "envoLabel": None,
           "method": "pending", "status": "pending"}
    if not cands:
        return out
    hits = store.search_many(cands, "envo", SEARCH_SIZE, within=BIOME_ROOT)
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
    out["note"] = "no candidate label is a live ENVO biome"
    return out


def run_biome(con: duckdb.DuckDBPyConnection, site: str, has_catalog: bool) -> dict[str, Any]:
    ctx = site_context(con, site, has_catalog)
    with store.store(read_only=True) as sc:
        cur = sc.execute("SELECT envoCurie, envoLabel, method, status FROM ref_site_envo WHERE siteCode = ?",
                         [site]).fetchone()
    if cur and (cur[2] == "curated" or cur[3] in ("curated", "accepted", "human_accepted")):
        return {"kept": True, "envoCurie": cur[0], "envoLabel": cur[1], "method": cur[2], "status": cur[3], **ctx}
    prop = propose_biome(ctx)
    with store.store() as sc:
        sc.execute("INSERT OR REPLACE INTO ref_site_envo (siteCode, envoCurie, envoLabel, typeEco, method, status) "
                   "VALUES (?, ?, ?, ?, ?, ?)",
                   [site, prop["envoCurie"], prop["envoLabel"], ctx["typeEco"], prop["method"], prop["status"]])
    return {"kept": False, **prop, **ctx}


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
             (f"  taxa: {tx['distinct']} distinct (exact {c['exact']}, genus {c['genus_fallback']}, "
              f"family {c['family_fallback']}, unresolved {c['unresolved']}, obsolete {c['obsolete']}, "
              f"pending {c['pending']}); resolved now {tx['resolvedNow']}, kept ok {tx['keptOk']}")]
    for ch in tx["typeChoices"]:
        tie = f"; TIE with {', '.join(ch['tiedWith'])}" if ch["tiedWith"] else ""
        lines.append(f"    {ch['productCode']}/{ch['tableName']}: {ch['taxonTypeCode']} "
                     f"({ch['nMatched']}/{ch['nTaxa']} = {100 * ch['matchRate']:.1f}%){tie}")
    uc = un["counts"]
    lines.append(f"  units: {un['distinct']} distinct; mapped {un['mapped']} (seed {uc['seed']}, exact {uc['exact']}, "
                 f"curated {uc['curated']}), unmapped {un['unmapped']}")
    for u, m in sorted(un["map"].items()):
        lines.append(f"    {u:<36} -> {m['uoCurie'] or '-'} {m['uoLabel'] or ''} [{m['method']}, {m['status']}]")
    lines.append(f"  nlcd->envo: {nl['verified']} verified, {len(nl['issues'])} issues")
    share = next((x["share"] for x in bi.get("nlcd", []) if x["nlcdClass"] == bi.get("nlcdDominant")), None)
    lines.append(f"  site biome: {bi.get('envoCurie') or '-'} {bi.get('envoLabel') or ''} "
                 f"[{bi.get('method')}, {bi.get('status')}; rule {bi.get('rule', 'kept')}; "
                 f"TypeEco={bi.get('typeEco')!r}; NLCD {bi.get('nlcdDominant')} "
                 f"{'' if share is None else f'{100 * share:.1f}%'}]")
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
