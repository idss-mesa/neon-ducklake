"""Hermetic fakes for the ontology store: a scripted OLS (terms + searches) and a fake OBO registry.

Imported by the other ``test_terms_*`` modules. ``ols_env`` redirects every project path into tmp
(``site_env``), writes ``ref/obofoundry_ontologies.jsonld`` and patches the two network seams of
``site/ontology_store.py``: ``_get`` (raw OLS v1 JSON: the term endpoint and the exact ``/search``
endpoint, so the real URL encoding and parsing run) and ``_fetch_search`` (mesa-mcp's OLSClient).
"""

from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake.site import ontology_store as store

from .test_site_common import site_env  # noqa: F401 - fixture

OBO_REGISTRY = {"ontologies": [
    {"id": "ncbitaxon", "activity_status": "active"},
    {"id": "uo", "activity_status": "active"},
    {"id": "envo", "activity_status": "active"},
    {"id": "oldont", "activity_status": "inactive"},
    {"id": "gone", "activity_status": "active", "is_obsolete": True},
]}


@dataclass
class FakeOLS:
    """Terms by CURIE; search = case-insensitive substring match on label within the ontology prefix.

    ``ancestors`` restrict ``within`` searches; ``fail_terms`` / ``fail_queries`` raise OLSUnavailable;
    ``extra_hits`` adds hits for a query (e.g. synonym matches OLS would return).
    """

    terms: dict[str, dict[str, Any]] = field(default_factory=dict)
    fail_terms: set[str] = field(default_factory=set)
    fail_queries: set[str] = field(default_factory=set)
    extra_hits: dict[str, list[str]] = field(default_factory=dict)
    term_calls: list[str] = field(default_factory=list)
    search_calls: list[tuple[str, str, int, str]] = field(default_factory=list)

    exact_calls: list[dict[str, Any]] = field(default_factory=list)

    def add(self, curie: str, label: str, *, obsolete: bool = False, ancestors: tuple[str, ...] = (),
            synonyms: tuple[str | tuple[str, str], ...] = ()) -> None:
        """``synonyms``: names, or (name, OBO type) pairs (type defaults to 'synonym')."""
        syn = [(x, "synonym") if isinstance(x, str) else x for x in synonyms]
        self.terms[curie] = {"label": label, "obsolete": obsolete,
                             "ancestors": {store.iri_for(a) for a in ancestors}, "synonyms": syn}

    def search_exact(self, url: str) -> tuple[int, bytes]:
        """The OLS v1 ``/search`` endpoint with ``exact=true`` over ``queryFields`` (label and/or synonym)."""
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        query, ont = q["q"][0], q["ontology"][0]
        fields = q["queryFields"][0].split(",")
        within = q.get("allChildrenOf", [""])[0]
        assert q["exact"] == ["true"], url
        self.exact_calls.append({"q": query, "ontology": ont, "fields": fields, "within": within})
        if query.lower() in self.fail_queries:
            raise store.OLSUnavailable(f"fake outage for {query}")
        want = query.casefold()
        docs = []
        for curie, t in sorted(self.terms.items()):
            if curie.split(":")[0].lower() != ont or (within and within not in t["ancestors"]):
                continue
            names = ([t["label"]] if "label" in fields else []) + \
                ([n for n, _ty in t.get("synonyms", [])] if "synonym" in fields else [])
            if any(n.casefold() == want for n in names):
                docs.append({"iri": store.iri_for(curie), "label": t["label"], "obo_id": curie,
                             "ontology_name": ont, "synonym": [n for n, _ty in t.get("synonyms", [])]})
        return 200, json.dumps({"response": {"numFound": len(docs), "docs": docs}}).encode()

    def get(self, url: str) -> tuple[int, bytes]:
        """The raw OLS v1 term endpoint (``/ontologies/<ont>/terms/<double-encoded IRI>``) and ``/search``."""
        if url.startswith(f"{store.OLS_API}/search?"):
            return self.search_exact(url)
        prefix = f"{store.OLS_API}/ontologies/"
        if not url.startswith(prefix) or "/terms/" not in url:
            raise AssertionError(f"unexpected network call: {url}")
        ont, enc = url[len(prefix):].split("/terms/", 1)
        iri = urllib.parse.unquote(urllib.parse.unquote(enc))
        curie = store.norm_curie(iri)
        assert curie is not None and curie.split(":")[0].lower() == ont, url
        self.term_calls.append(curie)
        if curie in self.fail_terms:
            raise store.OLSUnavailable(f"fake outage for {curie}")
        t = self.terms.get(curie)
        if t is None:
            return 404, b'{"status": 404}'
        syn = [{"name": n, "scope": "hasExactSynonym" if ty == "scientific name" else "hasRelatedSynonym",
                "type": ty, "xrefs": []} for n, ty in t.get("synonyms", [])]
        return 200, json.dumps({"iri": iri, "label": t["label"], "is_obsolete": bool(t["obsolete"]),
                                "ontology_name": ont, "obo_id": curie, "obo_synonym": syn or None,
                                "synonyms": [n for n, _ty in t.get("synonyms", [])] or None}).encode()

    def fetch_search(self, query: str, ontology_id: str, size: int, within: str) -> list[dict[str, Any]]:
        self.search_calls.append((query, ontology_id, size, within))
        if query.lower() in self.fail_queries:
            raise store.OLSUnavailable(f"fake outage for {query}")
        q = query.lower()
        hits = []
        for curie, t in self.terms.items():
            if curie.split(":")[0].lower() != ontology_id.lower():
                continue
            if within and within not in t["ancestors"]:
                continue
            if q in t["label"].lower():
                hits.append(curie)
        for extra in self.extra_hits.get(q, []):
            if extra not in hits:
                hits.append(extra)
        hits.sort(key=lambda c: (self.terms.get(c, {}).get("label", "").lower() != q, c))
        return [{"curie": c, "label": self.terms.get(c, {}).get("label", ""), "iri": store.iri_for(c),
                 "ontologyId": ontology_id.lower()} for c in hits[:size]]


@pytest.fixture
def ols_env(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> FakeOLS:  # noqa: F811
    fake = FakeOLS()
    reg = site_env / "ref" / "obofoundry_ontologies.jsonld"
    reg.parent.mkdir(parents=True, exist_ok=True)
    reg.write_text(json.dumps(OBO_REGISTRY))
    monkeypatch.setattr(store, "_get", fake.get)
    monkeypatch.setattr(store, "_fetch_search", fake.fetch_search)
    monkeypatch.setattr(store, "_TERM_MISSES", set())
    monkeypatch.setattr(store, "_OBO_MEMO", {})
    return fake
