"""DataCite helpers for site and product collections (used by site/avus.py).

``build_record(**fields)`` returns a validated DataCite 4.x record (``mesa_mcp.datacite.schema.
DataCiteMetadata``) as a plain dict; ``to_avus(record)`` serialises it with mesa-mcp's
``datacite_to_avus(naming="canonical")``, normalises the triples and proves they round-trip through
``avus_to_datacite``.

Conventions (plan § DataCite): ``identifier`` is the collection's CyVerse DE URL (identifierType URL);
the creator is NEON with its ROR id; release DOIs go in as relatedIdentifier ``IsDerivedFrom``.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

from .. import env
from .normalize import Triple, normalize_many

DE_BASE = "https://de.cyverse.org/data/ds"
NEON_NAME = "National Ecological Observatory Network (NEON)"
LICENSE = {"value": "Creative Commons Attribution 4.0 International (CC-BY-4.0)",
           "rightsURI": "https://creativecommons.org/licenses/by/4.0/"}


def de_url(irods_path: str) -> str:
    """CyVerse Discovery Environment URL of an iRODS collection."""
    return DE_BASE + quote("/" + irods_path.strip("/"), safe="/")


def neon_creator() -> dict[str, str]:
    return {"name": NEON_NAME, "nameType": "Organizational", "nameIdentifier": env.NEON_ROR,
            "nameIdentifierScheme": "ROR"}


def bare_doi(doi: str) -> str:
    s = doi.strip()
    s = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:)", "", s, flags=re.IGNORECASE)
    return s


_CONVENIENCE = {"irods_path", "title", "description", "methods", "keywords", "release_dois", "related_urls",
                "coverage", "geo_point", "geo_place", "geo_box"}


def build_record(**fields: Any) -> dict[str, Any]:
    """Build and validate a DataCite record; returns ``DataCiteMetadata.model_dump(mode="json")``.

    Convenience keys (kernel field names given directly win):
    - ``irods_path`` -> ``identifier`` (DE URL) with ``identifierType="URL"``
    - ``title`` -> ``titles``; ``description`` -> an Abstract; ``methods`` -> a Methods description
    - ``keywords`` (list of str, or (value, scheme) tuples) -> ``subjects``
    - ``release_dois`` -> ``relatedIdentifiers`` (DOI, IsDerivedFrom); ``related_urls`` -> (URL, References)
    - ``coverage=(start, end)`` -> a ``Collected`` date ``start/end``
    - ``geo_point="lat lon"``, ``geo_place``, ``geo_box`` -> one ``geoLocations`` entry
    Defaults: creator NEON (ROR), publisher NEON, publicationYear this year, resourceTypeGeneral Dataset,
    rights CC-BY-4.0, language en.
    """
    from mesa_mcp.datacite.schema import DataCiteMetadata

    rec: dict[str, Any] = {
        "identifierType": "URL",
        "creators": [neon_creator()],
        "publisher": NEON_NAME,
        "publicationYear": env.utcnow().year,
        "resourceTypeGeneral": "Dataset",
        "rightsList": [dict(LICENSE)],
        "language": "en",
    }
    if fields.get("irods_path"):
        rec["identifier"] = de_url(fields["irods_path"])
    if fields.get("title"):
        rec["titles"] = [fields["title"]]
    descs = []
    if fields.get("description"):
        descs.append({"value": fields["description"], "descriptionType": "Abstract"})
    if fields.get("methods"):
        descs.append({"value": fields["methods"], "descriptionType": "Methods"})
    if descs:
        rec["descriptions"] = descs
    if fields.get("keywords"):
        rec["subjects"] = [({"value": k[0], "subjectScheme": k[1]} if isinstance(k, (tuple, list)) else {"value": k})
                           for k in fields["keywords"] if k]
    rel = [{"value": bare_doi(d), "relatedIdentifierType": "DOI", "relationType": "IsDerivedFrom"}
           for d in fields.get("release_dois") or [] if d]
    rel += [{"value": u, "relatedIdentifierType": "URL", "relationType": "References"}
            for u in fields.get("related_urls") or [] if u]
    if rel:
        rec["relatedIdentifiers"] = rel
    if fields.get("coverage"):
        start, end = fields["coverage"]
        rec["dates"] = [{"date": f"{start}/{end}", "dateType": "Collected"}]
    geo = {k: fields[f"geo_{k}"] for k in ("point", "place", "box") if fields.get(f"geo_{k}")}
    if geo:
        rec["geoLocations"] = [geo]
    rec.update({k: v for k, v in fields.items() if k not in _CONVENIENCE})
    model = DataCiteMetadata(**rec)
    return model.model_dump(mode="json", exclude_none=True)


def _as_dicts(triples: list[Triple]) -> list[dict[str, str]]:
    return [{"attribute": a, "value": v, "unit": u} for a, v, u in triples]


def to_avus(record: dict[str, Any] | Any, *, check: bool = True) -> list[Triple]:
    """Canonical ``datacite.*`` AVU triples of a record (normalised); verifies the AVU round trip."""
    from mesa_mcp.datacite.schema import DataCiteMetadata
    from mesa_mcp.datacite.transform import datacite_to_avus

    model = record if isinstance(record, DataCiteMetadata) else DataCiteMetadata.model_validate(record)
    triples = normalize_many([(a["attribute"], a["value"], a.get("unit", "")) for a in
                              datacite_to_avus(model, naming="canonical")])
    if check:
        roundtrip(triples)
    return triples


def roundtrip(triples: list[Triple]) -> None:
    """Raise ValueError unless AVUs -> record -> AVUs reproduces exactly the same triple set."""
    from mesa_mcp.datacite.transform import avus_to_datacite, datacite_to_avus

    try:
        rebuilt = avus_to_datacite(_as_dicts(triples))
    except (KeyError, ValueError) as exc:  # pydantic's ValidationError is a ValueError
        raise ValueError(f"DataCite AVUs do not round-trip: {exc}") from exc
    again = normalize_many([(a["attribute"], a["value"], a.get("unit", "")) for a in
                            datacite_to_avus(rebuilt, naming="canonical")])
    if set(again) != set(triples):
        missing = sorted(set(triples) - set(again))[:5]
        extra = sorted(set(again) - set(triples))[:5]
        raise ValueError(f"DataCite AVUs do not round-trip: missing={missing} extra={extra}")
