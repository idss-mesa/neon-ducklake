from __future__ import annotations

import pydantic
import pytest

from neon_ducklake import env
from neon_ducklake.avu import datacite
from neon_ducklake.avu.normalize import ELLIPSIS

SITE = "/iplant/home/tswetnam/neon-ducklake/sites/SRER"


def record(**kw):
    base = {"irods_path": SITE, "title": "NEON data for Santa Rita Experimental Range (SRER)",
            "description": "All non-AOP basic-package data.", "keywords": ["NEON", ("desert", "ENVO")],
            "release_dois": ["https://doi.org/10.48443/7rs6-xs41", "doi:10.48443/abcd-1234"],
            "coverage": ("2017-01", "2026-08"), "geo_point": "31.91068 -110.83549", "geo_place": "SRER",
            "publicationYear": 2026}
    base.update(kw)
    return datacite.build_record(**base)


def test_record_shape() -> None:
    r = record()
    assert r["identifier"] == "https://de.cyverse.org/data/ds" + SITE and r["identifierType"] == "URL"
    assert r["creators"] == [{"name": "National Ecological Observatory Network (NEON)", "nameType": "Organizational",
                              "nameIdentifier": env.NEON_ROR, "nameIdentifierScheme": "ROR"}]
    assert {x["value"] for x in r["relatedIdentifiers"]} == {"10.48443/7rs6-xs41", "10.48443/abcd-1234"}
    assert {x["relationType"] for x in r["relatedIdentifiers"]} == {"IsDerivedFrom"}
    assert r["dates"] == [{"date": "2017-01/2026-08", "dateType": "Collected"}]
    assert r["rightsList"][0]["rightsURI"] == "https://creativecommons.org/licenses/by/4.0/"


def test_to_avus_canonical_and_roundtrip() -> None:
    avus = datacite.to_avus(record())
    d = {a: v for a, v, _u in avus}
    assert d["datacite.identifierType"] == "URL"
    assert d["datacite.creator.1.nameIdentifier"] == "https://ror.org/04j43p132"
    assert d["datacite.creator.1.nameIdentifierScheme"] == "ROR"
    assert d["datacite.relatedIdentifier.1.relationType"] == "IsDerivedFrom"
    assert d["datacite.subject.2.subjectScheme"] == "ENVO"
    assert all(u == "" for _a, _v, u in avus)
    datacite.roundtrip(avus)


def test_long_description_is_capped_and_still_roundtrips() -> None:
    avus = datacite.to_avus(record(description="A sentence about the site. " * 200))
    desc = next(v for a, v, _u in avus if a == "datacite.description.1.value")
    assert desc.endswith(ELLIPSIS) and len(desc.encode()) <= 2000


def test_roundtrip_detects_loss() -> None:
    avus = datacite.to_avus(record())
    with pytest.raises(ValueError, match="round-trip"):
        datacite.roundtrip([*avus, ("datacite.notAField", "x", "")])  # ignored on rebuild -> lost
    with pytest.raises(ValueError, match="round-trip"):
        datacite.roundtrip([*avus, ("datacite.creator.1.unknownField", "x", "")])  # rejected on rebuild


def test_invalid_record_rejected() -> None:
    with pytest.raises(pydantic.ValidationError):
        datacite.build_record(irods_path=SITE)  # no title
    with pytest.raises(pydantic.ValidationError):
        datacite.build_record(irods_path=SITE, title="t", resourceTypeGeneral="NotAType")
