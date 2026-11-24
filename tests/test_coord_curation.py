"""Generic curation shared through the (fake) Data Store: fetch before curating, claim, publish after validate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from neon_ducklake import coord, env
from neon_ducklake.site import curate, ontology_store, validate

from .test_coord_fakes import DSIrods, DSSession, put
from .test_curation_curate import calls, fake_claude, write_card  # noqa: F401 - fixture
from .test_curation_validate import P, fake_ols, standard_proposals  # noqa: F401 - fixture
from .test_site_common import SITE, site_env  # noqa: F401 - fixture


@pytest.fixture
def store(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:  # noqa: F811
    s = DSIrods()
    monkeypatch.setattr(coord, "get_session", lambda session=None: session if session is not None else DSSession(s))
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.setenv("NEON_DUCKLAKE_CURATION_SYNC", "on")
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    return s


def generic_doc(code: str) -> dict:
    item = {"proposalId": f"{abs(hash(code)) % 10**16:016d}", "productCode": code, "status": "accepted",
            "attribute": "pato.temperature", "value": "temperature", "unit": "PATO:0000146", "curie": "PATO:0000146",
            "label": "temperature", "ontologyId": "pato", "aspect": "measured_property", "column": None,
            "rationale": "r", "model": "claude-opus-5-5", "reps": 2, "grounded": True, "reason": None}
    return {"productCode": code, "site": "HARV", "accepted": [item], "proposed": [], "rejected": []}


def test_curate_fetches_generic_results_and_respects_claims(store: DSIrods, fake_claude: Path) -> None:  # noqa: F811
    for code in ("DP1.00002.001", "DP1.10003.001", "DP1.00004.001"):
        write_card(code)
    put(store, coord.generic_remote_path("DP1.00004.001"), json.dumps(generic_doc("DP1.00004.001")).encode())
    other = coord.claim_curation("DP1.10003.001", session=DSSession(store), vm="vm-b")
    assert other
    assert curate.main(SITE) == 0
    got = calls(fake_claude)
    assert {c["product"] for c in got} == {"DP1.00002.001"} and len(got) == 2  # 2 replicates, one product
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "curate_summary.json").read_text())
    assert summary["skipped"]["DP1.00004.001"] == "already validated (curation/generic)"
    assert summary["skipped"]["DP1.10003.001"] == "curation claimed by vm-b on the Data Store"
    # the fetched result is on disk and in curation_avus, so its AVUs apply at this site too
    assert json.loads(curate.generic_path("DP1.00004.001").read_text())["site"] == "HARV"
    with ontology_store.store(read_only=True) as con:
        assert con.execute("SELECT productCode FROM curation_avus").fetchall() == [("DP1.00004.001",)]
    # our claim stays until `site validate` publishes; the other VM's claim is untouched
    ours = coord.read_claim(coord.curation_claim_path("DP1.00002.001"), session=DSSession(store))
    assert ours is not None and ours.vm == "vm-a" and ours.kind == "curation"
    assert coord.read_claim(coord.curation_claim_path("DP1.10003.001"), session=DSSession(store)).vm == "vm-b"


def test_curate_releases_the_claim_of_a_failed_product(store: DSIrods, fake_claude: Path) -> None:  # noqa: F811
    write_card("DP1.00002.001")
    (fake_claude / "fake_config.json").write_text(json.dumps({"garbage": ["DP1.00002.001"]}))
    assert curate.main(SITE) == 2
    assert coord.read_claim(coord.curation_claim_path("DP1.00002.001"), session=DSSession(store)) is None


def test_curate_without_sync_never_touches_the_data_store(site_env: Path, fake_claude: Path,  # noqa: F811
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEON_DUCKLAKE_VM_ID", raising=False)
    monkeypatch.delenv("NEON_DUCKLAKE_HISTORY_MODE", raising=False)
    monkeypatch.delenv("NEON_DUCKLAKE_CURATION_SYNC", raising=False)
    monkeypatch.setattr(coord, "get_session", lambda session=None: (_ for _ in ()).throw(AssertionError("DS used")))
    write_card("DP1.00002.001")
    assert curate.main(SITE) == 0 and len(calls(fake_claude)) == 2


def test_validate_publishes_generic_and_releases_the_claim(store: DSIrods, fake_ols: dict) -> None:  # noqa: F811
    standard_proposals()
    assert coord.claim_curation(P, session=DSSession(store))  # taken by `site curate` on this VM
    assert validate.main(SITE) == 0
    remote = coord.generic_remote_path(P)
    assert json.loads(store.content(remote)) == json.loads(curate.generic_path(P).read_text())
    assert store.objs[remote]["replicas"][0]["checksum"]  # checksum-verified upload
    assert coord.read_claim(coord.curation_claim_path(P), session=DSSession(store)) is None
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "validate_summary.json").read_text())
    assert summary["dataStore"] == {"published": [P], "failed": {}}


def test_validate_publish_failure_is_reported_not_fatal(store: DSIrods, fake_ols: dict) -> None:  # noqa: F811
    standard_proposals()
    store.put_errors = [RuntimeError("injected")]
    assert validate.main(SITE) == 0
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "validate_summary.json").read_text())
    assert summary["dataStore"]["published"] == [] and "injected" in summary["dataStore"]["failed"][P]


def test_import_generic_only_for_products_without_rows(site_env: Path) -> None:  # noqa: F811
    env.write_json(curate.generic_path("DP1.00001.001"), generic_doc("DP1.00001.001"))
    assert validate.import_generic() == {"DP1.00001.001": 1}
    assert validate.import_generic() == {}  # rows exist now: local results win
    assert validate.import_generic(["DP9.99999.001"]) == {}
