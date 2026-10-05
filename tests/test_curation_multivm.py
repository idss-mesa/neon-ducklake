"""Curation across VMs: validate.pending_generic (M8), content versions in generic results (review-0 #6) and
the curate summary's pendingElsewhere."""

from __future__ import annotations

import json
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from neon_ducklake import coord, env
from neon_ducklake.site import curate, validate

from .test_coord_curation import generic_doc, store  # noqa: F401 - fixture
from .test_coord_fakes import DSIrods, DSSession, put
from .test_curation_curate import calls, fake_claude, write_card  # noqa: F401 - fixture
from .test_curation_validate import P, fake_ols, standard_proposals  # noqa: F401 - fixture
from .test_site_common import SITE, site_env  # noqa: F401 - fixture


def claim_by(store: DSIrods, code: str, vm: str, *, stale: bool = False) -> None:  # noqa: F811
    now = datetime.now(UTC)
    doc = {"kind": "curation", "key": code, "vm": vm, "host": "elsewhere", "pid": 1, "token": f"t-{vm}",
           "claimedAt": now.isoformat(), "heartbeatAt": now.isoformat(), "ttlS": 21600,
           "expiresAt": (now + (timedelta(minutes=-5) if stale else timedelta(hours=1))).isoformat()}
    store.add_obj(coord.curation_claim_path(code), content=json.dumps(doc).encode())


def test_pending_generic_lists_products_curated_elsewhere(store: DSIrods) -> None:  # noqa: F811
    codes = ["DP1.00001.001", "DP1.00002.001", "DP1.00003.001", "DP1.00004.001", "DP1.00005.001"]
    for code in codes:
        write_card(code)
    claim_by(store, "DP1.00001.001", "vm-b")  # live, no result yet: pending
    claim_by(store, "DP1.00002.001", "vm-b")  # live, but its result is published: not pending
    put(store, coord.generic_remote_path("DP1.00002.001"), json.dumps(generic_doc("DP1.00002.001")).encode())
    claim_by(store, "DP1.00003.001", "vm-b", stale=True)  # stale: this VM may curate it itself
    assert coord.claim_curation("DP1.00004.001", session=DSSession(store))  # ours
    assert validate.pending_generic(SITE) == ["DP1.00001.001"]


def test_pending_generic_is_empty_without_curation_sync(site_env: Path, monkeypatch) -> None:  # noqa: F811
    monkeypatch.setenv("NEON_DUCKLAKE_CURATION_SYNC", "off")
    monkeypatch.setattr(coord, "get_session", lambda session=None: (_ for _ in ()).throw(AssertionError("DS")))
    write_card("DP1.00001.001")
    assert validate.pending_generic(SITE) == []


def test_curate_summary_names_products_pending_elsewhere(store: DSIrods, fake_claude: Path) -> None:  # noqa: F811
    write_card("DP1.00002.001")
    write_card("DP1.10003.001")
    claim_by(store, "DP1.10003.001", "vm-b")
    assert curate.main(SITE) == 0
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "curate_summary.json").read_text())
    assert summary["pendingElsewhere"] == ["DP1.10003.001"]
    assert validate.pending_generic(SITE) == ["DP1.10003.001"]


def test_generic_results_carry_their_content_version(store: DSIrods, fake_ols: dict) -> None:  # noqa: F811
    standard_proposals()
    assert validate.main(SITE) == 0
    doc = json.loads(curate.generic_path(P).read_text())
    assert doc["validatedBy"] == {"vm": env.vm_id()} and doc["runId"]  # the VM id only: no host name
    assert socket.gethostname() not in curate.generic_path(P).read_text()
    assert coord.validated_here(doc) and coord.generic_version(doc)[1] == doc["runId"]
    remote = json.loads(store.content(coord.generic_remote_path(P)))
    assert remote["runId"] == doc["runId"]
    # an identical re-validation does not rewrite the file (validatedAt, validatedBy and runId are volatile)
    before = curate.generic_path(P).read_text()
    assert validate.main(SITE) == 0
    assert curate.generic_path(P).read_text() == before


def test_validate_never_overwrites_a_newer_result_on_the_data_store(store: DSIrods, fake_ols: dict) -> None:  # noqa: F811
    newer = generic_doc(P) | {"validatedAt": (datetime.now(UTC) + timedelta(days=1)).isoformat(), "runId": "later"}
    put(store, coord.generic_remote_path(P), json.dumps(newer).encode())
    standard_proposals()
    assert validate.main(SITE) == 0
    assert json.loads(store.content(coord.generic_remote_path(P)))["runId"] == "later"
    summary = json.loads((curate.curation_paths(SITE).site_dir / "reports" / "validate_summary.json").read_text())
    assert summary["dataStore"]["failed"] == {} and summary["dataStore"]["published"] == []
    assert summary["dataStore"]["skipped"] == {P: "remote-newer"}


@pytest.mark.parametrize("by", [None, {"vm": "someone"}, {"vm": "someone", "host": "else"}])
def test_validated_here_needs_this_vm(by, monkeypatch) -> None:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    assert not coord.validated_here({"validatedBy": by} if by else {})
    assert coord.validated_here({"validatedBy": {"vm": "vm-a"}})
    # results written before the host name was dropped (or with it blanked) still match on the VM id
    assert coord.validated_here({"validatedBy": {"vm": "vm-a", "host": "old-host.example"}})
    assert coord.validated_here({"validatedBy": {"vm": "vm-a", "host": None}})
