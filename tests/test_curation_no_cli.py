"""`site curate` on a VM without the claude CLI (review D2): reuse the generic curation results, start no Opus run,
take no curation claim, return 0 so the site's finalize can finish; the site pass alone and
NEON_DUCKLAKE_CURATION_REQUIRE_CLI=1 still fail, and the worker refuses to start with the latter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from neon_ducklake import coord, env, state, worker
from neon_ducklake.site import chunks, curate, ontology_store, stream

from .test_coord_curation import generic_doc, store  # noqa: F401 - fixture
from .test_coord_fakes import DSIrods, DSSession, hermetic_ds, put
from .test_curation_curate import write_card
from .test_site_common import SITE, site_env  # noqa: F401 - fixture

FETCHED = "DP1.00004.001"  # its generic result is on the Data Store only
LOCAL = "DP1.00005.001"  # its generic result is on this host only, not imported yet
NOTHING = "DP1.00002.001"  # no result anywhere: left uncurated
ELSEWHERE = "DP1.10003.001"  # vm-b curates it right now


@pytest.fixture
def no_cli(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:  # noqa: F811
    missing = site_env / "no-such-bin" / "claude"
    monkeypatch.setattr(curate, "CLAUDE_BIN", missing)
    monkeypatch.delenv(curate.REQUIRE_CLI_ENV, raising=False)
    (env.ROOT / "curation" / "generic").mkdir(parents=True)
    return missing


def summary() -> dict:
    return json.loads((curate.curation_paths(SITE).site_dir / "reports" / "curate_summary.json").read_text())


def seed(store: DSIrods) -> None:  # noqa: F811
    for code in (FETCHED, LOCAL, NOTHING, ELSEWHERE):
        write_card(code)
    put(store, coord.generic_remote_path(FETCHED), json.dumps(generic_doc(FETCHED)).encode())
    curate.generic_path(LOCAL).write_text(json.dumps(generic_doc(LOCAL)))
    assert coord.claim_curation(ELSEWHERE, session=DSSession(store), vm="vm-b")


def curation_products() -> list[str]:
    with ontology_store.store(read_only=True) as con:
        return sorted(r[0] for r in con.execute("SELECT DISTINCT productCode FROM curation_avus").fetchall())


def test_curate_without_the_cli_reuses_generic_results_and_succeeds(store: DSIrods, no_cli: Path) -> None:  # noqa: F811
    seed(store)
    assert curate.curator_unavailable() == f"claude CLI not found at {no_cli}"
    assert curate.main(SITE) == 0  # was 1: the finalize curate step failed on every run
    s = summary()
    assert s["status"] == curate.NO_CURATOR and s["curator"].startswith("claude CLI not found")
    assert s["uncurated"] == [NOTHING] and s["pendingElsewhere"] == [ELSEWHERE] and s["runs"] == []
    assert s["skipped"][FETCHED] == s["skipped"][LOCAL] == "already validated (curation/generic)"
    assert s["prefetch"][FETCHED] == "fetched"
    # both generic results apply at this site: the fetched one and the local one nobody had imported
    assert curation_products() == [FETCHED, LOCAL]
    # no product is claimed by this VM: it cannot curate any, and a claim would hold other VMs back
    claims = coord.curation_claims(session=DSSession(store))
    assert set(claims) == {ELSEWHERE} and claims[ELSEWHERE].vm == "vm-b"
    con = state.connect(state.DB_PATH)
    try:
        assert [r[0] for r in con.execute("SELECT status FROM runs WHERE stage='site-curate'")] == [curate.NO_CURATOR]
    finally:
        con.close()


def test_finalize_curate_step_succeeds_on_a_vm_without_the_cli(store: DSIrods, no_cli: Path) -> None:  # noqa: F811
    """The real finalize: every other step already ok, the curate step runs `site curate` (curate.main)."""
    seed(store)
    con = state.connect(state.DB_PATH)
    try:
        chunks.ensure_schema(con)
        for step in chunks.FINALIZE_STEPS:
            if step != "curate":
                chunks.set_step(con, SITE, step, "ok")
    finally:
        con.close()
    assert stream.finalize_site(SITE, session=DSSession(store)) == stream.RC_OK
    assert chunks.step_status(SITE, "curate") == "ok"
    assert curation_products() == [FETCHED, LOCAL]


def test_site_pass_alone_or_a_required_cli_still_fails(store: DSIrods, no_cli: Path, monkeypatch) -> None:  # noqa: F811
    seed(store)
    assert curate.main(SITE, site_level=True) == 1
    monkeypatch.setenv(curate.REQUIRE_CLI_ENV, "1")
    assert curate.main(SITE) == 1
    assert not curate.generic_path(FETCHED).exists()  # it stopped before the Data Store prefetch
    assert not (curate.curation_paths(SITE).site_dir / "reports" / "curate_summary.json").exists()


def test_a_cli_that_is_not_executable_is_unavailable(site_env: Path, monkeypatch) -> None:  # noqa: F811
    f = site_env / "claude"
    f.write_text("#!/bin/sh\n")
    f.chmod(0o644)
    monkeypatch.setattr(curate, "CLAUDE_BIN", f)
    assert curate.curator_unavailable() == f"claude CLI at {f} is not executable"
    f.chmod(0o755)
    assert curate.curator_unavailable() is None


def test_worker_warns_or_refuses_to_start_without_the_cli(tmp_path: Path, monkeypatch, capsys) -> None:
    ds = hermetic_ds(tmp_path, monkeypatch)
    monkeypatch.setattr(curate, "CLAUDE_BIN", tmp_path / "none" / "claude")
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    for var in ("NEON_DUCKLAKE_HISTORY_MODE", "NEON_DUCKLAKE_CURATION_SYNC", "NEON_DUCKLAKE_SITE_CLAIMS"):
        monkeypatch.setenv(var, "placeholder")
        monkeypatch.delenv(var)
    assert worker.check_curator().startswith("claude CLI not found")
    assert worker.main(session=DSSession(ds), run_sites=lambda sites, **kw: 0, universe=[], bootstrap=False) == 0
    assert "curation reuses the curation/generic results" in capsys.readouterr().out
    monkeypatch.setenv(curate.REQUIRE_CLI_ENV, "1")
    with pytest.raises(SystemExit, match="NEON_DUCKLAKE_CURATION_REQUIRE_CLI=1"):
        worker.main(session=DSSession(ds), run_sites=lambda sites, **kw: 0, universe=[], bootstrap=False)
    assert coord.vm_claim_path("vm-a") not in ds.objs  # refused before it registered
