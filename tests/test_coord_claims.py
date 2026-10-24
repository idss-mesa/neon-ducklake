"""Site and curation claims on the (fake) Data Store: atomicity, stale steal, heartbeat, release, .done."""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from neon_ducklake import coord, env

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)

SITE = "SRER"
CLAIM = f"{ROOT}/sites/{SITE}/.claim"


def stale_claim(store: DSIrods, *, vm: str = "vm-old", host: str = "gone-host", minutes_ago: int = 5) -> dict:
    past = datetime.now(UTC) - timedelta(hours=3)
    doc = {"kind": "site", "key": SITE, "vm": vm, "host": host, "pid": 999999, "token": "stale-token",
           "claimedAt": past.isoformat(), "heartbeatAt": past.isoformat(),
           "expiresAt": (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat(), "ttlS": 7200}
    store.add_obj(CLAIM, content=json.dumps(doc).encode())
    return doc


def test_claim_json_and_paths(ds: DSIrods) -> None:
    s = DSSession(ds)
    r = coord.claim_site(SITE, session=s, settle_s=0, vm="vm-a", info={"run": 1})
    assert r and r.status == "acquired" and r.claim.vm == "vm-a"
    doc = json.loads(ds.content(CLAIM))
    assert {"vm", "host", "pid", "claimedAt", "heartbeatAt", "expiresAt", "token"} <= set(doc)
    assert doc["kind"] == "site" and doc["key"] == SITE and doc["info"] == {"run": 1}
    exp = datetime.fromisoformat(doc["expiresAt"])
    assert timedelta(hours=1, minutes=59) < exp - datetime.fromisoformat(doc["claimedAt"]) <= timedelta(hours=2)
    assert coord.site_claim_path("srer") == CLAIM and coord.site_done_path(SITE) == f"{ROOT}/sites/{SITE}/.done"
    assert coord.curation_claim_path("DP1.10003.001") == f"{ROOT}/curation/claims/DP1.10003.001.claim"
    assert coord.generic_remote_path("DP1.10003.001") == f"{ROOT}/curation/generic/DP1.10003.001.validated.json"
    # create, never put: the object was created exclusively and then written in place
    assert ("create", CLAIM) in ds.ops and not any(p == CLAIM for p, _, _ in ds.put_log)


def test_second_vm_sees_the_claim_as_held(ds: DSIrods) -> None:
    a = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")
    b = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert a.acquired and not b.acquired and b.status == "held" and b.claim.vm == "vm-a"
    assert b.claim.token == a.claim.token


def test_two_clients_racing_past_the_client_side_check(ds: DSIrods) -> None:
    """Both clients pass python-irodsclient's exists() check; the server-side create lets only one through."""
    gate = threading.Barrier(2)
    ds.create_hook = lambda path: gate.wait(timeout=5) if path == CLAIM else None
    results: dict[str, coord.ClaimResult] = {}

    def go(vm: str) -> None:
        results[vm] = coord.try_claim(CLAIM, "site", SITE, session=DSSession(ds), settle_s=0, vm=vm)

    threads = [threading.Thread(target=go, args=(vm,)) for vm in ("vm-a", "vm-b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    won = [vm for vm, r in results.items() if r.acquired]
    assert len(won) == 1 and len(results) == 2
    lost = next(r for vm, r in results.items() if vm not in won)
    assert lost.status == "held"
    assert json.loads(ds.content(CLAIM))["vm"] == won[0]


def test_many_clients_one_winner(ds: DSIrods) -> None:
    gate = threading.Barrier(6)
    ds.create_hook = lambda path: gate.wait(timeout=5) if path == CLAIM else None
    results: list[coord.ClaimResult] = []
    lock = threading.Lock()

    def go(i: int) -> None:
        r = coord.try_claim(CLAIM, "site", SITE, session=DSSession(ds), settle_s=0.05, vm=f"vm-{i}")
        with lock:
            results.append(r)

    threads = [threading.Thread(target=go, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert sum(r.acquired for r in results) == 1 and len(results) == 6


def test_empty_claim_being_written_is_live(ds: DSIrods) -> None:
    ds.add_obj(CLAIM, content=b"")
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert not r and r.status == "held" and not r.claim.valid
    ds.mtimes[CLAIM] = datetime.now(UTC) - timedelta(seconds=coord.EMPTY_GRACE_S + 5)
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert r and r.status == "stolen" and r.previous is not None and not r.previous.valid


def test_stale_claim_is_stolen_and_token_reread(ds: DSIrods) -> None:
    stale_claim(ds)
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert r and r.status == "stolen" and r.previous.vm == "vm-old"
    doc = json.loads(ds.content(CLAIM))
    assert doc["vm"] == "vm-b" and doc["token"] == r.claim.token != "stale-token"
    assert f"{CLAIM}.break" not in ds.objs  # the break lock is gone
    ops = [op for op, p in ds.ops if p in (CLAIM, CLAIM + ".break")]
    assert ops == ["create", "unlink", "create", "unlink"]  # break lock, delete stale, create ours, unlock


def test_only_one_of_two_stealers_wins(ds: DSIrods) -> None:
    stale_claim(ds)
    gate = threading.Barrier(2)
    ds.create_hook = lambda path: gate.wait(timeout=5) if path.endswith(".break") else None
    results: dict[str, coord.ClaimResult] = {}

    def go(vm: str) -> None:
        results[vm] = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm=vm)

    threads = [threading.Thread(target=go, args=(vm,)) for vm in ("vm-a", "vm-b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    won = [vm for vm, r in results.items() if r.acquired]
    assert len(won) == 1 and results[won[0]].status == "stolen"
    assert json.loads(ds.content(CLAIM))["vm"] == won[0]


def test_a_claim_renewed_meanwhile_is_not_stolen(ds: DSIrods, monkeypatch) -> None:
    doc = stale_claim(ds)
    real_read = coord.read_claim
    calls = {"n": 0}

    def read_then_renew(path, *, session=None):
        calls["n"] += 1
        if calls["n"] == 2:  # the re-read under the break lock: the holder heartbeated just now
            fresh = dict(doc, expiresAt=(datetime.now(UTC) + timedelta(hours=2)).isoformat())
            ds.set_content(CLAIM, json.dumps(fresh).encode())
        return real_read(path, session=session)

    monkeypatch.setattr(coord, "read_claim", read_then_renew)
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert not r and r.status == "held" and json.loads(ds.content(CLAIM))["vm"] == "vm-old"


def test_abandoned_break_lock_is_cleared(ds: DSIrods) -> None:
    stale_claim(ds)
    ds.add_obj(CLAIM + ".break", content=b"{}", mtime=datetime.now(UTC) - timedelta(seconds=coord.BREAK_TTL_S + 60))
    first = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert not first and CLAIM + ".break" not in ds.objs  # removed for the next attempt
    assert coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b").status == "stolen"


def test_same_vm_resumes_after_restart(ds: DSIrods) -> None:
    stale = stale_claim(ds, vm="vm-a", host=coord.host())
    stale["expiresAt"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()  # live, but the pid is dead
    ds.set_content(CLAIM, json.dumps(stale).encode())
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")
    assert r and r.status == "resumed" and r.previous.pid == 999999
    other = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert not other


def test_heartbeat_renews_and_detects_loss(ds: DSIrods) -> None:
    s = DSSession(ds)
    r = coord.try_claim(CLAIM, "site", SITE, session=s, settle_s=0, vm="vm-a", ttl_s=60)
    first_exp = json.loads(ds.content(CLAIM))["expiresAt"]
    lost: list[str] = []
    hb = coord.Heartbeat([r.claim], interval_s=0.02, session=s, on_lost=lambda c: lost.append(c.key))
    with hb:
        deadline = time.monotonic() + 5
        while hb.beats < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(1.1)  # ISO seconds resolution
        while hb.beats < 4 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert json.loads(ds.content(CLAIM))["expiresAt"] > first_exp and not hb.lost.is_set()
        taken = json.loads(ds.content(CLAIM)) | {"vm": "vm-b", "token": "theirs"}
        ds.set_content(CLAIM, json.dumps(taken).encode())
        assert hb.lost.wait(5)
    assert lost == [SITE] and hb.lost_claims == [r.claim]
    assert json.loads(ds.content(CLAIM))["token"] == "theirs"  # the heartbeat never overwrote it
    assert ds.calls["clone"] >= 1  # the thread used its own session


def test_renew_and_release_only_with_our_token(ds: DSIrods) -> None:
    s = DSSession(ds)
    r = coord.try_claim(CLAIM, "site", SITE, session=s, settle_s=0, vm="vm-a")
    assert coord.renew(r.claim, session=s)
    impostor = coord.Claim.new(CLAIM, "site", SITE, ttl_s=60, vm="vm-a")
    assert not coord.renew(impostor, session=s) and not coord.release(impostor, session=s)
    assert CLAIM in ds.objs
    assert coord.release(r.claim, session=s) and CLAIM not in ds.objs
    assert not coord.release(r.claim, session=s)  # already gone


def test_done_marker_blocks_claims_and_lists(ds: DSIrods) -> None:
    s = DSSession(ds)
    r = coord.claim_site(SITE, session=s, settle_s=0, vm="vm-a")
    path = coord.write_done(SITE, {"rc": 0, "chunks": 3}, session=s)
    coord.release(r.claim, session=s)
    assert path == f"{ROOT}/sites/{SITE}/.done"
    done = coord.read_done(SITE, session=s)
    assert done["rc"] == 0 and done["site"] == SITE and done["vm"] == env.vm_id()
    assert coord.done_sites(session=s) == {SITE}
    again = coord.claim_site(SITE, session=s, settle_s=0, vm="vm-b")
    assert not again and again.status == "done"


def test_site_and_curation_claim_listing_and_release_cli(ds: DSIrods, capsys) -> None:
    s = DSSession(ds)
    coord.claim_site("HARV", session=s, settle_s=0, vm="vm-a")
    stale_claim(ds)
    coord.claim_curation("DP1.10003.001", session=s, settle_s=0, vm="vm-c")
    assert set(coord.site_claims(session=s)) == {"HARV", SITE}
    assert set(coord.curation_claims(session=s)) == {"DP1.10003.001"}
    assert coord.claims_main("list", session=s) == 0
    out = capsys.readouterr().out
    assert "site HARV: vm vm-a" in out and "[STALE]" in out and "curation DP1.10003.001" in out
    assert coord.claims_main("list", session=s, as_json=True) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["sites"][SITE]["stale"] and not doc["sites"]["HARV"]["stale"]
    # a stale claim may be released; a live claim of another VM only with --force
    assert coord.claims_main("release", [SITE], session=s) == 0 and CLAIM not in ds.objs
    assert coord.claims_main("release", ["DP1.10003.001"], session=s) == 1
    assert coord.claims_main("release", ["DP1.10003.001"], session=s, force=True) == 0  # the cli.py form
    assert f"{ROOT}/curation/claims/DP1.10003.001.claim" not in ds.objs
    with pytest.raises(SystemExit):
        coord.claims_main("steal", ["X"], session=s)


def test_release_curation_only_for_this_vm(ds: DSIrods) -> None:
    s = DSSession(ds)
    coord.claim_curation("DP1.00001.001", session=s, settle_s=0, vm="vm-a")
    assert not coord.release_curation("DP1.00001.001", session=s, vm="vm-b")
    assert coord.release_curation("DP1.00001.001", session=s, vm="vm-a")


def test_vm_id_and_curation_sync_switch(monkeypatch) -> None:
    assert env.vm_id({"NEON_DUCKLAKE_VM_ID": "de vm/7"}) == "de-vm-7"
    assert env.vm_id({}) and "/" not in env.vm_id({})
    assert not coord.curation_sync_enabled({})
    assert coord.curation_sync_enabled({"NEON_DUCKLAKE_VM_ID": "vm-1"})
    assert coord.curation_sync_enabled({"NEON_DUCKLAKE_HISTORY_MODE": "spool"})
    assert not coord.curation_sync_enabled({"NEON_DUCKLAKE_VM_ID": "vm-1", "NEON_DUCKLAKE_CURATION_SYNC": "off"})
    assert coord.curation_sync_enabled({"NEON_DUCKLAKE_CURATION_SYNC": "on"})


def test_fetch_verifies_checksum(ds: DSIrods, tmp_path) -> None:
    s = DSSession(ds)
    path = f"{ROOT}/x/data.bin"
    from .test_coord_fakes import put

    put(ds, path, b"payload")
    local = tmp_path / "dl" / "data.bin"
    obj = coord.fetch(s, path, local)
    assert local.read_bytes() == b"payload" and abs(local.stat().st_mtime - obj.modified.timestamp()) < 1
    ds.objs[path]["replicas"][0]["checksum"] = "0" * 32
    with pytest.raises(RuntimeError, match="checksum"):
        coord.fetch(s, path, tmp_path / "dl" / "bad.bin")
    assert not (tmp_path / "dl" / "bad.bin").exists() and not (tmp_path / "dl" / "bad.bin.part").exists()
