"""Losing a claim (B4, m4), self-expiry, site_claim(), .done re-check (M1) and VM identity (m5, #14)."""

from __future__ import annotations

import json
import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from neon_ducklake import coord, env

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds, put

SITE = "SRER"
CLAIM = f"{ROOT}/sites/{SITE}/.claim"
DONE = f"{ROOT}/sites/{SITE}/.done"


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    store = hermetic_ds(tmp_path, monkeypatch)
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.setattr(env, "machine_id", lambda: "machine-1")
    monkeypatch.setattr(env, "boot_id", lambda: "boot-1")
    return store


def foreign(store: DSIrods, path: str, *, vm: str = "vm-a", host: str | None = None, machine: str = "machine-1",
            boot: str = "boot-1", pid: int = 999999, heartbeat_ago_s: float = 0, expires_in_s: float = 3600) -> dict:
    now = datetime.now(UTC)
    doc = {"kind": "site", "key": SITE, "vm": vm, "host": host or coord.host(), "pid": pid, "token": "theirs",
           "machine": machine, "boot": boot, "claimedAt": now.isoformat(),
           "heartbeatAt": (now - timedelta(seconds=heartbeat_ago_s)).isoformat(),
           "expiresAt": (now + timedelta(seconds=expires_in_s)).isoformat(), "ttlS": 7200}
    store.add_obj(path, content=json.dumps(doc).encode())
    return doc


def token(store: DSIrods, path: str = CLAIM) -> str:
    return json.loads(store.content(path))["token"]


# ---------------------------------------------------------------- renew: write, then re-read (m4)

def test_renew_detects_a_claim_replaced_during_its_write(ds, monkeypatch) -> None:
    s = DSSession(ds)
    r = coord.try_claim(CLAIM, "site", SITE, session=s, settle_s=0, vm="vm-a")
    real = coord.overwrite

    def racing_overwrite(session, path, data, *, create=False):
        real(session, path, data, create=create)
        # another VM that broke the claim (after an outage) writes its own right after ours
        ds.set_content(path, json.dumps(json.loads(data) | {"vm": "vm-b", "token": "theirs"}).encode())

    monkeypatch.setattr(coord, "overwrite", racing_overwrite)
    assert coord.renew(r.claim, session=s) is False
    assert token(ds) == "theirs"


def test_renew_error_keeps_the_old_expiry(ds, monkeypatch) -> None:
    s = DSSession(ds)
    r = coord.try_claim(CLAIM, "site", SITE, session=s, settle_s=0, vm="vm-a", ttl_s=60)
    exp = r.claim.expires_at
    monkeypatch.setattr(coord, "overwrite", lambda *a, **k: (_ for _ in ()).throw(ConnectionError("iRODS down")))
    with pytest.raises(ConnectionError):
        coord.renew(r.claim, session=s)
    assert r.claim.expires_at == exp  # not renewed: self-expiry still counts from the last good renew


# ---------------------------------------------------------------- heartbeat: lost and self-expiry (B4)

def test_heartbeat_self_expires_when_it_cannot_renew(ds, monkeypatch) -> None:
    s = DSSession(ds)
    r = coord.try_claim(CLAIM, "site", SITE, session=s, settle_s=0, vm="vm-a", ttl_s=0.6)
    monkeypatch.setattr(coord, "renew", lambda c, session=None: (_ for _ in ()).throw(ConnectionError("down")))
    lost: list[str] = []
    with coord.Heartbeat([r.claim], interval_s=0.1, session=s, on_lost=lambda c: lost.append(c.key)) as hb:
        assert r.claim.lost.wait(3)  # before expiresAt: another VM may steal it right after
        assert hb.lost.is_set() and lost == [SITE]
        assert datetime.now(UTC) < r.claim.expires_at + timedelta(seconds=0.2)
    assert CLAIM in ds.objs  # nothing was written or deleted


def test_heartbeat_sets_claim_lost_on_takeover(ds) -> None:
    s = DSSession(ds)
    r = coord.try_claim(CLAIM, "site", SITE, session=s, settle_s=0, vm="vm-a")
    with coord.Heartbeat([r.claim], interval_s=0.02, session=s):
        ds.set_content(CLAIM, json.dumps(json.loads(ds.content(CLAIM)) | {"token": "theirs"}).encode())
        assert r.claim.lost.wait(3)


# ---------------------------------------------------------------- site_claim() context manager

def test_site_claim_yields_the_claim_and_releases_it(ds) -> None:
    s = DSSession(ds)
    with coord.site_claim(SITE, session=s, vm="vm-a", heartbeat_s=0.02) as claim:
        assert isinstance(claim.lost, threading.Event) and not claim.lost.is_set()
        assert claim.status == "acquired" and token(ds) == claim.token
    assert CLAIM not in ds.objs


def test_site_claim_raises_done_and_claimed(ds) -> None:
    s = DSSession(ds)
    foreign(ds, CLAIM, vm="vm-b", host="elsewhere", machine="m-2")
    with pytest.raises(coord.SiteClaimed) as exc, coord.site_claim(SITE, session=s, vm="vm-a"):
        pass
    assert exc.value.holder.vm == "vm-b" and exc.value.site == SITE
    put(ds, DONE, b"{}")
    with pytest.raises(coord.SiteDone), coord.site_claim(SITE, session=s, vm="vm-a"):
        pass
    assert token(ds) == "theirs"  # untouched


def test_holds_site_for_subprocesses(ds, monkeypatch) -> None:
    s = DSSession(ds)
    monkeypatch.setenv(coord.SITE_CLAIMS_ENV, "on")  # a claim is in force (worker, `run` with claims)
    assert not coord.holds_site(SITE, session=s)  # no claim at all: lost
    with coord.site_claim(SITE, session=s, vm="vm-a", heartbeat_s=60):
        assert coord.holds_site(SITE, session=s, vm="vm-a")
        assert not coord.holds_site(SITE, session=s, vm="vm-b")
    foreign(ds, CLAIM, vm="vm-a", boot="boot-0")  # this VM id, written before this boot
    assert not coord.holds_site(SITE, session=s, vm="vm-a")


@pytest.mark.parametrize("mode", [None, "off"])
def test_holds_site_without_a_claim_in_force_only_yields_to_a_live_owner(ds, monkeypatch, mode) -> None:
    """`run --no-claims` (off) and operator commands (unset): an unclaimed site may be written, a live claim of
    another VM never; a stale one does not block."""
    s = DSSession(ds)
    if mode is None:
        monkeypatch.delenv(coord.SITE_CLAIMS_ENV, raising=False)
    else:
        monkeypatch.setenv(coord.SITE_CLAIMS_ENV, mode)
    assert coord.holds_site(SITE, session=s)
    foreign(ds, CLAIM, vm="vm-b", host="elsewhere", machine="m-2")
    assert not coord.holds_site(SITE, session=s, vm="vm-a")
    foreign(ds, CLAIM, vm="vm-b", host="elsewhere", machine="m-2", expires_in_s=-60)
    assert coord.holds_site(SITE, session=s, vm="vm-a")
    assert not coord.holds_site(SITE, session=s, vm="vm-a", required=True)


def test_rename_is_exclusive(ds) -> None:
    s = DSSession(ds)
    put(ds, f"{ROOT}/a", b"1")
    put(ds, f"{ROOT}/b", b"2")
    assert coord.rename(s, f"{ROOT}/a", f"{ROOT}/b") == "exists" and ds.content(f"{ROOT}/b") == b"2"
    assert coord.rename(s, f"{ROOT}/a", f"{ROOT}/x/c") == "ok" and ds.content(f"{ROOT}/x/c") == b"1"
    assert coord.rename(s, f"{ROOT}/a", f"{ROOT}/d") == "missing"


def test_site_claim_lost_is_not_released(ds) -> None:
    s = DSSession(ds)
    with coord.site_claim(SITE, session=s, vm="vm-a", heartbeat_s=0.02) as claim:
        ds.set_content(CLAIM, json.dumps(json.loads(ds.content(CLAIM)) | {"vm": "vm-b", "token": "theirs"}).encode())
        assert claim.lost.wait(3)
    assert token(ds) == "theirs"  # the new owner's claim stays
    assert not coord.claim_is_current(claim, session=s)


# ---------------------------------------------------------------- claim_site re-checks .done (M1)

def test_claim_site_rechecks_done_after_winning(ds) -> None:
    def finish_meanwhile(path: str) -> None:
        if path == CLAIM:  # VM A writes .done and releases between our first check and our create
            put(ds, DONE, json.dumps({"site": SITE, "vm": "vm-a"}).encode())

    ds.create_hook = finish_meanwhile
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-b")
    assert not r and r.status == "done"
    assert CLAIM not in ds.objs  # released again


# ---------------------------------------------------------------- identity: machine id + boot id (m5, #14)

def test_claim_records_machine_and_boot(ds) -> None:
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")
    doc = json.loads(ds.content(CLAIM))
    assert doc["machine"] == "machine-1" and doc["boot"] == "boot-1" and r.claim.machine == "machine-1"


def test_same_vm_id_and_host_on_another_machine_is_not_resumed(ds) -> None:
    """Two VMs that share a host name and VM id: the dead-pid check is local, so machine id must match."""
    foreign(ds, CLAIM, vm="vm-a", machine="machine-2", pid=999999)
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")
    assert not r and r.status == "held" and token(ds) == "theirs"


def test_other_boot_is_resumed_only_when_it_stopped_heartbeating(ds) -> None:
    foreign(ds, CLAIM, vm="vm-a", boot="boot-0", pid=os.getpid(), heartbeat_ago_s=5)  # a live twin
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")
    assert not r and token(ds) == "theirs"
    ds.objs.pop(CLAIM)
    foreign(ds, CLAIM, vm="vm-a", boot="boot-0", pid=os.getpid(), heartbeat_ago_s=coord.quiet_s() + 60)
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")  # our own claim before a reboot
    assert r and r.status == "resumed" and token(ds) == r.claim.token


def test_same_boot_dead_pid_is_resumed(ds) -> None:
    foreign(ds, CLAIM, vm="vm-a", pid=999999)
    r = coord.claim_site(SITE, session=DSSession(ds), settle_s=0, vm="vm-a")
    assert r and r.status == "resumed"


def test_register_vm_refuses_a_live_twin(ds) -> None:
    s = DSSession(ds)
    path = coord.vm_claim_path("vm-a")
    assert path == f"{ROOT}/.mesa/vms/vm-a.claim"
    foreign(ds, path, vm="vm-a", host="other-host", machine="machine-2")
    with pytest.raises(coord.VmIdCollision, match="other-host"):
        coord.register_vm(session=s, vm="vm-a", settle_s=0)
    ds.objs.pop(path)
    foreign(ds, path, vm="vm-a", host="other-host", machine="machine-2", expires_in_s=-60)  # gone: stale
    reg = coord.register_vm(session=s, vm="vm-a", settle_s=0)
    assert reg.kind == "vm" and token(ds, path) == reg.token
    assert coord.release(reg, session=s) and path not in ds.objs


def test_fetch_generic_compares_content_versions_not_mtimes(ds, tmp_path) -> None:
    s = DSSession(ds)
    code = "DP1.00001.001"
    remote = {"productCode": code, "validatedAt": "2026-09-20T00:00:00+00:00", "runId": "r2", "accepted": [1]}
    put(ds, coord.generic_remote_path(code), json.dumps(remote).encode(),
        mtime=datetime(2026, 9, 20, tzinfo=UTC))
    local = tmp_path / "g" / f"{code}.validated.json"
    local.parent.mkdir()
    local.write_text(json.dumps({"productCode": code, "validatedAt": "2026-09-01T00:00:00+00:00", "accepted": []}))
    # a fresh git checkout: the local file's mtime is "now", newer than the Data Store copy
    assert coord.fetch_generic(code, local, session=s) == "fetched"
    assert json.loads(local.read_text())["runId"] == "r2"
    newer = {"productCode": code, "validatedAt": "2026-09-25T00:00:00+00:00", "accepted": [2]}
    local.write_text(json.dumps(newer))
    os.utime(local, (0, 0))  # an old mtime does not matter either
    assert coord.fetch_generic(code, local, session=s) == "kept"
    older = {"productCode": code, "validatedAt": "2026-09-10T00:00:00+00:00", "accepted": [0]}
    local.write_text(json.dumps(older))
    assert coord.publish_generic(code, local, session=s)["skipped"] == "remote-newer"
    assert json.loads(ds.content(coord.generic_remote_path(code)))["runId"] == "r2"
