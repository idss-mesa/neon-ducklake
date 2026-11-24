"""worker.main against a fake Data Store and a fake run.run_sites: pick, claim, run, journal, .done, release."""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from neon_ducklake import coord, journal_sync, state, worker

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)

UNIVERSE = [{"site": "TTTT", "habitat": "Terrestrial", "bytes": 5},
            {"site": "BBBB", "habitat": "Aquatic Wadeable Stream", "bytes": 20},
            {"site": "AAAA", "habitat": "Aquatic Lake", "bytes": 10}]


class FakeRun:
    """Stands in for run.run_sites: records calls, writes a journal row, optionally misbehaves."""

    def __init__(self, rc: int = 0, *, sleep: float = 0.0, action=None) -> None:
        self.rc, self.sleep, self.action = rc, sleep, action
        self.calls: list[tuple[list[str], dict]] = []
        self.env: list[dict[str, str | None]] = []

    def __call__(self, sites, **kw):
        self.calls.append((list(sites), kw))
        self.env.append({k: os.environ.get(k) for k in ("NEON_DUCKLAKE_VM_ID", "NEON_DUCKLAKE_HISTORY_MODE",
                                                        "NEON_DUCKLAKE_CURATION_SYNC", "NEON_DUCKLAKE_CHUNK_BYTES")})
        con = state.connect()
        con.execute("INSERT OR REPLACE INTO site_files(site, product, month, name, dl_status) VALUES (?,?,?,?,?)",
                    (sites[0], "DP1.00002.001", "2024-06", "f.csv", "ok"))
        con.close()
        if self.sleep:
            time.sleep(self.sleep)
        if self.action:
            self.action(sites[0])
        return self.rc


@pytest.fixture(autouse=True)
def lake_restores(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, list[str]]]:
    """stream.restore_lake (stream-core) stand-in: records the site and the journal rows present at the call."""
    calls: list[tuple[str, list[str]]] = []

    def fake(site: str) -> int:
        con = state.connect()
        calls.append((site, sorted(r[0] for r in con.execute("SELECT name FROM site_files WHERE site=?", (site,)))))
        con.close()
        return 0

    monkeypatch.setattr(worker, "_restore_lake", lambda: fake)
    return calls


@pytest.fixture
def vm(ds: DSIrods, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    # worker.main exports these into os.environ: register them so monkeypatch removes them afterwards
    for var in ("NEON_DUCKLAKE_HISTORY_MODE", "NEON_DUCKLAKE_CURATION_SYNC", "NEON_DUCKLAKE_CHUNK_BYTES",
                "NEON_DUCKLAKE_WORKSET_BYTES", "NEON_DUCKLAKE_WORKER_HISTORY_MODE", "NEON_DUCKLAKE_MESA_MULTIWRITER",
                "NEON_DUCKLAKE_SITE_CLAIMS"):
        monkeypatch.setenv(var, "placeholder")
        monkeypatch.delenv(var)
    return "vm-a"


def journal_files(ds: DSIrods, site: str) -> set[str]:
    """The files of the journal set that ``.mesa-journal/CURRENT`` points at (empty when there is none)."""
    j = f"{ROOT}/sites/{site}/.mesa-journal"
    if f"{j}/CURRENT" not in ds.objs:
        return set()
    setdir = f"{j}/" + json.loads(ds.content(f"{j}/CURRENT"))["manifest"].rsplit("/", 1)[0]
    return {p.rsplit("/", 1)[1] for p in ds.objs if p.startswith(setdir + "/")}


def run_worker(ds: DSIrods, fake: FakeRun, **kw) -> int:
    kw.setdefault("bootstrap", False)
    return worker.main(session=DSSession(ds), run_sites=fake, universe=UNIVERSE, **kw)


def foreign_claim(store: DSIrods, site: str, *, stale: bool) -> None:
    now = datetime.now(UTC)
    exp = now - timedelta(minutes=5) if stale else now + timedelta(hours=1)
    doc = {"kind": "site", "key": site, "vm": "vm-x", "host": "elsewhere", "pid": 1, "token": "x",
           "claimedAt": now.isoformat(), "heartbeatAt": now.isoformat(), "expiresAt": exp.isoformat(), "ttlS": 7200}
    store.add_obj(f"{ROOT}/sites/{site}/.claim", content=json.dumps(doc).encode())


def test_queue_order_is_aquatic_first_by_size() -> None:
    assert worker.plan_queue(None, universe=UNIVERSE) == ["AAAA", "BBBB", "TTTT"]
    assert worker.plan_queue(["TTTT", "AAAA"], universe=UNIVERSE) == ["TTTT", "AAAA"]
    assert worker.plan_queue(None, habitat="terrestrial", universe=UNIVERSE) == ["TTTT"]


def test_worker_skips_done_and_claimed_sites_and_finishes_one(ds, vm, monkeypatch) -> None:
    coord.write_done("AAAA", {"rc": 0}, session=DSSession(ds))
    foreign_claim(ds, "BBBB", stale=False)
    monkeypatch.setenv("NEON_DUCKLAKE_HISTORY_MODE", "direct")  # the worker forces spool anyway
    fake = FakeRun()
    assert run_worker(ds, fake, chunk_bytes=123) == 0
    assert [c[0] for c in fake.calls] == [["TTTT"]]
    kw = fake.calls[0][1]
    assert kw["stream"] is True and kw["history_mode"] == "spool" and kw["chunk_bytes"] == 123
    assert fake.env[0] == {"NEON_DUCKLAKE_VM_ID": "vm-a", "NEON_DUCKLAKE_HISTORY_MODE": "spool",
                           "NEON_DUCKLAKE_CURATION_SYNC": "on", "NEON_DUCKLAKE_CHUNK_BYTES": "123"}
    done = coord.read_done("TTTT", session=DSSession(ds))
    assert done["rc"] == 0 and done["vm"] == "vm-a" and done["journal"]["site_files"] == 1
    assert f"{ROOT}/sites/TTTT/.claim" not in ds.objs  # released
    assert "manifest.json" in journal_files(ds, "TTTT")
    assert f"{ROOT}/sites/BBBB/.claim" in ds.objs  # the other VM's claim untouched


def test_worker_steals_a_stale_site_and_restores_its_journal(ds, vm, tmp_path) -> None:
    other = state.connect(tmp_path / "other-vm.sqlite")
    other.execute("INSERT INTO site_files(site, product, month, name, dl_status, up_status) VALUES "
                  "('AAAA', 'DP1.20288.001', '2023-01', 'earlier.csv', 'ok', 'uploaded')")
    journal_sync.push_site_journal("AAAA", session=DSSession(ds), con=other)
    foreign_claim(ds, "AAAA", stale=True)
    seen: list[list[str]] = []

    def check_restored(site: str) -> None:
        con = state.connect()
        seen.append(sorted(r[0] for r in con.execute("SELECT name FROM site_files WHERE site=?", (site,))))
        con.close()

    fake = FakeRun(action=check_restored)
    assert run_worker(ds, fake, once=True) == 0
    assert seen == [["earlier.csv", "f.csv"]]  # the dead VM's journal was restored before the run
    assert coord.read_done("AAAA", session=DSSession(ds))["claimStatus"] == "stolen"


def test_direct_history_is_a_config_switch(ds, vm, monkeypatch) -> None:
    """DuckDB 2.0 multi-writer later: NEON_DUCKLAKE_WORKER_HISTORY_MODE=direct (with the multi-writer switch)
    records from every VM."""
    monkeypatch.setenv("NEON_DUCKLAKE_WORKER_HISTORY_MODE", "direct")
    monkeypatch.setenv("NEON_DUCKLAKE_MESA_MULTIWRITER", "1")
    fake = FakeRun()
    assert run_worker(ds, fake, once=True) == 0
    assert fake.calls[0][1]["history_mode"] == "direct" and fake.env[0]["NEON_DUCKLAKE_HISTORY_MODE"] == "direct"
    monkeypatch.setenv("NEON_DUCKLAKE_WORKER_HISTORY_MODE", "sometimes")
    with pytest.raises(SystemExit, match="spool or direct"):
        run_worker(ds, fake, once=True)


def test_direct_history_without_the_multiwriter_switch_stops_before_any_claim(ds, vm, monkeypatch) -> None:
    """N13: the orchestrator refuses direct history in stream mode without NEON_DUCKLAKE_MESA_MULTIWRITER=1
    (SystemExit); the worker must find that out at start, not after it claimed a site."""
    monkeypatch.setenv("NEON_DUCKLAKE_WORKER_HISTORY_MODE", "direct")
    monkeypatch.delenv("NEON_DUCKLAKE_MESA_MULTIWRITER", raising=False)
    fake = FakeRun()
    with pytest.raises(SystemExit, match="NEON_DUCKLAKE_MESA_MULTIWRITER"):
        run_worker(ds, fake)
    assert fake.calls == []
    assert not any(p.endswith(".claim") for p in ds.objs)  # no site claimed, no VM registered
    assert os.environ.get("NEON_DUCKLAKE_HISTORY_MODE") is None  # nothing exported either


def test_a_bad_run_setting_stops_the_worker_before_any_claim(ds, vm, monkeypatch) -> None:
    """Any other configuration error of the orchestrator (run.make_config) is found at start too."""
    monkeypatch.setenv("NEON_DUCKLAKE_WORKSET_BYTES", "lots")
    fake = FakeRun()
    with pytest.raises(SystemExit):
        run_worker(ds, fake)
    assert fake.calls == [] and not any(p.endswith(".claim") for p in ds.objs)


def test_failed_site_is_released_not_done_and_not_retried(ds, vm) -> None:
    fake = FakeRun(rc=2)
    assert run_worker(ds, fake, max_sites=2) == 1
    assert [c[0][0] for c in fake.calls] == ["AAAA", "BBBB"]
    assert coord.done_sites(session=DSSession(ds)) == set()
    assert not any(p.endswith("/.claim") for p in ds.objs)
    assert "manifest.json" in journal_files(ds, "AAAA")  # progress is still backed up


def test_interrupt_pushes_journal_and_releases(ds, vm) -> None:
    def interrupt(site: str) -> None:
        raise KeyboardInterrupt

    fake = FakeRun(action=interrupt)
    assert run_worker(ds, fake) == 130
    assert f"{ROOT}/sites/AAAA/.claim" not in ds.objs and "AAAA" not in coord.done_sites(session=DSSession(ds))
    assert "site_files.parquet" in journal_files(ds, "AAAA")


def test_orchestrator_signal_rc_stops_the_worker(ds, vm) -> None:
    fake = FakeRun(rc=130)  # run.run_sites caught SIGINT itself and returned 130
    assert run_worker(ds, fake) == 130
    assert [c[0] for c in fake.calls] == [["AAAA"]]  # no further site was started
    assert f"{ROOT}/sites/AAAA/.claim" not in ds.objs and "AAAA" not in coord.done_sites(session=DSSession(ds))


def test_lost_claim_is_left_to_the_new_owner(ds, vm) -> None:
    def takeover(site: str) -> None:
        path = f"{ROOT}/sites/{site}/.claim"
        doc = json.loads(ds.content(path)) | {"vm": "vm-b", "token": "theirs"}
        ds.set_content(path, json.dumps(doc).encode())
        time.sleep(0.3)  # several heartbeats

    fake = FakeRun(action=takeover)
    assert run_worker(ds, fake, once=True, heartbeat_s=0.02) == 1
    assert json.loads(ds.content(f"{ROOT}/sites/AAAA/.claim"))["token"] == "theirs"  # not released
    assert "AAAA" not in coord.done_sites(session=DSSession(ds))
    assert not any(p.startswith(f"{ROOT}/sites/AAAA/.mesa-journal") for p in ds.objs)  # nothing pushed over theirs


def test_heartbeat_runs_during_the_site(ds, vm) -> None:
    beats: list[str] = []

    def watch(site: str) -> None:
        beats.append(json.loads(ds.content(f"{ROOT}/sites/{site}/.claim"))["heartbeatAt"])

    fake = FakeRun(sleep=1.2, action=watch)
    assert run_worker(ds, fake, once=True, heartbeat_s=0.05) == 0
    claimed = coord.read_done("AAAA", session=DSSession(ds))
    assert beats and claimed["rc"] == 0


def test_nothing_to_do_and_bootstrap_without_a_lake(ds, vm, capsys) -> None:
    for site in ("AAAA", "BBBB", "TTTT"):
        coord.write_done(site, {"rc": 0}, session=DSSession(ds))
    fake = FakeRun()
    assert run_worker(ds, fake) == 0 and fake.calls == []
    assert "nothing left to claim (3 done" in capsys.readouterr().out
    # bootstrap finds no catalog lake anywhere and no universe is given: the worker cannot plan
    assert worker.main(session=DSSession(ds), run_sites=fake, bootstrap=True) == 1
