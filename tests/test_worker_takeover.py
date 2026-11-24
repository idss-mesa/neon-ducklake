"""worker: journal then site-lake restore before a run (B2, M3), stop on a lost claim (B4), cleanup rc 3 as done
with a warning (B2 #5), restore failures, and the VM id collision guard (m5)."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta

import pytest

from neon_ducklake import coord, env, journal_sync, state, worker
from neon_ducklake.site import chunks

from .test_coord_fakes import ROOT, DSSession
from .test_worker_loop import UNIVERSE, FakeRun, ds, lake_restores, run_worker, vm  # noqa: F401 - fixtures


def other_vm_push(ds, tmp_path, monkeypatch, site: str, names: list[str], vm_name: str = "vm-x") -> None:  # noqa: F811
    """Another VM pushes a journal of ``site`` holding ``names``."""
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", vm_name)
    other = state.connect(tmp_path / f"{vm_name}.sqlite")
    journal_sync.restore_site_journal(site, session=DSSession(ds), con=other)  # it continued from the backup
    for n in names:
        other.execute("INSERT OR REPLACE INTO site_files(site, product, month, name, dl_status, up_status) VALUES "
                      "(?, 'DP1.20288.001', '2023-01', ?, 'ok', 'uploaded')", (site, n))
    journal_sync.push_site_journal(site, session=DSSession(ds), con=other)
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")


def test_journal_then_lake_are_restored_before_the_run(ds, vm, tmp_path, monkeypatch, lake_restores) -> None:  # noqa: F811
    other_vm_push(ds, tmp_path, monkeypatch, "AAAA", ["earlier.csv"])
    fake = FakeRun()
    assert run_worker(ds, fake, once=True) == 0
    assert lake_restores == [("AAAA", ["earlier.csv"])]  # the lake restore saw the restored journal
    assert fake.calls[0][0] == ["AAAA"]


def test_a_newer_journal_of_another_vm_is_restored_over_local_rows(ds, vm, tmp_path, monkeypatch) -> None:  # noqa: F811
    """M3: this VM worked on the site before (it has rows and a push mark); another VM went further since."""
    con = state.connect()
    con.execute("INSERT INTO site_files(site, product, month, name, dl_status) VALUES "
                "('AAAA', 'DP1.20288.001', '2023-01', 'mine.csv', 'ok')")
    journal_sync.push_site_journal("AAAA", session=DSSession(ds), con=con)
    con.close()
    other_vm_push(ds, tmp_path, monkeypatch, "AAAA", ["mine.csv", "theirs.csv"])
    seen: list[list[str]] = []

    def names(site: str) -> None:
        c = state.connect()
        seen.append(sorted(r[0] for r in c.execute("SELECT name FROM site_files WHERE site=?", (site,))))
        c.close()

    assert run_worker(ds, FakeRun(action=names), once=True) == 0  # acquired normally, not stolen
    assert seen == [["f.csv", "mine.csv", "theirs.csv"]]


def test_a_failed_restore_does_not_run_the_site(ds, vm, tmp_path, monkeypatch) -> None:  # noqa: F811
    other_vm_push(ds, tmp_path, monkeypatch, "AAAA", ["earlier.csv"])
    monkeypatch.setattr(journal_sync, "restore_detail",
                        lambda site, **kw: (_ for _ in ()).throw(RuntimeError("sha256 mismatch")))
    fake = FakeRun()
    assert run_worker(ds, fake, once=True) == 1
    assert fake.calls == [] and f"{ROOT}/sites/AAAA/.claim" not in ds.objs
    assert "AAAA" not in coord.done_sites(session=DSSession(ds))


def test_a_failed_lake_restore_does_not_run_the_site(ds, vm, monkeypatch) -> None:  # noqa: F811
    monkeypatch.setattr(worker, "_restore_lake", lambda: (lambda site: 2))
    fake = FakeRun()
    assert run_worker(ds, fake, once=True) == 1 and fake.calls == []


def test_the_run_gets_the_claims_lost_event_and_stops(ds, vm) -> None:  # noqa: F811
    """B4: the orchestrator receives stop_event=claim.lost; a takeover stops the run, nothing is pushed."""
    got: dict[str, threading.Event] = {}

    def run_sites(sites, **kw):
        stop = kw["stop_event"]
        got["stop"] = stop
        path = f"{ROOT}/sites/{sites[0]}/.claim"
        ds.set_content(path, json.dumps(json.loads(ds.content(path)) | {"vm": "vm-b", "token": "theirs"}).encode())
        assert stop.wait(5), "the heartbeat never reported the lost claim"
        return 130  # the orchestrator stopped because stop_event was set

    rc = worker.main(session=DSSession(ds), run_sites=run_sites, universe=UNIVERSE, once=True, bootstrap=False,
                     heartbeat_s=0.02)
    assert rc == 1 and got["stop"].is_set()  # a lost site, not an interrupt of the worker
    assert json.loads(ds.content(f"{ROOT}/sites/AAAA/.claim"))["token"] == "theirs"
    assert not any(p.startswith(f"{ROOT}/sites/AAAA/.mesa-journal") for p in ds.objs)


def test_cleanup_rc3_counts_as_done_with_a_warning(ds, vm) -> None:  # noqa: F811
    def finished_but_files_kept(site: str) -> None:
        con = state.connect()
        chunks.ensure_schema(con)
        con.execute("INSERT INTO stream_chunks(site, chunk_id, product, months, status, done) VALUES "
                    "(?, 'P__2024-01_2024-01', 'P', '[\"2024-01\"]', 'deleted', 'deleted')", (site,))
        for step in chunks.SITE_STEPS:
            if step != "cleanup":
                chunks.set_step(con, site, step, "ok")
        chunks.set_step(con, site, "cleanup", "partial", error="2 file(s) kept")
        con.close()

    assert run_worker(ds, FakeRun(rc=1, action=finished_but_files_kept), once=True) == 0
    done = coord.read_done("AAAA", session=DSSession(ds))
    assert done["rc"] == 0 and "2 file(s) kept" in done["warnings"][0]


def test_a_real_failure_is_not_done(ds, vm) -> None:  # noqa: F811
    def half(site: str) -> None:
        con = state.connect()
        chunks.ensure_schema(con)
        chunks.set_step(con, site, "terms", "ok")
        chunks.set_step(con, site, "cleanup", "partial")
        con.close()

    assert run_worker(ds, FakeRun(rc=1, action=half), once=True) == 1
    assert "AAAA" not in coord.done_sites(session=DSSession(ds))


def test_a_live_twin_vm_id_stops_the_worker(ds, vm) -> None:  # noqa: F811
    now = datetime.now(UTC)
    doc = {"kind": "vm", "key": "vm-a", "vm": "vm-a", "host": "another-host", "pid": 1, "token": "twin",
           "machine": "m-2", "boot": "b-2", "claimedAt": now.isoformat(), "heartbeatAt": now.isoformat(),
           "expiresAt": (now + timedelta(hours=1)).isoformat(), "ttlS": 7200}
    ds.add_obj(f"{ROOT}/.mesa/vms/vm-a.claim", content=json.dumps(doc).encode())
    fake = FakeRun()
    with pytest.raises(coord.VmIdCollision, match="another-host"):
        run_worker(ds, fake)
    assert fake.calls == []


def test_the_vm_registration_is_released_at_exit(ds, vm) -> None:  # noqa: F811
    assert run_worker(ds, FakeRun(), once=True) == 0
    assert f"{ROOT}/.mesa/vms/vm-a.claim" not in ds.objs
    assert env.vm_id() == "vm-a"
