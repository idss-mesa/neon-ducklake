"""Cross-process iRODS connection budget (fcntl slot files) and the budgeted session pool (hermetic)."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from neon_ducklake import env, tuning
from neon_ducklake.tuning import BudgetTimeout, ConnectionBudget, SessionPool

from .test_avu_fakes import FakeIrods, FakeSession


def test_slots_are_exclusive_across_instances(tmp_path: Path) -> None:
    a = ConnectionBudget(3, lock_dir=tmp_path)
    b = ConnectionBudget(3, lock_dir=tmp_path)  # flock is per open file: a second instance acts like a process
    got_a = [a.try_acquire(), a.try_acquire()]
    got_b = b.try_acquire()
    assert None not in got_a and got_b is not None and len({*got_a, got_b}) == 3
    assert b.try_acquire() is None and a.try_acquire() is None
    assert a.in_use() == b.in_use() == 3 and a.held() == 2 and b.held() == 1
    a.release(got_a[0])
    assert b.try_acquire() == got_a[0]
    a.release_all()
    b.release_all()
    assert a.in_use() == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"irods-conn-{i:02d}.lock" for i in range(3)]


def test_acquire_waits_then_times_out(tmp_path: Path) -> None:
    now = [0.0]

    def sleep(dt: float) -> None:
        now[0] += dt

    holder = ConnectionBudget(1, lock_dir=tmp_path)
    assert holder.try_acquire() == 0
    b = ConnectionBudget(1, lock_dir=tmp_path, poll_s=1.0, timeout_s=5.0, warn_after_s=2.0, sleep=sleep,
                         clock=lambda: now[0])
    with pytest.raises(BudgetTimeout, match="1 slots"):
        b.acquire()
    assert b.waits >= 5
    holder.release(0)
    assert b.acquire() == 0


def test_a_killed_process_never_leaks_its_slot(tmp_path: Path) -> None:
    code = ("import sys, time; from neon_ducklake.tuning import ConnectionBudget; "
            "b = ConnectionBudget(2, lock_dir=sys.argv[1]); b.try_acquire(); b.try_acquire(); "
            "print('ready', flush=True); time.sleep(60)")
    proc = subprocess.Popen([sys.executable, "-c", code, str(tmp_path)], stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == "ready"
        mine = ConnectionBudget(2, lock_dir=tmp_path)
        assert mine.try_acquire() is None and mine.in_use() == 2  # held by the other process
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(10)
        assert mine.try_acquire() is not None and mine.try_acquire() is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(10)


def test_default_budget_resolves_lock_dir_and_size_at_call_time(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "locks")
    monkeypatch.delenv(tuning.BUDGET_ENV, raising=False)
    b = tuning.irods_budget()
    assert b.total == tuning.DEFAULT_IRODS_CONNECTIONS == 8 and b.lock_dir == tmp_path / "locks"
    monkeypatch.setenv(tuning.BUDGET_ENV, "12")
    assert tuning.irods_budget().total == 12
    monkeypatch.setenv(tuning.BUDGET_ENV, "zero")
    with pytest.raises(SystemExit, match="not an integer"):
        tuning.irods_budget()


# ---------------------------------------------------------------- session pool

def test_pool_reuses_idle_sessions_and_holds_one_slot_each(tmp_path: Path) -> None:
    s = FakeIrods()
    budget = ConnectionBudget(4, lock_dir=tmp_path)
    pool = SessionPool(FakeSession(s), budget=budget)
    with pool.lease() as a, pool.lease() as b:
        assert a is not b and budget.held() == 2
    with pool.lease() as c:
        assert c in (a, b)  # reused, not re-cloned
    assert s.calls["clone"] == 2 and budget.held() == 2  # idle sessions keep their connection (and slot)
    assert pool.trim(1) == 1 and budget.held() == 1
    pool.close()
    assert budget.held() == 0 and s.calls["cleanup"] == s.calls["clone"] == 2
    assert pool.stats()["sessions_created"] == 2 and pool.stats()["budget"]["total"] == 4


def test_pool_discards_a_session_after_a_connection_error(tmp_path: Path) -> None:
    s = FakeIrods()
    budget = ConnectionBudget(2, lock_dir=tmp_path)
    pool = SessionPool(FakeSession(s), budget=budget)
    with pytest.raises(ConnectionResetError), pool.lease():
        raise ConnectionResetError("Connection reset by peer")
    assert pool.discarded == 1 and budget.held() == 0 and s.calls["cleanup"] == 1
    with pytest.raises(ValueError), pool.lease():
        raise ValueError("semantic: keep the session")
    assert pool.discarded == 1 and budget.held() == 1
    pool.close()


def test_pool_waits_for_a_slot_and_reuses_returned_sessions(tmp_path: Path) -> None:
    s = FakeIrods()
    other = ConnectionBudget(2, lock_dir=tmp_path)
    assert other.try_acquire() is not None
    budget = ConnectionBudget(2, lock_dir=tmp_path, poll_s=0.001)
    pool = SessionPool(FakeSession(s), budget=budget)
    active, peak = [0], [0]
    lock = threading.Lock()

    def work() -> None:
        for _ in range(20):
            with pool.lease():
                with lock:
                    active[0] += 1
                    peak[0] = max(peak[0], active[0])
                time.sleep(0.0005)
                with lock:
                    active[0] -= 1

    threads = [threading.Thread(target=work) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak[0] == 1 and s.calls["clone"] == 1 and budget.peak_held == 1  # one free slot -> one session
    pool.close()
    assert budget.held() == 0


def test_dedicated_session_takes_extra_slots_and_evicts_idle_first(tmp_path: Path) -> None:
    s = FakeIrods()
    budget = ConnectionBudget(5, lock_dir=tmp_path, poll_s=0.001)
    pool = SessionPool(FakeSession(s), budget=budget)
    with pool.lease(), pool.lease(), pool.lease():
        pass
    assert budget.held() == 3
    with pool.dedicated(5) as big:
        assert budget.held() == 5 and big is not None  # the 3 idle clones gave way
    assert budget.held() == 0 and s.calls["cleanup"] == s.calls["clone"] == 4
    pool.close()


def test_pool_without_budget_or_clone() -> None:
    class Plain:
        pass

    root = Plain()
    pool = SessionPool(root)
    with pool.lease() as got:
        assert got is root
    pool.close()


def test_concurrent_multi_slot_leases_never_deadlock(tmp_path: Path) -> None:
    """Hold-and-wait regression: two dedicated() leases of 4 slots each on a 6-slot budget must both finish.

    Each budget instance is separate (different lock handles on the same files), mimicking two processes;
    before the all-or-nothing fix each could hold 3 slots and wait forever for a fourth.
    """
    lock_dir = tmp_path / "b6"
    done: list[str] = []
    gate = threading.Barrier(2)

    def worker(tag: str) -> None:
        budget = ConnectionBudget(6, lock_dir=lock_dir, poll_s=0.001, timeout_s=20)
        pool = SessionPool(FakeSession(FakeIrods()), budget=budget)
        for _ in range(25):
            gate.wait(timeout=20)
            with pool.dedicated(4):
                time.sleep(0.002)
        done.append(tag)

    threads = [threading.Thread(target=worker, args=(t,), daemon=True) for t in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert sorted(done) == ["a", "b"], "multi-slot leases deadlocked"
