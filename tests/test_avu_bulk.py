from __future__ import annotations

from datetime import datetime

import pytest

from neon_ducklake import env, state, tuning
from neon_ducklake.avu import bulk
from neon_ducklake.avu.bulk import LatencyAbort, LatencyMonitor, TokenBucket, apply_scope, replace_desired
from neon_ducklake.tuning import ConnectionBudget

from .test_avu_fakes import FakeIrods, FakeMeta, FakeSession

R = "/iplant/home/testuser/neon-ducklake"
C = f"{R}/sites/SRER/raw/DP1.00002.001/2024-06"
F1, F2 = f"{C}/a.csv", f"{C}/b.csv"


@pytest.fixture(autouse=True)
def _no_pacing(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    if "token_bucket" not in request.node.name and "adaptive_rate" not in request.node.name:
        monkeypatch.setattr(bulk.TokenBucket, "acquire", lambda self: 0.0)
        monkeypatch.setattr(tuning.AdaptiveRate, "acquire", lambda self: 0.0)


@pytest.fixture(autouse=True)
def _hermetic_locks(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The default iRODS connection budget lives in env.LOCK_DIR: never the real work/locks."""
    monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "locks")


@pytest.fixture
def con(tmp_path):
    return state.connect(tmp_path / "state.sqlite")


@pytest.fixture
def irods() -> FakeIrods:
    s = FakeIrods()
    s.add_coll(C, (("mesa.enabled", "true", ""), ("ipc_UUID", "c-1", ""), ("foreign.attr", "keep", "")))
    s.add_obj(F1, (("ipc_UUID", "u-1", ""),))
    s.add_obj(F2, (("ipc_UUID", "u-2", ""), ("neon.month", "2024-06", "")),
              replicas=[{"number": 0, "size": 1, "checksum": "x", "status": "1"},
                        {"number": 1, "size": 1, "checksum": "x", "status": "1"}])
    return s


def desired(extra: list[tuple] | None = None, drop: set[str] | None = None) -> list[tuple]:
    rows = [
        (C, C, "collection", "neon.month", "2024-06", "", "month", "site.avus"),
        (C, C, "collection", "neon.fileCount", "2", "count", "month", "site.avus"),
        (C, F1, "data_object", "neon.productCode", "DP1.00002.001", "", "object", "site.avus"),
        (C, F1, "data_object", "neon.month", "2024-06", "", "object", "site.avus"),
        (C, F2, "data_object", "neon.productCode", "DP1.00002.001", "", "object", "site.avus"),
        (C, F2, "data_object", "neon.month", "2024-06", "", "object", "site.avus"),  # already present, not ours
    ]
    rows = [r for r in rows if not drop or r[3] not in drop] + (extra or [])
    return rows


def journal(con, **where):
    q = "SELECT * FROM avu_journal"
    if where:
        q += " WHERE " + " AND ".join(f"{k}=?" for k in where)
    return [dict(r) for r in con.execute(q + " ORDER BY id", tuple(where.values()))]


def test_apply_adds_and_leaves_foreign_and_protected(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert irods.avus(C) >= {("neon.month", "2024-06", ""), ("neon.fileCount", "2", "count"),
                             ("mesa.enabled", "true", ""), ("ipc_UUID", "c-1", ""), ("foreign.attr", "keep", "")}
    assert ("neon.productCode", "DP1.00002.001", "") in irods.avus(F1)
    assert ("ipc_UUID", "u-2", "") in irods.avus(F2)
    assert stats["adds"] == 5 and stats["removes"] == 0 and stats["errors"] == 0
    # one GenQuery for the collection's AVUs + one for its objects' AVUs (replica rows collapsed), plus the
    # two spool listings (spool/, spool-done/ of this VM) looked up once for the adopted AVU's original time
    assert irods.calls["query"] == 4
    assert irods.calls["atomic"] == 3
    rows = journal(con)
    assert len(rows) == 6 and {r["status"] for r in rows} == {"applied"}
    for r in rows:
        assert datetime.fromisoformat(r["applied_at"]).utcoffset().total_seconds() == 0
    # the pre-existing, desired neon.month on F2 is adopted (journaled, not written), nothing else is
    (adopted,) = [r for r in rows if r["adopted"]]
    assert (adopted["path"], adopted["attribute"], adopted["op"]) == (F2, "neon.month", "add")
    assert stats["adopted"] == 1 and adopted["generator"] == "site.avus"
    assert not [r for r in rows if r["attribute"] in ("ipc_UUID", "mesa.enabled", "foreign.attr")]


def test_second_run_is_a_noop(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    n_rows, n_atomic = len(journal(con)), irods.calls["atomic"]
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert irods.calls["atomic"] == n_atomic
    assert len(journal(con)) == n_rows
    assert stats.get("adds", 0) == 0 and stats.get("removes", 0) == 0


def test_removes_only_journal_owned_and_before_adds(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    # a foreign writer adds another value of an attribute we own
    irods.colls[C]["avus"].add(("neon.fileCount", "99", "count"))
    new = [r for r in desired(drop={"neon.fileCount"})] + [
        (C, C, "collection", "neon.fileCount", "3", "count", "month", "site.avus")]
    new = [r for r in new if not (r[1] == F2 and r[3] == "neon.month")]  # un-desire the adopted AVU
    replace_desired(con, "SRER", new)
    irods.atomic_log.clear()
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    avus = irods.avus(C)
    assert ("neon.fileCount", "2", "count") not in avus  # ours: removed
    assert ("neon.fileCount", "99", "count") in avus  # foreign (never desired, never adopted): kept
    assert ("neon.fileCount", "3", "count") in avus
    assert ("neon.month", "2024-06", "") not in irods.avus(F2)  # adopted on the first run: owned, so removed
    assert ("ipc_UUID", "u-2", "") in irods.avus(F2)
    assert stats["removes"] == 2 and stats["adds"] == 1 and stats["adopted"] == 0
    (p1, ops1), (p2, ops2) = sorted(irods.atomic_log)
    assert (p1, [o for o, _ in ops1]) == (C, ["remove", "add"]) and (p2, [o for o, _ in ops2]) == (F2, ["remove"])
    rm = journal(con, op="remove")
    assert len(rm) == 2 and {r["status"] for r in rm} == {"applied"} and {r["generator"] for r in rm} == {"site.avus"}


def test_protected_desired_rows_are_dropped(con, irods) -> None:
    res = replace_desired(con, "SRER", desired(extra=[(C, C, "collection", "mesa.enabled", "false", "", "x", "g"),
                                                      (C, F1, "data_object", "ipc_x", "1", "", "x", "g"),
                                                      (C, F1, "data_object", "neon.empty", "  ", "", "x", "g")]))
    assert res["dropped_protected"] == 2 and res["dropped_empty"] == 1
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert ("mesa.enabled", "false", "") not in irods.avus(C)


def test_missing_targets_are_reported_and_skipped(con, irods) -> None:
    ghost_coll = f"{R}/sites/SRER/raw/DP1.00002.001/2024-07"
    replace_desired(con, "SRER", desired(extra=[
        (C, f"{C}/ghost.csv", "data_object", "neon.month", "2024-06", "", "object", "g"),
        (ghost_coll, ghost_coll, "collection", "neon.month", "2024-07", "", "month", "g"),
        (ghost_coll, f"{ghost_coll}/x.csv", "data_object", "neon.month", "2024-07", "", "object", "g")]))
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert stats["missing"] == 3
    assert set(stats["missing_paths"]) == {f"{C}/ghost.csv", ghost_coll, f"{ghost_coll}/x.csv"}
    assert not [r for r in journal(con) if "ghost" in r["path"] or "2024-07" in r["path"]]
    assert stats["errors"] == 0


def test_atomic_failure_retries_once(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    irods.fail_atomic = 1
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert stats["retries"] == 1 and stats["errors"] == 0
    assert {r["status"] for r in journal(con)} == {"applied"}
    assert irods.calls["meta_get"] == 1  # the re-read


def test_atomic_twice_then_per_avu_fallback_with_noops(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    irods.objs.pop(F2)
    replace_desired(con, "SRER", [r for r in desired() if r[1] != F2])
    irods.fail_atomic = 2
    state_ = {"n": 0}

    def concurrent_writer(s: FakeIrods) -> None:
        state_["n"] += 1
        if state_["n"] == 1:  # before the re-read: someone else adds one desired AVU
            s.colls[C]["avus"].add(("neon.month", "2024-06", ""))
        else:  # before the per-AVU fallback: and another one
            s.colls[C]["avus"].add(("neon.fileCount", "2", "count"))

    irods.on_atomic_fail.append(concurrent_writer)
    stats = apply_scope("SRER", only_units=[C], con=con, session=FakeSession(irods), threads=1)
    rows = {r["attribute"]: r["status"] for r in journal(con, path=C)}
    assert rows == {"neon.month": "noop", "neon.fileCount": "noop"}
    assert stats["noops"] == 2 and stats["errors"] == 0
    assert irods.calls["meta_add"] == 1  # fallback tried neon.fileCount -> already exists -> noop


def test_failed_call_that_actually_applied_is_confirmed(con, irods) -> None:
    replace_desired(con, "SRER", [r for r in desired() if r[1] == C])
    irods.fail_atomic = 1

    def server_committed(s: FakeIrods) -> None:  # connection dropped after the server applied everything
        s.colls[C]["avus"] |= {("neon.month", "2024-06", ""), ("neon.fileCount", "2", "count")}

    irods.on_atomic_fail.append(server_committed)
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert {r["status"] for r in journal(con, path=C)} == {"applied"}


def test_chunks_of_at_most_500_ops(con, irods) -> None:
    rows = [(C, F1, "data_object", f"neon.k{i:04d}", str(i), "", "object", "g") for i in range(1203)]
    replace_desired(con, "SRER", rows)
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    sizes = [len(ops) for p, ops in irods.atomic_log if p == F1]
    assert sizes == [500, 500, 203]
    assert len(irods.avus(F1)) == 1203 + 1


def test_stale_intents_are_reconciled(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    ins = ("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, unit, op, generator,"
           " status) VALUES ('SRER', ?, ?, ?, ?, ?, ?, 'add', 'g', 'intent')")
    # crash after the iRODS call: the AVU is there -> becomes applied (and thus owned)
    irods.objs[F1]["avus"].add(("neon.productCode", "DP1.00002.001", ""))
    con.execute(ins, (C, F1, "data_object", "neon.productCode", "DP1.00002.001", ""))
    # crash before the call: absent -> abandoned, then re-added normally
    con.execute(ins, (C, C, "collection", "neon.month", "2024-06", ""))
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert stats["intents_reconciled"] == 2
    st = [(r["path"], r["attribute"], r["status"]) for r in journal(con)[:2]]
    assert st == [(F1, "neon.productCode", "applied"), (C, "neon.month", "abandoned")]
    assert ("neon.month", "2024-06", "") in irods.avus(C)
    # the reconciled row is owned: dropping it from desired removes it
    replace_desired(con, "SRER", [r for r in desired() if not (r[1] == F1 and r[3] == "neon.productCode")])
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert ("neon.productCode", "DP1.00002.001", "") not in irods.avus(F1)


def test_owned_but_already_absent_records_a_delete(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    irods.objs[F1]["avus"].discard(("neon.month", "2024-06", ""))  # removed outside our tool
    replace_desired(con, "SRER", [r for r in desired() if not (r[1] == F1 and r[3] == "neon.month")])
    n = irods.calls["atomic"]
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert irods.calls["atomic"] == n
    assert stats["absent_removes"] == 1
    (rm,) = journal(con, op="remove")
    assert rm["status"] == "applied" and rm["error"] == "already absent in iRODS"


def test_units_absent_from_desired_are_untouched(con, irods) -> None:
    other = f"{R}/sites/SRER/raw/DP1.00002.001"
    irods.add_coll(other)
    replace_desired(con, "SRER", desired() + [(other, other, "collection", "neon.productCode", "DP1.00002.001",
                                               "", "product", "g")])
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    replace_desired(con, "SRER", desired())  # a filtered build: the product unit is not in it
    apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert ("neon.productCode", "DP1.00002.001", "") in irods.avus(other)


def test_dry_run_writes_nothing(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1, dry_run=True)
    assert stats["adds"] == 5 and irods.calls["atomic"] == 0 and journal(con) == []


def test_threads_use_one_clone_each(con) -> None:
    s = FakeIrods()
    rows = []
    for i in range(12):
        c = f"{R}/sites/SRER/raw/DP1.00002.001/2023-{i + 1:02d}"
        s.add_obj(f"{c}/f.csv", (("ipc_UUID", str(i), ""),))
        rows += [(c, c, "collection", "neon.month", c[-7:], "", "month", "g"),
                 (c, f"{c}/f.csv", "data_object", "neon.month", c[-7:], "", "object", "g")]
    replace_desired(con, "SRER", rows)
    stats = apply_scope("SRER", con=con, session=FakeSession(s), threads=40)
    assert stats["threads"] == bulk.MAX_THREADS
    assert stats["adds"] == 24 and stats["units_done"] == 12
    assert 1 <= s.calls["clone"] <= bulk.MAX_THREADS and s.calls["cleanup"] == s.calls["clone"]


def test_latency_abort_keeps_applied_rows(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    w = bulk.BulkWriter(con, "SRER", FakeSession(irods), monitor=LatencyMonitor(window=2, limit_s=-1.0))
    with pytest.raises(LatencyAbort):
        w.run(bulk.load_desired(con, "SRER"), threads=1)
    rows = journal(con)
    applied = [r for r in rows if r["status"] == "applied" and not r["adopted"]]  # adopted rows write nothing
    assert len(applied) == sum(len(ops) for _, ops in irods.atomic_log) and applied
    # the re-run resumes: stale intents reconciled, everything applied
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1)
    assert stats["errors"] == 0
    assert ("neon.month", "2024-06", "") in irods.avus(F1)


def test_latency_monitor_p95() -> None:
    m = LatencyMonitor(window=200, limit_s=2.0)
    for _ in range(189):
        m.observe(0.1)
    with pytest.raises(LatencyAbort):
        for _ in range(11):
            m.observe(5.0)
    m2 = LatencyMonitor(window=200, limit_s=2.0)
    for i in range(400):
        m2.observe(5.0 if i % 50 == 0 else 0.1)  # 2% slow: p95 stays low
    assert m2.p95() == pytest.approx(0.1)


def test_token_bucket_paces() -> None:
    now = [0.0]
    slept: list[float] = []

    def sleep(s: float) -> None:
        slept.append(s)
        now[0] += s

    b = TokenBucket(2.0, clock=lambda: now[0], sleep=sleep)
    for _ in range(5):
        b.acquire()
    assert sum(slept) == pytest.approx(2.0)


# ---------------------------------------------------------------- adaptive rate + connection budget

class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, dt: float) -> None:
        self.t += dt


def _many_objects(s: FakeIrods, n_colls: int, per_coll: int) -> list[tuple]:
    rows = []
    for i in range(n_colls):
        c = f"{R}/sites/SRER/raw/DP1.00002.001/2023-{i + 1:02d}"
        for j in range(per_coll):
            f = f"{c}/f{j:03d}.csv"
            s.add_obj(f)
            rows.append((c, f, "data_object", "neon.month", c[-7:], "", "object", "g"))
    return rows


def test_adaptive_rate_grows_while_fast_then_halves_to_the_floor(con, monkeypatch) -> None:
    clock = Clock()
    s = FakeIrods()
    replace_desired(con, "SRER", _many_objects(s, 12, 40))  # 480 objects -> 480 atomic calls
    calls = [0]
    orig = FakeMeta.apply_atomic_operations

    def timed(self, model, path, *ops):
        calls[0] += 1
        clock.t += 0.01 if calls[0] <= 300 else 1.5  # 10 ms, then a sick iCAT at 1.5 s per call
        return orig(self, model, path, *ops)

    monkeypatch.setattr(FakeMeta, "apply_atomic_operations", timed)
    stats = apply_scope("SRER", con=con, session=FakeSession(s), threads=1, rate=20.0, clock=clock,
                        sleep=clock.sleep)
    assert stats["adds"] == 480 and stats["errors"] == 0
    assert stats["rate_start"] == 20.0 and stats["rate_max"] > 30  # grew in +2/s steps while p95 < 250 ms
    assert stats["rate_final"] == bulk.RATE_MIN == stats["rate_min"]  # halved on p95 > 1 s, down to 5/s
    reasons = [c["reason"] for c in stats["rate_changes"]]
    assert any("< target 0.25" in r for r in reasons) and any("> ceiling 1" in r for r in reasons)


def test_adaptive_rate_halves_on_connection_errors_but_not_on_semantic_ones(con, irods, monkeypatch) -> None:
    clock = Clock()
    replace_desired(con, "SRER", desired())
    errors = [ConnectionResetError("Connection reset by peer"), RuntimeError("CAT_SQL_ERR semantic")]
    orig = FakeMeta.apply_atomic_operations

    def flaky(self, model, path, *ops):
        if errors:
            raise errors.pop(0)
        return orig(self, model, path, *ops)

    monkeypatch.setattr(FakeMeta, "apply_atomic_operations", flaky)
    stats = apply_scope("SRER", con=con, session=FakeSession(irods), threads=1, rate=20.0, clock=clock,
                        sleep=clock.sleep)
    assert stats["errors"] == 0 and stats["retries"] == 1  # reset -> re-read + atomic retry -> semantic -> per-AVU
    assert stats["rate_final"] == 10.0 and [c["reason"] for c in stats["rate_changes"]] == ["error: reset"]
    assert {r["status"] for r in journal(con)} == {"applied"}


def test_adaptive_rate_start_is_clamped(con, irods) -> None:
    replace_desired(con, "SRER", desired())
    assert apply_scope("SRER", con=con, session=FakeSession(irods), threads=1, rate=500.0)["rate_start"] == 60.0
    assert bulk.adaptive_rate(1.0).rate == bulk.RATE_MIN
    assert bulk.P95_LIMIT_S == 5.0  # the hard stop


def test_sessions_come_from_the_shared_budget(con, tmp_path) -> None:
    s = FakeIrods()
    replace_desired(con, "SRER", _many_objects(s, 12, 2))
    other = ConnectionBudget(3, lock_dir=tmp_path / "b")  # another process holds one slot
    assert other.try_acquire() is not None
    budget = ConnectionBudget(3, lock_dir=tmp_path / "b", poll_s=0.001)
    stats = apply_scope("SRER", con=con, session=FakeSession(s), threads=16, budget=budget)
    assert stats["adds"] == 24 and stats["units_done"] == 12 and stats["threads"] == 16
    assert stats["sessions_created"] <= 2 and budget.peak_held <= 2 and budget.held() == 0
    assert s.calls["cleanup"] == s.calls["clone"]
