"""AIMD controllers (tuning.py): increase / decrease / bounds / gates / thread safety (hermetic, fake clocks)."""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time

import pytest

from neon_ducklake.tuning import AIMD, AdaptiveLimit, AdaptiveRate, AsyncAdaptiveLimit, irods_error_kind, p95


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, dt: float) -> None:
        self.t += dt


def ctl(**kw) -> AIMD:
    base = {"start": 4, "lo": 2, "hi": 10, "target": 1.0, "ceiling": 3.0, "every": 5, "window": 20,
            "min_samples": 5, "cooldown_s": 10.0, "integer": True, "clock": kw.pop("clock", Clock())}
    base.update(kw)
    return AIMD("t", **base)


def test_p95_nearest_rank() -> None:
    assert p95([]) is None
    assert p95([0.1] * 19 + [9.0]) == pytest.approx(0.1)
    assert p95([0.1] * 18 + [9.0, 9.0]) == pytest.approx(9.0)


def test_additive_increase_every_k_successes_when_fast_and_binding(caplog) -> None:
    c = ctl()
    caplog.set_level(logging.INFO, logger="neon_ducklake.tuning")
    for _ in range(4):
        c.mark_binding()
        c.on_success(0.2)
    assert c.value == 4  # K = 5 not reached yet
    c.on_success(0.2)
    assert c.value == 5 and c.increases == 1
    assert "t: value 4 -> 5 (p95 0.2 < target 1 over 5 successes)" in caplog.text
    for _ in range(100):
        c.mark_binding()
        c.on_success(0.2)
    assert c.value == c.hi == 10 and c.max_seen == 10  # capped
    assert [ch.new for ch in c.history] == [5, 6, 7, 8, 9, 10]


def test_no_increase_when_not_binding_or_in_the_hold_band() -> None:
    c = ctl()
    for _ in range(50):
        c.on_success(0.2)  # never binding: a light load must not inflate the limit
    assert c.value == 4
    for _ in range(50):
        c.mark_binding()
        c.on_success(2.0)  # between target (1) and ceiling (3): hold
    assert c.value == 4 and c.decreases == 0


def test_multiplicative_decrease_on_error_with_cooldown_and_floor(caplog) -> None:
    clock = Clock()
    c = ctl(start=10, clock=clock)
    caplog.set_level(logging.INFO, logger="neon_ducklake.tuning")
    c.on_error("conn_limit")
    assert c.value == 5 and "value 10 -> 5 (error: conn_limit)" in caplog.text
    c.on_error("reset")  # same burst (within cooldown): no second halving
    assert c.value == 5 and c.errors == 2 and c.errors_by_kind == {"conn_limit": 1, "reset": 1}
    clock.t += 11
    c.on_error("timeout")
    assert c.value == 2  # floor(5 * 0.5) = 2 = lo
    clock.t += 11
    c.on_error("timeout")
    assert c.value == 2 and c.min_seen == 2 and c.decreases == 2


def test_errors_reset_the_success_count() -> None:
    c = ctl(cooldown_s=0.0, lo=4)
    for _ in range(4):
        c.mark_binding()
        c.on_success(0.1)
    c.on_error("x")  # at the floor: no change, but the next increase needs K fresh successes again
    for _ in range(4):
        c.mark_binding()
        c.on_success(0.1)
    assert c.value == 4
    c.mark_binding()
    c.on_success(0.1)
    assert c.value == 5


def test_decrease_when_p95_exceeds_the_ceiling() -> None:
    c = ctl(start=8)
    for _ in range(4):
        c.on_success(5.0)
    assert c.value == 8  # min_samples = 5 not reached
    c.on_success(5.0)
    assert c.value == 4 and c.history[-1].reason.startswith("p95 5 > ceiling 3")
    for _ in range(5):
        c.on_success(5.0)
    assert c.value == 4  # fresh window, but still inside the 10 s cooldown


def test_bounds_and_validation() -> None:
    assert ctl(start=100).value == 10 and ctl(start=0).value == 2
    rate = AIMD("r", start=7.5, lo=5, hi=60, target=0.25, ceiling=1.0)
    assert rate.value == 7.5 and rate.snapshot()["current"] == 7.5
    with pytest.raises(ValueError):
        AIMD("x", start=1, lo=5, hi=2, target=1, ceiling=2)
    with pytest.raises(ValueError):
        AIMD("x", start=1, lo=1, hi=2, target=3, ceiling=2)
    with pytest.raises(ValueError):
        AIMD("x", start=1, lo=1, hi=2, target=1, ceiling=2, factor=1.0)


def test_snapshot_reports_history() -> None:
    c = ctl(start=10)
    c.on_error("conn_limit")
    snap = c.snapshot()
    assert snap["start"] == 10 and snap["current"] == 5 and snap["min"] == 5 and snap["max"] == 10
    assert snap["changes"][0]["reason"] == "error: conn_limit" and snap["errors_by_kind"] == {"conn_limit": 1}


# ---------------------------------------------------------------- thread gate

def test_adaptive_limit_gate_blocks_at_the_limit() -> None:
    lim = AdaptiveLimit("g", start=2, lo=1, hi=4, every=1, min_samples=1, cooldown_s=0.0)
    lim.acquire()
    lim.acquire()
    assert lim.in_flight == 2 and not lim.acquire(timeout=0.01)  # blocked at the limit
    lim.on_error("x")  # 2 -> 1; calls in flight are not interrupted
    assert lim.limit == 1 and lim.in_flight == 2
    lim.release()
    assert not lim.acquire(timeout=0.01)  # 1 in flight >= new limit 1
    lim.release()
    assert lim.acquire(timeout=0.01)
    lim.release()


def test_adaptive_limit_growth_wakes_waiters() -> None:
    lim = AdaptiveLimit("g", start=1, lo=1, hi=3, every=1, min_samples=1)
    lim.acquire()
    entered = threading.Event()

    def waiter() -> None:
        with lim:
            entered.set()

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.02)
    assert not entered.is_set()
    lim.on_success(0.1)  # binding (the waiter queued) and fast -> 1 -> 2 wakes it
    assert entered.wait(1.0) and lim.limit == 2
    t.join()
    lim.release()


def test_adaptive_limit_is_thread_safe_under_concurrent_feedback() -> None:
    lim = AdaptiveLimit("g", start=4, lo=2, hi=12, every=3, min_samples=3, cooldown_s=0.0)
    violations: list[tuple[int, int]] = []
    guard = threading.Lock()
    active = [0]
    rng = random.Random(7)
    plan = [rng.random() for _ in range(2000)]

    def worker(k: int) -> None:
        for i in range(k, len(plan), 16):
            with lim:
                with guard:
                    active[0] += 1
                    if active[0] > lim.hi:
                        violations.append((active[0], lim.limit))
                time.sleep(0.0001)
                with guard:
                    active[0] -= 1
            if plan[i] < 0.03:
                lim.on_error("reset")
            else:
                lim.on_success(plan[i] * 0.5)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not violations and lim.in_flight == 0
    assert 2 <= lim.min_seen <= lim.limit <= lim.max_seen <= 12
    assert lim.peak <= 12 and lim.successes + lim.errors == 2000
    assert lim.increases > 0 and lim.decreases > 0


# ---------------------------------------------------------------- asyncio gate

def test_async_limit_caps_concurrency_and_grows() -> None:
    lim = AsyncAdaptiveLimit("a", start=2, lo=1, hi=6, every=4, min_samples=4)
    seen = {"active": 0, "max_over": 0, "peak": 0}

    async def job() -> None:
        async with lim:
            seen["active"] += 1
            seen["peak"] = max(seen["peak"], seen["active"])
            seen["max_over"] = max(seen["max_over"], seen["active"] - lim.limit)
            await asyncio.sleep(0.001)
            seen["active"] -= 1
        lim.on_success(0.01)

    async def main() -> None:
        await asyncio.gather(*(job() for _ in range(200)))

    asyncio.run(main())
    assert seen["max_over"] <= 0 and lim.in_flight == 0
    assert lim.limit == 6 and seen["peak"] == 6 and lim.peak == 6


def test_async_limit_decrease_and_cancellation() -> None:
    lim = AsyncAdaptiveLimit("a", start=2, lo=1, hi=4, cooldown_s=0.0)

    async def main() -> None:
        await lim.__aenter__()
        await lim.__aenter__()
        waiter = asyncio.ensure_future(lim.__aenter__())
        await asyncio.sleep(0)
        assert not waiter.done()
        waiter.cancel()  # a cancelled waiter must not leak a slot
        with pytest.raises(asyncio.CancelledError):
            await waiter
        lim.on_error("transport")  # 2 -> 1
        await lim.__aexit__(None, None, None)
        blocked = asyncio.ensure_future(lim.__aenter__())
        await asyncio.sleep(0)
        assert not blocked.done()  # 1 in flight >= limit 1
        await lim.__aexit__(None, None, None)
        await asyncio.wait_for(blocked, 1.0)
        assert lim.in_flight == 1
        await lim.__aexit__(None, None, None)

    asyncio.run(main())
    assert lim.in_flight == 0


# ---------------------------------------------------------------- adaptive rate

def test_adaptive_rate_paces_grows_and_halves() -> None:
    clock = Clock()
    r = AdaptiveRate("r", start=10, lo=5, hi=20, target=0.25, ceiling=1.0, step=2, every=10, window=20,
                     min_samples=5, cooldown_s=1.0, clock=clock, sleep=clock.sleep)
    t0 = clock.t
    for _ in range(11):
        r.acquire()
    assert clock.t - t0 == pytest.approx(1.0)  # 1 banked token + 10 at 10/s
    for _ in range(10):
        r.acquire()
        r.on_success(0.05)
    assert r.rate == 12  # the bucket was binding and p95 50 ms < 250 ms
    for _ in range(200):
        r.acquire()
        r.on_success(0.05)
    assert r.rate == 20 and r.max_seen == 20
    for _ in range(5):
        r.acquire()
        clock.t += 1.5
        r.on_success(1.5)
    assert r.rate == 10 and r.history[-1].reason.startswith("p95 1.5 > ceiling 1")
    clock.t += 2
    r.on_error("conn_limit")
    assert r.rate == 5 and r.min_seen == 5
    with pytest.raises(ValueError):
        AdaptiveRate("r", start=0, lo=5, hi=20, target=0.25, ceiling=1.0)


def test_adaptive_rate_does_not_grow_when_callers_are_slower_than_the_rate() -> None:
    clock = Clock()
    r = AdaptiveRate("r", start=10, lo=5, hi=60, target=0.25, ceiling=1.0, every=5, min_samples=5,
                     clock=clock, sleep=clock.sleep)
    for _ in range(100):
        r.acquire()
        clock.t += 0.2  # 5 calls/s < 10 calls/s: never waits, so never binding
        r.on_success(0.2)
    assert r.rate == 10


# ---------------------------------------------------------------- error classification

def test_irods_error_kind() -> None:
    class SYS_MAX_CONNECT_COUNT_EXCEEDED(Exception):
        pass

    class NetworkException(Exception):
        pass

    assert irods_error_kind(SYS_MAX_CONNECT_COUNT_EXCEEDED("x")) == "conn_limit"
    assert irods_error_kind(RuntimeError("LOCKED_DATA_OBJECT_ACCESS")) == "retryable"
    assert irods_error_kind(TimeoutError("read")) == "timeout"
    assert irods_error_kind(OSError("timed out")) == "timeout"
    assert irods_error_kind(ConnectionResetError(104, "reset")) == "reset"
    assert irods_error_kind(NetworkException("Could not receive server response")) == "reset"
    assert irods_error_kind(RuntimeError("CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME")) is None
    assert irods_error_kind(ValueError("bad")) is None


def test_learned_ceiling_stops_the_sawtooth_then_expires() -> None:
    """A reset at 10 caps growth at 9 for cap_hold_s; after it expires the controller probes again."""
    from neon_ducklake.tuning import AIMD

    now = [0.0]
    a = AIMD("upload", start=8, lo=4, hi=32, target=1.0, ceiling=3.0, every=4, min_samples=2, cooldown_s=0,
             integer=True, cap_hold_s=100.0, clock=lambda: now[0])

    def grow(n: int) -> None:
        for _ in range(n):
            a.mark_binding()
            a.on_success(0.5)

    grow(8)
    assert a.value == 10
    a.on_error("reset")
    assert a.value == 5 and a.cap == 9
    grow(80)
    assert a.value == 9  # never re-enters the limit that reset us
    now[0] += 101.0
    grow(8)
    assert a.value >= 10  # cap expired: probing resumes
    a.on_error("timeout")
    assert a.cap is not None and a.cap < a.hi


def test_semantic_errors_do_not_learn_a_ceiling() -> None:
    from neon_ducklake.tuning import AIMD

    a = AIMD("apply", start=20, lo=5, hi=60, target=0.25, ceiling=1.0, cooldown_s=0)
    a.on_error("catalog")
    assert a.cap is None
