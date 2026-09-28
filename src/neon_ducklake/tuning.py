"""Shared adaptive-throughput helpers: AIMD controllers and the cross-process iRODS connection budget.

AIMD policy (:class:`AIMD`, the base of :class:`AdaptiveLimit`, :class:`AsyncAdaptiveLimit` and
:class:`AdaptiveRate`). Callers report every outcome with ``on_success(latency)`` or ``on_error(kind)``.

- **Additive increase.** Once ``every`` successes have been seen since the last change, the value grows
  by ``step`` (capped at ``hi``), but only when the p95 of the latest ``every`` latencies is below
  ``target`` *and* the control was binding. Binding means a caller had to wait at the gate or bucket, so
  a light load never inflates the value.
- **Multiplicative decrease.** An error such as a connection limit, reset, timeout or retryable server
  state multiplies the value by ``factor`` (floored at ``lo``). So does a p95 above ``ceiling`` over at
  least ``min_samples`` fresh samples. Decreases within ``cooldown_s`` of the previous one are skipped:
  a burst of failures from requests already in flight costs one halving, not several.
- The latency window only holds samples since the last change, so each step is judged on fresh data.
- Every change is logged at INFO with its reason and kept in ``history``.

Latency units are the caller's choice. The upload and download limits pass latency divided by a
size-dependent per-file target, with ``target=1.0``. The AVU rate passes seconds.

Cross-process iRODS connection budget (:class:`ConnectionBudget`, :class:`SessionPool`):

- ``work/locks/irods-conn-NN.lock`` are N lock files. N is 32 by default, or
  ``NEON_DUCKLAKE_IRODS_CONNECTIONS``, and must be the same in every process.
- One slot is one iRODS connection. It is held with ``fcntl.flock(LOCK_EX | LOCK_NB)`` on one of those
  files. The kernel releases it when the process dies, so a ``kill -9`` never leaks a slot.
- A :class:`SessionPool` binds every cloned worker session to one slot for the session's whole life.
  Idle sessions keep their iRODS connection open, so they keep their slot.
- A parallel (multi-stream) put takes a dedicated session holding ``streams + 1`` slots, and that
  session is closed right after the put.
- ``mirror.push_tree`` and ``avu.bulk.apply_scope`` use it. Their worker connections across all
  processes therefore stay at or below N. Each process also has its one main pooled session, used for
  collection creation, which is outside the budget.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import logging
import math
import os
import random
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Self

from . import env

log = logging.getLogger("neon_ducklake.tuning")

DEFAULT_IRODS_CONNECTIONS = 32
BUDGET_ENV = "NEON_DUCKLAKE_IRODS_CONNECTIONS"

CONN_LIMIT = "SYS_MAX_CONNECT_COUNT_EXCEEDED"
RETRYABLE_IRODS = ("LOCKED_DATA_OBJECT_ACCESS", "SYS_REPLICA_INACCESSIBLE")
_TIMEOUT_MARKERS = ("TimeoutError", "timed out", "Timeout", "TIMEDOUT")
_RESET_MARKERS = ("NetworkException", "Connection reset", "Broken pipe", "ConnectionResetError",
                  "BrokenPipeError", "ConnectionAbortedError", "SYS_HEADER_READ_LEN_ERR", "SYS_SOCK_READ_ERR",
                  "SYS_SOCK_CONNECT_ERR", "SYS_SOCK_OPEN_ERR")


def p95(values: Any) -> float | None:
    """Nearest-rank p95 (the same definition as ``avu.bulk.LatencyMonitor``)."""
    s = sorted(values)
    if not s:
        return None
    return s[min(len(s) - 1, max(0, round(0.95 * len(s)) - 1))]


def irods_error_kind(exc: BaseException) -> str | None:
    """Classify an iRODS/network failure as a load signal.

    Returns one of ``conn_limit``, ``retryable``, ``timeout`` or ``reset``. Anything else, such as a
    semantic catalog error, returns None and is not a reason to back off.
    """
    s = f"{type(exc).__name__} {exc!r} {exc}"
    if CONN_LIMIT in s:
        return "conn_limit"
    if any(m in s for m in RETRYABLE_IRODS):
        return "retryable"
    if isinstance(exc, TimeoutError) or any(m in s for m in _TIMEOUT_MARKERS):
        return "timeout"
    if isinstance(exc, (ConnectionError, EOFError)) or any(m in s for m in _RESET_MARKERS):
        return "reset"
    return None


# ---------------------------------------------------------------- AIMD policy

@dataclass
class Change:
    at: float
    old: float
    new: float
    reason: str


CAP_ERROR_KINDS = frozenset({"reset", "conn_limit", "timeout"})


class AIMD:
    """Thread-safe additive-increase / multiplicative-decrease controller (see module docstring)."""

    unit = "value"

    def __init__(self, name: str, *, start: float, lo: float, hi: float, target: float, ceiling: float,
                 step: float = 1.0, factor: float = 0.5, every: int = 16, window: int = 64, min_samples: int = 8,
                 cooldown_s: float = 5.0, integer: bool = False, cap_hold_s: float = 900.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if lo > hi:
            raise ValueError(f"{name}: lo {lo} > hi {hi}")
        if not 0.0 < factor < 1.0:
            raise ValueError(f"{name}: factor must be in (0, 1)")
        if target > ceiling:
            raise ValueError(f"{name}: target {target} > ceiling {ceiling}")
        if step <= 0 or every < 1 or window < 1 or min_samples < 1:
            raise ValueError(f"{name}: step, every, window and min_samples must be positive")
        self.name, self.integer, self._clock = name, integer, clock
        self.lo, self.hi = self._round(lo), self._round(hi)
        self.target, self.ceiling, self.step, self.factor = target, ceiling, step, factor
        self.every, self.min_samples, self.cooldown_s = every, min(min_samples, window), cooldown_s
        self._lock = threading.RLock()
        self.value = self._clamp(start)
        self.start = self.value
        self.min_seen = self.max_seen = self.value
        self.successes = self.errors = self.increases = self.decreases = 0
        self.errors_by_kind: Counter[str] = Counter()
        self._samples: deque[float] = deque(maxlen=window)
        self._since_change = 0
        self._last_decrease = -math.inf
        self._binding = False
        self.history: deque[Change] = deque(maxlen=200)
        # Learned ceiling (like TCP ssthresh): a connection-type error at value V caps growth at V - step for
        # ``cap_hold_s``, so the controller settles just below a server-side connection limit instead of
        # sawtoothing into it (observed on CyVerse: resets at ~10 concurrent puts).
        self.cap_hold_s = cap_hold_s
        self.cap: float | None = None
        self._cap_until = -math.inf

    # ---------------------------------------------------------- helpers
    def _round(self, v: float) -> float:
        return float(int(v)) if self.integer else float(v)

    def _clamp(self, v: float) -> float:
        return min(self.hi, max(self.lo, self._round(v)))

    def _fmt(self, v: float) -> str:
        return str(int(v)) if self.integer else f"{v:.3g}"

    def _changed(self, old: float, new: float) -> None:
        """Hook for gates (wake waiters when the value grows). Called with the lock held."""

    def _can_increase(self) -> bool:
        return self._binding

    def mark_binding(self) -> None:
        """Record that callers are limited by this control (it is worth growing)."""
        with self._lock:
            self._binding = True

    def _set(self, new: float, reason: str) -> None:
        old = self.value
        new = self._clamp(new)
        self._samples.clear()
        self._since_change = 0
        self._binding = False
        if new == old:
            return
        self.value = new
        self.min_seen, self.max_seen = min(self.min_seen, new), max(self.max_seen, new)
        if new > old:
            self.increases += 1
        else:
            self.decreases += 1
        self.history.append(Change(self._clock(), old, new, reason))
        log.info("%s: %s %s -> %s (%s)", self.name, self.unit, self._fmt(old), self._fmt(new), reason)
        self._changed(old, new)

    def _decrease(self, reason: str) -> bool:
        now = self._clock()
        if now - self._last_decrease < self.cooldown_s:
            return False
        self._last_decrease = now
        if self.value <= self.lo:
            self._samples.clear()  # already at the floor: judge the next window afresh
            self._since_change = 0
            return False
        self._set(math.floor(self.value * self.factor) if self.integer else self.value * self.factor, reason)
        return True

    # ---------------------------------------------------------- feedback
    def on_success(self, latency: float) -> float:
        """Report one successful call and its latency. Returns the (possibly new) value."""
        with self._lock:
            self.successes += 1
            self._samples.append(max(0.0, float(latency)))
            self._since_change += 1
            n = len(self._samples)
            if n >= self.min_samples:
                p = p95(self._samples)
                assert p is not None
                if p > self.ceiling:
                    self._decrease(f"p95 {p:.3g} > ceiling {self.ceiling:.3g} over {n} calls")
                    return self.value
            if self._since_change >= self.every and self.value < self._effective_hi() and self._can_increase():
                recent = list(self._samples)[-self.every:]  # judged on the latest K only (not on idle-time samples)
                p = p95(recent)
                if p is not None and p < self.target:
                    self._set(self.value + self.step, f"p95 {p:.3g} < target {self.target:.3g} over "
                                                      f"{self._since_change} successes")
            return self.value

    def on_error(self, kind: str = "error") -> float:
        """Report one failed call (``kind`` is a short label such as ``conn_limit`` or ``timeout``)."""
        with self._lock:
            self.errors += 1
            self.errors_by_kind[kind] += 1
            self._since_change = 0
            if kind in CAP_ERROR_KINDS:
                self._learn_cap(kind)
            self._decrease(f"error: {kind}")
            return self.value

    def _effective_hi(self) -> float:
        if self.cap is not None and self._clock() < self._cap_until:
            return min(self.hi, self.cap)
        return self.hi

    def _learn_cap(self, kind: str) -> None:
        """Cap growth just below the value at which the server dropped us (held for ``cap_hold_s``)."""
        new_cap = max(self.lo, self._round(self.value - self.step))
        if self.cap is None or self._clock() >= self._cap_until or new_cap < self.cap:
            self.cap = new_cap
            log.info("%s: learned ceiling %s after %s at %s (held %.0fs)", self.name, self._fmt(new_cap), kind,
                     self._fmt(self.value), self.cap_hold_s)
        self._cap_until = self._clock() + self.cap_hold_s

    def p95(self) -> float | None:
        with self._lock:
            return p95(self._samples)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            p = p95(self._samples)
            return {"name": self.name, "start": self.start, "current": self.value, "min": self.min_seen,
                    "max": self.max_seen, "lo": self.lo, "hi": self.hi, "increases": self.increases,
                    "decreases": self.decreases, "successes": self.successes, "errors": self.errors,
                    "cap": self.cap if self._clock() < self._cap_until else None,
                    "errors_by_kind": dict(self.errors_by_kind), "p95": None if p is None else round(p, 4),
                    "changes": [asdict(c) for c in list(self.history)[-20:]]}


# ---------------------------------------------------------------- gates

class AdaptiveLimit(AIMD):
    """AIMD concurrency limit with a blocking thread gate: ``with limit: ...`` waits while in-flight >= limit.

    Lowering the limit never interrupts calls in flight; new callers wait until enough of them finish.
    """

    unit = "concurrency"

    def __init__(self, name: str = "limit", *, start: int, lo: int, hi: int, target: float = 1.0,
                 ceiling: float = 3.0, **kw: Any) -> None:
        super().__init__(name, start=start, lo=lo, hi=hi, target=target, ceiling=ceiling, integer=True, **kw)
        self.in_flight = 0
        self.peak = 0
        self._cv = threading.Condition(self._lock)

    @property
    def limit(self) -> int:
        return int(self.value)

    def acquire(self, timeout: float | None = None) -> bool:
        with self._cv:
            if self.in_flight >= self.limit:
                self._binding = True
            if not self._cv.wait_for(lambda: self.in_flight < self.limit, timeout):
                return False
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
            if self.in_flight >= self.limit:
                self._binding = True
            return True

    def release(self) -> None:
        with self._cv:
            self.in_flight -= 1
            self._cv.notify()

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    def _changed(self, old: float, new: float) -> None:
        if new > old:
            self._cv.notify_all()

    def snapshot(self) -> dict[str, Any]:
        out = super().snapshot()
        out["peak_in_flight"] = self.peak
        return out


class AsyncAdaptiveLimit(AIMD):
    """AIMD concurrency limit with an asyncio gate (``async with limit: ...``). Use from one event loop.

    Waiters are served FIFO. A released slot is handed straight to the next waiter, so a large backlog
    costs O(1) per release instead of a thundering herd.
    """

    unit = "concurrency"

    def __init__(self, name: str = "limit", *, start: int, lo: int, hi: int, target: float = 1.0,
                 ceiling: float = 3.0, **kw: Any) -> None:
        super().__init__(name, start=start, lo=lo, hi=hi, target=target, ceiling=ceiling, integer=True, **kw)
        self.in_flight = 0
        self.peak = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def limit(self) -> int:
        return int(self.value)

    def _note_in(self) -> None:
        self.peak = max(self.peak, self.in_flight)
        if self.in_flight >= self.limit:
            self._binding = True

    def _wake(self) -> None:
        while self._waiters and self.in_flight < self.limit:
            fut = self._waiters.popleft()
            if fut.done():
                continue
            self.in_flight += 1  # the slot is reserved for this waiter
            self._note_in()
            fut.set_result(None)

    async def __aenter__(self) -> Self:
        if self.in_flight < self.limit and not self._waiters:
            self.in_flight += 1
            self._note_in()
            return self
        self._binding = True
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():  # woken and cancelled at once: give the slot back
                self.in_flight -= 1
                self._wake()
            raise
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.in_flight -= 1
        self._wake()

    def _changed(self, old: float, new: float) -> None:
        if new > old:
            self._wake()

    def snapshot(self) -> dict[str, Any]:
        out = super().snapshot()
        out["peak_in_flight"] = self.peak
        return out


class AdaptiveRate(AIMD):
    """AIMD calls-per-second token bucket. ``acquire()`` blocks (``sleep``) until a token is available."""

    unit = "rate"

    def __init__(self, name: str = "rate", *, start: float, lo: float, hi: float, target: float, ceiling: float,
                 burst: float = 1.0, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep, **kw: Any) -> None:
        if start <= 0 or lo <= 0:
            raise ValueError("rate must be > 0")
        super().__init__(name, start=start, lo=lo, hi=hi, target=target, ceiling=ceiling, clock=clock, **kw)
        self.burst = max(1.0, float(burst))
        self._sleep = sleep
        self._tokens = self.burst
        self._last = clock()
        self.waited_s = 0.0

    @property
    def rate(self) -> float:
        return self.value

    def acquire(self) -> float:
        """Take one token, sleeping as needed; returns the seconds waited."""
        with self._lock:
            now = self._clock()
            self._tokens = min(self.burst, self._tokens + (now - self._last) * self.value)
            self._last = now
            self._tokens -= 1.0
            wait_s = 0.0 if self._tokens >= 0 else -self._tokens / self.value
            if wait_s > 0:
                self._binding = True
                self.waited_s += wait_s
        if wait_s > 0:
            self._sleep(wait_s)
        return wait_s


# ---------------------------------------------------------------- cross-process connection budget

class BudgetTimeout(RuntimeError):
    """No iRODS connection slot became free in time."""


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        n = int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name}={raw!r} is not an integer") from exc
    if n < 1:
        raise SystemExit(f"{name} must be >= 1 (got {n})")
    return n


class ConnectionBudget:
    """Counting semaphore across processes: N ``flock``-ed files ``<lock_dir>/<name>-NN.lock``."""

    def __init__(self, total: int = DEFAULT_IRODS_CONNECTIONS, *, name: str = "irods-conn",
                 lock_dir: Path | None = None, poll_s: float = 0.25, timeout_s: float = 3600.0,
                 warn_after_s: float = 60.0, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if total < 1:
            raise ValueError("total must be >= 1")
        self.total, self.name = total, name
        self.lock_dir = Path(lock_dir) if lock_dir is not None else env.LOCK_DIR
        self.poll_s, self.timeout_s, self.warn_after_s = poll_s, timeout_s, warn_after_s
        self._sleep, self._clock = sleep, clock
        self._held: dict[int, Any] = {}
        self._lock = threading.Lock()
        self.acquired = 0
        self.waits = 0
        self.peak_held = 0
        self._last_warn = -math.inf

    def path(self, i: int) -> Path:
        return self.lock_dir / f"{self.name}-{i:02d}.lock"

    def try_acquire(self) -> int | None:
        """One free slot index, or None when all N are held (by any process, including this one)."""
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        first = random.randrange(self.total)
        for k in range(self.total):
            i = (first + k) % self.total
            with self._lock:
                if i in self._held:
                    continue
            fh = self.path(i).open("a+")
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fh.close()
                continue
            with contextlib.suppress(OSError):
                fh.seek(0)
                fh.truncate()
                fh.write(f"{os.getpid()} {threading.get_ident()}\n")
                fh.flush()
            with self._lock:
                self._held[i] = fh
                self.acquired += 1
                self.peak_held = max(self.peak_held, len(self._held))
            return i
        return None

    def now(self) -> float:
        return self._clock()

    def wait_tick(self, started: float, what: str = "an iRODS connection slot") -> None:
        """Sleep one poll interval while waiting since ``started`` (a :meth:`now` value).

        Warns every ``warn_after_s`` and raises BudgetTimeout past ``timeout_s``.
        """
        now = self._clock()
        waited = now - started
        if waited > self.timeout_s:
            raise BudgetTimeout(f"no {what} free after {waited:.0f}s ({self.total} slots in {self.lock_dir})")
        if waited >= self.warn_after_s and now - self._last_warn >= self.warn_after_s:
            self._last_warn = now
            log.warning("waiting %.0fs for %s: %d/%d slots in use across processes (%d here)", waited, what,
                        self.in_use(), self.total, self.held())
        with self._lock:
            self.waits += 1
        self._sleep(self.poll_s)

    def acquire(self) -> int:
        started = self._clock()
        while True:
            i = self.try_acquire()
            if i is not None:
                return i
            self.wait_tick(started)

    def release(self, i: int) -> None:
        with self._lock:
            fh = self._held.pop(i, None)
        if fh is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()

    def release_all(self) -> None:
        for i in list(self._held):
            self.release(i)

    def held(self) -> int:
        with self._lock:
            return len(self._held)

    def in_use(self) -> int:
        """Slots held by any process right now (probes the free-looking ones without keeping them)."""
        busy = 0
        for i in range(self.total):
            with self._lock:
                if i in self._held:
                    busy += 1
                    continue
            p = self.path(i)
            if not p.exists():
                continue
            with p.open("a+") as fh:
                try:
                    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    busy += 1
                else:
                    fcntl.flock(fh, fcntl.LOCK_UN)
        return busy

    def stats(self) -> dict[str, Any]:
        return {"total": self.total, "acquired": self.acquired, "waits": self.waits, "peak_held": self.peak_held}


def irods_budget() -> ConnectionBudget:
    """The shared iRODS connection budget: ``env.LOCK_DIR`` (resolved now), N from the environment."""
    return ConnectionBudget(_env_int(BUDGET_ENV, DEFAULT_IRODS_CONNECTIONS), lock_dir=env.LOCK_DIR)


class SessionPool:
    """Cloned worker sessions of one root session; each clone holds one budget slot for its whole life.

    ``lease()`` hands out an idle clone or makes a new one once a slot is free. A clone that failed with
    a connection reset or timeout is discarded (closed, slot released) instead of being reused.
    ``trim(keep)`` closes idle clones beyond ``keep`` (call it after a concurrency decrease), and
    ``close()`` closes them all.
    """

    def __init__(self, root: Any, *, budget: ConnectionBudget | None = None) -> None:
        self.root, self.budget = root, budget
        self._lock = threading.Lock()
        self._idle: list[Any] = []
        self._slots: dict[int, list[int]] = {}
        self._leased = 0
        self.created = self.closed = self.discarded = self.peak = 0

    # ---------------------------------------------------------- internals
    def _clone(self) -> Any:
        return self.root.clone() if hasattr(self.root, "clone") else self.root

    def _register(self, s: Any, slots: list[int]) -> None:
        with self._lock:
            self._slots[id(s)] = slots
            self.created += 1

    def _close(self, s: Any) -> None:
        with self._lock:
            slots = self._slots.pop(id(s), [])
            self.closed += 1
        if s is not self.root and hasattr(s, "cleanup"):
            try:
                s.cleanup()
            except Exception as exc:  # noqa: BLE001 - a broken session must still release its slot
                log.debug("session cleanup failed: %s", exc)
        if self.budget is not None:
            for i in slots:
                self.budget.release(i)

    def _evict_idle(self) -> bool:
        with self._lock:
            s = self._idle.pop() if self._idle else None
        if s is None:
            return False
        self._close(s)
        return True

    def _new(self, slots: list[int]) -> Any:
        try:
            s = self._clone()
        except BaseException:
            if self.budget is not None:
                for i in slots:
                    self.budget.release(i)
            raise
        self._register(s, slots)
        return s

    def _get(self) -> Any:
        budget = self.budget
        started = budget.now() if budget is not None else 0.0
        while True:
            with self._lock:
                if self._idle:
                    self._leased += 1
                    return self._idle.pop()
            if budget is None:
                s = self._new([])
            else:
                slot = budget.try_acquire()
                if slot is None:
                    budget.wait_tick(started)
                    continue
                s = self._new([slot])
            with self._lock:
                self._leased += 1
                self.peak = max(self.peak, self._leased + len(self._idle))
            return s

    # ---------------------------------------------------------- public
    @contextlib.contextmanager
    def lease(self) -> Iterator[Any]:
        s = self._get()
        broken = False
        try:
            yield s
        except BaseException as exc:
            broken = irods_error_kind(exc) in ("reset", "timeout", "conn_limit")
            raise
        finally:
            with self._lock:
                self._leased -= 1
                if not broken:
                    self._idle.append(s)
            if broken:
                self.discarded += 1
                self._close(s)

    @contextlib.contextmanager
    def dedicated(self, slots: int) -> Iterator[Any]:
        """A fresh clone holding ``slots`` budget slots (for a multi-stream put); closed on exit."""
        got: list[int] = []
        budget = self.budget
        started = budget.now() if budget is not None else 0.0
        s = None
        try:
            # All-or-nothing: never hold a partial set while waiting. Two multi-slot puts that each held
            # part of the budget and waited for the rest would deadlock (hold-and-wait).
            while budget is not None and len(got) < slots:
                i = budget.try_acquire()
                if i is not None:
                    got.append(i)
                    continue
                if self._evict_idle():  # our own idle clones give way first
                    continue
                for j in got:
                    budget.release(j)
                got = []
                budget.wait_tick(started, f"{slots} iRODS connection slots")
            s, got = self._new(got), []
            yield s
        finally:
            if s is not None:
                self._close(s)
            if self.budget is not None:
                for i in got:
                    self.budget.release(i)

    def trim(self, keep: int) -> int:
        """Close idle clones until at most ``keep`` sessions (idle + leased) remain. Returns the number closed."""
        n = 0
        while True:
            with self._lock:
                if not self._idle or len(self._idle) + self._leased <= keep:
                    return n
                s = self._idle.pop()
            self._close(s)
            n += 1

    def close(self) -> None:
        while self._evict_idle():
            pass
        if self._leased:
            log.warning("session pool closed with %d session(s) still leased", self._leased)

    def stats(self) -> dict[str, Any]:
        out = {"sessions_created": self.created, "sessions_closed": self.closed,
               "sessions_discarded": self.discarded, "sessions_peak": self.peak}
        if self.budget is not None:
            out["budget"] = self.budget.stats()
        return out
