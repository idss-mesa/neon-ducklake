"""Reading a site claim while its heartbeat rewrites it (review D1): holds_site, claim_is_current, the journal
push, site_claims / pick_next and a worker's own site, against a fake Data Store whose ``open(path, 'w')``
truncates the object at once and keeps it "being written" until the handle closes, as iRODS does.

A reader then either gets ``LOCKED_DATA_OBJECT_ACCESS`` (iRODS 4.2.9+ logical locking: ``reader_sees='locked'``)
or the truncated, empty content (``reader_sees='empty'``). Before the fix each of these reads failed the stage
(``claim lost``) or raised.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from irods.exception import LOCKED_DATA_OBJECT_ACCESS, NetworkException

from neon_ducklake import coord, env, journal_sync, state, worker

from .test_coord_fakes import ROOT, DSDataObjects, DSIrods, DSSession, _Handle, hermetic_ds

SITE = "SRER"
CLAIM = f"{ROOT}/sites/{SITE}/.claim"
CURRENT = f"{ROOT}/sites/{SITE}/.mesa-journal/CURRENT"


class RWIrods(DSIrods):
    """DSIrods whose objects can be "being written": between ``open('w')`` (which truncates) and ``close``."""

    def __init__(self) -> None:
        super().__init__()
        self.writing: set[str] = set()
        self.reader_sees = "locked"  # or "empty"
        self.hold: threading.Event | None = None  # when set: a claim write's close waits for this event
        self.write_delay = 0.0  # seconds a claim write stays open
        self.write_started = threading.Event()
        self.blocked_reads: Counter[str] = Counter()  # reads that hit an object being written
        self.on_blocked_read: Any = None  # called (path) on the first blocked read


class _RWHandle(_Handle):
    def close(self) -> None:
        if self.closed:
            return
        store: RWIrods = self.store  # type: ignore[assignment]
        if self.path.endswith(coord.CLAIM_NAME):  # only claim writes are slowed down or held
            if store.hold is not None:
                store.hold.wait(10)
            if store.write_delay:
                time.sleep(store.write_delay)
        try:
            super().close()
        finally:
            with store.lock:
                store.writing.discard(self.path)


class RWObjects(DSDataObjects):
    store: RWIrods

    def open(self, path: str, mode: str = "r", create: bool = True, **opts: Any) -> _Handle:
        if mode in ("w", "w+"):
            super().open(path, mode, create=create, **opts).close()  # iRODS truncates at open
            with self.store.lock:
                self.store.writing.add(path)
            if path.endswith(coord.CLAIM_NAME):
                self.store.write_started.set()
            return _RWHandle(self.store, path, b"")
        if mode == "r":
            with self.store.lock:
                busy = path in self.store.writing
                first = busy and not self.store.blocked_reads[path]
                if busy:
                    self.store.blocked_reads[path] += 1
            if first and self.store.on_blocked_read:
                self.store.on_blocked_read(path)
            if busy and self.store.reader_sees == "locked":
                raise LOCKED_DATA_OBJECT_ACCESS(f"{path} is locked by a writer")
        return super().open(path, mode, create=create, **opts)


class RWSession(DSSession):
    def __init__(self, store: RWIrods) -> None:
        super().__init__(store)
        self.data_objects = RWObjects(store)

    def clone(self) -> RWSession:
        self.store.calls["clone"] += 1
        return RWSession(self.store)  # type: ignore[arg-type]


@pytest.fixture
def rw(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RWIrods:
    hermetic_ds(tmp_path, monkeypatch)  # paths, journal and locks in tmp_path
    store = RWIrods()
    store.add_coll(ROOT)
    monkeypatch.setattr(coord, "SETTLE_S", 0.0)
    monkeypatch.setattr(coord, "CLAIM_READ_PAUSE_S", 0.01)
    monkeypatch.setattr(env, "machine_id", lambda: "machine-1")
    monkeypatch.setattr(env, "boot_id", lambda: "boot-1")
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    monkeypatch.setenv(coord.SITE_CLAIMS_ENV, "on")  # a worker / `run` with claims: holds_site is strict
    return store


def claim(rw: RWIrods, site: str = SITE, vm: str = "vm-a") -> coord.Claim:
    res = coord.try_claim(coord.site_claim_path(site), "site", site, session=RWSession(rw), settle_s=0, vm=vm)
    assert res.acquired and res.claim is not None
    return res.claim


def renew_blocked(rw: RWIrods, c: coord.Claim, *, release_after_s: float = 0.2) -> tuple[threading.Thread, list]:
    """Start a real ``coord.renew`` (the heartbeat's call) whose write stays open until a reader has hit it,
    then ``release_after_s`` longer. Returns once the write is open (the claim object is empty and locked)."""
    rw.hold = threading.Event()
    rw.write_started.clear()
    rw.on_blocked_read = lambda _path: threading.Timer(release_after_s, rw.hold.set).start()
    out: list = []

    def go() -> None:
        try:
            out.append(coord.renew(c, session=RWSession(rw)))
        except Exception as exc:  # noqa: BLE001 - reported to the test
            out.append(exc)

    t = threading.Thread(target=go, daemon=True)
    t.start()
    assert rw.write_started.wait(5)
    return t, out


# ---------------------------------------------------------------- holds_site / claim_is_current

@pytest.mark.parametrize("reader_sees", ["locked", "empty"])
def test_holds_site_waits_out_a_renew_in_progress(rw: RWIrods, reader_sees: str) -> None:
    """The reviewer's scenario: a stage child checks the claim right when the heartbeat rewrites it. Before the
    fix: a lock error re-raised by read_bytes (the stage crashed, rc 2) or an empty claim (``claim lost``)."""
    c = claim(rw)
    rw.reader_sees = reader_sees
    t, out = renew_blocked(rw, c)
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a") is True
    t.join(5)
    assert out == [True]  # the renew itself went through
    assert rw.blocked_reads[CLAIM] >= 1  # holds_site really read the claim mid-write


def test_holds_site_waits_out_a_renew_in_progress_without_a_claim_in_force(rw, monkeypatch) -> None:
    """`run --no-claims` / an operator: another VM's claim being renewed stays a live claim (no write), and our
    own claim being renewed never reads as "somebody else's"."""
    monkeypatch.setenv(coord.SITE_CLAIMS_ENV, "off")
    rw.reader_sees = "empty"
    theirs = claim(rw, vm="vm-b")
    t, _ = renew_blocked(rw, theirs)
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a") is False  # a live claim of vm-b
    t.join(5)


def test_claim_is_current_during_a_renew_keeps_the_claim(rw: RWIrods) -> None:
    """The worker re-checks its claim before the journal push and ``.done`` while its heartbeat thread may be
    renewing it; an empty read used to set ``claim.lost`` (no push, no .done: the site failed)."""
    c = claim(rw)
    rw.reader_sees = "empty"
    t, out = renew_blocked(rw, c)
    assert coord.claim_is_current(c, session=RWSession(rw)) is True
    assert not c.lost.is_set()
    t.join(5)
    assert out == [True]


def test_a_claim_unreadable_for_the_whole_window_raises_instead_of_false(rw: RWIrods) -> None:
    """False means a readable answer only; a claim still locked after the window is unknown (transient)."""
    c = claim(rw)
    rw.hold = threading.Event()  # never released within the window
    t = threading.Thread(target=lambda: coord.renew(c, session=RWSession(rw)), daemon=True)
    t.start()
    assert rw.write_started.wait(5)
    try:
        with pytest.raises(coord.ClaimUnreadable) as err:
            coord.holds_site(SITE, session=RWSession(rw), vm="vm-a", retry_s=0.2)
        assert coord.transient_error(err.value)
        assert isinstance(err.value.__cause__, LOCKED_DATA_OBJECT_ACCESS)
    finally:
        rw.hold.set()
        t.join(5)
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a") is True


def test_a_young_empty_claim_that_nobody_finishes_is_unknown_not_lost(rw: RWIrods) -> None:
    rw.add_obj(CLAIM, content=b"")  # created, never written (yet): another VM's claim in its create window
    with pytest.raises(coord.ClaimUnreadable, match="empty or unparseable"):
        coord.holds_site(SITE, session=RWSession(rw), vm="vm-a", retry_s=0.1)
    rw.mtimes[CLAIM] = datetime.now(UTC) - timedelta(seconds=coord.EMPTY_GRACE_S + 5)  # abandoned: nobody's
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a") is False
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a", required=False) is True


def test_holds_site_still_refuses_a_readable_foreign_or_stale_claim(rw: RWIrods) -> None:
    c = claim(rw, vm="vm-b")
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a") is False
    doc = json.loads(rw.content(CLAIM)) | {"expiresAt": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
                                           "vm": "vm-a"}
    rw.set_content(CLAIM, json.dumps(doc).encode())
    assert coord.holds_site(SITE, session=RWSession(rw), vm="vm-a") is False  # ours, but stale
    assert c.token == doc["token"]


def test_renew_rewrites_a_claim_its_failed_write_left_truncated(rw: RWIrods, monkeypatch) -> None:
    c = claim(rw)
    real = coord.overwrite
    fails = [NetworkException("connection reset while writing")]

    def flaky(session, path, data, *, create=False):
        if fails:
            with session.data_objects.open(path, "w"):  # truncated, then the write dies
                pass
            raise fails.pop()
        real(session, path, data, create=create)

    monkeypatch.setattr(coord, "overwrite", flaky)
    assert coord.renew(c, session=RWSession(rw)) is True
    assert json.loads(rw.content(CLAIM))["token"] == c.token


def test_heartbeat_thread_and_readers_never_disagree(rw: RWIrods) -> None:
    """A real Heartbeat renewing every 10 ms (each write open 20 ms) while this thread checks the claim (at
    least 40 times, over at least 5 renews): every check says True."""
    c = claim(rw)
    rw.write_delay = 0.02
    results: list[bool] = []
    with coord.Heartbeat([c], interval_s=0.01, session=RWSession(rw)) as hb:
        deadline = time.monotonic() + 10
        while (len(results) < 40 or hb.beats < 5) and time.monotonic() < deadline:
            results.append(coord.holds_site(SITE, session=RWSession(rw), vm="vm-a"))
        assert hb.beats >= 5
    assert all(results) and not c.lost.is_set()
    assert rw.blocked_reads[CLAIM] > 0  # the race was hit


# ---------------------------------------------------------------- the journal push (journal_sync._publish)

def test_journal_push_during_a_claim_renew_publishes(rw: RWIrods, tmp_path: Path) -> None:
    """The run's journal-push thread calls holds_site right before the pointer while the heartbeat renews the
    claim. Before the fix the push raised (a lock error) or refused itself (ClaimLost), failing the stage."""
    c = claim(rw)
    rw.reader_sees = "locked"
    con = state.connect(tmp_path / "a.sqlite")
    con.execute("INSERT INTO site_files(site, product, month, name, dl_status) VALUES (?, 'DP1.00002.001',"
                " '2024-06', 'a.csv', 'ok')", (SITE,))
    t, out = renew_blocked(rw, c)
    res = journal_sync.push_site_journal(SITE, session=RWSession(rw), con=con)
    t.join(5)
    assert out == [True] and rw.blocked_reads[CLAIM] >= 1
    assert json.loads(rw.content(CURRENT))["pushId"] == res["pushId"]


# ---------------------------------------------------------------- site_claims, pick_next and the worker

def test_site_claims_and_pick_next_read_through_another_vms_renew(rw: RWIrods) -> None:
    """Another VM's heartbeat rewrites its claim while this worker picks a site: the listing used to raise the
    lock error (the worker crashed, pick_next had no try/except) or list an empty claim."""
    theirs = claim(rw, site="AAAA", vm="vm-b")
    rw.reader_sees = "locked"
    t, _ = renew_blocked(rw, theirs)
    site, info = worker.pick_next(["AAAA", "BBBB"], skip=set(), session=RWSession(rw), vm="vm-a")
    t.join(5)
    assert site == "BBBB" and info["claimed_elsewhere"] == 1
    assert rw.blocked_reads[coord.site_claim_path("AAAA")] >= 1
    claims = coord.site_claims(session=RWSession(rw))
    assert claims["AAAA"].valid and claims["AAAA"].vm == "vm-b"


def test_an_unreadable_claim_is_listed_as_held_by_somebody(rw: RWIrods, monkeypatch) -> None:
    monkeypatch.setattr(coord, "CLAIM_READ_RETRY_S", 0.05)
    rw.add_obj(coord.site_claim_path("AAAA"), content=b"")  # young and never written within the window
    claims = coord.site_claims(session=RWSession(rw))
    assert not claims["AAAA"].valid and not claims["AAAA"].is_stale() and "unreadable" in claims["AAAA"].info
    site, _ = worker.pick_next(["AAAA", "BBBB"], skip=set(), session=RWSession(rw), vm="vm-a")
    assert site == "BBBB"


def test_pick_next_retries_the_pass_on_a_transient_error(rw: RWIrods, monkeypatch) -> None:
    real = coord.done_sites
    errors = [NetworkException("connection reset"), LOCKED_DATA_OBJECT_ACCESS("x")]
    seen: list[int] = []

    def flaky(*, session=None):
        seen.append(1)
        if errors:
            raise errors.pop(0)
        return real(session=session)

    monkeypatch.setattr(coord, "done_sites", flaky)
    site, _ = worker.pick_next(["AAAA"], skip=set(), session=RWSession(rw), vm="vm-a", retry_s=5, pause_s=0.01)
    assert site == "AAAA" and len(seen) == 3
    monkeypatch.setattr(coord, "done_sites", lambda *, session=None: (_ for _ in ()).throw(NetworkException("down")))
    with pytest.raises(NetworkException):  # the window ends: it propagates
        worker.pick_next(["AAAA"], skip=set(), session=RWSession(rw), vm="vm-a", retry_s=0.05, pause_s=0.01)
    monkeypatch.setattr(coord, "done_sites", lambda *, session=None: (_ for _ in ()).throw(TypeError("bug")))
    t0 = time.monotonic()
    with pytest.raises(TypeError):  # a programming error is not retried
        worker.pick_next(["AAAA"], skip=set(), session=RWSession(rw), vm="vm-a", retry_s=30, pause_s=1)
    assert time.monotonic() - t0 < 1


@pytest.fixture
def worker_env(rw: RWIrods, monkeypatch: pytest.MonkeyPatch) -> RWIrods:
    monkeypatch.setattr(worker, "_restore_lake", lambda: (lambda site: 0))
    monkeypatch.setattr(worker, "check_curator", lambda: None)
    monkeypatch.setattr(worker, "PICK_PAUSE_S", 0.01)
    for var in ("NEON_DUCKLAKE_HISTORY_MODE", "NEON_DUCKLAKE_CURATION_SYNC", "NEON_DUCKLAKE_CHUNK_BYTES",
                "NEON_DUCKLAKE_WORKSET_BYTES", "NEON_DUCKLAKE_WORKER_HISTORY_MODE", "NEON_DUCKLAKE_MESA_MULTIWRITER"):
        monkeypatch.setenv(var, "placeholder")
        monkeypatch.delenv(var)
    return rw


UNIVERSE = [{"site": SITE, "habitat": "Terrestrial", "bytes": 5}]


def test_a_worker_site_survives_its_own_heartbeat(worker_env: RWIrods) -> None:
    """End to end, one VM: the site claim's heartbeat renews every 10 ms (writes open 20 ms) while the run's
    stages check the claim before their Data Store writes, and the worker re-checks it, pushes the journal and
    writes .done. Before the fix one of those reads failed the site."""
    rw = worker_env
    rw.write_delay = 0.02
    checks: list[bool] = []

    def run_sites(sites, **kw):
        con = state.connect()
        con.execute("INSERT OR REPLACE INTO site_files(site, product, month, name, dl_status) VALUES (?,?,?,?,?)",
                    (sites[0], "DP1.00002.001", "2024-06", "f.csv", "ok"))
        con.close()
        for _ in range(30):  # the stages' claim checks (upload_chunk, cleanup, the journal pushes)
            checks.append(coord.holds_site(sites[0], session=RWSession(rw)))
        return 0 if all(checks) else 1

    rc = worker.main(session=RWSession(rw), run_sites=run_sites, universe=UNIVERSE, bootstrap=False,
                     heartbeat_s=0.01)
    assert rc == 0 and all(checks) and len(checks) == 30
    assert rw.blocked_reads[CLAIM] > 0  # the race was hit
    assert coord.read_done(SITE, session=RWSession(rw))["rc"] == 0
    assert CLAIM not in rw.objs  # released


def test_a_worker_whose_pick_never_settles_stops_with_a_failure(worker_env: RWIrods, monkeypatch) -> None:
    monkeypatch.setenv(worker.PICK_RETRY_ENV, "0.05")
    monkeypatch.setattr(coord, "done_sites",
                        lambda *, session=None: (_ for _ in ()).throw(NetworkException("Data Store down")))
    rc = worker.main(session=RWSession(worker_env), run_sites=lambda sites, **kw: 0, universe=UNIVERSE,
                     bootstrap=False, heartbeat_s=3600)
    assert rc == 1  # reported, not a traceback out of the worker
    assert not [p for p in worker_env.objs if p.endswith("/.claim") and "/sites/" in p]


def test_a_transient_error_while_claiming_retries_the_site_later(worker_env: RWIrods, monkeypatch) -> None:
    real = coord.claim_site
    errors = [NetworkException("connection reset")]

    def flaky(site, **kw):
        if errors:
            raise errors.pop()
        return real(site, **kw)

    monkeypatch.setattr(coord, "claim_site", flaky)
    ran: list[str] = []
    rc = worker.main(session=RWSession(worker_env), run_sites=lambda sites, **kw: ran.append(sites[0]) or 0,
                     universe=UNIVERSE, bootstrap=False, heartbeat_s=3600)
    assert rc == 0 and ran == [SITE]
    assert coord.read_done(SITE, session=RWSession(worker_env)) is not None


# ---------------------------------------------------------------- curation claims (validate.pending_generic)

def test_pending_generic_reads_through_a_curation_claim_renew(rw: RWIrods, monkeypatch) -> None:
    """A product another VM curates must stay pending while that VM's heartbeat rewrites its curation claim;
    an empty read used to count as nobody's claim, and finalize built the product AVUs without its curation."""
    from neon_ducklake.site import curate, validate

    monkeypatch.setenv("NEON_DUCKLAKE_CURATION_SYNC", "on")
    monkeypatch.setattr(coord, "CLAIM_READ_RETRY_S", 0.2)
    for code in ("DP1.10003.001", "DP1.10072.001"):
        card = curate.curation_paths(SITE).card(code)
        card.parent.mkdir(parents=True, exist_ok=True)
        card.write_text(f"# {code}\n")
    res = coord.claim_curation("DP1.10003.001", session=RWSession(rw), settle_s=0, vm="vm-b")
    assert res.acquired
    rw.reader_sees = "empty"
    t, _ = renew_blocked(rw, res.claim)
    assert validate.pending_generic(SITE, session=RWSession(rw)) == ["DP1.10003.001"]
    t.join(5)
    assert rw.blocked_reads[coord.curation_claim_path("DP1.10003.001")] >= 1
    rw.add_obj(coord.curation_claim_path("DP1.10072.001"), content=b"")  # vm-c is creating its claim
    assert validate.pending_generic(SITE, session=RWSession(rw)) == ["DP1.10003.001", "DP1.10072.001"]
