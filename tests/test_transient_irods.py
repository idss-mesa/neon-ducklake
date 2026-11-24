"""A refused iRODS handshake (CyVerse's per-user connection limit: NetworkException "Could not receive server
response") is transient: the push listing retries it, a stream stage returns RC_RETRY instead of failing, and the
orchestrator's journal push retries it in place."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from irods.exception import NetworkException

from neon_ducklake import env, mirror, run, state
from neon_ducklake.mirror import push_tree
from neon_ducklake.site import chunks, stream
from neon_ducklake.tuning import transient_irods

from .test_avu_fakes import SYS_MAX_CONNECT_COUNT_EXCEEDED, FakeIrods, FakeSession
from .test_site_common import site_env  # noqa: F401 - fixture used by pipe
from .test_stream_stages import pipe  # noqa: F401 - fixture

REFUSED = "Could not receive server response"


@pytest.fixture(autouse=True)
def _hermetic_locks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(env, "LOCK_DIR", tmp_path / "locks")


def test_transient_classification() -> None:
    assert transient_irods(NetworkException(REFUSED))
    assert transient_irods(SYS_MAX_CONNECT_COUNT_EXCEEDED("SYS_MAX_CONNECT_COUNT_EXCEEDED"))
    assert transient_irods(ConnectionResetError("reset by peer")) and transient_irods(TimeoutError("timed out"))
    assert not transient_irods(ValueError("bad column")) and not transient_irods(KeyError("x"))


def _items(tmp_path: Path) -> list[tuple[Path, str, None]]:
    out = []
    for i, rel in enumerate(("data/main/t1/a.parquet", "data/main/t2/b.parquet")):
        p = tmp_path / "lake" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x" * (i + 1))
        out.append((p, f"/zone/home/u/proj/{rel}", None))
    return out


def test_push_listing_retries_a_refused_handshake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = mirror.remote_replicas
    fails = [NetworkException(REFUSED), NetworkException(REFUSED)]

    def flaky(session: Any, coll: str, name: str | None = None) -> Any:
        if name is None and coll.endswith("/t1") and fails:  # one collection, so the retries are sequential
            raise fails.pop(0)
        return real(session, coll, name)

    monkeypatch.setattr(mirror, "remote_replicas", flaky)
    slept: list[float] = []
    con = state.connect(tmp_path / "state.sqlite")
    stats = push_tree(_items(tmp_path), threads=1, session=FakeSession(FakeIrods()), con=con, sleep=slept.append)
    assert stats["uploaded"] == 2 and stats["errors"] == 0 and stats["net_retries"] == 2
    assert slept == [mirror.NET_WAIT_S, mirror.NET_WAIT_S * 2]


def test_push_listing_gives_up_after_the_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def refused(session: Any, coll: str, name: str | None = None) -> Any:
        raise NetworkException(REFUSED)

    monkeypatch.setattr(mirror, "remote_replicas", refused)
    slept: list[float] = []
    con = state.connect(tmp_path / "state.sqlite")
    with pytest.raises(NetworkException):
        push_tree(_items(tmp_path)[:1], threads=1, session=FakeSession(FakeIrods()), con=con, sleep=slept.append)
    assert len(slept) == mirror.MAX_CONN_RETRIES


def test_push_listing_does_not_retry_other_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(session: Any, coll: str, name: str | None = None) -> Any:
        raise ValueError("bad query")

    monkeypatch.setattr(mirror, "remote_replicas", broken)
    slept: list[float] = []
    con = state.connect(tmp_path / "state.sqlite")
    with pytest.raises(ValueError):
        push_tree(_items(tmp_path)[:1], threads=1, session=FakeSession(FakeIrods()), con=con, sleep=slept.append)
    assert slept == []


def test_a_stage_that_raises_a_refused_connection_retries_later(pipe: SimpleNamespace) -> None:  # noqa: F811
    ch = pipe.plan[0]

    def refused(c: chunks.Chunk, con: Any) -> tuple[int, dict[str, Any]]:
        raise NetworkException(REFUSED)

    assert stream._run_stage(ch.site, ch.chunk_id, "download", refused) == stream.RC_RETRY
    got = chunks.get_chunk(ch.site, ch.chunk_id)
    assert got is not None and got.status == "planned"  # not failed: the orchestrator dispatches it again later
    assert "transient" in (got.error or "") and REFUSED in (got.error or "")

    def bug(c: chunks.Chunk, con: Any) -> tuple[int, dict[str, Any]]:
        raise ValueError("a real bug")

    assert stream._run_stage(ch.site, ch.chunk_id, "download", bug) == stream.RC_FAILED
    got = chunks.get_chunk(ch.site, ch.chunk_id)
    assert got is not None and got.status == chunks.FAILED


def test_journal_push_retries_transient_failures() -> None:
    calls: list[str] = []
    slept: list[float] = []

    def push(site: str) -> dict[str, Any]:
        calls.append(site)
        if len(calls) < 3:
            raise NetworkException(REFUSED)
        return {}

    hook = run.make_journal_pusher(push=push, sleep=slept.append)
    assert hook("A", "delete", "c1") is None and len(calls) == 3
    assert slept == [run.JOURNAL_PUSH_WAIT_S, run.JOURNAL_PUSH_WAIT_S * 2]

    def always(site: str) -> dict[str, Any]:
        raise NetworkException(REFUSED)

    slept.clear()
    err = run.make_journal_pusher(push=always, sleep=slept.append)("A", "delete", "c1")
    assert err is not None and REFUSED in err and len(slept) == run.JOURNAL_PUSH_TRIES - 1


def test_journal_push_does_not_retry_a_conflict() -> None:
    slept: list[float] = []

    def conflict(site: str) -> dict[str, Any]:
        raise RuntimeError("JournalConflict: the Data Store holds a journal this host has not seen")

    err = run.make_journal_pusher(push=conflict, sleep=slept.append)("A", "delete", "c1")
    assert err is not None and "JournalConflict" in err and slept == []
