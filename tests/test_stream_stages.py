"""site/stream.py: the per-chunk stages end to end against in-memory iRODS fakes, delete/verify/cleanup safety,
finalize, and the working-set cap of the chunk download."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from neon_ducklake import env, mirror, state
from neon_ducklake.avu import bulk, history
from neon_ducklake.site import chunks, fetch, site_lake, stack, stream, terms

from .test_coord_fakes import DSIrods, DSSession
from .test_site_common import SITE, site_env  # noqa: F401 - site_env is a fixture
from .test_site_stack import IS, OS, _q, build_fixture

S = f"/zone/home/u/proj/sites/{SITE}"


@pytest.fixture
def pipe(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:  # noqa: F811
    """Fixture site on disk (journal: downloaded), fake iRODS, fake NEON download, spool-like history."""
    s = build_fixture(site_env)
    store = DSIrods()  # FakeIrods + downloads and modify times (the site lake is fetched back by restore_lake)
    session = DSSession(store)

    def fake_fetch_main(step: str, site: str, products: list[str] | None = None, months: list[str] | None = None,
                        threads: int = 8, lock_blocking: bool = False) -> int:  # files already on disk: inventory
        assert step == "download" and lock_blocking and products and months
        con = state.connect(state.DB_PATH)
        try:
            fetch.Fetcher(site=site, bundle=None, con=con, pairs=[]).write_lake("download", products)
        finally:
            con.close()
        return 0

    push = mirror.push_tree
    apply = bulk.apply_scope

    def fake_record(scope: str, *, rows_per_batch: int | None = None, con: Any = None) -> dict[str, Any]:
        c = con or state.connect(state.DB_PATH)
        n = c.execute("UPDATE avu_journal SET status='spooled', batch_id='b-1' WHERE scope=? AND status='applied'",
                      (scope,)).rowcount
        return {"rows_spooled": n}

    monkeypatch.setattr(fetch, "main", fake_fetch_main)
    monkeypatch.setattr(mirror, "push_tree", lambda items, threads=8, record_site=None, **kw: push(
        items, threads=threads, record_site=record_site, session=session, sleep=lambda _s: None))
    monkeypatch.setattr(bulk, "apply_scope", lambda scope, **kw: apply(scope, **{**kw, "session": session,
                                                                                  "sleep": lambda _s: None}))
    monkeypatch.setattr(history, "record_scope", fake_record)
    monkeypatch.setattr(env, "mesa", lambda: SimpleNamespace(session=session))
    # no OLS from the tests: the chunk's taxa mapping is exercised in test_stream_lake.py with a fake resolver
    monkeypatch.setattr(terms, "resolve_chunk_taxa", lambda site, product, months: {"distinct": 0, "pending": 0})
    monkeypatch.setenv(fetch.WORKSET_BYTES_ENV, "1G")
    plan = chunks.plan_chunks(SITE)
    return SimpleNamespace(site=s, store=store, session=session, plan=plan, root=site_env)


def _chunk(pipe: SimpleNamespace, product: str, month: str) -> chunks.Chunk:
    return next(c for c in pipe.plan if c.product == product and month in c.months)


def _local(pipe: SimpleNamespace, product: str, months: list[str]) -> list[Path]:
    rows = pipe.site.con.execute(f"SELECT local_path FROM site_files WHERE product=? AND month IN "
                                 f"({','.join('?' * len(months))})", (product, *months)).fetchall()
    return [pipe.root / r[0] for r in rows]


def test_every_chunk_streams_through_and_the_site_finalizes(pipe: SimpleNamespace,
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    assert [c.chunk_id for c in pipe.plan] == [f"{IS}__2019-05_2024-06", f"{OS}__2023-05_2024-05"]
    for c in pipe.plan:
        assert stream.run_chunk(SITE, c.chunk_id) == 0, chunks.get_chunk(SITE, c.chunk_id).error
        ch = chunks.get_chunk(SITE, c.chunk_id)
        assert (ch.status, ch.done, ch.error) == ("deleted", "deleted", None)
        assert set(ch.info) == set(chunks.STAGES) and ch.info["delete"]["deleted"] > 0
        assert not any(p.exists() for p in _local(pipe, c.product, c.months))  # nothing kept locally
    con = pipe.site.con
    assert con.execute("SELECT count(*) FROM site_files WHERE coalesce(local_evicted, 0) = 0").fetchone()[0] == 0
    # the data lives on in the lake and in iRODS, with its AVUs
    assert _q("SELECT count(*) FROM lake.dp1_10003_001__brd_countdata") == [(3,)]
    assert _q("SELECT count(*) FROM lake.dp1_00002_001__saat_30min") == [(2004,)]
    raw = [p for p in pipe.store.objs if p.startswith(f"{S}/raw/")]
    assert len(raw) == con.execute("SELECT count(*) FROM site_files").fetchone()[0]
    obj = next(p for p in raw if "brd_countdata.2024-05" in p)
    assert ("neon.productCode", OS, "") in pipe.store.avus(obj)
    assert con.execute("SELECT count(*) FROM avu_journal WHERE status IN ('intent', 'applied')").fetchone()[0] == 0
    # a re-run of a finished chunk is a no-op
    assert stream.upload_chunk(SITE, pipe.plan[0].chunk_id) == 0
    # ---- finalize (other modules' steps faked) and cleanup
    from neon_ducklake import verify_site
    from neon_ducklake.site import cards, curate, terms, validate

    calls: list[str] = []
    for mod, name in ((terms, "terms"), (cards, "cards"), (curate, "curate"), (validate, "validate")):
        monkeypatch.setattr(mod, "main", lambda site, *a, _n=name, **k: calls.append(_n) or 0)
    monkeypatch.setattr(validate, "has_proposals", lambda site: True)
    monkeypatch.setattr(verify_site, "main", lambda site, **kw: calls.append("verify") or 0)
    site_dir = site_lake.site_paths(SITE).site_dir
    for rel in ("cards/DP1.10003.001.md", "curation/review.md", "reports/plan.json", "cards/.scratch.tmp"):
        (site_dir / rel).parent.mkdir(parents=True, exist_ok=True)
        (site_dir / rel).write_text(rel)
    assert stream.cleanup_site(SITE, session=pipe.session) == 1  # finalize first
    assert stream.finalize_site(SITE, rate=60.0) == 0
    assert calls == ["terms", "cards", "curate", "validate", "verify"]
    steps = dict(con.execute("SELECT step, status FROM stream_sites WHERE site=?", (SITE,)).fetchall())
    assert all(steps[s] == "ok" for s in stream.FINALIZE_STEPS)
    assert f"{S}/ducklake/neon_test.ducklake" in pipe.store.objs and f"{S}/cards/DP1.10003.001.md" in pipe.store.objs
    lake_obj = next(p for p in pipe.store.objs if p.startswith(f"{S}/ducklake/data/"))
    assert pipe.store.avus(lake_obj)  # lake-file AVUs applied at finalize
    assert ("neon.siteCode", SITE, "") in pipe.store.avus(S)
    assert stream.finalize_site(SITE) == 0 and calls.count("terms") == 1  # ok steps are not re-run
    assert stream.cleanup_site(SITE, session=pipe.session) == 0
    data_dir = site_lake.site_paths(SITE).data_dir
    assert not [p for p in data_dir.rglob("*") if p.is_file()]
    assert not (site_dir / "cards/DP1.10003.001.md").exists() and not (site_dir / "curation/review.md").exists()
    assert (site_dir / "cards/.scratch.tmp").exists()  # never uploaded: kept, reported
    assert (site_dir / "reports/plan.json").exists() and site_lake.site_paths(SITE).catalog_file.exists()
    report = json.loads((site_dir / "reports/cleanup.json").read_text())
    assert report["deleted"] >= 3 and not report["blocked"] and report["notUploadedKept"]
    assert chunks.site_cleaned(SITE)
    # the lake data is gone locally: a new month cannot be stacked until the lake data is restored
    pipe.site.add(OS, "2025-05", f"NEON.D99.{SITE}.{OS}.brd_countdata.2025-05.basic.20260101T000000Z.csv",
                  b"uid,plotID\nu9,P_009\n", "data")
    res = stack.stack_chunk(SITE, OS, ["2025-05"], chunk_id="late")
    assert res["status"] == "failed" and "cleaned" in res["errors"][0]["error"]


def test_delete_refuses_without_a_passing_verify(pipe: SimpleNamespace) -> None:
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload", "apply", "history"]) == 0
    files = _local(pipe, c.product, c.months)
    assert stream.delete_chunk(SITE, c.chunk_id) == 1  # not verified: not ready
    assert all(p.exists() for p in files)
    # a journal that claims 'verified' without a passing verify record is refused too
    chunks.set_status(pipe.site.con, SITE, c.chunk_id, "verified", done="verified")
    assert stream.delete_chunk(SITE, c.chunk_id) == 3
    assert all(p.exists() for p in files)
    ch = chunks.get_chunk(SITE, c.chunk_id)
    assert ch.status == "failed" and ch.done == "verified" and "verify" in ch.error


def test_verify_catches_replicas_avus_and_journal(pipe: SimpleNamespace) -> None:
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload", "apply", "history"]) == 0
    objs = sorted(p for p in pipe.store.objs if p.startswith(f"{S}/raw/{OS}/"))
    # 1) a checksum that no longer matches dl_md5
    rep = pipe.store.objs[objs[0]]["replicas"][0]
    good_sum, rep["checksum"] = rep["checksum"], "0" * 32
    assert stream.verify_chunk(SITE, c.chunk_id) == 2
    ch = chunks.get_chunk(SITE, c.chunk_id)
    assert ch.status == "failed" and ch.done == "recorded" and "no good replica" in ch.error
    rep["checksum"] = good_sum
    # 2) a core AVU missing in iRODS
    avus = pipe.store.objs[objs[1]]["avus"]
    core = next(t for t in avus if t[0] == "neon.month")
    avus.discard(core)
    assert stream.verify_chunk(SITE, c.chunk_id) == 2 and "core AVUs missing" in chunks.get_chunk(SITE,
                                                                                                    c.chunk_id).error
    avus.add(core)
    # 3) a journal row neither recorded nor spooled
    con = pipe.site.con
    rid = con.execute("SELECT id FROM avu_journal WHERE path=? LIMIT 1", (objs[2],)).fetchone()[0]
    con.execute("UPDATE avu_journal SET status='applied' WHERE id=?", (rid,))
    assert stream.verify_chunk(SITE, c.chunk_id) == 2
    assert "not recorded or spooled" in chunks.get_chunk(SITE, c.chunk_id).error
    con.execute("UPDATE avu_journal SET status='recorded' WHERE id=?", (rid,))  # 'recorded' counts like 'spooled'
    assert stream.verify_chunk(SITE, c.chunk_id) == 0
    # the verify record gates the delete; a file uploaded again after it stays on disk
    con.execute("UPDATE site_files SET uploaded_at='2999-01-01T00:00:00+00:00' WHERE local_path LIKE ? AND "
                "name LIKE '%brd_countdata%'", (f"%/{OS}/2023-05/%",))
    assert stream.delete_chunk(SITE, c.chunk_id) == 3
    kept = [p for p in _local(pipe, c.product, c.months) if p.exists()]
    assert [p.name for p in kept] == [n for (n,) in con.execute(
        "SELECT name FROM site_files WHERE product=? AND month='2023-05' AND name LIKE '%brd_countdata%'", (OS,))]
    info = chunks.get_chunk(SITE, c.chunk_id).info["delete"]
    assert info["blocked"] == {"uploaded after the chunk was verified": 1} and info["deleted"] >= 3


def test_stage_order_and_unknown_chunks(pipe: SimpleNamespace) -> None:
    c = pipe.plan[0]
    assert stream.stack_chunk(SITE, c.chunk_id) == 1  # not downloaded yet
    assert stream.download_chunk(SITE, "nope") == 1
    assert stream.download_chunk(SITE, c.chunk_id) == 0
    assert stream.download_chunk(SITE, c.chunk_id) == 0  # idempotent
    assert chunks.get_chunk(SITE, c.chunk_id).done == "downloaded"
    with pytest.raises(SystemExit):
        stream.main("bogus", SITE, c.chunk_id)
    assert stream.main("stack", SITE, c.chunk_id) == 0
    assert stream.finalize_site(SITE) == 1  # chunks still open


def test_cleanup_keeps_files_whose_copy_does_not_match(pipe: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    con = pipe.site.con
    for c in pipe.plan:
        chunks.set_status(con, SITE, c.chunk_id, "deleted", done="deleted")
    for step in stream.FINALIZE_STEPS:
        chunks.set_step(con, SITE, step, "ok")
    site_dir = site_lake.site_paths(SITE).site_dir
    good, bad, lost = (site_dir / "cards/a.md", site_dir / "cards/b.md", site_dir / "curation/c.json")
    for p in (good, bad, lost):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(p.name)
    mirror.push_tree([(good, f"{S}/cards/a.md", None), (bad, f"{S}/cards/b.md", None)])
    bad.write_text("changed after the upload")
    assert stream.cleanup_site(SITE, session=pipe.session) == 3
    assert not good.exists() and bad.exists() and lost.exists()
    report = json.loads((site_dir / "reports/cleanup.json").read_text())
    assert report["blocked"]["no good replica with the local size and md5"]["files"] == 1
    assert report["blocked"]["not in the Data Store"]["files"] == 1
    assert chunks.step_status(SITE, "cleanup") == "partial" and chunks.site_cleaned(SITE)


def test_download_waits_for_the_working_set(pipe: SimpleNamespace) -> None:
    con = pipe.site.con
    # another site holds 900 MB locally; the OS chunk (not downloaded yet) needs 200 MB more; the cap is 1 GB
    con.execute("INSERT INTO site_files(site, product, month, name, local_path, dl_status, dl_bytes) VALUES "
                "('OTHR', 'DP1.1', '2024-01', 'big.bin', 'sites/OTHR/raw/big.bin', 'ok', 900000000)")
    (pipe.root / "sites/OTHR/raw").mkdir(parents=True)
    (pipe.root / "sites/OTHR/raw/big.bin").write_bytes(b"x")
    c = _chunk(pipe, OS, "2023-05")
    con.execute("UPDATE site_files SET dl_status=NULL, size=100000000 WHERE product=? AND month='2023-05' AND "
                "name LIKE '%brd_countdata%'", (OS,))
    con.execute("UPDATE site_files SET dl_status=NULL, size=100000000 WHERE product=? AND month='2024-05' AND "
                "name LIKE '%brd_countdata%'", (OS,))
    assert stream.download_chunk(SITE, c.chunk_id, timeout_s=0) == 3
    ch = chunks.get_chunk(SITE, c.chunk_id)
    assert ch.status == "failed" and ch.done == "planned" and "working set" in ch.error
    # the other site's file is evicted while we wait: the chunk proceeds and holds a reservation meanwhile
    clock = {"t": 0.0}

    def sleep(s: float) -> None:
        clock["t"] += s
        con.execute("UPDATE site_files SET dl_status='ok', local_evicted=1 WHERE site='OTHR'")

    con.execute("ALTER TABLE site_files ADD COLUMN local_evicted INTEGER DEFAULT 0")
    w = fetch.wait_for_workset(SITE, OS, c.months, cap=10**9, timeout_s=600, poll_s=30, sleep=sleep,
                               clock=lambda: clock["t"])
    assert w["ok"] and w["need"] == 200000000 and w["waited_s"] == 30
    assert [tuple(r) for r in con.execute("SELECT bytes FROM fetch_workset")] == [(200000000,)]
    # a second chunk cannot squeeze in beside the reservation
    other = fetch.wait_for_workset(SITE, IS, ["2019-05"], cap=250000000, timeout_s=0, sleep=sleep,
                                   clock=lambda: clock["t"])
    assert other["reserved"] == 200000000
    fetch.release_workset(w["key"])
    assert con.execute("SELECT count(*) FROM fetch_workset").fetchone()[0] == 0
    # nothing else local or reserved: even a chunk larger than the cap may start
    assert not fetch.wait_for_workset(SITE, OS, c.months, cap=1, timeout_s=0)["ok"]  # the IS files are local
    con.execute("UPDATE site_files SET local_evicted=1 WHERE product=?", (IS,))
    alone = fetch.wait_for_workset(SITE, OS, c.months, cap=1, timeout_s=0)
    assert alone["ok"]
    fetch.release_workset(alone["key"])
