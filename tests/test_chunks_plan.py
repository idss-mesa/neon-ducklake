"""site/chunks.py: byte-bounded plans, one product per chunk, stable ids, frozen started chunks, journal."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from neon_ducklake import state
from neon_ducklake.site import chunks

from .test_site_common import SITE, site_env  # noqa: F401 - site_env is a fixture

GB = 10**9
A, B = "DP1.00001.001", "DP1.00002.001"


def _files(con, product: str, month: str, sizes: list[int], *, status: str | None = None) -> None:
    start = con.execute("SELECT count(*) FROM site_files WHERE product=? AND month=?", (product, month)).fetchone()[0]
    for i, size in enumerate(sizes, start=start):
        con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status) VALUES (?,?,?,?,?,?)",
                    (SITE, product, month, f"{product}.{month}.f{i}.csv", size, status))


@pytest.fixture
def con(site_env: Path):  # noqa: F811
    c = state.connect(state.DB_PATH)
    yield c
    c.close()


def _ids(plan: list[chunks.Chunk]) -> list[str]:
    return [c.chunk_id for c in plan]


def test_byte_splits_one_product_per_chunk_and_ascending_months(con) -> None:
    for m in ("2024-01", "2024-02", "2024-03", "2024-04", "2024-05"):
        _files(con, A, m, [1 * GB, 2 * GB])  # 3 GB per month
    _files(con, B, "2023-12", [10])
    plan = chunks.plan_chunks(SITE, max_bytes=7 * GB, con=con)
    assert _ids(plan) == [f"{A}__2024-01_2024-02", f"{A}__2024-03_2024-04", f"{A}__2024-05_2024-05",
                          f"{B}__2023-12_2023-12"]
    first = plan[0]
    assert (first.product, first.months, first.n_files, first.bytes) == (A, ["2024-01", "2024-02"], 4, 6 * GB)
    assert all(c.status == "planned" and c.done == "planned" for c in plan)
    rows = con.execute("SELECT chunk_id, product, months, n_files, bytes, status FROM stream_chunks "
                       "ORDER BY chunk_id").fetchall()
    assert (rows[0][0], json.loads(rows[0][2])) == (f"{A}__2024-01_2024-02", ["2024-01", "2024-02"])
    assert chunks.get_chunk(SITE, plan[1].chunk_id, con=con).months == ["2024-03", "2024-04"]
    assert [c.chunk_id for c in chunks.list_chunks(SITE, con=con)] == _ids(plan)


def test_single_oversized_month_is_its_own_chunk(con) -> None:
    _files(con, A, "2024-01", [1 * GB])
    _files(con, A, "2024-02", [9 * GB, 3 * GB])  # 12 GB > 8 GB
    _files(con, A, "2024-03", [1 * GB])
    plan = chunks.plan_chunks(SITE, max_bytes=8 * GB, con=con)
    assert [(c.months, c.bytes) for c in plan] == [(["2024-01"], 1 * GB), (["2024-02"], 12 * GB),
                                                   (["2024-03"], 1 * GB)]


def test_ids_are_stable_and_started_chunks_are_frozen(con) -> None:
    for m in ("2024-01", "2024-02", "2024-03"):
        _files(con, A, m, [2 * GB])
    plan = chunks.plan_chunks(SITE, max_bytes=5 * GB, con=con)
    assert _ids(plan) == [f"{A}__2024-01_2024-02", f"{A}__2024-03_2024-03"]
    assert _ids(chunks.plan_chunks(SITE, max_bytes=5 * GB, con=con)) == _ids(plan)  # same inputs, same ids
    # the first chunk started; later a new month is listed and the chunk size shrinks
    chunks.set_status(con, SITE, plan[0].chunk_id, "downloaded", done="downloaded")
    _files(con, A, "2024-04", [2 * GB])
    again = chunks.plan_chunks(SITE, max_bytes=3 * GB, con=con)
    assert _ids(again) == [f"{A}__2024-01_2024-02", f"{A}__2024-03_2024-03", f"{A}__2024-04_2024-04"]
    assert again[0].status == "downloaded" and again[0].months == ["2024-01", "2024-02"]
    # the stale planned row of the old packing is gone, nothing overlaps
    months = [m for (ms,) in con.execute("SELECT months FROM stream_chunks") for m in json.loads(ms)]
    assert sorted(months) == ["2024-01", "2024-02", "2024-03", "2024-04"]


def test_new_month_between_frozen_chunks_and_filters(con) -> None:
    for m in ("2024-01", "2024-03"):
        _files(con, A, m, [1 * GB])
    plan = chunks.plan_chunks(SITE, max_bytes=8 * GB, con=con)
    assert _ids(plan) == [f"{A}__2024-01_2024-03"]
    chunks.set_status(con, SITE, plan[0].chunk_id, "stacked", done="stacked")
    _files(con, A, "2024-02", [1 * GB])  # appears inside the frozen chunk's range
    _files(con, A, "2024-05", [1 * GB])
    got = chunks.plan_chunks(SITE, max_bytes=8 * GB, con=con)
    assert _ids(got) == [f"{A}__2024-01_2024-03", f"{A}__2024-02_2024-02", f"{A}__2024-05_2024-05"]
    # product/month filters only (re)plan that slice and leave other rows alone
    _files(con, B, "2024-01", [5])
    only_b = chunks.plan_chunks(SITE, products=[B], con=con)
    assert _ids(only_b) == [f"{B}__2024-01_2024-01"]
    assert len(chunks.list_chunks(SITE, con=con)) == 4


def test_superseded_files_are_not_planned(con) -> None:
    from neon_ducklake.site.fetch import SUPERSEDED_SCHEMA

    con.executescript(SUPERSEDED_SCHEMA)
    _files(con, A, "2024-01", [1 * GB])
    _files(con, A, "2024-02", [1 * GB], status="superseded")
    con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status) VALUES (?,?,?,?,?,?)",
                (SITE, A, "2024-01", "old.csv", 7 * GB, "ok"))
    con.execute("INSERT INTO fetch_superseded VALUES (?,?,?,?,?,?)", (SITE, A, "2024-01", "old.csv", 1, "x"))
    plan = chunks.plan_chunks(SITE, con=con)
    assert [(c.chunk_id, c.n_files, c.bytes) for c in plan] == [(f"{A}__2024-01_2024-01", 1, 1 * GB)]


def test_finished_chunk_with_new_files_is_replanned(con) -> None:
    _files(con, A, "2024-01", [5], status="ok")
    [c] = chunks.plan_chunks(SITE, con=con)
    chunks.set_status(con, SITE, c.chunk_id, "deleted", done="deleted")
    assert chunks.plan_chunks(SITE, con=con)[0].status == "deleted"  # nothing new: stays finished
    _files(con, A, "2024-01", [7, 8])  # a re-list found two more files (not downloaded yet)
    [again] = chunks.plan_chunks(SITE, con=con)
    assert (again.chunk_id, again.status, again.done, again.n_files) == (c.chunk_id, "planned", "planned", 3)
    assert chunks.unfinished(SITE, con=con)[0].chunk_id == c.chunk_id


def test_sizes_env_and_schema(con, monkeypatch: pytest.MonkeyPatch) -> None:
    assert chunks.parse_bytes("8GB") == 8 * GB == chunks.parse_bytes("8 g") == chunks.parse_bytes(8e9)
    assert chunks.parse_bytes("1GiB") == 1 << 30 and chunks.parse_bytes("123") == 123
    with pytest.raises(ValueError):
        chunks.parse_bytes("eight")
    monkeypatch.setenv(chunks.CHUNK_BYTES_ENV, "2G")
    assert chunks.chunk_bytes() == 2 * GB
    monkeypatch.setenv(chunks.CHUNK_BYTES_ENV, "junk")
    assert chunks.chunk_bytes() == chunks.DEFAULT_CHUNK_BYTES
    monkeypatch.setenv(chunks.CHUNK_BYTES_ENV, "1G")
    _files(con, A, "2024-01", [GB])
    _files(con, A, "2024-02", [GB])
    assert len(chunks.plan_chunks(SITE, con=con)) == 2  # the env value is read at call time
    assert chunks.make_chunk_id(A, ["2024-03", "2024-01"]) == f"{A}__2024-01_2024-03"
    # a table another process created with only the contract columns gains the module columns
    con.execute("DROP TABLE stream_chunks")
    con.execute("CREATE TABLE stream_chunks (site TEXT NOT NULL, chunk_id TEXT NOT NULL, product TEXT NOT NULL,"
                " months TEXT NOT NULL, n_files INTEGER, bytes INTEGER, status TEXT, attempt INTEGER DEFAULT 0,"
                " started_at TEXT, finished_at TEXT, error TEXT, PRIMARY KEY (site, chunk_id))")
    chunks.ensure_schema(con)
    assert {"done", "info", "planned_at"} <= {r[1] for r in con.execute("PRAGMA table_info(stream_chunks)")}
    assert len(chunks.plan_chunks(SITE, con=con)) == 2


def test_replan_resets_finalize_steps_and_stacked_chunks_are_listed(con) -> None:
    _files(con, A, "2024-01", [5], status="ok")
    _files(con, B, "2024-01", [5], status="ok")
    a, b = chunks.plan_chunks(SITE, con=con)
    chunks.set_status(con, SITE, a.chunk_id, "deleted", done="deleted", info={"stack": {"status": "ok",
                                                                                         "lastSnapshot": 7}})
    chunks.set_status(con, SITE, b.chunk_id, "avus", done="avus", info={"stack": {"status": "empty", "files": 0}})
    assert chunks.stacked_required(SITE, con=con) == [a.chunk_id]  # an empty stack committed nothing
    assert chunks.stacked_required(SITE, con=con, exclude=[a.chunk_id]) == []
    for step in (*chunks.FINALIZE_STEPS, "cleanup"):
        chunks.set_step(con, SITE, step, "ok")
    chunks.plan_chunks(SITE, con=con)  # nothing new: the finalize steps stay
    assert chunks.step_status(SITE, "avus", con=con) == "ok"
    _files(con, A, "2024-01", [7])  # a re-list: the finished chunk is re-planned, so is the site's finalize
    chunks.plan_chunks(SITE, con=con)
    steps = dict(con.execute("SELECT step, status FROM stream_sites WHERE site=?", (SITE,)).fetchall())
    assert steps == {"cleanup": "ok"}  # cleanup stays: only restore_lake may clear it
    # lake pushes: only rows whose snapshot the local lake holds vouch for it
    chunks.record_lake_push(con, SITE, "A" * 32, 3, snapshot_key="3|t|m")
    chunks.record_lake_push(con, SITE, "b" * 32, 5, kind="restore", snapshot_key="5|t|n")
    assert chunks.last_lake_push(SITE, con=con)["catalog_md5"] == "b" * 32
    assert chunks.last_lake_push(SITE, con=con, local_keys={"3|t|m"})["catalog_md5"] == "a" * 32
    assert chunks.last_lake_push(SITE, con=con, local_keys=set()) is None
    with pytest.raises(ValueError):
        chunks.record_lake_push(con, SITE, "c" * 32, 1, kind="bogus")


def test_a_new_chunk_after_finalize_resets_the_finalize_steps(con) -> None:
    """A re-list that adds months (not files to a finished chunk) must rebuild the product/site AVUs too."""
    _files(con, A, "2024-01", [5], status="ok")
    [a] = chunks.plan_chunks(SITE, con=con)
    chunks.set_status(con, SITE, a.chunk_id, "deleted", done="deleted")
    for step in (*chunks.FINALIZE_STEPS, "cleanup"):
        chunks.set_step(con, SITE, step, "ok")
    chunks.plan_chunks(SITE, con=con)
    assert chunks.step_status(SITE, "avus", con=con) == "ok"  # nothing new
    _files(con, A, "2024-02", [5])  # a new month of the product: a new chunk beside the finished one
    plan = chunks.plan_chunks(SITE, con=con)
    assert [c.status for c in plan] == ["deleted", "planned"]
    steps = dict(con.execute("SELECT step, status FROM stream_sites WHERE site=?", (SITE,)).fetchall())
    assert steps == {"cleanup": "ok"}
