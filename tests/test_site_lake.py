"""site_lake: paths, attach/options, commit messages, MERGE upserts, checkpoint_detach."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from neon_ducklake.site import site_lake

from .test_site_common import SITE, site_env  # noqa: F401 - fixture

COLS = {"k": "VARCHAR", "v": "BIGINT", "tags": "VARCHAR[]"}


def _src(con: duckdb.DuckDBPyConnection, rows: list[dict], name: str = "src") -> str:
    con.register(name, site_lake.arrow_table(rows, COLS))
    return name


def test_paths_are_resolved_at_call_time(site_env: Path) -> None:  # noqa: F811
    p = site_lake.site_paths("test")
    assert p.site == SITE
    assert p.catalog_file == site_env / "sites" / SITE / "ducklake" / "neon_test.ducklake"
    assert p.raw_rel("DP1.00002.001", "2024-06", "x.csv") == "sites/TEST/raw/DP1.00002.001/2024-06/x.csv"
    assert p.irods_raw("DP1.00002.001", "2024-06", "x.csv") == \
        "/zone/home/u/proj/sites/TEST/raw/DP1.00002.001/2024-06/x.csv"


def test_upsert_commit_and_checkpoint(site_env: Path) -> None:  # noqa: F811
    con = site_lake.open_site_lake(SITE)
    opts = dict(con.execute("SELECT option_name, value FROM lake.options()").fetchall())
    assert opts["require_commit_message"] == "true" and opts["data_inlining_row_limit"] == "0"
    assert opts["data_path"].endswith("sites/TEST/ducklake/data/")
    with site_lake.commit(con, "first", {"n": 2}):
        r = site_lake.upsert(con, "t", _src(con, [{"k": "a", "v": 1, "tags": ["x"]}, {"k": "b", "v": 2}]), ["k"], COLS)
    assert r == {"changed": 2, "source": 2}
    # unchanged rows are not rewritten; one changed + one new
    with site_lake.commit(con, "second"):
        r = site_lake.upsert(con, "t", _src(con, [{"k": "a", "v": 1, "tags": ["x"]}, {"k": "b", "v": 3},
                                                  {"k": "c", "v": None}], "src2"), ["k"], COLS)
    assert r == {"changed": 2, "source": 3}
    assert con.execute("SELECT k, v FROM lake.t ORDER BY k").fetchall() == [("a", 1), ("b", 3), ("c", None)]
    snaps = con.execute("SELECT author, commit_message FROM lake.snapshots() WHERE author IS NOT NULL").fetchall()
    assert snaps == [("tswetnam", "first"), ("tswetnam", "second")]
    # a write without commit() is refused (require_commit_message)
    with pytest.raises(duckdb.Error):
        con.execute("CREATE TABLE lake.nope AS SELECT 1 AS x")
    site_lake.checkpoint_detach(con, SITE)
    p = site_lake.site_paths(SITE)
    assert not p.wal_file.exists()
    assert list(p.data_dir.rglob("*.parquet"))
    con.close()
    ro = site_lake.open_site_lake(SITE, read_only=True)
    assert ro.execute("SELECT count(*) FROM lake.t").fetchone()[0] == 3
    ro.close()


def test_rollback_on_error(site_env: Path) -> None:  # noqa: F811
    con = site_lake.open_site_lake(SITE)
    with pytest.raises(RuntimeError), site_lake.commit(con, "boom"):
        site_lake.ensure_table(con, "t", COLS)
        raise RuntimeError("x")
    assert not site_lake.table_exists(con, "t")
    con.close()


def test_replace_where_adds_columns(site_env: Path) -> None:  # noqa: F811
    import pyarrow as pa

    con = site_lake.open_site_lake(SITE)
    con.register("a", pa.table({"productCode": ["P1", "P1"], "x": ["1", "2"]}))
    con.register("b", pa.table({"productCode": ["P2"], "x": ["3"], "y": ["new"]}))
    con.register("c", pa.table({"productCode": ["P1"], "x": ["9"]}))
    with site_lake.commit(con, "a"):
        site_lake.replace_where(con, "s", "a", "productCode = ?", ["P1"])
    with site_lake.commit(con, "b"):
        site_lake.replace_where(con, "s", "b", "productCode = ?", ["P2"])
    with site_lake.commit(con, "c"):
        site_lake.replace_where(con, "s", "c", "productCode = ?", ["P1"])
    assert con.execute("SELECT productCode, x, y FROM lake.s ORDER BY 1").fetchall() == [("P1", "9", None),
                                                                                       ("P2", "3", "new")]
    con.close()


def test_attach_catalog_missing(site_env: Path) -> None:  # noqa: F811
    con = site_lake.connect(SITE)
    assert site_lake.attach_catalog(con) is False
    con.close()


def test_moved_root_reattaches(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """The absolute DATA_PATH is overridden on attach, so a moved tree still reads."""
    from neon_ducklake import env

    con = site_lake.open_site_lake(SITE)
    with site_lake.commit(con, "t"):
        site_lake.upsert(con, "t", _src(con, [{"k": "a", "v": 1}]), ["k"], COLS)
    site_lake.checkpoint_detach(con, SITE)
    con.close()
    moved = site_env.parent / "moved"
    site_env.rename(moved)
    monkeypatch.setattr(env, "ROOT", moved)
    monkeypatch.setattr(env, "SITES_DIR", moved / "sites")
    ro = site_lake.open_site_lake(SITE, read_only=True)
    assert ro.execute("SELECT v FROM lake.t").fetchall() == [(1,)]
    ro.close()


def test_files_integrity_columns_are_added_to_old_lakes(site_env: Path) -> None:  # noqa: F811
    """Contract (b): storageMd5/neonMd5/transferVerified are nullable; an old files table gains them on upsert."""
    assert site_lake.FILES_COLUMNS["storageMd5"] == "VARCHAR" and site_lake.FILES_COLUMNS["neonMd5"] == "VARCHAR"
    assert site_lake.FILES_COLUMNS["transferVerified"] == "BOOLEAN"
    assert site_lake.FILES_COLUMNS["md5Verified"] == "BOOLEAN"
    old = {c: t for c, t in site_lake.FILES_COLUMNS.items()
           if c not in ("storageMd5", "neonMd5", "transferVerified")}
    row = {"productCode": "DP1.00002.001", "month": "2024-06", "name": "a.csv", "md5": "x", "md5Verified": True}
    con = site_lake.open_site_lake(SITE)
    con.register("src_old", site_lake.arrow_table([row], old))
    with site_lake.commit(con, "old files"):
        site_lake.upsert(con, "files", "src_old", site_lake.FILES_KEYS, old)
    assert "storageMd5" not in site_lake.table_columns(con, "files")
    # a writer that does not know the new columns yet (NULLs) and one that does
    con.register("src_new", site_lake.arrow_table([row, {**row, "name": "b.csv", "storageMd5": "y", "neonMd5": "x",
                                                         "transferVerified": True, "md5Verified": False}],
                                                  site_lake.FILES_COLUMNS))
    with site_lake.commit(con, "new files"):
        r = site_lake.upsert(con, "files", "src_new", site_lake.FILES_KEYS, site_lake.FILES_COLUMNS)
    assert r == {"changed": 1, "source": 2}
    cols = site_lake.table_columns(con, "files")
    assert cols["transferVerified"] == "BOOLEAN" and cols["storageMd5"] == "VARCHAR"
    assert con.execute("SELECT name, storageMd5, neonMd5, transferVerified, md5Verified FROM lake.files "
                       "ORDER BY name").fetchall() == [("a.csv", None, None, None, True), ("b.csv", "y", "x", True, False)]
    con.close()
