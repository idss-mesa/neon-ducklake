"""DuckDB 2.0 refuses register/unregister while a fetchone() result is unfinished; the helpers finish it."""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from neon_ducklake import duck


def test_register_after_fetchone() -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT range AS i FROM range(10)")
    con.execute("SELECT i FROM t").fetchone()
    with pytest.raises(duckdb.Error):
        con.register("raw", pa.table({"a": [1]}))
    duck.register(con, "v", pa.table({"a": [1, 2]}))
    assert con.execute("SELECT sum(a) FROM v").fetchone() == (3,)
    duck.unregister(con, "v")
    assert con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name = 'v'").fetchone() == (0,)


def test_helpers_are_noops_when_nothing_is_pending() -> None:
    con = duckdb.connect()
    duck.register(con, "v", pa.table({"a": [1]}))
    duck.unregister(con, "v")
