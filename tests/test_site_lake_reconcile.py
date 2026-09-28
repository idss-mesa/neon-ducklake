"""An old module table with a drifted column type is reconciled before new rows are inserted."""

from __future__ import annotations

import duckdb

from neon_ducklake.site import site_lake


def test_replace_where_reconciles_declared_list_types() -> None:
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lake")
    decl = site_lake.STACK_SCHEMAS["schema_drift"]
    # an older release created the list columns as INTEGER[] (inferred from empty lists)
    old = {c: ("INTEGER[]" if c in ("missingVsUnion", "notInVariables") else t) for c, t in decl.items()}
    con.execute("CREATE TABLE lake.schema_drift (" + ", ".join(f'"{c}" {t}' for c, t in old.items()) + ")")
    con.execute("INSERT INTO lake.schema_drift (productCode, tableName, missingVsUnion) VALUES ('DP', 't', [])")
    con.execute("CREATE TEMP TABLE src AS SELECT 'DP' AS productCode, 't2' AS tableName, "
                "['eventID'] AS missingVsUnion, ['x'] AS notInVariables")
    n = site_lake.replace_where(con, "schema_drift", "src", "productCode = ? AND tableName = ?", ["DP", "t2"])
    assert n == 1
    types = site_lake.column_types(con, "schema_drift")
    assert types["missingVsUnion"] == "VARCHAR[]" and types["notInVariables"] == "VARCHAR[]"
    assert con.execute("SELECT missingVsUnion FROM lake.schema_drift WHERE tableName='t2'").fetchone()[0] == ["eventID"]
    assert con.execute("SELECT count(*) FROM lake.schema_drift").fetchone()[0] == 2  # old row kept
