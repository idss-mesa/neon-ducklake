"""site/avus.py writing: full builds replace the scope, filtered builds replace only their units."""

from __future__ import annotations

from pathlib import Path

import pytest

from neon_ducklake import state
from neon_ducklake.avu import bulk
from neon_ducklake.site import avus

from .test_site_common import site_env  # noqa: F401 - fixture
from .test_siteavus_common import P_IS, P_OS, SITE, detach_site_avus, make_facts, write_local_files

detach_site_avus()  # see test_siteavus_common.detach_site_avus

S = f"/zone/home/u/proj/sites/{SITE}"


@pytest.fixture
def con(site_env: Path):  # noqa: F811
    assert str(state.DB_PATH).startswith(str(site_env))  # never the real journal
    c = state.connect(state.DB_PATH)
    bulk.ensure_desired(c)
    yield c
    c.close()


def units(con, scope: str = SITE) -> dict[str, int]:
    return {r[0]: r[1] for r in con.execute("SELECT unit_id, count(*) FROM avu_desired WHERE scope=? GROUP BY 1",
                                            (scope,))}


def rows_of(con, unit: str) -> set[tuple]:
    return {tuple(r) for r in con.execute("SELECT path, attribute, value, unit FROM avu_desired WHERE scope=? AND "
                                          "unit_id=?", (SITE, unit))}


def test_full_build_summary_and_scope_isolation(con) -> None:
    f = make_facts()
    write_local_files(f)
    con.execute("INSERT INTO avu_desired(scope, unit_id, path, target_type, attribute, value, unit) "
                "VALUES ('catalog', '/x', '/x', 'collection', 'neon.project', 'neon-ducklake', '')")
    s = avus.build_desired("test", con=con, facts=f)
    assert s["site"] == SITE and not s["filtered"] and s["rows"] == s["scope_rows"] > 300
    assert s["levels"]["site"]["targets"] == 1 and s["levels"]["object"]["targets"] == len(f.files)
    assert s["object_profiles"] == {"hdf5": 1, "is": 5, "os": 2, "support": 10}
    assert "datacite_errors" not in s
    # values are normalised exactly like replace_desired (<= 2,000 bytes)
    assert con.execute("SELECT max(length(CAST(value AS BLOB))) FROM avu_desired WHERE scope=?",
                       (SITE,)).fetchone()[0] <= 2000
    # a second full build replaces (no duplicates) and never touches another scope
    s2 = avus.build_desired(SITE, con=con, facts=make_facts())
    assert s2["scope_rows"] == s["scope_rows"]
    assert con.execute("SELECT count(*) FROM avu_desired WHERE scope='catalog'").fetchone()[0] == 1
    text = avus.render(s2)
    assert "full scope" in text and "object" in text
    assert "[site]" in avus.samples(con, SITE)


def test_filtered_build_replaces_only_selected_units(con) -> None:
    f = make_facts()
    write_local_files(f)
    avus.build_desired(SITE, con=con, facts=f)
    before = units(con)
    other_month = f"{S}/raw/{P_IS}/2026-08"
    other_product = f"{S}/raw/{P_OS}"
    keep_month, keep_product = rows_of(con, other_month), rows_of(con, other_product)
    # the lake changed: one 2024-06 file vanished, and a product name changed everywhere
    f2 = make_facts()
    gone = next(r for r in f2.files if r["productCode"] == P_IS and r["month"] == "2024-06" and r["kind"] == "readme")
    f2.files.remove(gone)
    f2.products[P_OS]["productName"] = "Renamed birds"
    f2.site_info["siteName"] = "Renamed site"
    s = avus.build_desired(SITE, [P_IS], ["2024-06"], con=con, facts=f2)
    assert s["filtered"] and s["products_built"] == [P_IS] and s["months_built"] == 1
    after = units(con)
    assert set(after) == set(before)  # nothing added or lost at unit level
    assert rows_of(con, other_month) == keep_month  # unselected month untouched
    assert rows_of(con, other_product) == keep_product  # unselected product untouched (old name kept)
    assert ("neon.productName", "Breeding landbird point counts") in {(a, v) for _p, a, v, _u in keep_product}
    # the selected month lost exactly the vanished object; site-level units were rebuilt
    sel = rows_of(con, f"{S}/raw/{P_IS}/2024-06")
    assert not any(p.endswith(gone["name"]) for p, *_ in sel)
    assert (S, "neon.siteName", "Renamed site", "") in rows_of(con, S)
    assert s["rows_deleted"] >= s["rows"] - 5


def test_filtered_build_removes_units_that_disappeared(con) -> None:
    f = make_facts()
    avus.build_desired(SITE, con=con, facts=f)
    f2 = make_facts()
    f2.files = [r for r in f2.files if not (r["productCode"] == P_IS and r["month"] == "2026-08")]
    avus.build_desired(SITE, [P_IS], None, con=con, facts=f2)
    assert f"{S}/raw/{P_IS}/2026-08" not in units(con)  # the selected product's vanished month is gone
    assert f"{S}/raw/{P_OS}/2024-05" in units(con)


def test_replace_units_normalises_and_moves_paths(con) -> None:
    rows = [("u1", "/p/a", "collection", "neon.x", "  v  ", "", "lvl", "g"),
            ("u1", "/p/a", "collection", "neon.empty", "   ", "", "lvl", "g"),
            ("u1", "/p/a", "collection", "ipc_UUID", "x", "", "lvl", "g"),
            ("u1", "/p/a", "collection", "neon.long", "word " * 1000, "", "lvl", "g")]
    res = avus.replace_units(con, SITE, rows, lambda u: u == "u1")
    assert res["rows"] == 2 and res["dropped_empty"] == 1 and res["dropped_protected"] == 1
    got = {r[0]: r[1] for r in con.execute("SELECT attribute, value FROM avu_desired WHERE scope=?", (SITE,))}
    assert got["neon.x"] == "v" and len(got["neon.long"].encode()) <= 2000
    # the same path re-inserted under another unit replaces the old rows (PK is per path)
    avus.replace_units(con, SITE, [("u2", "/p/a", "collection", "neon.x", "w", "", "lvl", "g")], lambda u: u == "u2")
    assert {tuple(r) for r in con.execute("SELECT unit_id, attribute, value FROM avu_desired WHERE scope=?",
                                          (SITE,))} == {("u2", "neon.x", "w")}
    with pytest.raises(ValueError):
        avus.replace_units(con, SITE, [("u", "/p", "resource", "a", "b", "", "l", "g")], lambda u: True)


def test_bulk_loads_what_the_builder_wrote(con) -> None:
    avus.build_desired(SITE, con=con, facts=make_facts())
    loaded = bulk.load_desired(con, SITE)
    month = loaded[f"{S}/raw/{P_IS}/2024-06"]
    kinds = {t for _p, t in month.desired}
    assert kinds == {"collection", "data_object"}
    assert all(p.startswith(f"{S}/raw/{P_IS}/2024-06") for p, _t in month.desired)
