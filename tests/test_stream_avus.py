"""site/avus.py levels: chunk builds touch only month/object rows of their months; finalize the other levels;
variable counts come from the lake; units of cleaned-up directories are preserved."""

from __future__ import annotations

from pathlib import Path

import pytest

from neon_ducklake import state
from neon_ducklake.avu import bulk
from neon_ducklake.site import avus, chunks

from .test_site_common import site_env  # noqa: F401 - fixture
from .test_siteavus_common import P_IS, P_OS, SITE, detach_site_avus, make_facts, write_local_files

detach_site_avus()  # see test_siteavus_common.detach_site_avus

S = f"/zone/home/u/proj/sites/{SITE}"


@pytest.fixture
def con(site_env: Path):  # noqa: F811
    c = state.connect(state.DB_PATH)
    bulk.ensure_desired(c)
    yield c
    c.close()


def rows(con, where: str = "", args: tuple = ()) -> set[tuple]:
    return {tuple(r) for r in con.execute(f"SELECT unit_id, path, level, attribute, value, unit FROM avu_desired "
                                          f"WHERE scope=? {where}", (SITE, *args))}


def by_level(con) -> dict[str, set[tuple]]:
    out: dict[str, set[tuple]] = {}
    for r in rows(con):
        out.setdefault(r[2], set()).add(r)
    return out


def _changed_facts():
    """The lake moved on: new names everywhere, one 2024-06 object gone."""
    f = make_facts()
    f.site_info["siteName"] = "Renamed site"
    f.products[P_IS]["productName"] = "Renamed product"
    gone = next(r for r in f.files if r["productCode"] == P_IS and r["month"] == "2024-06" and r["kind"] == "readme")
    f.files.remove(gone)
    return f, gone


def test_chunk_build_only_touches_its_month_and_object_rows(con) -> None:
    f = make_facts()
    write_local_files(f)
    avus.build_desired(SITE, con=con, facts=f)
    before = by_level(con)
    f2, gone = _changed_facts()
    s = avus.build_desired(SITE, [P_IS], ["2024-06"], levels=avus.CHUNK_LEVELS, con=con, facts=f2)
    assert s["level_filter"] == ["month", "object"] and s["months_built"] == 1
    after = by_level(con)
    # no site/container/product/lake/side row was rebuilt (the renamed site and product are NOT applied)
    for level in set(before) - {"month", "object"}:
        assert after[level] == before[level], level
    assert (S, S, "site", "neon.siteName", "Test Site NEON", "") in after["site"]
    # the other month of the product (and other products' months) are untouched
    for unit in (f"{S}/raw/{P_IS}/2026-08", f"{S}/raw/{P_OS}/2024-05"):
        assert rows(con, "AND unit_id=?", (unit,)) == {r for r in before["month"] | before["object"] if r[0] == unit}
    # the chunk's month was rebuilt: the vanished object is gone, the others are there
    month = rows(con, "AND unit_id=?", (f"{S}/raw/{P_IS}/2024-06",))
    assert not any(r[1].endswith(gone["name"]) for r in month)
    assert {r[2] for r in month} == {"month", "object"} and len({r[1] for r in month}) == 6


def test_chunk_build_writes_no_product_or_site_rows_on_a_fresh_scope(con) -> None:
    s = avus.build_desired(SITE, [P_OS], ["2024-05"], levels={"month", "object"}, con=con, facts=make_facts())
    assert {r[2] for r in rows(con)} == {"month", "object"}
    assert {r[0] for r in rows(con)} == {f"{S}/raw/{P_OS}/2024-05"}
    assert s["products_built"] == [P_OS] and s["levels"].keys() == {"month", "object"}


def test_finalize_levels_rebuild_everything_but_month_rows(con) -> None:
    f = make_facts()
    write_local_files(f)
    for month, product in (("2024-06", P_IS), ("2026-08", P_IS), ("2024-05", P_OS)):
        avus.build_desired(SITE, [product], [month], levels=avus.CHUNK_LEVELS, con=con, facts=f)
    months_before = by_level(con)
    f2, _gone = _changed_facts()
    s = avus.build_desired(SITE, levels=avus.FINALIZE_LEVELS, con=con, facts=f2)
    after = by_level(con)
    assert after["month"] == months_before["month"] and after["object"] == months_before["object"]
    assert (S, S, "site", "neon.siteName", "Renamed site", "") in after["site"]
    assert any(r[3] == "neon.productName" and r[4] == "Renamed product" for r in after["product"])
    assert {"site", "container", "product"} <= set(after) and s["filtered"]
    # the lake/side groups expand to the builder levels
    assert avus.normalize_levels({"lake", "side"}) == {"lake-dir", "lake-file", "lake-catalog", "side-dir",
                                                      "side-file", "ref-file"}
    with pytest.raises(ValueError):
        avus.normalize_levels({"nope"})


def test_variable_count_comes_from_the_lake_not_the_csv(con) -> None:
    f = make_facts()  # no local files at all: streamed away
    var = next(r for r in f.files if r["kind"] == "variables" and r["month"] == "2024-06")
    f.variable_counts[var["name"]] = 42
    avus.build_desired(SITE, [P_IS], ["2024-06", "2026-08"], levels=avus.CHUNK_LEVELS, con=con, facts=f)
    got = {r[1].rsplit("/", 1)[1]: r[4] for r in rows(con, "AND attribute='neon.variableCount'")}
    assert got == {var["name"]: "42"}  # the other variables file has no lake count and no local CSV: no AVU


def test_raw_container_needs_no_local_raw_dir(con) -> None:
    f = make_facts()
    (Path(avus.site_lake.site_paths(SITE).site_dir) / "reports").mkdir(parents=True)
    avus.build_desired(SITE, levels={"container"}, con=con, facts=f)
    assert {r[1] for r in rows(con)} == {f"{S}/raw", f"{S}/reports"}


def test_cleaned_directories_are_preserved(con) -> None:
    f = make_facts()
    write_local_files(f)
    site_dir = avus.site_lake.site_paths(SITE).site_dir
    for rel in ("cards/DP1.00002.001.md", "curation/review.md", "ducklake/data/main/t/x.parquet", "reports/a.json"):
        (site_dir / rel).parent.mkdir(parents=True, exist_ok=True)
        (site_dir / rel).write_text("{}")
    avus.build_desired(SITE, con=con, facts=f)
    kept = rows(con, "AND (unit_id LIKE ? OR unit_id LIKE ?)", (f"{S}/cards%", f"{S}/curation%"))
    assert kept
    # cleanup_site ran: the local cards/curation/lake data are gone; a later full build must not drop their rows
    chunks.ensure_schema(con)
    chunks.set_step(con, SITE, "cleanup", "ok")
    for d in ("cards", "curation", "ducklake/data"):
        import shutil

        shutil.rmtree(site_dir / d)
    s = avus.build_desired(SITE, con=con, facts=make_facts())
    assert s["preserved"] == list(avus.CLEANED_DIRS) and s["filtered"]
    assert rows(con, "AND (unit_id LIKE ? OR unit_id LIKE ?)", (f"{S}/cards%", f"{S}/curation%")) == kept
    assert rows(con, "AND unit_id LIKE ?", (f"{S}/reports%",))  # still rebuilt from the kept reports
