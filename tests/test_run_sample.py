"""site/sample.py: sample-month selection (tiny DuckDB standing in for the catalog) and the list/download loops."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from neon_ducklake.site import fetch, sample

from .test_site_common import site_env  # noqa: F401 - fixture


def _catalog(rows: list[tuple[str, str, str, str, bool, bool]]) -> duckdb.DuckDBPyConnection:
    """(productCode, siteCode, month, release, isProvisional, isReleaseOnly) rows of v_site_inventory."""
    con = duckdb.connect()
    con.execute("CREATE TABLE v_site_inventory (productCode VARCHAR, siteCode VARCHAR, month VARCHAR, "
                "release VARCHAR, isProvisional BOOLEAN, isReleaseOnly BOOLEAN, isExternal BOOLEAN, "
                "fileCount BIGINT, totalBytes BIGINT)")
    for r in rows:
        con.execute("INSERT INTO v_site_inventory VALUES (?,?,?,?,?,?,false,1,1)", list(r))
    return con


def _months(first: str, n: int) -> list[str]:
    y, m = map(int, first.split("-"))
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


@pytest.fixture
def catalog() -> duckdb.DuckDBPyConnection:
    rows = []
    # IS product: released through 2025-12, PROVISIONAL 2026-01..2026-08
    for m in _months("2024-01", 32):
        prov = m >= "2026-01"
        rows.append(("DP1.00002.001", "SRER", m, "PROVISIONAL" if prov else "RELEASE-2026", prov, False))
    # released only (no provisional months)
    for m in _months("2020-03", 5):
        rows.append(("DP1.10003.001", "SRER", m, "RELEASE-2026", False, False))
    # provisional only
    for m in ("2026-06", "2026-07"):
        rows.append(("DP1.20288.001", "SRER", m, "PROVISIONAL", True, False))
    # release-only product (earlier releases only)
    for m in ("2019-01", "2019-02", "2019-03"):
        rows.append(("DP1.00006.001", "SRER", m, "RELEASE-2023", False, True))
    # current + older release-only months
    rows += [("DP1.10045.001", "SRER", "2018-06", "RELEASE-2022", False, True),
             ("DP1.10045.001", "SRER", "2023-07", "RELEASE-2026", False, False)]
    # another site must not leak in
    rows.append(("DP1.00002.001", "HARV", "2026-09", "PROVISIONAL", True, False))
    return _catalog(rows)


def test_two_months_are_latest_released_plus_latest_provisional(catalog: duckdb.DuckDBPyConnection) -> None:
    got = sample.months_for("srer", 2, con=catalog)
    assert got["DP1.00002.001"] == ["2025-12", "2026-08"]
    assert got["DP1.10003.001"] == ["2020-06", "2020-07"]
    assert got["DP1.20288.001"] == ["2026-06", "2026-07"]
    assert got["DP1.00006.001"] == ["2019-02", "2019-03"]  # release-only product: its latest months
    assert got["DP1.10045.001"] == ["2018-06", "2023-07"]
    assert "2026-09" not in got["DP1.00002.001"]  # HARV row ignored


def test_n_fills_with_most_recent_and_must_haves_survive_n1(catalog: duckdb.DuckDBPyConnection) -> None:
    got = sample.months_for("SRER", 4, con=catalog)
    assert got["DP1.00002.001"] == ["2025-12", "2026-06", "2026-07", "2026-08"]
    assert got["DP1.10003.001"] == ["2020-04", "2020-05", "2020-06", "2020-07"]
    one = sample.months_for("SRER", 1, con=catalog)
    assert one["DP1.00002.001"] == ["2025-12", "2026-08"]  # both release paths even for N=1
    assert one["DP1.10003.001"] == ["2020-07"]
    big = sample.months_for("SRER", 100, con=catalog)
    assert len(big["DP1.00002.001"]) == 32
    with pytest.raises(ValueError):
        sample.select_months([], 0)


def test_current_month_wins_over_release_only_duplicate() -> None:
    rows = [{"productCode": "P", "month": "2020-01", "isProvisional": False, "isReleaseOnly": True},
            {"productCode": "P", "month": "2020-01", "isProvisional": False, "isReleaseOnly": False},
            {"productCode": "P", "month": "2020-02", "isProvisional": False, "isReleaseOnly": True}]
    assert sample.select_months(rows, 1) == {"P": ["2020-01"]}


def test_run_list_loops_products_in_process_and_aggregates_rc(catalog: duckdb.DuckDBPyConnection,
                                                               monkeypatch: pytest.MonkeyPatch,
                                                               capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[Any, ...]] = []

    def fake_main(step: str, site: str, products: list[str] | None = None, months: list[str] | None = None,
                  threads: int = 8) -> int:
        calls.append((step, site, tuple(products or []), tuple(months or []), threads))
        if products == ["DP1.20288.001"]:
            return 2
        if products == ["DP1.10003.001"]:
            raise RuntimeError("listing exploded")
        return 0

    monkeypatch.setattr(fetch, "main", fake_main)
    rc = sample.run_list("SRER", 2, threads=4, con=catalog)
    assert rc == 2
    assert [c[2][0] for c in calls] == sorted(c[2][0] for c in calls)  # one call per product, sorted
    assert ("list", "SRER", ("DP1.00002.001",), ("2025-12", "2026-08"), 4) in calls
    out = capsys.readouterr().out
    assert "5 product(s)" in out and "2 failed" in out and "DP1.10003.001" in out
    calls.clear()
    assert sample.run_list("SRER", 2, products=["DP1.00002.001", "DP9.99999.001"], con=catalog) == 0
    assert [c[2] for c in calls] == [("DP1.00002.001",)]


def test_run_download_restricts_to_sampled_products_and_months(catalog: duckdb.DuckDBPyConnection,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(fetch, "main", lambda step, site, products=None, months=None, threads=8:
                        calls.append((step, site, products, months, threads)) or 0)
    assert sample.run_download("SRER", 2, products=["DP1.00002.001", "DP1.20288.001"], threads=3,
                               con=catalog) == 0
    assert calls == [("download", "SRER", ["DP1.00002.001", "DP1.20288.001"],
                      ["2025-12", "2026-06", "2026-07", "2026-08"], 3)]
    assert sample.run_download("SRER", 2, products=["NOPE"], con=catalog) == 1


def test_months_for_without_catalog_is_a_clear_error(site_env: Path) -> None:  # noqa: F811
    with pytest.raises(SystemExit, match="catalog lake missing"):
        sample.months_for("SRER", 2)
