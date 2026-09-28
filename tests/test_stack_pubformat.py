"""F5 + F9: dateTime columns typed from the NEON variables pubFormat (Java pattern -> strptime)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pytest

from neon_ducklake.site import stack

from .test_site_common import SITE, site_env  # noqa: F401 - site_env is a fixture
from .test_site_stack import Site, _csv, _q, _types

HBP = "DP1.10023.001"
SIM = "DP1.10111.001"


@pytest.mark.parametrize(("pub", "kind", "pattern"), [
    ("yyyy-MM-dd'T'HH'Z'(floor)", "timestamp", "%Y-%m-%dT%HZ"),
    ("yyyy-MM-dd'T'HH:mm'Z'(round)", "timestamp", "%Y-%m-%dT%H:%MZ"),
    ("yyyy-MM-dd'T'HH:mm:ss'Z'", "timestamp", "%Y-%m-%dT%H:%M:%SZ"),
    ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "timestamp", "%Y-%m-%dT%H:%M:%S.%gZ"),
    ("yyyy-MM-dd(floor)", "date", "%Y-%m-%d"),
    ("yyyy-MM-dd (floor)", "date", "%Y-%m-%d"),
    ("yyyy-MM(floor)", "month", "%Y-%m"),
    ("yyyy-MM", "month", "%Y-%m"),
    ("yyyy(floor)", "year", "%Y"),
    ("yyyy", "year", "%Y"),
    ("yyyy-MM-dd(ceil)", "date", "%Y-%m-%d"),
    ("dd/MM/yyyy", "date", "%d/%m/%Y"),
    ("yyyy'%'", "year", "%Y%%"),
    ("NA", "unknown", None),
    ("asIs", "unknown", None),
    ("", "unknown", None),
    (None, "unknown", None),
    ("yyyy-DDD", "unknown", None),  # day-of-year: not mapped
    ("HH:mm", "unknown", None),  # no year
    ("yyyy-MM-dd'T", "unknown", None),  # unterminated literal
])
def test_java_to_strptime(pub: str | None, kind: str, pattern: str | None) -> None:
    assert stack.java_to_strptime(pub) == stack.PubFormat(kind, pattern)


def _eval(expr: str, values: list[str | None]) -> list:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    vals = ", ".join("(NULL)" if v is None else f"('{v}')" for v in values)
    return [r[0] for r in con.execute(f"SELECT {expr} FROM (VALUES {vals}) t(x)").fetchall()]


def test_cast_for_pub_formats() -> None:
    e, t = stack.cast_for("x", "dateTime", "yyyy-MM-dd'T'HH'Z'(floor)", qual="")
    assert t == "TIMESTAMPTZ"
    got = _eval(e, ["2024-11-14T15Z", "2024-11-14T15:30Z", "2024-11-14", "junk", None])
    utc = dt.UTC
    assert [g.astimezone(utc) if g else None for g in got] == [
        dt.datetime(2024, 11, 14, 15, tzinfo=utc), dt.datetime(2024, 11, 14, 15, 30, tzinfo=utc),
        dt.datetime(2024, 11, 14, tzinfo=utc), None, None]
    e, t = stack.cast_for("x", "dateTime", "yyyy(floor)", qual="")
    assert t == "INTEGER"
    assert _eval(e, ["2021", " 2015 ", "2019-06-01", "21", "x", None]) == [2021, 2015, 2019, None, None, None]
    e, t = stack.cast_for("x", "dateTime", "yyyy-MM(floor)", qual="")
    assert t == "DATE"
    assert _eval(e, ["2024-06", "2024-06-17", "2024-13", None]) == [
        dt.date(2024, 6, 1), dt.date(2024, 6, 1), None, None]
    e, t = stack.cast_for("x", "dateTime", "yyyy-MM-dd(floor)", qual="")
    assert t == "DATE" and _eval(e, ["2024-06-17", "2024-06-17T12:05Z"]) == [dt.date(2024, 6, 17)] * 2
    # NA / unknown pattern -> generic list (TIMESTAMPTZ), which now also knows the hour-only form
    for pub in ("NA", None, "yyyy-DDD'T'HH"):
        e, t = stack.cast_for("x", "dateTime", pub, qual="")
        assert t == "TIMESTAMPTZ"
        assert _eval(f"CAST({e} AS VARCHAR)", ["2024-11-14T15Z"])[0].startswith("2024-11-14 15:00:00")
    assert stack.cast_for("x", "dateTime", "yyyy-DDD", qual="")[1] == "DATE"  # unknown date-only: as before
    # file_stats time expression follows the same pattern
    te = stack.time_expr("x", "dateTime", "yyyy(floor)", qual="")
    assert _eval(f"year({te})", ["2021"]) == [2021]


def _vars(table: str, rows: list[tuple[str, str, str]]) -> bytes:
    h = ["table", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat", "primaryKey",
         "categoricalCodeName"]
    return _csv(h, [[table, f, "d", t, "NA", "basic", pf, "N", ""] for f, t, pf in rows])


def test_pub_format_typing_end_to_end(site_env: Path) -> None:  # noqa: F811
    """KONZ hbp_massdata.collectDate ('2024-11-14T15Z') and ARIK sim year ('2017') are typed, not VARCHAR."""
    s = Site(site_env)
    hour = "yyyy-MM-dd'T'HH'Z'(floor)"
    pub = "20250101T000000Z"
    s.add(HBP, "2024-11", f"NEON.D06.{SITE}.{HBP}.variables.{pub}.csv", _vars("hbp_massdata", [
        ("uid", "string", "asIs"), ("setDate", "dateTime", hour), ("collectDate", "dateTime", hour),
        ("ovenStartDate", "dateTime", hour), ("dryMass", "real", "*.##(round)")]), "variables")
    s.add(HBP, "2024-11", f"NEON.D06.{SITE}.{HBP}.hbp_massdata.2024-11.basic.{pub}.csv", _csv(
        ["uid", "setDate", "collectDate", "ovenStartDate", "dryMass"],
        [[f"u{i}", "2024-09-20T16Z", f"2024-11-14T{15 + i}Z", "2024-11-16T23Z", f"{i}.5"] for i in range(4)]),
        "data")
    s.add(SIM, "2023-06", f"NEON.D10.{SITE}.{SIM}.variables.{pub}.csv", _vars("sim_locationVisitsAndAttributes", [
        ("uid", "string", "asIs"), ("year", "dateTime", "yyyy(floor)"), ("yearMonth", "dateTime", "yyyy-MM(floor)"),
        ("eventDate", "dateTime", "yyyy-MM-dd(floor)")]), "variables")
    s.add(SIM, "2023-06", f"NEON.D10.{SITE}.{SIM}.sim_locationVisitsAndAttributes.2023-06.basic.{pub}.csv", _csv(
        ["uid", "year", "yearMonth", "eventDate"],
        [["s1", "2017", "2017-05", "2017-05-02"], ["s2", "2020", "2020-06", "2020-06-03"]]), "data")
    assert stack.main(SITE) == 0
    hbp = "dp1_10023_001__hbp_massdata"
    t = _types(hbp)
    assert t["collectDate"] == t["setDate"] == t["ovenStartDate"] == "TIMESTAMP WITH TIME ZONE"
    got = _q(f"SELECT min(collectDate), max(collectDate) FROM lake.{hbp}")[0]
    assert (got[0].hour, got[1].hour, got[0].day) == (15, 18, 14)
    sim = "dp1_10111_001__sim_locationvisitsandattributes"
    t = _types(sim)
    assert t["year"] == "INTEGER" and t["yearMonth"] == "DATE" and t["eventDate"] == "DATE"
    assert _q(f"SELECT year, yearMonth FROM lake.{sim} ORDER BY year") == [
        (2017, dt.date(2017, 5, 1)), (2020, dt.date(2020, 6, 1))]
    assert _q("SELECT count(*) FROM lake.cast_errors") == [(0,)]
    # file_stats time range comes from the pubFormat-parsed time column
    fs = _q("SELECT timeColumn, timeMin, timeMax FROM lake.file_stats WHERE lakeTable = ?", [hbp])[0]
    assert fs[0] == "collectDate" and fs[1].hour == 15 and fs[2].hour == 18
    fs = _q("SELECT timeColumn, timeMin FROM lake.file_stats WHERE lakeTable = ?", [sim])[0]
    assert fs[0] == "eventDate" and fs[1].year == 2017


def test_cast_failure_rule_still_applies_with_pub_format(site_env: Path) -> None:  # noqa: F811
    s = Site(site_env)
    pub = "20250101T000000Z"
    s.add(HBP, "2024-11", f"NEON.D06.{SITE}.{HBP}.variables.{pub}.csv", _vars("hbp_massdata", [
        ("uid", "string", "asIs"), ("collectDate", "dateTime", "yyyy-MM-dd'T'HH'Z'(floor)")]), "variables")
    rows = [[f"u{i}", "yesterday" if i < 3 else f"2024-11-14T{i % 24:02d}Z"] for i in range(100)]
    s.add(HBP, "2024-11", f"NEON.D06.{SITE}.{HBP}.hbp_massdata.2024-11.basic.{pub}.csv",
          _csv(["uid", "collectDate"], rows), "data")
    assert stack.main(SITE) == 0
    assert _types("dp1_10023_001__hbp_massdata")["collectDate"] == "VARCHAR"
    assert _q("SELECT columnName, targetType, failures, action FROM lake.cast_errors") == [
        ("collectDate", "TIMESTAMPTZ", 3, "kept VARCHAR")]
