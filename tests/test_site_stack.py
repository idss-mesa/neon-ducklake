"""site stack on tiny NEON-shaped fixtures (IS + OS + HDF5), hermetic."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import duckdb
import numpy as np
import pytest

from neon_ducklake import env, state
from neon_ducklake.site import site_lake, stack

from .test_site_common import SITE, md5, site_env  # noqa: F401 - site_env is a fixture

IS = "DP1.00002.001"
OS = "DP1.10003.001"
EC = "DP4.00200.001"
LEGACY_SP = ["HOR.VER", "name", "description", "start", "end", "referenceName", "referenceDescription",
             "referenceStart", "referenceEnd", "xOffset", "yOffset", "zOffset", "pitch", "roll", "azimuth",
             "referenceLatitude", "referenceLongitude", "referenceElevation", "eastOffset", "northOffset", "xAzimuth",
             "yAzimuth"]
MODERN_SP = list(stack.SENSOR_COLUMNS)


def _csv(header: list[str], rows: list[list]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode()


class Site:
    """Writes files under sites/TEST/raw and journals them as downloaded."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.con = state.connect(state.DB_PATH)

    def add(self, product: str, month: str, name: str, body: bytes, kind: str, release: str = "RELEASE-2026") -> Path:
        rel = f"sites/{SITE}/raw/{product}/{month}/{name}"
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)
        self.con.execute(
            "INSERT OR REPLACE INTO site_files(site, product, month, name, release, kind, size, md5, local_path, "
            "dl_status, dl_md5, dl_bytes, md5_verified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (SITE, product, month, name, release, kind, len(body), md5(body), rel, "ok", md5(body), len(body), 1))
        return p


def _variables_old() -> bytes:
    h = ["table", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat"]
    fmt = "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)"
    return _csv(h, [
        ["SAAT_30min", "startDateTime", "start", "dateTime", "NA", "basic", fmt],
        ["SAAT_30min", "endDateTime", "end", "dateTime", "NA", "basic", fmt],
        ["SAAT_30min", "tempSingleMean", "mean", "real", "celsius", "basic", "*.####(round)"],
        ["SAAT_30min", "tempSingleNumPts", "n", "integer", "number", "basic", "integer"],
        ["SAAT_30min", "finalQF", "qf", "unsigned integer", "NA", "basic", "integer"],
        ["SAAT_30min", "flagText", "text-ish", "real", "NA", "basic", "asIs"],
        ["SAAT_30min", "obsDate", "a date", "dateTime", "NA", "basic", "yyyy-MM-dd"],
        ["sensor_positions", "HOR.VER", "hv", "string", "NA", "basic", "asIs"]])


def _variables_new() -> bytes:
    h = ["table", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat", "primaryKey",
         "categoricalCodeName"]
    fmt = "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)"
    return _csv(h, [
        ["SAAT_30min", "startDateTime", "start", "dateTime", "NA", "basic", fmt, "N", ""],
        ["SAAT_30min", "endDateTime", "end", "dateTime", "NA", "basic", fmt, "N", ""],
        ["SAAT_30min", "tempSingleMean", "mean", "real", "celsius", "basic", "*.####(round)", "N", ""],
        ["SAAT_30min", "tempSingleNumPts", "n", "real", "number", "basic", "*.####(round)", "N", ""],
        ["SAAT_30min", "finalQF", "qf", "unsigned integer", "NA", "basic", "integer", "N", ""],
        ["SAAT_30min", "flagText", "text-ish", "real", "NA", "basic", "asIs", "N", ""],
        ["SAAT_30min", "obsDate", "a date", "dateTime", "NA", "basic", "yyyy-MM-dd", "N", ""]])


def _sp_row(hv: str, loc: str) -> list:
    return [hv, loc, "desc", "2010-01-01T00:00:00Z", "", "TOWER1", "tower", "2010-01-01T00:00:00Z", "", "2.31", "5.35",
            "0.25", "0", "0", "0", "31.9", "-110.8", "997.48", "4.24", "-4.00", "200", "110"]


def build_fixture(root: Path) -> Site:
    s = Site(root)
    # ---- IS product: 2 months, header drift, cast failures
    s.add(IS, "2019-05", f"NEON.D99.{SITE}.{IS}.variables.20230220T165115Z.csv", _variables_old(), "variables")
    s.add(IS, "2024-06", f"NEON.D99.{SITE}.{IS}.variables.20240706T162340Z.csv", _variables_new(), "variables")
    h1 = ["startDateTime", "endDateTime", "tempSingleMean", "tempSingleNumPts", "finalQF", "flagText", "obsDate"]
    rows1 = [[f"2019-05-01T00:{i:02d}:00Z", f"2019-05-01T00:{i + 1:02d}:00Z", f"{20 + i}.5", "1800.0000",
              str(i % 2), "abc", "2019-05-01"] for i in range(4)]
    s.add(IS, "2019-05", f"NEON.D99.{SITE}.{IS}.000.010.030.SAAT_30min.2019-05.basic.20230220T165115Z.csv",
          _csv(h1, rows1), "data")
    h2 = ["endDateTime", "startDateTime", "tempSingleMean", "tempSingleNumPts", "finalQF", "flagText", "obsDate",
          "newCol"]
    rows2 = [[f"2024-06-02T{i // 60 % 24:02d}:{i % 60:02d}:00Z", f"2024-06-01T{i // 60 % 24:02d}:{i % 60:02d}:00Z",
              "bad" if i == 7 else f"{i / 10:.1f}", "1800", "1" if i < 20 else "0", "1.5" if i % 3 else "x",
              "2024-06-01", "n"] for i in range(2000)]
    s.add(IS, "2024-06", f"NEON.D99.{SITE}.{IS}.000.020.030.SAAT_30min.2024-06.basic.20240706T162340Z.csv",
          _csv(h2, rows2), "data")
    s.add(IS, "2019-05", f"NEON.D99.{SITE}.{IS}.sensor_positions.20230220T165115Z.csv",
          _csv(LEGACY_SP, [_sp_row("000.010", "CFGLOC1")]), "sensor_positions")
    s.add(IS, "2024-06", f"NEON.D99.{SITE}.{IS}.sensor_positions.20240706T162340Z.csv",
          _csv(MODERN_SP, [_sp_row("000.020", "CFGLOC2"), _sp_row("000.010", "CFGLOC1")]), "sensor_positions")
    s.add(IS, "2024-06", f"NEON.D99.{SITE}.{IS}.EML.20240601-20240701.20260123T000749Z.xml", b"<eml/>", "eml")
    s.add(IS, "2024-06", f"NEON.D99.{SITE}.{IS}.readme.20260123T000749Z.txt", b"readme", "readme")
    # ---- OS product: uid duplicates across publications, DP0 support files
    vh = ["table", "fieldName", "description", "dataType", "units", "downloadPkg", "pubFormat", "primaryKey",
          "categoricalCodeName"]
    ofmt = "yyyy-MM-dd'T'HH:mm'Z'(floor)"
    ovars = _csv(vh, [["brd_countdata", f, "d", t, "NA", "basic", pf, "N", ""] for f, t, pf in [
        ("uid", "string", "asIs"), ("plotID", "string", "asIs"), ("startDate", "dateTime", ofmt),
        ("taxonID", "string", "asIs"), ("scientificName", "string", "asIs"), ("taxonRank", "string", "asIs"),
        ("vernacularName", "string", "asIs"), ("clusterSize", "unsigned integer", "integer"),
        ("samplingProtocolVersion", "string", "asIs"), ("siteID", "string", "asIs")]])
    oh = ["uid", "plotID", "startDate", "taxonID", "scientificName", "taxonRank", "vernacularName", "clusterSize",
          "samplingProtocolVersion", "siteID"]
    cats = _csv(["name", "pubCode", "description", "startDate", "endDate"],
                [["brd.sex", "M", "male", "2012-01-01T00:00:00Z", ""], ["brd.sex", "F", "female", "2012-01-01T00:00:00Z", ""]])
    for month, pub, rows in [
        ("2023-05", "20240101T000000Z", [["u1", "P_001", "2023-05-02T12:05Z", "MODO", "Zenaida macroura", "species",
                                          "Mourning Dove", "1", "v1", SITE],
                                         ["u2", "P_002", "2023-05-03T12:05Z", "CACW", "Campylorhynchus", "species",
                                          "Cactus Wren", "2", "v1", SITE]]),
        ("2024-05", "20250101T000000Z", [["u1", "P_001", "2023-05-02T12:05Z", "MODO", "Zenaida macroura", "species",
                                          "Mourning Dove", "5", "v2", SITE],
                                         ["u3", "P_003", "2024-05-04T12:05Z", "", "", "", "", "1", "v2", SITE]])]:
        s.add(OS, month, f"NEON.D99.{SITE}.{OS}.variables.{pub}.csv", ovars, "variables")
        s.add(OS, month, f"NEON.D99.{SITE}.{OS}.brd_countdata.{month}.basic.{pub}.csv", _csv(oh, rows), "data")
        s.add(OS, month, f"NEON.D99.{SITE}.DP0.10003.001.categoricalCodes.{pub}.csv", cats, "categorical_codes")
        s.add(OS, month, f"NEON.D99.{SITE}.DP0.10003.001.validation.{pub}.csv",
              _csv(["table", "fieldName", "parserToCreate"], [["brd_countdata", "uid", "[CREATE_UID]"]]), "validation")
    return s


def make_h5(path: Path, *, with_dp04: bool = True) -> bytes:
    import h5py

    path.parent.mkdir(parents=True, exist_ok=True)
    tb = np.array([b"2024-06-01T00:00:00.000Z", b"2024-06-01T00:30:00.000Z", b"2024-06-01T01:00:00.000Z"])
    te = np.array([b"2024-06-01T00:29:59.950Z", b"2024-06-01T00:59:59.950Z", b"2024-06-01T01:29:59.950Z"])
    with h5py.File(path, "w") as f:
        f.attrs["SciVer"] = np.array([b"2.2.0"])
        g = f.create_group(SITE)
        g.attrs["LatTow"] = np.array([31.9])
        g.attrs["DistZaxsLvlMeasTow"] = np.array([b"0.60", b"1.87"])
        f.create_dataset("readMe", data=np.array([b"hello"]))
        if with_dp04:
            flux_t = np.dtype([("timeBgn", "S24"), ("timeEnd", "S24"), ("flux", "<f8")])
            qf_t = np.dtype([("timeBgn", "S24"), ("timeEnd", "S24"), ("qfFinl", "<i4")])
            foot_t = np.dtype([("timeBgn", "S24"), ("timeEnd", "S24"), ("veloFric", "<f8"), ("distXaxs90", "<f8")])
            d = g.create_dataset("dp04/data/fluxCo2/turb", data=np.array(
                list(zip(tb, te, [1.5, np.nan, -2.0], strict=True)), dtype=flux_t))
            d.attrs["unit"] = np.array([b"NA", b"NA", b"umolCo2 m-2 s-1"])
            g.create_dataset("dp04/qfqm/fluxCo2/turb", data=np.array(list(zip(tb, te, [0, 1, 0], strict=True)),
                                                                     dtype=qf_t))
            g.create_dataset("dp04/data/foot/stat", data=np.array(
                list(zip(tb, te, [0.3, 0.4, 0.5], [100.0, 110.0, 120.0], strict=True)), dtype=foot_t))
            g.create_dataset("dp04/qfqm/foot/turb", data=np.array(list(zip(tb, te, [0, 0, 1], strict=True)),
                                                                  dtype=qf_t))
    return path.read_bytes()


def _add_h5(s: Site, root: Path) -> None:
    for month, dp04 in (("2024-06", True), ("2024-07", False)):
        name = f"NEON.D99.{SITE}.{EC}.nsae.{month}.basic.20260115T171049Z.h5"
        tmp = root / "tmp_h5" / name
        s.add(EC, month, name, make_h5(tmp, with_dp04=dp04), "data")


def _catalog_lake(root: Path) -> None:
    """A minimal read-only catalog lake with locations + location_history."""
    lake = env.LAKE_DIR
    lake.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{lake / env.CATALOG_LAKE}' AS neon (DATA_PATH '{lake / 'data'}/')")
    con.execute("CREATE TABLE neon.locations AS SELECT * FROM (VALUES ('CFGLOC1', 'CONFIG', 31.9::DOUBLE, "
                "-110.8::DOUBLE, 998.0::DOUBLE)) "
                "t(locationName, locationType, locationDecimalLatitude, locationDecimalLongitude, locationElevation)")
    con.execute("CREATE TABLE neon.location_history AS SELECT * FROM (VALUES ('CFGLOC1', "
                "TIMESTAMPTZ '2010-01-01 00:00:00+00', NULL::TIMESTAMPTZ, 31.95::DOUBLE, -110.85::DOUBLE, "
                "999.0::DOUBLE)) t(locationName, "
                "locationStartDate, locationEndDate, locationDecimalLatitude, locationDecimalLongitude, "
                "locationElevation)")
    con.execute("DETACH neon")
    con.close()


def _q(sql: str, params: list | None = None) -> list[tuple]:
    con = site_lake.open_site_lake(SITE, read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def _types(table: str) -> dict[str, str]:
    return dict(_q("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name='lake' "
                   "AND table_name = ?", [table]))


def test_cast_expression_rules() -> None:
    assert stack.cast_for("x", "real", None)[1] == "DOUBLE"
    assert stack.cast_for("x", "signed integer", "integer")[1] == "BIGINT"
    assert stack.cast_for("x", "dateTime", "yyyy-MM-dd'T'HH:mm'Z'(floor)")[1] == "TIMESTAMPTZ"
    assert stack.cast_for("x", "dateTime", "yyyy-MM-dd (floor)")[1] == "DATE"
    assert stack.cast_for("x", "uri", "asIs")[1] == "VARCHAR"
    assert stack.lake_table_name("DP1.00002.001", "SAAT_30min") == "dp1_00002_001__saat_30min"
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    e, _ = stack.cast_for("x", "integer", None, qual="")
    assert con.execute(f"SELECT {e} FROM (VALUES ('1800.0000'), ('3'), ('1.5'), ('z')) t(x)").fetchall() == [
        (1800,), (3,), (None,), (None,)]
    e, _ = stack.cast_for("x", "dateTime", None, qual="")
    got = con.execute(f"SELECT CAST({e} AS VARCHAR) FROM (VALUES ('2024-05-02T12:05Z'), ('2024-05-02T12:05:07Z'), "
                      "('2024-05-02')) t(x)").fetchall()
    assert [g[0][:19] for g in got] == ["2024-05-02 12:05:00", "2024-05-02 12:05:07", "2024-05-02 00:00:00"]


def test_stack_end_to_end(site_env: Path) -> None:  # noqa: F811
    s = build_fixture(site_env)
    _add_h5(s, site_env)
    _catalog_lake(site_env)
    assert stack.main(SITE) == 0
    saat = "dp1_00002_001__saat_30min"
    t = _types(saat)
    assert t["startDateTime"] == "TIMESTAMP WITH TIME ZONE" and t["obsDate"] == "DATE"
    assert t["tempSingleMean"] == "DOUBLE" and t["finalQF"] == "BIGINT"
    assert t["tempSingleNumPts"] == "DOUBLE"  # latest variables definition (real) wins over the old integer
    assert t["flagText"] == "VARCHAR" and t["newCol"] == "VARCHAR"
    for c in ("siteID", "domainID", "month", "release", "publicationDate", "sourceFile", "horizontalPosition",
              "verticalPosition", "tmi"):
        assert c in t
    assert _q(f"SELECT count(*), count(DISTINCT sourceFile) FROM lake.{saat}") == [(2004, 2)]
    assert _q(f"SELECT count(*) FROM lake.{saat} WHERE tempSingleMean IS NULL") == [(1,)]
    assert _q(f"SELECT DISTINCT horizontalPosition, verticalPosition, tmi FROM lake.{saat} ORDER BY 2") == [
        ("000", "010", "030"), ("000", "020", "030")]
    errs = {r[0]: r[1:] for r in _q("SELECT columnName, action, failures, nonNull FROM lake.cast_errors")}
    assert errs["flagText"][0] == "kept VARCHAR"
    assert errs["tempSingleMean"] == ("cast; failures set NULL", 1, 2004)
    fs = {r[0]: r[1:] for r in _q("SELECT sourceFile, rowCount, timeMin, qfFailPct, hor, ver FROM lake.file_stats "
                                  "WHERE tableName='SAAT_30min'")}
    assert sorted(v[0] for v in fs.values()) == [4, 2000]
    two = next(v for v in fs.values() if v[0] == 2000)
    assert two[2] == pytest.approx(1.0) and two[1].year == 2024 and two[4] == "020"
    drift = _q("SELECT nFiles, missingVsUnion, notInVariables FROM lake.schema_drift WHERE tableName='SAAT_30min' "
               "ORDER BY nColumns")
    assert drift == [(1, ["newCol"], []), (1, [], ["newCol"])]
    # variables: latest per field, history keeps both definitions
    assert _q("SELECT dataType, primaryKey FROM lake.variables WHERE productCode=? AND fieldName='tempSingleNumPts'",
              [IS]) == [("real", "N")]
    assert _q("SELECT count(*) FROM lake.variables_history WHERE productCode=? AND fieldName='tempSingleNumPts'",
              [IS]) == [(2,)]
    # sensor positions: legacy aliased, positions computed, catalog joins applied
    sp = _q('SELECT month, "HOR.VER", sensorLocationID, positionStartDateTime, sensorLatitude, sensorLongitude, '
            'sensorElevation, locationType, historyLatitude FROM lake.sensor_positions ORDER BY month, 2')
    assert len(sp) == 3 and sp[0][:3] == ("2019-05", "000.010", "CFGLOC1")
    assert sp[0][4] == pytest.approx(31.9 - 4.0 / 111320) and sp[0][6] == pytest.approx(997.73)
    assert sp[0][5] == pytest.approx(-110.8 + 4.24 / (111320 * np.cos(np.radians(31.9))))
    assert sp[0][7] == "CONFIG" and sp[0][8] == pytest.approx(31.95)
    assert sp[2][2] == "CFGLOC2" and sp[2][7] is None  # unknown to the catalog
    # OS: uid dedup keeps the newest publication; stats are pre-dedup
    bird = "dp1_10003_001__brd_countdata"
    assert _q(f"SELECT uid, clusterSize, samplingProtocolVersion FROM lake.{bird} ORDER BY uid") == [
        ("u1", 5, "v2"), ("u2", 2, "v1"), ("u3", 1, "v2")]
    assert _types(bird)["startDate"] == "TIMESTAMP WITH TIME ZONE" and _types(bird)["clusterSize"] == "BIGINT"
    assert "siteID" in _types(bird) and _types(bird)["publicationDate"] == "TIMESTAMP WITH TIME ZONE"
    ost = _q("SELECT month, rowCount, plotIDs, plotCount, protocolVersions, taxonCount FROM lake.file_stats "
             "WHERE productCode=? ORDER BY month", [OS])
    assert ost == [("2023-05", 2, ["P_001", "P_002"], 2, ["v1"], 2), ("2024-05", 2, ["P_001", "P_003"], 2, ["v2"], 1)]
    assert _q("SELECT taxonID, nRecords FROM lake.file_taxa WHERE productCode=? ORDER BY sourceFile, taxonID",
              [OS]) == [("CACW", 1), ("MODO", 1), ("MODO", 1)]
    assert _q("SELECT dedupRemoved, rowsBeforeDedup FROM lake.table_catalog WHERE tableName=?", [bird]) == [(1, 4)]
    assert _q("SELECT code, nFiles FROM (SELECT pubCode AS code, nFiles FROM lake.categorical_codes) ORDER BY 1") == [
        ("F", 2), ("M", 2)]
    assert _q("SELECT productCode, count(*) FROM lake.validation GROUP BY 1") == [(OS, 1)]
    assert _q("SELECT emlStart, emlEnd FROM lake.eml_index") [0][0].isoformat() == "2024-06-01"
    # HDF5
    assert _q("SELECT count(*), sum(qfFinlCo2Turb), count(fluxCo2Turb) FROM lake.dp4_00200_001__nsae_dp04") == [
        (3, 1, 2)]
    assert _q("SELECT footVeloFric FROM lake.dp4_00200_001__nsae_dp04 ORDER BY timeBgn") == [(0.3,), (0.4,), (0.5,)]
    h5s = dict(_q("SELECT month, rowCount FROM lake.file_stats WHERE tableName='nsae'"))
    assert h5s == {"2024-06": 3, "2024-07": None}
    assert _q("SELECT count(*) FROM lake.h5_attrs WHERE attr='DistZaxsLvlMeasTow' AND value='0.60,1.87'") == [(2,)]
    assert _q("SELECT unit FROM lake.h5_datasets WHERE path=?", [f"/{SITE}/dp04/data/fluxCo2/turb"]) == [
        ("NA,NA,umolCo2 m-2 s-1",)]
    # re-run: every unit unchanged -> no new snapshot
    n = _q("SELECT count(*) FROM lake.snapshots()")[0][0]
    assert stack.main(SITE) == 0
    assert _q("SELECT count(*) FROM lake.snapshots()")[0][0] == n
    assert not site_lake.site_paths(SITE).wal_file.exists()
    assert not (site_lake.site_paths(SITE).tmp_dir / "stack_stage.duckdb").exists()
    # force rebuilds and the product filter limits the scope
    assert stack.main(SITE, products=[OS], force=True) == 0
    new = _q("SELECT commit_message FROM lake.snapshots() ORDER BY snapshot_id")[n:]
    assert [r[0] for r in new] == [f"stack {SITE} {OS} supporting", f"stack {SITE} {OS} brd_countdata"]


EVOLVED_SP = [*MODERN_SP[:3], "effectiveStartDateTime", "effectiveEndDateTime", *MODERN_SP[3:]]


def _evolved_row(hv: str, loc: str, eff_start: str, eff_end: str) -> list:
    r = _sp_row(hv, loc)
    return [*r[:3], eff_start, eff_end, *r[3:]]


def test_sensor_positions_accepts_additional_columns(site_env: Path) -> None:  # noqa: F811
    """F4: new columns (effectiveStart/EndDateTime, 2026-09) are unioned by name and recorded as drift."""
    s = build_fixture(site_env)
    reports = site_lake.site_paths(SITE).reports_dir
    reports.mkdir(parents=True, exist_ok=True)
    stale = reports / f"sensor_positions_unknown_headers_{IS}.json"
    stale.write_text("{}")  # left by an older stacker; removed once the unit builds
    s.add(IS, "2024-07", f"NEON.D99.{SITE}.{IS}.sensor_positions.20260915T213331Z.csv",
          _csv([*EVOLVED_SP, "mysteryColumn"],
               [[*_evolved_row("000.010", "CFGLOC1", "2010-01-01T00:00:00Z", "2019-01-02T23:59:59Z"), "?"],
                [*_evolved_row("000.010", "CFGLOC1", "2019-01-03T00:00:00Z", ""), ""]]), "sensor_positions")
    assert stack.main(SITE) == 0
    t = _types("sensor_positions")
    assert t["effectiveStartDateTime"] == "TIMESTAMP WITH TIME ZONE"
    assert t["effectiveEndDateTime"] == "TIMESTAMP WITH TIME ZONE" and t["mysteryColumn"] == "VARCHAR"
    rows = _q('SELECT month, effectiveStartDateTime, effectiveEndDateTime, mysteryColumn, sensorLatitude '
              'FROM lake.sensor_positions ORDER BY month, effectiveStartDateTime NULLS FIRST')
    assert len(rows) == 5  # 1 legacy + 2 modern + 2 evolved (both evolved rows kept: same position start)
    assert [r[0] for r in rows] == ["2019-05", "2024-06", "2024-06", "2024-07", "2024-07"]
    assert rows[0][1] is None and rows[3][1].year == 2010 and rows[3][2].year == 2019 and rows[4][2] is None
    assert rows[3][3] == "?" and rows[4][3] is None and rows[3][4] == pytest.approx(31.9 - 4.0 / 111320)
    drift = _q("SELECT nFiles, unknownColumns, missingVsUnion, firstMonth, lastMonth, firstPublication, "
               "lastPublication FROM lake.schema_drift WHERE productCode = ? AND tableName = 'sensor_positions' "
               "ORDER BY nColumns", [IS])
    assert len(drift) == 3  # legacy, modern and evolved header groups
    assert drift[-1][:2] == (1, ["effectiveStartDateTime", "effectiveEndDateTime", "mysteryColumn"])
    assert drift[-1][3:5] == ("2024-07", "2024-07") and drift[-1][5].year == 2026 == drift[-1][6].year
    assert drift[0][1] == [] and "effectiveStartDateTime" in drift[0][2]  # legacy group lacks the new ones
    assert not stale.exists()
    totals = json.loads((reports / "stack_summary.json").read_text())["totals"]
    assert totals["sensorPositionsExtraColumns"][IS] == ["effectiveStartDateTime", "effectiveEndDateTime",
                                                         "mysteryColumn"]
    assert totals["unitErrors"] == 0 and totals["unitsBuilt"] > 0


def test_sensor_positions_missing_required_is_a_hard_error(site_env: Path) -> None:  # noqa: F811
    s = build_fixture(site_env)
    cols = [c for c in MODERN_SP if c != "locationReferenceLatitude"]
    row = [v for c, v in zip(MODERN_SP, _sp_row("000.010", "CFGLOC1"), strict=True) if c != "locationReferenceLatitude"]
    s.add(IS, "2024-07", f"NEON.D99.{SITE}.{IS}.sensor_positions.20240806T000000Z.csv", _csv(cols, [row]),
          "sensor_positions")
    assert stack.main(SITE) == 2
    report = site_lake.site_paths(SITE).reports_dir / f"sensor_positions_missing_required_{IS}.json"
    assert "locationReferenceLatitude" in report.read_text()
    # the data tables were still built; the supporting unit was not
    assert _q("SELECT count(*) FROM lake.dp1_00002_001__saat_30min") == [(2004,)]
    assert not _q("SELECT * FROM lake.table_catalog WHERE tableName='sensor_positions'")


def test_sensor_positions_name_collisions(site_env: Path) -> None:  # noqa: F811
    """A legacy alias next to its modern name, and a NEON column named like a derived one, both survive."""
    s = Site(site_env)
    cols = [*MODERN_SP, "name", "sensorLatitude"]
    s.add(IS, "2024-06", f"NEON.D99.{SITE}.{IS}.sensor_positions.20240706T162340Z.csv",
          _csv(cols, [[*_sp_row("000.010", "CFGLOC1"), "legacy-name", "12.5"]]), "sensor_positions")
    assert stack.main(SITE) == 0
    got = _q('SELECT sensorLocationID, "name", sensorLatitude_neon, sensorLatitude FROM lake.sensor_positions')
    assert got[0][:3] == ("CFGLOC1", "legacy-name", "12.5") and got[0][3] == pytest.approx(31.9 - 4.0 / 111320)


def test_changed_input_rebuilds_only_that_table(site_env: Path) -> None:  # noqa: F811
    s = build_fixture(site_env)
    assert stack.main(SITE) == 0
    n = _q("SELECT count(*) FROM lake.snapshots()")[0][0]
    oh = ["uid", "plotID", "startDate", "taxonID", "scientificName", "taxonRank", "vernacularName", "clusterSize",
          "samplingProtocolVersion", "siteID"]
    s.add(OS, "2025-05", f"NEON.D99.{SITE}.{OS}.brd_countdata.2025-05.basic.20260101T000000Z.csv",
          _csv(oh, [["u9", "P_009", "2025-05-02T12:05Z", "MODO", "Zenaida macroura", "species", "Mourning Dove",
                     "1", "v3", SITE]]), "data")
    assert stack.main(SITE) == 0
    new = [r[0] for r in _q("SELECT commit_message FROM lake.snapshots() ORDER BY snapshot_id")[n:]]
    assert new == [f"stack {SITE} {OS} brd_countdata"]
    assert _q("SELECT count(*) FROM lake.dp1_10003_001__brd_countdata") == [(4,)]


def test_ragged_csv_is_read_leniently(site_env: Path) -> None:  # noqa: F811
    s = Site(site_env)
    body = b"uid,plotID,startDate\n\"u1\",\"P_001\",\"2024-05-02T12:05Z\"\n\"u2\",\"P_002\"\n"
    s.add(OS, "2024-05", f"NEON.D99.{SITE}.{OS}.brd_perpoint.2024-05.basic.20250101T000000Z.csv", body, "data")
    assert stack.main(SITE) == 0
    assert _q("SELECT uid, plotID, startDate IS NULL FROM lake.dp1_10003_001__brd_perpoint ORDER BY uid") == [
        ("u1", "P_001", False), ("u2", "P_002", True)]  # u2 padded with NULL; no variables -> VARCHAR
    assert _q("SELECT count(*) FROM lake.dp1_10003_001__brd_perpoint WHERE startDate = '2024-05-02T12:05Z'") == [(1,)]
