from __future__ import annotations

import gzip
import hashlib
import json
import re

import duckdb
import pytest

from neon_ducklake import env, state
from neon_ducklake.avu import vocab

RID = "20260927T143926Z"
IR = "/iplant/home/tswetnam/neon-ducklake"


@pytest.fixture
def root(tmp_path, monkeypatch):
    r = tmp_path / "root"
    monkeypatch.setattr(env, "ROOT", r)
    monkeypatch.setattr(env, "API_DIR", r / "api")
    monkeypatch.setattr(env, "RUNS_DIR", r / "api/runs")
    monkeypatch.setattr(env, "LAKE_DIR", r / "ducklake")
    monkeypatch.setattr(env, "SITES_DIR", r / "sites")
    monkeypatch.setattr(env, "IRODS_ROOT", IR)
    monkeypatch.setattr(env, "repo_versions", lambda: {"neon-mcp": "a5949d2", "neon-ducklake": "abc1234"})
    monkeypatch.setattr(env, "tool_versions", lambda: {"duckdb": "1.5.5", "ducklake": "d8a1881e"})
    monkeypatch.setattr(state, "DB_PATH", tmp_path / "state.sqlite")
    run = r / "api/runs" / RID
    (run / "sealed/data_query").mkdir(parents=True)
    (run / "fetch_manifest.json").write_text(json.dumps({
        "runId": RID, "status": "complete", "startedAt": "2026-09-27T14:39:26+00:00",
        "finishedAt": "2026-09-27T16:10:00+00:00", "failureCount": 0,
        "counts": {"products": {"ok": 1}, "data_query": {"ok": 4000, "cached": 10}}}))
    with gzip.open(run / "sealed/products.jsonl.gz", "wt") as fh:
        fh.write("{}\n{}\n{}\n")
    with gzip.open(run / "sealed/data_query/DP1.00002.001.jsonl.gz", "wt") as fh:
        fh.write("{}\n{}")
    (run / "sealed/stage").mkdir()
    with gzip.open(run / "sealed/stage/pairs.json.gz", "wt") as fh:
        fh.write("[1, 2, 3, 4]")
    (run / "sealed/MANIFEST.json").write_text(json.dumps({
        "runId": RID, "sealVersion": 1, "sealedAt": "2026-09-27T16:20:00+00:00",
        "files": [{"file": "products.jsonl.gz", "lines": 3, "md5": "m" * 32, "family": "products"},
                  {"file": "stage/pairs.json.gz", "family": "stage", "lines": None}]}))
    (r / "src/neon_ducklake/avu").mkdir(parents=True)
    (r / "src/neon_ducklake/avu/x.py").write_text("")
    con = state.connect(tmp_path / "state.sqlite")
    con.execute("INSERT INTO requests(run_id, family, key, status, fetched_at) VALUES (?, 'products', 'products',"
                " 'ok', '2026-09-27T14:40:00+00:00')", (RID,))
    for key, at in (("DP1.00002.001__SRER__2017-01_2020-12", "2026-09-27T15:00:00+00:00"),
                    ("DP1.00002.001__HARV__2017-01_2020-12", "2026-09-27T15:05:00+00:00"),
                    ("DP1.00003.001__HARV__2017-01_2020-12", "2026-09-27T15:30:00+00:00")):
        con.execute("INSERT INTO requests(run_id, family, key, status, fetched_at) VALUES (?, 'data_query', ?,"
                    " 'ok', ?)", (RID, key, at))
    return r, con


def make_lake(r) -> None:
    lake = r / "ducklake"
    lake.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect()
    c.execute("LOAD ducklake")
    c.execute(f"ATTACH 'ducklake:{lake / 'neon_catalog.ducklake'}' AS neon (DATA_PATH '{lake / 'data'}/')")
    c.execute("CALL neon.set_option('data_inlining_row_limit', 0)")
    c.execute("CREATE TABLE neon.main.products AS SELECT range AS productCode FROM range(5)")
    c.execute("CREATE TABLE neon.main.files AS SELECT range AS size FROM range(3)")
    c.execute("DETACH neon")
    c.close()


def rows_for(rows, path):
    return {(a, v, u) for (_unit, p, _t, a, v, u, _l, _g) in rows if p == path}


def test_manifest_fallback_rows(root) -> None:
    r, _con = root
    rows, info = vocab.build_rows(RID)
    assert info["source"] == "manifest" and info["sealed_files"] == 3
    top = rows_for(rows, IR)
    assert ("dc.license", "CC-BY-4.0", "https://creativecommons.org/licenses/by/4.0/") in top
    assert ("dc.publisher", "National Ecological Observatory Network (NEON)", "") in top
    assert ("dc.creator", "tswetnam", "iRODS user") in top
    assert ("mesa.local_root", str(r), "sparky-1 path") in top
    assert ("mesa.ducklake.catalog", "ducklake/neon_catalog.ducklake", "relative path") in top
    assert ("provenance.derivedFrom", "/iplant/home/tswetnam/neon-downloads/ducklake/neon_spatial.ducklake@90",
            "iRODS path") in top
    assert ("provenance.tool", "neon-mcp a5949d2", "") in top
    assert ("neon.catalog.harvestRun", RID, "") in top
    assert ("neon.api.dataQueryRequests", "4010", "count") in top
    assert ("dc.date", "2026-09-27", "ISO8601") in top
    sealed = rows_for(rows, f"{IR}/api/runs/{RID}/sealed/products.jsonl.gz")
    assert {("neon.api.family", "products", ""), ("neon.api.endpoint", "GET /products", ""),
            ("neon.rowCount", "3", "count"), ("neon.md5", "m" * 32, ""),  # from sealed/MANIFEST.json
            ("neon.fetchedAt", "2026-09-27T14:40:00+00:00", "ISO8601")} <= sealed
    dq = f"{IR}/api/runs/{RID}/sealed/data_query/DP1.00002.001.jsonl.gz"
    md5 = hashlib.md5((r / f"api/runs/{RID}/sealed/data_query/DP1.00002.001.jsonl.gz").read_bytes()).hexdigest()
    assert {("neon.rowCount", "2", "count"), ("neon.api.endpoint", "POST /data/query", ""), ("neon.md5", md5, ""),
            ("neon.productCode", "DP1.00002.001", ""),
            ("neon.fetchedAt", "2026-09-27T15:05:00+00:00", "ISO8601")} <= rows_for(rows, dq)
    stage = rows_for(rows, f"{IR}/api/runs/{RID}/sealed/stage/pairs.json.gz")
    assert ("neon.rowCount", "4", "count") in stage and not [a for a, _v, _u in stage if a.startswith("neon.api")]
    smf = rows_for(rows, f"{IR}/api/runs/{RID}/sealed/MANIFEST.json")
    assert ("neon.sealedAt", "2026-09-27T16:20:00+00:00", "ISO8601") in smf
    assert ("neon.api.family", "data_query", "") in rows_for(rows, f"{IR}/api/runs/{RID}/sealed/data_query")
    # every target collection is its own unit; files belong to their parent's unit
    units = {(u, p) for (u, p, t, *_rest) in rows if t == "data_object"}
    assert all(u == p.rsplit("/", 1)[0] for u, p in units)
    assert rows_for(rows, f"{IR}/src/neon_ducklake/avu")
    assert rows_for(rows, f"{IR}/sites")


def test_lake_facts_and_files(root) -> None:
    r, _con = root
    make_lake(r)
    rows, info = vocab.build_rows(RID)
    assert info["source"] == "lake" and info["lake_files"] >= 2
    top = rows_for(rows, IR)
    assert ("neon.api.products", "5", "count") in top and ("neon.api.files", "3", "count") in top
    assert ("neon.api.fileBytes", "3", "byte") in top
    snap = [v for a, v, _u in top if a == "neon.catalog.snapshotId"]
    assert snap and int(snap[0]) >= 1
    cat = rows_for(rows, f"{IR}/ducklake/neon_catalog.ducklake")
    assert ("ducklake.snapshotId", snap[0], "") in cat
    assert rows_for(rows, f"{IR}/ducklake/data/main/products") >= {("mesa.ducklake.table", "products", ""),
                                                                      ("neon.rowCount", "5", "count")}
    pq_rows = [(p, a, v) for (_u, p, t, a, v, _un, _l, _g) in rows
               if t == "data_object" and p.startswith(f"{IR}/ducklake/data/main/products/")]
    assert ("mesa.ducklake.table", "products") in {(a, v) for _p, a, v in pq_rows}
    assert "5" in {v for _p, a, v in pq_rows if a == "neon.rowCount"}


def test_neon_namespace_is_lower_camel(root) -> None:
    r, _con = root
    make_lake(r)
    rows, _ = vocab.build_rows(RID)
    bad = sorted({a for (_u, _p, _t, a, *_r) in rows
                  if a.startswith("neon.") and not re.fullmatch(r"neon(\.[a-z][A-Za-z0-9]*)+", a)})
    assert bad == []


def test_build_catalog_desired_replaces_scope(root) -> None:
    _r, con = root
    info = vocab.build_catalog_desired(RID, con=con)
    n = con.execute("SELECT count(*) FROM avu_desired WHERE scope='catalog'").fetchone()[0]
    assert n == info["rows"] and info["units"] >= 6
    assert con.execute("SELECT count(*) FROM avu_desired WHERE value = '' OR attribute = ''").fetchone()[0] == 0
    info2 = vocab.build_catalog_desired(RID, con=con)
    assert con.execute("SELECT count(*) FROM avu_desired WHERE scope='catalog'").fetchone()[0] == info2["rows"]


def test_latest_run_and_camel(root) -> None:
    assert vocab.latest_run_id() == RID
    assert vocab.lower_camel("location_history") == "locationHistory"
    assert vocab.lower_camel("samples/supportedClasses") == "samplesSupportedClasses"
    assert vocab.family_of("data_query/DP1.00002.001.jsonl.gz") == "data_query"
    assert vocab.family_of("products.jsonl.gz") == "products"
