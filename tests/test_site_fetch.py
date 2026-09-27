"""site list/download against a scripted fake NeonClient (hermetic)."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from neon_ducklake import env, state
from neon_ducklake.site import fetch, site_lake

from .test_site_common import (  # noqa: F401 - site_env is a fixture
    SITE,
    FakeBundle,
    FakeClient,
    md5,
    neon_file,
    query_payload,
    site_env,
    write_gz,
)

RUN = "20260101T000000Z"
SAAT = "NEON.D99.TEST.DP1.00002.001.000.010.030.SAAT_30min.{m}.basic.20250101T000000Z.csv"
BODIES: dict[str, bytes] = {}


def _body(name: str) -> bytes:
    return BODIES.setdefault(name, f"content of {name}\n".encode())


def _files_for(month: str) -> list[dict]:
    names = [SAAT.format(m=month), "NEON.D99.TEST.DP1.00002.001.variables.20250101T000000Z.csv"]
    out = [neon_file(n, _body(n)) for n in names]
    eml = f"NEON.D99.TEST.DP1.00002.001.EML.{month.replace('-', '')}01-x.20250101T000000Z.xml"
    out.append(neon_file(eml, _body(eml), size=False, md5_ok=month != "2024-06"))  # soft MD5 mismatch in 2024-06
    return out


def _stage(root: Path) -> None:
    run = root / "api" / "runs" / RUN
    pairs = [
        {"product": "DP1.00002.001", "site": SITE, "start": "2024-05", "end": "2024-07", "release": None,
         "months": ["2024-05", "2024-06", "2024-07"], "scope": "current"},
        {"product": "DP1.10003.001", "site": SITE, "start": "2024-06", "end": "2024-06", "release": None,
         "months": ["2024-06"], "scope": "current"},
        {"product": "DP1.00006.001", "site": SITE, "start": "2019-01", "end": "2019-02", "release": "RELEASE-2025",
         "months": ["2019-01", "2019-02"], "scope": "release-only"},
        {"product": "DP1.00033.001", "site": SITE, "start": "2024-06", "end": "2024-07", "release": None,
         "months": ["2024-06", "2024-07"], "scope": "current"},
        {"product": "DP1.00002.001", "site": "OTHR", "start": "2024-05", "end": "2024-05", "release": None,
         "months": ["2024-05"], "scope": "current"},
    ]
    write_gz(run / "stage" / "pairs.json.gz", pairs)
    write_gz(run / "raw" / "products" / "products.json.gz", [
        {"productCode": "DP1.00033.001", "productName": "Phenology images", "productScienceTeamAbbr": "TIS",
         "siteCodes": [{"siteCode": SITE, "availableMonths": ["2024-06", "2024-07"],
                        "availableReleases": [{"release": "PROVISIONAL", "availableMonths": ["2024-06", "2024-07"]}]}]}])
    write_gz(run / "raw" / "external" / f"DP1.00033.001__{SITE}__2024-07.json.gz",
             {"externalData": [{"name": "Phenocam", "type": "Phenocam Website", "url": "https://phenocam.example/x"}]})


def _client() -> FakeClient:
    c = FakeClient()
    c.responses["DP1.00002.001|2024-05|2024-07|"] = query_payload("DP1.00002.001", [
        ("RELEASE-2026", "2026-01-23T00:07:49Z", [("2024-05", _files_for("2024-05")), ("2024-06", _files_for("2024-06"))]),
        ("PROVISIONAL", None, [("2024-07", _files_for("2024-07"))])])
    bird = ["NEON.D99.TEST.DP1.10003.001.brd_countdata.2024-06.basic.20250101T000000Z.csv",
            "NEON.D99.TEST.DP0.10003.001.validation.20250101T000000Z.csv"]
    c.responses["DP1.10003.001|2024-06|2024-06|"] = query_payload(
        "DP1.10003.001", [("RELEASE-2026", "2026-01-23T00:07:49Z", [("2024-06", [neon_file(n, _body(n)) for n in bird])])])
    pre = "NEON.D99.TEST.DP1.00006.001.000.900.030.PRIPRE_30min.{m}.basic.20241201T000000Z.csv"
    precip = {m: [neon_file(pre.format(m=m), _body(pre.format(m=m)))] for m in ("2019-01", "2019-02", "2019-03")}
    c.responses["DP1.00006.001|2019-01|2019-02|RELEASE-2025"] = query_payload(
        "DP1.00006.001", [("RELEASE-2025", "2025-01-29T00:00:00Z", [(m, fs) for m, fs in precip.items()])])
    return c


@pytest.fixture
def fake(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> FakeBundle:  # noqa: F811
    _stage(site_env)
    lc = _client()
    dc = FakeClient(blobs={f"https://storage.googleapis.com/bucket/{n}": b for n, b in BODIES.items()})
    bundle = FakeBundle(lc, dc)
    monkeypatch.setattr(env, "neon_bundle", lambda roles=(), **kw: bundle)
    monkeypatch.setattr(fetch, "RETRY_DELAYS", [0, 0, 0, 0, 0])
    monkeypatch.setattr(fetch.random, "uniform", lambda a, b: 0.0)
    return bundle


def _rows(sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    con = state.connect(state.DB_PATH)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def test_helpers() -> None:
    assert fetch.month_runs(["2024-02", "2024-01", "2024-04", "2023-12"]) == [("2023-12", "2024-02"),
                                                                              ("2024-04", "2024-04")]
    assert fetch.max_bytes_for(None) == 64 << 20 and fetch.max_bytes_for(0) == 64 << 20
    assert fetch.max_bytes_for(1000) == 1100 + (1 << 20)
    pairs = [{"product": "A", "months": ["2024-01", "2024-02", "2024-05"], "start": "2024-01", "end": "2024-05"}]
    got = fetch.filter_pairs(pairs, ["A"], ["2024-02", "2024-05", "2030-01"])
    assert got[0]["months"] == ["2024-02", "2024-05"] and fetch.query_windows(got[0]) == [("2024-02", "2024-02"),
                                                                                          ("2024-05", "2024-05")]
    assert fetch.query_windows(fetch.filter_pairs(pairs, None, None)[0]) == [("2024-01", "2024-05")]
    assert fetch.filter_pairs(pairs, ["B"], None) == []


def test_list_writes_journal_and_lake(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    assert fetch.main("list", SITE) == 0
    rows = {(r["product"], r["month"], r["name"]): r for r in _rows("SELECT * FROM site_files WHERE site=?", (SITE,))}
    assert len(rows) == 9 + 2 + 2  # DP1.00002 3x3, birds 2, precip release-only 2 (2019-03 filtered out)
    val = rows[("DP1.10003.001", "2024-06", "NEON.D99.TEST.DP0.10003.001.validation.20250101T000000Z.csv")]
    assert val["kind"] == "validation"
    assert val["local_path"].startswith(f"sites/{SITE}/raw/DP1.10003.001/2024-06/NEON.D99.TEST.DP0.")
    pre = [r for k, r in rows.items() if k[0] == "DP1.00006.001"]
    assert {r["month"] for r in pre} == {"2019-01", "2019-02"}
    assert all(r["release"] == "RELEASE-2025" and r["release_only"] == 1 for r in pre)
    assert all("X-Goog-Signature" in r["url"] and r["listed_at"] for r in rows.values())
    eml = [r for r in rows.values() if r["kind"] == "eml"]
    assert eml and all(r["size"] is None for r in eml)
    prov = [r for r in rows.values() if r["month"] == "2024-07"]
    assert all(r["release"] == "PROVISIONAL" for r in prov)
    listings = _rows("SELECT product, part, status FROM listings WHERE site=? ORDER BY 1, 2", (SITE,))
    assert [tuple(r) for r in listings] == [
        ("DP1.00002.001", "2024-05_2024-07", "ok"), ("DP1.00006.001", "2019-01_2019-02__RELEASE-2025", "ok"),
        ("DP1.00033.001", "external__2024-07", "ok"), ("DP1.10003.001", "2024-06_2024-06", "ok")]
    # the external product was not queried; its landing was reused (no GET)
    assert not [c for c in fake.list_client.calls if "DP1.00033.001" in c[1]]
    con = site_lake.open_site_lake(SITE, read_only=True)
    try:
        assert con.execute("SELECT count(*) FROM lake.files").fetchone()[0] == 13
        assert con.execute("SELECT productCode FROM lake.files WHERE kind='validation'").fetchall() == [
            ("DP1.10003.001",)]
        ext = con.execute("SELECT productCode, url, name, monthProbed, months FROM lake.external_links").fetchall()
        assert ext == [("DP1.00033.001", "https://phenocam.example/x", "Phenocam", "2024-07", ["2024-06", "2024-07"])]
        sm = dict(con.execute("SELECT month, release FROM lake.site_months WHERE productCode='DP1.00033.001'"
                              ).fetchall())
        assert sm == {"2024-06": "PROVISIONAL", "2024-07": "PROVISIONAL"}
        assert con.execute("SELECT fileCount, isProvisional FROM lake.site_months WHERE productCode='DP1.00002.001' "
                           "AND month='2024-07'").fetchone() == (3, True)
        assert con.execute("SELECT count(*) FROM lake.snapshots() WHERE author='tswetnam'").fetchone()[0] == 1
    finally:
        con.close()
    # signed URLs never reach the lake
    for p in site_lake.site_paths(SITE).lake_dir.rglob("*"):
        if p.is_file():
            assert b"X-Goog" not in p.read_bytes()
    assert (site_lake.site_paths(SITE).reports_dir / "list_summary.json").exists()


def test_list_months_filter(fake: FakeBundle) -> None:
    assert fetch.main("list", SITE, products=["DP1.00002.001"], months=["2024-06"]) == 0
    assert [c for c in fake.list_client.calls if c[0] == "post"] == [("post", "DP1.00002.001|2024-06|2024-06|")]


def test_query_bisects_on_5xx(fake: FakeBundle) -> None:
    from neon_mcp.errors import NeonApiError

    lc = fake.list_client
    lc.post_failures["P|2023-11|2024-02|"] = [NeonApiError(503, "unavailable", "/data/query")]
    lc.responses["P|2023-11|2023-12|"] = query_payload("P", [("RELEASE-2026", None, [("2023-11", []), ("2023-12", [])])])
    lc.responses["P|2024-01|2024-02|"] = query_payload("P", [("PROVISIONAL", None, [("2024-01", [])])])
    con = state.connect(state.DB_PATH)
    f = fetch.Fetcher(site=SITE, bundle=fake, con=con, pairs=[])
    pair = {"product": "P", "release": None, "months": ["2023-11", "2024-02"], "scope": "current"}
    rel = asyncio.run(f.query(pair, "2023-11", "2024-02"))
    assert [pk["month"] for r in rel for pk in r["packages"]] == ["2023-11", "2023-12", "2024-01"]
    assert f.counts["listSplits"] == 1
    # a single month that keeps failing becomes a ListingError after app-level retries
    lc.post_failures["P|2024-03|2024-03|"] = [NeonApiError(0, "x", "/data/query", kind="transport")] * 6
    with pytest.raises(fetch.ListingError):
        asyncio.run(f.query(pair, "2024-03", "2024-03"))
    con.close()


def test_download_retries_relists_and_resumes(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    from neon_mcp.errors import NeonApiError, ToolError

    assert fetch.main("list", SITE) == 0
    dc = fake.download_client
    bad = SAAT.format(m="2024-05")
    gone = SAAT.format(m="2024-06")
    dc.stream_failures[bad] = [ToolError("checksum_mismatch", "x")]
    dc.stream_failures[gone] = [NeonApiError(403, "expired", "/bucket/x")]
    posts_before = len([c for c in fake.list_client.calls if c[0] == "post"])
    assert fetch.main("download", SITE) == 0
    rows = _rows("SELECT * FROM site_files WHERE site=?", (SITE,))
    assert all(r["dl_status"] == "ok" and r["url"] is None for r in rows)
    by_name = {r["name"]: r for r in rows}
    assert by_name[bad]["dl_attempts"] == 2 and by_name[bad]["md5_verified"] == 1
    assert by_name[gone]["dl_attempts"] == 2
    soft = next(r for r in rows if r["kind"] == "eml" and r["month"] == "2024-06")
    assert soft["md5_verified"] == 0 and soft["dl_md5"] == md5(_body(soft["name"]))  # kept, flagged
    assert all(r["md5_verified"] == 1 for r in rows if r["name"] != soft["name"])
    assert len([c for c in fake.list_client.calls if c[0] == "post"]) == posts_before + 1  # one GCS-403 re-list
    for r in rows:
        assert (site_env / r["local_path"]).read_bytes() == _body(r["name"])
    con = site_lake.open_site_lake(SITE, read_only=True)
    assert con.execute("SELECT count(*) FROM lake.files WHERE dlMd5 IS NOT NULL").fetchone()[0] == len(rows)
    assert con.execute("SELECT count(*) FROM lake.files WHERE NOT md5Verified").fetchone()[0] == 1
    con.close()
    # resume: a stale .part is removed and nothing is fetched again
    part = site_env / rows[0]["local_path"]
    part.with_name(part.name + ".part").write_bytes(b"junk")
    dc.calls.clear()
    assert fetch.main("download", SITE) == 0
    assert not [c for c in dc.calls if c[0] == "stream"]
    assert not list(site_env.rglob("*.part"))
    # a deleted local file is re-downloaded (URL refreshed by a re-list since it was nulled)
    (site_env / by_name[bad]["local_path"]).unlink()
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 0
    assert [c for c in dc.calls if c[0] == "stream"] == [("stream", bad)]


def test_superseded_pending_file(fake: FakeBundle) -> None:
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    key = "DP1.00002.001|2024-05|2024-07|"
    old = SAAT.format(m="2024-07")
    payload = fake.list_client.responses[key]
    for r in payload["releases"]:
        for pk in r["packages"]:
            pk["files"] = [f for f in pk["files"] if f["name"] != old]
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    r = _rows("SELECT dl_status, url FROM site_files WHERE name=?", (old,))[0]
    assert r["dl_status"] == "superseded" and r["url"] is None
    assert _rows("SELECT count(*) FROM fetch_superseded")[0][0] == 1
    con = site_lake.open_site_lake(SITE, read_only=True)
    assert con.execute("SELECT count(*) FROM lake.files WHERE name = ?", [old]).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM lake.files").fetchone()[0] == 8
    con.close()
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 0
    assert _rows("SELECT dl_status FROM site_files WHERE name=?", (old,))[0][0] == "superseded"


def test_external_without_landing_does_one_get(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    for p in (site_env / "api" / "runs").rglob("external/*.json.gz"):
        p.unlink()
    fake.list_client.responses[f"/data/DP1.00033.001/{SITE}/2024-07"] = {
        "externalData": [{"name": "Phenocam", "type": "Website", "url": "https://phenocam.example/y"}]}
    assert fetch.main("list", SITE, products=["DP1.00033.001"]) == 0
    assert fake.list_client.calls == [("get", f"/data/DP1.00033.001/{SITE}/2024-07")]
