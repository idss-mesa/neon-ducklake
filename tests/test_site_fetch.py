"""site list/download against a scripted fake NeonClient (hermetic)."""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import sqlite3
from pathlib import Path
from typing import Any

import httpx
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


def _b64md5(body: bytes) -> str:
    return base64.b64encode(hashlib.md5(body, usedforsecurity=False).digest()).decode()


class FakeResponse:
    """A streamed GCS response: headers + raw body chunks (``aiter_raw``), closed by ``aclose``."""

    def __init__(self, body: bytes, headers: httpx.Headers, on_close: Any = None, chunk: int = 7) -> None:
        self.body, self.headers, self.on_close, self.chunk = body, headers, on_close, chunk
        self.closed = False

    async def aiter_raw(self) -> Any:
        for i in range(0, len(self.body), self.chunk):
            await asyncio.sleep(0)
            yield self.body[i:i + self.chunk]

    async def aclose(self) -> None:
        if not self.closed and self.on_close:
            self.on_close()
        self.closed = True


class GcsClient(FakeClient):
    """Download client with NeonClient's opener: the storage md5 comes back in ``x-goog-hash``.

    Per file name: ``corrupt`` (flip the last byte for the next N transfers), ``no_hash`` (no x-goog-hash),
    ``gzip_stored`` (stored gzip, served gzip), ``transit_gzip`` (stored identity, gzip on the wire),
    ``transcoded`` (stored gzip, served decoded).
    """

    def __init__(self, blobs: dict[str, bytes] | None = None) -> None:
        super().__init__(blobs=dict(blobs or {}))
        self.corrupt: dict[str, int] = {}
        self.no_hash: set[str] = set()
        self.gzip_stored: set[str] = set()
        self.transit_gzip: set[str] = set()
        self.transcoded: set[str] = set()
        self.opened: list[dict[str, Any]] = []

    def on_close(self) -> None:
        return None

    async def _open(self, url: str, *, token: Any, stats: Any, allowed_hosts: Any, method: str = "GET",
                    extra_headers: Any = None, timeout_s: float | None = None) -> tuple[FakeResponse, str]:
        name = url.split("?")[0].rsplit("/", 1)[1]
        self.calls.append(("stream", name))
        self.opened.append({"token": token, "timeout_s": timeout_s, "allowed_hosts": allowed_hosts})
        assert token is None and "X-Goog-Signature" in url
        fails = self.stream_failures.get(name) or []
        if fails:
            raise fails.pop(0)
        body = self.blobs[url.split("?")[0]]
        stored, wire, hdr = body, body, [("x-goog-stored-content-encoding", "identity")]
        if name in self.gzip_stored or name in self.transcoded:
            stored = gzip.compress(body, mtime=0)
            hdr = [("x-goog-stored-content-encoding", "gzip")]
            if name in self.gzip_stored:
                wire = stored
                hdr.append(("content-encoding", "gzip"))
        elif name in self.transit_gzip:
            wire = gzip.compress(body, mtime=0)
            hdr.append(("content-encoding", "gzip"))
        if name not in self.no_hash:
            hdr += [("x-goog-hash", "crc32c=AAAAAA=="), ("x-goog-hash", f"md5={_b64md5(stored)}")]
        if self.corrupt.get(name):
            self.corrupt[name] -= 1
            wire = wire[:-1] + bytes([wire[-1] ^ 0xFF])
        hdr.append(("content-length", str(len(wire))))
        return FakeResponse(wire, httpx.Headers(hdr), on_close=self.on_close), url

    async def stream_to_file(self, *a: Any, **kw: Any) -> Any:  # pragma: no cover - must not be used
        raise AssertionError("the fetcher streams through _open")


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
    dc = GcsClient(blobs={f"https://storage.googleapis.com/bucket/{n}": b for n, b in BODIES.items()})
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
    from neon_mcp.errors import NeonApiError

    assert fetch.main("list", SITE) == 0
    dc = fake.download_client
    bad = SAAT.format(m="2024-05")
    gone = SAAT.format(m="2024-06")
    dc.corrupt[bad] = 1  # one corrupted transfer (bytes != storage md5): retried, no re-list
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
    assert all(r["transfer_verified"] == 1 and r["storage_md5"] == r["dl_md5"] for r in rows)
    assert all(r["neon_md5"] == r["md5"] for r in rows)
    assert not list(site_env.rglob("*.part"))
    assert len([c for c in fake.list_client.calls if c[0] == "post"]) == posts_before + 1  # one GCS-403 re-list
    for r in rows:
        assert (site_env / r["local_path"]).read_bytes() == _body(r["name"])
    con = site_lake.open_site_lake(SITE, read_only=True)
    assert con.execute("SELECT count(*) FROM lake.files WHERE dlMd5 IS NOT NULL").fetchone()[0] == len(rows)
    assert con.execute("SELECT count(*) FROM lake.files WHERE NOT md5Verified").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM lake.files WHERE transferVerified AND storageMd5 = dlMd5 AND "
                       "neonMd5 = md5").fetchone()[0] == len(rows)
    con.close()
    import json

    summary = json.loads((site_lake.site_paths(SITE).reports_dir / "download_summary.json").read_text())
    assert summary["counts"]["corruptRetries"] == 1 and summary["counts"]["transferVerified"] == len(rows)
    assert summary["integrity"]["eml"]["neonMismatch"] == 1 and summary["counts"]["neonStale"] == 1
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


# ---------------------------------------------------------------- adaptive download lanes + eviction

class SlowClient(GcsClient):
    """A download client that yields to the loop mid-transfer and records its concurrency (open -> close)."""

    def __init__(self, blobs: dict[str, bytes], meter: dict[str, int]) -> None:
        super().__init__(blobs=blobs)
        self.meter = meter
        self.active = 0
        self.peak = 0

    def on_close(self) -> None:
        self.active -= 1
        self.meter["active"] -= 1

    async def _open(self, url: str, **kw: Any) -> tuple[FakeResponse, str]:  # type: ignore[override]
        await asyncio.sleep(0.001)
        resp, final = await super()._open(url, **kw)
        self.active += 1
        self.meter["active"] += 1
        self.peak = max(self.peak, self.active)
        self.meter["peak"] = max(self.meter["peak"], self.meter["active"])
        return resp, final


def _seed_downloads(n: int) -> tuple[sqlite3.Connection, dict[str, bytes]]:
    con = state.connect(state.DB_PATH)
    con.executescript(fetch.SUPERSEDED_SCHEMA)
    blobs: dict[str, bytes] = {}
    now = env.utcnow().isoformat()
    for i in range(n):
        name = f"NEON.D99.TEST.DP1.00002.001.000.010.030.SAAT_30min.2024-01.basic.{i:05d}.csv"
        body = f"payload {i}\n".encode() * (1 + i % 5)
        blobs[f"https://storage.googleapis.com/bucket/{name}"] = body
        con.execute("INSERT INTO site_files(site, product, month, name, kind, size, md5, url, listed_at, local_path)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (SITE, "DP1.00002.001", "2024-01", name, "data", len(body), md5(body),
                     f"https://storage.googleapis.com/bucket/{name}?X-Goog-Signature=v1", now,
                     f"sites/{SITE}/raw/DP1.00002.001/2024-01/{name}"))
    return con, blobs


def test_downloads_spread_over_three_clients_and_grow_to_24(site_env: Path) -> None:  # noqa: F811
    con, blobs = _seed_downloads(400)
    meter = {"active": 0, "peak": 0}
    c1, c2, c3 = (SlowClient(blobs, meter) for _ in range(3))
    f = fetch.Fetcher(site=SITE, bundle=FakeBundle(FakeClient(), c1), con=con, pairs=[], threads=8,
                      extra_clients=[c2, c3])
    asyncio.run(f.download_all(None, None))
    st = f.download_stats
    assert f.counts["downloaded"] == 400 and f.counts["failed"] == 0
    assert st["clients"] == 3 and st["concurrencyStart"] == 8 and st["concurrencyMax"] == fetch.DL_MAX == 24
    assert meter["peak"] <= 24 and st["peakInFlight"] == meter["peak"] > 8
    assert max(c.peak for c in (c1, c2, c3)) <= fetch.DL_CONCURRENCY  # never more than 8 streams per client
    assert all(c.peak > 0 for c in (c1, c2, c3)) and st["peakPerClient"] == [c1.peak, c2.peak, c3.peak]
    assert st["mbPerS"] >= 0 and st["filesPerS"] > 0
    rows = con.execute("SELECT dl_status, local_path FROM site_files").fetchall()
    assert {r["dl_status"] for r in rows} == {"ok"} and all((site_env / r["local_path"]).exists() for r in rows)
    con.close()


def test_one_client_caps_at_eight_streams(site_env: Path) -> None:  # noqa: F811
    con, blobs = _seed_downloads(120)
    meter = {"active": 0, "peak": 0}
    c1 = SlowClient(blobs, meter)
    f = fetch.Fetcher(site=SITE, bundle=FakeBundle(FakeClient(), c1), con=con, pairs=[], threads=20)
    asyncio.run(f.download_all(None, None))
    assert f.counts["downloaded"] == 120 and f.download_stats["clients"] == 1  # a fake bundle has no config
    assert f.download_stats["concurrencyStart"] == 8 and c1.peak == 8 and meter["peak"] == 8
    con.close()


def test_transport_errors_halve_download_concurrency(site_env: Path, monkeypatch) -> None:  # noqa: F811
    from neon_mcp.errors import NeonApiError

    monkeypatch.setattr(fetch, "RETRY_DELAYS", [0, 0, 0, 0, 0])
    con, blobs = _seed_downloads(40)
    c1 = GcsClient(blobs=blobs)
    names = [r[0] for r in con.execute("SELECT name FROM site_files ORDER BY name LIMIT 3")]
    for n in names:
        c1.stream_failures[n] = [NeonApiError(0, "ReadTimeout", "/bucket/x", kind="transport")]
    c1.stream_failures[names[0]].append(NeonApiError(503, "unavailable", "/bucket/x"))
    f = fetch.Fetcher(site=SITE, bundle=FakeBundle(FakeClient(), c1), con=con, pairs=[], threads=8)
    asyncio.run(f.download_all(None, None))
    st = f.download_stats
    assert f.counts["downloaded"] == 40 and f.counts["failed"] == 0
    assert st["errorsByKind"] == {"transport": 3, "http_503": 1}
    assert st["concurrencyMin"] == 4 and st["changes"][0]["reason"] == "error: transport"  # one halving per burst
    con.close()


def test_evicted_files_are_never_redownloaded(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    assert fetch.main("list", SITE) == 0
    assert fetch.main("download", SITE) == 0
    con = state.connect(state.DB_PATH)
    con.execute("ALTER TABLE site_files ADD COLUMN local_evicted INTEGER DEFAULT 0")  # the evict step's column
    evicted = [r["local_path"] for r in con.execute(
        "SELECT local_path FROM site_files WHERE product='DP1.00002.001'")]
    con.execute("UPDATE site_files SET local_evicted=1 WHERE product='DP1.00002.001'")
    for rel in evicted:
        (site_env / rel).unlink()
    stale = SAAT.format(m="2024-05")
    con.execute("UPDATE site_files SET dl_status='stale' WHERE name=?", (stale,))  # upstream MD5 changed
    con.close()
    dc = fake.download_client
    dc.calls.clear()
    assert fetch.main("download", SITE) == 0
    assert [c for c in dc.calls if c[0] == "stream"] == [("stream", stale)]  # only the stale one
    rows = {r["name"]: r for r in _rows("SELECT * FROM site_files WHERE site=?", (SITE,))}
    assert rows[stale]["dl_status"] == "ok" and rows[stale]["local_evicted"] == 0
    assert all(r["dl_status"] == "ok" and r["local_evicted"] == 1 for n, r in rows.items()
               if r["product"] == "DP1.00002.001" and n != stale)
    assert sum(1 for rel in evicted if (site_env / rel).exists()) == 1
    import json

    summary = json.loads((site_lake.site_paths(SITE).reports_dir / "download_summary.json").read_text())
    assert summary["counts"]["evicted"] == len(evicted) - 1
    # a second pass does nothing at all
    dc.calls.clear()
    assert fetch.main("download", SITE) == 0
    assert not [c for c in dc.calls if c[0] == "stream"]


def test_download_tolerates_a_journal_without_the_evict_column(fake: FakeBundle) -> None:
    assert fetch.main("list", SITE, products=["DP1.10003.001"]) == 0
    assert fetch.main("download", SITE, products=["DP1.10003.001"]) == 0
    assert all(r["dl_status"] == "ok" for r in _rows("SELECT dl_status FROM site_files"))
    assert "local_evicted" not in {r[1] for r in _rows("PRAGMA table_info(site_files)")}


# ---------------------------------------------------------------- integrity (F1), re-list windows (F2), lab kind (F8)

def test_goog_md5_and_gunzip_helpers() -> None:
    body = b"hello world\n"
    assert fetch.goog_md5(f"crc32c=AAAAAA==, md5={_b64md5(body)}") == md5(body)
    assert fetch.goog_md5("crc32c=AAAAAA==") is None and fetch.goog_md5(None) is None
    assert fetch.goog_md5("md5=not*base64") is None and fetch.goog_md5("md5=AAAA") is None  # 3 bytes, not 16
    g = fetch._Gunzip()
    two = gzip.compress(b"abc", mtime=0) + gzip.compress(b"def", mtime=0)  # concatenated members
    assert b"".join(g.feed(two[i:i + 3]) for i in range(0, len(two), 3)) + g.finish() == b"abcdef"
    g = fetch._Gunzip()
    g.feed(gzip.compress(b"x" * 100, mtime=0)[:-6])
    with pytest.raises(fetch.TransferCorrupt):
        g.finish()


def _set_neon_md5(fake: FakeBundle, name: str, value: str) -> None:
    for payload in fake.list_client.responses.values():
        for r in payload.get("releases") or []:
            for pk in r.get("packages") or []:
                for f in pk.get("files") or []:
                    if f["name"] == name:
                        f["md5"] = value


def test_stale_neon_md5_on_a_data_file_is_kept_and_flagged(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    """F1 revised: storage md5 matches, NEON's listed md5 does not -> keep, md5_verified=0, no re-list."""
    data = SAAT.format(m="2024-06")
    var = "NEON.D99.TEST.DP1.00002.001.variables.20250101T000000Z.csv"
    _set_neon_md5(fake, data, "d" * 32)
    _set_neon_md5(fake, var, "e" * 32)
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    posts = len([c for c in fake.list_client.calls if c[0] == "post"])
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 0
    assert len([c for c in fake.list_client.calls if c[0] == "post"]) == posts  # never re-listed for it
    rows = {r["name"]: r for r in _rows("SELECT * FROM site_files WHERE site=?", (SITE,))}
    r = rows[data]
    assert r["dl_status"] == "ok" and r["dl_attempts"] == 1
    assert (r["md5_verified"], r["transfer_verified"]) == (0, 1)
    assert r["neon_md5"] == "d" * 32 and r["storage_md5"] == r["dl_md5"] == md5(_body(data))
    assert rows[var]["md5_verified"] == 0 and rows[var]["transfer_verified"] == 1
    con = site_lake.open_site_lake(SITE, read_only=True)
    assert con.execute("SELECT md5Verified, transferVerified, neonMd5, storageMd5 FROM lake.files WHERE name = ?",
                       [data]).fetchone() == (False, True, "d" * 32, md5(_body(data)))
    con.close()
    # the next listing (same stale md5) does not mark it stale; a real upstream change does
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    assert _rows("SELECT dl_status FROM site_files WHERE name=?", (data,))[0][0] == "ok"
    _set_neon_md5(fake, data, "c" * 32)
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    assert _rows("SELECT dl_status FROM site_files WHERE name=?", (data,))[0][0] == "stale"
    # NEON's md5 catching up with our bytes confirms the file
    _set_neon_md5(fake, var, md5(_body(var)))
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    assert _rows("SELECT md5_verified, neon_md5 FROM site_files WHERE name=?", (var,))[0][:] == (1, md5(_body(var)))


def test_upstream_changed_rule() -> None:
    row = {"dl_md5": "a" * 32, "neon_md5": "b" * 32}
    assert not fetch.upstream_changed("b" * 32, "data", row)  # the stale NEON md5 we recorded: no change
    assert not fetch.upstream_changed("a" * 32, "data", row)  # NEON now lists our bytes
    assert fetch.upstream_changed("c" * 32, "variables", row)  # NEON moved: re-download
    legacy = {"dl_md5": "a" * 32, "neon_md5": None}
    assert fetch.upstream_changed("c" * 32, "data", legacy) and not fetch.upstream_changed("c" * 32, "eml", legacy)
    assert not fetch.upstream_changed(None, "data", legacy)


def test_corrupt_transfers_retry_then_fail_without_relisting(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    dc = fake.download_client
    bad = SAAT.format(m="2024-07")
    dc.corrupt[bad] = fetch.DL_TRIES  # every attempt corrupted
    posts = len([c for c in fake.list_client.calls if c[0] == "post"])
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 2
    assert len([c for c in fake.list_client.calls if c[0] == "post"]) == posts
    r = _rows("SELECT * FROM site_files WHERE name=?", (bad,))[0]
    assert (r["dl_status"], r["dl_error"], r["dl_attempts"]) == ("error", "transfer_corrupt", fetch.DL_TRIES)
    assert [c for c in dc.calls if c == ("stream", bad)] == [("stream", bad)] * fetch.DL_TRIES
    assert not (site_env / r["local_path"]).exists() and not list(site_env.rglob("*.part"))
    # the next run gets good bytes
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 0
    assert _rows("SELECT dl_status, transfer_verified FROM site_files WHERE name=?", (bad,))[0][:] == ("ok", 1)


def test_corruption_never_replaces_a_good_local_file(site_env: Path) -> None:  # noqa: F811
    dest = site_env / "x" / "NEON.D99.TEST.DP1.00002.001.variables.20250101T000000Z.csv"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"good")
    c = GcsClient(blobs={"https://storage.googleapis.com/bucket/" + dest.name: b"new bytes"})
    c.corrupt[dest.name] = 1
    with pytest.raises(fetch.TransferCorrupt):
        asyncio.run(fetch.stream_verified(c, f"https://storage.googleapis.com/bucket/{dest.name}?X-Goog-Signature=1",
                                          dest, max_bytes=100, neon_md5=None, hard=True))
    assert dest.read_bytes() == b"good" and not dest.with_name(dest.name + ".part").exists()


def test_without_a_storage_md5_neon_md5_is_hard_for_data_soft_for_supporting(fake: FakeBundle,
                                                                              site_env: Path) -> None:  # noqa: F811
    data = SAAT.format(m="2024-06")
    var = "NEON.D99.TEST.DP1.00002.001.variables.20250101T000000Z.csv"
    _set_neon_md5(fake, data, "d" * 32)
    _set_neon_md5(fake, var, "e" * 32)
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    dc = fake.download_client
    dc.no_hash |= {data, var, SAAT.format(m="2024-05")}
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 2
    rows = {r["name"]: r for r in _rows("SELECT * FROM site_files WHERE site=?", (SITE,))}
    assert (rows[data]["dl_status"], rows[data]["dl_error"]) == ("error", "checksum_mismatch")
    assert rows[var]["dl_status"] == "ok" and rows[var]["md5_verified"] == 0 and rows[var]["transfer_verified"] is None
    ok = rows[SAAT.format(m="2024-05")]  # right NEON md5, no storage md5: verified against NEON only
    assert (ok["dl_status"], ok["md5_verified"], ok["transfer_verified"], ok["storage_md5"]) == ("ok", 1, None, None)
    # one re-list at the second attempt, over the recorded window only
    posts = [c for c in fake.list_client.calls if c[0] == "post"]
    assert posts[-1] == ("post", "DP1.00002.001|2024-05|2024-07|")


@pytest.mark.parametrize("mode", ["gzip_stored", "transit_gzip", "transcoded"])
def test_content_encodings(site_env: Path, mode: str) -> None:  # noqa: F811
    body = b"a,b,c\n" + b"1,2,3\n" * 200
    name = "NEON.D99.TEST.DP1.00002.001.000.010.030.SAAT_30min.2024-01.basic.20250101T000000Z.csv"
    c = GcsClient(blobs={f"https://storage.googleapis.com/bucket/{name}": body})
    getattr(c, mode).add(name)
    dest = site_env / "raw" / name
    out = asyncio.run(fetch.stream_verified(c, f"https://storage.googleapis.com/bucket/{name}?X-Goog-Signature=1",
                                            dest, max_bytes=10_000, neon_md5=md5(body), hard=True))
    assert dest.read_bytes() == body and out.md5 == md5(body) and out.bytes == len(body)
    if mode == "gzip_stored":  # the hash covers the stored (gzip) bytes = the raw body
        assert out.transfer_verified is True and out.storage_md5 == md5(gzip.compress(body, mtime=0))
        assert out.raw_bytes < len(body) and out.content_encoding == "gzip"
    elif mode == "transit_gzip":  # stored identity, gzip on the wire only: the hash covers the decoded body
        assert out.transfer_verified is True and out.storage_md5 == md5(body)
    else:  # served decoded from a gzip object: no comparable hash, NEON md5 decides
        assert out.transfer_verified is None


def test_download_limit_still_applies(site_env: Path) -> None:  # noqa: F811
    from neon_mcp.errors import ToolError

    name = "NEON.D99.TEST.DP1.00002.001.variables.20250101T000000Z.csv"
    c = GcsClient(blobs={f"https://storage.googleapis.com/bucket/{name}": b"x" * 500})
    dest = site_env / "raw" / name
    with pytest.raises(ToolError) as exc:
        asyncio.run(fetch.stream_verified(c, f"https://storage.googleapis.com/bucket/{name}?X-Goog-Signature=1",
                                          dest, max_bytes=100, neon_md5=None, hard=False))
    assert exc.value.code == "download_limit_exceeded" and not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()


def test_relist_reuses_the_recorded_sample_windows(fake: FakeBundle, site_env: Path) -> None:  # noqa: F811
    """F2 regression: a download without month filters (probe style) must not re-list the full window."""
    lc = fake.list_client
    lc.responses["DP1.00002.001|2024-06|2024-06|"] = query_payload("DP1.00002.001", [
        ("RELEASE-2026", "2026-01-23T00:07:49Z", [("2024-06", _files_for("2024-06"))])])
    assert fetch.main("list", SITE, products=["DP1.00002.001"], months=["2024-06"]) == 0
    gone = SAAT.format(m="2024-06")
    from neon_mcp.errors import NeonApiError

    fake.download_client.stream_failures[gone] = [NeonApiError(403, "expired", "/bucket/x")]
    lc.calls.clear()
    assert fetch.main("download", SITE) == 0  # unfiltered pairs: the full 2024-05..2024-07 window
    assert [c for c in lc.calls if c[0] == "post"] == [("post", "DP1.00002.001|2024-06|2024-06|")]
    months = {r[0] for r in _rows("SELECT DISTINCT month FROM site_files WHERE product='DP1.00002.001'")}
    assert months == {"2024-06"}  # nothing from outside the sample was journaled
    parts = [r[0] for r in _rows("SELECT part FROM listings WHERE product='DP1.00002.001'")]
    assert parts == ["2024-06_2024-06"]


def test_relist_windows_pick_the_narrowest_recorded_part(site_env: Path) -> None:  # noqa: F811
    con = state.connect(state.DB_PATH)
    f = fetch.Fetcher(site=SITE, bundle=FakeBundle(FakeClient(), FakeClient()), con=con, pairs=[])
    P = "DP1.00002.001"
    for part, rel, s, e in [("2024-06_2024-06", None, "2024-06", "2024-06"),
                            ("2016-05_2026-08", None, "2016-05", "2026-08"),  # a stray full window (F2 bug)
                            ("2019-01_2019-02__RELEASE-2025", "RELEASE-2025", "2019-01", "2019-02"),
                            ("external__2024-07", None, "2024-06", "2024-07")]:
        con.execute("INSERT INTO listings(site, product, part, release, start_month, end_month, listed_at, status)"
                    " VALUES (?,?,?,?,?,?,?,?)", (SITE, P, part, rel, s, e, "2026-09-01T00:00:00+00:00", "ok"))
    f.pending_months = {P: {("2024-06", False, "RELEASE-2026"), ("2019-02", True, "RELEASE-2025"),
                            ("2025-01", False, "PROVISIONAL"), ("2019-05", True, "RELEASE-2024")}}
    got = [(s, e, p["release"], p["months"], p["filtered"]) for p, s, e in f.relist_windows(P)]
    assert got == [
        ("2016-05", "2026-08", None, ["2025-01"], False),  # only the stray part covers 2025-01
        ("2024-06", "2024-06", None, ["2024-06"], False),  # the narrow part wins for 2024-06
        ("2019-05", "2019-05", "RELEASE-2024", ["2019-05"], True),  # no recorded part: exactly that month
        ("2019-01", "2019-02", "RELEASE-2025", ["2019-02"], True),
    ]
    # nothing pending -> nothing to re-list
    f.pending_months = {P: set()}
    assert f.relist_windows(P) == [] and asyncio.run(f.relist(P, "test")) is False
    con.close()


def test_lab_files_are_kind_lab_in_listings_and_healed(fake: FakeBundle) -> None:
    lab = "NEON.NEON_HQ.mct_taxonTableLabSummary.20251206T221502Z.csv"
    lc = fake.list_client
    lc.responses["DP1.10003.001|2024-06|2024-06|"]["releases"][0]["packages"][0]["files"].append(
        neon_file(lab, _body(lab)))
    fake.download_client.blobs[f"https://storage.googleapis.com/bucket/{lab}"] = _body(lab)
    assert fetch.main("list", SITE, products=["DP1.10003.001"]) == 0
    assert _rows("SELECT kind FROM site_files WHERE name=?", (lab,))[0][0] == "lab"
    con = state.connect(state.DB_PATH)
    con.execute("UPDATE site_files SET kind='other' WHERE name=?", (lab,))  # a row from an older listing
    con.close()
    assert fetch.main("download", SITE, products=["DP1.10003.001"]) == 0
    r = _rows("SELECT kind, dl_status, md5_verified FROM site_files WHERE name=?", (lab,))[0]
    assert tuple(r) == ("lab", "ok", 1)
    lake = site_lake.open_site_lake(SITE, read_only=True)
    assert lake.execute("SELECT kind, tableName FROM lake.files WHERE name=?", [lab]).fetchone() == (
        "lab", "mct_taxonTableLabSummary")
    lake.close()


def test_legacy_client_without_opener_uses_neon_md5(site_env: Path) -> None:  # noqa: F811
    con, blobs = _seed_downloads(5)
    f = fetch.Fetcher(site=SITE, bundle=FakeBundle(FakeClient(), FakeClient(blobs=blobs)), con=con, pairs=[])
    asyncio.run(f.download_all(None, None))
    rows = con.execute("SELECT dl_status, md5_verified, transfer_verified FROM site_files").fetchall()
    assert {tuple(r) for r in rows} == {("ok", 1, None)} and f.counts["noStorageMd5"] == 5
    con.close()


def test_lake_files_follow_the_journal(fake: FakeBundle) -> None:
    """Rows deleted from the journal (F2 stray-row cleanup) leave the site lake ``files`` on the next write."""
    assert fetch.main("list", SITE, products=["DP1.00002.001"]) == 0
    con = state.connect(state.DB_PATH)
    con.execute("DELETE FROM site_files WHERE month='2024-05'")
    con.close()
    assert fetch.main("download", SITE, products=["DP1.00002.001"]) == 0
    lake = site_lake.open_site_lake(SITE, read_only=True)
    assert {r[0] for r in lake.execute("SELECT DISTINCT month FROM lake.files").fetchall()} == {"2024-06", "2024-07"}
    lake.close()
    import json

    summary = json.loads((site_lake.site_paths(SITE).reports_dir / "download_summary.json").read_text())
    assert summary["lake"]["notInJournalRemoved"] == 3
