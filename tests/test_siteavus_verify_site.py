"""verify_site.py against the in-memory iRODS fake: read-only checks, statuses and failure detection."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import env, state, verify_site
from neon_ducklake.avu import bulk
from neon_ducklake.site import avus
from neon_ducklake.verify_site import SiteData, SiteVerifier

from .test_avu_fakes import FakeIrods, FakeSession
from .test_site_common import site_env  # noqa: F401 - fixture
from .test_siteavus_common import P_IS, P_OS, SITE, detach_site_avus, make_facts, write_local_files

detach_site_avus()  # see test_siteavus_common.detach_site_avus

S = f"/zone/home/u/proj/sites/{SITE}"
MONTH = f"{S}/raw/{P_IS}/2024-06"
LABELS = {"ENVO:01000218": "xeric shrubland biome", "ENVO:01000869": "area of scrub",
          "ENVO:01000888": "area of grassland", "UO:0000027": "degree Celsius", "ENVO:00002005": "air",
          "NCBITaxon:198939": "Amphispiza bilineata"}


def fake_ols(overrides: dict[str, Any] | None = None):
    calls: list[list[str]] = []

    def terms(curies: list[str]) -> dict[str, Any]:
        calls.append(list(curies))
        out: dict[str, Any] = {c: {"curie": c, "label": LABELS[c], "isObsolete": False} for c in curies if c in LABELS}
        out.update(overrides or {})
        return {k: v for k, v in out.items() if v != "DROP"}

    terms.calls = calls  # type: ignore[attr-defined]
    return terms


def site_data(facts) -> SiteData:
    d = SiteData(files=facts.files, site_months=facts.site_months, file_stats=facts.file_stats)
    d.inventory = [{"productCode": r["productCode"], "month": r["month"], "release": r["release"],
                    "isExternal": r.get("isExternal"), "fileCount": r.get("fileCount"),
                    "totalBytes": r.get("totalBytes")} for r in facts.site_months]
    d.sensor_keys = set(facts.sensor_positions)
    d.table_catalog = [{"tableName": "dp1_00002_001__saat_30min", "productCode": P_IS, "neonTable": "SAAT_30min",
                        "kind": "data", "nRows": 5760, "rowsBeforeDedup": 5760, "dedupRemoved": 0}]
    d.variables = {(v["productCode"], v["tableName"], v["fieldName"]): v["dataType"]
                   for vs in facts.variables.values() for v in vs}
    d.lake_columns = {"dp1_00002_001__saat_30min": {"startDateTime": "TIMESTAMP WITH TIME ZONE",
                                                    "tempSingleMean": "DOUBLE", "tempSingleVariance": "DOUBLE",
                                                    "finalQF": "BIGINT", "siteID": "VARCHAR"}}
    d.taxa = {(t["productCode"], t["taxonID"], t["scientificName"]) for ts in facts.file_taxa.values() for t in ts}
    d.journal = [{"product": r["productCode"], "month": r["month"], "name": r["name"], "kind": r["kind"],
                  "size": r["size"], "md5": r["md5"], "dl_status": "ok", "dl_md5": r["dlMd5"],
                  "dl_bytes": (env.ROOT / r["localRelPath"]).stat().st_size, "md5_verified": 1,
                  "local_path": r["localRelPath"], "irods_path": None,
                  "up_status": "present" if r["month"] == "2024-06" and r["productCode"] == P_IS else None,
                  "irods_checksum": None} for r in facts.files]
    return d


def tag_irods(con, data: SiteData, units: set[str]) -> FakeIrods:
    """Upload + tag exactly the desired rows of ``units`` (as `tag` would), with good replicas."""
    store = FakeIrods()
    j = {(r["product"], r["month"], r["name"]): r for r in data.journal}
    rows: dict[tuple[str, str], set] = defaultdict(set)
    for unit_id, path, ttype, a, v, u in con.execute(
            "SELECT unit_id, path, target_type, attribute, value, unit FROM avu_desired WHERE scope=?", (SITE,)):
        if unit_id in units:
            rows[(path, ttype)].add((a, v, u))
    for (path, ttype), avus_ in sorted(rows.items()):
        if ttype == "collection":
            store.add_coll(path, tuple(avus_) + (("ipc_UUID", "c", ""),))
        else:
            p, m, name = path[len(S) + 5:].split("/")
            jr = j[(p, m, name)]
            store.add_obj(path, tuple(avus_) + (("ipc_UUID", "o", ""),), replicas=[
                {"number": 0, "size": jr["dl_bytes"], "checksum": jr["dl_md5"], "status": "1"},
                {"number": 1, "size": jr["dl_bytes"], "checksum": "", "status": "0"}])
    return store


@pytest.fixture
def world(site_env: Path, monkeypatch: pytest.MonkeyPatch):  # noqa: F811
    facts = make_facts()
    write_local_files(facts)
    con = state.connect(state.DB_PATH)
    bulk.ensure_desired(con)
    avus.build_desired(SITE, con=con, facts=facts)
    data = site_data(facts)
    store = tag_irods(con, data, {S, f"{S}/raw", f"{S}/raw/{P_IS}", MONTH})
    monkeypatch.setattr(verify_site.siteavus, "load_ontology", lambda site, store=None: facts.ontology)
    yield facts, con, data, store
    con.close()


def run(con, data, store, ols=None, **kw) -> dict[str, dict]:
    v = SiteVerifier(SITE, [P_IS], ["2024-06"], session=FakeSession(store), con=con, data=data,
                     ols_terms=ols or fake_ols(), threads=2, **kw)
    rep = v.run()
    return {c["name"]: c for c in rep["checks"]} | {"_report": rep}


def test_all_good_on_the_tagged_month(world) -> None:
    _f, con, data, store = world
    r = run(con, data, store)
    status = {k: v["status"] for k, v in r.items() if k != "_report"}
    assert status == {"listing": "pass", "local files": "pass", "irods objects": "pass", "avu desired": "pass",
                      "avu core": "pass", "avu file kinds": "pass", "avu collections": "pass", "stack rows": "pass",
                      "stack typed": "pass", "stack sensors": "pass", "taxa": "skip", "ontology": "pass",
                      "datacite": "pass"}, {k: v["summary"] for k, v in r.items() if k != "_report"}
    assert r["_report"]["ok"] is True
    assert r["irods objects"]["details"]["n_good"] == 6
    assert r["local files"]["summary"].count("hard mismatches 0") == 1
    # typed share: tempSingleMean, tempSingleVariance, finalQF typed (3/3)
    assert r["stack typed"]["details"]["share"] == 1.0
    assert "iRODS round-trip ok 2" in r["datacite"]["summary"]
    # nothing was written to iRODS
    assert store.calls["atomic"] == 0 and store.calls["meta_add"] == 0 and store.calls["put"] == 0


def test_bad_replica_missing_core_and_bare_collection_fail(world) -> None:
    _f, con, data, store = world
    objs = sorted(p for p in store.objs if p.startswith(MONTH))
    store.objs[objs[0]]["replicas"][0]["checksum"] = "0" * 32
    store.objs[objs[1]]["avus"] = {t for t in store.objs[objs[1]]["avus"] if t[0] != "dc.source"}
    store.colls[f"{S}/raw"]["avus"] = {("ipc_UUID", "c", "")}
    r = run(con, data, store)
    assert r["irods objects"]["status"] == "fail" and "checksum differs" in r["irods objects"]["details"]["bad"][0]
    assert r["avu core"]["status"] == "fail" and "dc.source" in r["avu core"]["details"]["lacking"][0]
    assert r["avu desired"]["status"] == "fail"
    assert r["avu collections"]["status"] == "fail" and r["avu collections"]["details"]["bare"] == ["/raw"]
    assert r["_report"]["ok"] is False


def test_not_uploaded_is_counted_not_failed(world, site_env: Path) -> None:  # noqa: F811
    _f, con, data, store = world
    # the journal said 'present' for these objects: their absence is a failure
    r = run(con, data, FakeIrods())
    assert r["irods objects"]["status"] == "fail" and len(r["irods objects"]["details"]["journal_lost"]) == 6
    for j in data.journal:
        j["up_status"] = None
    r = run(con, data, FakeIrods())
    assert r["irods objects"]["status"] == "warn" and r["irods objects"]["details"]["n_uploaded"] == 0
    assert r["avu core"]["status"] == "skip" and r["avu collections"]["status"] == "skip"
    assert r["avu file kinds"]["status"] == "warn" and r["avu desired"]["status"] == "warn"
    # site-level targets that exist locally but were not uploaded are counted as such
    (site_env / "sites" / SITE / "reports").mkdir(parents=True, exist_ok=True)
    (site_env / "sites" / SITE / "reports" / "plan.json").write_text("{}")
    avus.build_desired(SITE, [P_IS], ["2024-06"], con=con, facts=make_facts())
    r = run(con, data, store)
    assert r["avu desired"]["status"] == "warn" and r["avu desired"]["details"]["not_uploaded"] == \
        {"container": 1, "side-file": 1}


def test_ontology_failures(world) -> None:
    _f, con, data, store = world
    r = run(con, data, store, ols=fake_ols({"UO:0000027": {"label": "degree Celsius", "isObsolete": True},
                                            "ENVO:00002005": {"label": "air mass", "isObsolete": False},
                                            "ENVO:01000888": None}))
    o = r["ontology"]
    assert o["status"] == "fail"
    assert o["details"]["obsolete"] == ["UO:0000027 degree Celsius"]
    assert o["details"]["unresolved"] == ["ENVO:01000888"]
    assert o["details"]["attribute_mismatch"] == ["envo.air [ENVO:00002005] expected envo.air_mass"]
    r2 = run(con, data, store, ols=fake_ols({"ENVO:00002005": "DROP"}))
    assert r2["ontology"]["status"] == "warn" and r2["ontology"]["details"]["unchecked"] == ["ENVO:00002005"]


def test_local_md5_soft_and_hard(world) -> None:
    _f, con, data, store = world
    eml = next(r for r in data.journal if r["kind"] == "eml" and r["month"] == "2024-06")
    eml["dl_md5"] = "f" * 32  # regenerated EML: soft (the lake mirrors the journal's local md5)
    next(f for f in data.files if f["name"] == eml["name"])["dlMd5"] = "f" * 32
    r = run(con, data, store)
    assert r["local files"]["status"] == "pass" and r["local files"]["details"]["soft"] == [eml["name"]]
    csv = next(r for r in data.journal if r["kind"] == "data" and r["month"] == "2024-06")
    csv["dl_md5"] = "e" * 32
    next(f for f in data.files if f["name"] == csv["name"])["dlMd5"] = "e" * 32
    r = run(con, data, store)
    assert r["local files"]["status"] == "fail" and r["local files"]["details"]["hard"] == [csv["name"]]


def test_stack_row_mismatch_and_sensor_gap(world) -> None:
    _f, con, data, store = world
    name = next(r["name"] for r in data.files if r["kind"] == "data" and r["productCode"] == P_IS
                and r["month"] == "2024-06")
    data.file_stats[name] = dict(data.file_stats[name], rowCount=7)
    data.sensor_keys = set()
    data.lake_columns["dp1_00002_001__saat_30min"]["tempSingleMean"] = "VARCHAR"
    r = run(con, data, store)
    assert r["stack rows"]["status"] == "fail" and "!= file_stats 7" in r["stack rows"]["details"]["mismatches"][0]
    assert r["stack sensors"]["status"] == "fail"
    assert r["stack typed"]["status"] == "fail" and r["stack typed"]["details"]["share"] == pytest.approx(2 / 3, 1e-3)


def test_listing_diffs_are_reported(world) -> None:
    _f, con, data, store = world
    data.inventory.append({"productCode": P_IS, "month": "2024-07", "release": "RELEASE-2026", "fileCount": 6,
                           "totalBytes": 400})
    v = SiteVerifier(SITE, [P_IS], None, session=FakeSession(store), con=con, data=data, ols_terms=fake_ols())
    rep = {c["name"]: c for c in v.run()["checks"]}
    assert rep["listing"]["status"] == "warn" and rep["listing"]["details"]["missing"] == [f"{P_IS}/2024-07"]


def test_taxa_rate_for_os_product(world) -> None:
    _f, con, data, store = world
    v = SiteVerifier(SITE, [P_OS], None, session=FakeSession(store), con=con, data=data, ols_terms=fake_ols())
    rep = {c["name"]: c for c in v.run()["checks"]}
    assert rep["taxa"]["status"] == "info" and rep["taxa"]["summary"].startswith("1/2 taxa resolved")


def test_no_irods_session_skips(world, monkeypatch: pytest.MonkeyPatch) -> None:
    _f, con, data, _store = world

    def no_creds():
        raise SystemExit("iRODS credentials missing")

    monkeypatch.setattr(env, "mesa", no_creds)
    v = SiteVerifier(SITE, [P_IS], ["2024-06"], con=con, data=data, ols_terms=fake_ols())
    rep = {c["name"]: c for c in v.run()["checks"]}
    assert all(rep[n]["status"] == "skip" for n in ("irods objects", "avu desired", "avu core", "avu collections"))


def test_main_writes_report_and_exit_code(world, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _f, con, data, store = world
    con.commit()
    monkeypatch.setattr(verify_site, "load_site_data", lambda site: data)
    from neon_ducklake.site import ontology_store

    monkeypatch.setattr(ontology_store, "ols_terms", fake_ols())
    rc = verify_site.main("test", "DP1.00002.001", "2024-06", session=FakeSession(store), threads=1)
    assert rc == 0
    out = site_env_path = env.SITES_DIR / SITE / "reports" / "verify.json"
    rep = json.loads(out.read_text())
    assert rep["filters"] == {"products": [P_IS], "months": ["2024-06"]} and rep["ok"] is True
    assert "verify site TEST" in capsys.readouterr().out and site_env_path.exists()
    store.objs[min(p for p in store.objs if p.startswith(MONTH))]["replicas"][0]["status"] = "0"
    assert verify_site.main(SITE, [P_IS], ["2024-06"], session=FakeSession(store), threads=1) == 1


class FlakyStore(FakeIrods):
    """Raises a network error on the first N queries touching one collection."""

    def __init__(self, base: FakeIrods, coll: str, failures: int) -> None:
        super().__init__()
        self.__dict__.update({k: v for k, v in base.__dict__.items() if k not in ("lock",)})
        self.coll, self.failures = coll, failures

    def rows(self, table: str) -> list[dict[str, Any]]:
        if self.failures > 0 and table == "obj":
            self.failures -= 1
            raise ConnectionError("Could not receive server response")
        return super().rows(table)


def test_transient_irods_errors_are_retried_and_persistent_ones_fail(world, monkeypatch: pytest.MonkeyPatch) -> None:
    _f, con, data, store = world
    monkeypatch.setattr(verify_site, "RETRY_DELAYS", (0.0, 0.0))
    r = run(con, data, FlakyStore(store, MONTH, 1))
    assert r["irods objects"]["status"] == "pass"  # one failure, then a fresh clone succeeds
    r = run(con, data, FlakyStore(store, MONTH, 100))
    assert r["irods objects"]["status"] == "fail" and "unreadable" in r["irods objects"]["summary"]
    assert r["irods objects"]["details"]["unreadable"][0].startswith(f"{P_IS}/2024-06: ConnectionError")


def test_local_evicted_and_storage_verified_stale_md5(world) -> None:
    """Evicted rows count as evicted (not missing); a storage-verified NEON-stale data file is flagged, not hard."""
    _f, con, data, store = world
    rows = [r for r in data.journal if r["month"] == "2024-06" and r["product"] == P_IS]
    ev = next(r for r in rows if r["kind"] == "data")
    (env.ROOT / ev["local_path"]).unlink()
    ev["local_evicted"] = 1
    r = run(con, data, store)
    assert r["local files"]["status"] == "pass", r["local files"]["summary"]
    assert r["local files"]["details"]["n_evicted"] == 1 and r["local files"]["details"]["missing"] == []
    stale = next(x for x in rows if x["kind"] == "data" and x is not ev)
    stale["md5"] = "d" * 32  # NEON lists a stale md5; our bytes matched the storage md5
    stale["storage_md5"], stale["transfer_verified"] = stale["dl_md5"], 1
    r = run(con, data, store)
    assert r["local files"]["status"] == "pass" and r["local files"]["details"]["neon_stale"] == [stale["name"]]
    stale["transfer_verified"], stale["storage_md5"] = None, None  # no storage evidence: a hard mismatch
    r = run(con, data, store)
    assert r["local files"]["status"] == "fail" and r["local files"]["details"]["hard"] == [stale["name"]]


def test_journal_columns_tolerate_missing_optional_columns(site_env) -> None:  # noqa: F811
    import sqlite3 as _sqlite3

    from neon_ducklake import verify_site as vs

    sc = _sqlite3.connect(":memory:")
    sc.execute("CREATE TABLE site_files (product, month, name, kind, size, md5, dl_status, dl_md5, dl_bytes, "
               "md5_verified, local_path, irods_path, up_status, irods_checksum, local_evicted)")
    cols = vs.journal_columns(sc)
    assert "local_evicted" in cols and "NULL AS storage_md5" in cols and "NULL AS local_evicted" not in cols
    sc.execute(f"SELECT {cols} FROM site_files").fetchall()
