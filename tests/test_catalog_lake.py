"""catalog_lake.py + diff.py + verify_catalog.py end to end on a tiny fake run (hermetic)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from neon_ducklake import catalog_lake as cl
from neon_ducklake import diff, env, flatten, seal, verify_catalog
from tests.test_catalog_fakes import (  # noqa: F401 - fixture
    FakeRun,
    fake_env,
    is_files,
    make_precedent,
    make_run,
    package,
)


def sealed_run(root: Path, status: str = "complete") -> FakeRun:
    run = make_run(root, status=status)
    seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    return run


def do_build(run: FakeRun, root: Path, **kw: Any) -> cl.BuildResult:
    lake, data, version = kw.pop("precedent", (None, None, None))
    spatial = cl.SpatialSource(lake, data, version) if lake else None
    return cl.build(run.run_id, run_dir=run.run_dir, lake_dir=root / "ducklake", ledger_db=run.ledger_db,
                    spatial=spatial, temp_dir=root / "work" / "tmp", workers=1, **kw)


def lake_con(root: Path) -> Any:
    con = duckdb.connect()
    con.execute("LOAD ducklake; LOAD spatial; SET TimeZone='UTC'")
    d = (root / "ducklake").resolve()
    con.execute(f"ATTACH 'ducklake:{d / env.CATALOG_LAKE}' AS lk (READ_ONLY, DATA_PATH '{d / 'data'}/', "
                "OVERRIDE_DATA_PATH true)")
    return con


def changed(res: cl.BuildResult) -> dict[str, tuple[int, int]]:
    names = set(res.inserted) | set(res.deleted)
    return {n: (res.inserted.get(n, 0), res.deleted.get(n, 0)) for n in names
            if res.inserted.get(n, 0) or res.deleted.get(n, 0)}


@pytest.fixture
def built(fake_env: Path) -> tuple[FakeRun, cl.BuildResult, Path]:  # noqa: F811
    run = sealed_run(fake_env)
    prec = make_precedent(fake_env)
    res = do_build(run, fake_env, precedent=prec)
    run.precedent = prec  # type: ignore[attr-defined]
    return run, res, fake_env


def test_first_build_creates_everything(built: tuple[FakeRun, cl.BuildResult, Path]) -> None:
    run, res, root = built
    assert res.status == "committed" and res.exit_code == 0 and res.snapshot_id == 1
    assert not (root / "ducklake" / "neon_catalog.ducklake.wal").exists()
    assert (root / "ducklake" / "data").is_dir()
    con = lake_con(root)
    snaps = con.execute("SELECT snapshot_id, author, commit_message, commit_extra_info FROM lk.snapshots() "
                        "ORDER BY 1").fetchall()
    assert [s[0] for s in snaps] == [0, 1]
    assert snaps[1][1] == "tswetnam" and snaps[1][2] == f"catalog build {run.run_id}"
    extra = json.loads(snaps[1][3])
    assert extra["runId"] == run.run_id and extra["inserted"]["files"] == 24 and extra["created"] is True
    opts = dict(con.execute("SELECT option_name, value FROM ducklake_options('lk')").fetchall())
    assert opts["parquet_compression"] == "zstd" and opts["data_inlining_row_limit"] == "0"
    assert opts["require_commit_message"] == "true"
    tables = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='lk'").fetchall()}
    assert set(flatten.TABLES) | {"neon_domains"} == tables  # api_* layers are not copied
    # the spatial layer is the pinned version (1 row, not the later 2)
    assert con.execute("SELECT count(*), any_value(typeof(geometry)) FROM lk.neon_domains").fetchone() == (1, "GEOMETRY")
    counts = {t: con.execute(f'SELECT count(*) FROM lk."{t}"').fetchone()[0] for t in flatten.TABLES}
    assert counts["products"] == 4 and counts["sites"] == 2 and counts["files"] == 24
    assert counts["availability_months"] == 9 and counts["product_site"] == 5 and counts["catalog_builds"] == 1
    assert counts["table_catalog"] == len(flatten.TABLES) + 1 + 3
    # contract columns and types
    cols = dict(con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name='lk' "
                            "AND table_name='files'").fetchall())
    assert cols["publicationTimestamp"] == "TIMESTAMP" and cols["size"] == "BIGINT"
    assert cols["releaseGenerationDate"] == "TIMESTAMP WITH TIME ZONE" and cols["rowHash"] == "VARCHAR"
    scols = dict(con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name='lk' "
                             "AND table_name='sites'").fetchall())
    assert scols["siteProperties"] == "MAP(VARCHAR, VARCHAR)"
    assert con.execute("SELECT siteProperties['TypeEco'] FROM lk.sites WHERE siteCode='SRER'").fetchone()[0] == \
        "Desert shrubland"
    assert con.execute("SELECT typeof(record) FROM lk.taxonomy LIMIT 1").fetchone()[0] == "JSON"
    assert con.execute("SELECT count(geometry) FROM lk.location_polygons").fetchone()[0] == 1
    # views
    inv = con.execute("SELECT productCode, siteCode, month, release, isProvisional, isReleaseOnly, isExternal, "
                      "fileCount FROM lk.v_site_inventory ORDER BY 1, 2, 3").fetchall()
    assert ("DP1.00001.001", "HARV", "2020-01", "RELEASE-2025", False, False, True, 0) in inv
    assert ("DP1.00006.001", "SRER", "2020-02", "RELEASE-2024", False, True, False, 2) in inv
    assert ("DP1.00001.001", "SRER", "2020-03", "PROVISIONAL", True, False, False, 5) in inv
    assert not [r for r in inv if r[0].startswith("DP3.")] and len(inv) == 7
    cov = con.execute("SELECT sum(cellsMissing) FROM lk.v_pair_coverage").fetchone()[0]
    assert cov == 0
    ps = dict(con.execute("SELECT productCode, fileCount FROM lk.v_product_summary").fetchall())
    assert ps["DP1.00001.001"] == 15 and ps["DP3.30006.001"] is None
    views = con.execute("SELECT tableName, definitionSha256 FROM lk.table_catalog WHERE tableGroup='view'").fetchall()
    assert len(views) == 3 and all(v[1] for v in views)
    src = con.execute("SELECT source FROM lk.table_catalog WHERE tableName='neon_domains'").fetchone()[0]
    assert "neonscience.org" in src and "@v" in src


def test_second_build_is_a_near_noop(built: tuple[FakeRun, cl.BuildResult, Path]) -> None:
    run, _res, root = built
    res2 = do_build(run, root, precedent=run.precedent)  # type: ignore[attr-defined]
    assert res2.status == "committed" and res2.snapshot_id == 2
    assert changed(res2) == {"catalog_builds": (1, 0), "table_catalog": (1, 1)}
    # diff of the last two snapshots only reports the bookkeeping
    con = diff.open_lake(root / "ducklake")
    md = diff.diff_markdown(con, *diff.default_range(con))
    con.close()
    assert "| catalog_builds | 1 | 0 | 1 | 0 | 0 |" in md and "## Products" not in md


def test_refresh_produces_a_real_diff(built: tuple[FakeRun, cl.BuildResult, Path]) -> None:
    run, _res, root = built
    p = run.payloads
    wind = next(x for x in p["products"]["products"] if x["productCode"] == "DP1.00001.001")
    srer = next(s for s in wind["siteCodes"] if s["siteCode"] == "SRER")
    srer["availableReleases"] = [{"release": "RELEASE-2026", "availableMonths": ["2020-01", "2020-02", "2020-03"]},
                                 {"release": "PROVISIONAL", "availableMonths": ["2020-04"]}]
    srer["availableMonths"] = ["2020-01", "2020-02", "2020-03", "2020-04"]
    srer["availableDataUrls"] = [f"https://x/{m}" for m in srer["availableMonths"]]
    wind["releases"].append({"release": "RELEASE-2026", "generationDate": "2026-01-23T00:00:00Z", "url": "u",
                             "productDoi": {"generationDate": "2026-01-23T00:00:00Z",
                                            "url": "https://doi.org/10.48443/new-2026"}})
    brd = next(x for x in p["products"]["products"] if x["productCode"] == "DP1.10003.001")
    brd["productStatus"] = "FUTURE"
    run.land("products", "products", p["products"]["products"], fetched_at="2026-02-01T00:00:00+00:00")
    q = p["data_query"]["DP1.00001.001__SRER__2020-01_2020-03"]
    q["releases"][1]["packages"].append(package("D14", "SRER", "2020-04", "2026-05-01T00:00:00Z", is_files(
        "D14", "SRER", "DP1.00001.001", "2020-04", "20260501T000000Z")[:2]))
    run.land("data_query", "DP1.00001.001__SRER__2020-01_2020-03", q, fetched_at="2026-02-01T00:00:00+00:00")
    run.land("taxonomy", "BIRD__0", {**p["taxonomy"]["BIRD__0"], "data": p["taxonomy"]["BIRD__0"]["data"][:1]},
             fetched_at="2026-02-01T00:00:00+00:00")
    seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    res = do_build(run, root)
    assert res.status == "committed"
    ch = changed(res)
    assert ch["files"] == (2, 0) and ch["taxonomy"] == (0, 1) and ch["availability_months"] == (4, 3)
    assert ch["products"] == (2, 2) and ch["product_releases"] == (1, 0)
    con = diff.open_lake(root / "ducklake")
    md = diff.diff_markdown(con, *diff.default_range(con))
    con.close()
    assert "| status | DP1.10003.001 | Breeding landbird point counts | ACTIVE | FUTURE |" in md
    assert "## PROVISIONAL → released (1 cells)" in md and "| DP1.00001.001 | RELEASE-2026 | 1 |" in md
    assert "## Months added (1 cells)" in md
    assert "| DP1.00001.001 | 10.48443/new-2026 |" not in md and "RELEASE-2026 | 10.48443/new-2026 |" in md
    assert "| BIRD | 0 | 1 | 0 |" in md
    assert "+2 / -0 files" in md


def test_diff_main_defaults_and_first_snapshot(built: tuple[FakeRun, cl.BuildResult, Path],
                                               capsys: pytest.CaptureFixture[str]) -> None:
    _run, _res, root = built
    assert diff.main(0, 1, lake_dir=root / "ducklake") == 0
    out = capsys.readouterr().out
    assert "# Catalog diff: snapshot 0 → 1" in out and "| products | 4 | 0 | 4 | 0 | 0 |" in out
    assert "| neon_domains | 1 | 0 |" in out
    assert diff.main(None, None, lake_dir=root / "ducklake") == 0  # default: the last two snapshots (0, 1)
    assert "# Catalog diff: snapshot 0 → 1" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="unknown snapshot"):
        diff.main(0, 7, lake_dir=root / "ducklake")
    assert diff.main(None, None, lake_dir=root / "nowhere") == 1


def test_dry_run_rolls_back(built: tuple[FakeRun, cl.BuildResult, Path], capsys: pytest.CaptureFixture[str]) -> None:
    run, _res, root = built
    res = do_build(run, root, dry_run=True)
    assert res.status == "dry-run" and res.snapshot_id is None and res.inserted["catalog_builds"] == 1
    cl.print_counts(res)
    assert "catalog_builds" in capsys.readouterr().out
    con = lake_con(root)
    assert con.execute("SELECT count(*) FROM lk.snapshots()").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM lk.catalog_builds").fetchone()[0] == 1


def test_dry_run_on_a_new_lake_leaves_nothing(fake_env: Path) -> None:  # noqa: F811
    run = sealed_run(fake_env)
    res = do_build(run, fake_env, dry_run=True)
    assert res.status == "dry-run" and res.inserted["files"] == 24 and res.rows["products"] == 4
    assert list((fake_env / "ducklake").iterdir()) == []


def test_partial_run_is_refused_unless_allowed(fake_env: Path) -> None:  # noqa: F811
    run = sealed_run(fake_env, status="partial")
    assert do_build(run, fake_env).exit_code == 2
    assert not (fake_env / "ducklake" / "neon_catalog.ducklake").exists()
    res = do_build(run, fake_env, allow_partial=True)
    assert res.status == "committed"
    con = lake_con(fake_env)
    assert con.execute("SELECT runStatus, allowPartial FROM lk.catalog_builds").fetchone() == ("partial", True)


def test_refused_seal_blocks_the_build(fake_env: Path) -> None:  # noqa: F811
    run = sealed_run(fake_env)
    run.land("external", "DP1.00001.001__SRER__2020-01", {"externalData": [
        {"url": "https://storage.googleapis.com/x.csv?X-Goog-Signature=abc"}]})
    man = seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    assert man["errors"] and not (run.run_dir / "sealed" / "external.jsonl.gz").exists()
    res = do_build(run, fake_env)
    assert res.exit_code == 2 and not (fake_env / "ducklake" / "neon_catalog.ducklake").exists()


def test_assertions_roll_back(built: tuple[FakeRun, cl.BuildResult, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    run, _res, root = built
    real = flatten.flatten_run

    def dup_products(*a: Any, **kw: Any) -> flatten.Flattened:
        fl = real(*a, **kw)
        rows = fl.tables["products"].to_pylist()
        rows[1] = {**rows[0], "productName": "duplicate key"}
        fl.tables["products"] = flatten.to_table("products", rows)
        signed = fl.tables["release_artifacts"].to_pylist()
        signed[0]["url"] += "?X-Goog-Signature=abc"
        fl.tables["release_artifacts"] = flatten.to_table("release_artifacts", signed)
        return fl

    monkeypatch.setattr(flatten, "flatten_run", dup_products)
    res = do_build(run, root)
    assert res.exit_code == 3 and res.status == "assertion-failed"
    assert any("products: 1 duplicate keys" in p for p in res.problems)
    assert any("release_artifacts" in p and "signed URL" in p for p in res.problems)
    con = lake_con(root)
    assert con.execute("SELECT count(*) FROM lk.snapshots()").fetchone()[0] == 2  # nothing committed
    assert con.execute("SELECT count(*) FROM lk.products WHERE productName = 'duplicate key'").fetchone()[0] == 0


def test_schema_evolution_adds_columns(built: tuple[FakeRun, cl.BuildResult, Path],
                                       monkeypatch: pytest.MonkeyPatch) -> None:
    run, _res, root = built
    spec = flatten.TABLES["sample_classes"]
    wider = flatten.TableSpec(spec.name, spec.group, spec.key, spec.columns + (("extra", "str"),), spec.description)
    monkeypatch.setitem(flatten.TABLES, "sample_classes", wider)
    res = do_build(run, root)
    assert res.status == "committed" and changed(res)["sample_classes"] == (2, 2)
    con = lake_con(root)
    cols = [r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='lk' AND "
                                      "table_name='sample_classes' ORDER BY column_index").fetchall()]
    assert cols == ["sampleClass", "description", "extra", "rowHash", "loadedRun"] or "extra" in cols


def test_verify_all_hard_checks_pass(built: tuple[FakeRun, cl.BuildResult, Path]) -> None:
    run, _res, root = built
    seen = []
    results = verify_catalog.verify(run.run_id, lake_dir=root / "ducklake", run_dir=run.run_dir, workers=1,
                                    hooks=[lambda ctx: seen.append(ctx.run_id) or
                                           {"irods.stub": verify_catalog.check(True, 1, 1)}])
    failed = {n: r for n, r in results.items() if r["hard"] and not r["ok"]}
    assert failed == {}
    assert seen == [run.run_id] and "irods.stub" in results
    assert results["count.files"]["actual"] == 24 and results["files.parseConsistent"]["ok"]
    assert results["count.availability_months"]["expected"] == 9
    assert results["count.locations"]["detail"].endswith("findLocations: 2")  # REALM, GHOST
    srer = json.loads(results["inventory.SRER"]["detail"])
    assert srer == {"products": 1, "siteMonths": 3, "files": 15, "bytes": srer["bytes"], "productsInclReleaseOnly": 2,
                    "siteMonthsInclReleaseOnly": 5, "filesInclReleaseOnly": 20, "bytesInclReleaseOnly":
                    srer["bytesInclReleaseOnly"], "externalSiteMonths": 0}
    assert not results["plan.products"]["ok"] and not results["plan.products"]["hard"]


def test_verify_flags_problems(built: tuple[FakeRun, cl.BuildResult, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    run, _res, root = built
    # a new current month without files, a failed data_query and a planted signed URL in sealed/
    p = run.payloads
    brd = next(x for x in p["products"]["products"] if x["productCode"] == "DP1.10003.001")
    sc = brd["siteCodes"][0]
    sc["availableReleases"][0]["availableMonths"].append("2020-07")
    sc["availableMonths"].append("2020-07")
    sc["availableDataUrls"].append("https://x/2020-07")
    run.land("products", "products", p["products"]["products"], fetched_at="2026-02-01T00:00:00+00:00")
    run.ledger_row("data_query", "DP1.10003.001__HARV__2020-07_2020-07", "error", "NeonApiError: 500")
    seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    assert do_build(run, root).status == "committed"
    (run.run_dir / "sealed" / "planted.jsonl.gz").write_bytes(__import__("gzip").compress(b"?X-Goog-Credential=x"))
    monkeypatch.setattr(env, "ACTOR", "tswetnam")
    results = verify_catalog.verify(run.run_id, lake_dir=root / "ducklake", run_dir=run.run_dir, workers=1, hooks=[])
    bad = {n for n, r in results.items() if r["hard"] and not r["ok"]}
    assert {"coverage.cells", "ledger.allOk", "ledger.dataQueryPairs", "signed.sealed"} <= bad
    assert results["coverage.cells"]["actual"] == 1
    # an exception file clears the coverage failure
    exc = root / "ref" / "catalog_coverage_exceptions.json"
    exc.parent.mkdir(parents=True, exist_ok=True)
    exc.write_text(json.dumps([{"productCode": "DP1.10003.001", "siteCode": "HARV", "month": "2020-07",
                                "reason": "test"}]))
    again = verify_catalog.verify(run.run_id, lake_dir=root / "ducklake", run_dir=run.run_dir, workers=1, hooks=[])
    assert again["coverage.cells"]["ok"]


def test_verify_main_exit_code(built: tuple[FakeRun, cl.BuildResult, Path], monkeypatch: pytest.MonkeyPatch,
                               capsys: pytest.CaptureFixture[str]) -> None:
    run, _res, root = built
    monkeypatch.setattr(verify_catalog, "HOOKS", [])
    assert verify_catalog.main(run.run_id, lake_dir=root / "ducklake") == 0
    assert "ok   count.products" in capsys.readouterr().out
    monkeypatch.setattr(verify_catalog, "HOOKS", [lambda ctx: {"mesa.stub": verify_catalog.check(False, 1, 0)}])
    assert verify_catalog.main(run.run_id, lake_dir=root / "ducklake") == 1


def test_attach_all_and_sql_file(built: tuple[FakeRun, cl.BuildResult, Path]) -> None:
    _run, _res, root = built
    con = cl.connect(None)
    assert cl.attach_all(con, root) == ["neon"]
    assert con.execute("SELECT count(*) FROM neon.v_site_inventory WHERE siteCode='SRER'").fetchone()[0] == 5
    con.close()
    sql = (cl.CODE_ROOT / "sql" / "attach_all.sql").read_text().replace("/home/tswetnam/neon-ducklake", str(root))
    con = duckdb.connect()
    ok, failed = [], []
    code = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    for body in [s.strip() for s in code.split(";")]:
        if not body:
            continue
        try:
            con.execute(body)
            ok.append(body.split()[0])
        except duckdb.Error:
            failed.append(body)
    assert ok.count("ATTACH") == 1 and len(failed) == 2  # SRER/HARV site lakes do not exist here
    assert con.execute("SELECT count(*) FROM neon.products").fetchone()[0] == 4
    assert not (root / "sites").exists()  # READ_ONLY attach never creates a lake


def test_load_views_parses_markers() -> None:
    views = cl.load_views()
    assert [v.name for v in views] == ["v_site_inventory", "v_pair_coverage", "v_product_summary"]
    assert all(v.sql.upper().startswith("CREATE OR REPLACE VIEW") and v.description for v in views)
