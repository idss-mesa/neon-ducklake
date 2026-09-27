"""site upload: item list, ordering (catalog last), exclusions; mirror.push_tree is faked."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import state
from neon_ducklake.site import site_lake, upload

from .test_site_common import SITE, md5, site_env  # noqa: F401 - site_env is a fixture


def _journal(root: Path) -> None:
    con = state.connect(state.DB_PATH)
    from neon_ducklake.site.fetch import SUPERSEDED_SCHEMA

    con.executescript(SUPERSEDED_SCHEMA)
    for month, name, status in [("2024-05", "a.csv", "ok"), ("2024-06", "b.csv", "ok"), ("2024-06", "c.csv", None),
                                ("2024-06", "old.csv", "ok")]:
        rel = f"sites/{SITE}/raw/DP1.00002.001/{month}/{name}"
        body = name.encode()
        if status == "ok":
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_bytes(body)
        con.execute("INSERT INTO site_files(site, product, month, name, local_path, dl_status, dl_md5, dl_bytes) "
                    "VALUES (?,?,?,?,?,?,?,?)", (SITE, "DP1.00002.001", month, name, rel, status, md5(body), len(body)))
    con.execute("INSERT INTO fetch_superseded VALUES (?,?,?,?,?,?)", (SITE, "DP1.00002.001", "2024-06", "old.csv", 1, "x"))
    con.close()
    for sub in ("reports/plan.json", "cards/DP1.00002.001.md", "curation/x.json", "reports/skip.part", "tmp/big.bin"):
        p = root / "sites" / SITE / sub
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    (root / "ref" / "ontology_maps").mkdir(parents=True)
    (root / "ref" / "ontology_maps" / "taxa.parquet").write_bytes(b"PAR1")


@pytest.fixture
def pushed(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def push_tree(items: list, threads: int = 8, record_site: str | None = None) -> dict:
        calls.append({"items": list(items), "threads": threads, "record_site": record_site})
        return {"uploaded": len(items), "failed": 0}

    mod = types.ModuleType("neon_ducklake.mirror")
    mod.push_tree = push_tree  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "neon_ducklake.mirror", mod)
    return calls


def test_upload_items_and_order(site_env: Path, pushed: list) -> None:  # noqa: F811
    _journal(site_env)
    con = site_lake.open_site_lake(SITE)
    with site_lake.commit(con, "t"):
        con.execute("CREATE TABLE lake.t AS SELECT 1 AS x")
    con.close()  # left un-checkpointed on purpose: upload must checkpoint + detach first
    assert upload.main(SITE) == 0
    assert len(pushed) == 2 and pushed[0]["record_site"] == SITE
    first = [(str(p.relative_to(site_env)), dest, m) for p, dest, m in pushed[0]["items"]]
    raw = [x for x in first if "/raw/" in x[0]]
    assert [x[0].rsplit("/", 1)[1] for x in raw] == ["a.csv", "b.csv"]  # pending + superseded excluded
    assert raw[0][1] == f"/zone/home/u/proj/sites/{SITE}/raw/DP1.00002.001/2024-05/a.csv" and raw[0][2] == md5(b"a.csv")
    rels = [x[0] for x in first]
    assert f"sites/{SITE}/cards/DP1.00002.001.md" in rels and "ref/ontology_maps/taxa.parquet" in rels
    assert not [r for r in rels if r.endswith(".part") or "/tmp/" in r]
    lake_data = [r for r in rels if "/ducklake/data/" in r]
    assert lake_data and rels.index(lake_data[0]) > rels.index(f"sites/{SITE}/reports/plan.json")
    assert [str(p.relative_to(site_env)) for p, _d, _m in pushed[1]["items"]] == [
        f"sites/{SITE}/ducklake/neon_{SITE.lower()}.ducklake"]
    assert not site_lake.site_paths(SITE).wal_file.exists()
    assert (site_env / "sites" / SITE / "reports" / "upload_summary.json").exists()


def test_upload_filters_and_failure_holds_back_catalog(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    _journal(site_env)
    site_lake.open_site_lake(SITE).close()
    calls = []

    def push_tree(items: list, threads: int = 8, record_site: str | None = None) -> dict:
        calls.append(items)
        return {"failed": 1}

    mod = types.ModuleType("neon_ducklake.mirror")
    mod.push_tree = push_tree  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "neon_ducklake.mirror", mod)
    assert upload.main(SITE, months=["2024-06"]) == 2
    assert len(calls) == 1  # catalog not pushed after a failure
    assert [p.name for p, _d, _m in calls[0] if "/raw/" in _d] == ["b.csv"]
