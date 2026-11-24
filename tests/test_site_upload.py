"""site upload: item list, ordering (catalog last, versioned), exclusions; against the Data Store fakes."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
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
def ds(site_env: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:  # noqa: F811
    """The real mirror.push_tree against the in-memory Data Store, with every push_tree call recorded."""
    from neon_ducklake import env, mirror

    from .test_coord_fakes import DSIrods, DSSession

    store = DSIrods()
    session = DSSession(store)
    calls: list[dict[str, Any]] = []
    real = mirror.push_tree

    def push_tree(items: list, threads: int = 8, record_site: str | None = None, **kw: Any) -> dict:
        calls.append({"items": list(items), "threads": threads, "record_site": record_site})
        return real(items, threads=threads, record_site=record_site, session=session, sleep=lambda _s: None)

    monkeypatch.setattr(mirror, "push_tree", push_tree)
    monkeypatch.setattr(env, "mesa", lambda: SimpleNamespace(session=session))
    return SimpleNamespace(store=store, session=session, calls=calls)


S = f"/zone/home/u/proj/sites/{SITE}"


def test_upload_items_and_order(site_env: Path, ds: SimpleNamespace) -> None:  # noqa: F811
    _journal(site_env)
    con = site_lake.open_site_lake(SITE)
    with site_lake.commit(con, "t"):
        con.execute("CREATE TABLE lake.t AS SELECT 1 AS x")
    con.close()  # left un-checkpointed on purpose: upload must checkpoint + detach first
    assert upload.main(SITE) == 0
    pushed = ds.calls
    assert pushed[0]["record_site"] == SITE
    first = [(str(p.relative_to(site_env)), dest, m) for p, dest, m in pushed[0]["items"]]
    raw = [x for x in first if "/raw/" in x[0]]
    assert [x[0].rsplit("/", 1)[1] for x in raw] == ["a.csv", "b.csv"]  # pending + superseded excluded
    assert raw[0][1] == f"{S}/raw/DP1.00002.001/2024-05/a.csv" and raw[0][2] == md5(b"a.csv")
    rels = [x[0] for x in first]
    assert f"sites/{SITE}/cards/DP1.00002.001.md" in rels and "ref/ontology_maps/taxa.parquet" in rels
    assert not [r for r in rels if r.endswith(".part") or "/tmp/" in r]
    lake_data = [r for r in rels if "/ducklake/data/" in r]
    assert lake_data and rels.index(lake_data[0]) > rels.index(f"sites/{SITE}/reports/plan.json")
    # the catalog goes last, through push_lake: a versioned copy, the CURRENT pointer, then the canonical copy
    catalog_puts = [dest for c in pushed[1:] for _p, dest, _m in c["items"]]
    assert catalog_puts[0].startswith(f"{S}/ducklake/catalogs/neon_test.") and catalog_puts[-1] == \
        f"{S}/ducklake/neon_test.ducklake"
    assert not [d for d in catalog_puts if "/ducklake/data/" in d]  # data files already pushed are not pushed again
    ptr = json.loads(ds.store.content(f"{S}/ducklake/CURRENT"))
    assert ptr["path"] == catalog_puts[0] and ds.store.content(ptr["path"]) == \
        site_lake.site_paths(SITE).catalog_file.read_bytes()
    assert not site_lake.site_paths(SITE).wal_file.exists()
    assert (site_env / "logs" / f"upload_summary_{SITE}.json").exists()


def test_upload_filters_and_failure_holds_back_catalog(site_env: Path, ds: SimpleNamespace) -> None:  # noqa: F811
    _journal(site_env)
    site_lake.open_site_lake(SITE).close()
    ds.store.put_errors.append(RuntimeError("SYS_INTERNAL_ERR"))  # the first put (b.csv) fails for good
    assert upload.main(SITE, months=["2024-06"]) == 2
    assert len(ds.calls) == 1  # catalog not pushed after a failure
    assert [p.name for p, _d, _m in ds.calls[0]["items"] if "/raw/" in _d] == ["b.csv"]
    assert not [p for p in ds.store.objs if p.endswith((".ducklake", "/CURRENT"))]


def test_evicted_rows_are_skipped_quietly(site_env: Path, ds: SimpleNamespace, caplog: pytest.LogCaptureFixture) -> None:  # noqa: F811
    _journal(site_env)
    con = state.connect(state.DB_PATH)
    con.execute("ALTER TABLE site_files ADD COLUMN local_evicted INTEGER DEFAULT 0")  # the evict step's column
    con.execute("UPDATE site_files SET local_evicted=1 WHERE name='a.csv'")
    con.close()
    (site_env / f"sites/{SITE}/raw/DP1.00002.001/2024-05/a.csv").unlink()  # evicted: deleted locally
    with caplog.at_level("WARNING", logger="neon_ducklake.site.upload"):
        items = upload.raw_items(SITE)
    assert [p.name for p, _d, _m in items] == ["b.csv"]
    assert not [r for r in caplog.records if "a.csv" in r.getMessage()]
