"""Hermetic checks of enroll.py / preflight.py helpers (the live paths are run by the orchestrator)."""

from __future__ import annotations

import os
import time
from pathlib import Path

from neon_ducklake import enroll, env, preflight

from .test_avu_fakes import FakeIrods, FakeSession

R = "/iplant/home/testuser/neon-ducklake"


def test_nested_mesa_enabled_only_below_root() -> None:
    s = FakeIrods()
    s.add_coll(R, (("mesa.enabled", "true", ""),))
    s.add_coll(f"{R}/sites/SRER", (("neon.siteCode", "SRER", ""),))
    s.add_coll("/iplant/home/testuser/neon-downloads", (("mesa.enabled", "true", ""),))
    assert enroll.nested_mesa_enabled(FakeSession(s), R) == []
    s.add_coll(f"{R}/sites/HARV", (("mesa.enabled", "true", ""),))
    assert enroll.nested_mesa_enabled(FakeSession(s), R) == [f"{R}/sites/HARV"]


def test_ensure_root_is_idempotent() -> None:
    s = FakeIrods()
    s.add_coll("/iplant/home/testuser")
    assert enroll.ensure_root(FakeSession(s), R) is True
    assert enroll.ensure_root(FakeSession(s), R) is False
    assert s.calls["coll_create"] == 1


def test_preflight_helpers(tmp_path, monkeypatch) -> None:
    assert preflight._count_files({"releases": [{"packages": [{"files": [1, 2]}, {"files": [3]}]}]}) == 3
    assert preflight._count_files({}) == 0
    assert preflight._catalog_path("duckdb:////home/x/catalog.duckdb") == Path("/home/x/catalog.duckdb")
    assert preflight._catalog_path(None).name == "catalog.duckdb"
    monkeypatch.setattr(env, "ROOT", tmp_path)
    monkeypatch.setattr(env, "SITES_DIR", tmp_path / "sites")
    r = preflight.Report()
    preflight.check_parts(r)
    part = tmp_path / "sites/SRER/raw/DP1.00002.001/2024-06/x.csv.part"
    part.parent.mkdir(parents=True)
    part.write_text("x")
    os.utime(part, (time.time() - 7200, time.time() - 7200))
    preflight.check_parts(r)
    assert [c.status for c in r.checks] == ["ok", "warn"] and "1 older than 1 h" in r.checks[1].detail
    preflight.check_disk(r, None)
    assert r.checks[-1].data["planned"] == preflight.DEFAULT_PLANNED_BYTES


def test_mesa_catalog_check_skips_when_held(monkeypatch) -> None:
    monkeypatch.setattr(env, "catalog_lock_holders", lambda path=None: [{"pid": "12", "cmd": "mesa-mcp"}])
    r = preflight.Report()
    preflight.check_mesa_catalog(r, None)
    assert [(c.name, c.status) for c in r.checks] == [("MESA catalog lock", "fail")]
