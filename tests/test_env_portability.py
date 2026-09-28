"""env.py / preflight.py portability: user resolution, configurable paths, dependency provenance, pins."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import catalog_lake as cl
from neon_ducklake import env, preflight
from tests.conftest import TEST_USER
from tests.test_catalog_fakes import fake_env  # noqa: F401 - fixture
from tests.test_catalog_lake import sealed_run

CHECKOUT = Path(__file__).resolve().parents[1]
HEX40 = "0123456789abcdef0123456789abcdef01234567"


def run_env(code: str, tmp_path: Path, **extra: str) -> subprocess.CompletedProcess[str]:
    """Import neon_ducklake.env in a fresh interpreter with a scrubbed identity (HOME/MESA_HOME in tmp)."""
    keep = {k: v for k, v in os.environ.items() if k in ("PATH", "LANG", "LC_ALL", "SYSTEMROOT")}
    child = {**keep, "HOME": str(tmp_path / "home"), "MESA_HOME": str(tmp_path / "mesa"), **extra}
    return subprocess.run([sys.executable, "-c", "import neon_ducklake.env as e\n" + code], capture_output=True,
                          text=True, env=child, cwd=str(tmp_path), timeout=120, check=False)


# ---------------------------------------------------------------- user and paths

def test_suite_identity_is_hermetic() -> None:
    assert env.CYVERSE_USER == TEST_USER and env.ACTOR == TEST_USER
    assert env.IRODS_ROOT == f"/iplant/home/{TEST_USER}/neon-ducklake"
    assert env.PRECEDENT_IRODS == f"/iplant/home/{TEST_USER}/neon-downloads/ducklake/neon_spatial.ducklake"
    assert env.ROOT == env.CODE_ROOT == CHECKOUT


def test_cyverse_user_resolution_order(tmp_path: Path) -> None:
    home = tmp_path / "mesa"
    home.mkdir()
    assert env.cyverse_user({}, home) is None
    (home / "cyverse-user").write_text("  fileuser \nignored\n")
    assert env.cyverse_user({}, home) == "fileuser"
    assert env.cyverse_user({"MESA_MCP_IRODS__USER": "anonymous"}, home) == "fileuser"
    assert env.cyverse_user({"MESA_MCP_IRODS__USER": "mcpuser"}, home) == "mcpuser"
    assert env.cyverse_user({"CYVERSE_USERNAME": "cvuser", "MESA_MCP_IRODS__USER": "mcpuser"}, home) == "cvuser"
    (home / "cyverse-user").write_text("\n")
    assert env.cyverse_user({"CYVERSE_USERNAME": " "}, home) is None


def test_defaults_from_the_mesa_install(tmp_path: Path) -> None:
    (tmp_path / "mesa").mkdir()
    (tmp_path / "mesa" / "cyverse-user").write_text("alice\n")
    r = run_env("print(e.IRODS_ROOT, e.ACTOR, e.ROOT, e.MESA_CATALOG, e.PRECEDENT_LAKE, e.PRECEDENT_DATA, "
                "e.PRECEDENT_VERSION, e.PRECEDENT_IRODS)", tmp_path)
    assert r.returncode == 0, r.stderr
    got = r.stdout.split()
    assert got[:4] == ["/iplant/home/alice/neon-ducklake", "alice", str(CHECKOUT),
                       str(tmp_path / "mesa" / "ducklake" / "catalog.duckdb")]
    assert got[4:] == [str(tmp_path / "home/neon-downloads/ducklake/neon_spatial.ducklake"),
                       str(tmp_path / "home/neon-downloads/ducklake/data"), "90",
                       "/iplant/home/alice/neon-downloads/ducklake/neon_spatial.ducklake"]


def test_overrides_and_disabled_precedent(tmp_path: Path) -> None:
    r = run_env("print(e.IRODS_ROOT, e.ACTOR, e.ROOT, e.PRECEDENT_LAKE, e.PRECEDENT_DATA, e.PRECEDENT_IRODS)",
                tmp_path, NEON_DUCKLAKE_IRODS_ROOT="/tempZone/home/bob/proj/", NEON_DUCKLAKE_ROOT=str(tmp_path),
                NEON_DUCKLAKE_PRECEDENT_LAKE="none")
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["/tempZone/home/bob/proj", "bob", str(tmp_path), "None", "None", "None"]


def test_unresolved_user_fails_clearly_only_when_needed(tmp_path: Path) -> None:
    r = run_env("print('offline', e.ROOT.name, e.CYVERSE_USER, e.PRECEDENT_IRODS)", tmp_path)
    assert r.returncode == 0 and r.stdout.split() == ["offline", CHECKOUT.name, "None", "None"], r.stderr
    for name in ("IRODS_ROOT", "ACTOR"):
        r = run_env(f"print(e.{name})", tmp_path)
        assert r.returncode == 1 and "cannot resolve the CyVerse user" in r.stderr and "CYVERSE_USERNAME" in r.stderr
    r = run_env("e.NOT_THERE", tmp_path)
    assert r.returncode == 1 and "AttributeError" in r.stderr


def test_precedent_is_optional(fake_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                               caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(logging.getLogger("neon_ducklake"), "propagate", True)
    caplog.set_level(logging.WARNING, logger="neon_ducklake.catalog_lake")
    monkeypatch.setattr(env, "PRECEDENT_LAKE", None)
    assert cl.SpatialSource.default() is None and "no precedent lake configured" in caplog.text
    monkeypatch.setattr(env, "PRECEDENT_LAKE", fake_env / "missing" / "neon_spatial.ducklake")
    monkeypatch.setattr(env, "PRECEDENT_DATA", fake_env / "missing" / "data")
    spatial = cl.SpatialSource.default()
    assert spatial is not None and spatial.lake == env.PRECEDENT_LAKE and spatial.version == env.PRECEDENT_VERSION
    run = sealed_run(fake_env)
    res = cl.build(run.run_id, run_dir=run.run_dir, lake_dir=fake_env / "ducklake", ledger_db=run.ledger_db,
                   spatial=spatial, temp_dir=fake_env / "work" / "tmp", workers=1)
    assert res.status == "committed" and res.exit_code == 0
    assert not any(n.startswith(cl.SPATIAL_PREFIX) for n in res.inserted)
    assert "not found" in caplog.text and "spatial-layer copy is skipped" in caplog.text


# ---------------------------------------------------------------- dependency provenance

class FakeDist:
    def __init__(self, version: str, direct: dict[str, Any] | None) -> None:
        self.version = version
        self._direct = direct

    def read_text(self, name: str) -> str | None:
        return json.dumps(self._direct) if name == "direct_url.json" and self._direct is not None else None


def fake_distributions(monkeypatch: pytest.MonkeyPatch, dists: dict[str, FakeDist]) -> None:
    from importlib import metadata

    def distribution(name: str) -> FakeDist:
        if name not in dists:
            raise metadata.PackageNotFoundError(name)
        return dists[name]

    monkeypatch.setattr(metadata, "distribution", distribution)


def git_repo(path: Path) -> str:
    path.mkdir(parents=True)
    run = [["git", "init", "-q"], ["git", "add", "f.txt"],
           ["git", "-c", "user.name=t", "-c", "user.email=t@example.org", "commit", "-q", "-m", "x"]]
    (path / "f.txt").write_text("a")
    for cmd in run:
        subprocess.run(cmd, cwd=path, check=True, capture_output=True)
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=path, check=True, capture_output=True,
                          text=True).stdout.strip()


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_dist_info_from_direct_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = git_repo(tmp_path / "checkout")
    fake_distributions(monkeypatch, {
        "neon-mcp": FakeDist("0.1.0", {"url": "https://github.com/idss-mesa/neon-mcp",
                                       "vcs_info": {"vcs": "git", "commit_id": HEX40, "requested_revision": HEX40}}),
        "mesa-mcp": FakeDist("0.2.0", {"url": (tmp_path / "checkout").as_uri(), "dir_info": {"editable": True}}),
        "mesa-ducklake": FakeDist("1.0", None),
    })
    vcs = env.dist_info("neon-mcp")
    assert (vcs.commit, vcs.short, vcs.source, vcs.editable) == (HEX40, HEX40[:7],
                                                                 "https://github.com/idss-mesa/neon-mcp", False)
    ed = env.dist_info("mesa-mcp")
    assert (ed.commit, ed.dirty, ed.editable, ed.source) == (sha, False, True, str(tmp_path / "checkout"))
    (tmp_path / "checkout" / "f.txt").write_text("b")
    assert env.dist_info("mesa-mcp").short == sha[:7] + "-dirty"
    assert "editable" in env.dist_info("mesa-mcp").describe()
    idx = env.dist_info("mesa-ducklake")
    assert (idx.commit, idx.short, idx.source) == (None, "1.0", None) and "package index" in idx.describe()
    missing = env.dist_info("nope")
    assert missing.version is None and missing.short == "unknown" and missing.describe() == "not installed"
    versions = env.repo_versions()
    assert list(versions) == [*env.DEPENDENCIES, env.SELF]
    assert versions["neon-mcp"] == HEX40[:7] and versions["mesa-ducklake"] == "1.0"


def test_installed_dependencies_are_the_pinned_revs() -> None:
    pins = preflight.pinned_revs()
    assert pins is not None and set(pins) == set(env.DEPENDENCIES)
    assert all(v and len(v) == 40 for v in pins.values())
    for name, info in env.dependency_infos().items():
        assert info.version, name
        if name != env.SELF:
            status, detail = preflight.dependency_check(info, pins[name], True)
            assert status in ("ok", "warn"), detail  # a local editable override only warns


def test_dependency_check_warns_never_fails() -> None:
    d = env.DistInfo("neon-mcp", "0.1.0", commit=HEX40, source="https://x")
    assert preflight.dependency_check(d, HEX40, True)[0] == "ok"
    assert preflight.dependency_check(d, HEX40[:12], True)[0] == "ok"
    st, detail = preflight.dependency_check(d, "f" * 40, True)
    assert st == "warn" and "differs from the pinned rev fffffff" in detail
    assert preflight.dependency_check(env.DistInfo("x", "1", commit=HEX40, dirty=True), HEX40, True)[0] == "warn"
    assert preflight.dependency_check(env.DistInfo("x", "1"), HEX40, True)[0] == "warn"
    assert preflight.dependency_check(d, None, True)[0] == "warn"
    assert preflight.dependency_check(d, HEX40, False)[0] == "info"


def test_pinned_revs_parse(tmp_path: Path) -> None:
    pp = tmp_path / "pyproject.toml"
    pp.write_text('[tool.uv.sources]\nneon-mcp = { git = "https://x", rev = "abc" }\n'
                  'mesa-mcp = { path = "../mesa-mcp", editable = true }\n')
    assert preflight.pinned_revs(pp) == {"neon-mcp": "abc", "mesa-mcp": None, "mesa-ducklake": None}
    assert preflight.pinned_revs(tmp_path / "absent.toml") is None


def test_check_imports_and_identity() -> None:
    r = preflight.Report()
    preflight.check_imports(r)
    mods = {c.name: c for c in r.checks if c.name.startswith("module ")}
    assert set(mods) == {"module neon_mcp", "module mesa_mcp", "module mesa_ducklake", "module neon_ducklake"}
    assert all(c.status != "fail" for c in mods.values())
    assert mods["module neon_ducklake"].data["source"] == str(CHECKOUT)
    r = preflight.Report()
    preflight.check_identity(r)
    assert r.checks[0].status == "ok" and TEST_USER in r.checks[0].detail


def test_check_identity_fails_without_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(env, "IRODS_ROOT")
    monkeypatch.delattr(env, "ACTOR")
    r = preflight.Report()
    preflight.check_identity(r)
    assert r.checks[0].status == "fail" and "CYVERSE_USERNAME" in r.checks[0].detail
    assert preflight._irods_root() is None
