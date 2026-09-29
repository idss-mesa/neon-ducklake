"""NEON_DUCKLAKE_SECRETS=auto|env|keyring: env.secrets_mode/secrets_sources and scripts/with-secrets.sh.

The script runs for real with a scrubbed environment, a tmp HOME and a fake MESA install whose keyring
helpers print dummy values; nothing touches the real keyring or ~/.irods.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from neon_ducklake import env

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "with-secrets.sh"
DUMP = [sys.executable, "-c", "import json, os; print(json.dumps(dict(os.environ)))"]


def fake_mesa(home: Path) -> Path:
    """A MESA install whose keyring yields 'kr-token' / 'kr-pass' for user 'kruser'."""
    mesa = home / ".mesa"
    (mesa / "bin").mkdir(parents=True)
    (mesa / ".venv" / "bin").mkdir(parents=True)
    (mesa / "cyverse-user").write_text("kruser\n")
    (mesa / "bin" / "mesa-keyring-common.sh").write_text(
        'CYVERSE_USERNAME="${CYVERSE_USERNAME:-$(head -1 "$MESA_HOME/cyverse-user")}"\n'
        "mesa_keyring_password() { printf 'kr-pass'; }\n")
    py = mesa / ".venv" / "bin" / "python"
    py.write_text('#!/bin/sh\n[ "$*" = "-m keyring get neon api-token" ] && printf kr-token\n')
    py.chmod(0o755)
    return mesa


def irods_env_file(home: Path, user: str = "iuser", password: str = "i-pass") -> None:
    from irods.password_obfuscation import encode

    d = home / ".irods"
    d.mkdir()
    (d / "irods_environment.json").write_text(json.dumps({"irods_user_name": user, "irods_zone_name": "iplant",
                                                          "irods_host": "data.cyverse.org", "irods_port": 1247}))
    (d / ".irodsA").write_text(encode(password))


def run(tmp_path: Path, extra: dict[str, str], *, mesa: bool = True) -> tuple[int, dict[str, str], str]:
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    base = {"PATH": "/usr/bin:/bin", "HOME": str(home), "NEON_DUCKLAKE_PYTHON": sys.executable,
            "MESA_HOME": str(fake_mesa(home) if mesa and not (home / ".mesa").exists() else home / ".mesa")}
    p = subprocess.run(["bash", str(SCRIPT), *DUMP], env=base | extra, capture_output=True, text=True, timeout=60,
                       check=False)
    out = json.loads(p.stdout) if p.returncode == 0 else {}
    return p.returncode, out, p.stderr


def test_secrets_mode_and_sources(tmp_path: Path) -> None:
    assert env.secrets_mode({}) == "auto" and env.secrets_mode({"NEON_DUCKLAKE_SECRETS": " ENV "}) == "env"
    with pytest.raises(SystemExit, match="auto, env, keyring"):
        env.secrets_mode({"NEON_DUCKLAKE_SECRETS": "vault"})
    home = tmp_path / "h"
    home.mkdir()
    both = {"NEON_TOKEN": "t", "MESA_MCP_IRODS__USER": "u", "MESA_MCP_IRODS__PASSWORD": "p"}
    assert env.secrets_sources({}, home) == {"mode": "auto", "neon": "keyring", "irods": "keyring"}
    assert env.secrets_sources(both, home) == {"mode": "auto", "neon": "env", "irods": "env"}
    assert env.secrets_sources(both | {"NEON_DUCKLAKE_SECRETS": "keyring"}, home)["irods"] == "keyring"
    assert env.secrets_sources({"NEON_DUCKLAKE_SECRETS": "env"}, home) == {"mode": "env", "neon": "missing",
                                                                           "irods": "missing"}
    irods_env_file(home)
    assert env.secrets_sources({"NEON_TOKEN": "t"}, home) == {"mode": "auto", "neon": "env", "irods": "irods-env-file"}
    files = env.irods_env_files({"IRODS_ENVIRONMENT_FILE": "/x/e.json", "IRODS_AUTHENTICATION_FILE": "/x/a"}, home)
    assert files == (Path("/x/e.json"), Path("/x/a"))


def test_env_mode_uses_the_environment_and_never_the_keyring(tmp_path: Path) -> None:
    rc, out, err = run(tmp_path, {"NEON_DUCKLAKE_SECRETS": "env", "NEON_TOKEN": "env-token",
                                  "MESA_MCP_IRODS__USER": "envuser", "MESA_MCP_IRODS__PASSWORD": "env-pass"},
                       mesa=False)
    assert rc == 0, err
    assert out["NEON_TOKEN"] == "env-token" and out["MESA_MCP_IRODS__PASSWORD"] == "env-pass"
    assert out["CYVERSE_USERNAME"] == "envuser" and out["MESA_MCP_DUCKLAKE__CATALOG_DSN"].startswith("duckdb:///")
    assert "env-pass" not in err and "env-token" not in err


def test_env_mode_missing_secrets_exit_codes(tmp_path: Path) -> None:
    rc, _, err = run(tmp_path, {"NEON_DUCKLAKE_SECRETS": "env"}, mesa=False)
    assert rc == 3 and "NEON_TOKEN" in err
    rc, _, err = run(tmp_path, {"NEON_DUCKLAKE_SECRETS": "env", "NEON_TOKEN": "t"}, mesa=False)
    assert rc == 4 and "iinit" in err
    rc, _, err = run(tmp_path, {"NEON_DUCKLAKE_SECRETS": "sometimes"}, mesa=False)
    assert rc == 2 and "auto, env or keyring" in err


def test_env_mode_reads_the_irods_environment_file(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    irods_env_file(home, "iuser", "s3cret with spaces")
    rc, out, err = run(tmp_path, {"NEON_DUCKLAKE_SECRETS": "env", "NEON_TOKEN": "t"}, mesa=False)
    assert rc == 0, err
    assert out["MESA_MCP_IRODS__USER"] == "iuser" and out["MESA_MCP_IRODS__PASSWORD"] == "s3cret with spaces"
    assert out["CYVERSE_USERNAME"] == "iuser" and out["MESA_MCP_IRODS__ZONE"] == "iplant"
    assert out["MESA_MCP_IRODS__HOST"] == "data.cyverse.org" and "s3cret" not in err


def test_keyring_mode_ignores_the_environment(tmp_path: Path) -> None:
    rc, out, err = run(tmp_path, {"NEON_DUCKLAKE_SECRETS": "keyring", "NEON_TOKEN": "env-token",
                                  "MESA_MCP_IRODS__USER": "envuser", "MESA_MCP_IRODS__PASSWORD": "env-pass"})
    assert rc == 0, err
    assert out["NEON_TOKEN"] == "kr-token" and out["MESA_MCP_IRODS__PASSWORD"] == "kr-pass"
    assert out["MESA_MCP_IRODS__USER"] == "kruser"


def test_auto_mode_mixes_per_secret(tmp_path: Path) -> None:
    rc, out, err = run(tmp_path, {})  # sparky-1: nothing in the environment -> the keyring for both
    assert rc == 0, err
    assert (out["NEON_TOKEN"], out["MESA_MCP_IRODS__USER"], out["MESA_MCP_IRODS__PASSWORD"]) == \
        ("kr-token", "kruser", "kr-pass")
    rc, out, err = run(tmp_path, {"NEON_TOKEN": "env-token"})  # token from env, password from the keyring
    assert rc == 0 and out["NEON_TOKEN"] == "env-token" and out["MESA_MCP_IRODS__PASSWORD"] == "kr-pass"


def test_auto_mode_without_a_mesa_install_needs_env_secrets(tmp_path: Path) -> None:
    rc, _, err = run(tmp_path, {}, mesa=False)
    assert rc == 2 and "NEON_DUCKLAKE_SECRETS=env" in err
    rc, out, _ = run(tmp_path, {"NEON_TOKEN": "t", "MESA_MCP_IRODS__USER": "u", "MESA_MCP_IRODS__PASSWORD": "p"},
                     mesa=False)
    assert rc == 0 and out["MESA_MCP_IRODS__USER"] == "u"
