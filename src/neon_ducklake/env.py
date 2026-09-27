"""Shared runtime: paths, NEON clients (one per role, sharing one rate limiter), MESA/iRODS bootstrap.

Secrets arrive only through the environment set by ``scripts/with-secrets.sh``; nothing here prints them.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(os.environ.get("NEON_DUCKLAKE_ROOT", "/home/tswetnam/neon-ducklake"))
IRODS_ROOT = os.environ.get("NEON_DUCKLAKE_IRODS_ROOT", "/iplant/home/tswetnam/neon-ducklake")
ACTOR = "tswetnam"
API_DIR = ROOT / "api"
RUNS_DIR = API_DIR / "runs"
LAKE_DIR = ROOT / "ducklake"
CATALOG_LAKE = "neon_catalog.ducklake"
SITES_DIR = ROOT / "sites"
WORK_DIR = ROOT / "work"
LOCK_DIR = WORK_DIR / "locks"
MESA_CACHE_DIR = WORK_DIR / "mesa-cache"
QUARANTINE_DIR = WORK_DIR / "quarantine"
LOG_DIR = ROOT / "logs"
PRECEDENT_LAKE = Path("/home/tswetnam/neon-downloads/ducklake/neon_spatial.ducklake")
PRECEDENT_DATA = Path("/home/tswetnam/neon-downloads/ducklake/data/")
PRECEDENT_VERSION = 90
NEON_ROR = "https://ror.org/04j43p132"
DUCKDB_SETTINGS = {"memory_limit": "40GB", "threads": 8}

REPOS = {
    "neon-mcp": Path("/home/tswetnam/github/idss-mesa/neon-mcp"),
    "mesa-mcp": Path("/home/tswetnam/.mesa/repos/mesa-mcp"),
    "mesa-ducklake": Path("/home/tswetnam/.mesa/repos/mesa-ducklake"),
    "neon-ducklake": ROOT,
}

log = logging.getLogger("neon_ducklake")


def utcnow() -> datetime:
    return datetime.now(UTC)


def run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%SZ")


def setup_logging(level: str = "warning") -> None:
    """neon-mcp's logging (adds the secret filter, silences httpx URL logs) + our own logger."""
    from neon_mcp.logging import setup_logging as neon_setup

    neon_setup(level, "stdio")
    for noisy in ("httpx", "httpcore", "irods"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    root = logging.getLogger("neon_ducklake")
    if not root.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root.addHandler(h)
    root.setLevel(logging.INFO)
    root.propagate = False


def repo_versions() -> dict[str, str]:
    """git SHA (+ '-dirty') of every code tree we run, for provenance."""
    out: dict[str, str] = {}
    for name, path in REPOS.items():
        try:
            sha = subprocess.run(["git", "-C", str(path), "rev-parse", "--short", "HEAD"],
                                 capture_output=True, text=True, check=True).stdout.strip()
            dirty = subprocess.run(["git", "-C", str(path), "status", "--porcelain", "--untracked-files=no"],
                                   capture_output=True, text=True, check=True).stdout.strip()
            out[name] = sha + ("-dirty" if dirty else "")
        except (subprocess.CalledProcessError, FileNotFoundError):
            out[name] = "unknown"
    return out


def tool_versions() -> dict[str, str]:
    import duckdb
    import irods
    import neon_mcp

    v = {"duckdb": duckdb.__version__, "python-irodsclient": getattr(irods, "__version__", "3.3.0"),
         "neon-mcp": getattr(neon_mcp, "__version__", "?")}
    try:
        con = duckdb.connect()
        con.execute("LOAD ducklake")
        row = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'").fetchone()
        v["ducklake"] = row[0] if row else "?"
        con.close()
    except Exception:  # noqa: BLE001
        v["ducklake"] = "?"
    return v


# ---------------------------------------------------------------- locks

@contextlib.contextmanager
def file_lock(name: str, *, blocking: bool = False) -> Iterator[None]:
    """Process-level exclusive lock in work/locks (fcntl; released on exit/crash)."""
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    path = LOCK_DIR / f"{name}.lock"
    fh = path.open("a+")
    try:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(fh, flags)
        except BlockingIOError as exc:
            raise SystemExit(f"another neon-ducklake process holds the '{name}' lock ({path})") from exc
        fh.seek(0)
        fh.truncate()
        fh.write(f"{os.getpid()}\n")
        fh.flush()
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


# ---------------------------------------------------------------- NEON

@dataclass
class NeonBundle:
    """One shared rate limiter + cache; one NeonClient per role (separate connection pools)."""

    config: Any
    token: Any
    clients: dict[str, Any]

    def client(self, role: str) -> Any:
        return self.clients[role]

    async def aclose(self) -> None:
        for c in self.clients.values():
            await c.aclose()


ROLE_SETTINGS: dict[str, dict[str, Any]] = {
    # Phase-1 harvest: few internal retries so the app can bisect a slow pair quickly.
    "harvest": {"max_attempts": 2, "read_timeout_s": 180.0},
    # Phase-2 listing (POST /data/query per product) – separate pool from downloads.
    "list": {"max_attempts": 3, "read_timeout_s": 300.0},
    # GCS downloads: retries at the request level; per-file retry loop in site/fetch.py.
    "download": {"max_attempts": 5, "read_timeout_s": 300.0},
    # Small metadata calls (preflight, HEADs).
    "meta": {"max_attempts": 4, "read_timeout_s": 120.0},
}


def neon_bundle(roles: tuple[str, ...] = ("harvest", "meta"), *, require_token: bool = True) -> NeonBundle:
    from neon_mcp.config import load_config
    from neon_mcp.neon.auth import resolve_token
    from neon_mcp.neon.cache import TTLCache
    from neon_mcp.neon.client import NeonClient
    from neon_mcp.neon.ratelimit import RateLimiter

    cfg = load_config()
    token = resolve_token(cfg, transport="stdio", request_headers=None)
    if require_token and token is None:
        raise SystemExit("NEON token missing: run through scripts/with-secrets.sh")
    cache = TTLCache(cfg.cache)
    limiter = RateLimiter(cfg.neon.rate_limit, max_concurrency=max(cfg.neon.max_concurrency, 8) * len(roles))
    clients = {}
    for role in roles:
        ncfg = cfg.neon.model_copy(deep=True)
        s = ROLE_SETTINGS[role]
        ncfg.retries.max_attempts = s["max_attempts"]
        ncfg.read_timeout_s = s["read_timeout_s"]
        ncfg.download_read_timeout_s = max(ncfg.download_read_timeout_s, s["read_timeout_s"])
        clients[role] = NeonClient(ncfg, cache=cache, limiter=limiter,
                                   send_token_on_public=cfg.effective_token_for_public("stdio"),
                                   allowed_hosts=cfg.downloads.allowed_hosts)
    return NeonBundle(config=cfg, token=token, clients=clients)


# ---------------------------------------------------------------- MESA / iRODS

@dataclass
class Mesa:
    config: Any
    auth: Any
    session: Any  # main pooled iRODS session

    def clone(self) -> Any:
        return self.session.clone()


def mesa() -> Mesa:
    from mesa_mcp.auth.extract import extract_from_env
    from mesa_mcp.config import load_config, set_active_config
    from mesa_mcp.irods.client_pool import default_pool

    cfg = load_config()
    set_active_config(cfg)
    auth = extract_from_env(cfg)
    if auth.username in (None, "", "anonymous"):
        raise SystemExit("iRODS credentials missing: run through scripts/with-secrets.sh")
    session = default_pool().get(auth)
    return Mesa(config=cfg, auth=auth, session=session)


_DUCKLAKE_CLIENT: Any = None


def mesa_ducklake_client(m: Mesa) -> Any:
    """The ONE mesa-ducklake client of this process, also installed as mesa-mcp's default.

    Opening it takes the single-writer lock on ~/.mesa/ducklake/catalog.duckdb, so callers take
    the global 'mesa-tag' file lock first and keep the window short.
    """
    global _DUCKLAKE_CLIENT
    if _DUCKLAKE_CLIENT is None:
        from mesa_ducklake import DuckLakeClient
        from mesa_mcp.ducklake.client import set_default_client

        MESA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        dsn = m.config.ducklake.catalog_dsn
        if not dsn:
            raise SystemExit("MESA_MCP_DUCKLAKE__CATALOG_DSN is not set")
        _DUCKLAKE_CLIENT = DuckLakeClient(catalog_dsn=dsn, irods_session=None,
                                          cache_dir=str(MESA_CACHE_DIR), cache_cap_bytes=0)
        set_default_client(_DUCKLAKE_CLIENT)
    return _DUCKLAKE_CLIENT


def close_mesa_ducklake_client() -> None:
    global _DUCKLAKE_CLIENT
    if _DUCKLAKE_CLIENT is not None:
        from mesa_mcp.ducklake.client import set_default_client

        _DUCKLAKE_CLIENT.close()
        set_default_client(None)
        _DUCKLAKE_CLIENT = None


def catalog_lock_holders(path: Path | None = None) -> list[dict[str, str]]:
    """Processes (other than us) with the MESA DuckDB catalog open, via /proc/*/fd."""
    target = str(path or Path.home() / ".mesa/ducklake/catalog.duckdb")
    me = os.getpid()
    out = []
    for pid_dir in Path("/proc").iterdir():
        if not pid_dir.name.isdigit() or int(pid_dir.name) == me:
            continue
        try:
            for fd in (pid_dir / "fd").iterdir():
                try:
                    if os.readlink(fd) == target:
                        cmd = (pid_dir / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
                        out.append({"pid": pid_dir.name, "cmd": cmd[:200]})
                        break
                except OSError:
                    continue
        except (PermissionError, FileNotFoundError):
            continue
    return out


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    tmp.write_text(json.dumps(obj, indent=1, default=str, ensure_ascii=False))
    os.replace(tmp, path)
