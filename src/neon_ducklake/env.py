"""Shared runtime: paths, NEON clients (one per role, sharing one rate limiter), MESA/iRODS bootstrap.

Secrets arrive only through the environment set by ``scripts/with-secrets.sh``; nothing here prints them.

Every location is configurable; the defaults need no user-specific literal:

==================================== =========================================================================
``NEON_DUCKLAKE_ROOT``               data root (default: the repository checkout, i.e. the parent of ``src/``)
``NEON_DUCKLAKE_IRODS_ROOT``         iRODS project collection (default ``/iplant/home/<user>/neon-ducklake``)
``CYVERSE_USERNAME``                 CyVerse user; else ``MESA_MCP_IRODS__USER``, else the first line of
                                     ``$MESA_HOME/cyverse-user`` (written by the MESA local install)
``MESA_HOME``                        MESA local install (default ``~/.mesa``)
``NEON_DUCKLAKE_PRECEDENT_LAKE``     neon-downloads spatial lake whose ``neon_*`` layers the catalog build copies
                                     (default ``~/neon-downloads/ducklake/neon_spatial.ducklake``; optional:
                                     absent or ``none`` skips the copy with a warning)
``NEON_DUCKLAKE_PRECEDENT_DATA``     its DATA_PATH (default ``<lake dir>/data/``)
``NEON_DUCKLAKE_PRECEDENT_VERSION``  its pinned snapshot (default 90)
``NEON_DUCKLAKE_PRECEDENT_IRODS``    its iRODS path, for ``provenance.derivedFrom``
                                     (default ``/iplant/home/<user>/neon-downloads/ducklake/neon_spatial.ducklake``)
==================================== =========================================================================

``IRODS_ROOT`` and ``ACTOR`` need the CyVerse user. When it cannot be resolved, reading either raises a
``SystemExit`` that says how to set it, so offline commands still work and iRODS work fails clearly.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import socket
import subprocess
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CODE_ROOT = Path(__file__).resolve().parents[2]  # the checkout (pyproject.toml, src/, sql/) when not installed


def _default_root() -> Path:
    if (CODE_ROOT / "pyproject.toml").is_file() and (CODE_ROOT / "src" / "neon_ducklake").is_dir():
        return CODE_ROOT
    return Path.cwd()


def _opt_path(var: str, default: Path | None) -> Path | None:
    """``$var`` as a path; unset -> ``default``; empty/``none``/``off`` -> None (feature disabled)."""
    raw = os.environ.get(var)
    if raw is None:
        return default
    raw = raw.strip()
    if raw.lower() in ("", "none", "off", "0", "false"):
        return None
    return Path(raw).expanduser()


ROOT = Path(os.environ.get("NEON_DUCKLAKE_ROOT") or _default_root()).expanduser()
MESA_HOME = Path(os.environ.get("MESA_HOME") or Path.home() / ".mesa").expanduser()
IRODS_ZONE_HOME = "/iplant/home"
USER_HINT = ("set CYVERSE_USERNAME (or run through scripts/with-secrets.sh, which sets MESA_MCP_IRODS__USER), "
             "write the user name to $MESA_HOME/cyverse-user, or set NEON_DUCKLAKE_IRODS_ROOT")


def cyverse_user(environ: Mapping[str, str] | None = None, mesa_home: Path | None = None) -> str | None:
    """``$CYVERSE_USERNAME``, ``$MESA_MCP_IRODS__USER`` (not ``anonymous``), then ``$MESA_HOME/cyverse-user``."""
    e = os.environ if environ is None else environ
    for var in ("CYVERSE_USERNAME", "MESA_MCP_IRODS__USER"):
        v = (e.get(var) or "").strip()
        if v and v != "anonymous":
            return v
    try:
        lines = ((mesa_home or MESA_HOME) / "cyverse-user").read_text().splitlines()
    except OSError:
        return None
    v = lines[0].strip() if lines else ""
    return v or None


def _user_from_irods_path(path: str | None) -> str | None:
    m = re.match(r"^/[^/]+/home/([^/]+)(/|$)", path or "")
    return m.group(1) if m else None


CYVERSE_USER = cyverse_user()
_IRODS_ROOT = os.environ.get("NEON_DUCKLAKE_IRODS_ROOT") or (
    f"{IRODS_ZONE_HOME}/{CYVERSE_USER}/neon-ducklake" if CYVERSE_USER else None)
_ACTOR = CYVERSE_USER or _user_from_irods_path(_IRODS_ROOT)
# Only defined when resolvable; otherwise module __getattr__ below raises a clear SystemExit on first use.
if _IRODS_ROOT:
    IRODS_ROOT: str = _IRODS_ROOT.rstrip("/")
if _ACTOR:
    ACTOR: str = _ACTOR  # snapshot author / MESA history actor / dc.creator: the iRODS user doing the writes

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
MESA_CATALOG = MESA_HOME / "ducklake" / "catalog.duckdb"
PRECEDENT_LAKE = _opt_path("NEON_DUCKLAKE_PRECEDENT_LAKE",
                           Path.home() / "neon-downloads" / "ducklake" / "neon_spatial.ducklake")
PRECEDENT_DATA = _opt_path("NEON_DUCKLAKE_PRECEDENT_DATA", PRECEDENT_LAKE.parent / "data" if PRECEDENT_LAKE else None)
PRECEDENT_VERSION = int(os.environ.get("NEON_DUCKLAKE_PRECEDENT_VERSION") or 90)
PRECEDENT_IRODS = os.environ.get("NEON_DUCKLAKE_PRECEDENT_IRODS") or (
    f"{IRODS_ZONE_HOME}/{CYVERSE_USER}/neon-downloads/ducklake/neon_spatial.ducklake" if CYVERSE_USER else None)
NEON_ROR = "https://ror.org/04j43p132"
DUCKDB_SETTINGS = {"memory_limit": "40GB", "threads": 8}

# The three code dependencies (installed from the git revs pinned in pyproject [tool.uv.sources]).
DEPENDENCIES = {"neon-mcp": "neon_mcp", "mesa-mcp": "mesa_mcp", "mesa-ducklake": "mesa_ducklake"}
SELF = "neon-ducklake"

log = logging.getLogger("neon_ducklake")


def __getattr__(name: str) -> Any:
    if name in ("IRODS_ROOT", "ACTOR"):
        raise SystemExit(f"neon-ducklake: cannot resolve the CyVerse user for {name}: {USER_HINT}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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


# ---------------------------------------------------------------- versions (provenance)

def git_state(path: Path) -> tuple[str, bool] | None:
    """(full HEAD SHA, dirty) of the git work tree at ``path``; None when it is not one."""
    try:
        sha = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"],
                             capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(path), "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, check=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, NotADirectoryError):
        return None
    return (sha, bool(dirty)) if sha else None


@dataclass(frozen=True)
class DistInfo:
    """Where an installed distribution came from (PEP 610 ``direct_url.json``)."""

    name: str
    version: str | None = None  # distribution version; None = not installed
    commit: str | None = None  # full git commit: vcs_info.commit_id, or HEAD of an editable git checkout
    dirty: bool = False  # editable checkout with uncommitted changes
    source: str | None = None  # VCS URL or local path; None for an index/wheel install
    editable: bool = False
    requested: str | None = None  # requested revision (vcs_info.requested_revision)

    @property
    def short(self) -> str:
        """``a5949d2`` / ``a5949d2-dirty`` when the commit is known, else the version (or ``unknown``)."""
        if self.commit:
            return self.commit[:7] + ("-dirty" if self.dirty else "")
        return self.version or "unknown"

    def describe(self) -> str:
        if self.version is None:
            return "not installed"
        how = "editable " if self.editable else ""
        where = f" from {how}{self.source}" if self.source else " from the package index"
        return f"{self.version} ({self.short}){where}"


def _file_url_path(url: str) -> Path:
    from urllib.parse import unquote, urlparse

    return Path(unquote(urlparse(url).path))


def dist_info(name: str) -> DistInfo:
    """Version and provenance of an installed distribution (``neon-mcp``, ``mesa-mcp``, ...)."""
    from importlib import metadata

    try:
        dist = metadata.distribution(name)
    except metadata.PackageNotFoundError:
        return DistInfo(name)
    version = dist.version
    try:
        raw = dist.read_text("direct_url.json")
        direct = json.loads(raw) if raw else {}
    except (OSError, ValueError):
        direct = {}
    url = direct.get("url") or ""
    vcs = direct.get("vcs_info") or {}
    if vcs:
        return DistInfo(name, version, commit=vcs.get("commit_id"), source=url or None,
                        requested=vcs.get("requested_revision"))
    if url.startswith("file://"):
        path = _file_url_path(url)
        editable = bool((direct.get("dir_info") or {}).get("editable"))
        g = git_state(path) if path.is_dir() else None
        return DistInfo(name, version, commit=g[0] if g else None, dirty=g[1] if g else False,
                        source=str(path), editable=editable)
    return DistInfo(name, version, source=url or None)


def self_info() -> DistInfo:
    """This package: the git state of the checkout it runs from, else its distribution metadata."""
    g = git_state(CODE_ROOT) if (CODE_ROOT / "src" / "neon_ducklake").is_dir() else None
    if g:
        d = dist_info(SELF)
        return DistInfo(SELF, d.version, commit=g[0], dirty=g[1], source=str(CODE_ROOT), editable=True)
    return dist_info(SELF)


def dependency_infos() -> dict[str, DistInfo]:
    """The three dependencies and this package, in a fixed order."""
    out = {name: dist_info(name) for name in DEPENDENCIES}
    out[SELF] = self_info()
    return out


def repo_versions() -> dict[str, str]:
    """Short git commit (+ ``-dirty``) of every code tree we run, else its version, for provenance."""
    return {name: d.short for name, d in dependency_infos().items()}


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
def file_lock(name: str, *, blocking: bool = False, timeout_s: float | None = None,
              poll_s: float = 0.5) -> Iterator[None]:
    """Process-level exclusive lock in work/locks (fcntl; released on exit/crash).

    ``blocking=False``: fail at once (``SystemExit``) when another process holds it. ``blocking=True``: wait;
    with ``timeout_s`` the wait is bounded (polled every ``poll_s``) and ends in ``SystemExit``.
    """
    import time

    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    path = LOCK_DIR / f"{name}.lock"
    fh = path.open("a+")
    try:
        if blocking and timeout_s is None:
            fcntl.flock(fh, fcntl.LOCK_EX)
        else:
            deadline = time.monotonic() + (timeout_s or 0.0) if blocking else None
            while True:
                try:
                    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if deadline is None or time.monotonic() >= deadline:
                        waited = f" after waiting {timeout_s:.0f}s" if deadline is not None else ""
                        raise SystemExit(f"another neon-ducklake process holds the '{name}' lock ({path})"
                                         f"{waited}") from exc
                    time.sleep(max(0.01, min(poll_s, deadline - time.monotonic())))
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

    Opening it takes the single-writer lock on $MESA_HOME/ducklake/catalog.duckdb, so callers take
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
    target = str(path or MESA_CATALOG)
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


# ---------------------------------------------------------------- multi-VM identity and secrets mode

VM_ID_ENV = "NEON_DUCKLAKE_VM_ID"
SECRETS_ENV = "NEON_DUCKLAKE_SECRETS"
SECRETS_MODES = ("auto", "env", "keyring")
_VM_ID_BAD = re.compile(r"[^A-Za-z0-9._-]+")


MESA_LOCK_TIMEOUT_ENV = "NEON_DUCKLAKE_MESA_LOCK_TIMEOUT_S"
MESA_LOCK_TIMEOUT_DEFAULT_S = 1800.0


def mesa_lock_timeout(environ: Mapping[str, str] | None = None) -> float:
    """``$NEON_DUCKLAKE_MESA_LOCK_TIMEOUT_S`` (default 1800): how long history/recorder wait for ``mesa-tag``."""
    e = os.environ if environ is None else environ
    raw = (e.get(MESA_LOCK_TIMEOUT_ENV) or "").strip()
    if not raw:
        return MESA_LOCK_TIMEOUT_DEFAULT_S
    try:
        v = float(raw)
    except ValueError as exc:
        raise SystemExit(f"{MESA_LOCK_TIMEOUT_ENV}={raw!r} is not a number of seconds") from exc
    return max(0.0, v)


def _read_id(*paths: str) -> str:
    for p in paths:
        try:
            v = Path(p).read_text().strip()
        except OSError:
            continue
        if v:
            return v
    return ""


def machine_id() -> str:
    """``/etc/machine-id`` (or the D-Bus copy); "" when unavailable. Stable across reboots of one machine."""
    return _read_id("/etc/machine-id", "/var/lib/dbus/machine-id")


def boot_id() -> str:
    """``/proc/sys/kernel/random/boot_id``; "" when unavailable. New at every boot of the kernel."""
    return _read_id("/proc/sys/kernel/random/boot_id")


def vm_id(environ: Mapping[str, str] | None = None) -> str:
    """This worker's identity in Data Store claims and spool paths (docs/STREAMING.md).

    ``$NEON_DUCKLAKE_VM_ID`` when set, else the short host name. Characters outside ``[A-Za-z0-9._-]``
    become ``-`` so the id is always a safe iRODS path component.
    """
    e = os.environ if environ is None else environ
    raw = (e.get(VM_ID_ENV) or "").strip() or socket.gethostname().split(".")[0]
    return _VM_ID_BAD.sub("-", raw).strip("-.") or "unknown-vm"


def secrets_mode(environ: Mapping[str, str] | None = None) -> str:
    """``$NEON_DUCKLAKE_SECRETS``: ``auto`` (default), ``env`` or ``keyring``; anything else is an error.

    ``scripts/with-secrets.sh`` implements the modes; :func:`secrets_sources` mirrors its choice.
    """
    e = os.environ if environ is None else environ
    raw = (e.get(SECRETS_ENV) or "auto").strip().lower() or "auto"
    if raw not in SECRETS_MODES:
        raise SystemExit(f"{SECRETS_ENV}={raw!r}: choose one of {', '.join(SECRETS_MODES)}")
    return raw


def irods_env_files(environ: Mapping[str, str] | None = None, home: Path | None = None) -> tuple[Path, Path]:
    """(``irods_environment.json``, ``.irodsA``) honouring ``IRODS_ENVIRONMENT_FILE`` / ``IRODS_AUTHENTICATION_FILE``."""
    e = os.environ if environ is None else environ
    base = (home or Path.home()) / ".irods"
    env_file = Path(e["IRODS_ENVIRONMENT_FILE"]).expanduser() if e.get("IRODS_ENVIRONMENT_FILE") \
        else base / "irods_environment.json"
    auth_file = Path(e["IRODS_AUTHENTICATION_FILE"]).expanduser() if e.get("IRODS_AUTHENTICATION_FILE") \
        else base / ".irodsA"
    return env_file, auth_file


def secrets_sources(environ: Mapping[str, str] | None = None, home: Path | None = None) -> dict[str, str]:
    """Where ``with-secrets.sh`` takes each secret from, without reading any secret.

    ``{"mode": ..., "neon": "env"|"keyring"|"missing", "irods": "env"|"irods-env-file"|"keyring"|"missing"}``.

    - ``keyring``: both from the login keyring (the sparky-1 behaviour).
    - ``env``: ``NEON_TOKEN`` and ``MESA_MCP_IRODS__USER``/``__PASSWORD`` from the environment, else the
      iRODS environment file with ``.irodsA`` (``iinit``); the keyring is never touched.
    - ``auto``: per secret, the environment (or the iRODS environment file) when present, else the keyring.
    """
    e = os.environ if environ is None else environ
    mode = secrets_mode(e)
    env_file, auth_file = irods_env_files(e, home)
    have_token = bool((e.get("NEON_TOKEN") or "").strip())
    have_irods_env = bool((e.get("MESA_MCP_IRODS__USER") or "").strip() and (e.get("MESA_MCP_IRODS__PASSWORD") or ""))
    have_irods_file = env_file.is_file() and auth_file.is_file()
    if mode == "keyring":
        return {"mode": mode, "neon": "keyring", "irods": "keyring"}
    neon = "env" if have_token else ("missing" if mode == "env" else "keyring")
    irods = "env" if have_irods_env else "irods-env-file" if have_irods_file else (
        "missing" if mode == "env" else "keyring")
    return {"mode": mode, "neon": neon, "irods": irods}
