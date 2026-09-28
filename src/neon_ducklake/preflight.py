"""``neon-ducklake preflight [--site SITE]``: read-only readiness checks (plan § Run order, P0).

Nothing here writes to iRODS, NEON or MESA. The MESA catalog is opened READ-ONLY with a plain
``duckdb.connect(read_only=True)`` — only when no other process holds it and this process has not
opened it — and closed right away. Prints a table, writes ``work/preflight-<ts>.json`` and exits 1
on any hard failure. Secrets are never printed.

The code dependencies (neon-mcp, mesa-mcp, mesa-ducklake) must import; each reports its version and git
commit (PEP 610 ``direct_url.json``). A commit that differs from the rev pinned in ``pyproject.toml``
``[tool.uv.sources]`` (e.g. a local editable checkout) is a warning, not a failure.
"""

from __future__ import annotations

import asyncio
import email.utils
import json
import logging
import shutil
import time
import urllib.parse
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from . import env

log = logging.getLogger("neon_ducklake.preflight")

MIN_IRODS = (4, 2, 8)
DEFAULT_PLANNED_BYTES = 250 * 10**9
DISK_FACTOR = 1.5
MAX_SKEW_S = 60.0
PROBE = {"productCode": "DP1.00002.001", "siteCodes": ["SRER"], "startDateMonth": "2024-06",
         "endDateMonth": "2024-06", "package": "basic", "includeProvisional": True}


@dataclass
class Check:
    name: str
    status: str  # ok | warn | fail | info
    detail: str
    data: dict[str, Any] = field(default_factory=dict)


class Report:
    def __init__(self) -> None:
        self.checks: list[Check] = []

    def add(self, name: str, status: str, detail: str, **data: Any) -> Check:
        c = Check(name, status, detail, data)
        self.checks.append(c)
        return c

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if c.status == "fail"]


def _catalog_path(dsn: str | None) -> Path:
    if dsn and dsn.startswith("duckdb://"):
        rest = dsn[len("duckdb://"):]
        return Path("/" + rest.lstrip("/")) if rest.startswith("/") else Path(rest).expanduser()
    return env.MESA_CATALOG


def pinned_revs(pyproject: Path | None = None) -> dict[str, str | None] | None:
    """``[tool.uv.sources]`` git revs of the dependencies (None for a non-git source); None without pyproject."""
    import tomllib

    path = pyproject or env.CODE_ROOT / "pyproject.toml"
    try:
        data = tomllib.loads(path.read_text())
    except (OSError, ValueError):
        return None
    sources = ((data.get("tool") or {}).get("uv") or {}).get("sources") or {}
    return {name: (sources.get(name) or {}).get("rev") for name in env.DEPENDENCIES}


def dependency_check(info: env.DistInfo, pinned: str | None, have_pins: bool) -> tuple[str, str]:
    """(status, detail) for one dependency: warn, never fail, when it is not the pinned commit."""
    detail = info.describe()
    if not have_pins:
        return "info", detail + "; no pyproject.toml next to the code, pinned rev unknown"
    if pinned is None:
        return "warn", detail + "; pyproject pins no git rev for it (local source?)"
    if info.commit is None:
        return "warn", detail + f"; commit unknown, pinned {pinned[:7]}"
    if not info.commit.startswith(pinned) and not pinned.startswith(info.commit):
        return "warn", detail + f"; differs from the pinned rev {pinned[:7]}"
    if info.dirty:
        return "warn", detail + "; pinned commit but the checkout has uncommitted changes"
    return "ok", detail


def _irods_root() -> str | None:
    try:
        return env.IRODS_ROOT
    except SystemExit:
        return None


# ---------------------------------------------------------------- local checks

def check_identity(r: Report) -> None:
    """The CyVerse user (snapshot author / MESA actor) and the iRODS project root, both resolved in env."""
    root = _irods_root()
    try:
        actor = env.ACTOR
    except SystemExit:
        actor = None
    if root and actor:
        r.add("CyVerse user", "ok", f"{actor}; project root {root}", user=actor, irods_root=root)
    else:
        r.add("CyVerse user", "fail", f"not resolvable: {env.USER_HINT}")

def check_imports(r: Report) -> None:
    import importlib

    pins = pinned_revs()
    for dist, mod in (*env.DEPENDENCIES.items(), (env.SELF, "neon_ducklake")):
        try:
            m = importlib.import_module(mod)
        except Exception as exc:  # noqa: BLE001
            r.add(f"module {mod}", "fail", f"import failed: {type(exc).__name__}: {exc}")
            continue
        f = str(Path(m.__file__ or "").resolve())
        if dist == env.SELF:
            info = env.self_info()
            status, detail = "ok", info.describe()
        else:
            info = env.dist_info(dist)
            status, detail = dependency_check(info, (pins or {}).get(dist), pins is not None)
        r.add(f"module {mod}", status, detail, file=str(f), version=info.version, commit=info.commit,
              dirty=info.dirty, source=info.source, editable=info.editable,
              pinned=(pins or {}).get(dist) if dist != env.SELF else None)
    for mod, want in (("duckdb", "1.5.5"), ("irods", "3.3.0"), ("pyarrow", None), ("h5py", None), ("numpy", None)):
        try:
            m = importlib.import_module(mod)
            v = getattr(m, "__version__", "?")
            ok = want is None or v == want
            r.add(f"import {mod}", "ok" if ok else "fail", f"{v}" + ("" if ok else f" (want {want})"),
                  version=v, file=str(m.__file__))
        except Exception as exc:  # noqa: BLE001
            r.add(f"import {mod}", "fail", f"{type(exc).__name__}: {exc}")


def check_duckdb_extensions(r: Report) -> None:
    import duckdb

    con = duckdb.connect()
    try:
        for ext in ("ducklake", "spatial"):
            try:
                con.execute(f"LOAD {ext}")
                v = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name=?",
                                [ext]).fetchone()
                r.add(f"LOAD {ext}", "ok", f"version {v[0] if v else '?'}")
            except Exception as exc:  # noqa: BLE001
                r.add(f"LOAD {ext}", "fail", f"{type(exc).__name__}: {exc} (never INSTALL; check ~/.duckdb)")
    finally:
        con.close()


def check_disk(r: Report, site: str | None) -> None:
    planned, source = DEFAULT_PLANNED_BYTES, "default 250 GB"
    if site:
        from .avu.vocab import open_catalog_lake_ro

        lake = open_catalog_lake_ro()
        if lake is not None:
            try:
                row = lake.execute("SELECT sum(totalBytes) FROM neon.main.v_site_inventory WHERE siteCode=?",
                                   [site]).fetchone()
                if row and row[0]:
                    planned, source = int(row[0]), f"v_site_inventory {site}"
            except Exception as exc:  # noqa: BLE001
                log.info("site plan unavailable: %s", exc)
            finally:
                lake.close()
    free = shutil.disk_usage(env.ROOT).free
    need = int(planned * DISK_FACTOR)
    r.add("disk free", "ok" if free >= need else "fail",
          f"{free / 1e9:.0f} GB free; need {need / 1e9:.0f} GB (1.5 x {planned / 1e9:.1f} GB, {source})",
          free=free, planned=planned)


def check_parts(r: Report) -> None:
    parts = sorted(env.SITES_DIR.glob("*/raw/**/*.part")) if env.SITES_DIR.exists() else []
    if not parts:
        r.add("stale *.part", "ok", "none under sites/*/raw")
        return
    now = time.time()
    old = [p for p in parts if now - p.stat().st_mtime > 3600]
    r.add("stale *.part", "warn", f"{len(parts)} .part files ({len(old)} older than 1 h) under sites/*/raw",
          sample=[str(p.relative_to(env.ROOT)) for p in parts[:10]])


# ---------------------------------------------------------------- NEON

def _count_files(data: Any) -> int:
    n = 0
    for rel in (data or {}).get("releases") or []:
        for pk in rel.get("packages") or []:
            n += len(pk.get("files") or [])
    return n


async def _neon_probe(r: Report) -> None:
    try:
        bundle = env.neon_bundle(("meta",))
    except SystemExit as exc:
        r.add("NEON token", "fail", str(exc))
        return
    try:
        r.add("NEON token", "ok" if bundle.token is not None else "fail",
              f"present (source={getattr(bundle.token, 'source', '?')})" if bundle.token else "missing")
        client = bundle.client("meta")
        t0 = time.time()
        envl = await client.post_json("/data/query", PROBE, token=bundle.token, timeout_s=120)
        t1 = time.time()
        n = _count_files(envl.data)
        r.add("NEON /data/query", "ok" if n > 0 else "fail",
              f"DP1.00002.001 SRER 2024-06 basic: HTTP {envl.status}, {n} files, {envl.elapsed_ms:.0f} ms",
              files=n, http_status=envl.status)
        date = next((v for k, v in envl.headers.items() if k.lower() == "date"), None)
        if not date:
            r.add("clock skew", "warn", "no Date header in the NEON response")
        else:
            server = email.utils.parsedate_to_datetime(date).timestamp()
            skew = abs(server - (t0 + t1) / 2)
            r.add("clock skew", "ok" if skew < MAX_SKEW_S else "fail", f"{skew:.1f} s vs NEON Date header",
                  skew_s=round(skew, 1))
    except Exception as exc:  # noqa: BLE001
        r.add("NEON /data/query", "fail", f"{type(exc).__name__}: {str(exc)[:200]}")
    finally:
        await bundle.aclose()


# ---------------------------------------------------------------- iRODS / MESA

def check_irods(r: Report) -> Any | None:
    try:
        m = env.mesa()
    except SystemExit as exc:
        r.add("iRODS auth", "fail", str(exc))
        return None
    except Exception as exc:  # noqa: BLE001
        r.add("iRODS auth", "fail", f"{type(exc).__name__}: {exc}")
        return None
    s = m.session
    try:
        v = tuple(s.server_version)
        r.add("iRODS server", "ok" if v >= MIN_IRODS else "fail", f"{'.'.join(map(str, v))} as {m.auth.username}"
              f"@{m.auth.zone}", version=list(v))
        home_path = f"/{m.auth.zone}/home/{m.auth.username}"
        home = s.collections.get(home_path)
        r.add("iRODS read", "ok", f"read {home_path} (collection id {home.id})")
    except Exception as exc:  # noqa: BLE001
        r.add("iRODS server", "fail", f"{type(exc).__name__}: {exc}")
        return m
    try:
        from irods.models import Quota, User

        uid = next(iter(s.query(User.id).filter(User.name == m.auth.username)))[User.id]
        rows = list(s.query(Quota.limit, Quota.over, Quota.resc_id).filter(Quota.user_id == uid))
        if rows:
            q = [{"limit": x[Quota.limit], "over": x[Quota.over], "resc": x[Quota.resc_id]} for x in rows]
            r.add("iRODS quota", "info", json.dumps(q), quota=q)
        else:
            r.add("iRODS quota", "ok", "no quota rows; unlimited Data Store allocation (confirmed by user 2026-09-27)")
    except Exception as exc:  # noqa: BLE001
        r.add("iRODS quota", "ok", f"not queryable ({type(exc).__name__}); unlimited allocation (confirmed by user 2026-09-27)")
    root = _irods_root()
    if root is None:
        r.add("project root", "fail", f"iRODS project root unknown: {env.USER_HINT}")
        return m
    try:
        from irods.exception import CollectionDoesNotExist

        try:
            s.collections.get(root)
            root_exists = True
        except CollectionDoesNotExist:
            root_exists = False
        if root_exists:
            from .enroll import nested_mesa_enabled

            nested = nested_mesa_enabled(s, root)
            r.add("no nested mesa.enabled", "ok" if not nested else "fail",
                  f"{root} exists; " + ("no mesa.enabled below it" if not nested else f"found {nested[:5]}"))
        else:
            r.add("project root", "info", f"{root} does not exist yet (enroll creates it)")
    except Exception as exc:  # noqa: BLE001
        r.add("no nested mesa.enabled", "fail", f"{type(exc).__name__}: {exc}")
    return m


def check_mesa_catalog(r: Report, m: Any | None) -> None:
    holders = env.catalog_lock_holders()
    if holders:
        r.add("MESA catalog lock", "fail", "held by " + ", ".join(f"PID {h['pid']} ({h['cmd'][:60]})"
                                                                     for h in holders), holders=holders)
        return
    r.add("MESA catalog lock", "ok", "no other process holds catalog.duckdb")
    try:
        from mesa_mcp.ducklake import client as mc

        opened_here = env._DUCKLAKE_CLIENT is not None or bool(getattr(mc, "_default_client_initialised", False))
    except Exception:  # noqa: BLE001
        opened_here = env._DUCKLAKE_CLIENT is not None
    if opened_here:
        r.add("MESA catalog", "warn", "this process already opened the catalog; read-only inspection skipped")
        return
    import duckdb

    dsn = getattr(getattr(getattr(m, "config", None), "ducklake", None), "catalog_dsn", None) if m else None
    path = _catalog_path(dsn)
    if not path.exists():
        r.add("MESA catalog", "fail", f"{path} does not exist")
        return
    con = duckdb.connect(str(path), read_only=True)
    try:
        projects = con.execute("SELECT project_id, irods_path FROM mesa.projects ORDER BY created_at").fetchall()
        snaps = {pid: (n, pend, fail) for pid, n, pend, fail in con.execute(
            "SELECT project_id, count(*), count(*) FILTER (WHERE parquet_file='pending'),"
            " count(*) FILTER (WHERE parquet_file='failed') FROM mesa.snapshots GROUP BY project_id").fetchall()}
        wal = dict(con.execute("SELECT s.project_id, count(*) FROM mesa.pending_pushes p LEFT JOIN mesa.snapshots s"
                               " USING (snapshot_id) GROUP BY s.project_id").fetchall())
    finally:
        con.close()
    rows = []
    for pid, ipath in projects:
        n, pend, fail = snaps.get(pid, (0, 0, 0))
        rows.append({"project_id": pid, "irods_path": ipath, "snapshots": n, "pending": pend, "failed": fail,
                     "pending_pushes": wal.get(pid, 0)})
    root = _irods_root()
    ours = [x for x in rows if root and x["irods_path"] == root]
    total_wal = sum(wal.values())
    bad_ours = [x for x in ours if x["failed"] or x["pending_pushes"] or x["pending"]]
    others_bad = [x for x in rows if x not in ours and (x["failed"] or x["pending_pushes"])]
    status = "fail" if bad_ours else ("warn" if others_bad or total_wal else "ok")
    detail = (f"{len(rows)} project(s); pending pushes {total_wal}; failed snapshots "
              f"{sum(x['failed'] for x in rows)}; ours: "
              + (f"{ours[0]['project_id']} ({ours[0]['snapshots']} snapshots)" if ours else "not enrolled yet"))
    r.add("MESA catalog", status, detail, projects=rows, path=str(path))


def check_ror(r: Report) -> None:
    label, how, obsolete = None, None, False
    try:
        from mesa_mcp.ols.client import get_ols_client

        term = get_ols_client().get_term("ror", env.NEON_ROR)
        if term:
            label, how = term.get("label"), "OLS (mesa-mcp client)"
            obsolete = bool(term.get("is_obsolete") or term.get("obsolete"))
    except Exception as exc:  # noqa: BLE001
        log.info("mesa-mcp OLS lookup failed: %s", exc)
    if label is None:
        import httpx

        enc = urllib.parse.quote(urllib.parse.quote(env.NEON_ROR, safe=""), safe="")
        ror_id = env.NEON_ROR.rsplit("/", 1)[-1]
        for url, name in ((f"https://www.ebi.ac.uk/ols4/api/ontologies/ror/individuals/{enc}", "OLS (raw)"),
                          (f"https://api.ror.org/v2/organizations/{ror_id}", "ROR API")):
            try:
                resp = httpx.get(url, timeout=30, follow_redirects=True)
                if resp.status_code == 200:
                    d = resp.json()
                    names = d.get("names") or []
                    label = d.get("label") or d.get("name") or next(
                        (n.get("value") for n in names if "ror_display" in (n.get("types") or [])), None)
                    obsolete = bool(d.get("is_obsolete")) or d.get("status") in ("inactive", "withdrawn")
                    how = name
                    break
            except Exception as exc:  # noqa: BLE001
                log.info("%s lookup failed: %s", name, exc)
    ok = bool(label) and "national ecological observatory network" in label.lower() and not obsolete
    r.add("NEON ROR", "ok" if ok else "fail", f"{env.NEON_ROR} -> {label!r} via {how}"
          + (" (obsolete)" if obsolete else ""), label=label)


# ---------------------------------------------------------------- driver

def _print(r: Report) -> None:
    w = max(len(c.name) for c in r.checks)
    for c in r.checks:
        print(f"{c.name:<{w}}  {c.status.upper():<4}  {c.detail}")


def main(site: str | None = None) -> int:
    site = site.upper() if site else None
    r = Report()
    t0 = time.time()
    check_imports(r)
    check_identity(r)
    check_duckdb_extensions(r)
    asyncio.run(_neon_probe(r))
    m = check_irods(r)
    check_mesa_catalog(r, m)
    check_ror(r)
    check_disk(r, site)
    check_parts(r)
    versions = env.repo_versions()
    r.add("repo versions", "info", ", ".join(f"{k} {v}" for k, v in versions.items()), versions=versions)
    _print(r)
    ts = env.run_id()
    out = env.WORK_DIR / f"preflight-{ts}.json"
    env.write_json(out, {"ts": env.utcnow().isoformat(), "site": site, "seconds": round(time.time() - t0, 1),
                         "failed": [c.name for c in r.failed], "checks": [asdict(c) for c in r.checks]})
    print(f"\n{len(r.failed)} hard failure(s); report {out}")
    return 1 if r.failed else 0
