"""`site upload <SITE>` — push the site subtree to iRODS through ``mirror.push_tree`` (idempotent).

Order (the site-lake lock is held throughout, so nothing rewrites the lake mid-upload):
  1. raw files from the journal (``site_files.dl_status='ok'``, not superseded) with their local MD5;
  2. ``sites/<SITE>/{cards,curation,reports}/**`` and ``ref/ontology_maps/**``;
  3. the site lake after ``checkpoint_detach`` (no ``.wal``): ``ducklake/data/**``;
  4. the ``.ducklake`` catalog file LAST, in its own ``push_tree`` call.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .. import env, state
from . import site_lake

log = logging.getLogger("neon_ducklake.site.upload")

Item = tuple[Path, str, str | None]
SIDE_DIRS = ("cards", "curation", "reports")
SKIP_SUFFIXES = (".part", ".wal", ".tmp", ".lock")


def irods_path_for(local: Path) -> str:
    """Mirror path of a file under the local root."""
    return f"{env.IRODS_ROOT}/{local.resolve().relative_to(env.ROOT.resolve()).as_posix()}"


def _tree(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*") if p.is_file() and not p.name.endswith(SKIP_SUFFIXES)
                  and not p.name.startswith("."))


def raw_items(site: str, products: list[str] | None = None, months: list[str] | None = None) -> list[Item]:
    from .fetch import SUPERSEDED_SCHEMA

    paths = site_lake.site_paths(site)
    con = state.connect(state.DB_PATH)
    try:
        con.executescript(SUPERSEDED_SCHEMA)
        where, args = "f.site=? AND f.dl_status='ok' AND s.name IS NULL", [site]
        if products:
            where += f" AND f.product IN ({','.join('?' * len(products))})"
            args += list(products)
        if months:
            where += f" AND f.month IN ({','.join('?' * len(months))})"
            args += list(months)
        rows = con.execute(
            "SELECT f.product, f.month, f.name, f.local_path, f.dl_md5, f.dl_bytes FROM site_files f "
            "LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND s.month=f.month AND "
            f"s.name=f.name WHERE {where} ORDER BY f.product, f.month, f.name", args).fetchall()
    finally:
        con.close()
    items: list[Item] = []
    for r in rows:
        local = env.ROOT / r["local_path"]
        try:
            if local.stat().st_size != r["dl_bytes"]:
                log.warning("size changed since download, skipped: %s", r["local_path"])
                continue
        except FileNotFoundError:
            log.warning("missing local file, skipped: %s", r["local_path"])
            continue
        items.append((local, paths.irods_raw(r["product"], r["month"], r["name"]), r["dl_md5"]))
    return items


def side_items(site: str) -> list[Item]:
    paths = site_lake.site_paths(site)
    files = [f for d in SIDE_DIRS for f in _tree(paths.site_dir / d)]
    files += _tree(env.ROOT / "ref" / "ontology_maps")
    return [(f, irods_path_for(f), None) for f in files]


def lake_items(site: str) -> tuple[list[Item], Item | None]:
    """(data/** items, catalog item) of a checkpointed, detached site lake."""
    paths = site_lake.site_paths(site)
    if not paths.catalog_file.exists():
        return [], None
    con = site_lake.open_site_lake(site)
    try:
        site_lake.checkpoint_detach(con, site)
    finally:
        con.close()
    data = [(f, irods_path_for(f), None) for f in _tree(paths.data_dir)]
    return data, (paths.catalog_file, irods_path_for(paths.catalog_file), None)


def build_items(site: str, products: list[str] | None = None,
                months: list[str] | None = None) -> tuple[list[Item], list[Item]]:
    """(first batch, catalog batch). Call with the site-lake lock held."""
    site = site.upper()
    first = raw_items(site, products, months) + side_items(site)
    data, catalog = lake_items(site)
    return first + data, [catalog] if catalog else []


def _failures(result: Any) -> int:
    if isinstance(result, dict):
        for key in ("failed", "errors", "error"):
            v = result.get(key)
            if isinstance(v, int):
                return v
            if isinstance(v, (list, tuple)):
                return len(v)
    return 0


def external_products(site: str, products: list[str] | None = None) -> list[str]:
    """Externally hosted products (no /data files) recorded in the site lake's external_links."""
    try:
        con = site_lake.open_site_lake(site, read_only=True)
    except Exception:  # noqa: BLE001 - no lake yet
        return []
    try:
        rows = [r[0] for r in con.execute("SELECT DISTINCT productCode FROM lake.external_links").fetchall()]
    except Exception:  # noqa: BLE001 - table missing
        rows = []
    finally:
        con.close()
    return sorted(p for p in rows if not products or p in products)


def ensure_external_collections(site: str, products: list[str] | None = None) -> list[str]:
    """Create raw/<DP> collections for external-only products so their product AVUs have a target."""
    ext = external_products(site, products)
    if not ext:
        return []
    m = env.mesa()
    made = []
    for code in ext:
        path = f"{env.IRODS_ROOT}/sites/{site}/raw/{code}"
        m.session.collections.create(path, recurse=True)
        made.append(path)
    log.info("upload %s: ensured %d external-product collection(s)", site, len(made))
    return made


def main(site: str, products: list[str] | None = None, months: list[str] | None = None, threads: int = 8) -> int:
    from ..mirror import push_tree  # written by the mirror owner; imported lazily

    site = site.upper()
    ensure_external_collections(site, products)
    with env.file_lock(f"site-lake-{site}", blocking=True):
        first, catalog = build_items(site, products, months)
        n_raw = sum(1 for it in first if "/raw/" in it[1])
        log.info("upload %s: %d raw + %d other/lake file(s), then the catalog", site, n_raw, len(first) - n_raw)
        res1 = push_tree(first, threads=threads, record_site=site)
        fails = _failures(res1)
        res2: Any = None
        if catalog and not fails:
            res2 = push_tree(catalog, threads=1, record_site=site)
            fails += _failures(res2)
        elif catalog:
            log.warning("upload %s: %d failure(s); catalog file NOT uploaded", site, fails)
    env.write_json(site_lake.site_paths(site).reports_dir / "upload_summary.json",
                   {"site": site, "items": len(first) + len(catalog), "raw": n_raw, "result": res1,
                    "catalogResult": res2, "failures": fails, "at": env.utcnow().isoformat()})
    return 0 if not fails else 2
