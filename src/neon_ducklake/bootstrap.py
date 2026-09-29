"""``neon-ducklake bootstrap [--what catalog,ontology,curation] [--force]``: prepare a fresh worker VM.

A DE VM starts with a clone of the code and nothing else. Everything it needs comes from the Data Store
(docs/STREAMING.md, "Bootstrap"):

``catalog`` (alias ``lake``)
    The catalog lake ``<IRODS_ROOT>/ducklake/`` -> ``ducklake/`` when the local catalog is missing or older
    (by modify time) than the Data Store copy and differs from it. Data files (``data/**``, immutable,
    uniquely named) are fetched when missing or of another size; the ``.ducklake`` catalog file goes last,
    through ``.part`` and an atomic rename, and keeps the remote modify time. Refused while a process has
    the local catalog open or a ``.wal`` exists.
``ontology``
    Seeds ``work/ontology_cache.duckdb`` from every VM's export ``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/*.parquet``
    (and the older shared ``ref/ontology_maps/*.parquet``): rows missing here are inserted; a row whose key
    exists here replaces it only when its ``status`` is better (``ontology_store.merge_parquet``); else local
    rows win. Then re-exports ``ref/ontology_maps/``.
``curation``
    Syncs ``curation/generic/*.validated.json`` both ways by content version (``validatedAt``, then ``runId``;
    never file modify times): a result only on the Data Store, or newer there, is fetched and imported into
    ``curation_avus``; a result only here, or newer here, is published only when it was validated on this
    host (``validatedBy``), so a git checkout never overwrites newer results of other VMs.

:func:`publish_ontology` (called by the orchestrator at a site's finalize) exports this VM's ontology store
to ``work/ontology_export/vm/<vm>/`` and uploads it to ``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/``: each VM
writes only its own export, so no VM overwrites another's mappings.

Every download is verified against the iRODS size and checksum.
"""

from __future__ import annotations

import json
import logging
import posixpath
import shutil
from pathlib import Path
from typing import Any

from . import coord, env

log = logging.getLogger("neon_ducklake.bootstrap")

WHAT = ("catalog", "ontology", "curation")
ALIASES = {"lake": "catalog"}
GENERIC_SUFFIX = ".validated.json"


def _parse_what(what: str | set[str] | list[str] | None) -> list[str]:
    if what is None:
        return list(WHAT)
    items = what.split(",") if isinstance(what, str) else list(what)
    chosen = {ALIASES.get(w.strip().lower(), w.strip().lower()) for w in items if w and w.strip()}
    unknown = chosen - set(WHAT)
    if unknown:
        raise SystemExit(f"unknown --what {sorted(unknown)} (choose from {', '.join(WHAT)})")
    return [w for w in WHAT if w in chosen]


# ---------------------------------------------------------------- catalog lake

def sync_lake(*, session: Any | None = None, force: bool = False) -> dict[str, Any]:
    s = coord.get_session(session)
    remote_dir = f"{env.IRODS_ROOT}/ducklake"
    cat_remote = f"{remote_dir}/{env.CATALOG_LAKE}"
    local_cat = env.LAKE_DIR / env.CATALOG_LAKE
    obj = coord.stat(s, cat_remote)
    if obj is None:
        return {"status": "local-only" if local_cat.exists() else "missing",
                "detail": f"{cat_remote} is not on the Data Store"}
    if local_cat.exists() and not force:
        if coord.checksum_matches(obj.checksum, local_cat):
            return {"status": "current"}
        if obj.modified is not None and local_cat.stat().st_mtime >= obj.modified.timestamp():
            return {"status": "local-newer", "detail": "the local catalog is newer than the Data Store copy"}
    wal = sorted(env.LAKE_DIR.glob("*.wal")) if env.LAKE_DIR.exists() else []
    if wal:
        raise SystemExit(f"{[w.name for w in wal]} present in {env.LAKE_DIR}: a writer is active; not replacing")
    if local_cat.exists() and env.catalog_lock_holders(local_cat):
        raise SystemExit(f"{local_cat} is open by another process; not replacing it")
    fetched = present = nbytes = 0
    data = coord.list_objects(s, f"{remote_dir}/data", recursive=True)
    for path, o in sorted(data.items()):
        rel = posixpath.relpath(path, remote_dir)
        local = env.LAKE_DIR / rel
        if local.exists() and local.stat().st_size == o.size:
            present += 1
            continue
        coord.fetch(s, path, local, obj=o)
        fetched += 1
        nbytes += o.size
    coord.fetch(s, cat_remote, local_cat, obj=obj)
    log.info("catalog lake fetched: %d data file(s) (%.1f MB), %d present, catalog %s", fetched, nbytes / 1e6,
             present, local_cat)
    return {"status": "fetched", "data_fetched": fetched, "data_present": present, "bytes": nbytes + obj.size}


# ---------------------------------------------------------------- ontology cache

def maps_remote() -> str:
    return f"{env.IRODS_ROOT}/ref/ontology_maps"


def vm_maps_remote(vm: str | None = None) -> str:
    """``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>``: one VM's own ontology export."""
    return f"{maps_remote()}/vm/{vm or coord.vm_id()}"


def ontology_exports(session: Any) -> dict[str, dict[str, coord.RemoteObject]]:
    """source -> {table: remote Parquet}; source ``shared`` (``ref/ontology_maps/*.parquet``, written before
    per-VM exports) first, then ``vm/<vm>`` in name order."""
    from .site import ontology_store

    root = maps_remote()
    out: dict[str, dict[str, coord.RemoteObject]] = {}
    for path, o in coord.list_objects(session, root, recursive=True).items():
        if not path.endswith(".parquet"):
            continue
        table = posixpath.basename(path)[: -len(".parquet")]
        if table not in ontology_store.EXPORT_TABLES:
            continue
        rel = posixpath.relpath(posixpath.dirname(path), root)
        if rel == ".":
            source = "shared"
        elif rel.startswith("vm/") and rel.count("/") == 1:
            source = rel
        else:
            continue
        out.setdefault(source, {})[table] = o
    return dict(sorted(out.items(), key=lambda kv: (kv[0] != "shared", kv[0])))


def seed_ontology(*, session: Any | None = None) -> dict[str, Any]:
    from .site import ontology_store

    s = coord.get_session(session)
    sources = ontology_exports(s)
    if not sources:
        return {"status": "missing", "detail": f"no Parquet under {maps_remote()}"}
    work = env.WORK_DIR / "bootstrap" / "ontology_maps"
    shutil.rmtree(work, ignore_errors=True)
    inserted: dict[str, int] = {}
    upgraded: dict[str, int] = {}
    try:
        with ontology_store.store() as con:
            for source, tables in sources.items():
                for table in ontology_store.EXPORT_TABLES:
                    o = tables.get(table)
                    if o is None:
                        continue
                    f = work / source.replace("/", "_") / f"{table}.parquet"
                    coord.fetch(s, o.path, f, obj=o)
                    r = ontology_store.merge_parquet(con, table, f)
                    inserted[table] = inserted.get(table, 0) + r["inserted"]
                    upgraded[table] = upgraded.get(table, 0) + r["upgraded"]
                    f.unlink(missing_ok=True)
            ontology_store.export_parquet(con)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    log.info("ontology cache seeded from %d export(s) on the Data Store: inserted %s, upgraded %s", len(sources),
             inserted, upgraded)
    return {"status": "seeded", "sources": list(sources), "inserted": inserted, "upgraded": upgraded}


def publish_ontology(*, session: Any | None = None) -> dict[str, Any]:
    """Export this VM's ontology store (``export_parquet(vm_scoped=True)``) and upload it, checksum-verified
    and idempotent, to ``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/``. ``status`` is ``published`` or ``error``."""
    from . import mirror
    from .site import ontology_store

    s = coord.get_session(session)
    local = ontology_store.export_parquet(vm_scoped=True)
    assert isinstance(local, Path)
    remote = vm_maps_remote()
    items = [(p, f"{remote}/{p.name}", None) for p in sorted(local.glob("*.parquet"))]
    stats = mirror.push_tree(items, threads=4, session=s)
    out = {"status": "error" if stats.get("errors") else "published", "remote": remote, "local": str(local),
           "files": len(items), "uploaded": stats.get("uploaded", 0), "present": stats.get("present", 0)}
    if stats.get("errors"):
        out["error"] = str(stats.get("error_samples"))[:500]
        log.error("ontology export not fully published to %s: %s", remote, out["error"])
    else:
        log.info("ontology export published to %s (%d uploaded, %d unchanged)", remote, out["uploaded"],
                 out["present"])
    return out


# ---------------------------------------------------------------- curation/generic

def sync_curation(*, session: Any | None = None, publish: bool = True) -> dict[str, Any]:
    from .site import validate
    from .site.curate import generic_path

    s = coord.get_session(session)
    remote_dir = f"{env.IRODS_ROOT}/curation/generic"
    remote = {posixpath.basename(p)[: -len(GENERIC_SUFFIX)]: o for p, o in coord.list_objects(s, remote_dir).items()
              if p.endswith(GENERIC_SUFFIX)}
    local_dir = generic_path("X").parent
    local = {p.name[: -len(GENERIC_SUFFIX)] for p in local_dir.glob(f"*{GENERIC_SUFFIX}")} if local_dir.exists() \
        else set()
    out: dict[str, Any] = {"status": "synced", "fetched": [], "published": [], "same": 0, "kept": [],
                           "notValidatedHere": []}
    for code in sorted(remote):
        st = coord.fetch_generic(code, generic_path(code), session=s)
        if st == "fetched":
            out["fetched"].append(code)
        elif st == "same":
            out["same"] += 1
        elif st == "kept":
            out["kept"].append(code)
    if publish:
        for code in sorted((local - set(remote)) | set(out["kept"])):
            try:
                doc = json.loads(generic_path(code).read_text("utf-8"))
            except (OSError, ValueError):
                doc = None
            if not coord.validated_here(doc):  # e.g. a git checkout's copy: never pushed over other VMs' results
                out["notValidatedHere"].append(code)
                continue
            stats = coord.publish_generic(code, generic_path(code), session=s)
            if stats.get("errors"):
                raise RuntimeError(f"publishing {code} failed: {stats.get('error_samples')}")
            if stats.get("skipped"):
                continue
            out["published"].append(code)
    out["imported"] = validate.import_generic(out["fetched"]) if out["fetched"] else {}
    log.info("curation/generic: %d fetched, %d published, %d identical", len(out["fetched"]), len(out["published"]),
             out["same"])
    return out


# ---------------------------------------------------------------- entry point

STEPS = {"catalog": sync_lake, "ontology": seed_ontology, "curation": sync_curation}


def run(what: str | set[str] | list[str] | None = None, *, session: Any | None = None, force: bool = False
        ) -> dict[str, dict[str, Any]]:
    """Run the chosen steps; an error in one is recorded (``status: error``) and the others still run."""
    s = coord.get_session(session)
    results: dict[str, dict[str, Any]] = {}
    for w in _parse_what(what):
        try:
            results[w] = sync_lake(session=s, force=force) if w == "catalog" else STEPS[w](session=s)
        except (SystemExit, Exception) as exc:  # noqa: BLE001 - reported per step
            log.error("bootstrap %s failed: %s", w, exc)
            results[w] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"[:500]}
    return results


def main(what: str | set[str] | list[str] | None = None, *, session: Any | None = None, force: bool = False,
         as_json: bool = False) -> int:
    """``neon-ducklake bootstrap``. Exit 1 when a step failed or no catalog lake is available at all."""
    results = run(what, session=session, force=force)
    if as_json:
        print(json.dumps(results, indent=1, default=str))
    else:
        for w, r in results.items():
            detail = {k: v for k, v in r.items() if k != "status" and v not in ([], {}, 0, None)}
            print(f"== bootstrap {w}: {r.get('status')}" + (f" {json.dumps(detail, default=str)[:400]}" if detail
                                                             else ""))
    bad = any(r.get("status") == "error" for r in results.values()) or \
        results.get("catalog", {}).get("status") == "missing"
    return 1 if bad else 0


def cli(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="neon-ducklake bootstrap", description=__doc__.split("\n\n")[0])
    ap.add_argument("--what", default=",".join(WHAT))
    ap.add_argument("--force", action="store_true", help="re-fetch the catalog lake even when it looks current")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    env.setup_logging("warning")
    return main(a.what, force=a.force, as_json=a.json)


if __name__ == "__main__":
    import sys

    sys.exit(cli())
