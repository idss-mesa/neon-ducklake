"""`site upload <SITE>` — push the site subtree to iRODS through ``mirror.push_tree`` (idempotent).

Order:
  1. raw files from the journal (``site_files.dl_status='ok'``, not superseded) with their local MD5;
     evicted rows (``local_evicted = 1``: verified in iRODS, deleted locally) are skipped quietly;
  2. ``sites/<SITE>/{cards,curation,reports}/**`` and ``ref/ontology_maps/**``;
  3. the site lake after ``checkpoint_detach`` (no ``.wal``): ``ducklake/data/**``;
  4. the ``.ducklake`` catalog LAST, through :func:`push_lake` (catalog guard, versioned copy, ``CURRENT``).

Streaming (docs/STREAMING.md): :func:`upload_chunk` pushes one chunk's raw files only; :func:`push_lake`
pushes the site lake after every chunk (``site/stream.py`` ``upload_chunk``): new ``data/**`` files, then a
checkpointed copy of the ``.ducklake`` catalog LAST; :func:`finalize` pushes the lake through :func:`push_lake`,
then cards/curation/reports (the shared ``ref/ontology_maps`` export is not pushed from a stream run: each VM
publishes its own, ``bootstrap.publish_ontology``).

Catalog guard (a site can move between VMs): the catalog the Data Store serves (:func:`current_catalog`) is
replaced only when it is absent, equals the local catalog, equals the catalog this host last pushed or restored
(``lake_pushes``, site/chunks.py), or when every snapshot it holds is also in the local catalog (the local lake is
its continuation). Anything else means another VM pushed a lake this host has not seen: nothing is written
(``stream.restore_lake``). A catalog is written only while this VM holds the site's claim (:func:`claim_problem`,
``coord.holds_site``), and it is never overwritten in place: see "the site lake" below.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
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
        if any(r[1] == "local_evicted" for r in con.execute("PRAGMA table_info(site_files)")):
            where += " AND coalesce(f.local_evicted, 0) = 0"  # already in iRODS, deleted locally
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


def side_items(site: str, *, shared_refs: bool = True) -> list[Item]:
    """cards/curation/reports of the site, plus the shared ``ref/ontology_maps/**`` export when ``shared_refs``
    (the whole-site upload; a stream finalize leaves it to ``bootstrap.publish_ontology``'s per-VM export, since
    a shared copy pushed from several VMs is last-writer-wins)."""
    paths = site_lake.site_paths(site)
    files = [f for d in SIDE_DIRS for f in _tree(paths.site_dir / d)]
    if shared_refs:
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


def ensure_external_collections(site: str, products: list[str] | None = None,
                                ext: list[str] | None = None) -> list[str]:
    """Create raw/<DP> collections for external-only products so their product AVUs have a target.

    ``ext``: the external products when the caller already read them (under the site-lake lock)."""
    if ext is None:
        with env.file_lock(f"site-lake-{site.upper()}", blocking=True):
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
    """Whole-site upload: raw files, side files and the lake's ``data/**`` in one batch, then the catalog through
    :func:`push_lake` (guarded and versioned, like a stream push). 0 ok, 2 failures (or a refused catalog)."""
    from ..mirror import push_tree  # imported lazily

    site = site.upper()
    with env.file_lock(f"site-lake-{site}", blocking=True):
        ext = external_products(site, products)
    ensure_external_collections(site, products, ext)
    with env.file_lock(f"site-lake-{site}", blocking=True):
        first, catalog = build_items(site, products, months)
    n_raw = sum(1 for it in first if "/raw/" in it[1])
    log.info("upload %s: %d raw + %d other/lake file(s), then the catalog", site, n_raw, len(first) - n_raw)
    res1 = push_tree(first, threads=threads, record_site=site)
    fails = _failures(res1)
    lake: dict[str, Any] = {"skipped": "no site lake"} if not catalog else {}
    if catalog and not fails:
        lake = push_lake(site, threads=threads)  # data files already pushed above are skipped (uploads journal)
        fails += int(lake.get("failures") or 0)
    elif catalog:
        lake = {"skipped": f"{fails} upload failure(s)"}
        log.warning("upload %s: %d failure(s); catalog file NOT uploaded", site, fails)
    env.write_json(env.ROOT / "logs" / f"upload_summary_{site}.json",
                   {"site": site, "items": len(first) + len(catalog), "raw": n_raw, "result": res1,
                    "lake": lake, "failures": fails, "at": env.utcnow().isoformat()})
    return 0 if not fails else 2


# ---------------------------------------------------------------- streaming

def upload_chunk(site: str, product: str, months: list[str], *, threads: int = 8) -> dict[str, Any]:
    """Push one stream chunk's raw files (``product`` × ``months``) and nothing else. ``failures`` > 0 on error."""
    from ..mirror import push_tree

    site = site.upper()
    ms = sorted(set(months))
    items = raw_items(site, [product], ms) if ms else []
    res: Any = push_tree(items, threads=threads, record_site=site) if items else {"files": 0}
    out = {"site": site, "product": product, "months": ms, "items": len(items), "result": res,
           "failures": _failures(res)}
    log.info("upload chunk %s %s %s: %d raw file(s), %d failure(s)", site, product, ",".join(ms), len(items),
             out["failures"])
    return out


def finalize_items(site: str) -> tuple[list[Item], list[Item]]:
    """(side files + lake ``data/**``, catalog) of a finalize upload. Call with the site-lake lock held."""
    site = site.upper()
    data, catalog = lake_items(site)
    return side_items(site) + data, [catalog] if catalog else []


def finalize_summary(site: str, *, threads: int = 8, session: Any | None = None) -> dict[str, Any]:
    """Upload the site lake through :func:`push_lake` (data, then the catalog LAST: guarded, versioned, only under
    the site's claim), then cards, reports and curation. No raw files. Nothing else is uploaded when the lake was
    not pushed (a host whose lake is not the Data Store's must not replace its cards either). Returns the summary
    (``rc``: 0 ok, 1 refused or claim lost, 2 upload failures, 4 retry later: the Data Store catalog is being
    written)."""
    from ..mirror import push_tree

    site = site.upper()
    ensure_external_collections(site)
    lake = push_lake(site, threads=threads, session=session)
    fails = int(lake.get("failures") or 0)
    first: list[Item] = []
    res1: Any = {"skipped": "the site lake was not pushed"}
    if not fails:
        first = side_items(site, shared_refs=False)
        log.info("upload finalize %s: site lake pushed; %d side file(s)", site, len(first))
        res1 = push_tree(first, threads=threads, record_site=site) if first else {"files": 0}
        fails += _failures(res1)
    rc = 4 if lake.get("retry") else 1 if lake.get("refused") or lake.get("claimLost") else 0 if not fails else 2
    out = {"site": site, "items": len(first), "result": res1, "lake": lake, "failures": fails, "rc": rc,
           "at": env.utcnow().isoformat()}
    env.write_json(env.ROOT / "logs" / f"upload_finalize_{site}.json", out)
    return out


def finalize(site: str, *, threads: int = 8, session: Any | None = None) -> int:
    """:func:`finalize_summary`'s return code."""
    return int(finalize_summary(site, threads=threads, session=session)["rc"])


# ---------------------------------------------------------------- the site lake (per chunk and at finalize)
#
# Data Store layout of one site lake (docs/STREAMING.md "The site lake on the Data Store"):
#
#   sites/<SITE>/ducklake/data/**                               immutable DuckLake data files
#   sites/<SITE>/ducklake/catalogs/neon_<site>.<vkey>.ducklake  versioned catalogs, written once, never overwritten
#   sites/<SITE>/ducklake/CURRENT                               pointer (JSON) to the current versioned catalog
#   sites/<SITE>/ducklake/neon_<site>.ducklake                  canonical copy, refreshed after the pointer
#
# ``<vkey>`` = ``<last snapshot id, 10 digits>-<sha1(snapshot key)[:10]>-<md5[:12]>``: the name pins both the
# lake history and the bytes, so a same-name put can only (re)write identical content. A publish is: versioned copy
# (verified by ``push_tree``) -> pointer (written to ``CURRENT.new-<id>``, then moved over ``CURRENT``) -> canonical
# copy, each write right after ``coord.holds_site``. Readers (:func:`current_catalog`, :func:`fetch_current_catalog`)
# take the pointer's target, else the newest verifying versioned catalog, else the canonical copy (sites pushed
# before versioning). A crash or a claim-loss kill at any point leaves the previous version readable. After a
# publish, versions beyond the newest ``NEON_DUCKLAKE_LAKE_VERSIONS_KEEP`` (default 10; 0 keeps all) are deleted,
# never the pointer's target or its predecessor, and so are ``CURRENT.new-*`` leftovers older than an hour.

VERSIONS_DIR = "catalogs"
POINTER_NAME = "CURRENT"
POINTER_FORMAT = 1
REQUIRE_CLAIM_ENV = "NEON_DUCKLAKE_REQUIRE_CLAIM"
VERSIONS_KEEP_ENV = "NEON_DUCKLAKE_LAKE_VERSIONS_KEEP"
VERSIONS_KEEP = 10
POINTER_LEFTOVER_S = 3600.0
_VERSION_RE = re.compile(r"^neon_[a-z0-9_-]+\.(?P<sid>\d{10})-(?P<key>[0-9a-f]{10})-(?P<md5>[0-9a-f]{12})\.ducklake$")


class CatalogRefused(RuntimeError):
    """The Data Store holds a catalog of the site lake that this host did not push and does not contain."""


class NoCatalog(RuntimeError):
    """No Data Store catalog of the site lake could be fetched and verified (being written, or every copy bad)."""


def remote_catalog_path(site: str) -> str:
    """The canonical Data Store copy of the site's catalog (``ducklake/neon_<site>.ducklake``)."""
    return irods_path_for(site_lake.site_paths(site).catalog_file)


def remote_lake_dir(site: str) -> str:
    return irods_path_for(site_lake.site_paths(site).lake_dir)


def versions_coll(site: str) -> str:
    return f"{remote_lake_dir(site)}/{VERSIONS_DIR}"


def pointer_path(site: str) -> str:
    return f"{remote_lake_dir(site)}/{POINTER_NAME}"


def version_key(snap: site_lake.Snapshot | None, md5: str) -> str:
    """File-name-safe key of one catalog: last snapshot id, a hash of its snapshot key, and the catalog md5."""
    sid = int(snap[0]) if snap else 0
    key = site_lake.snapshot_key(snap) if snap else ""
    return f"{sid:010d}-{hashlib.sha1(key.encode()).hexdigest()[:10]}-{md5.lower()[:12]}"


def version_name(site: str, snap: site_lake.Snapshot | None, md5: str) -> str:
    return f"neon_{site.lower()}.{version_key(snap, md5)}.ducklake"


def parse_version(name: str) -> tuple[int, str] | None:
    """(last snapshot id, md5 prefix) of a versioned catalog name; None for any other object."""
    m = _VERSION_RE.match(name)
    return (int(m.group("sid")), m.group("md5")) if m else None


@dataclass
class RemoteLake:
    """The catalog of the site lake the Data Store serves (see the layout above)."""

    exists: bool = False          # the Data Store holds some catalog of the lake (or a pointer to one)
    path: str | None = None       # the object readers use; None when nothing is readable
    md5: str | None = None        # its md5 (a good replica's registered checksum, or the fetched bytes)
    source: str = "none"          # pointer | version (newest verifying versioned catalog) | canonical | none
    mid_write: bool = False       # the pointer is missing, unreadable or names a copy without a good replica
    pointer: dict[str, Any] | None = None
    snapshots: list[site_lake.Snapshot] | None = None  # set by fetch_current_catalog

    def describe(self) -> dict[str, Any]:
        return {"exists": self.exists, "path": self.path, "md5": self.md5, "source": self.source,
                "midWrite": self.mid_write}


def _good_md5(reps: list[Any]) -> str | None:
    from ..mirror import norm_checksum

    for r in reps:
        if r.status == "1" and r.checksum:
            return norm_checksum(r.checksum)
    return None


def _survey(session: Any, site: str) -> tuple[dict[str, Any] | None, bool, list[RemoteLake], RemoteLake | None]:
    """(pointer document, a pointer object exists, versioned catalogs newest first, canonical copy or None)."""
    from .. import coord
    from ..mirror import remote_replicas

    raw = coord.read_bytes(session, pointer_path(site))
    ptr: dict[str, Any] | None = None
    if raw is not None:
        try:
            doc = json.loads(raw.decode("utf-8"))
            ptr = doc if isinstance(doc, dict) and doc.get("catalog") and doc.get("md5") else None
        except (UnicodeDecodeError, ValueError):
            ptr = None
    coll = versions_coll(site)
    mtimes = {o.name: o.modified for o in coord.list_objects(session, coll).values()}
    found: list[tuple[int, Any, str, RemoteLake]] = []
    for name, reps in remote_replicas(session, coll).items():
        parsed = parse_version(name)
        if parsed is None:
            continue
        sid, prefix = parsed
        md5 = _good_md5(reps)
        ok = md5 is not None and md5.startswith(prefix)
        found.append((sid, mtimes.get(name) or datetime.min.replace(tzinfo=UTC), name,
                      RemoteLake(True, f"{coll}/{name}", md5 if ok else None, "version", mid_write=not ok)))
    found.sort(key=lambda t: (t[0], t[1], t[2]), reverse=True)
    ccoll, cname = remote_catalog_path(site).rsplit("/", 1)
    creps = remote_replicas(session, ccoll, cname).get(cname, [])
    canon = None
    if creps:
        md5 = _good_md5(creps)
        canon = RemoteLake(True, f"{ccoll}/{cname}", md5, "canonical", mid_write=md5 is None)
    return ptr, raw is not None, [t[3] for t in found], canon


def _good_rows(session: Any, queries: list[Any]) -> dict[str, Any]:
    from irods.models import Collection, DataObject

    from .. import coord

    out: dict[str, coord.RemoteObject] = {}
    for q in queries:
        for r in q:
            size, checksum = int(r[DataObject.size] or 0), str(r[DataObject.checksum] or "").strip()
            if str(r[DataObject.replica_status]) != "1" or size <= 0 or not checksum:
                continue  # stale / intermediate replica, or an interrupted put (size 0, no checksum)
            path = f"{r[Collection.name]}/{r[DataObject.name]}"
            if path in out:
                continue  # good replicas of one object hold the same bytes
            mt = r[DataObject.modify_time]
            if isinstance(mt, datetime) and mt.tzinfo is None:
                mt = mt.replace(tzinfo=UTC)
            out[path] = coord.RemoteObject(path, size, checksum, mt if isinstance(mt, datetime) else None)
    return out


def _object_query(session: Any) -> Any:
    from irods.models import Collection, DataObject

    return session.query(Collection.name, DataObject.name, DataObject.size, DataObject.checksum,
                         DataObject.modify_time, DataObject.replica_status, DataObject.replica_number)


def good_objects(session: Any, coll: str, *, recursive: bool = False) -> dict[str, Any]:
    """path -> ``coord.RemoteObject`` of a GOOD replica (replica status 1, size > 0, a registered checksum) of every
    data object in ``coll`` (and below). Objects without one are left out: an interrupted put leaves its object
    registered with a stale or intermediate replica, size 0 or partial and no checksum, and such an object is never
    read (``coord.list_objects`` falls back to those replicas)."""
    from irods.column import Like
    from irods.models import Collection

    queries = [_object_query(session).filter(Collection.name == coll)]
    if recursive:
        queries.append(_object_query(session).filter(Like(Collection.name, coll.rstrip("/") + "/%")))
    return _good_rows(session, queries)


def good_object(session: Any, path: str) -> Any | None:
    """``coord.RemoteObject`` of a good replica of ``path`` (see :func:`good_objects`); None when it has none."""
    from irods.models import Collection, DataObject

    coll, name = path.rsplit("/", 1)
    return _good_rows(session, [_object_query(session).filter(Collection.name == coll)
                                .filter(DataObject.name == name)]).get(path)


def fetch_verified(session: Any, obj: Any, local: Path) -> Any:
    """``coord.fetch`` of ``obj``, a good replica from :func:`good_objects` / :func:`good_object`, through
    ``<local>.part``: the download must have the replica's size and checksum before it replaces ``local``. An object
    of size 0 or without a checksum is refused (``coord.TransferMismatch``): with them ``coord.fetch`` would skip its
    size or checksum check and accept a truncated copy."""
    from .. import coord

    if obj is None:
        raise FileNotFoundError("no good replica to fetch")
    if int(obj.size or 0) <= 0 or not str(obj.checksum or "").strip():
        raise coord.TransferMismatch(f"{obj.path}: no good replica with a size and a checksum (size {obj.size}, "
                                     f"checksum {obj.checksum!r}): not fetched")
    return coord.fetch(session, obj.path, local, obj=obj)


def _pointed(ptr: dict[str, Any] | None, versions: list[RemoteLake]) -> RemoteLake | None:
    if not ptr:
        return None
    return next((v for v in versions if v.path and v.path.rsplit("/", 1)[1] == str(ptr["catalog"])), None)


def current_catalog(session: Any, site: str) -> RemoteLake:
    """The catalog the Data Store serves, from metadata only (no download): the pointer's target when it has a
    good replica with the pointer's md5; else the newest versioned catalog whose good replica matches its name
    (``mid_write``); else the canonical copy. ``exists`` False: the site has no lake in the Data Store."""
    ptr, ptr_present, versions, canon = _survey(session, site)
    target = _pointed(ptr, versions)
    if ptr and target is not None and target.md5 and target.md5 == str(ptr["md5"]).lower():
        return replace(target, source="pointer", pointer=ptr)
    mid = ptr_present or bool(versions)  # a pointer is being replaced (or was lost): readers fall back
    for v in versions:
        if v.md5:
            return replace(v, mid_write=mid, pointer=ptr)
    if canon is not None:
        return replace(canon, mid_write=canon.mid_write or mid, pointer=ptr)
    if mid:
        return RemoteLake(True, None, None, "none", mid_write=True, pointer=ptr)
    return RemoteLake()


def fetch_current_catalog(session: Any, site: str, dest: Path) -> RemoteLake:
    """Download the catalog the Data Store serves to ``dest`` and verify it: the pointer's target, else the newest
    verifying versioned catalog, else the canonical copy. A copy verifies when it has a good replica with a size and
    a checksum and the download matches both (:func:`fetch_verified`), its md5 is the one the pointer (or its name)
    pins, and it opens as a DuckLake catalog. Returns the copy used, with its snapshots. Raises
    ``FileNotFoundError`` (nothing in the Data Store) or :class:`NoCatalog`."""
    from ..mirror import md5_of

    ptr, ptr_present, versions, canon = _survey(session, site)
    order: list[tuple[RemoteLake, str | None]] = []
    target = _pointed(ptr, versions)
    if ptr and target is not None:
        order.append((replace(target, source="pointer", pointer=ptr), str(ptr["md5"]).lower()))
    order += [(v, (parse_version(v.path.rsplit("/", 1)[1]) or (0, None))[1]) for v in versions
              if v.md5 and v is not target and v.path]
    if canon is not None:
        order.append((canon, None))
    if not order:
        if ptr_present or versions:
            raise NoCatalog(f"{remote_lake_dir(site)}: no readable catalog (a pointer or versioned copy is being "
                            "written)")
        raise FileNotFoundError(f"no catalog of the site lake {site} in the Data Store")
    errors: list[str] = []
    dest.parent.mkdir(parents=True, exist_ok=True)
    for cand, expect in order:
        assert cand.path is not None
        try:
            obj = good_object(session, cand.path)
            if obj is None:
                raise FileNotFoundError(f"{cand.path}: no good replica with a size and a checksum")
            fetch_verified(session, obj, dest)
            md5 = md5_of(dest).lower()
            if expect and not md5.startswith(expect):
                raise RuntimeError(f"md5 {md5} is not the pinned {expect}")
            snaps = site_lake.catalog_snapshots(dest, site)
        except Exception as exc:  # noqa: BLE001 - try the next copy
            errors.append(f"{cand.path.rsplit('/', 1)[1]}: {type(exc).__name__}: {exc}"[:300])
            dest.unlink(missing_ok=True)
            log.warning("site lake %s: Data Store catalog %s does not verify (%s); trying the next copy", site,
                        cand.path, errors[-1])
            continue
        mid = cand.source != "pointer" and (ptr_present or bool(versions))
        if errors:
            log.warning("site lake %s: using %s (%s); skipped: %s", site, cand.path, cand.source, "; ".join(errors))
        return replace(cand, md5=md5, snapshots=snaps, mid_write=mid)
    raise NoCatalog("no Data Store catalog of the site lake verifies: " + "; ".join(errors[:3]))


def remote_catalog(session: Any, site: str) -> tuple[bool, str | None]:
    """(exists, md5) of the catalog the Data Store serves (:func:`current_catalog`; md5 None while it is being
    written and nothing else is readable)."""
    cur = current_catalog(session, site)
    return cur.exists, cur.md5


def fetch_remote_catalog(session: Any, site: str, dest: Path) -> Path:
    """Download the catalog the Data Store serves to ``dest`` (:func:`fetch_current_catalog`)."""
    fetch_current_catalog(session, site, dest)
    return dest


@dataclass
class Guard:
    ok: bool
    why: str
    remote: RemoteLake
    retry: bool = False  # not refused for good: the Data Store catalog is being written; try again later


def catalog_guard(session: Any, site: str, local_catalog: Path, local_md5: str, *,
                  con: Any | None = None, local_snaps: list[site_lake.Snapshot] | None = None) -> Guard:
    """May the local catalog replace the one the Data Store serves? (module docstring's catalog guard)

    The ``lake_pushes`` shortcut only counts rows whose snapshot the local catalog holds, so a journal copied
    from another VM never vouches for this host's lake."""
    from .chunks import last_lake_push

    cur = current_catalog(session, site)
    local_md5 = local_md5.lower()
    if not cur.exists:
        return Guard(True, "no catalog in the Data Store yet", cur)
    if cur.md5 and cur.md5 == local_md5:
        return Guard(True, "the Data Store already holds this catalog", cur)
    snaps = local_snaps if local_snaps is not None else site_lake.catalog_snapshots(local_catalog, site)
    last = last_lake_push(site, con=con, local_keys={site_lake.snapshot_key(x) for x in snaps})
    if cur.md5 and last and last["catalog_md5"] == cur.md5:
        return Guard(True, f"the Data Store holds the catalog this lake last {last['kind']}ed", cur)
    tmp = site_lake.site_paths(site).tmp_dir / f"remote_{os.getpid()}_{local_catalog.name}"
    try:
        got = fetch_current_catalog(session, site, tmp)
    except (NoCatalog, FileNotFoundError) as exc:
        return Guard(False, f"the Data Store catalog of the site lake is being written or unreadable ({exc}); "
                            "retry later", cur, retry=True)
    finally:
        tmp.unlink(missing_ok=True)
        tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
    rel = site_lake.lineage(snaps, got.snapshots or [])
    if rel in ("same", "local_ahead"):
        return Guard(True, f"the local lake continues the Data Store catalog ({rel})", got)
    last = last_lake_push(site, con=con)
    return Guard(False, (f"the Data Store catalog (md5 {got.md5}, {got.source}) is not the one this host last pushed "
                         f"({last['catalog_md5'] if last else 'none'}) and the local lake does not contain it ({rel}): "
                         "another VM pushed this site's lake; restore it (stream.restore_lake) before pushing"), got)


def _require_claim(environ: dict[str, str] | None = None) -> bool:
    """``$NEON_DUCKLAKE_REQUIRE_CLAIM`` (``run`` exports 1 when it or its caller, the worker, holds the site's
    claim, 0 for ``run --no-claims``)."""
    return (environ if environ is not None else os.environ).get(REQUIRE_CLAIM_ENV, "").strip().lower() in (
        "1", "true", "yes", "on")


def claim_problem(site: str, session: Any | None = None) -> str | None:
    """None when this process may write the site's lake catalog right now (``coord.holds_site``, called right
    before each catalog or pointer write); otherwise the reason (``claim lost: ...``) and the caller writes nothing.

    With a claim in force (``NEON_DUCKLAKE_REQUIRE_CLAIM=1`` or ``NEON_DUCKLAKE_SITE_CLAIMS=on``, exported by
    ``run`` and the worker) only this VM's live claim allows the write; without one (``run --no-claims``, a stage
    run by hand) only another VM's live claim forbids it."""
    import inspect

    from .. import coord

    required = _require_claim()
    if not required and hasattr(coord, "site_claims_required"):
        required = bool(coord.site_claims_required())
    if "required" in inspect.signature(coord.holds_site).parameters:
        ok = coord.holds_site(site, session=session, required=required)
        holder = None if ok else coord.read_claim(coord.site_claim_path(site), session=session)
    else:  # a holds_site that always requires this VM's live claim
        ok = coord.holds_site(site, session=session)
        holder = None if ok else coord.read_claim(coord.site_claim_path(site), session=session)
        if not ok and not required and (holder is None or (holder.valid and holder.is_stale())):
            ok = True
    if ok:
        return None
    if holder is None:
        return f"claim lost: site {site} has no claim and a claim is in force ({REQUIRE_CLAIM_ENV}=1)"
    who = holder.vm if holder.valid else "an unreadable claim"
    state_ = "stale" if holder.valid and holder.is_stale() else "live"
    return f"claim lost: site {site} is claimed by {who} ({state_}), not by this VM"


def write_pointer(session: Any, site: str, doc: dict[str, Any]) -> None:
    """Replace ``ducklake/CURRENT``: write ``CURRENT.new-<id>``, read it back, then move it over the old pointer
    (readers that find no pointer in between fall back to the newest verifying versioned catalog)."""
    from .. import coord

    ptr = pointer_path(site)
    data = (json.dumps(doc, indent=1, sort_keys=True, default=str) + "\n").encode()
    new = f"{ptr}.new-{uuid.uuid4().hex[:12]}"
    coord.overwrite(session, new, data, create=True)
    if coord.read_bytes(session, new) != data:
        raise RuntimeError(f"{new}: the new pointer does not read back")
    coord.unlink(session, ptr)
    session.data_objects.move(new, ptr)


def versions_keep(environ: dict[str, str] | None = None) -> int:
    raw = (environ if environ is not None else os.environ).get(VERSIONS_KEEP_ENV, "").strip()
    try:
        return max(0, int(raw)) if raw else VERSIONS_KEEP
    except ValueError:
        log.warning("%s=%r is not a number; keeping %d versions", VERSIONS_KEEP_ENV, raw, VERSIONS_KEEP)
        return VERSIONS_KEEP


def prune_versions(session: Any, site: str, *, protect: set[str] = frozenset(), keep: int | None = None
                   ) -> list[str]:
    """Delete the versioned catalogs beyond the newest ``keep`` (by last snapshot id, then modify time), never a
    path in ``protect``, and ``CURRENT.new-*`` leftovers of interrupted pointer updates older than an hour.
    Best effort: returns the paths deleted."""
    from .. import coord

    keep = versions_keep() if keep is None else keep
    gone: list[str] = []
    floor = datetime.min.replace(tzinfo=UTC)
    if keep:
        found = sorted(((pv[0], o.modified or floor, o.path) for o in coord.list_objects(
            session, versions_coll(site)).values() if (pv := parse_version(o.name))), reverse=True)
        for _sid, _mt, path in found[keep:]:
            if path not in protect and coord.unlink(session, path):
                gone.append(path)
    now = datetime.now(UTC)
    for o in coord.list_objects(session, remote_lake_dir(site)).values():
        if o.name.startswith(f"{POINTER_NAME}.new-") and o.modified and \
                (now - o.modified).total_seconds() > POINTER_LEFTOVER_S and coord.unlink(session, o.path):
            gone.append(o.path)
    if gone:
        log.info("site lake %s: %d old catalog version(s) / pointer leftover(s) deleted", site, len(gone))
    return gone


def _publish_catalog(session: Any, site: str, copy: Path, local_md5: str, snaps: list[site_lake.Snapshot],
                     snapshot: int | None, cur: RemoteLake) -> dict[str, Any]:
    """Versioned copy (never overwritten) -> ``CURRENT`` -> canonical copy; ``coord.holds_site`` right before each
    write. Call with the ``lake-push-<SITE>`` lock held. Returns ``failures``, ``claimLost``, ``version``, ..."""
    from .. import coord
    from ..mirror import push_tree, remote_replicas

    out: dict[str, Any] = {"failures": 0}
    last = snaps[-1] if snaps else None
    name = version_name(site, last, local_md5)
    vpath = f"{versions_coll(site)}/{name}"
    out["version"] = vpath
    if not (cur.source == "pointer" and cur.md5 == local_md5 and cur.path == vpath):
        have = _good_md5(remote_replicas(session, versions_coll(site), name).get(name, []))
        if have and have != local_md5:
            out.update(failures=1, error=f"{vpath} holds md5 {have}: a versioned catalog is never overwritten")
            return out
        if have != local_md5:
            why = claim_problem(site, session)
            if why:
                out.update(claimLost=why, failures=1)
                return out
            res = push_tree([(copy, vpath, local_md5)], threads=1, record_site=site)
            out["versionResult"] = res
            out["failures"] = _failures(res)
            if out["failures"]:
                return out
        why = claim_problem(site, session)
        if why:
            out.update(claimLost=why, failures=1)
            return out
        write_pointer(session, site, {
            "format": POINTER_FORMAT, "site": site, "catalog": name, "path": vpath, "md5": local_md5,
            "size": copy.stat().st_size, "snapshotId": snapshot,
            "snapshotKey": site_lake.snapshot_key(last) if last else None, "vm": coord.vm_id(),
            "host": coord.host(), "pushedAt": env.utcnow().isoformat(),
            "previous": (cur.pointer or {}).get("catalog")})
        out["pointer"] = True
        try:
            prev = (cur.pointer or {}).get("catalog")
            out["pruned"] = len(prune_versions(session, site, protect={vpath, f"{versions_coll(site)}/{prev}"}))
        except Exception as exc:  # noqa: BLE001 - housekeeping only
            log.warning("site lake %s: old catalog versions not pruned (%s)", site, exc)
    ccoll, cname = remote_catalog_path(site).rsplit("/", 1)
    if _good_md5(remote_replicas(session, ccoll, cname).get(cname, [])) != local_md5:
        why = claim_problem(site, session)
        if why:
            out.update(claimLost=why, failures=1)
            return out
        res = push_tree([(copy, remote_catalog_path(site), local_md5)], threads=1, record_site=site)
        out["canonicalResult"] = res
        if _failures(res):  # readers use the pointer: a stale canonical copy is refreshed by the next push
            out["canonicalFailures"] = _failures(res)
            log.warning("push lake %s: the canonical catalog copy was not refreshed (%d failure(s))", site,
                        out["canonicalFailures"])
    return out


def _pushed_already(files: list[Path]) -> set[Path]:
    """Files the ``uploads`` journal records as uploaded/present with their current size (DuckLake data files
    are immutable, so they need no new upload)."""
    if not files:
        return set()
    con = state.connect(state.DB_PATH)
    try:
        have = {r[0]: int(r[1] or -1) for r in con.execute(
            "SELECT local_path, size FROM uploads WHERE status IN ('uploaded', 'present')")} \
            if con.execute("SELECT 1 FROM sqlite_master WHERE name='uploads'").fetchone() else {}
    finally:
        con.close()
    out = set()
    for f in files:
        size = have.get(str(f))
        if size is not None:
            try:
                if f.stat().st_size == size:
                    out.add(f)
            except OSError:
                continue
    return out


def _refused(out: dict[str, Any], site: str, g: Guard) -> dict[str, Any]:
    log.error("push lake %s: catalog NOT pushed: %s", site, g.why)
    out.update(refused=g.why, failures=1)
    if g.retry:
        out["retry"] = True
    return out


def push_lake(site: str, *, threads: int = 8, session: Any | None = None) -> dict[str, Any]:
    """Push the site lake to the Data Store: new ``data/**`` files, then the catalog LAST (guarded, versioned).

    Under the site-lake lock the lake is checkpointed (no ``.wal``), its catalog copied aside and its data files
    listed; the data uploads then run without the lock (data files never change; the pushed catalog is the copy).
    The catalog is published (versioned copy, ``CURRENT``, canonical copy) and journaled in ``lake_pushes`` under
    the host lock ``lake-push-<SITE>``, which ``stream.verify_chunk`` takes around its lake check, so a verify never
    sees a half-published push of this host. Returns a summary with ``failures`` (> 0 on an upload error),
    ``refused`` (the catalog guard's reason; ``retry`` when the Data Store catalog is being written) and
    ``claimLost`` (``coord.holds_site`` said no: nothing was written).
    """
    from ..mirror import md5_of, push_tree
    from .chunks import last_lake_push, record_lake_push

    site = site.upper()
    paths = site_lake.site_paths(site)
    out: dict[str, Any] = {"site": site, "failures": 0}
    copy = paths.tmp_dir / "lake_push" / f"{os.getpid()}-{paths.catalog_file.name}"
    with env.file_lock(f"site-lake-{site}", blocking=True):
        if not paths.catalog_file.exists():
            out["skipped"] = "no site lake"
            return out
        con = site_lake.open_site_lake(site)
        try:
            snapshot = site_lake.last_snapshot_id(con)
            site_lake.checkpoint_detach(con, site)
        finally:
            con.close()
        copy.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(paths.catalog_file, copy)
        data = _tree(paths.data_dir)
    try:
        local_md5 = md5_of(copy).lower()
        snaps = site_lake.catalog_snapshots(copy, site)
        key = site_lake.snapshot_key(snaps[-1]) if snaps else None
        sess = session if session is not None else env.mesa().session
        g = catalog_guard(sess, site, copy, local_md5, local_snaps=snaps)
        out.update(snapshotId=snapshot, catalogMd5=local_md5, remoteMd5=g.remote.md5, guard=g.why)
        if not g.ok:
            return _refused(out, site, g)
        done = _pushed_already(data)
        todo = [f for f in data if f not in done]
        res1: Any = push_tree([(f, irods_path_for(f), None) for f in todo], threads=threads, record_site=site) \
            if todo else {"files": 0}
        out.update(dataFiles=len(data), dataPushed=len(todo), dataResult=res1)
        out["failures"] = _failures(res1)
        if out["failures"]:
            log.warning("push lake %s: %d data upload failure(s); catalog NOT pushed", site, out["failures"])
            return out
        with env.file_lock(f"lake-push-{site}", blocking=True):
            g = catalog_guard(sess, site, copy, local_md5, local_snaps=snaps)  # again: another push may have landed
            out.update(remoteMd5=g.remote.md5, guard=g.why)
            if not g.ok:
                return _refused(out, site, g)
            pub = _publish_catalog(sess, site, copy, local_md5, snaps, snapshot, g.remote)
            out["catalog"] = {k: v for k, v in pub.items() if k not in ("failures",)}
            if pub.get("claimLost"):
                log.error("push lake %s: catalog NOT pushed: %s", site, pub["claimLost"])
                out.update(claimLost=pub["claimLost"], failures=1)
                return out
            if pub["failures"]:
                out["failures"] = pub["failures"]
                if pub.get("error"):
                    out["error"] = pub["error"]
                return out
            last = last_lake_push(site)
            if not last or last["catalog_md5"] != local_md5 or last["kind"] != "push" or \
                    last.get("snapshot_key") != key:
                c = state.connect(state.DB_PATH)
                try:
                    record_lake_push(c, site, local_md5, snapshot, kind="push", snapshot_key=key)
                finally:
                    c.close()
        log.info("push lake %s: %d/%d data file(s) pushed, catalog md5 %s (snapshot %s, %s)", site, len(todo),
                 len(data), local_md5, snapshot, out["catalog"].get("version"))
        return out
    finally:
        copy.unlink(missing_ok=True)  # the pushed copy (a later push makes a new one)
