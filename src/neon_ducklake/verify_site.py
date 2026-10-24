"""`neon-ducklake verify <SITE>` — Phase-2 checks of one site subtree (plan § Verification, Phase 2).

Read-only everywhere: the site and catalog lakes are attached READ_ONLY, ``work/state.sqlite`` is opened
``mode=ro``, iRODS is only queried (GenQuery, replica-aware: rows collapse by ``DataObject.id``) and the
ontology check goes through ``site/ontology_store.ols_terms`` (OLS, cached in the ontology store).

Checks (status pass / warn / fail / skip; any ``fail`` gives a non-zero exit):

- listing          site-lake ``site_months`` vs catalog ``v_site_inventory`` (diffs reported, warn)
- local files      journal ``dl_md5`` == NEON md5, except flagged soft kinds (regenerated supporting files)
                   and storage-verified NEON-stale files (``transfer_verified`` = local md5 == the storage
                   md5, NEON's listed md5 is stale); files on disk; evicted rows (``local_evicted = 1``)
                   are counted as evicted, not missing
- irods objects    per month collection: one good replica (status '1', size, checksum == dl_md5) per
                   object; objects not uploaded yet are counted, not failed; journal agreement
- avu core         0 uploaded objects missing a core attribute
- avu collections  every collection under ``sites/<SITE>`` carries >= 1 ``neon.*``/``mesa.*`` AVU
- avu file kinds   per-``fileKind`` counts carrying AVUs vs the listing
- avu desired      ``desired - current`` is empty (one collection at a time)
- stack rows       sampled CSV record counts (``wc -l`` semantics, quoted newlines handled) == file_stats;
                   Σ file_stats.rowCount == table_catalog rows before dedup
- stack typed      >= 95 % of the columns with a non-string NEON type are typed in the lake
- stack sensors    >= 99 % of IS data files resolve to a sensor_positions row (same month, HOR.VER)
- taxa             resolution rate against ref_ncbitaxon_map (info)
- ontology         every CURIE of the scope's desired AVUs re-resolves in OLS, not obsolete, and the attribute
                   equals ``<prefix>.<_label_to_snake(label)>``
- datacite         site/product ``datacite.*`` AVUs round-trip (``avus_to_datacite`` + ``DataCiteMetadata``)

Filters ``products``/``months`` restrict every check to those product/month units (dry runs).
``metadata_only`` (the stream finalize: the raw files were verified per chunk and deleted) skips every check
of local files: the ``local files`` check keeps the journal md5 comparisons but not presence/size on disk, and
``stack rows`` skips the sampled CSV record counts (Σ file_stats vs table_catalog stays).
The site lake is read under the ``site-lake-<SITE>`` lock (a concurrent writer would fail the attach).
Output: a table on stdout and ``sites/<SITE>/reports/verify.json``.
"""

from __future__ import annotations

import logging
import posixpath
import random
import re
import sqlite3
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from . import env, state
from .avu import bulk
from .filenames import REGENERATED_KINDS
from .site import avus as siteavus
from .site import site_lake

log = logging.getLogger("neon_ducklake.verify_site")

SOFT_KINDS = REGENERATED_KINDS  # NEON regenerates these in place; their listed md5 is advisory
STRING_TYPES = frozenset({"string", "str", "uri", "varchar", "text", ""})
NON_ONTOLOGY_PREFIXES = frozenset({"neon", "mesa", "dc", "datacite", "provenance", "ducklake"})
CURIE_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_]*):([A-Za-z0-9_]+)$")
OUR_NAMESPACES = ("neon.", "mesa.")
SAMPLE_FILES = 20
TYPED_MIN = 0.95
SENSOR_MIN = 0.99
MAX_SAMPLES = 20
READ_TRIES = 4
RETRY_DELAYS = (5.0, 20.0, 60.0)  # CyVerse refuses handshakes for up to minutes under the per-user connection limit
RC_INCOMPLETE = 4  # main(): only refused iRODS reads failed (site/stream.py RC_RETRY: verify again later)
DEFAULT_THREADS = 4


@dataclass
class Check:
    name: str
    status: str  # pass | warn | fail | skip | info
    summary: str
    details: dict[str, Any] = field(default_factory=dict)


def _sample(items: Iterable[Any], n: int = MAX_SAMPLES) -> list[Any]:
    out = []
    for i, x in enumerate(items):
        if i >= n:
            break
        out.append(x)
    return out


@dataclass
class ReadError:
    """An iRODS read that failed after retries (the item stays unverified and the check fails)."""

    item: Any
    error: str


class SessionPool:
    """One ``session.clone()`` per worker thread; every read is retried with a fresh clone on failure."""

    def __init__(self, session: Any, *, sleep: Callable[[float], None] = time.sleep) -> None:
        self.root = session
        self._local = threading.local()
        self._clones: list[Any] = []
        self._lock = threading.Lock()
        self._sleep = sleep

    def get(self, fresh: bool = False) -> Any:
        s = getattr(self._local, "s", None)
        if s is not None and fresh and s is not self.root and hasattr(s, "cleanup"):
            try:  # drop the broken connection before opening a new one
                s.cleanup()
            except Exception as exc:  # noqa: BLE001
                log.debug("session cleanup failed: %s", exc)
        if s is None or fresh:
            s = self.root.clone() if hasattr(self.root, "clone") else self.root
            self._local.s = s
            with self._lock:
                self._clones.append(s)
        return s

    def call(self, fn: Callable[[Any, Any], Any], item: Any, session: Any | None = None) -> Any:
        err: BaseException | None = None
        for attempt in range(READ_TRIES):
            try:
                return fn(session if session is not None and attempt == 0 else self.get(fresh=attempt > 0), item)
            except Exception as exc:  # noqa: BLE001 - NetworkException, SYS_MAX_CONNECT_COUNT_EXCEEDED, …
                err = exc
                log.warning("iRODS read of %s failed (%s: %s); attempt %d/%d", item, type(exc).__name__,
                            str(exc)[:120], attempt + 1, READ_TRIES)
                if attempt < READ_TRIES - 1:
                    self._sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
        return ReadError(item, f"{type(err).__name__}: {str(err)[:200]}")

    def map(self, fn: Callable[[Any, Any], Any], items: list[Any], threads: int) -> list[Any]:
        if threads <= 1 or len(items) <= 1:
            return [self.call(fn, it, self.root) for it in items]
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="verify") as ex:
            return list(ex.map(lambda it: self.call(fn, it), items))

    def close(self) -> None:
        for s in self._clones:
            if s is not self.root and hasattr(s, "cleanup"):
                try:
                    s.cleanup()
                except Exception as exc:  # noqa: BLE001
                    log.debug("session cleanup failed: %s", exc)
        self._clones.clear()


# ---------------------------------------------------------------- inputs

@dataclass
class SiteData:
    files: list[dict[str, Any]] = field(default_factory=list)
    site_months: list[dict[str, Any]] = field(default_factory=list)
    inventory: list[dict[str, Any]] | None = None
    file_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    sensor_keys: set[tuple[str, str, str]] = field(default_factory=set)
    table_catalog: list[dict[str, Any]] = field(default_factory=list)
    variables: dict[tuple[str, str, str], str] = field(default_factory=dict)  # (product, table, field) -> type
    lake_columns: dict[str, dict[str, str]] = field(default_factory=dict)
    cast_errors: list[dict[str, Any]] = field(default_factory=list)
    taxa: set[tuple[str, str, str]] = field(default_factory=set)  # (productCode, taxonID, scientificName)
    journal: list[dict[str, Any]] = field(default_factory=list)


def load_site_data(site: str) -> SiteData:
    with env.file_lock(f"site-lake-{site.upper()}", blocking=True):
        return _load_site_data(site)


def _load_site_data(site: str) -> SiteData:
    import duckdb

    d = SiteData()
    con = site_lake.connect(site)
    try:
        try:
            site_lake.attach(con, site, read_only=True)
        except (duckdb.Error, FileNotFoundError) as exc:
            raise SystemExit(f"site lake of {site} not readable: {str(exc).splitlines()[0][:200]}") from exc
        d.files = siteavus._lake_rows(con, "files", site_lake.FILES_COLUMNS, "ORDER BY productCode, month, name")
        d.site_months = siteavus._lake_rows(con, "site_months", site_lake.SITE_MONTHS_COLUMNS)
        for r in siteavus._lake_rows(con, "file_stats", site_lake.STACK_SCHEMAS["file_stats"]):
            d.file_stats[r["sourceFile"]] = r
        for r in siteavus._lake_rows(con, "sensor_positions", ["productCode", "month", "HOR.VER"]):
            if r.get("HOR.VER") is not None:
                d.sensor_keys.add((r["productCode"], r["month"], str(r["HOR.VER"])))
        d.table_catalog = siteavus._lake_rows(con, "table_catalog", site_lake.STACK_SCHEMAS["table_catalog"])
        for r in siteavus._lake_rows(con, "variables", ["productCode", "tableName", "fieldName", "dataType"]):
            d.variables[(r["productCode"], r["tableName"], r["fieldName"])] = (r.get("dataType") or "").lower()
        for r in d.table_catalog:
            if r.get("kind") in ("data", "h5") and site_lake.table_exists(con, r["tableName"]):
                d.lake_columns[r["tableName"]] = site_lake.table_columns(con, r["tableName"])
        d.cast_errors = siteavus._lake_rows(con, "cast_errors", site_lake.STACK_SCHEMAS["cast_errors"])
        for r in siteavus._lake_rows(con, "file_taxa", ["productCode", "taxonID", "scientificName"]):
            if r.get("taxonID"):
                d.taxa.add((r["productCode"], r["taxonID"], r.get("scientificName") or ""))
        if site_lake.attach_catalog(con):
            try:
                d.inventory = siteavus._rows(con, f"SELECT * FROM {site_lake.CATALOG_ALIAS}.v_site_inventory "
                                                  "WHERE siteCode = ?", [site])
            except duckdb.Error as exc:
                log.warning("v_site_inventory unreadable: %s", str(exc).splitlines()[0][:160])
    finally:
        con.close()
    if state.DB_PATH.exists():
        sc = sqlite3.connect(f"file:{state.DB_PATH}?mode=ro", uri=True, timeout=60)
        sc.row_factory = sqlite3.Row
        try:
            d.journal = [dict(r) for r in sc.execute(
                f"SELECT {journal_columns(sc)} FROM site_files WHERE site=?", (site,))]
        finally:
            sc.close()
    return d


JOURNAL_COLUMNS = ("product", "month", "name", "kind", "size", "md5", "dl_status", "dl_md5", "dl_bytes", "md5_verified",
                   "local_path", "irods_path", "up_status", "irods_checksum")
# columns other stages add later (evict: local_evicted; fetch: download integrity); NULL when absent
OPTIONAL_JOURNAL_COLUMNS = ("local_evicted", "storage_md5", "neon_md5", "transfer_verified")


def journal_columns(sc: sqlite3.Connection) -> str:
    have = {r[1] for r in sc.execute("PRAGMA table_info(site_files)")}
    return ", ".join([*JOURNAL_COLUMNS, *(c if c in have else f"NULL AS {c}" for c in OPTIONAL_JOURNAL_COLUMNS)])


def storage_verified(r: dict[str, Any]) -> bool:
    """The local bytes equal what the storage provider holds (the download's transfer check passed)."""
    if r.get("transfer_verified") is not None:
        return bool(r["transfer_verified"])
    s, local = r.get("storage_md5"), r.get("dl_md5")
    return bool(s and local and str(s).lower() == str(local).lower())


# ---------------------------------------------------------------- the verifier

class SiteVerifier:
    def __init__(self, site: str, products: list[str] | None = None, months: list[str] | None = None, *,
                 session: Any | None = None, con: sqlite3.Connection | None = None, data: SiteData | None = None,
                 threads: int = DEFAULT_THREADS, sample: int = SAMPLE_FILES, ontology: bool = True,
                 ols_terms: Callable[[list[str]], dict[str, Any]] | None = None,
                 metadata_only: bool = False) -> None:
        self.site = site.upper()
        self.metadata_only = metadata_only
        self.P = set(products) if products else None
        self.M = set(months) if months else None
        self.S = f"{env.IRODS_ROOT}/sites/{self.site}"
        self.session = session
        self.con = con
        self.data = data
        self.threads = max(1, min(threads, bulk.MAX_THREADS))
        self.sample = sample
        self.ontology = ontology
        self._ols_terms = ols_terms
        self.checks: list[Check] = []
        self.remote: dict[tuple[str, str], dict[str, list[Any]]] = {}  # (product, month) -> name -> replicas
        self.unit_reads: dict[str, dict[str, Any]] = {}
        self.current_colls: dict[str, set[tuple[str, str, str]]] = {}

    # ------------------------------------------------------------ filters
    def p_ok(self, p: str) -> bool:
        return self.P is None or p in self.P

    def pm_ok(self, p: str, m: str) -> bool:
        return self.p_ok(p) and (self.M is None or m in self.M)

    def unit_ok(self, unit_id: str) -> bool:
        raw = f"{self.S}/raw/"
        if not unit_id.startswith(raw):
            return True
        parts = unit_id[len(raw):].split("/")
        return self.p_ok(parts[0]) if len(parts) == 1 else self.pm_ok(parts[0], parts[1])

    def add(self, name: str, status: str, summary: str, **details: Any) -> Check:
        c = Check(name, status, summary, details)
        self.checks.append(c)
        log.info("%-18s %-5s %s", name, status, summary)
        return c

    @property
    def files(self) -> list[dict[str, Any]]:
        return [r for r in self.data.files if self.pm_ok(r["productCode"], r["month"])]

    # ------------------------------------------------------------ listing + local
    def check_listing(self) -> None:
        d = self.data
        if d.inventory is None:
            self.add("listing", "skip", "catalog lake / v_site_inventory not readable")
            return
        inv = {(r["productCode"], r["month"]): r for r in d.inventory if self.pm_ok(r["productCode"], r["month"])}
        lake = {(r["productCode"], r["month"]): r for r in d.site_months if self.pm_ok(r["productCode"], r["month"])}
        external = sorted(k for k in set(inv) - set(lake) if inv[k].get("isExternal"))
        covered = self._listed_windows()
        unlisted = sorted(k for k in set(inv) - set(lake)
                          if not inv[k].get("isExternal") and covered is not None and not _in_windows(k, covered))
        missing = sorted(k for k in set(inv) - set(lake) if not inv[k].get("isExternal") and k not in set(unlisted))
        extra = sorted(set(lake) - set(inv))
        diffs = []
        for k in sorted(set(inv) & set(lake)):
            a, b = inv[k], lake[k]
            if bool(a.get("isExternal")) or bool(b.get("isExternal")):
                continue
            if int(a.get("fileCount") or 0) != int(b.get("fileCount") or 0) or \
                    int(a.get("totalBytes") or 0) != int(b.get("totalBytes") or 0):
                diffs.append({"key": "/".join(k), "inventory": [a.get("fileCount"), a.get("totalBytes")],
                              "lake": [b.get("fileCount"), b.get("totalBytes")], "release": [a.get("release"),
                                                                                             b.get("release")]})
        status = "pass" if not (missing or extra or diffs) else "warn"
        self.add("listing", status, f"inventory {len(inv)} site-months ({len(external)} external, link-only; "
                                    f"{len(unlisted)} outside the listed windows, e.g. a --sample run), "
                                    f"lake {len(lake)}; missing {len(missing)}, "
                                    f"extra {len(extra)}, count/byte diffs {len(diffs)}",
                 missing=["/".join(k) for k in _sample(missing)], extra=["/".join(k) for k in _sample(extra)],
                 diffs=_sample(diffs), n_missing=len(missing), n_extra=len(extra), n_diffs=len(diffs),
                 inventory_files=sum(int(r.get("fileCount") or 0) for r in inv.values()),
                 lake_files=sum(int(r.get("fileCount") or 0) for r in lake.values()))

    def _listed_windows(self) -> dict[str, list[tuple[str, str]]] | None:
        """product -> [(start, end)] of the recorded listing windows (None when the journal has none)."""
        try:
            from . import state

            con = state.connect()
            try:
                rows = con.execute("SELECT product, start_month, end_month FROM listings WHERE site=? AND "
                                   "start_month IS NOT NULL", (self.site,)).fetchall()
            finally:
                con.close()
        except Exception:  # noqa: BLE001 - no journal: treat everything as in scope
            return None
        if not rows:
            return None
        out: dict[str, list[tuple[str, str]]] = {}
        for p, a, b in rows:
            out.setdefault(p, []).append((a, b))
        return out

    def check_local(self) -> None:
        rows = [r for r in self.data.journal if self.pm_ok(r["product"], r["month"])]
        if not rows:
            self.add("local files", "skip", "no journal rows for the selection")
            return
        ok = [r for r in rows if r["dl_status"] == "ok"]
        not_dl = [r for r in rows if r["dl_status"] not in ("ok", "superseded")]
        hard, soft, stale, no_md5, missing, size_bad, evicted = [], [], [], [], [], [], []
        for r in ok:
            if not r.get("md5"):
                no_md5.append(r["name"])
            elif (r.get("dl_md5") or "").lower() != r["md5"].lower():
                if r.get("kind") in SOFT_KINDS:
                    soft.append(r["name"])
                elif storage_verified(r):
                    stale.append(r["name"])  # bytes == storage, NEON's listed md5 is stale: flagged, kept
                else:
                    hard.append(r["name"])
            if int(r.get("local_evicted") or 0) == 1:
                evicted.append(r["name"])  # verified in iRODS and deleted locally (site evict)
                continue
            if self.metadata_only:
                continue  # stream finalize: the raw files were checked per chunk and are gone by design
            local = env.ROOT / (r.get("local_path") or "")
            try:
                if local.stat().st_size != int(r.get("dl_bytes") or -1):
                    size_bad.append(r["name"])
            except OSError:
                missing.append(r["name"])
        lake_md5 = {(r["productCode"], r["month"], r["name"]): r.get("dlMd5") for r in self.files}
        lake_bad = [r["name"] for r in ok if (r["product"], r["month"], r["name"]) in lake_md5
                    and lake_md5[(r["product"], r["month"], r["name"])] not in (None, r.get("dl_md5"))]
        status = "fail" if (hard or missing or size_bad or lake_bad) else ("warn" if not_dl else "pass")
        n_match = len(ok) - len(hard) - len(soft) - len(stale) - len(no_md5)
        self.add("local files", status,
                 ("metadata only (presence on disk not checked); " if self.metadata_only else "")
                 + f"{len(ok)}/{len(rows)} downloaded; md5 == NEON for {n_match}"
                 f"; flagged soft (regenerated supporting files) {len(soft)}; NEON md5 stale (storage-verified)"
                 f" {len(stale)}; hard mismatches {len(hard)}; no NEON md5 {len(no_md5)}; evicted {len(evicted)};"
                 f" missing on disk {len(missing)}; size != journal {len(size_bad)}; not downloaded {len(not_dl)}",
                 hard=_sample(hard), soft=_sample(soft), neon_stale=_sample(stale), missing=_sample(missing),
                 size_mismatch=_sample(size_bad), lake_dlmd5_mismatch=_sample(lake_bad),
                 not_downloaded=_sample(r["name"] for r in not_dl), n_evicted=len(evicted), n_neon_stale=len(stale),
                 n_soft=len(soft))

    # ------------------------------------------------------------ iRODS objects
    def month_colls(self) -> list[tuple[str, str]]:
        return sorted({(r["productCode"], r["month"]) for r in self.files})

    def check_irods_objects(self, pool: SessionPool) -> None:
        from .mirror import norm_checksum, remote_replicas

        keys = self.month_colls()

        def read(session: Any, key: tuple[str, str]) -> tuple[tuple[str, str], dict[str, list[Any]]]:
            return key, remote_replicas(session, f"{self.S}/raw/{key[0]}/{key[1]}")

        unreadable: list[str] = []
        for res in pool.map(read, keys, self.threads):
            if isinstance(res, ReadError):
                unreadable.append(f"{'/'.join(res.item)}: {res.error}")
                continue
            self.remote[res[0]] = res[1]
        unread_keys = {tuple(u.split(": ", 1)[0].split("/")) for u in unreadable}
        journal = {(r["product"], r["month"], r["name"]): r for r in self.data.journal}
        n = up = good = 0
        bad: list[str] = []
        not_up: list[str] = []
        lag: list[str] = []
        lost: list[str] = []
        extra: list[str] = []
        irods_bytes = local_bytes = 0
        for r in self.files:
            n += 1
            key = (r["productCode"], r["month"])
            j = journal.get((r["productCode"], r["month"], r["name"])) or {}
            if key in unread_keys:
                continue
            reps = self.remote.get(key, {}).get(r["name"])
            journal_up = (j.get("up_status") or "") in ("uploaded", "present")
            if not reps:
                not_up.append(r["name"])
                if journal_up:
                    lost.append(r["name"])
                continue
            up += 1
            md5 = (j.get("dl_md5") or r.get("dlMd5") or "").lower()
            size = int(j.get("dl_bytes") or r.get("size") or -1)
            ok = [x for x in reps if x.status == "1" and x.size == size and norm_checksum(x.checksum) == md5]
            if ok:
                good += 1
                irods_bytes += ok[0].size
                local_bytes += size
                if not journal_up:
                    lag.append(r["name"])
            else:
                why = "no replica with status 1" if not any(x.status == "1" for x in reps) else \
                    "size differs" if not any(x.size == size for x in reps) else "checksum differs or missing"
                bad.append(f"{r['name']}: {why}")
        listed = {(r["productCode"], r["month"], r["name"]) for r in self.files}
        for key, reps in self.remote.items():
            extra += [f"{key[0]}/{key[1]}/{nm}" for nm in reps if (key[0], key[1], nm) not in listed]
        status = "fail" if (bad or lost or unreadable) else ("warn" if (not_up or extra) else "pass")
        self.add("irods objects", status,
                 f"{up}/{n} uploaded in {len(keys)} month collections ({len(unreadable)} unreadable); good replica "
                 f"{good}; bad {len(bad)}; "
                 f"not uploaded {len(not_up)}; journal says uploaded but absent {len(lost)}; journal lag {len(lag)}; "
                 f"extra iRODS objects {len(extra)}; Σsize iRODS {irods_bytes} vs local {local_bytes}",
                 bad=_sample(bad), unreadable=_sample(unreadable), not_uploaded=_sample(not_up),
                 journal_lost=_sample(lost), journal_lag=_sample(lag),
                 extra=_sample(extra), n_objects=n, n_uploaded=up, n_good=good,
                 incomplete=bool(unreadable) and not (bad or lost))

    # ------------------------------------------------------------ AVUs
    def desired_units(self) -> dict[str, bulk.Unit]:
        """avu_desired rows of the selected units (read without bulk.load_desired's CREATE on a ro journal)."""
        units: dict[str, bulk.Unit] = {}
        for u, path, ttype, a, v, unit in self.con.execute(
                "SELECT unit_id, path, target_type, attribute, value, unit FROM avu_desired WHERE scope=?",
                (self.site,)):
            if self.unit_ok(u):
                units.setdefault(u, bulk.Unit(u)).desired[(path, ttype)].add((a, v, unit or ""))
        return units

    def read_unit(self, session: Any, unit: bulk.Unit, colls_known: set[str] | None) -> dict[str, Any]:
        """Current AVUs of every desired target of a unit (collection AVUs + direct-object AVUs per parent)."""
        cur: dict[tuple[str, str], set[tuple[str, str, str]]] = {}
        missing: set[tuple[str, str]] = set()
        colls = sorted({p for p, t in unit.desired if t == "collection"})
        parents = sorted({posixpath.dirname(p) for p, t in unit.desired if t == "data_object"})
        for c in colls:
            has, avus = bulk.query_collection_avus(session, c)
            exists = has or (c in colls_known if colls_known is not None and c.startswith(self.S)
                             else bulk.collection_exists(session, c))
            if exists:
                cur[(c, "collection")] = avus
            else:
                missing.add((c, "collection"))
        for parent in parents:
            objs = bulk.query_object_avus(session, parent)
            want = [p for p, t in unit.desired if t == "data_object" and posixpath.dirname(p) == parent]
            names = bulk.object_names(session, parent) if any(p not in objs for p in want) else set()
            for p in want:
                if p in objs:
                    cur[(p, "data_object")] = objs[p]
                elif posixpath.basename(p) in names:
                    cur[(p, "data_object")] = set()  # present, no AVUs at all
                else:
                    missing.add((p, "data_object"))
        return {"current": cur, "missing": missing}

    def check_avus(self, pool: SessionPool, units: dict[str, bulk.Unit]) -> None:
        from .mirror import existing_collections

        known = pool.call(lambda s, _i: existing_collections(s, self.S), "subtree collections", pool.root)
        colls_known: set[str] | None = None if isinstance(known, ReadError) else known
        levels: dict[str, str] = {}
        for r in self.con.execute("SELECT path, level FROM avu_desired WHERE scope=?", (self.site,)):
            levels[r[0]] = r[1] or "?"
        items = sorted(units)
        reads = pool.map(lambda s, u: (u, self.read_unit(s, units[u], colls_known)), items, self.threads)
        unreadable = [f"{r.item}: {r.error}" for r in reads if isinstance(r, ReadError)]
        unread_units = {r.item for r in reads if isinstance(r, ReadError)}
        self.unit_reads = dict(r for r in reads if not isinstance(r, ReadError))
        for res in self.unit_reads.values():
            for (p, t), avus in res["current"].items():
                if t == "collection":
                    self.current_colls[p] = avus
        # desired - current
        not_uploaded = Counter()
        gaps: dict[str, int] = Counter()
        gap_samples: list[str] = []
        checked = 0
        for u, res in self.unit_reads.items():
            unit = units[u]
            for key, want in unit.desired.items():
                lvl = levels.get(key[0], "?")
                if key in res["missing"]:
                    not_uploaded[lvl] += 1
                    continue
                checked += 1
                lack = {t for t in want if not bulk.is_protected(t[0])} - res["current"].get(key, set())
                if lack:
                    gaps[lvl] += 1
                    if len(gap_samples) < MAX_SAMPLES:
                        gap_samples.append(f"{key[0]}: {len(lack)} missing, e.g. {min(lack)}")
        status = "fail" if (gaps or unreadable) else ("warn" if not_uploaded else "pass")
        self.add("avu desired", status,
                 f"{checked} existing targets checked ({len(unreadable)} units unreadable); with desired AVUs "
                 f"missing {sum(gaps.values())} "
                 f"({dict(gaps)}); desired targets not in iRODS yet {sum(not_uploaded.values())} ({dict(not_uploaded)})",
                 gaps=gap_samples, not_uploaded=dict(not_uploaded), levels_with_gaps=dict(gaps),
                 unreadable=_sample(unreadable), incomplete=bool(unreadable) and not gaps)
        # core attributes + fileKind counts on uploaded raw objects
        raw_objs: dict[str, set[tuple[str, str, str]]] = {}
        for res in self.unit_reads.values():
            for (p, t), avus in res["current"].items():
                if t == "data_object" and p.startswith(f"{self.S}/raw/"):
                    raw_objs[p] = avus
        unread_objs: set[str] = set()  # in a collection whose AVUs could not be read: not known to lack anything
        for key, reps in self.remote.items():  # uploaded objects that have no desired rows at all
            for nm in reps:
                p = f"{self.S}/raw/{key[0]}/{key[1]}/{nm}"
                if posixpath.dirname(p) in unread_units:
                    unread_objs.add(nm)
                    continue
                raw_objs.setdefault(p, set())
        lacking = []
        for p, avus in sorted(raw_objs.items()):
            have = {a for a, _v, _u in avus}
            miss = [a for a in siteavus.CORE_ATTRS if a not in have]
            if miss:
                lacking.append(f"{posixpath.basename(p)}: {', '.join(miss[:4])}{'…' if len(miss) > 4 else ''}")
        self.add("avu core", "fail" if (lacking or unread_objs) else ("pass" if raw_objs else "skip"),
                 f"{len(raw_objs)} uploaded raw objects; missing a core attribute: {len(lacking)}"
                 + (f"; unreadable {len(unread_objs)}" if unread_objs else "")
                 + ("" if raw_objs or unread_objs else " (nothing uploaded yet)"), lacking=_sample(lacking),
                 n_lacking=len(lacking), n_unreadable=len(unread_objs), incomplete=bool(unread_objs) and not lacking)
        kinds_avu = Counter(v for avus in raw_objs.values() for a, v, _u in avus if a == "neon.fileKind")
        files = [r for r in self.files if r["name"] not in unread_objs]
        kinds_listing = Counter(r.get("kind") or "other" for r in files)
        up_names = {nm for reps in self.remote.values() for nm in reps}
        kinds_uploaded = Counter(r.get("kind") or "other" for r in files if r["name"] in up_names)
        if kinds_avu == kinds_listing:
            st = "pass"
        elif kinds_avu == kinds_uploaded:
            st = "warn"
        else:
            st = "fail"
        incomplete = bool(unread_objs) and st != "fail"  # the kinds that were read agree; some were not read
        self.add("avu file kinds", "fail" if incomplete else st,
                 f"objects with neon.fileKind {sum(kinds_avu.values())} vs listing "
                 f"{sum(kinds_listing.values())} (uploaded {sum(kinds_uploaded.values())})"
                 + (f"; unreadable {len(unread_objs)}" if unread_objs else ""), incomplete=incomplete,
                 carrying=dict(sorted(kinds_avu.items())), listing=dict(sorted(kinds_listing.items())),
                 uploaded=dict(sorted(kinds_uploaded.items())))
        # every collection under the subtree has >= 1 neon.*/mesa.* AVU. Equality GenQueries only: a LIKE over
        # CollectionMeta runs into the iCAT's 120 s timeout at CyVerse; desired collections were read above.
        if colls_known is None:
            self.add("avu collections", "fail", "subtree collections unreadable (iRODS errors after retries)",
                     error=str(known), incomplete=True)
            return
        in_scope = sorted(c for c in colls_known if self.coll_ok(c))
        coll_avus = dict(self.current_colls)
        todo = [c for c in in_scope if c not in coll_avus]
        failed = []
        for res in pool.map(lambda s, c: (c, bulk.query_collection_avus(s, c)[1]), todo, self.threads):
            if isinstance(res, ReadError):
                failed.append(f"{res.item}: {res.error}")
            else:
                coll_avus[res[0]] = res[1]
        bare = [c for c in in_scope if c in coll_avus
                and not any(str(a).startswith(OUR_NAMESPACES) for a, _v, _u in coll_avus[c])]
        self.add("avu collections", "fail" if (bare or failed) else ("pass" if in_scope else "skip"),
                 f"{len(in_scope)} collections under sites/{self.site} in scope; without neon.*/mesa.* AVU: "
                 f"{len(bare)}; unreadable {len(failed)}",
                 bare=_sample(c[len(self.S):] or "/" for c in bare), n_bare=len(bare), unreadable=_sample(failed),
                 incomplete=bool(failed) and not bare)

    def coll_ok(self, c: str) -> bool:
        # dot collections (.mesa-journal/v/<push>, staged/, retired/) are the pipeline's own bookkeeping, not
        # NEON data: they never carry neon.*/mesa.* AVUs
        if any(part.startswith(".") for part in c[len(self.S):].split("/")):
            return False
        raw = f"{self.S}/raw/"
        if not c.startswith(raw):
            return True
        parts = c[len(raw):].split("/")
        return self.p_ok(parts[0]) if len(parts) == 1 else self.pm_ok(parts[0], parts[1])

    # ------------------------------------------------------------ stacking
    def check_stack(self) -> None:
        d = self.data
        files = self.files
        # rows: sampled local CSV record counts vs file_stats
        cands = [r for r in files if r.get("kind") == "data" and (r.get("ext") or "").lower() == "csv"
                 and r["name"] in d.file_stats] if not self.metadata_only else []
        rng = random.Random(f"{self.site}-{len(cands)}")
        pick = rng.sample(cands, min(self.sample, len(cands)))
        bad, checked, lines_total = [], 0, 0
        for r in pick:
            path = env.ROOT / (r.get("localRelPath") or f"sites/{self.site}/raw/{r['productCode']}/{r['month']}/"
                                                      f"{r['name']}")
            recs = siteavus.csv_records(path)
            if recs is None:
                continue
            checked += 1
            with path.open("rb") as fh:
                lines = sum(1 for _ in fh)
            lines_total += lines
            want = int(d.file_stats[r["name"]].get("rowCount") or 0)
            if recs != want:
                bad.append(f"{r['name']}: csv records {recs} (wc -l {lines}) != file_stats {want}")
        # Σ file_stats per lake table vs table_catalog
        sums: Counter = Counter()
        for s in d.file_stats.values():
            if self.p_ok(s["productCode"]) and s.get("lakeTable"):
                sums[s["lakeTable"]] += int(s.get("rowCount") or 0)
        sum_bad = []
        full = self.M is None
        for t in d.table_catalog:
            if t.get("kind") not in ("data", "h5") or not self.p_ok(t["productCode"]) or t["tableName"] not in sums:
                continue
            before = t.get("rowsBeforeDedup")
            before = int(before) if before is not None else int(t.get("nRows") or 0) + int(t.get("dedupRemoved") or 0)
            if full and sums[t["tableName"]] != before:
                sum_bad.append(f"{t['tableName']}: Σ file_stats {sums[t['tableName']]} != table_catalog {before}")
        if not pick and not sums:
            self.add("stack rows", "skip", "no stacked data files in the selection")
        else:
            self.add("stack rows", "fail" if (bad or sum_bad) else "pass",
                     f"{checked} sampled CSVs: record count == file_stats for {checked - len(bad)} "
                     f"(wc -l total {lines_total}); " + (f"lake tables Σ rowCount == table_catalog for "
                                                         f"{len(sums) - len(sum_bad)}/{len(sums)}" if full else
                                                         "Σ rowCount vs table_catalog skipped (month filter)"),
                     mismatches=_sample(bad), sum_mismatches=_sample(sum_bad))
        # typed share
        intended = typed = 0
        untyped = []
        for t in d.table_catalog:
            if t.get("kind") != "data" or not self.p_ok(t["productCode"]):
                continue
            cols = d.lake_columns.get(t["tableName"], {})
            for c, ctype in cols.items():
                vt = d.variables.get((t["productCode"], t.get("neonTable"), c))
                if vt is None or vt in STRING_TYPES:
                    continue
                intended += 1
                if ctype.upper() != "VARCHAR":
                    typed += 1
                else:
                    untyped.append(f"{t['tableName']}.{c} ({vt})")
        if intended:
            share = typed / intended
            self.add("stack typed", "pass" if share >= TYPED_MIN else "fail",
                     f"{typed}/{intended} non-string NEON columns typed ({100 * share:.1f}%, need >= "
                     f"{100 * TYPED_MIN:.0f}%); cast_errors rows {len(d.cast_errors)}",
                     kept_varchar=_sample(untyped), share=round(share, 4))
        else:
            self.add("stack typed", "skip", "no typed NEON columns in the selection")
        # IS files -> sensor positions
        is_files = [r for r in files if r.get("kind") == "data" and r.get("hor") and r.get("ver")]
        if is_files:
            miss = [r["name"] for r in is_files
                    if (r["productCode"], r["month"], f"{r['hor']}.{r['ver']}") not in d.sensor_keys]
            share = 1 - len(miss) / len(is_files)
            self.add("stack sensors", "pass" if share >= SENSOR_MIN else "fail",
                     f"{len(is_files) - len(miss)}/{len(is_files)} IS files resolve to a sensor position "
                     f"({100 * share:.1f}%, need >= {100 * SENSOR_MIN:.0f}%)", unresolved=_sample(miss))
        else:
            self.add("stack sensors", "skip", "no IS data files in the selection")

    def check_taxa(self, onto: siteavus.Ontology | None) -> None:
        taxa = {(tid, sci) for p, tid, sci in self.data.taxa if self.p_ok(p)}
        if not taxa:
            self.add("taxa", "skip", "no taxa in the selection")
            return
        if onto is None or not onto.has("ref_ncbitaxon_map"):
            self.add("taxa", "skip", f"{len(taxa)} taxa; ref_ncbitaxon_map not available")
            return
        by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for (_tt, tid), r in onto.taxa.items():
            by_id[tid].append(r)
        ok = [t for t in taxa if any(r.get("status") == "ok" for r in by_id.get(t[0], []))]
        methods = Counter(r.get("method") for t in ok for r in by_id[t[0]][:1])
        self.add("taxa", "info", f"{len(ok)}/{len(taxa)} taxa resolved to NCBITaxon "
                                 f"({100 * len(ok) / len(taxa):.1f}%); methods {dict(methods)}",
                 unresolved=_sample(sorted(f"{tid} {sci}" for tid, sci in taxa - set(ok))))

    # ------------------------------------------------------------ ontology + DataCite
    def ontology_pairs(self) -> dict[str, set[str]]:
        """CURIE -> attributes that carry it (ontology AVUs of the selected desired units)."""
        out: dict[str, set[str]] = defaultdict(set)
        for unit_id, a, u in self.con.execute(
                "SELECT unit_id, attribute, unit FROM avu_desired WHERE scope=? AND unit != ''", (self.site,)):
            if not self.unit_ok(unit_id):
                continue
            m = CURIE_RE.match(u or "")
            prefix = a.split(".", 1)[0].lower()
            if m and "." in a and prefix not in NON_ONTOLOGY_PREFIXES and m.group(1).lower() == prefix:
                out[u].add(a)
        return out

    def check_ontology(self) -> None:
        pairs = self.ontology_pairs()
        if not pairs:
            self.add("ontology", "skip", "no ontology AVUs in the desired set")
            return
        fn = self._ols_terms
        norm: Callable[[str], str | None] = lambda c: c
        if fn is None:
            try:
                from .site import ontology_store
            except ImportError as exc:
                self.add("ontology", "skip", f"ontology_store unavailable: {exc}")
                return
            fn, norm = ontology_store.ols_terms, ontology_store.norm_curie
        try:
            terms = fn(sorted(pairs)) or {}
        except Exception as exc:  # noqa: BLE001 - OLS down, store locked …
            self.add("ontology", "warn", f"{len(pairs)} CURIEs not re-resolved: {str(exc)[:200]}")
            return
        snake = siteavus.snake
        unresolved, obsolete, wrong, unchecked = [], [], [], []
        for curie, attrs in sorted(pairs.items()):
            key = norm(curie) or curie
            if key not in terms:
                unchecked.append(curie)
                continue
            t = terms[key]
            if t is None:
                unresolved.append(curie)
                continue
            if t.get("isObsolete"):
                obsolete.append(f"{curie} {t.get('label')}")
            for a in sorted(attrs):
                want = f"{a.split('.', 1)[0]}.{snake(t.get('label') or '')}"
                if a != want:
                    wrong.append(f"{a} [{curie}] expected {want}")
        status = "fail" if (unresolved or obsolete or wrong) else ("warn" if unchecked else "pass")
        self.add("ontology", status,
                 f"{len(pairs)} CURIEs: unresolved {len(unresolved)}, obsolete {len(obsolete)}, attribute != "
                 f"prefix.snake(label) {len(wrong)}, not checked (OLS unreachable) {len(unchecked)}",
                 unresolved=_sample(unresolved), obsolete=_sample(obsolete), attribute_mismatch=_sample(wrong),
                 unchecked=_sample(unchecked), prefixes=dict(Counter(c.split(":")[0] for c in pairs)))

    def check_datacite(self) -> None:
        from .avu import datacite

        by_path: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        for unit_id, path, a, v, u in self.con.execute(
                "SELECT unit_id, path, attribute, value, unit FROM avu_desired WHERE scope=? AND target_type="
                "'collection' AND attribute LIKE 'datacite.%'", (self.site,)):
            if self.unit_ok(unit_id):
                by_path[path].add((a, v, u or ""))
        if not by_path:
            self.add("datacite", "skip", "no datacite.* AVUs in the desired set")
            return
        bad, cur_bad, cur_ok, cur_none = [], [], 0, 0
        for path, triples in sorted(by_path.items()):
            try:
                datacite.roundtrip(sorted(triples))
            except ValueError as exc:
                bad.append(f"{path[len(self.S):] or '/'}: {str(exc)[:160]}")
            current = {t for t in self.current_colls.get(path, set()) if t[0].startswith("datacite.")}
            if not current:
                cur_none += 1
                continue
            try:
                datacite.roundtrip(sorted(current))
                cur_ok += 1
            except ValueError as exc:
                cur_bad.append(f"{path[len(self.S):] or '/'}: {str(exc)[:160]}")
        self.add("datacite", "fail" if (bad or cur_bad) else "pass",
                 f"{len(by_path)} site/product records: desired round-trip ok {len(by_path) - len(bad)}; "
                 f"iRODS round-trip ok {cur_ok}, failed {len(cur_bad)}, not tagged yet {cur_none}",
                 desired_failures=_sample(bad), irods_failures=_sample(cur_bad))

    # ------------------------------------------------------------ driver
    def run(self) -> dict[str, Any]:
        own_con = self.con is None
        if self.con is None:
            self.con = sqlite3.connect(f"file:{state.DB_PATH}?mode=ro", uri=True, timeout=60) \
                if state.DB_PATH.exists() else state.connect(state.DB_PATH)
        self.data = self.data or load_site_data(self.site)
        self.check_listing()
        self.check_local()
        units = self.desired_units() if self._has_desired() else {}
        if self.session is None:
            try:
                self.session = env.mesa().session
            except (SystemExit, Exception) as exc:  # noqa: BLE001 - no credentials / iRODS down
                log.warning("iRODS not available (%s): iRODS checks skipped", exc)
        if self.session is not None:
            pool = SessionPool(self.session)
            try:
                self.check_irods_objects(pool)
                if units:
                    self.check_avus(pool, units)
                else:
                    self.add("avu desired", "skip", "no avu_desired rows for this site: run `site build-avus` first")
            finally:
                pool.close()
        else:
            for name in ("irods objects", "avu desired", "avu core", "avu file kinds", "avu collections"):
                self.add(name, "skip", "no iRODS session (run through scripts/with-secrets.sh)")
        self.check_stack()
        onto = None
        try:
            onto = siteavus.load_ontology(self.site)
        except Exception as exc:  # noqa: BLE001
            log.warning("ontology store unreadable: %s", exc)
        self.check_taxa(onto)
        if self.ontology:
            self.check_ontology()
        self.check_datacite()
        if own_con:
            self.con.close()
            self.con = None
        counts = Counter(c.status for c in self.checks)
        return {"site": self.site, "generatedAt": env.utcnow().isoformat(),
                "filters": {"products": sorted(self.P) if self.P else None, "months": sorted(self.M) if self.M else None},
                "metadataOnly": self.metadata_only,
                "irods": self.session is not None, "summary": dict(counts),
                "ok": counts.get("fail", 0) == 0, "incomplete": self.incomplete(),
                "checks": [asdict(c) for c in self.checks]}

    def incomplete(self) -> bool:
        """True when every failed check failed only because some iRODS reads were refused (after retries): the
        site is not known to be wrong, so the verify should run again later rather than fail."""
        failed = [c for c in self.checks if c.status == "fail"]
        return bool(failed) and all(c.details.get("incomplete") for c in failed)

    def _has_desired(self) -> bool:
        try:
            return bool(self.con.execute("SELECT 1 FROM avu_desired WHERE scope=? LIMIT 1", (self.site,)).fetchone())
        except sqlite3.OperationalError:
            return False


def render(report: dict[str, Any]) -> str:
    f = report["filters"]
    head = f"verify site {report['site']}" + (f" (products={f['products'] or '*'} months={f['months'] or '*'})"
                                              if f["products"] or f["months"] else "")
    lines = [head, f"{'check':18} {'status':6} summary", "-" * 100]
    for c in report["checks"]:
        lines.append(f"{c['name']:18} {c['status'].upper():6} {c['summary']}")
    lines.append("-" * 100)
    lines.append("result: " + ", ".join(f"{k}={v}" for k, v in sorted(report["summary"].items()))
                 + (" -> OK" if report["ok"] else " -> INCOMPLETE (iRODS reads refused; retry later)"
                    if report.get("incomplete") else " -> FAILED"))
    return "\n".join(lines)


def main(site: str, products: list[str] | str | None = None, months: list[str] | str | None = None, *,
         threads: int = DEFAULT_THREADS, session: Any | None = None, out: Path | None = None,
         metadata_only: bool = False) -> int:
    """Run every Phase-2 check for ``site``; write ``sites/<SITE>/reports/verify.json``; 1 on any failure, 4
    (``RC_INCOMPLETE``: retry later) when the only failures are iRODS reads that were refused after retries.

    ``metadata_only`` skips the local-file and sampled-CSV checks (a streamed site keeps no raw file)."""
    def split(v: list[str] | str | None) -> list[str] | None:
        items = v.split(",") if isinstance(v, str) else (v or [])
        return [s.strip() for s in items if s and s.strip()] or None

    site = site.upper()
    v = SiteVerifier(site, split(products), split(months), session=session, threads=threads,
                     metadata_only=metadata_only)
    report = v.run()
    path = out or site_lake.site_paths(site).reports_dir / "verify.json"
    env.write_json(path, report)
    print(render(report))
    print(f"\nwrote {path}")
    return 0 if report["ok"] else (RC_INCOMPLETE if report["incomplete"] else 1)


def _in_windows(key: tuple[str, str], windows: dict[str, list[tuple[str, str]]]) -> bool:
    product, month = key
    return any(a <= month <= b for a, b in windows.get(product, []))
