"""`site list|download <SITE>` — file listing (POST /data/query per product) and verified downloads.

list:
  * product set from ``v_site_inventory`` (or the harvest ``stage/pairs`` while the catalog lake is
    missing), filtered by ``--products``/``--months``;
  * one ``POST /data/query`` per product window: current window with ``includeProvisional``; release-only
    months with ``release=<newest containing release>`` and the response filtered to those months;
    year bisection on transport/5xx/413 exactly like ``harvest.Harvest.data_query``;
  * signed URLs go only into ``work/state.sqlite`` ``site_files.url`` (+ ``listed_at``); a pending file
    that disappears from a newer listing is recorded in ``fetch_superseded`` (kept on disk, excluded from
    stacking/upload);
  * externally hosted products get ``external_links`` (Phase-1 external landing, else one GET);
  * site lake tables ``files``, ``site_months``, ``external_links`` are upserted in one snapshot.

download:
  * ``NeonClient.stream_to_file`` with token=None (GCS signed URLs);
  * up to 3 download clients (the bundle's 'download' client plus 2 built like it, sharing the
    bundle's rate limiter and cache), each with at most 8 streams (its httpx pool size);
  * the number of concurrent streams is adaptive (``tuning.AsyncAdaptiveLimit``, AIMD):
    - it starts at ``threads``, with a floor of ``min(4, threads)`` and a ceiling of 24;
    - each file reports its latency divided by ``5 s + bytes / 1.5 MB/s``;
    - it grows by 1 per 16 on-time files while binding;
    - it halves on transport errors, timeouts, 429 or 5xx, or when p95 exceeds 4x the target;
    - each new stream goes to the least-loaded client;
  * MD5 is hard for data files and soft for EML/readme (kept with ``md5_verified=0`` on mismatch);
  * per-file retry x4: checksum mismatch -> retry then re-list the product; GCS 400/403/404 -> re-list;
    transport/5xx -> backoff; rate_limited -> sleep ``retryAfterSeconds``;
  * resume: ``dl_status='ok'`` rows whose file exists with ``size == dl_bytes`` are skipped; stale
    ``*.part`` files are removed first; URLs older than 6 days are refreshed by a re-list;
  * evicted files are never re-downloaded. These have ``site_files.local_evicted = 1``, a column added by
    the evict step that may be absent. Their raw file was deleted locally after it was verified in iRODS,
    so they count as ok. The one exception is a row a re-list marked ``stale`` (upstream MD5 changed):
    it is downloaded again and its ``local_evicted`` is reset to 0;
  * MB/s and concurrency are logged with progress and in ``reports/download_summary.json``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .. import env, state
from ..tuning import AsyncAdaptiveLimit
from . import plan as planmod
from . import site_lake

log = logging.getLogger("neon_ducklake.site.fetch")

SOFT_KINDS = frozenset({"eml", "readme"})
RELIST_AFTER = timedelta(days=6)
DL_CONCURRENCY = 8  # streams per download client (= its httpx connection pool)
DL_CLIENTS = 3
DL_MAX = DL_CLIENTS * DL_CONCURRENCY
DL_MIN = 4
DL_TARGET_S = 5.0  # per-file latency target: 5 s + bytes / DL_REF_BPS
DL_REF_BPS = 1.5e6
DL_CEILING = 4.0  # halve when p95(latency / target) exceeds this
DL_GROW_EVERY = 16
DL_WINDOW = 64
DL_MIN_SAMPLES = 16
DL_COOLDOWN_S = 5.0
LIST_CONCURRENCY = 4
DL_TRIES = 4
PROGRESS_EVERY = 500
MIB = 1 << 20
RETRY_DELAYS = [5, 15, 45, 120, 300]
MAX_RELISTS_PER_PRODUCT = 3

SUPERSEDED_SCHEMA = """
CREATE TABLE IF NOT EXISTS fetch_superseded (
  site TEXT NOT NULL, product TEXT NOT NULL, month TEXT NOT NULL, name TEXT NOT NULL,
  was_downloaded INTEGER, superseded_at TEXT, PRIMARY KEY (site, product, month, name)
);
"""


class ListingError(RuntimeError):
    """A /data/query window that failed after bisection and app-level retries."""


def max_bytes_for(size: int | None) -> int:
    return int(size * 1.1) + MIB if size else 64 * MIB


def dl_target_seconds(nbytes: int | None) -> float:
    return DL_TARGET_S + max(0, nbytes or 0) / DL_REF_BPS


def extra_download_clients(bundle: Any, n: int) -> list[Any]:
    """``n`` more 'download' NeonClients built exactly like ``env.neon_bundle`` builds that role.

    They share the bundle's rate limiter and cache. Each client is its own 8-connection pool. This
    returns [] for a bundle that isn't a real ``env.NeonBundle`` (tests).
    """
    cfg = getattr(bundle, "config", None)
    base = bundle.client("download")
    limiter = getattr(base, "_limiter", None)
    cache = getattr(base, "cache", None)
    if n <= 0 or cfg is None or limiter is None or cache is None:
        return []
    from neon_mcp.neon.client import NeonClient

    role = env.ROLE_SETTINGS["download"]
    out = []
    for _ in range(n):
        ncfg = cfg.neon.model_copy(deep=True)
        ncfg.retries.max_attempts = role["max_attempts"]
        ncfg.read_timeout_s = role["read_timeout_s"]
        ncfg.download_read_timeout_s = max(ncfg.download_read_timeout_s, role["read_timeout_s"])
        out.append(NeonClient(ncfg, cache=cache, limiter=limiter,
                              send_token_on_public=cfg.effective_token_for_public("stdio"),
                              allowed_hosts=cfg.downloads.allowed_hosts))
    return out


class DownloadLanes:
    """Adaptive concurrent GCS streams spread over download clients, never more than 8 per client."""

    def __init__(self, clients: list[Any], start: int) -> None:
        if not clients:
            raise ValueError("at least one download client is required")
        self.clients = clients
        hi = max(1, min(DL_MAX, len(clients) * DL_CONCURRENCY))
        start = max(1, min(start, hi))
        self.limit = AsyncAdaptiveLimit("download", start=start, lo=min(DL_MIN, start), hi=hi, target=1.0,
                                        ceiling=DL_CEILING, every=DL_GROW_EVERY, window=DL_WINDOW,
                                        min_samples=DL_MIN_SAMPLES, cooldown_s=DL_COOLDOWN_S)
        self.in_flight = [0] * len(clients)
        self.peak_per_client = [0] * len(clients)

    @contextlib.asynccontextmanager
    async def slot(self) -> Any:
        async with self.limit:
            i = min(range(len(self.clients)), key=self.in_flight.__getitem__)
            self.in_flight[i] += 1
            self.peak_per_client[i] = max(self.peak_per_client[i], self.in_flight[i])
            try:
                yield self.clients[i]
            finally:
                self.in_flight[i] -= 1

    def stats(self) -> dict[str, Any]:
        snap = self.limit.snapshot()
        return {"clients": len(self.clients), "concurrencyStart": int(snap["start"]),
                "concurrencyFinal": int(snap["current"]), "concurrencyMin": int(snap["min"]),
                "concurrencyMax": int(snap["max"]), "peakInFlight": snap["peak_in_flight"],
                "peakPerClient": list(self.peak_per_client), "latencyP95Ratio": snap["p95"],
                "errorsByKind": snap["errors_by_kind"], "changes": snap["changes"]}


def month_runs(months: list[str]) -> list[tuple[str, str]]:
    """Contiguous runs of sorted YYYY-MM months -> [(start, end)]."""
    from ..harvest import month_range

    runs: list[tuple[str, str]] = []
    ms = sorted(set(months))
    i = 0
    while i < len(ms):
        j = i
        while j + 1 < len(ms) and month_range(ms[j], ms[j + 1]) == [ms[j], ms[j + 1]]:
            j += 1
        runs.append((ms[i], ms[j]))
        i = j + 1
    return runs


def filter_pairs(pairs: list[dict], products: list[str] | None, months: list[str] | None) -> list[dict]:
    """Restrict pairs to the requested products and months (dropping pairs left without months)."""
    want_p = set(products or [])
    want_m = set(months or [])
    out = []
    for p in pairs:
        if want_p and p["product"] not in want_p:
            continue
        ms = [m for m in p["months"] if not want_m or m in want_m]
        if not ms:
            continue
        out.append({**p, "months": ms, "filtered": bool(want_m)})
    return out


def query_windows(pair: dict) -> list[tuple[str, str]]:
    """Unfiltered pairs use the harvest's one full window; a month filter queries contiguous runs only."""
    if pair.get("filtered"):
        return month_runs(pair["months"])
    return [(pair["start"], pair["end"])]


def safe_error(exc: BaseException) -> str:
    """Error text without response bodies (GCS error XML can echo signed-URL parts)."""
    from neon_mcp.errors import NeonApiError, ToolError

    if isinstance(exc, ToolError):
        return f"tool:{exc.code}"
    if isinstance(exc, NeonApiError):
        return f"neon:{exc.kind}:{exc.status}"
    return type(exc).__name__


def _ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _pub_ts(stamp: str | None) -> datetime | None:
    if not stamp:
        return None
    try:
        return datetime.strptime(stamp + "+0000", "%Y%m%dT%H%M%SZ%z").replace(tzinfo=None)  # naive UTC
    except ValueError:
        return None


@dataclass
class ListedFile:
    product: str
    month: str
    name: str
    release: str | None
    release_only: bool
    kind: str
    size: int | None
    md5: str | None
    crc32c: str | None
    url: str | None


@dataclass
class Fetcher:
    """One site's list/download state (single asyncio loop, one SQLite connection)."""

    site: str
    bundle: Any
    con: sqlite3.Connection
    pairs: list[dict]
    threads: int = 8
    run_id: str = field(default_factory=env.run_id)
    site_months: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    external_rows: list[dict[str, Any]] = field(default_factory=list)
    listed_products: set[str] = field(default_factory=set)
    errors: list[dict[str, Any]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    list_sem: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(LIST_CONCURRENCY))
    relist_locks: dict[str, asyncio.Lock] = field(default_factory=lambda: defaultdict(asyncio.Lock))
    relist_count: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    relisted_at: dict[str, float] = field(default_factory=dict)
    sleep: Any = asyncio.sleep
    extra_clients: list[Any] | None = None  # None: built from the bundle when downloads start
    download_stats: dict[str, Any] = field(default_factory=dict)
    _own_extra: list[Any] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.site = self.site.upper()
        self.paths = site_lake.site_paths(self.site)
        self.con.executescript(SUPERSEDED_SCHEMA)

    # ------------------------------------------------------------ app-level retry
    async def _call(self, fn: Any, tries: int = 5) -> Any:
        from neon_mcp.errors import NeonApiError, ToolError

        last: Exception | None = None
        for attempt in range(tries):
            try:
                return await fn()
            except ToolError as exc:
                if exc.code != "rate_limited":
                    raise
                last = exc
                await self.sleep(min(float((exc.details or {}).get("retryAfterSeconds", 30)) + 1, 300))
            except NeonApiError as exc:
                last = exc
                if not (exc.kind == "transport" or exc.status in (429, 500, 502, 503, 504)):
                    raise
                await self.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
        assert last is not None
        raise last

    # ------------------------------------------------------------ listing
    async def query(self, pair: dict, start: str, end: str, depth: int = 0) -> list[dict]:
        """``releases`` of one /data/query window (bisected on transport/5xx/413, like the harvest)."""
        from neon_mcp.errors import NeonApiError

        from ..harvest import split_window

        rel = pair["release"]
        body: dict[str, Any] = {"productCode": pair["product"], "siteCodes": [self.site], "startDateMonth": start,
                                "endDateMonth": end, "package": "basic", "includeProvisional": rel is None}
        if rel:
            body["release"] = rel
        client = self.bundle.client("list")
        try:
            env_ = await client.post_json("/data/query", body, token=self.bundle.token, timeout_s=300)
        except NeonApiError as exc:
            splittable = exc.kind == "transport" or (exc.status or 0) >= 500 or exc.status == 413
            parts = split_window(start, end) if splittable and depth < 6 else []
            if parts:
                self.counts["listSplits"] += 1
                log.info("list %s %s %s..%s: %s -> split", pair["product"], rel or "current", start, end,
                         safe_error(exc))
                out: list[dict] = []
                for s, e in parts:
                    out.extend(await self.query(pair, s, e, depth + 1))
                return out
            try:
                env_ = await self._call(lambda: client.post_json("/data/query", body, token=self.bundle.token,
                                                                 timeout_s=300))
            except Exception as exc2:
                raise ListingError(f"{pair['product']} {start}..{end} {rel or ''}: {safe_error(exc2)}") from exc2
        data = env_.data or {}
        releases = data.get("releases") or []
        keep = set(pair["months"]) if (rel or pair.get("filtered")) else None
        if keep is not None:
            for r in releases:
                r["packages"] = [pk for pk in r.get("packages") or [] if pk.get("month") in keep]
        return releases

    def _files_from(self, pair: dict, releases: list[dict], listed_at: str) -> list[ListedFile]:
        from neon_mcp.neon.downloads import BASENAME_RE
        from neon_mcp.neon.filenames import parse_neon_filename

        product = pair["product"]
        release_only = pair["scope"] == "release-only"
        out: list[ListedFile] = []
        for r in releases:
            tag = r.get("release")
            for pk in r.get("packages") or []:
                month = pk.get("month")
                files = pk.get("files") or []
                sm = self.site_months.setdefault((product, month), {
                    "productCode": product, "month": month, "release": tag, "isProvisional": tag == "PROVISIONAL",
                    "isReleaseOnly": release_only, "releaseGenerationDate": _ts(r.get("generationDate")),
                    "packageGenerationDate": _ts(pk.get("generationDate")), "fileCount": 0, "totalBytes": 0,
                    "isExternal": False})
                sm["fileCount"] = len(files)
                sm["totalBytes"] = sum(int(f.get("size") or 0) for f in files)
                for f in files:
                    name = f.get("name") or ""
                    if not BASENAME_RE.match(name):
                        log.warning("skipping unsafe file name in %s %s: %r", product, month, name[:80])
                        self.counts["unsafeNames"] += 1
                        continue
                    parsed = parse_neon_filename(name)
                    out.append(ListedFile(product=product, month=month, name=name, release=tag,
                                          release_only=release_only, kind=parsed.kind,
                                          size=int(f["size"]) if f.get("size") not in (None, "") else None,
                                          md5=(f.get("md5") or None), crc32c=(f.get("crc32c") or None),
                                          url=f.get("url")))
        return out

    def record_files(self, product: str, files: list[ListedFile], listed_months: set[str], listed_at: str) -> dict:
        """Upsert listed files into site_files; mark pending rows missing from this listing as superseded."""
        existing = {(r["month"], r["name"]): r for r in self.con.execute(
            "SELECT month, name, md5, kind, dl_status, dl_md5 FROM site_files WHERE site=? AND product=?",
            (self.site, product))}
        seen: set[tuple[str, str]] = set()
        stats = {"files": 0, "bytes": 0, "new": 0, "stale": 0}
        self.con.execute("BEGIN")
        try:
            for f in files:
                key = (f.month, f.name)
                seen.add(key)
                ex = existing.get(key)
                dl_status = ex["dl_status"] if ex else None
                if ex is None:
                    stats["new"] += 1
                if (ex is not None and dl_status == "ok" and f.kind not in SOFT_KINDS and f.md5 and ex["dl_md5"]
                        and f.md5.lower() != ex["dl_md5"].lower()):
                    dl_status = "stale"  # upstream content changed: re-download
                    stats["stale"] += 1
                if dl_status == "superseded":
                    dl_status = None
                url = None if dl_status == "ok" else f.url
                self.con.execute(
                    "INSERT INTO site_files(site, product, month, name, release, release_only, kind, size, md5, crc32c,"
                    " url, listed_at, local_path, dl_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(site, product, month, name) DO UPDATE SET release=excluded.release,"
                    " release_only=excluded.release_only, kind=excluded.kind, size=excluded.size, md5=excluded.md5,"
                    " crc32c=excluded.crc32c, url=excluded.url, listed_at=excluded.listed_at,"
                    " local_path=excluded.local_path, dl_status=excluded.dl_status",
                    (self.site, product, f.month, f.name, f.release, int(f.release_only), f.kind, f.size, f.md5,
                     f.crc32c, url, listed_at, self.paths.raw_rel(product, f.month, f.name), dl_status))
                self.con.execute("DELETE FROM fetch_superseded WHERE site=? AND product=? AND month=? AND name=?",
                                 (self.site, product, f.month, f.name))
                stats["files"] += 1
                stats["bytes"] += f.size or 0
            superseded = [(k, r) for k, r in existing.items() if k not in seen and k[0] in listed_months]
            for (month, name), r in superseded:
                was_ok = r["dl_status"] == "ok"
                self.con.execute("INSERT OR REPLACE INTO fetch_superseded VALUES (?,?,?,?,?,?)",
                                 (self.site, product, month, name, int(was_ok), listed_at))
                if not was_ok:
                    self.con.execute("UPDATE site_files SET dl_status='superseded', url=NULL WHERE site=? AND "
                                     "product=? AND month=? AND name=?", (self.site, product, month, name))
            stats["superseded"] = len(superseded)
            self.con.execute("COMMIT")
        except BaseException:
            self.con.execute("ROLLBACK")
            raise
        return stats

    def _listing_row(self, product: str, part: str, release: str | None, start: str, end: str, listed_at: str,
                     status: str, n_files: int, n_bytes: int, error: str | None) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO listings(site, product, part, release, start_month, end_month, listed_at, status,"
            " n_files, n_bytes, error) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (self.site, product, part, release, start, end, listed_at, status, n_files, n_bytes, error))

    async def list_product(self, product: str) -> dict[str, Any]:
        """List every (filtered) window of one product; external products get their links instead."""
        pairs = [p for p in self.pairs if p["product"] == product]
        if not pairs:
            return {"product": product, "files": 0}
        if any(p.get("external") for p in pairs):
            await self.external(pairs)
            return {"product": product, "external": True}
        listed_at = env.utcnow().isoformat()
        files: list[ListedFile] = []
        months: set[str] = set()
        ok = True
        async with self.list_sem:
            for pair in pairs:
                for start, end in query_windows(pair):
                    part = f"{start}_{end}" + (f"__{pair['release']}" if pair["release"] else "")
                    try:
                        releases = await self.query(pair, start, end)
                    except ListingError as exc:
                        ok = False
                        self.errors.append({"product": product, "part": part, "error": str(exc)})
                        self._listing_row(product, part, pair["release"], start, end, listed_at, "error", 0, 0,
                                          str(exc)[:500])
                        log.warning("list %s failed: %s", product, exc)
                        continue
                    got = self._files_from(pair, releases, listed_at)
                    files.extend(got)
                    months.update(m for m in pair["months"] if start <= m <= end)
                    months.update(pk.get("month") for r in releases for pk in r.get("packages") or [])
                    self._listing_row(product, part, pair["release"], start, end, listed_at, "ok", len(got),
                                      sum(f.size or 0 for f in got), None)
        if not files and ok:
            # A product with availability but no files is externally hosted (phenocam/AERONET pattern).
            log.info("list %s: 0 files -> treating as external", product)
            await self.external(pairs)
            return {"product": product, "external": True}
        stats = self.record_files(product, files, months if ok else set(), listed_at)
        self.listed_products.add(product)
        self.counts["files"] += stats["files"]
        self.counts["bytes"] += stats["bytes"]
        log.info("list %s: %d files %.1f MB (new %d, stale %d, superseded %d)", product, stats["files"],
                 stats["bytes"] / 1e6, stats["new"], stats["stale"], stats["superseded"])
        return {"product": product, **stats, "ok": ok}

    async def external(self, pairs: list[dict]) -> None:
        """external_links rows from the Phase-1 landing, else one GET /data/{p}/{site}/{lastMonth}."""
        product = pairs[0]["product"]
        all_months = sorted({m for p in pairs for m in (p.get("allMonths") or p["months"])})
        last = all_months[-1]
        payload = planmod.latest_raw("external", f"{product}__{self.site}__{last}")
        if payload is None:
            client = self.bundle.client("list")
            try:
                env_ = await self._call(lambda: client.get_json(f"/data/{product}/{self.site}/{last}",
                                                                {"package": "basic"}, token=self.bundle.token,
                                                                timeout_s=120))
                payload = env_.data or {}
            except Exception as exc:  # noqa: BLE001
                self.errors.append({"product": product, "part": f"external {last}", "error": safe_error(exc)})
                log.warning("external %s %s failed: %s", product, last, safe_error(exc))
                return
        links = payload.get("externalData") or []
        for link in links:
            self.external_rows.append({"productCode": product, "url": link.get("url"), "name": link.get("name"),
                                       "type": link.get("type"), "monthProbed": last, "months": all_months})
        releases = {}
        for p in pairs:
            releases.update(p.get("monthReleases") or {})
        for p in pairs:
            for m in p["months"]:
                tag = p["release"] or releases.get(m) or payload.get("release")
                self.site_months[(product, m)] = {
                    "productCode": product, "month": m, "release": tag, "isProvisional": tag == "PROVISIONAL",
                    "isReleaseOnly": p["scope"] == "release-only", "releaseGenerationDate": None,
                    "packageGenerationDate": None, "fileCount": 0, "totalBytes": 0, "isExternal": True}
        self.listed_products.add(product)
        self.counts["externalLinks"] += len(links)
        self._listing_row(product, f"external__{last}", None, all_months[0], last, env.utcnow().isoformat(), "ok",
                          0, 0, None)
        log.info("list %s: external, %d link(s)", product, len(links))

    async def list_all(self) -> list[dict]:
        products = sorted({p["product"] for p in self.pairs})
        results = await asyncio.gather(*(self.list_product(p) for p in products))
        return list(results)

    # ------------------------------------------------------------ download
    def pending_rows(self, products: list[str] | None, months: list[str] | None) -> list[sqlite3.Row]:
        """Rows to download; ``ok`` rows whose file is missing or the wrong size are reset to pending."""
        where = ["site=?", "coalesce(dl_status, '') != 'superseded'"]
        args: list[Any] = [self.site]
        if products:
            where.append(f"product IN ({','.join('?' * len(products))})")
            args += products
        if months:
            where.append(f"month IN ({','.join('?' * len(months))})")
            args += months
        rows = self.con.execute(f"SELECT * FROM site_files WHERE {' AND '.join(where)}", args).fetchall()
        todo = []
        evict_col = bool(rows) and "local_evicted" in set(rows[0].keys())  # column added by the evict step
        for r in rows:
            if evict_col and r["local_evicted"] == 1 and r["dl_status"] != "stale":
                self.counts["evicted"] += 1  # verified in iRODS and deleted locally: counts as ok
                continue
            if r["dl_status"] == "ok":
                dest = env.ROOT / r["local_path"]
                try:
                    if dest.stat().st_size == r["dl_bytes"]:
                        self.counts["skipped"] += 1
                        continue
                except FileNotFoundError:
                    pass
                self.con.execute("UPDATE site_files SET dl_status=NULL WHERE site=? AND product=? AND month=? AND "
                                 "name=?", (self.site, r["product"], r["month"], r["name"]))
                self.counts["resetMissing"] += 1
            todo.append(r)
        return todo

    def clean_parts(self) -> int:
        n = 0
        if self.paths.raw_dir.exists():
            for p in self.paths.raw_dir.rglob("*.part"):
                p.unlink(missing_ok=True)
                n += 1
        return n

    def _url(self, r: sqlite3.Row) -> tuple[str | None, str | None]:
        row = self.con.execute("SELECT url, listed_at FROM site_files WHERE site=? AND product=? AND month=? AND "
                               "name=?", (self.site, r["product"], r["month"], r["name"])).fetchone()
        return (row["url"], row["listed_at"]) if row else (None, None)

    async def relist(self, product: str, reason: str) -> bool:
        """Refresh a product's signed URLs (at most MAX_RELISTS_PER_PRODUCT per run; coalesced per minute)."""
        async with self.relist_locks[product]:
            if time.monotonic() - self.relisted_at.get(product, -1e9) < 60:
                return True
            if self.relist_count[product] >= MAX_RELISTS_PER_PRODUCT:
                return False
            self.relist_count[product] += 1
            self.counts["relists"] += 1
            log.info("re-listing %s (%s)", product, reason)
            res = await self.list_product(product)
            self.relisted_at[product] = time.monotonic()
            return bool(res.get("ok", True))

    async def refresh_expired(self, rows: list[sqlite3.Row]) -> None:
        now = env.utcnow()
        need = set()
        for r in rows:
            listed = _ts(r["listed_at"])
            if not r["url"] or listed is None or now - listed > RELIST_AFTER:
                need.add(r["product"])
        if need:
            log.info("refreshing signed URLs for %d product(s): %s", len(need), ", ".join(sorted(need)))
            await asyncio.gather(*(self.relist(p, "url missing or older than 6 days") for p in sorted(need)))

    async def download_one(self, r: sqlite3.Row, lanes: DownloadLanes, progress: dict[str, Any]) -> None:
        import httpx
        from neon_mcp.errors import NeonApiError, ToolError

        dest = env.ROOT / r["local_path"]
        soft = r["kind"] in SOFT_KINDS
        expected = None if soft else (r["md5"] or None)
        limit = max_bytes_for(r["size"])
        last = "no_attempt"
        attempts = waits = 0
        outcome = None
        while attempts < DL_TRIES:
            url, _listed = self._url(r)
            if not url:
                if not await self.relist(r["product"], "no url"):
                    last = "no_url"
                    break
                url, _listed = self._url(r)
                if not url:
                    last = "not_in_listing"
                    break
            attempts += 1
            try:
                async with lanes.slot() as client:
                    t0 = time.monotonic()
                    outcome = await client.stream_to_file(url, dest, token=None, max_bytes=limit,
                                                          expected_md5=expected)
                lanes.limit.on_success((time.monotonic() - t0) / dl_target_seconds(outcome.bytes))
                break
            except ToolError as exc:
                last = f"tool:{exc.code}"
                if exc.code == "rate_limited" and waits < 10:
                    attempts -= 1  # waiting for the limiter is not a failed attempt
                    waits += 1
                    await self.sleep(min(float((exc.details or {}).get("retryAfterSeconds", 30)) + 1, 300))
                elif exc.code == "checksum_mismatch":
                    self.counts["checksumRetries"] += 1
                    if attempts >= 2:
                        await self.relist(r["product"], "checksum mismatch")
                    await self.sleep(RETRY_DELAYS[min(attempts - 1, 2)] + random.uniform(0, 1))
                elif exc.code == "download_limit_exceeded" and (soft or not r["size"]):
                    limit *= 4
                else:
                    break
            except NeonApiError as exc:
                last = safe_error(exc)
                if exc.status in (400, 403, 404, 410):
                    self.counts["gcsRelists"] += 1
                    if not await self.relist(r["product"], f"GCS {exc.status}"):
                        break
                elif exc.kind == "transport" or exc.status == 429 or (exc.status or 0) >= 500:
                    lanes.limit.on_error("transport" if exc.kind == "transport" else f"http_{exc.status}")
                    await self.sleep(RETRY_DELAYS[min(attempts - 1, len(RETRY_DELAYS) - 1)])
                else:
                    break
            except (httpx.HTTPError, OSError) as exc:  # mid-body failures are not retried by the client
                last = f"transport:{type(exc).__name__}"
                if isinstance(exc, httpx.TimeoutException | TimeoutError):
                    lanes.limit.on_error("timeout")
                elif isinstance(exc, httpx.HTTPError | ConnectionError):
                    lanes.limit.on_error("transport")
                await self.sleep(RETRY_DELAYS[min(attempts - 1, len(RETRY_DELAYS) - 1)])
        key = (self.site, r["product"], r["month"], r["name"])
        if outcome is None:
            self.con.execute("UPDATE site_files SET dl_status='error', dl_error=?, dl_attempts=coalesce(dl_attempts,0)+?"
                             " WHERE site=? AND product=? AND month=? AND name=?", (last, attempts, *key))
            self.counts["failed"] += 1
            self.errors.append({"product": r["product"], "month": r["month"], "name": r["name"], "error": last})
            log.warning("download failed %s/%s/%s: %s", r["product"], r["month"], r["name"], last)
        else:
            if soft:
                verified = bool(r["md5"]) and outcome.md5.lower() == str(r["md5"]).lower()
                if not verified:
                    self.counts["softMismatch"] += 1
            else:
                verified = outcome.md5_verified is True
                if not verified:
                    self.counts["unverified"] += 1
            evicted = ", local_evicted=0" if progress.get("evictedColumn") else ""
            self.con.execute(
                "UPDATE site_files SET dl_md5=?, dl_bytes=?, md5_verified=?, downloaded_at=?, dl_status='ok',"
                f" dl_error=NULL, url=NULL, dl_attempts=coalesce(dl_attempts,0)+?{evicted} WHERE site=? AND"
                " product=? AND month=? AND name=?",
                (outcome.md5, outcome.bytes, int(verified), env.utcnow().isoformat(), attempts, *key))
            self.counts["downloaded"] += 1
            self.counts["downloadedBytes"] += outcome.bytes
        progress["n"] += 1
        if progress["n"] % PROGRESS_EVERY == 0 or progress["n"] == progress["total"]:
            el = max(time.monotonic() - progress["t0"], 1e-6)
            mb = self.counts["downloadedBytes"] / 1e6
            log.info("download %d/%d files, %.1f MB, %.2f MB/s, concurrency %d (in flight %d over %d client(s)),"
                     " failed %d", progress["n"], progress["total"], mb, mb / el, lanes.limit.limit,
                     lanes.limit.in_flight, len(lanes.clients), self.counts["failed"])

    def download_clients(self, pending: int) -> list[Any]:
        """The bundle's download client plus up to DL_CLIENTS - 1 more (only as many as ``pending`` can use)."""
        want = max(0, min(DL_CLIENTS - 1, -(-pending // DL_CONCURRENCY) - 1))
        if self.extra_clients is None:
            self.extra_clients = extra_download_clients(self.bundle, want)
            self._own_extra = list(self.extra_clients)
        return [self.bundle.client("download"), *self.extra_clients[:want]]

    async def aclose_extra(self) -> None:
        for c in self._own_extra:
            with contextlib.suppress(Exception):
                await c.aclose()
        self._own_extra = []

    def _has_evicted_column(self) -> bool:
        return any(r[1] == "local_evicted" for r in self.con.execute("PRAGMA table_info(site_files)"))

    async def download_all(self, products: list[str] | None, months: list[str] | None) -> None:
        self.counts["partsRemoved"] = self.clean_parts()
        rows = self.pending_rows(products, months)
        log.info("download %s: %d file(s) pending, %d already ok, %d evicted (kept in iRODS only)", self.site,
                 len(rows), self.counts["skipped"], self.counts["evicted"])
        if not rows:
            return
        await self.refresh_expired(rows)
        lanes = DownloadLanes(self.download_clients(len(rows)), max(self.threads, 1))
        progress: dict[str, Any] = {"n": 0, "total": len(rows), "t0": time.monotonic(),
                                    "evictedColumn": self._has_evicted_column()}
        log.info("download %s: %d client(s), concurrency %d (adaptive %d..%d)", self.site, len(lanes.clients),
                 lanes.limit.limit, int(lanes.limit.lo), int(lanes.limit.hi))
        # biggest first keeps the tail short; supporting files ride along
        rows.sort(key=lambda r: -(r["size"] or 0))
        try:
            await asyncio.gather(*(self.download_one(r, lanes, progress) for r in rows))
        finally:
            secs = max(time.monotonic() - progress["t0"], 1e-6)
            mb = self.counts["downloadedBytes"] / 1e6
            self.download_stats = {**lanes.stats(), "seconds": round(secs, 1), "mb": round(mb, 1),
                                   "mbPerS": round(mb / secs, 2), "filesPerS": round(progress["n"] / secs, 2)}
            log.info("download %s: %.1f MB in %.0fs = %.2f MB/s (%.1f files/s); concurrency %d -> %d (min %d, max %d,"
                     " peak in flight %d, per client %s)", self.site, mb, secs, mb / secs, progress["n"] / secs,
                     self.download_stats["concurrencyStart"], self.download_stats["concurrencyFinal"],
                     self.download_stats["concurrencyMin"], self.download_stats["concurrencyMax"],
                     self.download_stats["peakInFlight"], self.download_stats["peakPerClient"])

    # ------------------------------------------------------------ site lake
    def lake_file_rows(self, products: list[str] | None = None) -> list[dict[str, Any]]:
        from neon_mcp.neon.filenames import parse_neon_filename

        where, args = "f.site=?", [self.site]
        if products:
            where += f" AND f.product IN ({','.join('?' * len(products))})"
            args += products
        rows = self.con.execute(
            f"SELECT f.* FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND "
            f"s.month=f.month AND s.name=f.name WHERE {where} AND s.name IS NULL", args).fetchall()
        parsed = {r["name"]: parse_neon_filename(r["name"]) for r in rows}
        domains = [p.domain for p in parsed.values() if p.domain]
        default_domain = max(set(domains), key=domains.count) if domains else None
        out = []
        for r in rows:
            p = parsed[r["name"]]
            out.append({
                "productCode": r["product"], "siteCode": self.site, "domainCode": p.domain or default_domain,
                "month": r["month"], "release": r["release"], "isProvisional": r["release"] == "PROVISIONAL",
                "isReleaseOnly": bool(r["release_only"]), "name": r["name"], "size": r["size"], "md5": r["md5"],
                "crc32c": r["crc32c"], "kind": r["kind"], "tableName": p.table, "hor": p.hor, "ver": p.ver,
                "tmi": p.tmi, "publicationTimestamp": _pub_ts(p.timestamp), "ext": p.ext,
                "localRelPath": r["local_path"],
                "irodsPath": r["irods_path"] or self.paths.irods_raw(r["product"], r["month"], r["name"]),
                "dlMd5": r["dl_md5"], "md5Verified": None if r["md5_verified"] is None else bool(r["md5_verified"]),
            })
        return out

    def write_lake(self, step: str, products: list[str] | None) -> dict[str, Any]:
        files = self.lake_file_rows(products)
        tables: list[tuple[str, list[dict], dict[str, str], tuple[str, ...]]] = [
            ("files", files, site_lake.FILES_COLUMNS, site_lake.FILES_KEYS)]
        if step == "list":
            tables.append(("site_months", list(self.site_months.values()), site_lake.SITE_MONTHS_COLUMNS,
                           site_lake.SITE_MONTHS_KEYS))
            tables.append(("external_links", [r for r in self.external_rows if r.get("url")],
                           site_lake.EXTERNAL_LINKS_COLUMNS, site_lake.EXTERNAL_LINKS_KEYS))
        result: dict[str, Any] = {}
        with env.file_lock(f"site-lake-{self.site}", blocking=True):
            con = site_lake.open_site_lake(self.site)
            try:
                for name, rows, cols, _keys in tables:
                    if rows:
                        con.register(f"src_{name}", site_lake.arrow_table(rows, cols))
                sup = [dict(r) for r in self.con.execute(
                    "SELECT product AS productCode, month, name FROM fetch_superseded WHERE site=?", (self.site,))]
                if sup:
                    con.register("src_superseded", site_lake.arrow_table(
                        sup, {"productCode": "VARCHAR", "month": "VARCHAR", "name": "VARCHAR"}))
                extra = {"runId": self.run_id, "products": sorted(products or self.listed_products)}
                with site_lake.commit(con, f"site {step} {self.site}", extra):
                    for name, rows, cols, keys in tables:
                        site_lake.ensure_table(con, name, cols)
                        if rows:
                            result[name] = site_lake.upsert(con, name, f"src_{name}", keys, cols)
                        else:
                            result[name] = {"changed": 0, "source": 0}
                    if sup:  # superseded files leave the inventory (kept on disk, reported in fetch_superseded)
                        n = con.execute(
                            f"SELECT count(*) FROM {site_lake.ALIAS}.files f JOIN src_superseded s USING "
                            f"(productCode, month, name)").fetchone()[0]
                        if n:
                            con.execute(f"DELETE FROM {site_lake.ALIAS}.files f USING src_superseded s WHERE "
                                        f"f.productCode = s.productCode AND f.month = s.month AND f.name = s.name")
                        result["supersededRemoved"] = int(n)
                result["snapshots"] = site_lake.snapshot_count(con)
                site_lake.checkpoint_detach(con, self.site)
            finally:
                con.close()
        return result


def _pairs_for(site: str, products: list[str] | None, months: list[str] | None) -> tuple[list[dict], str]:
    pairs, source = planmod.site_pairs(site)
    idx = None
    for p in pairs:
        p["allMonths"] = list(p["months"])
        if p.get("external") and "monthReleases" not in p:
            idx = idx if idx is not None else planmod.products_index()
            mr: dict[str, str] = {}
            for sc in (idx.get(p["product"]) or {}).get("siteCodes") or []:
                if sc.get("siteCode") == site:
                    for ar in sc.get("availableReleases") or []:
                        for m in ar.get("availableMonths") or []:
                            mr[m] = ar.get("release")
            p["monthReleases"] = mr
    out = filter_pairs(pairs, products, months)
    kept = {p["product"] for p in out}
    for code in sorted(set(products or []) - kept):
        have = sorted({m for p in pairs if p["product"] == code for m in p["months"]})
        if have:
            log.warning("%s has no availability at %s in the requested months (it has %d month(s): %s..%s)",
                        code, site, len(have), have[0], have[-1])
        else:
            log.warning("%s is not a non-AOP product at %s", code, site)
    return out, source


async def _run(step: str, site: str, products: list[str] | None, months: list[str] | None, threads: int) -> int:
    con = state.connect(state.DB_PATH)
    rid = env.run_id()
    state.start_run(con, rid, f"site-{step}", site, [step, site, ",".join(products or []), ",".join(months or [])])
    pairs, source = _pairs_for(site, products, months)
    bundle = env.neon_bundle(("list", "download"))
    f = Fetcher(site=site, bundle=bundle, con=con, pairs=pairs, threads=threads, run_id=rid)
    t0 = time.monotonic()
    status = "failed"
    summary: dict[str, Any] = {"site": site, "step": step, "runId": rid, "source": source,
                               "products": sorted({p["product"] for p in pairs}), "months": months}
    try:
        with env.file_lock(f"site-fetch-{site}"):
            if step == "list":
                log.info("list %s: %d product window(s) from %s", site, len(pairs), source)
                summary["listed"] = await f.list_all()
            else:
                await f.download_all(products, months)
            lake_products = sorted({p["product"] for p in pairs}) if (products or months) else None
            summary["lake"] = f.write_lake(step, lake_products)
        status = "ok" if not f.errors else "partial"
    finally:
        summary.update({"status": status, "seconds": round(time.monotonic() - t0, 1), "counts": dict(f.counts),
                        "errors": f.errors[:200], "errorCount": len(f.errors)})
        if f.download_stats:
            summary["download"] = f.download_stats
        env.write_json(f.paths.reports_dir / f"{step}_summary.json", summary)
        state.finish_run(con, rid, f"site-{step}", site, status, {k: summary[k] for k in ("counts", "errorCount")})
        await f.aclose_extra()
        await bundle.aclose()
        con.close()
    c = f.counts
    if step == "list":
        log.info("list %s done in %.0fs: %d files %.2f GB, %d external link(s), %d error(s)", site,
                 summary["seconds"], c["files"], c["bytes"] / 1e9, c["externalLinks"], len(f.errors))
    else:
        log.info("download %s done in %.0fs: %d downloaded (%.2f GB, %s MB/s), %d skipped, %d evicted, %d failed,"
                 " %d soft-MD5 mismatch, %d relists", site, summary["seconds"], c["downloaded"],
                 c["downloadedBytes"] / 1e9, f.download_stats.get("mbPerS", 0.0), c["skipped"], c["evicted"],
                 c["failed"], c["softMismatch"], c["relists"])
    return 0 if status == "ok" else 2


def main(step: str, site: str, products: list[str] | None = None, months: list[str] | None = None,
         threads: int = 8) -> int:
    if step not in ("list", "download"):
        raise SystemExit(f"unknown fetch step {step}")
    return asyncio.run(_run(step, site.upper(), products, months, threads))


def local_file(rel: str) -> Path:
    """Absolute path of a journal ``local_path``."""
    return env.ROOT / rel
