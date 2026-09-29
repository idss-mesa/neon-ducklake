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
  * streamed through the download client's own opener (``NeonClient._open``: signed GCS URL, token=None,
    the host allow-list enforced on every redirect hop) so the response headers are readable;
  * up to 3 download clients (the bundle's 'download' client plus 2 built like it, sharing the
    bundle's rate limiter and cache), each with at most 8 streams (its httpx pool size);
  * the number of concurrent streams is adaptive (``tuning.AsyncAdaptiveLimit``, AIMD):
    - it starts at ``threads``, with a floor of ``min(4, threads)`` and a ceiling of 24;
    - each file reports its latency divided by ``5 s + bytes / 1.5 MB/s``;
    - it grows by 1 per 16 on-time files while binding;
    - it halves on transport errors, timeouts, 429 or 5xx, or when p95 exceeds 4x the target;
    - each new stream goes to the least-loaded client;
  * integrity (probe finding F1) — the body is written to ``<dest>.part`` while its MD5 is computed, and
    ``os.replace``d only after the check:
    - **transfer**: the MD5 of the received bytes must equal the storage provider's MD5 from the GET
      response ``x-goog-hash`` (``md5=<base64>``). The raw body is read (``aiter_raw``) so no transparent
      decoding changes what is hashed: a gzip-*stored* object served gzip is checked on its raw bytes and
      decompressed to the destination; a body gzip-encoded only in transit is checked after decoding;
      a stored-gzip object served decoded has no comparable hash. A mismatch is corruption: the
      ``.part`` is deleted and the file retried (3 retries), then it fails. It never triggers a re-list;
    - **NEON md5**: ``md5_verified`` = local MD5 == the MD5 NEON lists. When the storage MD5 matched but
      NEON's differs (NEON regenerated the object without refreshing its metadata), the file is kept with
      ``md5_verified=0`` and both MD5s recorded — for every kind, data files included;
    - no storage MD5 in the response: fall back to NEON's MD5 — hard for data/lab/h5 (mismatch = retry,
      one re-list, then fail), soft for the regenerated kinds (variables, sensor_positions, readme, eml,
      categorical_codes, validation, science_review_flags: kept with ``md5_verified=0``);
    - journal columns (added when missing): ``storage_md5``, ``neon_md5``, ``transfer_verified``,
      ``md5_verified``; site lake ``files``: ``storageMd5``, ``neonMd5``, ``transferVerified``,
      ``md5Verified``;
  * per-file retry x4: GCS 400/403/404/410 -> re-list; transport/5xx -> backoff; rate_limited -> sleep
    ``retryAfterSeconds``;
  * a re-list (expired/missing URL, GCS 4xx) reuses the product's recorded listing windows and release
    parameters (``listings`` parts; per pending month the narrowest part that covers it), never the
    product's full availability window (probe finding F2); months without a recorded part are listed as
    contiguous runs of exactly those months;
  * resume: ``dl_status='ok'`` rows whose file exists with ``size == dl_bytes`` are skipped; stale
    ``*.part`` files are removed first; URLs older than 6 days are refreshed by a re-list;
  * evicted files are never re-downloaded. These have ``site_files.local_evicted = 1``, a column added by
    the evict step that may be absent. Their raw file was deleted locally after it was verified in iRODS,
    so they count as ok. The one exception is a row a re-list marked ``stale`` (upstream MD5 changed):
    it is downloaded again and its ``local_evicted`` is reset to 0;
  * file kinds come from ``filenames.classify`` (lab files are kind ``lab``); journal rows an older
    listing left as ``other`` are re-classified before a download;
  * MB/s, concurrency and per-kind integrity counts are logged and written to
    ``reports/download_summary.json``.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import hashlib
import logging
import os
import random
import sqlite3
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .. import env, state
from ..filenames import REGENERATED_KINDS, classify
from ..tuning import AsyncAdaptiveLimit
from . import plan as planmod
from . import site_lake

log = logging.getLogger("neon_ducklake.site.fetch")

# kinds whose NEON-listed md5 is advisory (NEON regenerates them in place): soft when no storage md5 exists
SOFT_KINDS = REGENERATED_KINDS
LEGACY_SOFT_KINDS = frozenset({"eml", "readme"})  # the soft kinds of rows downloaded before neon_md5 existed
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
RELIST_COALESCE_S = 60.0
NEON_STALE_LOG_MAX = 25  # per run, the first N "NEON md5 stale" files are logged by name

# download-integrity columns of the journal (added when missing) and of the site lake ``files`` table
INTEGRITY_COLUMNS: dict[str, str] = {"storage_md5": "TEXT", "neon_md5": "TEXT", "transfer_verified": "INTEGER",
                                     "md5_verified": "INTEGER"}
LAKE_INTEGRITY_COLUMNS: dict[str, str] = {"storageMd5": "VARCHAR", "neonMd5": "VARCHAR", "transferVerified": "BOOLEAN"}
LAKE_FILES_COLUMNS: dict[str, str] = {**site_lake.FILES_COLUMNS, **LAKE_INTEGRITY_COLUMNS}

SUPERSEDED_SCHEMA = """
CREATE TABLE IF NOT EXISTS fetch_superseded (
  site TEXT NOT NULL, product TEXT NOT NULL, month TEXT NOT NULL, name TEXT NOT NULL,
  was_downloaded INTEGER, superseded_at TEXT, PRIMARY KEY (site, product, month, name)
);
"""


class ListingError(RuntimeError):
    """A /data/query window that failed after bisection and app-level retries."""


class TransferCorrupt(RuntimeError):
    """The received bytes do not hash to the storage provider's MD5 (or the body is truncated/undecodable)."""


class NeonChecksumMismatch(RuntimeError):
    """No storage MD5 to check against, and a hard kind's bytes differ from NEON's listed MD5."""


def ensure_integrity_columns(con: sqlite3.Connection) -> list[str]:
    """Add the download-integrity columns to ``site_files`` when missing (safe when two processes race)."""
    have = {r[1] for r in con.execute("PRAGMA table_info(site_files)")}
    added = []
    for col, typ in INTEGRITY_COLUMNS.items():
        if col in have:
            continue
        try:
            con.execute(f"ALTER TABLE site_files ADD COLUMN {col} {typ}")
            added.append(col)
        except sqlite3.OperationalError as exc:
            if "duplicate column" not in str(exc).lower():
                raise
    return added


def ensure_lake_integrity_columns(con: Any, alias: str = site_lake.ALIAS) -> None:
    """``files`` table with the integrity columns (ALTER TABLE ... ADD COLUMN IF NOT EXISTS; inside a commit)."""
    site_lake.ensure_table(con, "files", site_lake.FILES_COLUMNS, alias)
    for col, typ in LAKE_INTEGRITY_COLUMNS.items():
        con.execute(f"ALTER TABLE {alias}.files ADD COLUMN IF NOT EXISTS {site_lake.ident(col)} {typ}")


def goog_md5(value: str | None) -> str | None:
    """Hex MD5 from an ``x-goog-hash`` value (``crc32c=<b64>, md5=<b64>``); None when absent or malformed."""
    for part in (value or "").split(","):
        key, _, b64 = part.strip().partition("=")
        if key.strip().lower() != "md5" or not b64:
            continue
        try:
            raw = base64.b64decode(b64.strip(), validate=True)
        except (binascii.Error, ValueError):
            return None
        return raw.hex() if len(raw) == 16 else None
    return None


def _header(headers: Any, name: str) -> str:
    """One header's value; repeated headers (GCS sends ``x-goog-hash`` twice) joined with ', '."""
    get_list = getattr(headers, "get_list", None)
    if callable(get_list):
        return ", ".join(v for v in get_list(name) if v)
    for key, value in dict(headers or {}).items():
        if key.lower() == name.lower():
            return str(value)
    return ""


class _Gunzip:
    """Streaming gzip decoder that accepts concatenated members; ``finish`` fails on a truncated body."""

    def __init__(self) -> None:
        self._d = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def feed(self, data: bytes) -> bytes:
        out = []
        while data:
            if self._d.eof:
                self._d = zlib.decompressobj(16 + zlib.MAX_WBITS)
            out.append(self._d.decompress(data))
            data = self._d.unused_data if self._d.eof else b""
        return b"".join(out)

    def finish(self) -> bytes:
        tail = b"" if self._d.eof else self._d.flush()
        if not self._d.eof:
            raise TransferCorrupt("truncated gzip body")
        return tail


@dataclass
class Downloaded:
    """One verified download (the file is at ``path``)."""

    path: Path
    bytes: int  # bytes written to ``path``
    md5: str  # MD5 of the local file
    storage_md5: str | None  # the storage provider's MD5 (x-goog-hash), when the response carried one
    transfer_verified: bool | None  # True: received == stored bytes; None: no comparable storage MD5
    raw_bytes: int = 0  # body bytes on the wire
    content_encoding: str | None = None
    seconds: float = 0.0


def _download_timeout(client: Any) -> float | None:
    cfg = getattr(client, "config", None)
    value = getattr(cfg, "download_read_timeout_s", None)
    return float(value) if isinstance(value, int | float) else None


async def stream_verified(client: Any, url: str, dest: Path, *, max_bytes: int, neon_md5: str | None,
                          hard: bool) -> Downloaded:
    """GET ``url`` to ``dest`` via ``<dest>.part`` + ``os.replace``, verified as described in the module doc.

    Raises :class:`TransferCorrupt` (storage MD5 mismatch / bad body), :class:`NeonChecksumMismatch`
    (no storage MD5, ``hard`` and NEON's MD5 differs), ``ToolError('download_limit_exceeded')`` beyond
    ``max_bytes``, or the client's ``NeonApiError``/``ToolError``/httpx errors. The ``.part`` never survives
    a failure and ``dest`` is only replaced by verified bytes.
    """
    from neon_mcp.errors import ToolError

    opener = getattr(client, "_open", None)
    if opener is None:  # a client without the opener (older neon-mcp, simple fakes): no storage MD5
        return await _legacy_stream(client, url, dest, max_bytes=max_bytes, neon_md5=neon_md5, hard=hard)
    from neon_mcp.context import UpstreamStats

    t0 = time.monotonic()
    neon = (neon_md5 or "").strip().lower() or None
    response, _final = await opener(url, token=None, stats=UpstreamStats(),
                                    allowed_hosts=getattr(client, "allowed_hosts", None),
                                    timeout_s=_download_timeout(client))
    partial = dest.with_name(dest.name + ".part")
    try:
        headers = response.headers
        encoding = _header(headers, "content-encoding").strip().lower()
        stored_encoding = _header(headers, "x-goog-stored-content-encoding").strip().lower()
        storage = goog_md5(_header(headers, "x-goog-hash"))
        if encoding not in ("", "identity", "gzip", "x-gzip"):
            raise TransferCorrupt(f"unsupported content-encoding {encoding!r}")
        gz = encoding in ("gzip", "x-gzip")
        # x-goog-hash covers the STORED bytes: the raw body when stored gzip is served gzip, the decoded body
        # otherwise; a stored-gzip object served decoded (decompressive transcoding) has no comparable hash
        basis = None
        if storage:
            if stored_encoding == "gzip":
                basis = "raw" if gz else None
            else:
                basis = "decoded"
        raw_md5 = hashlib.md5(usedforsecurity=False)
        out_md5 = hashlib.md5(usedforsecurity=False)
        decoder = _Gunzip() if gz else None
        raw_n = out_n = 0
        dest.parent.mkdir(parents=True, exist_ok=True)
        with partial.open("wb") as fh:
            try:
                async for chunk in response.aiter_raw():
                    raw_n += len(chunk)
                    raw_md5.update(chunk)
                    data = decoder.feed(chunk) if decoder else chunk
                    out_n += len(data)
                    if raw_n > max_bytes or out_n > max_bytes:
                        raise ToolError("download_limit_exceeded",
                                        f"{dest.name} exceeds the per-file limit of {max_bytes} bytes.",
                                        details={"file": dest.name, "maxBytes": max_bytes})
                    if data:
                        out_md5.update(data)
                        fh.write(data)
                if decoder:
                    tail = decoder.finish()
                    out_n += len(tail)
                    out_md5.update(tail)
                    fh.write(tail)
            except zlib.error as exc:
                raise TransferCorrupt(f"undecodable gzip body: {exc}") from exc
        length = _header(headers, "content-length").strip()
        if length.isdigit() and int(length) != raw_n:
            raise TransferCorrupt(f"short body: {raw_n} of {length} bytes")
        local = out_md5.hexdigest()
        transfer: bool | None = None
        if basis:
            got = raw_md5.hexdigest() if basis == "raw" else local
            if got != storage:
                raise TransferCorrupt(f"md5 {got} != storage md5 {storage}")
            transfer = True
        elif hard and neon and local != neon:
            raise NeonChecksumMismatch(f"md5 {local} != NEON md5 {neon} (no storage md5 to check against)")
        os.replace(partial, dest)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    finally:
        await response.aclose()
    return Downloaded(path=dest, bytes=out_n, md5=local, storage_md5=storage, transfer_verified=transfer,
                      raw_bytes=raw_n, content_encoding=encoding or None, seconds=round(time.monotonic() - t0, 3))


async def _legacy_stream(client: Any, url: str, dest: Path, *, max_bytes: int, neon_md5: str | None,
                         hard: bool) -> Downloaded:
    """``stream_to_file`` (no response headers): NEON's MD5 is the only reference, hard kinds only."""
    from neon_mcp.errors import ToolError

    try:
        out = await client.stream_to_file(url, dest, token=None, max_bytes=max_bytes,
                                          expected_md5=neon_md5 if hard else None)
    except ToolError as exc:
        if exc.code == "checksum_mismatch":
            raise NeonChecksumMismatch("md5 != NEON md5 (no storage md5 to check against)") from exc
        raise
    return Downloaded(path=dest, bytes=out.bytes, md5=out.md5, storage_md5=None, transfer_verified=None,
                      raw_bytes=out.bytes, seconds=float(getattr(out, "seconds", 0.0) or 0.0))


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


def upstream_changed(listed_md5: str | None, kind: str | None, row: Any) -> bool:
    """Did NEON's content change since an ``ok`` download (so the file must be fetched again)?

    ``row`` has ``dl_md5`` and ``neon_md5`` (the md5 NEON listed when the file was downloaded). With a
    recorded ``neon_md5`` the file is stale only when NEON now lists a different md5 than then AND it is not
    the md5 of the bytes we hold: a stale NEON md5 whose storage bytes verified (F1) is not a change. Rows
    downloaded before ``neon_md5`` existed keep the old rule (hard kinds: listed md5 != local md5).
    """
    if not listed_md5 or not row["dl_md5"]:
        return False
    new = listed_md5.lower()
    if new == str(row["dl_md5"]).lower():
        return False
    ref = row["neon_md5"]
    if ref:
        return new != str(ref).lower()
    return (kind or "") not in LEGACY_SOFT_KINDS


def _month_index(month: str) -> int:
    y, m = month.split("-")[:2]
    return int(y) * 12 + int(m) - 1


def _part_rank(part: dict[str, Any]) -> tuple[int, float, str]:
    """Narrowest window first, then the most recently listed, then the part name (deterministic)."""
    listed = _ts(part.get("listed_at"))
    return (_month_index(part["end_month"]) - _month_index(part["start_month"]),
            -listed.timestamp() if listed else 0.0, str(part.get("part")))


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
    integrity: dict[str, dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    # product -> {(month, release_only, release)} still to download in this run (the months a re-list covers)
    pending_months: dict[str, set[tuple[str, bool, str | None]]] = field(default_factory=dict)
    _own_extra: list[Any] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.site = self.site.upper()
        self.paths = site_lake.site_paths(self.site)
        self.con.executescript(SUPERSEDED_SCHEMA)
        ensure_integrity_columns(self.con)

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
                    parsed = classify(name, product)
                    out.append(ListedFile(product=product, month=month, name=name, release=tag,
                                          release_only=release_only, kind=parsed.kind,
                                          size=int(f["size"]) if f.get("size") not in (None, "") else None,
                                          md5=(f.get("md5") or None), crc32c=(f.get("crc32c") or None),
                                          url=f.get("url")))
        return out

    def record_files(self, product: str, files: list[ListedFile], listed_months: set[str], listed_at: str) -> dict:
        """Upsert listed files into site_files; mark pending rows missing from this listing as superseded."""
        existing = {(r["month"], r["name"]): r for r in self.con.execute(
            "SELECT month, name, md5, kind, dl_status, dl_md5, neon_md5, md5_verified FROM site_files WHERE site=? "
            "AND product=?", (self.site, product))}
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
                if ex is not None and dl_status == "ok" and upstream_changed(f.md5, f.kind, ex):
                    dl_status = "stale"  # upstream content changed: re-download
                    stats["stale"] += 1
                # NEON lists the md5 of the bytes we hold: (re)confirm, and backfill neon_md5 of older rows
                confirmed = (ex is not None and dl_status == "ok" and f.md5 and ex["dl_md5"]
                             and f.md5.lower() == ex["dl_md5"].lower()
                             and (ex["md5_verified"] != 1 or not ex["neon_md5"]))
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
                if confirmed:
                    self.con.execute("UPDATE site_files SET md5_verified=1, neon_md5=? WHERE site=? AND product=? AND "
                                     "month=? AND name=?", (f.md5, self.site, product, f.month, f.name))
                    stats["confirmed"] = stats.get("confirmed", 0) + 1
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
        files, months, ok, listed_at = await self._list_windows(
            product, [(pair, start, end) for pair in pairs for start, end in query_windows(pair)])
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

    async def _list_windows(self, product: str, windows: list[tuple[dict, str, str]]
                            ) -> tuple[list[ListedFile], set[str], bool, str]:
        """Query every (pair, start, end) window; record each as a ``listings`` part.

        Returns (files, months covered, all windows ok, listed_at).
        """
        listed_at = env.utcnow().isoformat()
        files: list[ListedFile] = []
        months: set[str] = set()
        ok = True
        async with self.list_sem:
            for pair, start, end in windows:
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
        return files, months, ok, listed_at

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
        url, listed, _md5 = self._current(r)
        return url, listed

    def _current(self, r: sqlite3.Row) -> tuple[str | None, str | None, str | None]:
        """(signed url, listed_at, NEON md5) of a row as the latest listing left them."""
        row = self.con.execute("SELECT url, listed_at, md5 FROM site_files WHERE site=? AND product=? AND month=? "
                               "AND name=?", (self.site, r["product"], r["month"], r["name"])).fetchone()
        return (row["url"], row["listed_at"], row["md5"]) if row else (None, None, None)

    def _needed_months(self, product: str) -> set[tuple[str, bool, str | None]]:
        """(month, release_only, release) of the product's rows this run still has to download."""
        need = self.pending_months.get(product)
        if need is not None:
            return set(need)
        return {(r["month"], bool(r["release_only"]), r["release"]) for r in self.con.execute(
            "SELECT month, release_only, release FROM site_files WHERE site=? AND product=? AND "
            "coalesce(dl_status, '') NOT IN ('ok', 'superseded')", (self.site, product))}

    def relist_windows(self, product: str) -> list[tuple[dict, str, str]]:
        """The windows a re-list queries: the product's recorded listing parts, never its full availability.

        For every month still to download, the narrowest recorded ``listings`` part (same release parameter:
        current, or the release-only tag) that covers it is reused, with that part's release. Months no part
        covers are queried as contiguous runs of exactly those months. Current windows keep every month
        they return (as the original listing did); release-only windows keep the product's release-only
        months (the original listing filtered its response the same way).
        """
        need = self._needed_months(product)
        if not need:
            return []
        parts = [dict(r) for r in self.con.execute(
            "SELECT part, release, start_month, end_month, listed_at FROM listings WHERE site=? AND product=?",
            (self.site, product)) if r["start_month"] and r["end_month"] and not str(r["part"]).startswith("external__")]
        chosen: dict[tuple[str, str, str | None], set[str]] = defaultdict(set)
        loose: dict[str | None, set[str]] = defaultdict(set)
        for month, release_only, release in sorted(need, key=lambda x: (x[0], x[1], x[2] or "")):
            cands = [p for p in parts if p["start_month"] <= month <= p["end_month"]
                     and bool(p["release"]) == bool(release_only)]
            if release_only:
                cands = [p for p in cands if p["release"] == release] or cands
            if cands:
                best = min(cands, key=_part_rank)
                chosen[(best["start_month"], best["end_month"], best["release"])].add(month)
            else:
                loose[release if release_only else None].add(month)
        for rel, ms in loose.items():
            for start, end in month_runs(sorted(ms)):
                chosen[(start, end, rel)].update(m for m in ms if start <= m <= end)
        release_months: set[str] | None = None
        windows: list[tuple[dict, str, str]] = []
        for (start, end, rel), ms in sorted(chosen.items(), key=lambda kv: (kv[0][2] or "", kv[0][0], kv[0][1])):
            if rel:
                if release_months is None:
                    release_months = {r["month"] for r in self.con.execute(
                        "SELECT DISTINCT month FROM site_files WHERE site=? AND product=? AND release_only=1",
                        (self.site, product))}
                keep = sorted(ms | {m for m in release_months if start <= m <= end})
                pair = {"product": product, "site": self.site, "release": rel, "months": keep, "filtered": True,
                        "scope": "release-only", "start": start, "end": end}
            else:
                pair = {"product": product, "site": self.site, "release": None, "months": sorted(ms),
                        "filtered": False, "scope": "current", "start": start, "end": end}
            windows.append((pair, start, end))
        return windows

    async def relist(self, product: str, reason: str) -> bool:
        """Refresh a product's signed URLs over its recorded listing windows (see :meth:`relist_windows`).

        At most MAX_RELISTS_PER_PRODUCT per run; coalesced per minute (one re-list covers every month of
        the product still pending in this run).
        """
        async with self.relist_locks[product]:
            if time.monotonic() - self.relisted_at.get(product, -1e9) < RELIST_COALESCE_S:
                return True
            if self.relist_count[product] >= MAX_RELISTS_PER_PRODUCT:
                return False
            windows = self.relist_windows(product)
            if not windows:
                return False
            self.relist_count[product] += 1
            self.counts["relists"] += 1
            log.info("re-listing %s (%s): %s", product, reason,
                     ", ".join(f"{s}..{e}" + (f" {p['release']}" if p["release"] else "") for p, s, e in windows))
            files, months, ok, listed_at = await self._list_windows(product, windows)
            stats = self.record_files(product, files, months if ok else set(), listed_at)
            self.counts["relistFiles"] += stats["files"]
            if stats["new"]:
                log.info("re-list %s: %d new file(s) upstream (downloaded by the next run)", product, stats["new"])
            self.relisted_at[product] = time.monotonic()
            return ok

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
        kind = r["kind"] or classify(r["name"], r["product"]).kind
        regenerated = kind in REGENERATED_KINDS
        limit = max_bytes_for(r["size"])
        last = "no_attempt"
        attempts = waits = 0
        outcome: Downloaded | None = None
        neon: str | None = r["md5"] or None
        while attempts < DL_TRIES:
            url, _listed, neon = self._current(r)
            if not url:
                if not await self.relist(r["product"], "no url"):
                    last = "no_url"
                    break
                url, _listed, neon = self._current(r)
                if not url:
                    last = "not_in_listing"
                    break
            attempts += 1
            try:
                async with lanes.slot() as client:
                    t0 = time.monotonic()
                    outcome = await stream_verified(client, url, dest, max_bytes=limit, neon_md5=neon,
                                                    hard=not regenerated)
                lanes.limit.on_success((time.monotonic() - t0) / dl_target_seconds(outcome.bytes))
                break
            except TransferCorrupt as exc:  # bytes differ from what storage holds: retry, never re-list
                last = "transfer_corrupt"
                self.counts["corruptRetries"] += 1
                log.warning("transfer corrupt %s/%s/%s (attempt %d): %s", r["product"], r["month"], r["name"],
                            attempts, str(exc)[:200])
                await self.sleep(RETRY_DELAYS[min(attempts - 1, 2)] + random.uniform(0, 1))
            except NeonChecksumMismatch:  # no storage md5 and a hard kind: NEON's md5 is the only reference
                last = "checksum_mismatch"
                self.counts["checksumRetries"] += 1
                if attempts == 2:
                    await self.relist(r["product"], "checksum mismatch without a storage md5")
                await self.sleep(RETRY_DELAYS[min(attempts - 1, 2)] + random.uniform(0, 1))
            except ToolError as exc:
                last = f"tool:{exc.code}"
                if exc.code == "rate_limited" and waits < 10:
                    attempts -= 1  # waiting for the limiter is not a failed attempt
                    waits += 1
                    await self.sleep(min(float((exc.details or {}).get("retryAfterSeconds", 30)) + 1, 300))
                elif exc.code == "download_limit_exceeded" and (regenerated or not r["size"]):
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
            self.integrity[kind]["failed"] += 1
            self.errors.append({"product": r["product"], "month": r["month"], "name": r["name"], "error": last})
            log.warning("download failed %s/%s/%s: %s", r["product"], r["month"], r["name"], last)
        else:
            self._record_download(r, kind, key, outcome, neon, attempts, progress)
        progress["n"] += 1
        if progress["n"] % PROGRESS_EVERY == 0 or progress["n"] == progress["total"]:
            el = max(time.monotonic() - progress["t0"], 1e-6)
            mb = self.counts["downloadedBytes"] / 1e6
            log.info("download %d/%d files, %.1f MB, %.2f MB/s, concurrency %d (in flight %d over %d client(s)),"
                     " failed %d", progress["n"], progress["total"], mb, mb / el, lanes.limit.limit,
                     lanes.limit.in_flight, len(lanes.clients), self.counts["failed"])

    def _record_download(self, r: sqlite3.Row, kind: str, key: tuple[str, str, str, str], outcome: Downloaded,
                         neon: str | None, attempts: int, progress: dict[str, Any]) -> None:
        """Journal one finished download with its integrity verdicts and count them."""
        c, k = self.counts, self.integrity[kind]
        neon_md5 = (neon or "").strip().lower() or None
        md5_ok = neon_md5 is not None and outcome.md5.lower() == neon_md5
        transfer = outcome.transfer_verified
        k["downloaded"] += 1
        if transfer:
            c["transferVerified"] += 1
            k["transferVerified"] += 1
        else:
            c["noStorageMd5"] += 1
            k["noStorageMd5"] += 1
        if neon_md5 is None:
            c["noNeonMd5"] += 1
        elif not md5_ok:
            k["neonMismatch"] += 1
            if kind in REGENERATED_KINDS:
                c["softMismatch"] += 1
            if transfer:  # storage bytes verified, NEON's metadata is stale: keep and flag
                c["neonStale"] += 1
                if c["neonStale"] <= NEON_STALE_LOG_MAX:
                    log.info("NEON md5 stale (storage-verified, kept): %s/%s/%s kind=%s neon=%s storage=%s",
                             r["product"], r["month"], r["name"], kind, neon_md5, outcome.storage_md5)
        if not md5_ok and not transfer:
            c["unverified"] += 1
        evicted = ", local_evicted=0" if progress.get("evictedColumn") else ""
        self.con.execute(
            "UPDATE site_files SET dl_md5=?, dl_bytes=?, md5_verified=?, storage_md5=?, neon_md5=?, transfer_verified=?,"
            " downloaded_at=?, dl_status='ok', dl_error=NULL, url=NULL, dl_attempts=coalesce(dl_attempts,0)+?"
            f"{evicted} WHERE site=? AND product=? AND month=? AND name=?",
            (outcome.md5, outcome.bytes, int(md5_ok), outcome.storage_md5, neon_md5,
             None if transfer is None else int(transfer), env.utcnow().isoformat(), attempts, *key))
        c["downloaded"] += 1
        c["downloadedBytes"] += outcome.bytes

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

    def heal_kinds(self) -> int:
        """Re-classify journal rows an older listing left as ``other`` (lab files -> ``lab``)."""
        n = 0
        for r in self.con.execute("SELECT product, month, name FROM site_files WHERE site=? AND "
                                  "coalesce(kind, 'other') = 'other'", (self.site,)).fetchall():
            kind = classify(r["name"], r["product"]).kind
            if kind != "other":
                self.con.execute("UPDATE site_files SET kind=? WHERE site=? AND product=? AND month=? AND name=?",
                                 (kind, self.site, r["product"], r["month"], r["name"]))
                n += 1
        if n:
            log.info("download %s: %d journal row(s) re-classified (lab files)", self.site, n)
        return n

    async def download_all(self, products: list[str] | None, months: list[str] | None) -> None:
        self.counts["partsRemoved"] = self.clean_parts()
        self.counts["kindsHealed"] = self.heal_kinds()
        rows = self.pending_rows(products, months)
        self.pending_months = defaultdict(set)
        for r in rows:
            self.pending_months[r["product"]].add((r["month"], bool(r["release_only"]), r["release"]))
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
        where, args = "f.site=?", [self.site]
        if products:
            where += f" AND f.product IN ({','.join('?' * len(products))})"
            args += products
        rows = self.con.execute(
            f"SELECT f.* FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND "
            f"s.month=f.month AND s.name=f.name WHERE {where} AND s.name IS NULL", args).fetchall()
        parsed = {r["name"]: classify(r["name"], r["product"]) for r in rows}
        domains = [p.domain for p in parsed.values() if p.domain]
        default_domain = max(set(domains), key=domains.count) if domains else None
        out = []
        for r in rows:
            p = parsed[r["name"]]
            out.append({
                "productCode": r["product"], "siteCode": self.site, "domainCode": p.domain or default_domain,
                "month": r["month"], "release": r["release"], "isProvisional": r["release"] == "PROVISIONAL",
                "isReleaseOnly": bool(r["release_only"]), "name": r["name"], "size": r["size"], "md5": r["md5"],
                "crc32c": r["crc32c"], "kind": r["kind"] if r["kind"] not in (None, "other") else p.kind,
                "tableName": p.table, "hor": p.hor, "ver": p.ver,
                "tmi": p.tmi, "publicationTimestamp": _pub_ts(p.timestamp), "ext": p.ext,
                "localRelPath": r["local_path"],
                "irodsPath": r["irods_path"] or self.paths.irods_raw(r["product"], r["month"], r["name"]),
                "dlMd5": r["dl_md5"], "md5Verified": None if r["md5_verified"] is None else bool(r["md5_verified"]),
                "storageMd5": r["storage_md5"], "neonMd5": r["neon_md5"],
                "transferVerified": None if r["transfer_verified"] is None else bool(r["transfer_verified"]),
            })
        return out

    def write_lake(self, step: str, products: list[str] | None) -> dict[str, Any]:
        files = self.lake_file_rows(products)
        tables: list[tuple[str, list[dict], dict[str, str], tuple[str, ...]]] = [
            ("files", files, LAKE_FILES_COLUMNS, site_lake.FILES_KEYS)]
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
                journal = [dict(r) for r in self.con.execute(
                    "SELECT product AS productCode, month, name FROM site_files WHERE site=?", (self.site,))]
                con.register("src_journal_keys", site_lake.arrow_table(
                    journal, {"productCode": "VARCHAR", "month": "VARCHAR", "name": "VARCHAR"}))
                extra = {"runId": self.run_id, "products": sorted(products or self.listed_products)}
                with site_lake.commit(con, f"site {step} {self.site}", extra):
                    ensure_lake_integrity_columns(con)  # files gains storageMd5/neonMd5/transferVerified
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
                    # the journal is the inventory's source of truth: rows removed from it (e.g. the stray
                    # full-window rows of the F2 re-list bug) leave the lake too
                    gone = ("NOT EXISTS (SELECT 1 FROM src_journal_keys j WHERE j.productCode = f.productCode AND "
                            "j.month = f.month AND j.name = f.name)")
                    n = con.execute(f"SELECT count(*) FROM {site_lake.ALIAS}.files f WHERE {gone}").fetchone()[0]
                    if n:
                        con.execute(f"DELETE FROM {site_lake.ALIAS}.files f WHERE {gone}")
                        log.info("site lake %s: %d file row(s) no longer in the journal removed", self.site, n)
                    result["notInJournalRemoved"] = int(n)
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


async def _run(step: str, site: str, products: list[str] | None, months: list[str] | None, threads: int,
               lock_blocking: bool = False) -> int:
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
        with env.file_lock(f"site-fetch-{site}", blocking=lock_blocking):
            # both steps upsert into the site lake, which reads its existing Parquet: a stream site cleaned up
            # earlier (or a lake another VM pushed) comes back from the Data Store first, data included
            from .stream import ensure_lake_data

            why = ensure_lake_data(site, con=con)
            if why:
                summary["error"] = why
                log.error("%s %s: %s; nothing listed or downloaded", step, site, why)
            else:
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
        if f.integrity:
            summary["integrity"] = {k: dict(v) for k, v in sorted(f.integrity.items())}
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
        log.info("download %s done in %.0fs: %d downloaded (%.2f GB, %s MB/s), %d skipped, %d evicted, %d failed;"
                 " integrity: %d transfer-verified (storage md5), %d NEON md5 stale (kept, md5Verified=false),"
                 " %d without storage md5, %d corrupt-transfer retries; %d relists", site, summary["seconds"],
                 c["downloaded"], c["downloadedBytes"] / 1e9, f.download_stats.get("mbPerS", 0.0), c["skipped"],
                 c["evicted"], c["failed"], c["transferVerified"], c["neonStale"], c["noStorageMd5"],
                 c["corruptRetries"], c["relists"])
    return 0 if status == "ok" else 2


def main(step: str, site: str, products: list[str] | None = None, months: list[str] | None = None,
         threads: int = 8, *, lock_blocking: bool = False) -> int:
    """``site list|download``; ``lock_blocking`` waits for another fetch of the site instead of failing."""
    if step not in ("list", "download"):
        raise SystemExit(f"unknown fetch step {step}")
    return asyncio.run(_run(step, site.upper(), products, months, threads, lock_blocking))


def local_file(rel: str) -> Path:
    """Absolute path of a journal ``local_path``."""
    return env.ROOT / rel


# ---------------------------------------------------------------- streaming: one chunk's rows under the working-set cap

WORKSET_BYTES_ENV = "NEON_DUCKLAKE_WORKSET_BYTES"
DEFAULT_WORKSET_BYTES = 50 * 10**9
WORKSET_TIMEOUT_ENV = "NEON_DUCKLAKE_WORKSET_TIMEOUT_S"
DEFAULT_WORKSET_TIMEOUT_S = 4 * 3600.0
WORKSET_POLL_ENV = "NEON_DUCKLAKE_WORKSET_POLL_S"
DEFAULT_WORKSET_POLL_S = 30.0
WORKSET_SCHEMA = """
CREATE TABLE IF NOT EXISTS fetch_workset (
  key TEXT PRIMARY KEY, site TEXT NOT NULL, bytes INTEGER NOT NULL, pid INTEGER NOT NULL, host TEXT, created_at TEXT
);
"""
RC_WORKSET_TIMEOUT = 3


def workset_bytes(environ: dict[str, str] | None = None) -> int:
    """``$NEON_DUCKLAKE_WORKSET_BYTES`` (read at call time; ``50G``/``50GB``/bytes), default 50 GB."""
    from .chunks import env_bytes

    return env_bytes(WORKSET_BYTES_ENV, DEFAULT_WORKSET_BYTES, environ)


def _env_seconds(var: str, default: float) -> float:
    try:
        v = float(os.environ.get(var) or default)
    except ValueError:
        return default
    return v if v > 0 else default


def _evict_expr(con: sqlite3.Connection) -> str:
    have = {r[1] for r in con.execute("PRAGMA table_info(site_files)")}
    return "coalesce(local_evicted, 0) = 0" if "local_evicted" in have else "1"


def local_raw_bytes(con: sqlite3.Connection, *, exclude: tuple[str, str, list[str]] | None = None,
                    verify: bool = False) -> int:
    """Raw bytes on local disk according to the journal (every site: ``ok`` and not evicted).

    ``exclude`` = (site, product, months) leaves one chunk's own files out. ``verify`` stats each file, so
    rows whose file is already gone (deleted by hand, a crashed eviction) do not count.
    """
    where = f"dl_status='ok' AND {_evict_expr(con)}"
    args: list[Any] = []
    if exclude:
        site, product, months = exclude
        where += (f" AND NOT (site=? AND product=? AND month IN ({','.join('?' * len(months))}))" if months
                  else " AND NOT (site=? AND product=?)")
        args += [site, product, *months]
    if not verify:
        return int(con.execute(f"SELECT coalesce(sum(coalesce(dl_bytes, size, 0)), 0) FROM site_files WHERE {where}",
                               args).fetchone()[0])
    total = 0
    for r in con.execute(f"SELECT local_path, coalesce(dl_bytes, size, 0) AS b FROM site_files WHERE {where}", args):
        with contextlib.suppress(OSError):
            if r["local_path"] and (env.ROOT / r["local_path"]).is_file():
                total += int(r["b"])
    return total


def local_workset_bytes(site: str | None = None, *, con: sqlite3.Connection | None = None,
                        verify: bool = True) -> int:
    """The local working set: raw files on disk (``ok``, not evicted: superseded files included, they stay on
    disk until ``stream.delete_chunk``/``cleanup_site`` remove them) + the site lakes' ``data/**`` bytes.

    ``site`` limits both parts to one site. ``verify`` (default) stats each raw file, so journal rows whose file
    is already gone do not count.
    """
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        where = f"dl_status='ok' AND {_evict_expr(con)}"
        args: list[Any] = []
        if site:
            where += " AND site=?"
            args.append(site.upper())
        raw = 0
        if verify:
            for r in con.execute(f"SELECT local_path, coalesce(dl_bytes, size, 0) AS b FROM site_files WHERE {where}",
                                 args):
                with contextlib.suppress(OSError):
                    if r["local_path"] and (env.ROOT / r["local_path"]).is_file():
                        raw += int(r["b"])
        else:
            raw = int(con.execute(f"SELECT coalesce(sum(coalesce(dl_bytes, size, 0)), 0) FROM site_files "
                                  f"WHERE {where}", args).fetchone()[0])
    finally:
        if own:
            con.close()
    return raw + site_lake.data_bytes(site.upper() if site else None)


def chunk_bytes_needed(con: sqlite3.Connection, site: str, product: str, months: list[str]) -> tuple[int, int]:
    """(bytes the download will add, bytes of the chunk already on disk) for one product's months."""
    con.executescript(SUPERSEDED_SCHEMA)
    ev = _evict_expr(con).replace("local_evicted", "f.local_evicted")
    rows = con.execute(
        f"SELECT f.dl_status, f.local_path, f.size, f.dl_bytes, CASE WHEN {ev} THEN 0 ELSE 1 END AS evicted"
        " FROM site_files f LEFT JOIN fetch_superseded s ON s.site=f.site AND s.product=f.product AND"
        " s.month=f.month AND s.name=f.name WHERE f.site=? AND f.product=? AND s.name IS NULL AND"
        f" coalesce(f.dl_status, '') != 'superseded' AND f.month IN ({','.join('?' * len(months))})",
        (site, product, *months)).fetchall() if months else []
    need = have = 0
    for r in rows:
        if r["evicted"] and r["dl_status"] != "stale":
            continue  # in iRODS only, never downloaded again
        if r["dl_status"] == "ok" and r["local_path"] and (env.ROOT / r["local_path"]).is_file():
            have += int(r["dl_bytes"] or r["size"] or 0)
        else:
            need += int(r["size"] or r["dl_bytes"] or 0)
    return need, have


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _reserved(con: sqlite3.Connection, key: str) -> int:
    """Bytes reserved by other live downloads of this host (stale reservations are dropped)."""
    host = os.uname().nodename
    total = 0
    for r in con.execute("SELECT key, bytes, pid, host FROM fetch_workset").fetchall():
        if r["key"] == key:
            continue
        if r["host"] == host and not _pid_alive(int(r["pid"])):
            con.execute("DELETE FROM fetch_workset WHERE key=?", (r["key"],))
            continue
        total += int(r["bytes"])
    return total


def wait_for_workset(site: str, product: str, months: list[str], *, cap: int | None = None,
                     timeout_s: float | None = None, poll_s: float | None = None, sleep: Any = time.sleep,
                     clock: Any = time.monotonic, con: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Wait until local raw bytes + site-lake data + other reservations + this chunk's download fit ``cap``;
    then reserve.

    The site lakes' ``data/**`` count toward the cap (the stacked Parquet is local until ``cleanup_site``), but
    a chunk may always start when no other raw file is local and nothing is reserved (a chunk larger than the
    cap, or beside a lake larger than the cap, runs alone). Returns ``{'ok', 'key', 'need', 'local', 'lake',
    'reserved', 'cap', 'waited_s'}``; ``ok`` False after ``timeout_s``. The reservation (``fetch_workset``) is
    released by :func:`release_workset`.
    """
    site = site.upper()
    cap = int(cap) if cap else workset_bytes()
    timeout_s = timeout_s if timeout_s is not None else _env_seconds(WORKSET_TIMEOUT_ENV, DEFAULT_WORKSET_TIMEOUT_S)
    poll_s = poll_s if poll_s is not None else _env_seconds(WORKSET_POLL_ENV, DEFAULT_WORKSET_POLL_S)
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    key = f"{site}|{product}|{','.join(sorted(months))}"
    t0 = clock()
    logged = False
    try:
        con.executescript(WORKSET_SCHEMA)
        while True:
            need, _have = chunk_bytes_needed(con, site, product, months)
            lake = site_lake.data_bytes()
            con.execute("BEGIN IMMEDIATE")
            try:
                other = local_raw_bytes(con, exclude=(site, product, months))
                if other + lake + need > cap and other:
                    other = local_raw_bytes(con, exclude=(site, product, months), verify=True)
                reserved = _reserved(con, key)
                fits = need == 0 or other + lake + reserved + need <= cap or (other == 0 and reserved == 0)
                if fits:
                    if need:
                        con.execute("INSERT OR REPLACE INTO fetch_workset(key, site, bytes, pid, host, created_at)"
                                    " VALUES (?,?,?,?,?,?)", (key, site, need, os.getpid(), os.uname().nodename,
                                                              env.utcnow().isoformat()))
                    con.execute("COMMIT")
                    waited = round(clock() - t0, 1)
                    if logged:
                        log.info("workset: %s %s may download %.2f GB now (local %.2f GB, reserved %.2f GB, cap "
                                 "%.2f GB) after %.0fs", site, product, need / 1e9, other / 1e9, reserved / 1e9,
                                 cap / 1e9, waited)
                    return {"ok": True, "key": key, "need": need, "local": other, "lake": lake,
                            "reserved": reserved, "cap": cap, "waited_s": waited}
                con.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    con.execute("ROLLBACK")
                raise
            waited = clock() - t0
            if waited >= timeout_s:
                log.error("workset: %s %s needs %.2f GB but local raw %.2f GB + lake data %.2f GB + reserved "
                          "%.2f GB exceed the cap %.2f GB after %.0fs", site, product, need / 1e9, other / 1e9,
                          lake / 1e9, reserved / 1e9, cap / 1e9, waited)
                return {"ok": False, "key": key, "need": need, "local": other, "lake": lake, "reserved": reserved,
                        "cap": cap, "waited_s": round(waited, 1)}
            if not logged:
                log.info("workset: %s %s waits: needs %.2f GB, local raw %.2f GB + lake data %.2f GB + reserved "
                         "%.2f GB, cap %.2f GB", site, product, need / 1e9, other / 1e9, lake / 1e9, reserved / 1e9,
                         cap / 1e9)
                logged = True
            sleep(min(poll_s, max(timeout_s - waited, 0.01)))
    finally:
        if own:
            con.close()


def release_workset(key: str, *, con: sqlite3.Connection | None = None) -> None:
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        con.executescript(WORKSET_SCHEMA)
        con.execute("DELETE FROM fetch_workset WHERE key=?", (key,))
    finally:
        if own:
            con.close()


def download_rows(site: str, product: str, months: list[str], *, threads: int = 8, cap: int | None = None,
                  timeout_s: float | None = None, poll_s: float | None = None, sleep: Any = time.sleep,
                  clock: Any = time.monotonic) -> int:
    """Download only one product's rows of ``months`` (a stream chunk) under the working-set cap.

    0 = every row downloaded, 2 = some rows failed (``site download`` semantics), 3 = the working set did
    not free up within the timeout (nothing downloaded).
    """
    site = site.upper()
    months = sorted(set(months))
    w = wait_for_workset(site, product, months, cap=cap, timeout_s=timeout_s, poll_s=poll_s, sleep=sleep,
                         clock=clock)
    if not w["ok"]:
        return RC_WORKSET_TIMEOUT
    try:  # a concurrent chunk download of the same site finishes first (the site-fetch lock is waited for)
        return main("download", site, products=[product], months=months, threads=threads, lock_blocking=True)
    finally:
        release_workset(w["key"])
