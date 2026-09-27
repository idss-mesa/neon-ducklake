"""Phase 1 — harvest the whole NEON Data API (metadata + non-AOP basic file inventory).

Everything goes through neon-mcp's own NeonClient in-process (same rate limiter, token handling and
retry policy as the MCP server) — not through MCP tools, whose 50 KB result budget and 200-row pages
make a 3.5M-row inventory impractical.

Landing: api/runs/<runId>/raw/<family>/<key>.json.gz, written atomically (.part -> os.replace); one
ledger row per request in work/state.sqlite. Signed URL query strings are stripped before landing.
Resumable: a key whose landing exists and gunzips is skipped.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import re
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import state
from .env import API_DIR, RUNS_DIR, NeonBundle, neon_bundle, utcnow, write_json
from .env import run_id as new_run_id

log = logging.getLogger("neon_ducklake.harvest")

TAXON_TYPES = ["ALGAE", "BEETLE", "BIRD", "FISH", "HERPETOLOGY", "MACROINVERTEBRATE", "MOSQUITO",
               "MOSQUITO_PATHOGENS", "PLANT", "SMALL_MAMMAL", "TICK"]
DOMAINS = [f"D{i:02d}" for i in range(1, 21)]
AOP_CODE_RE = re.compile(r"^DP[1-4]\.30\d{3}\.\d{3}$")
HISTORY_TYPES = {"CONFIG", "TOWER", "SITE"}
LOC_BATCH = 500
CONCURRENCY = 8
INTROSPECTION_Q = """query IntrospectionQuery { __schema {
  queryType { name } mutationType { name } subscriptionType { name }
  types { kind name description
    fields(includeDeprecated: true) { name description args { name description type { ...TypeRef } defaultValue }
      type { ...TypeRef } isDeprecated deprecationReason }
    inputFields { name description type { ...TypeRef } defaultValue }
    interfaces { ...TypeRef }
    enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
    possibleTypes { ...TypeRef } } } }
fragment TypeRef on __Type { kind name ofType { kind name ofType { kind name ofType { kind name
  ofType { kind name ofType { kind name ofType { kind name } } } } } } }"""
LOC_Q = """query Q($query: LocationQuery!) { findLocations(query: $query) {
  locationName locationType locationDescription siteCode domainCode
  locationDecimalLatitude locationDecimalLongitude locationElevation
  locationUtmEasting locationUtmNorthing locationUtmZone locationUtmHemisphere
  alphaOrientation betaOrientation gammaOrientation xOffset yOffset zOffset
  offsetLocation { locationName }
  locationParent activePeriods { activatedDate deactivatedDate }
  locationProperties { locationPropertyName locationPropertyValue }
  locationPolygon { coordinates { latitude longitude elevation } } } }"""


def is_aop(code: str, team: str | None = None) -> bool:
    return (team or "").upper() == "AOP" or bool(AOP_CODE_RE.match(code))


def strip_query(url: Any) -> Any:
    if isinstance(url, str) and "?" in url:
        return url.split("?", 1)[0]
    return url


def scrub_urls(obj: Any) -> Any:
    """Remove signed-URL query strings anywhere in a payload (keys 'url'/'*Url')."""
    if isinstance(obj, dict):
        return {k: (strip_query(v) if isinstance(v, str) and (k == "url" or k.endswith("Url")) and "X-Goog" in v
                    else scrub_urls(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [scrub_urls(v) for v in obj]
    return obj


def month_range(first: str, last: str) -> list[str]:
    y, m = map(int, first.split("-"))
    ly, lm = map(int, last.split("-"))
    out = []
    while (y, m) <= (ly, lm):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def split_window(start: str, end: str) -> list[tuple[str, str]]:
    months = month_range(start, end)
    if len(months) <= 1:
        return []
    years = sorted({mm[:4] for mm in months})
    if len(years) > 1:  # split on a year boundary near the middle
        mid_year = years[len(years) // 2]
        left = [mm for mm in months if mm[:4] < mid_year]
        right = [mm for mm in months if mm[:4] >= mid_year]
    else:
        half = len(months) // 2
        left, right = months[:half], months[half:]
    return [(left[0], left[-1]), (right[0], right[-1])]


@dataclass
class Harvest:
    run_id: str
    bundle: NeonBundle
    con: Any
    pilot: bool = False
    raw_dir: Path = field(init=False)
    counts: dict[str, dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    sem: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(CONCURRENCY))
    t0: float = field(default_factory=time.time)
    consecutive_failures: int = 0

    def __post_init__(self) -> None:
        self.raw_dir = RUNS_DIR / self.run_id / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ landing
    def path_for(self, family: str, key: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9._=+-]", "_", key)
        return self.raw_dir / family / f"{safe}.json.gz"

    def landed(self, family: str, key: str) -> Any | None:
        p = self.path_for(family, key)
        if not p.exists():
            return None
        try:
            return json.loads(gzip.decompress(p.read_bytes()))
        except (OSError, ValueError, EOFError):
            p.unlink(missing_ok=True)
            return None

    def land(self, family: str, key: str, payload: Any, *, method: str, path: str, params: Any,
             status: int, attempts: int, elapsed_ms: float, response_bytes: int | None) -> Path:
        p = self.path_for(family, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(scrub_urls(payload), separators=(",", ":"), ensure_ascii=False).encode()
        blob = gzip.compress(data, compresslevel=6)
        tmp = p.with_name(p.name + ".part")
        tmp.write_bytes(blob)
        os.replace(tmp, p)
        self.con.execute(
            "INSERT OR REPLACE INTO requests(run_id,family,key,method,path,params,http_status,attempts,elapsed_ms,"
            "response_bytes,landed_path,landed_bytes,sha256,status,error,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (self.run_id, family, key, method, path, json.dumps(params, default=str), status, attempts, elapsed_ms,
             response_bytes, str(p.relative_to(RUNS_DIR / self.run_id)), len(blob), hashlib.sha256(blob).hexdigest(),
             "ok", None, utcnow().isoformat()))
        self.counts[family]["ok"] += 1
        return p

    def fail(self, family: str, key: str, *, method: str, path: str, params: Any, error: str,
             status: int | None = None, final: bool = True) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO requests(run_id,family,key,method,path,params,http_status,status,error,fetched_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (self.run_id, family, key, method, path, json.dumps(params, default=str), status,
             "error" if final else "split", error[:500], utcnow().isoformat()))
        if final:
            self.counts[family]["error"] += 1

    # ------------------------------------------------------------ requests
    async def call(self, fn: Callable[[], Awaitable[Any]], *, family: str, key: str, tries: int = 5) -> Any:
        """App-level retry around a client call (transport/5xx/429/rate_limited), with a circuit breaker."""
        from neon_mcp.errors import NeonApiError, ToolError

        delays = [5, 15, 45, 120, 300]
        last: Exception | None = None
        for attempt in range(tries):
            try:
                async with self.sem:
                    out = await fn()
                self.consecutive_failures = 0
                return out, attempt + 1
            except ToolError as exc:
                if exc.code != "rate_limited":
                    raise
                last = exc
                wait = float((exc.details or {}).get("retryAfterSeconds", 30))
                await asyncio.sleep(min(wait + 1, 300))
            except NeonApiError as exc:
                last = exc
                retryable = exc.kind == "transport" or exc.status in (429, 500, 502, 503, 504)
                if not retryable:
                    raise
                self.consecutive_failures += 1
                if self.consecutive_failures >= 20:
                    log.warning("circuit breaker: 20 consecutive failures; pausing 5 min")
                    await asyncio.sleep(300)
                    self.consecutive_failures = 0
                await asyncio.sleep(delays[min(attempt, len(delays) - 1)])
        assert last is not None
        raise last

    async def get(self, family: str, key: str, path: str, params: dict[str, Any] | None = None, *,
                  role: str = "harvest", timeout_s: float = 180, token: bool = True) -> Any:
        cached = self.landed(family, key)
        if cached is not None:
            self.counts[family]["cached"] += 1
            return cached
        client = self.bundle.client(role)
        tok = self.bundle.token if token else None
        t = time.time()
        try:
            env, attempts = await self.call(
                lambda: client.get_json(path, params, token=tok, timeout_s=timeout_s), family=family, key=key)
        except Exception as exc:
            self.fail(family, key, method="GET", path=path, params=params, error=f"{type(exc).__name__}: {exc}",
                      status=getattr(exc, "status", None))
            raise
        self.land(family, key, env.data, method="GET", path=path, params=params, status=env.status,
                  attempts=attempts, elapsed_ms=(time.time() - t) * 1000, response_bytes=int(env.headers.get("content-length", 0) or 0) or None)
        return scrub_urls(env.data)

    # ------------------------------------------------------------ families
    async def releases(self) -> list[dict]:
        data = await self.get("releases", "releases", "/releases")
        items = data if isinstance(data, list) else data.get("releases", data)
        client = self.bundle.client("meta")
        raw_rel = None
        for rel in items:
            tag = rel["release"]
            for art in rel.get("artifacts") or []:
                if art.get("type") != "MANIFEST_AVAILABLE":
                    continue
                key = f"{tag}"
                if self.landed("manifests", key) is not None:
                    self.counts["manifests"]["cached"] += 1
                    continue
                if raw_rel is None:  # need signed URLs: re-fetch /releases unscrubbed, not landed
                    env = await client.get_json("/releases", token=self.bundle.token, timeout_s=120)
                    raw_rel = {r["release"]: r for r in (env.data if isinstance(env.data, list) else env.data["releases"])}
                url = next(a["url"] for a in raw_rel[tag]["artifacts"] if a["name"] == art["name"])
                t = time.time()
                blob, attempts = await self.call(
                    lambda url=url: client.fetch_bytes(url, token=None, max_bytes=128 << 20), family="manifests", key=key)
                md5 = hashlib.md5(blob, usedforsecurity=False).hexdigest()
                if art.get("md5") and md5 != art["md5"]:
                    self.fail("manifests", key, method="GET", path=art["name"], params=None,
                              error=f"md5 mismatch {md5} != {art['md5']}")
                    continue
                payload = json.loads(blob)
                payload["_artifact"] = {"name": art["name"], "md5": md5, "size": len(blob), "md5Verified": bool(art.get("md5"))}
                self.land("manifests", key, payload, method="GET", path=f"GCS {art['name']}", params=None, status=200,
                          attempts=attempts, elapsed_ms=(time.time() - t) * 1000, response_bytes=len(blob))
        return items

    async def products(self) -> list[dict]:
        return await self.get("products", "products", "/products", timeout_s=600)

    async def sites(self) -> list[dict]:
        return await self.get("sites", "sites", "/sites", timeout_s=600)

    async def release_records(self, tags: list[str]) -> None:
        async def one(tag: str, kind: str) -> None:
            await self.get(f"release_{kind}", tag, f"/releases/{tag}/{kind}", timeout_s=600)
        await asyncio.gather(*(one(t, k) for t in tags for k in ("products", "sites")))

    async def small(self) -> None:
        await asyncio.gather(
            self.get("locations_sites", "locations_sites", "/locations/sites", timeout_s=300),
            self.get("samples", "supportedClasses", "/samples/supportedClasses"),
            self.get("prototype", "datasets", "/prototype/datasets", timeout_s=300),
            self.graphql_schema(),
        )

    async def graphql_schema(self) -> None:
        family, key = "graphql", "schema"
        if self.landed(family, key) is not None:
            return
        client = self.bundle.client("meta")
        t = time.time()
        env, attempts = await self.call(lambda: client.graphql(INTROSPECTION_Q, None, token=self.bundle.token,
                                                               timeout_s=120), family=family, key=key)
        payload = env.data
        if payload.get("errors") and not (payload.get("data") or {}).get("__schema"):
            self.fail(family, key, method="POST", path="/graphql", params="introspection",
                      error=json.dumps(payload["errors"])[:400])
            return
        self.land(family, key, payload, method="POST", path="/graphql", params="introspection", status=env.status,
                  attempts=attempts, elapsed_ms=(time.time() - t) * 1000, response_bytes=None)

    async def prototype_data(self, datasets: list[dict]) -> None:
        async def one(d_uuid: str) -> None:
            try:
                await self.get("prototype_data", d_uuid, f"/prototype/data/{d_uuid}")
            except Exception as exc:  # noqa: BLE001 - recorded in the ledger
                log.debug("prototype_data %s failed: %s", d_uuid, exc)
        await asyncio.gather(*(one(d["uuid"]) for d in datasets if d.get("uuid")))

    async def taxonomy(self) -> None:
        async def type_pages(ttype: str) -> None:
            limit = 500
            first = await self.get("taxonomy", f"{ttype}__0", "/taxonomy",
                                   {"taxonTypeCode": ttype, "verbose": "true", "offset": 0, "limit": limit})
            total = int(first.get("total") or 0)
            got = int(first.get("count") or len(first.get("data") or []))
            page = got if 0 < got < limit and got < total else limit
            offs = list(range(page, total, page))
            if self.pilot:
                offs = offs[:1]

            async def one(off: int) -> None:
                try:
                    await self.get("taxonomy", f"{ttype}__{off}", "/taxonomy",
                                   {"taxonTypeCode": ttype, "verbose": "true", "offset": off, "limit": page})
                except Exception as exc:  # noqa: BLE001 - recorded in the ledger
                    log.debug("taxonomy %s@%d failed: %s", ttype, off, exc)
            await asyncio.gather(*(one(o) for o in offs))
        await asyncio.gather(*(type_pages(t) for t in TAXON_TYPES))

    async def documents(self, spec_numbers: list[str]) -> None:
        client = self.bundle.client("meta")

        async def one(spec: str) -> None:
            family, key = "documents_head", spec
            if self.landed(family, key) is not None:
                self.counts[family]["cached"] += 1
                return
            t = time.time()
            try:
                info, attempts = await self.call(lambda: client.head(f"/documents/{spec}", token=None),
                                                 family=family, key=key)
                payload = {"specNumber": spec, "status": info.status, "contentType": info.content_type,
                           "contentLength": info.content_length, "fileName": info.filename}
                self.land(family, key, payload, method="HEAD", path=f"/documents/{spec}", params=None,
                          status=info.status, attempts=attempts, elapsed_ms=(time.time() - t) * 1000,
                          response_bytes=None)
            except Exception as exc:  # noqa: BLE001
                self.land(family, key, {"specNumber": spec, "status": getattr(exc, "status", None),
                                        "error": f"{type(exc).__name__}: {str(exc)[:200]}"},
                          method="HEAD", path=f"/documents/{spec}", params=None,
                          status=getattr(exc, "status", 0) or 0, attempts=0, elapsed_ms=0, response_bytes=None)
        await asyncio.gather(*(one(s) for s in spec_numbers))

    async def locations(self, site_codes: list[str]) -> None:
        from neon_mcp.projections.locations import flatten_hierarchy

        nodes: dict[str, dict[str, Any]] = {}
        for d in DOMAINS + ["REALM"]:
            nodes[d] = {"hierarchyDepth": 0, "hierarchyParent": "REALM" if d != "REALM" else None,
                        "hierarchySite": None, "locationType": "DOMAIN" if d != "REALM" else "REALM"}

        async def hierarchy(code: str) -> None:
            raw = await self.get("location_hierarchy", code, f"/locations/{code}", {"hierarchy": "true"},
                                 timeout_s=600)
            items, _types, _scanned = flatten_hierarchy(raw, location_type=None, max_depth=12)
            nodes[code] = {"hierarchyDepth": 0, "hierarchyParent": raw.get("locationParent"),
                           "hierarchySite": code, "locationType": raw.get("locationType")}
            for it in items:
                nodes[it.location_name] = {"hierarchyDepth": it.depth, "hierarchyParent": it.parent,
                                           "hierarchySite": code, "locationType": it.location_type}
        await asyncio.gather(*(hierarchy(c) for c in site_codes))
        self.write_stage("location_nodes", nodes)
        names = sorted(nodes)
        batches = [names[i:i + LOC_BATCH] for i in range(0, len(names), LOC_BATCH)]
        log.info("locations: %d nodes, %d findLocations batches", len(names), len(batches))
        client = self.bundle.client("harvest")

        async def hydrate(i: int, batch: list[str]) -> None:
            family, key = "locations", f"batch_{i:05d}"
            if self.landed(family, key) is not None:
                self.counts[family]["cached"] += 1
                return
            t = time.time()
            try:
                env, attempts = await self.call(
                    lambda: client.graphql(LOC_Q, {"query": {"locationNames": batch}}, token=self.bundle.token,
                                           timeout_s=180), family=family, key=key)
            except Exception as exc:  # noqa: BLE001
                self.fail(family, key, method="POST", path="/graphql findLocations", params={"n": len(batch)},
                          error=f"{type(exc).__name__}: {exc}")
                return
            rows = ((env.data or {}).get("data") or {}).get("findLocations") or []
            self.land(family, key, {"names": batch, "rows": rows, "errors": (env.data or {}).get("errors")},
                      method="POST", path="/graphql findLocations", params={"n": len(batch)}, status=env.status,
                      attempts=attempts, elapsed_ms=(time.time() - t) * 1000, response_bytes=None)
        await asyncio.gather(*(hydrate(i, b) for i, b in enumerate(batches)))
        hist = [n for n, v in nodes.items() if (v.get("locationType") or "").upper() in HISTORY_TYPES]
        if self.pilot:
            hist = hist[:20]
        log.info("location history: %d locations (types %s)", len(hist), sorted(HISTORY_TYPES))

        async def history(name: str) -> None:
            try:
                await self.get("location_history", name, f"/locations/{name}", {"history": "true"}, timeout_s=120)
            except Exception as exc:  # noqa: BLE001 - recorded in the ledger
                log.debug("location history %s failed: %s", name, exc)
        await asyncio.gather(*(history(n) for n in hist))

    def write_stage(self, name: str, obj: Any) -> None:
        p = RUNS_DIR / self.run_id / "stage" / f"{name}.json.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(gzip.compress(json.dumps(obj).encode()))

    # ------------------------------------------------------------ inventory
    @staticmethod
    def plan_pairs(products: list[dict], manifests: dict[str, dict]) -> list[dict]:
        """Non-AOP product x site pairs: current availability + release-only cells from every manifest."""
        current: dict[tuple[str, str], set[str]] = defaultdict(set)
        team: dict[str, str] = {}
        for p in products:
            code = p["productCode"]
            team[code] = p.get("productScienceTeamAbbr") or ""
            for sc in p.get("siteCodes") or []:
                current[(code, sc["siteCode"])].update(sc.get("availableMonths") or [])
        in_release: dict[tuple[str, str, str], list[str]] = defaultdict(list)
        for tag, man in sorted(manifests.items()):
            for dp in man.get("dpSiteMonths") or []:
                code = re.sub(r"^NEON\.DOM\.SITE\.", "", dp["dp"])
                for sm in dp.get("siteMonths") or []:
                    for m in sm.get("months") or []:
                        in_release[(code, sm["site"], m)].append(tag)
        release_only: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
        for (code, site, m), tags in in_release.items():
            if m not in current.get((code, site), set()):
                release_only[(code, site)][m] = max(tags)  # newest release containing the cell
        pairs = []
        keys = set(current) | set(release_only)
        for code, site in sorted(keys):
            if is_aop(code, team.get(code)):
                continue
            cur = sorted(current.get((code, site), set()))
            ro = release_only.get((code, site), {})
            if cur:
                pairs.append({"product": code, "site": site, "start": cur[0], "end": cur[-1], "release": None,
                              "months": cur, "scope": "current"})
            by_rel: dict[str, list[str]] = defaultdict(list)
            for m, tag in ro.items():
                by_rel[tag].append(m)
            for tag, months in sorted(by_rel.items()):
                months.sort()
                pairs.append({"product": code, "site": site, "start": months[0], "end": months[-1], "release": tag,
                              "months": months, "scope": "release-only"})
        return pairs

    async def data_query(self, pair: dict, start: str, end: str, depth: int = 0) -> tuple[int, int]:
        from neon_mcp.errors import NeonApiError

        rel = pair["release"]
        key = f"{pair['product']}__{pair['site']}__{start}_{end}" + (f"__{rel}" if rel else "")
        family = "data_query"
        cached = self.landed(family, key)
        if cached is not None:
            self.counts[family]["cached"] += 1
            return self._count_files(cached)
        body = {"productCode": pair["product"], "siteCodes": [pair["site"]], "startDateMonth": start,
                "endDateMonth": end, "package": "basic", "includeProvisional": rel is None}
        if rel:
            body["release"] = rel
        client = self.bundle.client("harvest")
        t = time.time()
        try:
            async with self.sem:
                env = await client.post_json("/data/query", body, token=self.bundle.token, timeout_s=180)
            attempts = 1
        except NeonApiError as exc:
            splittable = exc.kind == "transport" or (exc.status or 0) >= 500 or exc.status == 413
            parts = split_window(start, end) if splittable and depth < 6 else []
            if parts:
                self.fail(family, key, method="POST", path="/data/query", params=body,
                          error=f"{exc.kind}:{exc.status} -> split", status=exc.status, final=False)
                n = b = 0
                for s, e in parts:
                    fn, fb = await self.data_query(pair, s, e, depth + 1)
                    n, b = n + fn, b + fb
                return n, b
            # last resort: app-level retries for non-splittable windows
            try:
                env, attempts = await self.call(
                    lambda: client.post_json("/data/query", body, token=self.bundle.token, timeout_s=300),
                    family=family, key=key)
            except Exception as exc2:  # noqa: BLE001
                self.fail(family, key, method="POST", path="/data/query", params=body,
                          error=f"{type(exc2).__name__}: {exc2}", status=getattr(exc2, "status", None))
                return 0, 0
        data = env.data or {}
        if rel:  # keep only the release-only months from a release-scoped response
            keep = set(pair["months"])
            for r in data.get("releases") or []:
                r["packages"] = [pk for pk in r.get("packages") or [] if pk.get("month") in keep]
        data["_request"] = {"pair": {k: pair[k] for k in ("product", "site", "release", "scope")},
                            "window": [start, end]}
        self.land(family, key, data, method="POST", path="/data/query", params=body, status=env.status,
                  attempts=attempts, elapsed_ms=(time.time() - t) * 1000, response_bytes=None)
        return self._count_files(data)

    @staticmethod
    def _count_files(data: dict) -> tuple[int, int]:
        n = b = 0
        for r in data.get("releases") or []:
            for pk in r.get("packages") or []:
                for f in pk.get("files") or []:
                    n += 1
                    b += int(f.get("size") or 0)
        return n, b

    async def inventory(self, pairs: list[dict]) -> list[dict]:
        def weight(p: dict) -> float:
            return len(p["months"]) * (25 if p["product"].startswith(("DP1.00", "DP4.00", "DP1.20", "DP2.00")) else 5)
        order = sorted(pairs, key=weight, reverse=True)
        done = {"n": 0}
        zero: list[dict] = []
        total = len(order)

        async def one(p: dict) -> None:
            n, _b = await self.data_query(p, p["start"], p["end"])
            p["fileCount"] = n
            if n == 0:
                zero.append(p)
            done["n"] += 1
            if done["n"] % 200 == 0 or done["n"] == total:
                el = time.time() - self.t0
                log.info("inventory %d/%d pairs (%.0fs, %s)", done["n"], total, el, dict(self.counts["data_query"]))
        await asyncio.gather(*(one(p) for p in order))
        return zero

    async def external(self, zero_pairs: list[dict]) -> None:
        async def one(p: dict) -> None:
            m = p["end"]
            try:
                await self.get("external", f"{p['product']}__{p['site']}__{m}",
                               f"/data/{p['product']}/{p['site']}/{m}", {"package": "basic"}, timeout_s=120)
            except Exception as exc:  # noqa: BLE001 - recorded in the ledger
                log.debug("external probe %s failed: %s", p["product"], exc)
        await asyncio.gather(*(one(p) for p in zero_pairs if p["scope"] == "current"))

    # ------------------------------------------------------------ orchestration
    async def run(self, stages: set[str]) -> dict[str, Any]:
        log.info("harvest %s stages=%s pilot=%s", self.run_id, sorted(stages), self.pilot)
        products = await self.products()
        releases = await self.releases()
        tags = [r["release"] for r in releases]
        manifests = {t: m for t in tags if (m := self.landed("manifests", t)) is not None}
        sites = await self.sites()
        site_codes = sorted(s["siteCode"] for s in sites)
        pairs = self.plan_pairs(products, manifests)
        self.write_stage("pairs", pairs)
        log.info("products=%d releases=%d manifests=%d sites=%d pairs=%d (release-only %d)", len(products),
                 len(releases), len(manifests), len(sites), len(pairs),
                 sum(1 for p in pairs if p["scope"] == "release-only"))
        if "catalog" in stages:
            specs = sorted({s["specNumber"] for p in products for s in (p.get("specs") or []) if s.get("specNumber")})
            await self.small()
            protos = self.landed("prototype", "datasets") or []
            protos = protos if isinstance(protos, list) else protos.get("data", [])
            await asyncio.gather(
                self.release_records(tags[:1] if self.pilot else tags),
                self.taxonomy(),
                self.documents(specs[:10] if self.pilot else specs),
                self.prototype_data(protos[:3] if self.pilot else protos),
            )
        if "locations" in stages:
            await self.locations(site_codes[:2] if self.pilot else site_codes)
        if "inventory" in stages:
            if self.pilot:
                want = {("DP1.00041.001", "SRER"), ("DP1.10003.001", "HARV"), ("DP1.00006.001", "SRER"),
                        ("DP1.00033.001", "SRER")}
                pairs = [p for p in pairs if (p["product"], p["site"]) in want]
            zero = await self.inventory(pairs)
            self.write_stage("zero_pairs", zero)
            await self.external(zero)
        summary = {fam: dict(c) for fam, c in self.counts.items()}
        return {"runId": self.run_id, "seconds": round(time.time() - self.t0, 1), "counts": summary,
                "pairs": len(pairs)}


def latest_run() -> str | None:
    p = API_DIR / "LATEST"
    return p.read_text().strip() if p.exists() else None


def open_run(resume: bool) -> str:
    if resume:
        cands = sorted(d.name for d in RUNS_DIR.iterdir() if d.is_dir()) if RUNS_DIR.exists() else []
        for rid in reversed(cands):
            man = RUNS_DIR / rid / "fetch_manifest.json"
            if not man.exists() or json.loads(man.read_text()).get("status") != "complete":
                return rid
    return new_run_id()


async def _main(stages: set[str], pilot: bool, resume: bool, rid: str | None) -> int:
    from .env import repo_versions, tool_versions

    rid = rid or open_run(resume)
    con = state.connect()
    bundle = neon_bundle(("harvest", "meta"))
    state.start_run(con, rid, "catalog-fetch", None, sorted(stages) + (["pilot"] if pilot else []))
    h = Harvest(rid, bundle, con, pilot=pilot)
    status = "complete"
    try:
        result = await h.run(stages)
    except BaseException as exc:
        status = "interrupted" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else "failed"
        result = {"runId": rid, "error": f"{type(exc).__name__}: {exc}",
                  "counts": {f: dict(c) for f, c in h.counts.items()}}
        raise
    finally:
        failures = con.execute("SELECT family, key, error FROM requests WHERE run_id=? AND status='error'",
                               (rid,)).fetchall()
        if status == "complete" and failures:
            status = "partial"
        if pilot and status == "complete":
            status = "pilot"
        manifest = {"runId": rid, "status": status, "stages": sorted(stages), "pilot": pilot,
                    "startedAt": con.execute("SELECT started_at FROM runs WHERE run_id=? AND stage='catalog-fetch'",
                                             (rid,)).fetchone()[0],
                    "finishedAt": utcnow().isoformat(), "versions": repo_versions(), "tools": tool_versions(),
                    "tunables": {"concurrency": CONCURRENCY, "locBatch": LOC_BATCH,
                                 "maxWaitS": bundle.config.neon.rate_limit.max_wait_s,
                                 "roles": {r: {"maxAttempts": c.config.retries.max_attempts,
                                               "readTimeoutS": c.config.read_timeout_s}
                                           for r, c in bundle.clients.items()}},
                    "transforms": ["signed-url query strings stripped"],
                    "failures": [dict(f) for f in failures][:500], "failureCount": len(failures), **result}
        write_json(RUNS_DIR / rid / "fetch_manifest.json", manifest)
        if status == "complete":
            (API_DIR / "LATEST").write_text(rid + "\n")
        state.finish_run(con, rid, "catalog-fetch", None, status, {"failures": len(failures)})
        await bundle.aclose()
    log.info("harvest %s: %s (%d failures)", rid, status, len(failures))
    return 0 if status in ("complete", "pilot") else 2


def main(stages: set[str], pilot: bool = False, resume: bool = True, rid: str | None = None) -> int:
    return asyncio.run(_main(stages, pilot, resume, rid))
