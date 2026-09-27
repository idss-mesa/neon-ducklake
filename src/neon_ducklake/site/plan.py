"""`site plan <SITE>` — read-only preview of one site's Phase-2 scope, plus the shared inventory readers.

Source of truth is the catalog lake view ``v_site_inventory`` (non-AOP product x site x month). Until the
catalog lake exists, the plan falls back to the newest harvest run: ``stage/pairs.json.gz`` (current and
release-only windows), the ``raw/data_query/*__<SITE>__*`` landings (file counts/bytes) and
``raw/products`` (names, teams, PROVISIONAL months, AOP exclusions). Only complete ``*.json.gz`` landings
are read (never ``*.part``); a harvest may still be writing new ones.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from .. import env
from . import site_lake

log = logging.getLogger("neon_ducklake.site.plan")

# Externally hosted products: /data returns 0 files and an externalData link instead.
EXTERNAL_PRODUCTS = frozenset({"DP1.00033.001", "DP1.00042.001", "DP1.00043.001"})


# ---------------------------------------------------------------- harvest landings

def read_gz(path: Path) -> Any | None:
    """A complete gzip JSON landing, or None when missing/partial/corrupt."""
    try:
        return json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, ValueError, EOFError):
        return None


def run_dirs() -> list[Path]:
    """Harvest run directories, newest first."""
    if not env.RUNS_DIR.exists():
        return []
    return sorted((d for d in env.RUNS_DIR.iterdir() if d.is_dir()), key=lambda d: d.name, reverse=True)


def latest_stage(name: str) -> tuple[Any, str] | tuple[None, None]:
    for run in run_dirs():
        data = read_gz(run / "stage" / f"{name}.json.gz")
        if data is not None:
            return data, run.name
    return None, None


def latest_raw(family: str, key: str) -> Any | None:
    for run in run_dirs():
        data = read_gz(run / "raw" / family / f"{key}.json.gz")
        if data is not None:
            return data
    return None


def products_index() -> dict[str, dict]:
    """productCode -> product record from the newest ``raw/products`` landing ({} if none)."""
    data = latest_raw("products", "products")
    if isinstance(data, dict):
        data = data.get("data", [])
    return {p["productCode"]: p for p in data or []}


def site_landings(site: str) -> dict[tuple[str, str | None], list[dict]]:
    """(product, release-or-None) -> data_query payloads for the site, each pair from its newest run."""
    out: dict[tuple[str, str | None], list[dict]] = {}
    for run in run_dirs():
        found: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
        for p in sorted((run / "raw" / "data_query").glob(f"*__{site}__*.json.gz")):
            payload = read_gz(p)
            if payload is None:
                continue
            req = (payload.get("_request") or {}).get("pair") or {}
            parts = p.name[: -len(".json.gz")].split("__")
            product = req.get("product") or parts[0]
            release = req.get("release") if req else (parts[3] if len(parts) > 3 else None)
            found[(product, release)].append(payload)
        for key, payloads in found.items():
            out.setdefault(key, payloads)
    return out


def landed_files(payloads: list[dict]) -> dict[str, dict[str, Any]]:
    """month -> {files, bytes, release, generationDate} over the (possibly bisected) landings of one pair."""
    months: dict[str, dict[str, Any]] = {}
    seen: set[tuple[str, str]] = set()
    for payload in payloads:
        for rel in payload.get("releases") or []:
            for pk in rel.get("packages") or []:
                m = pk.get("month")
                slot = months.setdefault(m, {"files": 0, "bytes": 0, "release": rel.get("release"),
                                             "generationDate": rel.get("generationDate")})
                for f in pk.get("files") or []:
                    if (m, f.get("name")) in seen:
                        continue
                    seen.add((m, f.get("name")))
                    slot["files"] += 1
                    slot["bytes"] += int(f.get("size") or 0)
    return months


# ---------------------------------------------------------------- site inventory

def _pairs_from_rows(site: str, rows: list[dict]) -> list[dict]:
    """v_site_inventory rows -> harvest-style pairs (one current window + one per release-only release)."""
    by_product: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_product[r["productCode"]].append(r)
    pairs = []
    for code in sorted(by_product):
        rs = by_product[code]
        external = any(r.get("isExternal") for r in rs) or code in EXTERNAL_PRODUCTS
        month_releases = {r["month"]: r.get("release") for r in rs}
        cur = sorted({r["month"] for r in rs if not r.get("isReleaseOnly")})
        if cur:
            pairs.append({"product": code, "site": site, "start": cur[0], "end": cur[-1], "release": None,
                          "months": cur, "scope": "current", "external": external, "monthReleases": month_releases})
        by_rel: dict[str, set[str]] = defaultdict(set)
        for r in rs:
            if r.get("isReleaseOnly"):
                by_rel[r["release"]].add(r["month"])
        for tag, ms in sorted(by_rel.items()):
            ms_sorted = sorted(ms)
            pairs.append({"product": code, "site": site, "start": ms_sorted[0], "end": ms_sorted[-1],
                          "release": tag, "months": ms_sorted, "scope": "release-only", "external": external,
                          "monthReleases": month_releases})
    return pairs


def lake_inventory(site: str) -> list[dict] | None:
    """v_site_inventory rows for the site, or None when the catalog lake is missing/unreadable."""
    if not site_lake.catalog_lake_file().exists():
        return None
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("LOAD ducklake")
        if not site_lake.attach_catalog(con):
            return None
        cur = con.execute(f"SELECT * FROM {site_lake.CATALOG_ALIAS}.v_site_inventory WHERE siteCode = ?", [site])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    except duckdb.Error as exc:
        log.warning("v_site_inventory unreadable (%s); using harvest landings", str(exc).splitlines()[0][:160])
        return None
    finally:
        con.close()


def site_pairs(site: str) -> tuple[list[dict], str]:
    """The site's non-AOP pairs and where they came from ('lake' or 'stage:<runId>')."""
    rows = lake_inventory(site)
    if rows is not None:
        return _pairs_from_rows(site, rows), "lake"
    pairs, rid = latest_stage("pairs")
    if pairs is None:
        raise SystemExit("no catalog lake and no harvest stage/pairs.json.gz: run `catalog fetch` first")
    zero, _ = latest_stage("zero_pairs")
    zero_keys = {(z["product"], z["site"]) for z in zero or []}
    out = []
    for p in pairs:
        if p["site"] != site:
            continue
        q = dict(p)
        q["external"] = p["product"] in EXTERNAL_PRODUCTS or (p["product"], site) in zero_keys
        out.append(q)
    return out, f"stage:{rid}"


def aop_exclusions(site: str) -> list[dict]:
    """AOP products with availability at the site (excluded from Phase 2)."""
    from ..harvest import is_aop

    if site_lake.catalog_lake_file().exists():
        import duckdb

        con = duckdb.connect()
        try:
            con.execute("LOAD ducklake")
            if site_lake.attach_catalog(con):
                a = site_lake.CATALOG_ALIAS
                rows = con.execute(
                    f"SELECT p.productCode, any_value(p.productName), count(DISTINCT m.month) "
                    f"FROM {a}.availability_months m JOIN {a}.products p USING (productCode) "
                    f"WHERE m.siteCode = ? AND p.isAop GROUP BY 1 ORDER BY 1", [site]).fetchall()
                return [{"productCode": r[0], "productName": r[1], "months": int(r[2])} for r in rows]
        except Exception as exc:  # noqa: BLE001 - fall back to the landing
            log.warning("AOP exclusions from lake failed (%s)", str(exc).splitlines()[0][:160])
        finally:
            con.close()
    out = []
    for code, p in sorted(products_index().items()):
        if not is_aop(code, p.get("productScienceTeamAbbr")):
            continue
        for sc in p.get("siteCodes") or []:
            if sc.get("siteCode") == site:
                out.append({"productCode": code, "productName": p.get("productName"),
                            "months": len(sc.get("availableMonths") or [])})
    return out


# ---------------------------------------------------------------- plan

def build_plan(site: str) -> dict[str, Any]:
    site = site.upper()
    rows = lake_inventory(site)
    products: dict[str, dict[str, Any]] = {}
    if rows is not None:
        source = "lake:v_site_inventory"
        for r in rows:
            p = products.setdefault(r["productCode"], {
                "productCode": r["productCode"], "productName": r.get("productName"),
                "team": r.get("productScienceTeamAbbr"), "months": set(), "provisionalMonths": 0,
                "releaseOnlyMonths": 0, "releases": set(), "files": 0, "bytes": 0, "external": False,
                "listed": True})
            p["months"].add(r["month"])
            p["releases"].add(r.get("release"))
            p["provisionalMonths"] += bool(r.get("isProvisional"))
            p["releaseOnlyMonths"] += bool(r.get("isReleaseOnly"))
            p["files"] += int(r.get("fileCount") or 0)
            p["bytes"] += int(r.get("totalBytes") or 0)
            p["external"] = p["external"] or bool(r.get("isExternal"))
    else:
        pairs, source = site_pairs(site)
        idx = products_index()
        landings = site_landings(site)
        for pair in pairs:
            code = pair["product"]
            rec = idx.get(code, {})
            p = products.setdefault(code, {
                "productCode": code, "productName": rec.get("productName"),
                "team": rec.get("productScienceTeamAbbr"), "months": set(), "provisionalMonths": 0,
                "releaseOnlyMonths": 0, "releases": set(), "files": 0, "bytes": 0,
                "external": bool(pair.get("external")), "listed": True})
            p["months"].update(pair["months"])
            if pair["scope"] == "release-only":
                p["releaseOnlyMonths"] += len(pair["months"])
                p["releases"].add(pair["release"])
            else:
                prov = set()
                for sc in rec.get("siteCodes") or []:
                    if sc.get("siteCode") == site:
                        for ar in sc.get("availableReleases") or []:
                            if ar.get("release") == "PROVISIONAL":
                                prov.update(ar.get("availableMonths") or [])
                            else:
                                p["releases"].add(ar.get("release"))
                p["provisionalMonths"] += len(prov & set(pair["months"]))
                if prov & set(pair["months"]):
                    p["releases"].add("PROVISIONAL")
            payloads = landings.get((code, pair["release"]))
            if payloads is None:
                p["listed"] = p["external"]  # external pairs need no file listing
                continue
            keep = set(pair["months"])
            for m, info in landed_files(payloads).items():
                if m in keep:
                    p["files"] += info["files"]
                    p["bytes"] += info["bytes"]
            if p["files"] == 0 and code not in EXTERNAL_PRODUCTS and payloads:
                p["external"] = p["external"] or all(
                    not (pk.get("files")) for pl in payloads for r in pl.get("releases") or []
                    for pk in r.get("packages") or [])
    out_products = []
    for code in sorted(products):
        p = products[code]
        months = sorted(p.pop("months"))
        cur = len(months) - p["releaseOnlyMonths"]
        scope = ("current+release-only" if p["releaseOnlyMonths"] and cur > 0
                 else "release-only" if p["releaseOnlyMonths"] else "current")
        out_products.append({**p, "scope": scope, "months": len(months),
                             "firstMonth": months[0] if months else None,
                             "lastMonth": months[-1] if months else None,
                             "releases": sorted(r for r in p["releases"] if r)})
    totals = {
        "products": len(out_products),
        "currentProducts": sum(1 for p in out_products if p["scope"] != "release-only"),
        "releaseOnlyProducts": sum(1 for p in out_products if p["scope"] == "release-only"),
        "externalProducts": sum(1 for p in out_products if p["external"]),
        "siteMonths": sum(p["months"] for p in out_products),
        "releaseOnlySiteMonths": sum(p["releaseOnlyMonths"] for p in out_products),
        "provisionalSiteMonths": sum(p["provisionalMonths"] for p in out_products),
        "files": sum(p["files"] for p in out_products),
        "bytes": sum(p["bytes"] for p in out_products),
        "productsNotYetListed": [p["productCode"] for p in out_products if not p["listed"]],
    }
    return {"site": site, "source": source, "generatedAt": env.utcnow().isoformat(), "package": "basic",
            "products": out_products, "totals": totals, "excludedAop": aop_exclusions(site)}


def _fmt_bytes(n: int) -> str:
    for unit, div in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if n >= div:
            return f"{n / div:.2f} {unit}"
    return f"{n} B"


def render(plan: dict[str, Any]) -> str:
    lines = [f"site plan {plan['site']} (source: {plan['source']}, package basic, AOP excluded)", ""]
    hdr = f"{'product':15} {'team':5} {'scope':20} {'months':>6} {'files':>7} {'size':>10}  {'range':17} name"
    lines += [hdr, "-" * len(hdr)]
    for p in plan["products"]:
        files = str(p["files"]) if p["listed"] else "?"
        size = _fmt_bytes(p["bytes"]) if p["listed"] else "?"
        scope = p["scope"] + (" ext" if p["external"] else "")
        rng = f"{p['firstMonth'] or ''}..{p['lastMonth'] or ''}"
        lines.append(f"{p['productCode']:15} {(p['team'] or '')[:5]:5} {scope:20} {p['months']:>6} {files:>7} "
                     f"{size:>10}  {rng:17} {(p['productName'] or '')[:48]}")
    t = plan["totals"]
    lines += ["", (f"totals: {t['products']} products ({t['currentProducts']} current, "
                   f"{t['releaseOnlyProducts']} release-only, {t['externalProducts']} external), "
                   f"{t['siteMonths']} site-months ({t['provisionalSiteMonths']} provisional, "
                   f"{t['releaseOnlySiteMonths']} release-only), {t['files']} files, {_fmt_bytes(t['bytes'])}")]
    if t["productsNotYetListed"]:
        lines.append(f"not yet inventoried by the harvest ({len(t['productsNotYetListed'])}): "
                     + ", ".join(t["productsNotYetListed"]))
    ex = plan["excludedAop"]
    lines.append(f"excluded AOP products ({len(ex)}): " + ", ".join(f"{e['productCode']}({e['months']}m)" for e in ex))
    return "\n".join(lines)


def main(site: str) -> int:
    site = site.upper()
    plan = build_plan(site)
    out = site_lake.site_paths(site).reports_dir / "plan.json"
    env.write_json(out, plan)
    print(render(plan))
    print(f"\nwrote {out}")
    return 0
