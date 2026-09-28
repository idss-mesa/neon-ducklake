"""Sample months for probing a site: ``site list|download <SITE> --sample N`` and ``run --sample N``.

:func:`months_for` reads the catalog lake view ``v_site_inventory`` (attached READ_ONLY in a private
in-memory DuckDB) and keeps, per non-AOP product at the site, the N most recent months. The latest
released month and the latest PROVISIONAL month are always included when the product has them, so a
sample exercises both release paths; a product known only from earlier releases gets its latest months.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from . import site_lake

log = logging.getLogger("neon_ducklake.site.sample")


def catalog_query(sql: str, params: Iterable[Any] = (), *, con: Any | None = None) -> list[dict[str, Any]]:
    """Rows of ``sql`` against the catalog lake; ``{cat}`` in ``sql`` is the catalog prefix.

    With ``con`` (tests: a plain DuckDB holding tables of the same names) ``{cat}`` is empty. Otherwise the
    catalog lake is attached READ_ONLY in a new in-memory DuckDB that is closed afterwards.
    """
    def fetch(c: Any, prefix: str) -> list[dict[str, Any]]:
        cur = c.execute(sql.format(cat=prefix), list(params))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    if con is not None:
        return fetch(con, "")
    if not site_lake.catalog_lake_file().exists():
        raise SystemExit(f"catalog lake missing ({site_lake.catalog_lake_file()}): run `catalog build` first")
    import duckdb

    c = duckdb.connect()
    try:
        c.execute("LOAD ducklake")
        if not site_lake.attach_catalog(c):
            raise SystemExit("catalog lake is not attachable (is a `catalog build` writing it?)")
        return fetch(c, f"{site_lake.CATALOG_ALIAS}.")
    finally:
        c.close()


def inventory_rows(site: str, *, con: Any | None = None) -> list[dict[str, Any]]:
    return catalog_query("SELECT productCode, month, isProvisional, isReleaseOnly FROM {cat}v_site_inventory "
                         "WHERE siteCode = ?", [site.upper()], con=con)


def select_months(rows: Iterable[dict[str, Any]], n: int) -> dict[str, list[str]]:
    """productCode -> sorted sample months (see the module docstring)."""
    if n < 1:
        raise ValueError("sample size must be >= 1")
    by_product: dict[str, dict[str, tuple[bool, bool]]] = defaultdict(dict)
    for r in rows:
        if r.get("productCode") and r.get("month"):
            prov, ro = bool(r.get("isProvisional")), bool(r.get("isReleaseOnly"))
            old = by_product[r["productCode"]].get(r["month"])
            # a month present as current wins over a release-only duplicate
            if old is None or (old[1] and not ro):
                by_product[r["productCode"]][r["month"]] = (prov, ro)
    out: dict[str, list[str]] = {}
    for code in sorted(by_product):
        months = by_product[code]
        recent = sorted(months, reverse=True)
        released = [m for m in recent if not months[m][0] and not months[m][1]]
        release_only = [m for m in recent if months[m][1]]
        provisional = [m for m in recent if months[m][0]]
        pick: set[str] = set()
        if released:
            pick.add(released[0])
        elif release_only:
            pick.add(release_only[0])
        if provisional:
            pick.add(provisional[0])
        for m in recent:
            if len(pick) >= n:
                break
            pick.add(m)
        out[code] = sorted(pick)
    return out


def months_for(site: str, n: int, *, con: Any | None = None) -> dict[str, list[str]]:
    """productCode -> the sample months of every non-AOP product at ``site`` (catalog ``v_site_inventory``)."""
    return select_months(inventory_rows(site, con=con), n)


def _plan(site: str, n: int, products: list[str] | None, con: Any | None) -> dict[str, list[str]]:
    plan = months_for(site, n, con=con)
    if products:
        for code in sorted(set(products) - set(plan)):
            log.warning("%s has no non-AOP availability at %s", code, site)
        plan = {p: m for p, m in plan.items() if p in set(products)}
    return plan


def run_list(site: str, n: int, *, products: list[str] | None = None, threads: int = 8,
             con: Any | None = None) -> int:
    """``site list <SITE> --sample N``: one in-process ``fetch.main('list')`` per product with its months."""
    from . import fetch

    site = site.upper()
    plan = _plan(site, n, products, con)
    if not plan:
        log.warning("list %s --sample %d: no products to list", site, n)
        return 1
    rc = 0
    failed: dict[str, Any] = {}
    for code, months in plan.items():
        try:
            r = fetch.main("list", site, products=[code], months=months, threads=threads)
        except Exception as exc:  # noqa: BLE001 - one product must not stop the sample
            log.error("list %s %s failed: %s", site, code, f"{type(exc).__name__}: {exc}"[:300])
            r = 2
        if r:
            failed[code] = r
            rc = max(rc, int(r))
    n_months = sum(len(m) for m in plan.values())
    print(f"list {site} --sample {n}: {len(plan)} product(s), {n_months} product-month(s), "
          f"{len(failed)} failed" + (f": {', '.join(sorted(failed))}" if failed else ""))
    return rc


def run_download(site: str, n: int, *, products: list[str] | None = None, threads: int = 8,
                 con: Any | None = None) -> int:
    """``site download <SITE> --sample N``: one download restricted to the sampled products and months."""
    from . import fetch

    site = site.upper()
    plan = _plan(site, n, products, con)
    if not plan:
        log.warning("download %s --sample %d: no products to download", site, n)
        return 1
    months = sorted({m for ms in plan.values() for m in ms})
    return fetch.main("download", site, products=sorted(plan), months=months, threads=threads)
