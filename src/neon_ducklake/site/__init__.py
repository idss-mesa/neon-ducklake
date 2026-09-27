"""Phase 2 — one site subtree. `dispatch` routes `neon-ducklake site <step> <SITE>`."""

from __future__ import annotations

from typing import Any


def dispatch(a: Any) -> int:
    site = a.site.upper()
    products = [p.strip() for p in (a.products or "").split(",") if p.strip()] or None
    months = [m.strip() for m in (a.months or "").split(",") if m.strip()] or None
    if a.step == "plan":
        from .plan import main
        return main(site)
    if a.step in ("list", "download"):
        from .fetch import main
        return main(a.step, site, products=products, months=months, threads=a.threads)
    if a.step == "stack":
        from .stack import main
        return main(site, products=products, force=a.force)
    if a.step == "terms":
        from .terms import main
        return main(site)
    if a.step == "cards":
        from .cards import main
        return main(site, products=products)
    if a.step == "curate":
        from .curate import main
        return main(site, products=products, force=a.force)
    if a.step == "validate":
        from .validate import main
        return main(site, accept=a.accept)
    if a.step == "upload":
        from .upload import main
        return main(site, products=products, months=months, threads=a.threads)
    if a.step == "build-avus":
        from .avus import main
        return main(site, products=products, months=months)
    raise SystemExit(f"unknown step {a.step}")
