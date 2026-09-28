"""Phase 2 — one site subtree. `dispatch` routes `neon-ducklake site <step> <SITE>`.

`list`/`download` take `--sample N` (site/sample.py); `evict` frees verified raw files (site/evict.py).
"""

from __future__ import annotations

from typing import Any


def dispatch(a: Any) -> int:
    site = a.site.upper()
    products = [p.strip() for p in (a.products or "").split(",") if p.strip()] or None
    months = [m.strip() for m in (a.months or "").split(",") if m.strip()] or None
    sample = getattr(a, "sample", None)
    if sample is not None:
        if a.step not in ("list", "download"):
            raise SystemExit("--sample applies to `site list` and `site download` only")
        if months:
            raise SystemExit("--sample and --months are mutually exclusive")
        if sample < 1:
            raise SystemExit("--sample must be >= 1")
        from . import sample as samplemod

        run = samplemod.run_list if a.step == "list" else samplemod.run_download
        return run(site, sample, products=products, threads=a.threads)
    if a.step == "evict":
        from .evict import main
        return main(site, dry_run=bool(getattr(a, "dry_run", False)))
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
        return main(site, products=products, force=a.force, site_level=getattr(a, "site_level", None))
    if a.step == "validate":
        from .validate import main
        return main(site, accept=a.accept, site_level=getattr(a, "site_level", None))
    if a.step == "upload":
        from .upload import main
        return main(site, products=products, months=months, threads=a.threads)
    if a.step == "build-avus":
        from .avus import main
        return main(site, products=products, months=months)
    raise SystemExit(f"unknown step {a.step}")
