"""`neon-ducklake verify catalog|mesa|<SITE>` dispatcher."""

from __future__ import annotations


def _split(v: str | None) -> list[str] | None:
    return [x.strip() for x in (v or "").split(",") if x.strip()] or None


def main(scope: str, run_id: str | None = None, products: str | None = None, months: str | None = None,
         threads: int = 4) -> int:
    if scope == "catalog":
        from .verify_catalog import main as run
        return run(run_id)
    if scope == "mesa":
        from .verify_mesa import main as run
        return run()
    from .verify_site import main as run
    return run(scope.upper(), products=_split(products), months=_split(months), threads=threads)
