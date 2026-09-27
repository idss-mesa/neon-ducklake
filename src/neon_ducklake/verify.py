"""`neon-ducklake verify catalog|mesa|<SITE>` dispatcher."""

from __future__ import annotations


def main(scope: str, run_id: str | None = None) -> int:
    if scope == "catalog":
        from .verify_catalog import main as run
        return run(run_id)
    if scope == "mesa":
        from .verify_mesa import main as run
        return run()
    from .verify_site import main as run
    return run(scope.upper())
