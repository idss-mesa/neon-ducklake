"""``neon-ducklake tag <scope>``: build desired AVUs -> bulk-apply to iRODS -> record MESA history.

``scope`` is ``catalog`` (Phase-1 vocabulary, avu/vocab.py) or a site code (site/avus.py's
``build_desired``). ``--apply-only`` defers recording; ``--record-only`` records journaled applied rows
only; ``--dry-run`` computes the plan against iRODS (read-only) and writes nothing to iRODS or MESA.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

from .. import state

log = logging.getLogger("neon_ducklake.avu.tag")


def _split(v: str | list[str] | None) -> list[str] | None:
    if v is None:
        return None
    items = v.split(",") if isinstance(v, str) else list(v)
    out = [s.strip() for s in items if s and s.strip()]
    return out or None


def site_builder() -> Callable[..., Any]:
    """Lazily import ``neon_ducklake.site.avus.build_desired`` (written by the site pipeline)."""
    try:
        from ..site import avus
    except ImportError as exc:
        raise SystemExit(f"site AVU builder unavailable (neon_ducklake.site.avus): {exc}. "
                         "Run `neon-ducklake site build-avus <SITE>` once that module exists.") from exc
    fn = getattr(avus, "build_desired", None)
    if fn is None:
        raise SystemExit("neon_ducklake.site.avus has no build_desired(site, products, months)")
    return fn


def _show(title: str, d: dict[str, Any] | None) -> None:
    if not d:
        return
    print(f"== {title}")
    for k, v in d.items():
        if isinstance(v, list | dict):
            v = json.dumps(v, default=str)[:300]
        print(f"  {k:<22} {v}")


def main(scope: str, dry_run: bool = False, apply_only: bool = False, record_only: bool = False,
         threads: int = 8, rate: float = 20.0, products: str | list[str] | None = None,
         months: str | list[str] | None = None) -> int:
    from . import bulk, history

    key = "catalog" if scope.lower() == "catalog" else scope.upper()
    prods, mons = _split(products), _split(months)
    if apply_only and record_only:
        raise SystemExit("--apply-only and --record-only are mutually exclusive")
    con = state.connect()
    built = applied = recorded = None
    if not record_only:
        if key == "catalog":
            from .vocab import build_catalog_desired

            if prods or mons:
                log.warning("--products/--months ignored for scope 'catalog'")
            built = build_catalog_desired(None, con=con)
        else:
            res = site_builder()(key, prods, mons)
            built = res if isinstance(res, dict) else {"result": res}
        _show(f"desired ({key})", built)
        applied = bulk.apply_scope(key, threads=threads, rate=rate, dry_run=dry_run, con=con)
        _show("apply" + (" (dry run: nothing written)" if dry_run else ""), applied)
    if not (apply_only or dry_run):
        recorded = history.record_scope(key, con=con)
        _show("record (MESA history)", recorded)
    pending = con.execute("SELECT count(*) FROM avu_journal WHERE scope=? AND status='applied'",
                          (key,)).fetchone()[0]
    print(f"== {key}: journal rows applied but not yet recorded: {pending}")
    failed = bool(applied and (applied.get("errors") or applied.get("unit_failures"))) or \
        bool(recorded and (recorded.get("failed_batch") or recorded.get("blocked_rows")))
    return 1 if failed else 0
