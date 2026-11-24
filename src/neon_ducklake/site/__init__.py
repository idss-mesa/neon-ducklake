"""Phase 2 — one site subtree. `dispatch` routes `neon-ducklake site <step> <SITE>`.

`list`/`download` take `--sample N` (site/sample.py); `evict` frees verified raw files (site/evict.py).

Streaming (docs/STREAMING.md; site/chunks.py and site/stream.py):

- `site chunk-plan <SITE> [--chunk-bytes N] [--products ..] [--months ..]` -> ``chunks.plan_chunks``
- `site chunk <stage> <SITE> --chunk <ID>` -> ``stream.<stage>_chunk(site, chunk_id)`` for the stages
  download, stack, avus, upload, apply, history, verify, delete
- `site finalize <SITE>` -> ``stream.finalize_site(site)``; `site cleanup <SITE>` -> ``stream.cleanup_site(site)``
- `site chunk ...` and `site finalize` always spool their MESA history, like `run`: they default
  ``NEON_DUCKLAKE_HISTORY_MODE`` to ``spool`` and refuse ``direct`` without ``NEON_DUCKLAKE_MESA_MULTIWRITER=1``
- `site restore <SITE> [--force]` -> the first stream stage of every run of a site: restores the site's journal
  backup when ``journal_sync.needs_restore(site)`` (another VM pushed a newer one; ``--force`` always), then
  ``stream.restore_lake(site)`` (fetches the site lake from the Data Store when the local copy is missing or
  older, and re-plans the chunks it does not cover). Exit 0, 2 (a newer journal backup was not restored, or
  the lake restore failed) or 4 (``RC_RETRY``: retry later, e.g. the lake's catalog is being written)
- every stage's return code is its exit code; the orchestrator retries 4 (``run.RETRY_RC``) with backoff
"""

from __future__ import annotations

import contextlib
import importlib
import inspect
import os
from collections.abc import Iterator
from typing import Any

CHUNK_STAGES = ("download", "stack", "avus", "upload", "apply", "history", "verify", "delete")


def _mod(name: str) -> Any:
    """Import a sibling module lazily (through sys.modules, so tests can substitute it)."""
    return importlib.import_module(f"{__name__}.{name}")


def _rc(value: Any) -> int:
    return int(value or 0)


def _chunk_plan(site: str, a: Any, products: list[str] | None, months: list[str] | None) -> int:
    chunks = _mod("chunks")
    kw: dict[str, Any] = {"products": products, "months": months}
    max_bytes = getattr(a, "chunk_bytes", None)
    if max_bytes:
        kw["max_bytes"] = max_bytes
    plan = list(chunks.plan_chunks(site, **kw))
    total = sum(int(getattr(c, "bytes", 0) or 0) for c in plan)
    files = sum(int(getattr(c, "n_files", 0) or 0) for c in plan)
    products_n = len({getattr(c, "product", None) for c in plan})
    print(f"{site}: {len(plan)} chunk(s) over {products_n} product(s), {files} file(s), {total / 1e9:.1f} GB")
    for c in plan[:5]:
        print(f"  {c.chunk_id}  {int(c.n_files or 0)} file(s)  {int(c.bytes or 0) / 1e9:.2f} GB")
    if len(plan) > 5:
        print(f"  ... {len(plan) - 5} more")
    return 0


def _call(fn: Any, *args: Any, threads: int | None = None) -> int:
    """Call a stream stage, passing ``threads`` only to stages that take it."""
    try:
        takes = "threads" in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        takes = False
    return _rc(fn(*args, threads=threads) if takes and threads else fn(*args))


def _chunk(site: str, stage: str, chunk_id: str, threads: int | None = None) -> int:
    return _call(getattr(_mod("stream"), f"{stage}_chunk"), site, chunk_id, threads=threads)


@contextlib.contextmanager
def _stream_history() -> Iterator[None]:
    """Stream stages spool their history (docs/STREAMING.md "History backends"), also when run by hand: an
    unset ``NEON_DUCKLAKE_HISTORY_MODE`` means spool for the stage; direct needs the multi-writer switch."""
    from ..run import DIRECT_IN_STREAM, ENV_HISTORY_MODE, multiwriter_enabled

    mode = os.environ.get(ENV_HISTORY_MODE, "").strip().lower()
    if mode == "direct" and not multiwriter_enabled():
        raise SystemExit(DIRECT_IN_STREAM)
    if mode:
        yield
        return
    os.environ[ENV_HISTORY_MODE] = "spool"
    try:
        yield
    finally:
        os.environ.pop(ENV_HISTORY_MODE, None)


def _restore(site: str, *, force: bool = False) -> int:
    """`site restore SITE`: journal backup (when another VM pushed a newer one), then the site lake."""
    journal_sync = importlib.import_module("neon_ducklake.journal_sync")
    if force or journal_sync.needs_restore(site):
        res = journal_sync.restore_site_journal(site, force=force)
        restored = bool(res.get("restored")) if isinstance(res, dict) else bool(res)
        if not restored:
            print(f"{site}: the Data Store holds a newer journal backup but it was not restored; check it with "
                  f"`neon-ducklake journal restore {site}` (--force to replace the local journal)")
            return 2
        print(f"{site}: journal restored from the Data Store backup")
    else:
        print(f"{site}: local journal is current")
    return _rc(_mod("stream").restore_lake(site))


def dispatch(a: Any) -> int:
    step = a.step
    extra = getattr(a, "chunk_site", None)
    chunk_id = getattr(a, "chunk", None)
    stage = ""
    if step == "chunk":
        stage = a.site.strip().lower().removesuffix("_chunk")
        if stage not in CHUNK_STAGES:
            raise SystemExit(f"unknown chunk stage {a.site!r}; one of {', '.join(CHUNK_STAGES)} "
                             "(usage: site chunk STAGE SITE --chunk ID)")
        if not extra:
            raise SystemExit("usage: neon-ducklake site chunk STAGE SITE --chunk ID (the SITE is missing)")
        if not chunk_id:
            raise SystemExit("site chunk needs --chunk ID (see `neon-ducklake status` or stream_chunks.chunk_id)")
        site = extra.upper()
    else:
        if extra:
            raise SystemExit(f"unexpected argument {extra!r}: `site {step}` takes one SITE")
        if chunk_id:
            raise SystemExit("--chunk applies to `site chunk STAGE SITE` only")
        site = a.site.upper()
    if getattr(a, "chunk_bytes", None) and step != "chunk-plan":
        raise SystemExit("--chunk-bytes applies to `site chunk-plan` only (use `run --chunk-bytes`)")
    products = [p.strip() for p in (a.products or "").split(",") if p.strip()] or None
    months = [m.strip() for m in (a.months or "").split(",") if m.strip()] or None
    sample = getattr(a, "sample", None)
    if sample is not None:
        if step not in ("list", "download"):
            raise SystemExit("--sample applies to `site list` and `site download` only")
        if months:
            raise SystemExit("--sample and --months are mutually exclusive")
        if sample < 1:
            raise SystemExit("--sample must be >= 1")
        from . import sample as samplemod

        run = samplemod.run_list if step == "list" else samplemod.run_download
        return run(site, sample, products=products, threads=a.threads)
    if step == "chunk":
        with _stream_history():
            return _chunk(site, stage, chunk_id, getattr(a, "threads", None))
    if step == "chunk-plan":
        return _chunk_plan(site, a, products, months)
    if step == "finalize":
        with _stream_history():
            return _call(_mod("stream").finalize_site, site, threads=getattr(a, "threads", None))
    if step == "cleanup":
        return _rc(_mod("stream").cleanup_site(site))
    if step == "restore":
        return _restore(site, force=bool(getattr(a, "force", False)))
    if step == "evict":
        from .evict import main
        return main(site, dry_run=bool(getattr(a, "dry_run", False)))
    if step == "plan":
        from .plan import main
        return main(site)
    if step in ("list", "download"):
        from .fetch import main
        return main(step, site, products=products, months=months, threads=a.threads)
    if step == "stack":
        from .stack import main
        return main(site, products=products, force=a.force)
    if step == "terms":
        from .terms import main
        return main(site)
    if step == "cards":
        from .cards import main
        return main(site, products=products)
    if step == "curate":
        from .curate import main
        return main(site, products=products, force=a.force, site_level=getattr(a, "site_level", None))
    if step == "validate":
        from .validate import main
        return main(site, accept=a.accept, site_level=getattr(a, "site_level", None))
    if step == "upload":
        from .upload import main
        return main(site, products=products, months=months, threads=a.threads)
    if step == "build-avus":
        from .avus import main
        return main(site, products=products, months=months)
    raise SystemExit(f"unknown step {step}")
