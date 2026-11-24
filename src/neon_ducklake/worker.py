"""``neon-ducklake worker``: one VM's loop over NEON sites (docs/STREAMING.md, "Worker").

    neon-ducklake worker [--sites S ...] [--habitat H] [--max-sites N] [--chunk-bytes N] [--workset-bytes N]
                         [--once] [--no-bootstrap]

Run it through ``scripts/with-secrets.sh`` on every CyVerse DE VM (docs/DE_SANDBOX.md). Each VM needs a
unique ``NEON_DUCKLAKE_VM_ID`` (default: the short host name); the worker registers it on the Data Store
(``coord.register_vm``) and refuses to start while another live worker uses the same id. The loop:

1. **bootstrap** (catalog lake, ontology cache, ``curation/generic``) from the Data Store;
2. **pick** the next site of the queue (the named sites in order, else aquatic sites first, then by size;
   ``--habitat`` filters). Sites with a ``.done`` marker, a live claim of another VM, or a failed attempt in
   this process are skipped;
3. **claim** it (``coord.site_claim``: a heartbeat thread renews the claim every 5 minutes; ``claim.lost``
   is set when it is lost). Then, before anything runs, the journal backup is restored when the Data Store
   holds a journal this host has not seen (``journal_sync.needs_restore``: another VM worked on the site
   since, or this host never did), and the site lake is brought up to date (``stream.restore_lake``);
   either failing fails the site attempt;
4. **run** ``run.run_sites([site], stream=True, history_mode='spool', stop_event=claim.lost, ...)``: the
   orchestrator stops when the claim is lost (taken over after an iRODS outage, or self-expired);
5. **finish**: push the journal (``journal_sync.push_site_journal``), write ``.done`` when the run succeeded
   (a cleanup that kept some local files, rc 3, counts as done with a warning), and release the claim.

History backend: ``spool`` (the central recorder) unless ``NEON_DUCKLAKE_WORKER_HISTORY_MODE=direct``, the
switch for DuckDB 2.0 multi-writer MESA catalogs once they exist (every VM then records into the catalog
itself); ``direct`` also needs ``NEON_DUCKLAKE_MESA_MULTIWRITER=1``. The history mode and the orchestrator's
configuration (``run.make_config``) are validated at start, before the VM registers or claims a site, so a
configuration error never leaves a claimed site behind. The worker sets ``NEON_DUCKLAKE_SITE_CLAIMS=on``
(``coord.holds_site`` is strict: every Data Store write of a site needs its live claim) and exports
``NEON_DUCKLAKE_VM_ID``, ``NEON_DUCKLAKE_HISTORY_MODE`` (that backend) and ``NEON_DUCKLAKE_CURATION_SYNC=on``
(unless already set) so stage subprocesses agree. SIGINT (Ctrl-C) or
SIGTERM interrupts the run; the journal is pushed and the claim released, so another VM can continue the
site. A claim lost during a run is neither released nor marked done, and the journal is not pushed over the
new owner's.

The claude CLI is optional: without it (``check_curator``, logged at start) a site's curate step reuses the
published generic curation results and leaves the other products uncurated; ``NEON_DUCKLAKE_CURATION_REQUIRE_CLI=1``
makes it a start requirement. A transient Data Store error while picking a site (a claim that another VM's
heartbeat is rewriting, a lock or network error) retries the pick (``pick_next``, up to
``NEON_DUCKLAKE_PICK_RETRY_S``); one while claiming a site tries that site again later (``CLAIM_ATTEMPTS``).

Exit codes: 0 every attempted site finished; 1 a site failed, the Data Store stayed unreachable while picking,
or bootstrap left no catalog lake; 130 interrupted.
"""

from __future__ import annotations

import contextlib
import inspect
import logging
import os
import signal
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

from . import coord, env, journal_sync

log = logging.getLogger("neon_ducklake.worker")

HISTORY_MODE_ENV = "NEON_DUCKLAKE_HISTORY_MODE"
CHUNK_BYTES_ENV = "NEON_DUCKLAKE_CHUNK_BYTES"
WORKSET_BYTES_ENV = "NEON_DUCKLAKE_WORKSET_BYTES"
INTERRUPTED = 130  # run.run_sites' return code after a signal
WORKER_HISTORY_ENV = "NEON_DUCKLAKE_WORKER_HISTORY_MODE"
MULTIWRITER_ENV = "NEON_DUCKLAKE_MESA_MULTIWRITER"
CLEANUP_KEPT_FILES = "partial"  # stream_sites cleanup status when cleanup_site returned 3
# pick_next: a Data Store error (a claim being rewritten, a lock or network error) retries the pass, with a pause
# growing from PICK_PAUSE_S to PICK_MAX_PAUSE_S, for up to $NEON_DUCKLAKE_PICK_RETRY_S (default 15 min)
PICK_RETRY_ENV = "NEON_DUCKLAKE_PICK_RETRY_S"
PICK_RETRY_S = 900.0
PICK_PAUSE_S = 5.0
PICK_MAX_PAUSE_S = 120.0
CLAIM_ATTEMPTS = 3  # a site whose claim attempt hit a transient Data Store error is tried this often in all


def multiwriter_enabled() -> bool:
    """``$NEON_DUCKLAKE_MESA_MULTIWRITER`` (run.multiwriter_enabled): the MESA catalog takes parallel writers."""
    try:
        from .run import multiwriter_enabled as enabled
    except ImportError:
        return os.environ.get(MULTIWRITER_ENV, "").strip().lower() in ("1", "true", "yes", "on")
    return bool(enabled())


def worker_history_mode(mode: str | None = None) -> str:
    """``mode``, else ``$NEON_DUCKLAKE_WORKER_HISTORY_MODE``, else ``spool``.

    ``direct`` needs ``NEON_DUCKLAKE_MESA_MULTIWRITER=1`` (the orchestrator refuses it in stream mode otherwise);
    the worker checks this at start, before it registers or claims anything (SystemExit)."""
    raw = (mode or os.environ.get(WORKER_HISTORY_ENV) or "spool").strip().lower()
    if raw not in ("spool", "direct"):
        raise SystemExit(f"{WORKER_HISTORY_ENV}={raw!r}: choose spool or direct")
    if raw == "direct" and not multiwriter_enabled():
        raise SystemExit(f"{WORKER_HISTORY_ENV}=direct needs {MULTIWRITER_ENV}=1 (DuckDB 2.0 multi-writer MESA "
                         "catalogs): stream runs refuse per-chunk direct history without it. Use spool (the "
                         "central recorder) or set the switch once the MESA catalog supports parallel writers.")
    return raw


def check_run_config(mode: str, *, chunk_bytes: int | None = None, workset_bytes: int | None = None) -> None:
    """Validate the orchestrator's configuration once, at worker start: ``run.make_config`` raises SystemExit
    on a bad combination, which must never happen inside ``work_site`` after a site was claimed."""
    try:
        from .run import make_config
    except ImportError:
        return
    kw: dict[str, Any] = {"stream": True, "history_mode": mode, "chunk_bytes": chunk_bytes,
                          "workset_bytes": workset_bytes}
    cfg = make_config(**{k: v for k, v in kw.items() if _accepts(make_config, k)})
    child_env = getattr(cfg, "child_env", None)
    if callable(child_env):
        child_env()  # parses the environment-derived settings (working-set cap, history mode) as the run will


def check_curator() -> str | None:
    """Why this VM cannot run the Opus curation (``site.curate.curator_unavailable``: no claude CLI), else None.

    The CLI is optional (docs/DE_SANDBOX.md): without it a site's curate step reuses the published generic
    results and leaves the other products uncurated. With ``NEON_DUCKLAKE_CURATION_REQUIRE_CLI=1`` the worker
    refuses to start instead (SystemExit, before it registers or claims anything)."""
    from .site import curate

    why = curate.curator_unavailable()
    if why and curate.cli_required():
        raise SystemExit(f"{why}, and {curate.REQUIRE_CLI_ENV}=1: install the claude CLI (or set "
                         f"NEON_DUCKLAKE_CLAUDE) before starting the worker")
    return why


def say(msg: str) -> None:
    print(f"{env.utcnow().strftime('%H:%M:%S')} [{coord.vm_id()}] {msg}", flush=True)


def plan_queue(sites: list[str] | None = None, *, habitat: str | None = None,
               universe: list[dict[str, Any]] | None = None) -> list[str]:
    """The site queue: named sites in the given order, else every site (``--habitat`` filter) aquatic-first."""
    from . import run

    codes = [c for c in (sites or []) if c and c.strip()]
    rows = run.select_sites(codes or None, all_sites=not codes and not habitat, habitat=habitat,
                            order="given" if codes else "aquatic-first", universe=universe)
    return [r["site"] for r in rows]


def _run_sites() -> Callable[..., int]:
    try:
        from .run import run_sites
    except ImportError as exc:
        raise SystemExit(f"neon_ducklake.run.run_sites is not available ({exc}); the streaming orchestrator is "
                         "required by the worker") from exc
    return run_sites


def _restore_lake() -> Callable[[str], int]:
    from .site import stream

    fn = getattr(stream, "restore_lake", None)
    if fn is None:
        raise RuntimeError("neon_ducklake.site.stream.restore_lake is not available; a site cannot be taken over "
                           "safely without it")
    return fn


def _accepts(fn: Callable[..., Any], name: str) -> bool:
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return name in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


@contextlib.contextmanager
def _sigterm_as_interrupt() -> Iterator[None]:
    """SIGTERM behaves like Ctrl-C (KeyboardInterrupt) so the claim is released on `kill` too."""
    def handler(signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt(f"signal {signum}")

    try:
        old = signal.signal(signal.SIGTERM, handler)
    except ValueError:  # not the main thread
        yield
        return
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old)


@contextlib.contextmanager
def _interrupt_when(event: threading.Event) -> Iterator[None]:
    """For an orchestrator without ``stop_event``: SIGINT this process when ``event`` is set."""
    done = threading.Event()

    def watch() -> None:
        while not done.is_set():
            if event.wait(0.5):
                if not done.is_set():
                    log.error("claim lost: interrupting the run")
                    os.kill(os.getpid(), signal.SIGINT)
                return

    t = threading.Thread(target=watch, name="claim-lost-watch", daemon=True)
    t.start()
    try:
        yield
    finally:
        done.set()
        t.join(timeout=2)


def _chunk_summary(site: str) -> dict[str, int]:
    from . import state

    con = state.connect()
    try:
        if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stream_chunks'").fetchone():
            return {}
        return {r[0]: r[1] for r in con.execute("SELECT status, count(*) FROM stream_chunks WHERE site=? GROUP BY 1",
                                                (site,))}
    finally:
        con.close()


def cleanup_warning(site: str) -> str | None:
    """A warning when the site finished everything but its cleanup kept some local files (rc 3), else None."""
    from . import state
    from .site import chunks

    con = state.connect()
    try:
        chunks.ensure_schema(con)
        steps = {r["step"]: r["status"] for r in con.execute("SELECT step, status FROM stream_sites WHERE site=?",
                                                             (site,))}
        if steps.get("cleanup") != CLEANUP_KEPT_FILES:
            return None
        if any(steps.get(s) != "ok" for s in chunks.SITE_STEPS if s != "cleanup"):
            return None
        left = con.execute("SELECT count(*) FROM stream_chunks WHERE site=? AND status != 'deleted'",
                           (site,)).fetchone()[0]
        if left:
            return None
        err = con.execute("SELECT error FROM stream_sites WHERE site=? AND step='cleanup'", (site,)).fetchone()[0]
        return f"cleanup kept some local files (rc 3): {err or 'see reports/cleanup.json'}"
    finally:
        con.close()


def _pick_retry_s() -> float:
    raw = (os.environ.get(PICK_RETRY_ENV) or "").strip()
    try:
        return float(raw) if raw else PICK_RETRY_S
    except ValueError as exc:
        raise SystemExit(f"{PICK_RETRY_ENV}={raw!r} is not a number of seconds") from exc


def pick_next(queue: list[str], *, skip: set[str], session: Any, vm: str, retry_s: float | None = None,
              pause_s: float | None = None) -> tuple[str | None, dict[str, Any]]:
    """The first site of ``queue`` not done, not live-claimed by another VM and not in ``skip``.

    A pass reads the ``.done`` markers and every site claim (``coord.site_claims``: settled reads, so a claim
    that another VM's heartbeat is rewriting is read again). A transient Data Store error in a pass
    (``coord.transient_error``: a lock or replica error, a network error, an unreadable claim) retries the
    whole pass with a growing pause for up to ``retry_s`` (``$NEON_DUCKLAKE_PICK_RETRY_S``, 15 min); after
    that, or for any other error, it propagates (the worker loop stops with a failure instead of crashing).
    """
    window = _pick_retry_s() if retry_s is None else max(0.0, retry_s)
    pause = PICK_PAUSE_S if pause_s is None else max(0.0, pause_s)
    deadline = time.monotonic() + window
    while True:
        try:
            return _pick_pass(queue, skip=skip, session=session, vm=vm)
        except Exception as exc:
            left = deadline - time.monotonic()
            if not coord.transient_error(exc) or left <= 0:
                raise
            wait = min(pause, left)
            log.warning("site pick: %s: %s; retrying the pass in %.1fs", type(exc).__name__, exc, wait)
            say(f"site pick failed ({type(exc).__name__}: {str(exc)[:160]}); retrying in {wait:.0f}s")
            time.sleep(wait)
            pause = min(max(pause * 2, 0.05), PICK_MAX_PAUSE_S)


def _pick_pass(queue: list[str], *, skip: set[str], session: Any, vm: str) -> tuple[str | None, dict[str, Any]]:
    done = coord.done_sites(session=session)
    claims = coord.site_claims(session=session)
    now = env.utcnow()
    info = {"done": len(done & set(queue)), "claimed_elsewhere": 0, "left": 0}
    choice = None
    for site in queue:
        if site in done or site in skip:
            continue
        c = claims.get(site)
        if c is not None and not c.is_stale(now) and not c.is_mine(vm):
            info["claimed_elsewhere"] += 1
            continue
        info["left"] += 1
        if choice is None:
            choice = site
    return choice, info


def prepare_site(site: str, *, session: Any) -> str | None:
    """Restore the journal (when the Data Store has one this host has not seen), then the site lake.

    Returns None when the site may run, else why not.
    """
    try:
        if journal_sync.needs_restore(site, session=session):
            rest = journal_sync.restore_detail(site, session=session)
            if rest.get("restored"):
                extra = "".join(f", {k}: {rest[k]}" for k in ("chunks", "steps") if rest.get(k))
                if rest.get("fallback"):
                    extra += (f"; the newest backup set was broken, restored the previous set {rest['fallback']} "
                              f"({len(rest.get('skipped') or [])} skipped)")
                say(f"{site}: journal restored from {rest.get('from_vm')} ({rest.get('pushedAt')}): "
                    f"{rest['tables']}{extra}")
    except Exception as exc:  # never run a site over a journal older than the Data Store's
        log.exception("%s: journal restore failed", site)
        return f"journal restore failed: {type(exc).__name__}: {exc}"[:300]
    try:
        rc = int(_restore_lake()(site) or 0)
    except Exception as exc:
        log.exception("%s: site lake restore failed", site)
        return f"site lake restore failed: {type(exc).__name__}: {exc}"[:300]
    if rc != 0:
        return f"site lake restore returned {rc}"
    return None


def work_site(site: str, claim: coord.Claim, *, session: Any, run_sites: Callable[..., int],
              chunk_bytes: int | None, workset_bytes: int | None, history_mode: str = "spool") -> int:
    """Run one claimed site (the caller releases the claim); pushes the journal unless the claim was lost."""
    t0 = time.monotonic()
    why = prepare_site(site, session=session)
    if why is not None:
        say(f"{site}: not run: {why}")
        return 1
    rc = 1
    interrupted = False
    kw: dict[str, Any] = {"stream": True, "history_mode": history_mode, "chunk_bytes": chunk_bytes,
                          "workset_bytes": workset_bytes}
    watch: contextlib.AbstractContextManager[None]
    if _accepts(run_sites, "stop_event"):
        kw["stop_event"] = claim.lost
        watch = contextlib.nullcontext()
    else:
        watch = _interrupt_when(claim.lost)
    try:
        with watch:
            rc = int(run_sites([site], **kw) or 0)
        interrupted = rc == INTERRUPTED and not claim.lost.is_set()  # the orchestrator caught a signal
    except KeyboardInterrupt:
        interrupted = not claim.lost.is_set()
        if interrupted:
            raise
    except Exception:  # one site's crash must not stop the worker
        log.exception("%s: run failed", site)
        rc = 1
    finally:
        secs = round(time.monotonic() - t0, 1)
        lost = claim.lost.is_set()
        if not lost:
            try:
                lost = not coord.claim_is_current(claim, session=session)
            except Exception as exc:  # noqa: BLE001 - unknown: the heartbeat decides (self-expiry)
                log.warning("%s: claim not re-read: %s", site, exc)
        if lost:
            say(f"{site}: claim lost during the run; not pushing the journal, not marking done")
            rc = 1
        else:
            try:
                journal = journal_sync.push_site_journal(site, session=session)
            except Exception as exc:  # noqa: BLE001 - reported; the claim is still released by the caller
                log.error("%s: journal push failed: %s", site, exc)
                journal = {"error": str(exc)[:300]}
                rc = rc or 1
            warning = cleanup_warning(site) if rc not in (0, INTERRUPTED) and not interrupted else None
            if warning:
                say(f"{site}: {warning}; counted as done")
                rc = 0
            if rc == 0 and not interrupted:
                coord.write_done(site, {"rc": rc, "seconds": secs, "chunks": _chunk_summary(site),
                                        "journal": journal.get("tables"), "claimStatus": claim.status,
                                        "warnings": [warning] if warning else []}, session=session)
                say(f"{site}: done in {secs:.0f}s" + (" (with a warning)" if warning else ""))
    return rc


def main(sites: list[str] | None = None, *, habitat: str | None = None, max_sites: int | None = None,
         chunk_bytes: int | None = None, workset_bytes: int | None = None, once: bool = False,
         bootstrap: bool = True, session: Any | None = None, run_sites: Callable[..., int] | None = None,
         universe: list[dict[str, Any]] | None = None, heartbeat_s: float | None = None,
         history_mode: str | None = None) -> int:
    """The worker loop (see the module docstring). ``session``/``run_sites``/``universe`` are for tests."""
    vm = env.vm_id()
    mode = worker_history_mode(history_mode)  # SystemExit before anything is registered or claimed
    check_run_config(mode, chunk_bytes=chunk_bytes, workset_bytes=workset_bytes)
    os.environ[env.VM_ID_ENV] = vm
    os.environ[HISTORY_MODE_ENV] = mode
    os.environ.setdefault(coord.CURATION_SYNC_ENV, "on")
    if chunk_bytes:
        os.environ[CHUNK_BYTES_ENV] = str(chunk_bytes)
    if workset_bytes:
        os.environ[WORKSET_BYTES_ENV] = str(workset_bytes)
    s = coord.get_session(session)
    say(f"worker starting on {coord.host()} (pid {os.getpid()}), Data Store {env.IRODS_ROOT}, history {mode}")
    no_curator = check_curator()
    if no_curator:
        say(f"{no_curator}: curation reuses the curation/generic results (Data Store and local) only; products "
            "without one are left uncurated")
    # a worker writes a site's shared state only under its claim: coord.holds_site is strict here and in the
    # stage subprocesses
    old_claims = os.environ.get(coord.SITE_CLAIMS_ENV)
    os.environ[coord.SITE_CLAIMS_ENV] = "on"
    try:
        reg = coord.register_vm(session=s, vm=vm)  # VmIdCollision (a SystemExit) when another live VM uses the id
        try:
            with coord.Heartbeat([reg], interval_s=heartbeat_s, session=s) as reg_hb:
                return _loop(sites, vm=vm, mode=mode, habitat=habitat, max_sites=max_sites,
                             chunk_bytes=chunk_bytes, workset_bytes=workset_bytes, once=once, bootstrap=bootstrap,
                             s=s, run_sites=run_sites, universe=universe, heartbeat_s=heartbeat_s, reg_hb=reg_hb)
        finally:
            if not reg.lost.is_set():
                try:
                    coord.release(reg, session=s)
                except Exception as exc:  # noqa: BLE001 - it expires after the TTL
                    log.error("VM registration not released: %s", exc)
    finally:
        if old_claims is None:
            os.environ.pop(coord.SITE_CLAIMS_ENV, None)
        else:
            os.environ[coord.SITE_CLAIMS_ENV] = old_claims


def _loop(sites: list[str] | None, *, vm: str, mode: str, habitat: str | None, max_sites: int | None,
          chunk_bytes: int | None, workset_bytes: int | None, once: bool, bootstrap: bool, s: Any,
          run_sites: Callable[..., int] | None, universe: list[dict[str, Any]] | None, heartbeat_s: float | None,
          reg_hb: coord.Heartbeat) -> int:
    if bootstrap:
        from . import bootstrap as boot

        results = boot.run(None, session=s)
        for w, r in results.items():
            say(f"bootstrap {w}: {r.get('status')}" + (f" ({r.get('error') or r.get('detail')})"
                                                       if r.get("error") or r.get("detail") else ""))
        if results.get("catalog", {}).get("status") in ("missing", "error") and not (
                (env.LAKE_DIR / env.CATALOG_LAKE).exists() or universe is not None):
            say("no catalog lake: cannot plan sites")
            return 1
    queue = plan_queue(sites, habitat=habitat, universe=universe)
    say(f"queue: {len(queue)} site(s)")
    runner = run_sites or _run_sites()
    attempted: set[str] = set()
    claim_errors: dict[str, int] = {}
    finished: list[str] = []
    failed: list[str] = []
    with _sigterm_as_interrupt():
        try:
            while True:
                if max_sites is not None and len(attempted) >= max_sites:
                    break
                if reg_hb.lost.is_set():
                    say("this VM's registration was lost (another worker with the same VM id?); stopping")
                    failed.append("(vm registration)")
                    break
                try:
                    site, info = pick_next(queue, skip=attempted, session=s, vm=vm)
                except Exception as exc:  # the Data Store stayed unreachable for the whole retry window
                    log.exception("site pick failed")
                    say(f"cannot pick a site ({type(exc).__name__}: {str(exc)[:200]}); stopping")
                    failed.append("(site pick)")
                    break
                if site is None:
                    say(f"nothing left to claim ({info['done']} done, {info['claimed_elsewhere']} claimed by other VMs)")
                    break
                attempted.add(site)
                held = False
                try:
                    with coord.site_claim(site, session=s, vm=vm, heartbeat_s=heartbeat_s) as claim:
                        held = True
                        say(f"{site}: claimed ({claim.status}); {info['left'] - 1} other site(s) left")
                        rc = work_site(site, claim, session=s, run_sites=runner, chunk_bytes=chunk_bytes,
                                       workset_bytes=workset_bytes, history_mode=mode)
                except coord.SiteUnavailable as exc:
                    say(f"{site}: not claimed ({exc})")
                    continue
                except Exception as exc:  # a Data Store error while claiming or finishing a site
                    log.exception("%s: claim or finish failed", site)
                    if held:  # work_site ran; its journal push or .done failed: the claim was released
                        say(f"{site}: failed after the run ({type(exc).__name__}: {str(exc)[:200]})")
                        rc = 1
                    else:
                        n = claim_errors[site] = claim_errors.get(site, 0) + 1
                        if n < CLAIM_ATTEMPTS and coord.transient_error(exc):
                            attempted.discard(site)  # try it again on a later pass
                            say(f"{site}: not claimed ({type(exc).__name__}: {str(exc)[:200]}); "
                                f"retried later ({n}/{CLAIM_ATTEMPTS})")
                            time.sleep(PICK_PAUSE_S)
                        else:
                            say(f"{site}: not claimed ({type(exc).__name__}: {str(exc)[:200]}); giving up")
                            failed.append(site)
                        continue
                if rc == INTERRUPTED:
                    raise KeyboardInterrupt
                (finished if rc == 0 else failed).append(site)
                if rc:
                    say(f"{site}: failed (rc {rc}); claim released, not retried by this worker")
                if once:
                    break
        except KeyboardInterrupt:
            say("interrupted: journal pushed and claim released for the site in progress")
            return 130
    say(f"worker finished: {len(finished)} site(s) done, {len(failed)} failed"
        + (f" ({', '.join(failed)})" if failed else ""))
    return 1 if failed else 0


def cli(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="neon-ducklake worker", description=__doc__.split("\n\n")[0])
    ap.add_argument("--sites", nargs="*", help="site codes, in this order (default: every site)")
    ap.add_argument("--habitat", help="Terrestrial | aquatic | one aquatic class")
    ap.add_argument("--max-sites", type=int)
    ap.add_argument("--chunk-bytes", type=int)
    ap.add_argument("--workset-bytes", type=int)
    ap.add_argument("--once", action="store_true", help="one site, then exit")
    ap.add_argument("--no-bootstrap", dest="bootstrap", action="store_false")
    a = ap.parse_args(argv)
    env.setup_logging("warning")
    return main(a.sites, habitat=a.habitat, max_sites=a.max_sites, chunk_bytes=a.chunk_bytes,
                workset_bytes=a.workset_bytes, once=a.once, bootstrap=a.bootstrap)


if __name__ == "__main__":
    import sys

    sys.exit(cli())
