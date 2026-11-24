"""neon-ducklake command line. Run every command through scripts/with-secrets.sh."""

from __future__ import annotations

import argparse
import inspect
import json
import sys

BOOTSTRAP_PARTS = ("catalog", "ontology", "curation")
CHUNK_STAGE_NAMES = ("download", "stack", "avus", "upload", "apply", "history", "verify", "delete")


def _size(text: str) -> int:
    """argparse type for byte sizes: 8000000000, 8G, 8GB, 500M, 1.5T, 8GiB."""
    from .run import parse_size

    try:
        return parse_size(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _codes(values: list[str] | None) -> list[str] | None:
    """Site codes from repeatable, space- or comma-separated values (None when none given)."""
    out = [c.strip().upper() for v in values or [] for c in v.replace(",", " ").split() if c.strip()]
    return list(dict.fromkeys(out)) or None


def _recorder(a: argparse.Namespace) -> int:
    """``neon-ducklake recorder``: options are passed only when given, each checked against avu/recorder.main."""
    if a.interval < 1 or (a.max_rows is not None and a.max_rows < 1):
        raise SystemExit("--interval and --max-rows must be >= 1")
    if a.min_rows is not None and a.min_rows < 1:
        raise SystemExit("--min-rows must be >= 1 (use --flush to record everything now)")
    if a.max_age_h is not None and a.max_age_h <= 0:
        raise SystemExit("--max-age-h must be positive")
    if a.flush and (a.min_rows is not None or a.max_age_h is not None):
        raise SystemExit("--flush records everything pending now: drop --min-rows / --max-age-h")
    from .avu.recorder import main as run

    params = inspect.signature(run).parameters
    kw: dict[str, object] = {"watch": a.watch, ("interval_s" if "interval_s" in params else "interval"): a.interval}
    wanted: list[tuple[str, str, object]] = []  # (option, keyword, value)
    if a.max_rows is not None:  # else the recorder's default (honours $NEON_DUCKLAKE_ROWS_PER_BATCH)
        wanted.append(("--max-rows", "max_rows", a.max_rows))
    if a.min_rows is not None:
        wanted.append(("--min-rows", "min_rows", a.min_rows))
    if a.max_age_h is not None:
        if "max_age_h" not in params and "max_age_s" in params:
            wanted.append(("--max-age-h", "max_age_s", a.max_age_h * 3600.0))
        else:
            wanted.append(("--max-age-h", "max_age_h", a.max_age_h))
    if a.flush:
        wanted.append(("--flush", "flush", True))
    if a.dry_run:
        wanted.append(("--dry-run", "dry_run", True))
    for opt, key, value in wanted:
        if key not in params:
            raise SystemExit(f"this recorder has no {opt}")
        kw[key] = value
    return run(**kw)


def _journal(action: str, site: str, *, force: bool = False) -> int:
    """``neon-ducklake journal push|restore SITE``: journal_sync.push_site_journal / restore_site_journal."""
    from . import journal_sync

    if action == "push":
        if force:
            raise SystemExit("--force applies to `journal restore` only")
        out = journal_sync.push_site_journal(site)
        print(json.dumps(out, indent=1, default=str))
        return 1 if isinstance(out, dict) and out.get("error") else 0
    res = journal_sync.restore_site_journal(site, force=force)
    restored = bool(res.get("restored")) if isinstance(res, dict) else bool(res)
    if restored:
        print(f"{site}: journal restored from the Data Store backup")
        return 0
    print(f"{site}: journal not restored (no backup on the Data Store, or the local journal is current; "
          "--force restores anyway)")
    return 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="neon-ducklake", description=__doc__)
    ap.add_argument("--log-level", default="warning")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("preflight", help="read-only readiness checks").add_argument("--site")

    cat = sub.add_parser("catalog", help="Phase 1: harvest / seal / build the catalog lake")
    csub = cat.add_subparsers(dest="action", required=True)
    f = csub.add_parser("fetch")
    f.add_argument("--stages", default="catalog,locations,inventory")
    f.add_argument("--pilot", action="store_true")
    f.add_argument("--new", action="store_true", help="start a new run instead of resuming the latest incomplete one")
    f.add_argument("--run-id")
    s = csub.add_parser("seal")
    s.add_argument("--run-id")
    b = csub.add_parser("build")
    b.add_argument("--run-id")
    b.add_argument("--dry-run", action="store_true")
    b.add_argument("--allow-partial", action="store_true")

    sub.add_parser("enroll", help="create + enroll the iRODS project (in-process init_project)")
    m = sub.add_parser("mirror", help="upload code / sealed API pulls / catalog lake to iRODS")
    m.add_argument("--what", default="code,api,lake")
    m.add_argument("--run-id")
    m.add_argument("--threads", type=int, default=8)

    t = sub.add_parser("tag", help="write AVUs (bulk writer) and record MESA history")
    t.add_argument("scope", help="'catalog' or a site code")
    t.add_argument("--dry-run", action="store_true")
    t.add_argument("--apply-only", action="store_true", help="apply to iRODS, defer MESA recording")
    t.add_argument("--record-only", action="store_true", help="only record journaled applied rows")
    t.add_argument("--threads", type=int, default=8)
    t.add_argument("--rate", type=float, default=20.0, help="atomic metadata calls per second")
    t.add_argument("--products", help="comma list (dry runs)")
    t.add_argument("--months", help="comma list (dry runs)")

    st = sub.add_parser("site", help="Phase 2 steps for one site",
                        usage="%(prog)s STEP SITE [options]\n       %(prog)s chunk STAGE SITE --chunk ID")
    st.add_argument("step", choices=["plan", "list", "download", "stack", "terms", "cards", "curate", "validate",
                                     "upload", "build-avus", "evict", "restore", "chunk-plan", "chunk", "finalize",
                                     "cleanup"])
    st.add_argument("site", help="site code; for `site chunk` the chunk stage: " + ", ".join(CHUNK_STAGE_NAMES))
    st.add_argument("chunk_site", nargs="?", metavar="SITE", help="`site chunk STAGE SITE` only: the site code")
    st.add_argument("--chunk", metavar="ID", help="`site chunk`: the chunk id (stream_chunks.chunk_id)")
    st.add_argument("--chunk-bytes", type=_size, metavar="N",
                    help="chunk-plan: largest chunk (default $NEON_DUCKLAKE_CHUNK_BYTES or 8 GB; 8G, 500M, ...)")
    st.add_argument("--products", help="comma list of product codes (dry runs)")
    st.add_argument("--months", help="comma list of YYYY-MM (dry runs)")
    st.add_argument("--sample", type=int, metavar="N",
                    help="list/download: only the N most recent months per product (always the latest released "
                         "and latest PROVISIONAL month)")
    st.add_argument("--threads", type=int, default=8)
    st.add_argument("--accept", default="", help="curation: extra proposal ids or 'proposed'")
    st.add_argument("--force", action="store_true",
                    help="stack/curate: redo; restore: restore the journal backup even when the local one looks "
                         "current")
    lvl = st.add_mutually_exclusive_group()
    lvl.add_argument("--site-level", dest="site_level", action="store_true", default=None,
                     help="curate/validate: only the site-level (biome/feature) pass")
    lvl.add_argument("--no-site-level", dest="site_level", action="store_false",
                     help="curate/validate: only product curation")
    st.add_argument("--dry-run", action="store_true", help="evict: report what would be deleted, delete nothing")

    r = sub.add_parser("run", help="multi-site orchestrator: every Phase-2 stage per site, resumable")
    r.add_argument("sites", nargs="*", help="site codes (in this order)")
    r.add_argument("--all", dest="all_sites", action="store_true", help="every site of the catalog")
    r.add_argument("--habitat", help="Terrestrial | aquatic | 'Aquatic Wadeable Stream' | 'Aquatic Lake' | "
                                     "'Aquatic Non-wadeable Stream'")
    r.add_argument("--sample", type=int, metavar="N", help="probe: N most recent months per product (no evict)")
    r.add_argument("--stages", help="comma list of stages to run (default: all)")
    r.add_argument("--from-stage", help="re-run from this stage even when later stages are ok")
    r.add_argument("--no-evict", action="store_true", help="keep raw files on disk (skip the evict stage)")
    r.add_argument("--max-sites-in-flight", type=int, default=3)
    r.add_argument("--dry-run", action="store_true", help="print the plan and commands; run nothing")
    r.add_argument("--restack", action="store_true", help="re-download evicted inputs and force a full re-stack")
    r.add_argument("--order", choices=["aquatic-first", "size", "name", "given"],
                   help="site order (default: given for SITES, aquatic-first for --all/--habitat)")
    r.add_argument("--threads", type=int, help="passed to list/download/upload")
    r.add_argument("--stream", action=argparse.BooleanOptionalAction, default=True,
                   help="stream each site in bounded chunks: download, stack, AVUs, upload, apply, history, "
                        "verify, delete (default); --no-stream runs the previous whole-site stages")
    r.add_argument("--chunk-bytes", type=_size, metavar="N",
                   help="largest chunk (NEON_DUCKLAKE_CHUNK_BYTES for the children; default 8 GB)")
    r.add_argument("--workset-bytes", type=_size, metavar="N",
                   help="cap on raw bytes held locally by chunks (NEON_DUCKLAKE_WORKSET_BYTES; default 50 GB)")
    r.add_argument("--history-mode", choices=["direct", "spool"],
                   help="MESA history backend of the children (NEON_DUCKLAKE_HISTORY_MODE). Stream: always spool "
                        "(the recorder ingests it); direct is refused until DuckDB 2.0 multi-writer "
                        "(NEON_DUCKLAKE_MESA_MULTIWRITER=1). --no-stream: default direct")
    r.add_argument("--chunks-in-flight", type=int, default=2, metavar="N",
                   help="stream: chunks of one site between download and delete at once (default 2)")
    r.add_argument("--no-claims", dest="claims", action="store_false",
                   help="stream: do not take Data Store site claims (.claim/.done); only when no worker VM can "
                        "touch these sites")
    r.add_argument("--poll", type=float, default=5.0, help=argparse.SUPPRESS)

    ss = sub.add_parser("status", help="per-site stage table, chunk progress, spool backlog + totals; writes "
                                       "logs/status.json")
    ss.add_argument("sites", nargs="*")
    ss.add_argument("--json", action="store_true", help="print the JSON report instead of the table")

    w = sub.add_parser("worker", help="multi-VM worker: claim the next site on the Data Store, stream it, release")
    w.add_argument("--sites", nargs="+", metavar="SITE", help="only these sites (space- or comma-separated)")
    w.add_argument("--habitat", help="Terrestrial | aquatic | 'Aquatic Wadeable Stream' | 'Aquatic Lake' | "
                                     "'Aquatic Non-wadeable Stream'")
    w.add_argument("--max-sites", type=int, metavar="N", help="stop after N sites")
    w.add_argument("--chunk-bytes", type=_size, metavar="N", help="largest chunk (8G, 500M, ...)")
    w.add_argument("--workset-bytes", type=_size, metavar="N", help="working-set cap (50G, ...)")
    w.add_argument("--once", action="store_true", help="process at most one site, then exit")
    w.add_argument("--no-bootstrap", dest="bootstrap", action="store_false",
                   help="skip the start-up bootstrap (catalog lake, ontology cache, curation/generic)")

    rc_ = sub.add_parser("recorder", help="ingest spooled AVU history batches into MESA (run on the MESA-catalog "
                                          "host)")
    rc_.add_argument("--watch", action="store_true", help="keep polling the spool")
    rc_.add_argument("--interval", type=int, default=60, metavar="S", help="seconds between polls (--watch)")
    rc_.add_argument("--max-rows", type=int, metavar="N",
                     help="rows per MESA snapshot (default 250,000 or $NEON_DUCKLAKE_ROWS_PER_BATCH)")
    rc_.add_argument("--min-rows", type=int, metavar="N",
                     help="coalesce: record only once N spooled rows are pending (or --max-age-h passed), so the "
                          "900-snapshot MESA budget lasts (default: the recorder's own)")
    rc_.add_argument("--max-age-h", type=float, metavar="H",
                     help="coalesce: record anyway once the oldest pending batch is H hours old")
    rc_.add_argument("--flush", action="store_true",
                     help="record everything pending now, ignoring --min-rows/--max-age-h (end of a sweep)")
    rc_.add_argument("--dry-run", action="store_true", help="report what would be ingested; write nothing")

    cl = sub.add_parser("claims", help="list site/curation claims on the Data Store; --release SITE frees one")
    cl.add_argument("--release", metavar="SITE", action="append",
                    help="release this claim (a site code; also a product code or claim path); repeatable. "
                         "Our own or stale claims only, unless --force")
    cl.add_argument("--force", action="store_true", help="--release a live claim of another VM (it is gone)")
    cl.add_argument("--json", action="store_true", help="list as JSON")

    jn = sub.add_parser("journal", help="push or restore one site's journal backup on the Data Store "
                                        "(<IRODS_ROOT>/sites/<SITE>/.mesa-journal/)")
    jn.add_argument("action", choices=["push", "restore"])
    jn.add_argument("site", help="site code")
    jn.add_argument("--force", action="store_true",
                    help="restore: replace the local journal rows even when they look current")

    bs = sub.add_parser("bootstrap", help="fetch the catalog lake, seed the ontology cache, sync curation/generic")
    bs.add_argument("--what", default=",".join(BOOTSTRAP_PARTS),
                    help="comma list of: " + ", ".join(BOOTSTRAP_PARTS) + " (default: all)")
    bs.add_argument("--force", action="store_true", help="refresh even when the local copies look current")
    bs.add_argument("--json", action="store_true", help="print the result as JSON")

    v = sub.add_parser("verify")
    v.add_argument("scope", help="'catalog', 'mesa', or a site code")
    v.add_argument("--run-id")
    v.add_argument("--products", help="comma list (site scope)")
    v.add_argument("--months", help="comma list (site scope)")
    v.add_argument("--threads", type=int, default=4)
    sub.add_parser("recover", help="drain MESA pending pushes + reconcile batches")
    d = sub.add_parser("diff")
    d.add_argument("--from", dest="frm", type=int)
    d.add_argument("--to", type=int)

    a = ap.parse_args(argv)
    from .env import setup_logging

    setup_logging(a.log_level)

    if a.cmd == "preflight":
        from .preflight import main as run
        return run(site=a.site)
    if a.cmd == "catalog":
        if a.action == "fetch":
            from .harvest import main as run
            return run(set(a.stages.split(",")), pilot=a.pilot, resume=not a.new, rid=a.run_id)
        if a.action == "seal":
            from .seal import main as run
            return run(a.run_id)
        if a.action == "build":
            from .catalog_lake import main as run
            return run(a.run_id, dry_run=a.dry_run, allow_partial=a.allow_partial)
    if a.cmd == "enroll":
        from .enroll import main as run
        return run()
    if a.cmd == "mirror":
        from .mirror import main as run
        return run(set(a.what.split(",")), run_id=a.run_id, threads=a.threads)
    if a.cmd == "tag":
        from .avu.tag import main as run
        return run(a.scope, dry_run=a.dry_run, apply_only=a.apply_only, record_only=a.record_only,
                   threads=a.threads, rate=a.rate, products=a.products, months=a.months)
    if a.cmd == "site":
        from .site import dispatch
        return dispatch(a)
    if a.cmd == "run":
        from .run import main as run
        return run(a.sites, all_sites=a.all_sites, habitat=a.habitat, sample=a.sample, stages=a.stages,
                   from_stage=a.from_stage, evict=not a.no_evict, max_in_flight=a.max_sites_in_flight,
                   dry_run=a.dry_run, restack=a.restack, order=a.order, threads=a.threads, poll_s=a.poll,
                   stream=a.stream, chunk_bytes=a.chunk_bytes, workset_bytes=a.workset_bytes,
                   history_mode=a.history_mode, chunk_depth=a.chunks_in_flight, claims=a.claims)
    if a.cmd == "status":
        from .run import status_main as run
        return run(a.sites, as_json=a.json)
    if a.cmd == "worker":
        if a.max_sites is not None and a.max_sites < 1:
            raise SystemExit("--max-sites must be >= 1")
        from .worker import main as run
        kw = {"sites": _codes(a.sites), "habitat": a.habitat, "max_sites": a.max_sites, "chunk_bytes": a.chunk_bytes,
              "workset_bytes": a.workset_bytes, "once": a.once}
        if not a.bootstrap:
            if "bootstrap" not in inspect.signature(run).parameters:
                raise SystemExit("this worker has no --no-bootstrap")
            kw["bootstrap"] = False
        return run(**kw)
    if a.cmd == "recorder":
        return _recorder(a)
    if a.cmd == "claims":
        if a.force and not a.release:
            raise SystemExit("--force applies to --release SITE only")
        if a.json and a.release:
            raise SystemExit("--json lists the claims; it does not apply to --release")
        targets = [x.strip() if x.strip().startswith("/") else x.strip().upper() for x in a.release or [] if x.strip()]
        from .coord import claims_main as run  # claims_main(action, targets, *, force, as_json)
        return run("release" if targets else "list", targets or None, force=a.force, as_json=a.json)
    if a.cmd == "journal":
        return _journal(a.action, a.site.strip().upper(), force=a.force)
    if a.cmd == "bootstrap":
        what = {w.strip().lower() for w in a.what.split(",") if w.strip()}
        bad = sorted(what - set(BOOTSTRAP_PARTS))
        if bad or not what:
            raise SystemExit(f"bootstrap --what: unknown part(s) {', '.join(bad) or '(none)'}; choose from "
                             + ", ".join(BOOTSTRAP_PARTS))
        from .bootstrap import main as run
        params = inspect.signature(run).parameters
        kw = {k: True for k, on in (("force", a.force), ("as_json", a.json)) if on}
        missing = [k for k in kw if k not in params]
        if missing:
            raise SystemExit(f"this bootstrap has no {', '.join('--' + k.replace('as_', '') for k in missing)}")
        return run(what, **kw)
    if a.cmd == "verify":
        from .verify import main as run
        return run(a.scope, run_id=a.run_id, products=a.products, months=a.months, threads=a.threads)
    if a.cmd == "recover":
        from .avu.history import recover_main as run
        return run()
    if a.cmd == "diff":
        from .diff import main as run
        return run(a.frm, a.to)
    return 1


if __name__ == "__main__":
    sys.exit(main())
