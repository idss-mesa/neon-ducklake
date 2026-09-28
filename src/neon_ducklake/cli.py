"""neon-ducklake command line. Run every command through scripts/with-secrets.sh."""

from __future__ import annotations

import argparse
import sys


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

    st = sub.add_parser("site", help="Phase 2 steps for one site")
    st.add_argument("step", choices=["plan", "list", "download", "stack", "terms", "cards", "curate", "validate",
                                     "upload", "build-avus", "evict"])
    st.add_argument("site")
    st.add_argument("--products", help="comma list of product codes (dry runs)")
    st.add_argument("--months", help="comma list of YYYY-MM (dry runs)")
    st.add_argument("--sample", type=int, metavar="N",
                    help="list/download: only the N most recent months per product (always the latest released "
                         "and latest PROVISIONAL month)")
    st.add_argument("--threads", type=int, default=8)
    st.add_argument("--accept", default="", help="curation: extra proposal ids or 'proposed'")
    st.add_argument("--force", action="store_true")
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
    r.add_argument("--poll", type=float, default=5.0, help=argparse.SUPPRESS)

    ss = sub.add_parser("status", help="per-site stage table + totals; writes logs/status.json")
    ss.add_argument("sites", nargs="*")
    ss.add_argument("--json", action="store_true", help="print the JSON report instead of the table")

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
                   dry_run=a.dry_run, restack=a.restack, order=a.order, threads=a.threads, poll_s=a.poll)
    if a.cmd == "status":
        from .run import status_main as run
        return run(a.sites, as_json=a.json)
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
