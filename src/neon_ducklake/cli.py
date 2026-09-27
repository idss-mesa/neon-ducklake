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
                                     "upload", "build-avus"])
    st.add_argument("site")
    st.add_argument("--products", help="comma list of product codes (dry runs)")
    st.add_argument("--months", help="comma list of YYYY-MM (dry runs)")
    st.add_argument("--threads", type=int, default=8)
    st.add_argument("--accept", default="", help="curation: extra proposal ids or 'proposed'")
    st.add_argument("--force", action="store_true")

    v = sub.add_parser("verify")
    v.add_argument("scope", help="'catalog', 'mesa', or a site code")
    v.add_argument("--run-id")
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
    if a.cmd == "verify":
        from .verify import main as run
        return run(a.scope, run_id=a.run_id)
    if a.cmd == "recover":
        from .avu.history import recover_main as run
        return run()
    if a.cmd == "diff":
        from .diff import main as run
        return run(a.frm, a.to)
    return 1


if __name__ == "__main__":
    sys.exit(main())
