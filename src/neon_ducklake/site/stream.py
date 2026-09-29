"""Streaming stages (docs/STREAMING.md): fetch a bounded chunk, analyse it, tag it, push it, verify it, delete it.

Per chunk (``stream_chunks`` journal, :mod:`site.chunks`), each ``(site, chunk_id) -> int``:

=================  ===========================================================================================
``download_chunk``  only this chunk's rows (``fetch.download_rows``), waiting while the local working set (raw
                    files + site-lake data) plus the chunk would exceed ``NEON_DUCKLAKE_WORKSET_BYTES`` (rc 3
                    when it never fits in time)
``stack_chunk``     incremental append into the site lake (``stack.stack_chunk``) of the chunk's files on disk;
                    never reads earlier chunks, and refuses (rc 1) unless the local lake holds the
                    ``stream stack <SITE> <chunk>`` snapshots of every chunk stacked before (:func:`restore_lake`)
``avus_chunk``      NCBITaxon mapping of the chunk's taxa (``terms.resolve_chunk_taxa``; rc 4 while OLS leaves taxa
                    pending), then ``avus.build_desired(site, [product], months, levels={'month', 'object'})``
``upload_chunk``    this chunk's raw files, then the site lake (:func:`push_lake`: new ``data/**``, then the
                    catalog LAST under the catalog guard, as a versioned copy + ``CURRENT`` pointer, only while
                    this VM holds the site's claim; journaled in ``lake_pushes``)
``apply_chunk``     ``avu.bulk.apply_scope(site, only_units=<the chunk's month collections>)``
``history_chunk``   ``avu.history.record_scope(site)`` (direct or spool, ``NEON_DUCKLAKE_HISTORY_MODE``)
``verify_chunk``    good replica + checksum == ``dl_md5`` + core AVUs in iRODS; journal rows recorded or spooled;
                    the Data Store lake catalog is a recorded push that holds the chunk's stack snapshots, or
                    its snapshot list holds the chunk's stack snapshot (checked under the host lock
                    ``lake-push-<SITE>``, which :func:`push_lake` holds while it publishes)
``delete_chunk``    the chunk's local raw files, only after ``verify_chunk`` passed (``local_evicted = 1``), and
                    the superseded downloaded files of its months
=================  ===========================================================================================

Return codes: 0 done (or already done), 1 not ready (unknown chunk, an earlier stage missing, the chunk busy
in another process, the local lake lacks earlier chunks, the site's claim is lost; the status is left as it was),
2 the stage failed (status ``failed``, ``error`` = ``<stage>: <message>``), 3 blocked (the working set stayed full;
files a delete refused), 4 retry later (transient: OLS left taxa pending, the Data Store lake catalog is being
written; the status is left as it was). Every stage is idempotent: a chunk whose ``done`` status is at or past
the stage returns 0 at once (checked again once the chunk's lock is held); a failed stage re-runs from its own
start.

Per site: :func:`finalize_site` (after every chunk is deleted: terms, cards, curate, validate, the
site/container/product/lake/side AVUs, the finalize upload, apply, history, a metadata-only verify; each step
journaled in ``stream_sites`` and skipped once ok; not ready while another VM still curates one of the site's
products) and :func:`cleanup_site` (deletes the local lake ``data/**``, cards and curation after checking each
Data Store copy, and any superseded raw file left; reports, the lake catalog file and the journal stay).

The site lake in the Data Store: :func:`push_lake` after every chunk and at finalize; :func:`restore_lake`
fetches it back (a VM taking over the site, a lost local lake, a site cleaned up before a chunk was re-planned),
syncing only the data files the installed catalog references and only from good replicas, and re-plans the chunks
it does not cover; :func:`ensure_lake_data` restores a cleaned site's lake data before
any stage writes the lake (``site list``/``download``, stack, finalize).
"""

from __future__ import annotations

import contextlib
import inspect
import logging
import os
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .. import env, state
from . import chunks as chunkmod
from . import site_lake
from .chunks import FAILED, RANK, STAGE_STATUS, STATUSES, Chunk

log = logging.getLogger("neon_ducklake.site.stream")

RC_OK, RC_NOT_READY, RC_FAILED, RC_BLOCKED, RC_RETRY = 0, 1, 2, 3, 4
LAKE_PUSH_WAIT_S = 3600.0  # verify_chunk waits this long for a catalog push of this host, then asks to retry later
StageFn = Callable[[Chunk, sqlite3.Connection], tuple[int, dict[str, Any]]]


# ---------------------------------------------------------------- the stage wrapper

def _gate(ch: Chunk | None, site: str, chunk_id: str, stage: str) -> int | None:
    """RC_OK (already done), RC_NOT_READY (unknown / earlier stage missing) or None (the stage may run)."""
    target = STAGE_STATUS[stage]
    need = STATUSES[RANK[target] - 1]
    if ch is None:
        log.error("%s %s: unknown chunk %s (plan the site first)", stage, site, chunk_id)
        return RC_NOT_READY
    if RANK.get(ch.done, 0) >= RANK[target]:
        log.info("%s %s %s: already %s", stage, site, chunk_id, ch.done)
        return RC_OK
    if RANK.get(ch.done, 0) < RANK[need]:
        log.error("%s %s %s: not ready (reached %s; needs %s)", stage, site, chunk_id, ch.done, need)
        return RC_NOT_READY
    return None


def _run_stage(site: str, chunk_id: str, stage: str, fn: StageFn, *, con: sqlite3.Connection | None = None) -> int:
    site = site.upper()
    target = STAGE_STATUS[stage]
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        chunkmod.ensure_schema(con)
        gate = _gate(chunkmod.get_chunk(site, chunk_id, con=con), site, chunk_id, stage)
        if gate is not None:
            return gate
        held = contextlib.ExitStack()
        try:
            held.enter_context(env.file_lock(f"stream-{site}-{chunk_id}"))
        except SystemExit as exc:  # another process runs a stage of this chunk
            log.error("%s %s %s: %s", stage, site, chunk_id, exc)
            return RC_NOT_READY
        with held:
            # read again under the lock: another process may have run this stage since the first read
            ch = chunkmod.get_chunk(site, chunk_id, con=con)
            gate = _gate(ch, site, chunk_id, stage)
            if gate is not None:
                return gate
            assert ch is not None
            attempt = ch.attempt + 1 if ch.status == FAILED else 1
            chunkmod.set_status(con, site, chunk_id, ch.done, started=True, attempt=attempt)
            t0 = time.monotonic()
            try:
                rc, info = fn(ch, con)
            except (Exception, SystemExit) as exc:  # a stage failure is journaled, never raised
                log.exception("%s %s %s crashed", stage, site, chunk_id)
                rc, info = RC_FAILED, {"error": f"{type(exc).__name__}: {exc}"[:500]}
            info = {**info, "rc": rc, "seconds": round(time.monotonic() - t0, 1), "at": env.utcnow().isoformat()}
            if rc == RC_OK:
                chunkmod.set_status(con, site, chunk_id, target, done=target, finished=True, info={stage: info})
                log.info("%s %s %s: %s", stage, site, chunk_id, target)
            elif rc in (RC_NOT_READY, RC_RETRY):  # the chunk keeps its status, the reason is journaled
                what = "not ready" if rc == RC_NOT_READY else "retry later"
                msg = str(info.get("error") or what)[:500]
                cur = chunkmod.get_chunk(site, chunk_id, con=con)  # the stage may have re-planned it
                chunkmod.set_status(con, site, chunk_id, cur.status if cur else ch.status,
                                    error=f"{stage}: {what}: {msg}", finished=True, info={stage: info})
                log.error("%s %s %s %s: %s", stage, site, chunk_id, what, msg)
            else:
                msg = str(info.get("error") or f"rc {rc}")[:500]
                chunkmod.set_status(con, site, chunk_id, FAILED, error=f"{stage}: {msg}", finished=True,
                                    info={stage: info})
                log.error("%s %s %s failed (rc %d): %s", stage, site, chunk_id, rc, msg)
            return rc
    finally:
        if own:
            con.close()


def _month_units(site: str, ch: Chunk) -> list[str]:
    return [f"{env.IRODS_ROOT}/sites/{site}/raw/{ch.product}/{m}" for m in ch.months]


def _not_ok(con: sqlite3.Connection, site: str, ch: Chunk, column: str, ok: tuple[str, ...]) -> list[str]:
    """Names of the chunk's (non-superseded, downloaded unless ``column`` is dl_status) rows not in ``ok``."""
    from .evict import chunk_rows

    rows = chunk_rows(con, site, ch.product, ch.months)
    if column != "dl_status":
        rows = [r for r in rows if r["dl_status"] == "ok" and not r["evicted"]]
    return [r["name"] for r in rows if r[column] not in ok]


# ---------------------------------------------------------------- per-chunk stages

def ensure_lake_data(site: str, *, con: sqlite3.Connection | None = None, session: Any | None = None,
                     missing: str = "known", exclude: tuple[str, ...] = ()) -> str | None:
    """Restore the local site lake, data included, before a stage writes it (see :func:`_lake_data_rc`). Returns an
    error message, or None when the lake is usable."""
    return _lake_data_rc(site, con=con, session=session, missing=missing, exclude=exclude)[1]


def _lake_data_rc(site: str, *, con: sqlite3.Connection | None = None, session: Any | None = None,
                  missing: str = "known", exclude: tuple[str, ...] = ()) -> tuple[int, str | None]:
    """Bring the local site lake back from the Data Store, DATA included, before a stage writes (or reads) it.

    - After ``cleanup_site`` the lake's ``data/**`` is gone locally: DuckLake upserts and appends read the
      existing Parquet, so ``site list``/``download``, stack and finalize restore it first (``fetch_data=True``,
      which also clears the site's ``cleanup`` step; the orchestrator's cleanup then runs again);
    - without a local catalog, by ``missing``: ``always`` (download, stack: they must extend the Data Store's
      lake), ``known`` (``site list``/``download``: only when the journal knows the site has a lake in the Data
      Store, a ``lake_pushes`` row or a stacked chunk, so a new site simply starts one) or ``never``.

    Returns (rc, error message or None): rc 0 usable, else :func:`restore_lake`'s rc (4: retry later).
    ``exclude``: chunks whose lock the caller holds. The caller must not hold the site-lake lock.
    """
    site = site.upper()
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        chunkmod.ensure_schema(con)
        paths = site_lake.site_paths(site)
        cleaned = chunkmod.site_cleaned(site, con=con)
        lost = missing != "never" and not paths.catalog_file.exists()
        if lost and missing != "always":
            lost = bool(chunkmod.lake_pushes(site, con=con) or chunkmod.stacked_required(site, con=con))
        if not cleaned and not lost:
            return RC_OK, None
        say = log.warning if cleaned else log.info
        say("site %s: the local lake %s; restoring it (data included) from the Data Store", site,
            "was cleaned up" if cleaned else "is missing")
        rc = restore_lake(site, session=session, con=con, fetch_data=True, exclude=exclude)
        if rc:
            return rc, f"the site lake could not be restored from the Data Store (restore_lake rc {rc})"
        return RC_OK, None
    finally:
        if own:
            con.close()


def _ensure_lake(site: str, con: sqlite3.Connection, *, session: Any | None = None,
                 chunk_id: str | None = None) -> tuple[int, dict[str, Any]] | None:
    """:func:`ensure_lake_data` for a chunk stage (a missing local lake is always restored). None when the lake is
    usable, else the stage's (rc, info): 4 when the restore asked to retry later, else 1."""
    rc, why = _lake_data_rc(site, con=con, session=session, missing="always",
                            exclude=(chunk_id,) if chunk_id else ())
    if why is None:
        return None
    return (RC_RETRY if rc == RC_RETRY else RC_NOT_READY), {"error": why}


def download_chunk(site: str, chunk_id: str, *, threads: int = 8, workset_bytes: int | None = None,
                   timeout_s: float | None = None, poll_s: float | None = None, session: Any | None = None) -> int:
    """Download only this chunk's rows, under the working-set cap (rc 3 when it stays full)."""
    from . import fetch

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        blocked = _ensure_lake(site.upper(), con, session=session, chunk_id=ch.chunk_id)  # the download writes
        if blocked:  # the lake's files table
            return blocked
        stale = stale_raw_rows(con, site.upper(), ch)  # 'ok' rows whose local file is missing, resized or changed
        if stale:
            _mark_stale(con, site.upper(), stale)
            _unlink_stale(stale)
            log.warning("download %s %s: %d raw file(s) on disk do not match the journal (size/md5): downloading "
                        "them again", site, ch.chunk_id, len(stale))
        rc = fetch.download_rows(site.upper(), ch.product, ch.months, threads=threads, cap=workset_bytes,
                                 timeout_s=timeout_s, poll_s=poll_s)
        if rc == fetch.RC_WORKSET_TIMEOUT:
            return RC_BLOCKED, {"error": "the local working set stayed above NEON_DUCKLAKE_WORKSET_BYTES"}
        missing = _not_ok(con, site.upper(), ch, "dl_status", ("ok",))
        if rc == 0 and missing:
            rc = RC_FAILED
        info: dict[str, Any] = {"notDownloaded": len(missing), "sample": missing[:5]}
        if rc:
            info["error"] = f"{len(missing)} file(s) not downloaded (see reports/download_summary.json)"
            rc = RC_FAILED
        return rc, info

    return _run_stage(site, chunk_id, "download", fn)


def stack_chunk(site: str, chunk_id: str, *, force: bool = False, session: Any | None = None) -> int:
    """Append this chunk to the site lake (``stack.stack_chunk``).

    A cleaned-up site is restored from the Data Store first. Not ready (rc 1, nothing written) while the local
    lake lacks the snapshots of a chunk stacked earlier: run :func:`restore_lake`.
    """
    from . import stack

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        blocked = _ensure_lake(site.upper(), con, session=session, chunk_id=ch.chunk_id)
        if blocked:
            return blocked
        required = chunkmod.stacked_required(site, con=con, exclude=[ch.chunk_id])
        res = stack.stack_chunk(site.upper(), ch.product, ch.months, chunk_id=ch.chunk_id, force=force,
                                required_chunks=required)
        info = {"status": res["status"], "files": res.get("files"), "unitsBuilt": res.get("unitsBuilt", 0),
                "unitsSkipped": res.get("unitsSkipped", 0), "errors": res.get("errors", [])[:5],
                "lastSnapshot": res.get("lastSnapshot"),
                "widened": {r["table"]: r["widened"] for r in res.get("results", []) if r.get("widened")}}
        if res["status"] in ("ok", "empty"):
            return RC_OK, info
        info["error"] = "; ".join(f"{e.get('unit')}: {e.get('error')}" for e in res.get("errors", [])[:3])
        if res["status"] == "not_ready":
            return RC_NOT_READY, info
        return RC_FAILED, info

    return _run_stage(site, chunk_id, "stack", fn)


def avus_chunk(site: str, chunk_id: str) -> int:
    """NCBITaxon terms of the chunk's taxa, then the desired AVUs of its month collections and data objects.

    The taxa are resolved here (``terms.resolve_chunk_taxa``: OLS lookups cached in the ontology store) because
    the object level is built per chunk only: without it the chunk's objects would carry ``neon.taxon``
    placeholders for every taxon the ontology store has not seen yet. Taxa left pending (OLS unavailable) end the
    stage with rc 4 (retry later; the chunk keeps its status) instead of tagging placeholders for good.
    """
    from . import avus, terms

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        tx = terms.resolve_chunk_taxa(site.upper(), ch.product, ch.months)
        if tx.get("pending"):
            return RC_RETRY, {"taxa": tx, "error": f"{tx['pending']} taxon mapping(s) pending (OLS unavailable); "
                                                   "retry the stage later"}
        s = avus.build_desired(site.upper(), [ch.product], ch.months, levels=avus.CHUNK_LEVELS, con=con)
        return RC_OK, {"taxa": tx, **{k: s.get(k) for k in ("rows", "units_built", "months_built", "rows_deleted")}}

    return _run_stage(site, chunk_id, "avus", fn)


def upload_chunk(site: str, chunk_id: str, *, threads: int = 8, session: Any | None = None) -> int:
    """Push this chunk's raw files, then the site lake (:func:`push_lake`: its new data files and the catalog)."""
    from . import upload

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        res = upload.upload_chunk(site.upper(), ch.product, ch.months, threads=threads)
        missing = _not_ok(con, site.upper(), ch, "up_status", ("uploaded", "present"))
        info: dict[str, Any] = {"items": res["items"], "failures": res["failures"], "notUploaded": len(missing),
                                "sample": missing[:5]}
        if res["failures"] or missing:
            info["error"] = f"{res['failures']} upload failure(s), {len(missing)} file(s) not in iRODS"
            return RC_FAILED, info
        lake = upload.push_lake(site.upper(), threads=threads, session=session)
        info["lake"] = {k: lake.get(k) for k in ("snapshotId", "catalogMd5", "dataFiles", "dataPushed", "refused",
                                                 "retry", "claimLost", "skipped", "failures", "error") if k in lake}
        if lake.get("claimLost"):  # coord.holds_site said no: nothing was written; the new owner continues
            info["error"] = str(lake["claimLost"])
            return RC_NOT_READY, info
        if lake.get("retry"):
            info["error"] = f"site lake not pushed yet: {lake['refused']}"
            return RC_RETRY, info
        if lake.get("refused"):
            info["error"] = f"site lake not pushed: {lake['refused']}"
            return RC_FAILED, info
        if lake.get("failures"):
            info["error"] = f"site lake push: {lake['failures']} failure(s)"
            return RC_FAILED, info
        return RC_OK, info

    return _run_stage(site, chunk_id, "upload", fn)


def apply_chunk(site: str, chunk_id: str, *, threads: int = 8, rate: float = 20.0, session: Any | None = None) -> int:
    """Apply the desired AVUs of this chunk's month collections (and their objects) to iRODS."""
    from ..avu import bulk

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        bulk.ensure_desired(con)
        units = [u for u in _month_units(site.upper(), ch) if con.execute(
            "SELECT 1 FROM avu_desired WHERE scope=? AND unit_id=? LIMIT 1", (site.upper(), u)).fetchone()]
        if not units:
            return RC_FAILED, {"error": "no desired AVUs for the chunk's months (run avus_chunk)"}
        st = bulk.apply_scope(site.upper(), only_units=units, threads=threads, rate=rate, con=con, session=session)
        info = {k: st.get(k) for k in ("units", "targets", "adds", "removes", "noops", "errors", "missing",
                                       "unit_failures")}
        if st.get("errors") or st.get("unit_failures") or st.get("missing"):
            info["error"] = (f"{st.get('errors', 0)} error(s), {st.get('unit_failures', 0)} unit failure(s), "
                             f"{st.get('missing', 0)} target(s) missing in iRODS")
            return RC_FAILED, info
        return RC_OK, info

    return _run_stage(site, chunk_id, "apply", fn)


def history_chunk(site: str, chunk_id: str) -> int:
    """Record the site's applied AVU journal rows in MESA history (direct, or spooled for the recorder)."""
    from ..avu import history

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        out = history.record_scope(site.upper(), con=con) or {}
        info = {k: out.get(k) for k in ("batches_planned", "batches_recorded", "rows_recorded", "blocked_rows",
                                        "batches_spooled", "rows_spooled", "mode") if k in out}
        if out.get("error") or out.get("failed_batch") or out.get("blocked_rows"):
            info["error"] = str(out.get("error") or f"{out.get('blocked_rows')} row(s) blocked by a pending batch")
            return RC_FAILED, info
        return RC_OK, info

    return _run_stage(site, chunk_id, "history", fn)


def verify_chunk(site: str, chunk_id: str, *, session: Any | None = None) -> int:
    """Check this chunk in iRODS (replicas, checksums, core AVUs) and its AVU journal rows (recorded/spooled)."""
    from . import evict

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        sess = session if session is not None else env.mesa().session
        res = evict.verify_chunk_objects(con, site.upper(), ch.product, ch.months, session=sess)
        lake = _lake_push_problem(site.upper(), ch, con, sess)
        lake_rc, lake_err = lake if lake else (RC_OK, None)
        if lake_err:
            res["ok"] = False
            res["failures"] = {**res["failures"], "site lake": 1}
            res["samples"] = {**res["samples"], "site lake": [lake_err]}
        info = {"ok": res["ok"], "files": res["files"], "good": res["good"], "failures": res["failures"],
                "samples": {k: v[:3] for k, v in res["samples"].items()}, "verifiedAt": env.utcnow().isoformat()}
        if not res["ok"]:
            info["error"] = "; ".join(f"{k}: {n}" if k != "site lake" else f"site lake: {lake_err}"
                                      for k, n in res["failures"].items())
            if lake_rc == RC_RETRY and set(res["failures"]) == {"site lake"}:
                return RC_RETRY, info  # only the lake check, and the Data Store catalog is being written
            return RC_FAILED, info
        return RC_OK, info

    return _run_stage(site, chunk_id, "verify", fn)


def delete_chunk(site: str, chunk_id: str) -> int:
    """Delete this chunk's local raw files (only after ``verify_chunk`` passed) and flag them evicted."""
    from . import evict

    def fn(ch: Chunk, con: sqlite3.Connection) -> tuple[int, dict[str, Any]]:
        ver = ch.info.get("verify") or {}
        if not ver.get("ok") or not ver.get("verifiedAt"):
            return RC_BLOCKED, {"error": "no passing verify_chunk record"}
        res = evict.delete_chunk_files(con, site.upper(), ch.product, ch.months, verified_at=ver["verifiedAt"])
        info = {k: res[k] for k in ("files", "deleted", "bytes", "already", "flagged", "blocked", "errors",
                                    "superseded", "supersededBytes") if k in res}
        if res["blocked"] or res["errors"]:
            info["error"] = "; ".join([*(f"{k}: {n}" for k, n in res["blocked"].items()), *res["errors"][:3]])
            info["blockedSamples"] = {k: v[:3] for k, v in res["blockedSamples"].items()}
            return RC_BLOCKED, info
        log.info("delete %s %s: %d file(s), %.2f GB freed", site, chunk_id, res["deleted"], res["bytes"] / 1e9)
        return RC_OK, info

    return _run_stage(site, chunk_id, "delete", fn)


def _lake_chunk_snapshots(site: str) -> dict[str, int]:
    """chunk id -> last ``stream stack`` snapshot id in the local lake ({} without a lake). Takes the lake lock."""
    paths = site_lake.site_paths(site)
    with env.file_lock(f"site-lake-{site}", blocking=True):
        if not paths.catalog_file.exists():
            return {}
        return _ids_of(site_lake.catalog_snapshots(paths.catalog_file, site), site)


def _ids_of(snaps: list[site_lake.Snapshot], site: str) -> dict[str, int]:
    prefix = f"stream stack {site} "
    out: dict[str, int] = {}
    for sid, _t, msg in snaps:
        if msg.startswith(prefix) and (cid := msg[len(prefix):].split(" ", 1)[0]):
            out[cid] = max(out.get(cid, 0), sid)
    return out


def _chunk_last_snapshot(site: str, ch: Chunk) -> int | None:
    """The newest lake snapshot of the chunk's stack (from its stack record, else from the lake)."""
    st = ch.info.get("stack") if isinstance(ch.info, dict) else None
    if isinstance(st, dict) and "lastSnapshot" in st:
        return None if st["lastSnapshot"] is None else int(st["lastSnapshot"])
    return _lake_chunk_snapshots(site).get(ch.chunk_id)


def _lake_push_problem(site: str, ch: Chunk, con: sqlite3.Connection, session: Any) -> tuple[int, str] | None:
    """Why the chunk's stacked rows are not safe in the Data Store yet, as (rc, message); None when they are.

    Safe when the catalog the Data Store serves (``upload.current_catalog``) is a push this host journaled in
    ``lake_pushes`` that covers the chunk's last stack snapshot, or when that catalog's snapshot list holds the
    chunk's stack snapshot (a later push of the same lake). The check runs under the host lock
    ``lake-push-<SITE>`` that :func:`push_lake` holds from its catalog publish to its ``lake_pushes`` row, so the
    verify of chunk k never sees the push of chunk k+1 half done. rc 4 (retry) when the Data Store catalog is being
    written and nothing readable proves the chunk; rc 2 otherwise."""
    from . import upload

    need = _chunk_last_snapshot(site, ch)
    if need is None:
        return None  # nothing of this chunk was committed to the lake
    paths = site_lake.site_paths(site)
    with env.file_lock(f"site-lake-{site}", blocking=True):
        snaps = site_lake.catalog_snapshots(paths.catalog_file, site) if paths.catalog_file.exists() else []
    local = {site_lake.snapshot_key(x) for x in snaps}
    prefix = f"stream stack {site} {ch.chunk_id} "
    need_key = next((site_lake.snapshot_key(x) for x in snaps if x[0] == need and x[2].startswith(prefix)), None)
    held = contextlib.ExitStack()
    try:
        held.enter_context(env.file_lock(f"lake-push-{site}", blocking=True, timeout_s=LAKE_PUSH_WAIT_S))
    except SystemExit as exc:  # a push of this host has been publishing for that long: check again later
        return RC_RETRY, f"a catalog push of this host still holds the lake-push lock ({exc})"[:500]
    with held:
        rows = [r for r in chunkmod.lake_pushes(site, con=con) if r["snapshot_id"] is not None
                and int(r["snapshot_id"]) >= need and r.get("snapshot_key") in local]
        cur = upload.current_catalog(session, site)
        if not cur.exists:
            return RC_FAILED, "the Data Store has no catalog of the site lake"
        if cur.md5 and cur.md5 in {r["catalog_md5"] for r in rows}:
            return None
        fetched = ""
        if need_key:
            tmp = paths.tmp_dir / f"verify_{os.getpid()}_{paths.catalog_file.name}"
            try:
                got = upload.fetch_current_catalog(session, site, tmp)
                if need_key in {site_lake.snapshot_key(x) for x in got.snapshots or []}:
                    log.info("verify %s %s: the Data Store catalog (%s, md5 %s) holds the chunk's stack snapshot %s",
                             site, ch.chunk_id, got.source, got.md5, need)
                    return None
                fetched = f"; the Data Store catalog ({got.source}, md5 {got.md5}) lacks snapshot {need}"
            except (upload.NoCatalog, FileNotFoundError) as exc:
                fetched = f"; {exc}"
            finally:
                tmp.unlink(missing_ok=True)
                tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
        if cur.mid_write or not cur.md5:
            return RC_RETRY, (f"the Data Store catalog of the site lake is being written ({cur.source}); retry later"
                              f"{fetched}")[:500]
        if not rows:
            return RC_FAILED, (f"the site lake with this chunk (snapshot {need}) was never pushed to the Data Store "
                               f"(lake_pushes; re-run upload_chunk){fetched}")[:500]
        return RC_FAILED, (f"the Data Store lake catalog (md5 {cur.md5}) is not a push of this host that holds the "
                           f"chunk (snapshot {need}){fetched}")[:500]


CHUNK_STAGES: dict[str, Callable[..., int]] = {
    "download": download_chunk, "stack": stack_chunk, "avus": avus_chunk, "upload": upload_chunk,
    "apply": apply_chunk, "history": history_chunk, "verify": verify_chunk, "delete": delete_chunk,
}


def run_chunk(site: str, chunk_id: str, *, stages: list[str] | None = None, **kw: Any) -> int:
    """Run the chunk's stages in order (all, or ``stages``) and stop at the first non-zero rc.

    Keyword arguments reach the stages that accept them (``threads``, ``session``, ``workset_bytes`` ...).
    """
    for stage in stages or list(CHUNK_STAGES):
        fn = CHUNK_STAGES[stage]
        accepted = inspect.signature(fn).parameters
        rc = fn(site, chunk_id, **{k: v for k, v in kw.items() if k in accepted})
        if rc:
            return rc
    return RC_OK


# ---------------------------------------------------------------- the site lake in the Data Store

def push_lake(site: str, *, threads: int = 8, session: Any | None = None) -> int:
    """Push the site lake: checkpoint, new ``data/**`` files, then the catalog LAST (a versioned copy, the
    ``CURRENT`` pointer, the canonical copy); the pushed catalog's md5, time and snapshot id go to ``lake_pushes``.
    0 pushed (or nothing to push), 1 refused by the catalog guard (another VM pushed a lake this host does not
    contain: :func:`restore_lake`) or the site's claim is lost, 2 an upload failed, 4 retry later (the Data Store
    catalog is being written)."""
    from . import upload

    res = upload.push_lake(site.upper(), threads=threads, session=session)
    if res.get("claimLost"):
        return RC_NOT_READY
    if res.get("retry"):
        return RC_RETRY
    if res.get("refused"):
        return RC_NOT_READY
    return RC_FAILED if res.get("failures") else RC_OK


def _catalog_behind(session: Any, site: str, cur: Any, con: sqlite3.Connection,
                    tmp: Path) -> tuple[bool, str, Any]:
    """(replace the local catalog by the Data Store copy, why, the fetched ``upload.RemoteLake`` or None).
    Call with the site-lake lock held; ``tmp`` receives the Data Store catalog when it had to be fetched."""
    from ..mirror import md5_of
    from . import upload

    paths = site_lake.site_paths(site)
    if not paths.catalog_file.exists():
        return True, "no local lake", None
    lake = site_lake.open_site_lake(site)
    try:
        site_lake.checkpoint_detach(lake, site)
    finally:
        lake.close()
    local_md5 = md5_of(paths.catalog_file).lower()
    if cur.md5 and cur.md5 == local_md5:
        return False, "the local catalog is the Data Store copy", None
    local = site_lake.catalog_snapshots(paths.catalog_file, site)
    last = chunkmod.last_lake_push(site, con=con, local_keys={site_lake.snapshot_key(x) for x in local})
    if cur.md5 and last and last["catalog_md5"] == cur.md5:
        return False, f"the Data Store holds the catalog this lake last {last['kind']}ed (it is the same or newer)", None
    got = upload.fetch_current_catalog(session, site, tmp)
    rel = site_lake.lineage(local, got.snapshots or [])
    return rel in ("remote_ahead", "diverged"), f"local vs Data Store ({got.source}): {rel}", got


class LakeDataUnavailable(RuntimeError):
    """A data file the catalog being installed references is neither on this host nor in a good Data Store replica."""


def _sync_lake_data(session: Any, site: str, catalog: Path, remote: dict[str, Any], data_coll: str) -> dict[str, Any]:
    """Bring the local ``data/**`` files that ``catalog`` (the catalog being installed) references up to the Data
    Store. ``remote``: ``upload.good_objects`` of ``data_coll``, so only good replicas (status 1, size > 0, a
    checksum) are ever read; a remote object the catalog does not reference is never fetched (an interrupted push
    leaves its data file registered, with a stale replica, before any catalog refers to it).

    - a local file of the size the catalog records is kept as is;
    - a missing local file is fetched from its good replica when that replica has the catalog's size
      (``upload.fetch_verified``: size and checksum checked on ``<file>.part`` before it lands);
    - a local file of another size is replaced only by such a verified download; without a good replica of the
      catalog's size (or when the catalog records no size) it is kept and logged (``keptLocal``);
    - a missing file without a usable good replica, or whose download fails, is ``unavailable``.

    Returns {referenced, fetched, replaced, keptLocal, unavailable, remoteUnreferenced}."""
    from . import upload

    paths = site_lake.site_paths(site)
    refs = site_lake.catalog_data_files(catalog)
    out: dict[str, Any] = {"referenced": len(refs), "fetched": 0, "replaced": [], "keptLocal": [], "unavailable": [],
                           "remoteUnreferenced": len(set(remote) - {f"{data_coll}/{r}" for r in refs})}
    for rel, size in sorted(refs.items()):
        local = paths.data_dir / rel
        obj = remote.get(f"{data_coll}/{rel}")
        have = local.stat().st_size if local.is_file() else None
        usable = obj is not None and (size is None or obj.size == size)
        want = size if size is not None else obj.size if obj is not None else None
        if have is not None:
            if want is None or have == want:
                continue  # the file the catalog references (nothing says otherwise)
            if size is None or not usable:
                log.warning("restore lake %s: local data file %s has %d bytes, the catalog records %s; no good Data "
                            "Store replica of that size (%s): the local file is kept", site, rel, have, size,
                            f"{obj.size} bytes" if obj is not None else "none")
                out["keptLocal"].append(rel)
                continue
        elif not usable:
            log.warning("restore lake %s: data file %s is missing here and has no good Data Store replica%s", site,
                        rel, f" of {size} bytes (it has {obj.size})" if obj is not None else "")
            out["unavailable"].append(rel)
            continue
        try:
            upload.fetch_verified(session, obj, local)
        except Exception as exc:  # noqa: BLE001 - reported per file
            log.warning("restore lake %s: data file %s could not be fetched (%s: %s); %s", site, rel,
                        type(exc).__name__, exc, "the local file is kept" if have is not None else "unavailable")
            out["keptLocal" if have is not None else "unavailable"].append(rel)
            continue
        out["fetched"] += 1
        if have is not None:
            log.warning("restore lake %s: local data file %s had %d bytes, the catalog records %d: replaced by the "
                        "verified Data Store copy", site, rel, have, size)
            out["replaced"].append(rel)
    return out


def _cached_md5(con: sqlite3.Connection, path: Path, st: os.stat_result) -> str:
    """md5 of a local file, cached in ``mirror_md5`` by (path, size, mtime) like ``mirror.push_tree``."""
    from ..mirror import MD5_DDL, md5_of

    con.execute(MD5_DDL)
    row = con.execute("SELECT size, mtime_ns, md5 FROM mirror_md5 WHERE local_path=?", (str(path),)).fetchone()
    if row and row[0] == st.st_size and row[1] == st.st_mtime_ns:
        return str(row[2]).lower()
    md5 = md5_of(path).lower()
    con.execute("INSERT OR REPLACE INTO mirror_md5(local_path, size, mtime_ns, md5, computed_at) VALUES (?,?,?,?,?)",
                (str(path), st.st_size, st.st_mtime_ns, md5, env.utcnow().isoformat()))
    return md5


def stale_raw_rows(con: sqlite3.Connection, site: str, ch: Chunk,
                   rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """The chunk's downloaded (``dl_status='ok'``), not evicted journal rows whose local file is missing, of
    another size than ``dl_bytes``, or whose md5 is not ``dl_md5`` (e.g. files an earlier run of this host left
    for a journal restored from another VM): they must be downloaded again before the chunk is stacked or
    uploaded. ``rows``: the site's ``evict.site_rows`` when the caller already read them."""
    from .evict import norm_md5, site_rows

    ms = set(ch.months)
    out = []
    for r in rows if rows is not None else site_rows(con, site):
        if r["product"] != ch.product or r["month"] not in ms or r["dl_status"] != "ok" or r["evicted"]:
            continue
        p = env.ROOT / (r["local_path"] or "")
        try:
            st = p.stat()
        except OSError:
            out.append({**r, "why": "missing"})
            continue
        if r["dl_bytes"] is not None and st.st_size != int(r["dl_bytes"]):
            out.append({**r, "why": f"size {st.st_size} != {r['dl_bytes']}"})
            continue
        want = norm_md5(r["dl_md5"])
        if want and _cached_md5(con, p, st) != want:
            out.append({**r, "why": "md5 differs from dl_md5"})
    return out


def _mark_stale(con: sqlite3.Connection, site: str, stale: list[dict[str, Any]]) -> None:
    for r in stale:
        con.execute("UPDATE site_files SET dl_status=NULL, dl_error=? WHERE site=? AND product=? AND month=? AND "
                    "name=?", (f"re-download: {r['why']}"[:500], site, r["product"], r["month"], r["name"]))


def _unlink_stale(stale: list[dict[str, Any]]) -> None:
    for r in stale:
        if r["why"] != "missing":
            with contextlib.suppress(OSError):
                (env.ROOT / r["local_path"]).unlink()


def _replan(con: sqlite3.Connection, site: str, ch: Chunk, why: str, stale: list[dict[str, Any]], *,
            uncovered: bool) -> None:
    """Send a chunk back to ``planned``: stale raw rows are downloaded again (and their local files removed), and a
    chunk whose stack the lake lacks gets its evicted rows downloaded again too (``dl_status='stale'``)."""
    from .evict import has_evict_column

    con.execute("BEGIN IMMEDIATE")
    try:
        _mark_stale(con, site, stale)
        if uncovered and has_evict_column(con):
            con.execute(f"UPDATE site_files SET dl_status='stale' WHERE site=? AND product=? AND month IN "
                        f"({','.join('?' * len(ch.months))}) AND dl_status='ok' AND coalesce(local_evicted, 0)=1",
                        (site, ch.product, *ch.months))
        if ch.done != "planned" or ch.status != "planned":
            con.execute("UPDATE stream_chunks SET status='planned', done='planned', attempt=0, error=? "
                        "WHERE site=? AND chunk_id=?", (why[:500], site, ch.chunk_id))
            chunkmod.reset_finalize(con, site)
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    _unlink_stale(stale)
    log.warning("chunk %s %s re-planned: %s (%d raw file(s) to download again)", site, ch.chunk_id, why, len(stale))


def reconcile_chunks(site: str, con: sqlite3.Connection, uncovered: list[str], *,
                     exclude: tuple[str, ...] = ()) -> dict[str, list[str]]:
    """Re-plan (status ``planned``) the chunks the local lake cannot continue from:

    - ``uncovered``: chunks whose ``done`` is at least ``stacked`` but whose ``stream stack`` snapshots the lake
      lacks (the lake came back from the Data Store without them: another VM's lineage, an older versioned
      catalog, a lake lost before its push). Their evicted raw files are downloaded again;
    - chunks between ``downloaded`` and ``uploaded`` whose downloaded raw files are missing or stale on this host
      (size and md5 checked against ``dl_bytes``/``dl_md5``, :func:`stale_raw_rows`). A chunk not downloaded yet
      is checked by its own download stage.

    A chunk whose lock another process holds (a live stage) is left alone and reported ``busy``; ``exclude``
    names chunks whose lock the caller holds. Returns {replanned, busy, staleFiles}."""
    from .evict import site_rows

    out: dict[str, list[str]] = {"replanned": [], "busy": [], "staleFiles": []}
    todo = set(uncovered)
    rows: list[dict[str, Any]] | None = None
    for ch in chunkmod.list_chunks(site, con=con):
        if ch.chunk_id in exclude:
            continue
        check = ch.status != "deleted" and RANK["downloaded"] <= RANK.get(ch.done, 0) < RANK["uploaded"]
        if ch.chunk_id not in todo and not check:
            continue
        if check and rows is None:
            rows = site_rows(con, site)
        stale = stale_raw_rows(con, site, ch, rows) if check else []
        if ch.chunk_id not in todo and not stale:
            continue
        why = ("re-planned by restore_lake: the site lake lacks its stack" if ch.chunk_id in todo else
               f"re-planned by restore_lake: {len(stale)} raw file(s) missing or stale on this host")
        try:
            with env.file_lock(f"stream-{site}-{ch.chunk_id}"):
                current = chunkmod.get_chunk(site, ch.chunk_id, con=con) or ch
                _replan(con, site, current, why, stale, uncovered=ch.chunk_id in todo)
        except SystemExit:  # a live stage holds the chunk
            out["busy"].append(ch.chunk_id)
            continue
        out["replanned"].append(ch.chunk_id)
        out["staleFiles"] += [r["name"] for r in stale]
    return out


def restore_lake(site: str, *, session: Any | None = None, con: sqlite3.Connection | None = None,
                 fetch_data: bool | None = None, exclude: tuple[str, ...] = ()) -> int:
    """Bring the local site lake up to the Data Store copy, then re-plan the chunks it does not cover.

    - the Data Store copy is the catalog ``ducklake/CURRENT`` points to, else the newest verifying versioned
      catalog, else the canonical ``neon_<site>.ducklake`` (``upload.fetch_current_catalog``);
    - ``data/**``: only the data files the catalog being installed references, and only from good replicas
      (:func:`_sync_lake_data`): a missing file is fetched (size and checksum verified before it lands); a local
      file of the size the catalog records is never touched, and one of another size is replaced only by a
      verified download of a good replica of that size (else kept and logged). A remote object without a good
      replica (the orphan of an interrupted push: size 0 or partial, no checksum) is never read, so a torn put
      can neither wedge the restore nor overwrite a good local file. ``fetch_data=None`` (default) syncs unless
      ``cleanup_site`` ran for the site: a finished site keeps its Parquet in the Data Store only, and a stage that
      must write the lake again restores it with ``fetch_data=True`` (:func:`ensure_lake_data`, which also clears
      the site's ``cleanup`` step);
    - the catalog: replaced by the Data Store copy when the local lake is missing or behind it (its snapshots are
      a prefix of the remote ones), or diverged from it (another VM continued the site), or references data files
      that neither this host nor the Data Store holds (while the Data Store copy's are all available). The replaced
      catalog is kept as ``<catalog>.replaced-<time>``. The restored catalog's md5 is journaled in ``lake_pushes``
      (``kind='restore'``), so the next push knows the Data Store copy is one this host has seen;
    - chunks: :func:`reconcile_chunks` re-plans every chunk whose ``done`` is at least ``stacked`` but whose
      ``stream stack`` snapshots the lake lacks, and every chunk below ``uploaded`` whose raw files are missing or
      stale here (size + md5).

    0 = the local lake covers every chunk still marked stacked (the others were re-planned); 2 = the restore
    failed; 4 = retry later (the Data Store catalog is being written and no copy verifies, a data file the catalog
    to install references is neither here nor in a good replica, or an uncovered chunk is busy in another process).
    ``exclude``: chunks whose lock the caller holds (left alone).
    """
    from . import upload

    site = site.upper()
    paths = site_lake.site_paths(site)
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        chunkmod.ensure_schema(con)
        sess = session if session is not None else env.mesa().session
        tmp = paths.tmp_dir / f"restore_{os.getpid()}_{paths.catalog_file.name}"
        cleaned = chunkmod.site_cleaned(site, con=con)
        want_data = (not cleaned) if fetch_data is None else fetch_data
        summary: dict[str, Any] = {"site": site, "at": env.utcnow().isoformat(), "fetchData": want_data}
        with env.file_lock(f"site-lake-{site}", blocking=True):
            try:
                cur = upload.current_catalog(sess, site)
                summary["remote"] = cur.describe()
                if cur.exists:
                    paths.tmp_dir.mkdir(parents=True, exist_ok=True)
                    replace, why, got = _catalog_behind(sess, site, cur, con, tmp)
                    if replace and (got is None or not tmp.exists()):
                        got = upload.fetch_current_catalog(sess, site, tmp)
                    if want_data:  # only the files the catalog being installed references, from good replicas
                        data_coll = upload.irods_path_for(paths.data_dir)
                        remote_data = upload.good_objects(sess, data_coll, recursive=True)
                        sync = _sync_lake_data(sess, site, tmp if replace else paths.catalog_file, remote_data,
                                               data_coll)
                        if sync["unavailable"] and not replace:
                            # the local catalog references data this host lost that no push completed: the Data
                            # Store copy (a consistent lake: data files are pushed before their catalog) replaces it
                            if got is None or not tmp.exists():
                                got = upload.fetch_current_catalog(sess, site, tmp)
                            remote_sync = _sync_lake_data(sess, site, tmp, remote_data, data_coll)
                            if not remote_sync["unavailable"]:
                                replace, why = True, (f"the local catalog references {len(sync['unavailable'])} "
                                                      "data file(s) that neither this host nor the Data Store holds")
                                remote_sync["fetched"] += sync["fetched"]
                                sync = remote_sync
                        summary["dataFetched"] = sync["fetched"]
                        summary["data"] = {k: v[:50] if isinstance(v, list) else v for k, v in sync.items()}
                        if sync["unavailable"]:
                            raise LakeDataUnavailable(
                                f"{len(sync['unavailable'])} data file(s) the catalog references are neither here "
                                f"nor in a good Data Store replica (e.g. {sync['unavailable'][0]}): the catalog is "
                                "not installed")
                    summary.update(replaceCatalog=replace, why=why)
                    if replace:
                        if paths.catalog_file.exists():
                            stamp = env.utcnow().strftime("%Y%m%dT%H%M%SZ")
                            for f in (paths.catalog_file, paths.wal_file):
                                if f.exists():
                                    os.replace(f, f.with_name(f"{f.name}.replaced-{stamp}"))
                        paths.lake_dir.mkdir(parents=True, exist_ok=True)
                        os.replace(tmp, paths.catalog_file)
                        log.warning("restore lake %s: catalog replaced by the Data Store copy %s (%s)", site,
                                    got.path, why)
                    local_keys = {site_lake.snapshot_key(x) for x in site_lake.catalog_snapshots(
                        paths.catalog_file, site)}
                    known = {r["catalog_md5"] for r in chunkmod.lake_pushes(site, con=con)
                             if r.get("snapshot_key") in local_keys}
                    remote_md5 = got.md5 if got is not None else cur.md5
                    if replace or not remote_md5 or remote_md5 not in known:
                        if got is None:  # in sync (same md5) or this lake continues the remote one
                            got = upload.fetch_current_catalog(sess, site, tmp)
                        snaps = got.snapshots or []
                        if got.md5:
                            chunkmod.record_lake_push(
                                con, site, got.md5, snaps[-1][0] if snaps else None, kind="restore",
                                snapshot_key=site_lake.snapshot_key(snaps[-1]) if snaps else None)
                    if got is not None:
                        summary["used"] = got.describe()
                    if cleaned and want_data:
                        chunkmod.clear_cleanup(con, site)
                        summary["cleanupCleared"] = True
            except (upload.NoCatalog, LakeDataUnavailable) as exc:  # being written: nothing verifies now
                log.warning("restore lake %s: %s; retry later", site, exc)
                summary["retry"] = str(exc)[:500]
            except Exception as exc:  # reported, never raised
                log.exception("restore lake %s failed", site)
                summary["error"] = f"{type(exc).__name__}: {exc}"[:500]
            finally:
                tmp.unlink(missing_ok=True)
                tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
            have = _ids_of(site_lake.catalog_snapshots(paths.catalog_file, site), site) \
                if paths.catalog_file.exists() and not summary.get("error") and not summary.get("retry") else {}
        rc = RC_FAILED if summary.get("error") else RC_RETRY if summary.get("retry") else RC_OK
        if rc == RC_OK:
            lacking = [c for c in chunkmod.stacked_required(site, con=con) if c not in have]
            summary["uncovered"] = lacking
            rec = reconcile_chunks(site, con, lacking, exclude=exclude)
            summary.update(replanned=rec["replanned"], busy=rec["busy"], staleFiles=rec["staleFiles"][:50])
            if lacking:
                log.warning("restore lake %s: the lake lacks %d stacked chunk(s) (%s): re-planned %s", site,
                            len(lacking), ", ".join(lacking[:5]), ", ".join(rec["replanned"][:5]) or "none")
            if set(rec["busy"]) & set(lacking) or set(lacking) & set(exclude):
                rc = RC_RETRY  # an uncovered chunk is still running somewhere: its stages must not go on
        with contextlib.suppress(OSError):
            env.write_json(paths.reports_dir / "restore_lake.json", summary)
        if rc == RC_OK:
            log.info("restore lake %s: ok (%s)", site, summary.get("why") or "no lake in the Data Store")
        return rc
    finally:
        if own:
            con.close()


# ---------------------------------------------------------------- site finalize + cleanup

FINALIZE_STEPS: tuple[str, ...] = ("terms", "cards", "curate", "validate", "avus", "upload", "apply", "history",
                                   "verify")


def _site_step(con: sqlite3.Connection, site: str, step: str, fn: Callable[[], tuple[int, dict[str, Any]]]) -> int:
    if chunkmod.step_status(site, step, con=con) == "ok":
        return RC_OK
    chunkmod.set_step(con, site, step, "running", started=True)
    t0 = time.monotonic()
    try:
        rc, info = fn()
    except (Exception, SystemExit) as exc:  # journaled, never raised
        log.exception("finalize %s %s crashed", site, step)
        rc, info = RC_FAILED, {"error": f"{type(exc).__name__}: {exc}"[:500]}
    info = {**info, "rc": rc, "seconds": round(time.monotonic() - t0, 1)}
    if rc == RC_OK:
        chunkmod.set_step(con, site, step, "ok", info=info)
    elif rc in (RC_NOT_READY, RC_RETRY):  # nothing done: run the step again later
        chunkmod.set_step(con, site, step, "waiting", error=str(info.get("error") or f"rc {rc}")[:500], info=info)
        log.warning("finalize %s: step %s not done yet (rc %d): %s", site, step, rc, info.get("error"))
    else:
        chunkmod.set_step(con, site, step, "failed", error=str(info.get("error") or f"rc {rc}")[:500], info=info)
        log.error("finalize %s: step %s failed (rc %d)", site, step, rc)
    return rc


def _not_ready(con: sqlite3.Connection, site: str) -> str | None:
    open_chunks = chunkmod.unfinished(site, con=con)
    if open_chunks:
        names = ", ".join(ch.chunk_id for ch in open_chunks[:3]) + (" ..." if len(open_chunks) > 3 else "")
        return f"{len(open_chunks)} chunk(s) not deleted yet: {names}"
    if not chunkmod.list_chunks(site, con=con) and chunkmod.month_inventory(con, site):
        return "no chunks planned (run plan_chunks after the listing)"
    return None


def _generic_pending(site: str) -> list[str]:
    """``validate.pending_generic(site)``: products of the site whose curation another live VM holds and whose
    generic result is not in the Data Store yet ([] when the multi-VM curation sync is not installed).

    The ``curate`` step is then left ``waiting`` so the next finalize pass runs it again: its Data Store
    prefetch imports the published results before the product AVUs are built."""
    from . import validate

    fn = getattr(validate, "pending_generic", None)
    if fn is None:
        return []
    try:
        return sorted(fn(site) or [])
    except Exception as exc:  # noqa: BLE001 - the Data Store is unreachable: better wait than build without them
        log.warning("finalize %s: pending generic curation unknown (%s); retry later", site, exc)
        return [f"<unknown: {type(exc).__name__}>"]


def _main_rc(rc: int) -> tuple[int, dict[str, Any]]:
    return (RC_OK, {}) if rc == 0 else (RC_FAILED, {"error": f"rc {rc}"})


def finalize_site(site: str, *, threads: int = 8, rate: float = 20.0, session: Any | None = None) -> int:
    """Everything after the last chunk: terms, cards, curation, site-level AVUs, finalize upload, apply,
    history and a metadata-level verify (each step journaled in ``stream_sites``; ok steps are skipped)."""
    from . import avus

    site = site.upper()
    con = state.connect(state.DB_PATH)
    try:
        chunkmod.ensure_schema(con)
        why = _not_ready(con, site)
        if why:
            log.error("finalize %s: %s", site, why)
            return RC_NOT_READY
        if any(chunkmod.step_status(site, st, con=con) != "ok" for st in FINALIZE_STEPS):
            rc, why = _lake_data_rc(site, con=con, session=session, missing="never")  # a cleaned site's data
            if why:
                log.error("finalize %s: %s", site, why)
                return RC_RETRY if rc == RC_RETRY else RC_NOT_READY

        def terms() -> tuple[int, dict[str, Any]]:
            from .terms import main
            return _main_rc(main(site))

        def cards() -> tuple[int, dict[str, Any]]:
            from .cards import main
            return _main_rc(main(site))

        def curate() -> tuple[int, dict[str, Any]]:
            from .curate import main
            return _main_rc(main(site))

        def validate() -> tuple[int, dict[str, Any]]:
            from .validate import has_proposals, main
            if not has_proposals(site):
                return RC_OK, {"skipped": "no proposals: every product reuses curation/generic or is external"}
            return _main_rc(main(site))

        def build() -> tuple[int, dict[str, Any]]:
            s = avus.build_desired(site, levels=avus.FINALIZE_LEVELS, con=con)
            info = {k: s.get(k) for k in ("rows", "units_built", "products_built", "preserved")}
            if s.get("datacite_errors"):
                return RC_FAILED, {**info, "error": f"DataCite errors: {s['datacite_errors'][:3]}"}
            return RC_OK, info

        def push() -> tuple[int, dict[str, Any]]:
            from .upload import finalize_summary
            out = finalize_summary(site, threads=threads, session=session)
            lake = out.get("lake") or {}
            if lake.get("claimLost"):
                return RC_NOT_READY, {"error": str(lake["claimLost"])}
            if out["rc"] == RC_RETRY:
                return RC_RETRY, {"error": f"the site lake was not pushed yet: {lake.get('refused')}"}
            if out["rc"] == 1:
                return RC_FAILED, {"error": "the site lake was not pushed: the Data Store holds a lake this host does "
                                            f"not contain (another VM); restore_lake, then finalize again "
                                            f"({lake.get('refused')})"[:500]}
            return _main_rc(out["rc"])

        def apply() -> tuple[int, dict[str, Any]]:
            from ..avu import bulk
            raw = f"{env.IRODS_ROOT}/sites/{site}/raw/"
            units = [u for (u,) in con.execute("SELECT DISTINCT unit_id FROM avu_desired WHERE scope=?", (site,))
                     if not (u.startswith(raw) and u[len(raw):].count("/") == 1)]  # month units: applied per chunk
            if not units:
                return RC_FAILED, {"error": "no site-level desired AVUs"}
            st = bulk.apply_scope(site, only_units=units, threads=threads, rate=rate, con=con, session=session)
            info = {k: st.get(k) for k in ("units", "targets", "adds", "removes", "errors", "missing",
                                           "unit_failures")}
            if st.get("errors") or st.get("unit_failures"):
                return RC_FAILED, {**info, "error": f"{st.get('errors', 0)} error(s)"}
            return RC_OK, info

        def history() -> tuple[int, dict[str, Any]]:
            from ..avu import history as hist
            out = hist.record_scope(site, con=con) or {}
            info = {k: out.get(k) for k in ("batches_recorded", "rows_recorded", "blocked_rows", "rows_spooled")
                    if k in out}
            if out.get("error") or out.get("failed_batch") or out.get("blocked_rows"):
                return RC_FAILED, {**info, "error": str(out.get("error") or "rows blocked by a pending batch")}
            return RC_OK, info

        def verify() -> tuple[int, dict[str, Any]]:
            from .. import verify_site
            kw: dict[str, Any] = {"metadata_only": True}  # the raw files and CSVs are gone: metadata level only
            if session is not None:
                kw["session"] = session
            return _main_rc(verify_site.main(site, **kw))

        for step, fn in (("terms", terms), ("cards", cards), ("curate", curate), ("validate", validate),
                         ("avus", build), ("upload", push), ("apply", apply), ("history", history),
                         ("verify", verify)):
            if step == "avus" and chunkmod.step_status(site, "avus", con=con) != "ok":
                waiting = _generic_pending(site)
                if waiting:  # another VM curates these products: build the product AVUs once it has published
                    msg = (f"waiting for the generic curation of {len(waiting)} product(s) claimed by other VMs: "
                           f"{', '.join(waiting[:5])}{' ...' if len(waiting) > 5 else ''}")
                    chunkmod.set_step(con, site, "curate", "waiting", error=msg[:500], info={"pending": waiting})
                    log.warning("finalize %s: not ready: %s", site, msg)
                    return RC_NOT_READY
            rc = _site_step(con, site, step, fn)
            if rc:
                return rc if rc in (RC_NOT_READY, RC_RETRY) else RC_FAILED
        log.info("finalize %s: done", site)
        return RC_OK
    finally:
        con.close()


def cleanup_site(site: str, *, session: Any | None = None) -> int:
    """Delete the local lake ``data/**``, cards and curation once each file's Data Store copy checks out.

    Needs a finished :func:`finalize_site`. The site lake is pushed first (:func:`push_lake`: a no-op when the Data
    Store already serves the local catalog), so lake rows written since finalize (a re-list of a finished site)
    reach the Data Store instead of being kept locally. Keeps ``reports/``, the lake catalog file and the journal.
    Writes ``reports/cleanup.json``. 0 = everything verified and deleted, 1 = not ready (or the site's claim is
    lost), 3 = some files kept.
    """
    from . import evict, site_lake, upload

    site = site.upper()
    con = state.connect(state.DB_PATH)
    try:
        chunkmod.ensure_schema(con)
        why = _not_ready(con, site)
        todo = [s for s in FINALIZE_STEPS if chunkmod.step_status(site, s, con=con) != "ok"]
        if why or todo:
            log.error("cleanup %s: %s", site, why or f"finalize not finished ({', '.join(todo)})")
            return RC_NOT_READY
        sess = session if session is not None else env.mesa().session
        lake = upload.push_lake(site, session=sess)  # lake changes since finalize, before the local data goes
        if lake.get("claimLost"):
            log.error("cleanup %s: %s", site, lake["claimLost"])
            return RC_NOT_READY
        if lake.get("failures"):
            log.warning("cleanup %s: the site lake was not pushed (%s); only files with a Data Store copy are "
                        "deleted", site, lake.get("refused") or lake.get("error") or f"{lake['failures']} failure(s)")
        chunkmod.set_step(con, site, "cleanup", "running", started=True)
        with env.file_lock(f"site-lake-{site}", blocking=True):
            files, skipped = evict.cleanup_candidates(site)
            ok, blocked = evict.verify_copies(sess, files)
            removed = evict.remove_files(ok, site)
        sup = evict.delete_superseded(con, site)  # superseded raw files no chunk's delete reached
        removed["errors"] += sup["errors"]
        report = {"site": site, "at": env.utcnow().isoformat(), "candidates": len(files), "verified": len(ok),
                  "deleted": removed["deleted"], "bytesFreed": removed["bytes"] + sup["bytes"],
                  "supersededDeleted": sup["deleted"], "errors": removed["errors"],
                  "blocked": {k: {"files": len(v), "sample": v[:20]} for k, v in sorted(blocked.items())},
                  "notUploadedKept": [str(p.relative_to(env.ROOT)) if p.is_relative_to(env.ROOT) else str(p)
                                      for p in skipped][:50]}
        with contextlib.suppress(OSError):
            env.write_json(site_lake.site_paths(site).reports_dir / "cleanup.json", report)
        status = "ok" if not blocked and not removed["errors"] else "partial"
        chunkmod.set_step(con, site, "cleanup", status, info={k: v for k, v in report.items()
                                                             if k in ("deleted", "bytesFreed", "blocked")},
                          error=None if status == "ok" else f"{sum(len(v) for v in blocked.values())} file(s) kept")
        log.info("cleanup %s: %d file(s) deleted (%.2f GB); %d kept", site, removed["deleted"],
                 removed["bytes"] / 1e9, sum(len(v) for v in blocked.values()))
        return RC_OK if status == "ok" else RC_BLOCKED
    finally:
        con.close()


SITE_STAGES: dict[str, Callable[..., int]] = {"finalize": finalize_site, "cleanup": cleanup_site}


def main(stage: str, site: str, chunk_id: str | None = None, **kw: Any) -> int:
    """CLI entry: ``<chunk stage> SITE CHUNK_ID`` or ``finalize|cleanup SITE``; ``plan`` prints the chunks."""
    if stage == "plan":
        for ch in chunkmod.plan_chunks(site, **{k: v for k, v in kw.items() if k in ("max_bytes", "products",
                                                                                   "months")}):
            print(f"{ch.chunk_id}\t{len(ch.months)} month(s)\t{ch.n_files} file(s)\t{ch.bytes / 1e9:.2f} GB\t"
                  f"{ch.status}")
        return RC_OK
    if stage in SITE_STAGES:
        fn = SITE_STAGES[stage]
        return fn(site, **{k: v for k, v in kw.items() if k in inspect.signature(fn).parameters})
    if stage not in CHUNK_STAGES:
        raise SystemExit(f"unknown stream stage {stage!r} (plan, {', '.join(CHUNK_STAGES)}, finalize, cleanup)")
    if not chunk_id:
        raise SystemExit(f"stream {stage} needs a chunk id")
    fn = CHUNK_STAGES[stage]
    return fn(site, chunk_id, **{k: v for k, v in kw.items() if k in inspect.signature(fn).parameters})
