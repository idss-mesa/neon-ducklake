"""`neon-ducklake verify mesa` — the project's MESA AVU history vs the journal and iRODS (read-only).

The MESA catalog is opened through the ONE ``env.mesa_ducklake_client`` of this process, under the global
``mesa-tag`` file lock and only when no other process holds ``~/.mesa/ducklake/catalog.duckdb``; only read
calls are made (``find_project_by_path``, ``list_snapshots(include_pending=True)``, ``list_pending_pushes``,
``get_avus`` from the local cache — no session, so nothing is fetched or pushed). Snapshot Parquet is read
from ``work/mesa-cache/<project>/`` only. iRODS is read with GenQuery only.

Checks (plan § Verification, MESA):
- project        the project root is a MESA project
- pending        0 pending pushes and 0 failed/pending snapshots of the project
- snapshots      <= 200 snapshots; every snapshot actor is ``tswetnam``
- journal        journal rows ``recorded`` == Σ rows of our (``batch=``) snapshots, per snapshot too
- sources        every row of our snapshots has source ``neon-ducklake:*`` and actor ``tswetnam``
- get_avus       for up to 50 random journal paths per level: MESA effective AVUs == iRODS AVUs minus
                 ``ipc*``/``mesa.enabled``/non-journal-owned triples
"""

from __future__ import annotations

import logging
import posixpath
import random
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import env, state
from .avu import bulk, history
from .avu.normalize import is_protected
from .verify_site import Check, _sample

log = logging.getLogger("neon_ducklake.verify_mesa")

MAX_SNAPSHOTS = 200
PATHS_PER_LEVEL = 50
DEAD = ("pending", "failed")
SOURCE_PREFIX = "neon-ducklake:"

Triple = tuple[str, str, str]


def object_avus(session: Any, path: str) -> tuple[bool, set[Triple]]:
    """(exists, AVUs) of one data object by GenQuery (replica rows collapse)."""
    from irods.models import Collection, DataObject, DataObjectMeta

    parent, name = posixpath.split(path)
    out: set[Triple] = set()
    seen = False
    q = session.query(DataObject.id, DataObjectMeta.name, DataObjectMeta.value, DataObjectMeta.units)
    for row in q.filter(Collection.name == parent).filter(DataObject.name == name):
        seen = True
        out.add((row[DataObjectMeta.name], row[DataObjectMeta.value], row[DataObjectMeta.units] or ""))
    if not seen:
        seen = name in bulk.object_names(session, parent)
    return seen, out


def irods_avus(session: Any, path: str, target_type: str) -> tuple[bool, set[Triple]]:
    if target_type == "collection":
        has, avus = bulk.query_collection_avus(session, path)
        return (has or bulk.collection_exists(session, path)), avus
    return object_avus(session, path)


def journal_owned(con: sqlite3.Connection, paths: list[str]) -> dict[str, set[Triple]]:
    """Triples each path owns per the journal (recorded adds not later recorded-removed), every scope."""
    owned: dict[str, set[Triple]] = defaultdict(set)
    for i in range(0, len(paths), 500):
        chunk = paths[i:i + 500]
        for path, a, v, u, op in con.execute(
                f"SELECT path, attribute, value, unit, op FROM avu_journal WHERE status='recorded' AND path IN "
                f"({','.join('?' * len(chunk))}) ORDER BY id", chunk):
            t = (a, v, u or "")
            if op == "add":
                owned[path].add(t)
            else:
                owned[path].discard(t)
    return owned


class MesaVerifier:
    def __init__(self, client: Any, session: Any, con: sqlite3.Connection, *, cache_dir: Path | None = None,
                 project_path: str | None = None, per_level: int = PATHS_PER_LEVEL, seed: Any = None) -> None:
        self.client, self.session, self.con = client, session, con
        self.cache_dir = cache_dir or env.MESA_CACHE_DIR
        self.project_path = project_path or env.IRODS_ROOT
        self.per_level = per_level
        self.rng = random.Random(seed)
        self.checks: list[Check] = []

    def add(self, name: str, status: str, summary: str, **details: Any) -> None:
        self.checks.append(Check(name, status, summary, details))
        log.info("%-10s %-5s %s", name, status, summary)

    # ------------------------------------------------------------ checks
    def run(self) -> dict[str, Any]:
        project = self.client.find_project_by_path(self.project_path)
        if project is None:
            self.add("project", "fail", f"{self.project_path} is not a MESA project")
            return self.report(None)
        pid = project.project_id
        self.add("project", "pass", f"{self.project_path} -> project {pid}")
        snaps = history._catalog_snapshots(self.client, pid)
        ids = {s.snapshot_id for s in snaps}
        pushes = [p for p in history._pending_pushes(self.client) if p.snapshot_id in ids]
        dead = Counter(s.parquet_file for s in snaps if s.parquet_file in DEAD)
        self.add("pending", "fail" if (pushes or dead) else "pass",
                 f"pending pushes {len(pushes)}; failed snapshots {dead.get('failed', 0)}; pending snapshots "
                 f"{dead.get('pending', 0)}", pushes=_sample(p.snapshot_id for p in pushes),
                 dead=_sample(s.snapshot_id for s in snaps if s.parquet_file in DEAD))
        actors = Counter(s.actor for s in snaps)
        foreign = {a: n for a, n in actors.items() if a != env.ACTOR}
        self.add("snapshots", "fail" if (len(snaps) > MAX_SNAPSHOTS or foreign) else "pass",
                 f"{len(snaps)} snapshots (<= {MAX_SNAPSHOTS}); actors {dict(actors)}", foreign_actors=foreign)
        self.check_journal(pid, snaps)
        self.check_get_avus(pid)
        return self.report(pid)

    def check_journal(self, pid: Any, snaps: list[Any]) -> None:
        import duckdb

        batches = {r[0]: r[1] for r in self.con.execute("SELECT batch_id, snapshot_id FROM batches")}
        ours = [s for s in snaps if s.parquet_file not in DEAD and history.batch_id_of(s.note) in batches]
        others = [s.snapshot_id for s in snaps if s.parquet_file not in DEAD and s not in ours]
        files = {s.snapshot_id: self.cache_dir / str(pid) / f"snapshot_{s.snapshot_id}.parquet" for s in ours}
        absent = sorted(sid for sid, f in files.items() if not f.exists())
        present = [str(f) for sid, f in sorted(files.items()) if f.exists()]
        per_snap: dict[int, int] = {}
        sources: Counter = Counter()
        actors: Counter = Counter()
        if present:
            con = duckdb.connect()
            try:
                for sid, n in con.execute("SELECT snapshot_id, count(*) FROM read_parquet(?) GROUP BY 1",
                                          [present]).fetchall():
                    per_snap[int(sid)] = int(n)
                sources.update(dict(con.execute("SELECT source, count(*) FROM read_parquet(?) GROUP BY 1",
                                                [present]).fetchall()))
                actors.update(dict(con.execute("SELECT actor, count(*) FROM read_parquet(?) GROUP BY 1",
                                               [present]).fetchall()))
            finally:
                con.close()
        recorded = self.con.execute("SELECT count(*) FROM avu_journal WHERE status='recorded'").fetchone()[0]
        j_per = {int(sid): int(n) for sid, n in self.con.execute(
            "SELECT snapshot_id, count(*) FROM avu_journal WHERE status='recorded' GROUP BY 1") if sid is not None}
        total = sum(per_snap.values())
        diff = {sid: {"journal": j_per.get(sid, 0), "parquet": per_snap.get(sid, 0)}
                for sid in sorted(set(j_per) | set(per_snap)) if j_per.get(sid, 0) != per_snap.get(sid, 0)
                and sid not in absent}
        orphans = sorted(set(j_per) - {s.snapshot_id for s in ours})
        status = "fail" if (recorded != total or diff or absent or orphans) else "pass"
        self.add("journal", status,
                 f"journal recorded {recorded} vs Σ parquet rows {total} over {len(ours)} batch snapshots "
                 f"(missing local parquet {len(absent)}; per-snapshot diffs {len(diff)}; journal snapshot ids "
                 f"without a batch snapshot {len(orphans)}; other project snapshots {len(others)})",
                 missing_parquet=_sample(absent), per_snapshot_diffs=dict(list(diff.items())[:20]),
                 orphan_snapshot_ids=_sample(orphans), other_snapshots=_sample(others))
        bad_src = {s: n for s, n in sources.items() if not str(s or "").startswith(SOURCE_PREFIX)}
        bad_act = {a: n for a, n in actors.items() if a != env.ACTOR}
        self.add("sources", "fail" if (bad_src or bad_act) else ("pass" if sources else "skip"),
                 f"{sum(sources.values())} rows: sources {len(sources)} distinct, non-neon-ducklake {sum(bad_src.values())}"
                 f"; actors {dict(actors)}", sources=dict(sources.most_common(20)), bad_sources=bad_src,
                 bad_actors=bad_act)

    def sample_paths(self) -> dict[str, list[tuple[str, str]]]:
        rows = self.con.execute("SELECT DISTINCT scope, path, target_type FROM avu_journal WHERE status='recorded'"
                                " ORDER BY scope, path").fetchall()
        levels: dict[tuple[str, str], str] = {}
        try:
            for scope, path, level in self.con.execute("SELECT scope, path, level FROM avu_desired"):
                levels[(scope, path)] = level or "?"
        except sqlite3.OperationalError:
            pass
        by_level: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for scope, path, ttype in rows:
            by_level[f"{scope}:{levels.get((scope, path), ttype)}"].append((path, ttype))
        return {lvl: (self.rng.sample(items, self.per_level) if len(items) > self.per_level else items)
                for lvl, items in sorted(by_level.items())}

    def check_get_avus(self, pid: Any) -> None:
        picks = self.sample_paths()
        if not picks:
            self.add("get_avus", "skip", "no recorded journal rows yet")
            return
        owned = journal_owned(self.con, sorted({p for items in picks.values() for p, _t in items}))
        mismatch, foreign, gone = [], Counter(), []
        checked: Counter = Counter()
        for lvl, items in picks.items():
            for path, ttype in items:
                exists, cur = irods_avus(self.session, path, ttype)
                mine = owned.get(path, set())
                got = {(a.attribute, a.value, a.unit or "") for a in self.client.get_avus(pid, path)}
                got = {t for t in got if not is_protected(t[0])}
                if not exists:
                    gone.append(path)
                    if got & mine:
                        mismatch.append(f"[{lvl}] {path}: gone from iRODS but MESA holds {len(got & mine)} owned AVUs")
                    continue
                checked[lvl] += 1
                expected = {t for t in cur if not is_protected(t[0]) and t in mine}
                missing = expected - got
                extra = (got & mine) - expected
                if missing or extra:
                    eg = min(missing or extra)
                    mismatch.append(f"[{lvl}] {path}: iRODS-only {len(missing)}, MESA-only {len(extra)}, e.g. {eg}")
                if got - mine:
                    foreign[lvl] += len(got - mine)
        status = "fail" if mismatch else ("warn" if (foreign or gone) else "pass")
        self.add("get_avus", status,
                 f"{sum(checked.values())} paths over {len(picks)} levels ({dict(checked)}): mismatches "
                 f"{len(mismatch)}; MESA AVUs not owned by the journal {sum(foreign.values())}; paths gone "
                 f"from iRODS {len(gone)}", mismatches=_sample(mismatch), foreign=dict(foreign), gone=_sample(gone))

    def report(self, pid: Any) -> dict[str, Any]:
        counts = Counter(c.status for c in self.checks)
        return {"project": str(pid) if pid else None, "projectPath": self.project_path,
                "generatedAt": env.utcnow().isoformat(), "summary": dict(counts),
                "ok": counts.get("fail", 0) == 0, "checks": [asdict(c) for c in self.checks]}


def render(report: dict[str, Any]) -> str:
    lines = [f"verify mesa {report['projectPath']} (project {report['project']})", f"{'check':10} {'status':6} summary",
             "-" * 100]
    lines += [f"{c['name']:10} {c['status'].upper():6} {c['summary']}" for c in report["checks"]]
    lines += ["-" * 100, "result: " + ", ".join(f"{k}={v}" for k, v in sorted(report["summary"].items()))
              + (" -> OK" if report["ok"] else " -> FAILED")]
    return "\n".join(lines)


def main(per_level: int = PATHS_PER_LEVEL, out: Path | None = None) -> int:
    """Open the MESA catalog read-only-in-practice under the mesa-tag lock and run every check."""
    holders = env.catalog_lock_holders()
    if holders:
        pids = ", ".join(f"{h['pid']} ({h['cmd'][:60]})" for h in holders)
        print(f"MESA catalog is held by PID(s) {pids}; close that session and re-run")
        return 2
    con = sqlite3.connect(f"file:{state.DB_PATH}?mode=ro", uri=True, timeout=60) if state.DB_PATH.exists() \
        else state.connect(state.DB_PATH)
    try:
        with env.file_lock("mesa-tag"):
            if env.catalog_lock_holders():
                print("MESA catalog became busy; re-run later")
                return 2
            m = env.mesa()
            try:
                client = env.mesa_ducklake_client(m)
                report = MesaVerifier(client, m.session, con, per_level=per_level).run()
            finally:
                env.close_mesa_ducklake_client()
    finally:
        con.close()
    path = out or env.LOG_DIR / "verify_mesa.json"
    env.write_json(path, report)
    print(render(report))
    print(f"\nwrote {path}")
    return 0 if report["ok"] else 1
