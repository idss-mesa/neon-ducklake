"""site/stream.py + upload.py: the site lake in the Data Store (per-chunk push, restore, catalog guard), a site
taken over by another VM, a cleaned-up site whose finished chunk gains files, the working set, finalize waiting
for another VM's curation, the chunk's taxa mapping and the site-lake lock of lake readers."""

from __future__ import annotations

import contextlib
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from neon_ducklake import env, state
from neon_ducklake.site import chunks, fetch, site_lake, stream, terms, upload

from .test_site_common import SITE, site_env  # noqa: F401 - fixtures
from .test_site_stack import IS, OS, _csv, _q
from .test_stream_stages import S, _chunk, pipe  # noqa: F401 - pipe is a fixture

CATALOG = f"{S}/ducklake/neon_test.ducklake"
BIRD = "dp1_10003_001__brd_countdata"
SAAT = "dp1_00002_001__saat_30min"
REAL_RESOLVE_CHUNK_TAXA = terms.resolve_chunk_taxa  # the pipe fixture stubs it (no OLS in the tests)


def _md5(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def _use_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Point every project path (and the journal) at ``root``: one "VM" of a multi-VM test."""
    for attr, rel in (("ROOT", ""), ("API_DIR", "api"), ("RUNS_DIR", "api/runs"), ("LAKE_DIR", "ducklake"),
                      ("SITES_DIR", "sites"), ("WORK_DIR", "work"), ("LOCK_DIR", "work/locks"),
                      ("MESA_CACHE_DIR", "work/mesa-cache"), ("QUARANTINE_DIR", "work/quarantine")):
        monkeypatch.setattr(env, attr, root / rel if rel else root)
    monkeypatch.setattr(state, "DB_PATH", root / "work" / "state.sqlite")


def _copy_journal(src: Path, dst: Path) -> None:
    """A journal restore (multi-VM ``restore_site_journal``) reduced to a consistent copy of the whole file."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    a, b = sqlite3.connect(src), sqlite3.connect(dst)
    try:
        a.backup(b)
    finally:
        a.close()
        b.close()


def _files(root: Path, product: str, months: list[str]) -> list[str]:
    con = sqlite3.connect(root / "work" / "state.sqlite")
    try:
        return [r[0] for r in con.execute(f"SELECT local_path FROM site_files WHERE product=? AND month IN "
                                          f"({','.join('?' * len(months))})", (product, *months))]
    finally:
        con.close()


def _error(chunk_id: str) -> str:
    return chunks.get_chunk(SITE, chunk_id).error or ""


POINTER = f"{S}/ducklake/CURRENT"
VERSIONS = f"{S}/ducklake/catalogs/"


def _pointer(pipe: SimpleNamespace) -> dict[str, Any]:  # noqa: F811
    return json.loads(pipe.store.content(POINTER))


def _versions(pipe: SimpleNamespace) -> list[str]:  # noqa: F811
    return sorted(p for p in pipe.store.objs if p.startswith(VERSIONS))


def foreign_catalog(where: Path, msg: str = "another VM's lake") -> tuple[bytes, list[site_lake.Snapshot]]:
    """A DuckLake catalog with a history of its own (another VM's lake), as bytes plus its snapshots."""
    import duckdb

    f = where / "neon_test.ducklake"
    f.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute("LOAD ducklake")
        con.execute(f"ATTACH 'ducklake:{f}' AS o (DATA_PATH '{where / 'data'}/')")
        con.execute("BEGIN")
        con.execute("CREATE TABLE o.t AS SELECT 1 AS x")
        con.execute("CALL ducklake_set_commit_message('o', 'vm-b', ?)", [msg])
        con.execute("COMMIT")
        con.execute("DETACH o")
    finally:
        con.close()
    return f.read_bytes(), site_lake.catalog_snapshots(f, SITE)


def publish_foreign(pipe: SimpleNamespace, where: Path) -> str:  # noqa: F811
    """Another VM publishes its lake the way push_lake does: a versioned copy, then the CURRENT pointer."""
    data, snaps = foreign_catalog(where)
    md5 = _md5(data)
    name = upload.version_name(SITE, snaps[-1], md5)
    pipe.store.add_obj(f"{VERSIONS}{name}", content=data)
    upload.write_pointer(pipe.session, SITE, {"catalog": name, "path": f"{VERSIONS}{name}", "md5": md5})
    return md5


# ---------------------------------------------------------------- per-chunk lake push + verify (review B2.1)

def test_upload_pushes_the_lake_and_verify_requires_that_push(pipe: SimpleNamespace,  # noqa: F811
                                                              tmp_path: Path) -> None:
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    ch = chunks.get_chunk(SITE, c.chunk_id)
    snap = ch.info["stack"]["lastSnapshot"]
    assert snap and ch.info["stack"]["status"] == "ok"
    # the chunk's stacked rows are in the Data Store: data files, then the catalog (a versioned copy, the CURRENT
    # pointer, the canonical copy), journaled in lake_pushes
    assert CATALOG in pipe.store.objs
    assert any(p.startswith(f"{S}/ducklake/data/") for p in pipe.store.objs)
    [push] = chunks.lake_pushes(SITE)
    assert push["kind"] == "push" and push["snapshot_id"] >= snap
    assert push["catalog_md5"] == _md5(pipe.store.content(CATALOG)) == ch.info["upload"]["lake"]["catalogMd5"]
    ptr = _pointer(pipe)
    assert _versions(pipe) == [ptr["path"]] and ptr["md5"] == push["catalog_md5"]
    assert pipe.store.content(ptr["path"]) == pipe.store.content(CATALOG) and ptr["snapshotId"] == push["snapshot_id"]
    assert stream.run_chunk(SITE, c.chunk_id, stages=["apply", "history", "verify"]) == 0
    con = pipe.site.con
    # N3: no lake_pushes row covers the chunk (e.g. a journal restored from an older backup), but the catalog the
    # Data Store serves holds the chunk's stack snapshot: the chunk is safe there
    chunks.set_status(con, SITE, c.chunk_id, "recorded", done="recorded")
    con.execute("DELETE FROM lake_pushes")
    assert stream.verify_chunk(SITE, c.chunk_id) == 0
    assert stream.push_lake(SITE) == 0  # the Data Store already holds this catalog: only journaled again
    assert chunks.last_lake_push(SITE)["catalog_md5"] == push["catalog_md5"] and _versions(pipe) == [ptr["path"]]
    # another VM published a lake without this chunk: the chunk is not safe there any more
    chunks.set_status(con, SITE, c.chunk_id, "recorded", done="recorded")
    publish_foreign(pipe, tmp_path / "vm-b")
    assert stream.verify_chunk(SITE, c.chunk_id) == 2 and "not a push of this host" in _error(c.chunk_id)
    assert "lacks snapshot" in _error(c.chunk_id)
    chunks.set_status(con, SITE, c.chunk_id, "recorded", done="recorded")
    con.execute("DELETE FROM lake_pushes")
    assert stream.verify_chunk(SITE, c.chunk_id) == 2 and "never pushed" in _error(c.chunk_id)


# ---------------------------------------------------------------- takeover (review B2 items 1, 3, 4)

def test_a_site_taken_over_by_another_vm_keeps_its_lake(pipe: SimpleNamespace, tmp_path: Path,  # noqa: F811
                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    vm_a = pipe.root
    os_c, is_c = _chunk(pipe, OS, "2023-05"), _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, os_c.chunk_id) == 0  # VM A streams the OS chunk away completely
    assert stream.download_chunk(SITE, is_c.chunk_id) == 0
    a_catalog = pipe.store.content(CATALOG)
    # VM A dies. VM B has A's journal (restored from the Data Store) and downloads the IS chunk again
    vm_b = tmp_path / "vm-b"
    _copy_journal(vm_a / "work" / "state.sqlite", vm_b / "work" / "state.sqlite")
    for rel in _files(vm_a, IS, is_c.months):
        (vm_b / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(vm_a / rel, vm_b / rel)
    _use_root(monkeypatch, vm_b)
    # B3: a lake that exists without the stacked chunks (e.g. written by some other step) is never stacked onto
    lake = site_lake.open_site_lake(SITE)
    site_lake.checkpoint_detach(lake, SITE)
    lake.close()
    assert stream.stack_chunk(SITE, is_c.chunk_id) == 1
    ch = chunks.get_chunk(SITE, is_c.chunk_id)
    assert (ch.status, ch.done) == ("downloaded", "downloaded") and "restore_lake" in ch.error
    assert not _stacked_in_lake()  # nothing was written
    # restore: the Data Store lake replaces the local one (kept aside), the stacked chunks are covered again
    assert stream.restore_lake(SITE) == 0
    lake_dir = site_lake.site_paths(SITE).lake_dir
    assert list(lake_dir.glob("neon_test.ducklake.replaced-*"))
    assert chunks.last_lake_push(SITE)["kind"] == "restore"
    assert stream.stack_chunk(SITE, is_c.chunk_id) == 0
    assert _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]  # VM A's rows, never re-stacked
    assert _q(f"SELECT count(*) FROM lake.{SAAT}") == [(2004,)]
    # B pushes its lake: the Data Store copy is the one it restored, so the guard lets it through
    assert stream.run_chunk(SITE, is_c.chunk_id, stages=["avus", "upload"]) == 0
    b_catalog = pipe.store.content(CATALOG)
    assert b_catalog != a_catalog and _md5(b_catalog) == chunks.last_lake_push(SITE)["catalog_md5"]
    # B4: VM A comes back with its older lake: neither a chunk push nor the finalize upload overwrites B's catalog
    _use_root(monkeypatch, vm_a)
    assert stream.push_lake(SITE) == 1
    card = site_lake.site_paths(SITE).site_dir / "cards" / "stale.md"
    card.parent.mkdir(parents=True, exist_ok=True)
    card.write_text("A's stale card")
    assert upload.finalize(SITE) == 1
    assert pipe.store.content(CATALOG) == b_catalog
    assert f"{S}/cards/stale.md" not in pipe.store.objs  # a refused lake uploads no side file either
    # A restores B's lake (A's is a prefix of it) and may push again afterwards
    assert stream.restore_lake(SITE) == 0
    assert stream.push_lake(SITE) == 0 and pipe.store.content(CATALOG) == b_catalog
    assert _q(f"SELECT count(*) FROM lake.{SAAT}") == [(2004,)]


def _stacked_in_lake() -> dict[str, int]:
    con = site_lake.open_site_lake(SITE, read_only=True)
    try:
        return site_lake.stream_chunk_ids(con, SITE)
    finally:
        con.close()


def test_a_new_vm_without_the_lake_restores_it_before_writing(pipe: SimpleNamespace, tmp_path: Path,  # noqa: F811
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    os_c, is_c = _chunk(pipe, OS, "2023-05"), _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, os_c.chunk_id) == 0
    vm_b = tmp_path / "vm-b"
    _copy_journal(pipe.root / "work" / "state.sqlite", vm_b / "work" / "state.sqlite")
    for rel in _files(pipe.root, IS, is_c.months):
        (vm_b / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pipe.root / rel, vm_b / rel)
    _use_root(monkeypatch, vm_b)
    assert not site_lake.site_paths(SITE).catalog_file.exists()
    # the download writes the lake's files table: the lake comes back from the Data Store first
    assert stream.download_chunk(SITE, is_c.chunk_id) == 0
    assert chunks.last_lake_push(SITE)["kind"] == "restore"
    assert stream.stack_chunk(SITE, is_c.chunk_id) == 0
    assert _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]


# ---------------------------------------------------------------- a cleaned-up site gains files (review M7)

def _finalize_fakes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from neon_ducklake import verify_site
    from neon_ducklake.site import cards, curate, validate

    calls: list[str] = []
    for mod, name in ((terms, "terms"), (cards, "cards"), (curate, "curate"), (validate, "validate")):
        monkeypatch.setattr(mod, "main", lambda site, *a, _n=name, **k: calls.append(_n) or 0)
    monkeypatch.setattr(validate, "has_proposals", lambda site: True)
    monkeypatch.setattr(verify_site, "main", lambda site, **kw: calls.append(f"verify:{kw}") or 0)
    return calls


def test_a_cleaned_site_is_restored_before_a_replanned_chunk(pipe: SimpleNamespace,  # noqa: F811
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    for c in pipe.plan:
        assert stream.run_chunk(SITE, c.chunk_id) == 0, _error(c.chunk_id)
    calls = _finalize_fakes(monkeypatch)
    assert stream.finalize_site(SITE, rate=60.0) == 0
    assert "verify:{'metadata_only': True}" in calls  # M11: the finalize verify is metadata-only
    assert stream.cleanup_site(SITE, session=pipe.session) == 0 and chunks.site_cleaned(SITE)
    data_dir = site_lake.site_paths(SITE).data_dir
    assert not [p for p in data_dir.rglob("*") if p.is_file()]
    # a re-list finds a new file in a month of the finished OS chunk
    new = f"NEON.D99.{SITE}.{OS}.brd_countdata.2024-05.basic.20260301T000000Z.csv"
    pipe.site.add(OS, "2024-05", new, _csv(["uid", "plotID"], [["u9", "P_009"]]), "data")
    con = pipe.site.con
    con.execute("UPDATE site_files SET dl_status=NULL WHERE name=?", (new,))
    plan = chunks.plan_chunks(SITE)
    os_c = next(c for c in plan if c.product == OS)
    assert (os_c.status, os_c.done) == ("planned", "planned")
    steps = dict(con.execute("SELECT step, status FROM stream_sites WHERE site=?", (SITE,)).fetchall())
    assert steps == {"cleanup": "ok"}  # the finalize steps are reset; cleanup stays until the lake is back
    # a plain restore (the worker's site preparation) leaves a cleaned site's Parquet in the Data Store
    assert stream.restore_lake(SITE) == 0
    assert chunks.site_cleaned(SITE) and not [p for p in data_dir.rglob("*") if p.is_file()]

    def fake_download(step: str, site: str, products: list[str] | None = None, months: list[str] | None = None,
                      threads: int = 8, lock_blocking: bool = False) -> int:
        c2 = state.connect(state.DB_PATH)
        try:
            c2.execute("UPDATE site_files SET dl_status='ok' WHERE site=? AND dl_status IS NULL", (site,))
            fetch.Fetcher(site=site, bundle=None, con=c2, pairs=[]).write_lake("download", products)
        finally:
            c2.close()
        return 0

    monkeypatch.setattr(fetch, "main", fake_download)
    assert stream.download_chunk(SITE, os_c.chunk_id) == 0, _error(os_c.chunk_id)
    assert not chunks.site_cleaned(SITE) and [p for p in data_dir.rglob("*") if p.is_file()]
    # only the new file is on disk; the evicted ones are not "missing": their rows stay in the lake
    assert stream.stack_chunk(SITE, os_c.chunk_id) == 0, _error(os_c.chunk_id)
    assert _q(f"SELECT uid FROM lake.{BIRD} ORDER BY uid") == [("u1",), ("u2",), ("u3",), ("u9",)]
    assert chunks.get_chunk(SITE, os_c.chunk_id).info["stack"]["files"] == 1


# ---------------------------------------------------------------- working set + superseded files (M12, #9, #10)

def test_workset_counts_superseded_files_and_the_lake_and_delete_removes_them(
        pipe: SimpleNamespace) -> None:  # noqa: F811
    con = pipe.site.con
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack"]) == 0
    raw = sum((pipe.root / r[0]).stat().st_size for r in con.execute("SELECT local_path FROM site_files"))
    lake = site_lake.data_bytes(SITE)
    assert lake > 0 and fetch.local_workset_bytes() == raw + lake == fetch.local_workset_bytes(SITE)
    # a later listing superseded a downloaded file of the chunk: it still takes local disk
    old = pipe.site.add(OS, "2023-05", f"NEON.D99.{SITE}.{OS}.brd_countdata.2023-05.basic.20230101T000000Z.csv",
                        b"x" * 1000, "data")
    con.execute("INSERT INTO fetch_superseded VALUES (?,?,?,?,?,?)", (SITE, OS, "2023-05", old.name, 1, "t"))
    assert fetch.local_workset_bytes() == raw + lake + 1000
    assert stream.run_chunk(SITE, c.chunk_id) == 0, _error(c.chunk_id)
    assert not old.exists()
    assert con.execute("SELECT dl_status FROM site_files WHERE name=?", (old.name,)).fetchone()[0] == "superseded"
    info = chunks.get_chunk(SITE, c.chunk_id).info["delete"]
    assert info["superseded"] == 1 and info["supersededBytes"] == 1000
    left = sum((pipe.root / r[0]).stat().st_size for r in con.execute(
        "SELECT local_path FROM site_files WHERE dl_status='ok' AND coalesce(local_evicted, 0) = 0"))
    assert fetch.local_workset_bytes() == left + site_lake.data_bytes()


def test_the_download_wait_counts_the_site_lake(pipe: SimpleNamespace) -> None:  # noqa: F811
    con = pipe.site.con
    c = _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack"]) == 0
    os_c = _chunk(pipe, OS, "2023-05")
    con.execute("UPDATE site_files SET dl_status=NULL WHERE product=?", (OS,))
    need, _have = fetch.chunk_bytes_needed(con, SITE, OS, os_c.months)
    other = fetch.local_raw_bytes(con, exclude=(SITE, OS, os_c.months))
    lake = site_lake.data_bytes()
    assert need and other and lake
    no = fetch.wait_for_workset(SITE, OS, os_c.months, cap=other + need + lake - 1, timeout_s=0)
    assert not no["ok"] and no["lake"] == lake
    yes = fetch.wait_for_workset(SITE, OS, os_c.months, cap=other + need + lake, timeout_s=0)
    assert yes["ok"]
    fetch.release_workset(yes["key"])


# ---------------------------------------------------------------- the stage wrapper (review #15)

def test_a_stage_finished_by_another_process_is_not_run_again(pipe: SimpleNamespace,  # noqa: F811
                                                             monkeypatch: pytest.MonkeyPatch) -> None:
    c = _chunk(pipe, OS, "2023-05")
    real_lock = env.file_lock

    @contextlib.contextmanager
    def racing_lock(name: str, **kw: Any):
        with real_lock(name, **kw):
            if name == f"stream-{SITE}-{c.chunk_id}":  # the other process finished while we took the lock
                chunks.set_status(pipe.site.con, SITE, c.chunk_id, "downloaded", done="downloaded")
            yield

    monkeypatch.setattr(env, "file_lock", racing_lock)
    monkeypatch.setattr(fetch, "download_rows", lambda *a, **k: pytest.fail("the stage ran again"))
    assert stream.download_chunk(SITE, c.chunk_id) == 0
    ch = chunks.get_chunk(SITE, c.chunk_id)
    assert (ch.status, ch.done, ch.attempt) == ("downloaded", "downloaded", 0)


# ---------------------------------------------------------------- finalize and other VMs' curation (M8)

def test_finalize_waits_for_generic_curation_claimed_elsewhere(pipe: SimpleNamespace,  # noqa: F811
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake.site import avus, validate

    con = pipe.site.con
    for c in pipe.plan:
        chunks.set_status(con, SITE, c.chunk_id, "deleted", done="deleted")
    calls = _finalize_fakes(monkeypatch)
    pending = {"now": [OS]}
    monkeypatch.setattr(validate, "pending_generic", lambda site: list(pending["now"]), raising=False)

    def build(*a: Any, **k: Any) -> dict[str, Any]:
        calls.append("avus")
        raise RuntimeError("stop here")

    monkeypatch.setattr(avus, "build_desired", build)
    assert stream.finalize_site(SITE) == 1
    assert calls == ["terms", "cards", "curate", "validate"]  # no product AVUs without the curated terms
    assert chunks.step_status(SITE, "curate") == "waiting" and chunks.step_status(SITE, "avus") is None
    assert stream.finalize_site(SITE) == 1 and calls.count("curate") == 2  # curate runs again (its prefetch)
    pending["now"] = []  # the other VM published its result
    assert stream.finalize_site(SITE) == 2  # (the fake build stops the run)
    assert calls[-2:] == ["curate", "avus"] and chunks.step_status(SITE, "curate") == "ok"


# ---------------------------------------------------------------- the chunk's taxa (M6)

def test_avus_chunk_maps_the_chunks_taxa_before_building_object_avus(pipe: SimpleNamespace,  # noqa: F811
                                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(terms, "resolve_chunk_taxa", REAL_RESOLVE_CHUNK_TAXA)
    asked: list[list[str]] = []

    def fake_resolve(taxa: list[terms.Taxon]) -> dict[tuple[str, str], terms.Resolution]:
        asked.append(sorted(t.taxonID for t in taxa))
        return {t.key: terms.Resolution(f"NCBITaxon:{100 + i}", f"{t.neonName} label", "exact", "ok", t.queryName)
                for i, t in enumerate(taxa)}

    monkeypatch.setattr(terms, "resolve_taxa", fake_resolve)
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus"]) == 0, _error(c.chunk_id)
    assert asked == [["CACW", "MODO"]]
    assert chunks.get_chunk(SITE, c.chunk_id).info["avus"]["taxa"]["ok"] == 2
    got = {(p.rsplit("/", 1)[1], a) for p, a in pipe.site.con.execute(
        "SELECT path, attribute FROM avu_desired WHERE level='object' AND attribute LIKE 'ncbitaxon.%'")}
    assert got and all("brd_countdata" in name for name, _a in got)
    assert not pipe.site.con.execute("SELECT 1 FROM avu_desired WHERE attribute='neon.taxon'").fetchone()
    # N10: OLS unavailable: the taxa stay pending and the stage asks to be retried later (rc 4) instead of tagging
    # placeholders; the chunk keeps its status (it is not failed)
    other = _chunk(pipe, IS, "2019-05")
    monkeypatch.setattr(terms, "resolve_chunk_taxa", lambda site, product, months: {"pending": 3})
    assert stream.run_chunk(SITE, other.chunk_id, stages=["download", "stack", "avus"]) == stream.RC_RETRY == 4
    ch = chunks.get_chunk(SITE, other.chunk_id)
    assert (ch.status, ch.done) == ("stacked", "stacked") and "retry later" in ch.error and "pending" in ch.error
    monkeypatch.setattr(terms, "resolve_chunk_taxa", lambda site, product, months: {"pending": 0})
    assert stream.avus_chunk(SITE, other.chunk_id) == 0


# ---------------------------------------------------------------- lake readers hold the site-lake lock (M5)

def test_lake_readers_take_the_site_lake_lock(pipe: SimpleNamespace,  # noqa: F811
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake import verify_site
    from neon_ducklake.site import avus

    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack"]) == 0
    real_lock = env.file_lock
    held: list[str] = []
    inside: list[bool] = []

    @contextlib.contextmanager
    def recording_lock(name: str, **kw: Any):
        with real_lock(name, **kw):
            held.append(name)
            try:
                yield
            finally:
                held.remove(name)

    real_attach = site_lake.attach

    def attach(con: Any, site: str, **kw: Any) -> Any:
        inside.append(f"site-lake-{site.upper()}" in held)
        return real_attach(con, site, **kw)

    monkeypatch.setattr(env, "file_lock", recording_lock)
    monkeypatch.setattr(site_lake, "attach", attach)
    avus.load_facts(SITE, products=[OS], months=["2023-05"])
    verify_site.load_site_data(SITE)
    monkeypatch.setattr(terms, "resolve_chunk_taxa", REAL_RESOLVE_CHUNK_TAXA)
    monkeypatch.setattr(terms, "resolve_taxa", lambda taxa: {})
    terms.resolve_chunk_taxa(SITE, OS, ["2023-05"])
    assert len(inside) >= 3 and all(inside)
