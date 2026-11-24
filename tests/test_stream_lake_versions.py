"""The site lake on the Data Store, review round 3: versioned catalogs behind ``ducklake/CURRENT`` (N5), the
``lake-push-<SITE>`` host lock and the snapshot-key check of verify (N3), catalog writes only under the site's
claim (N7), restore re-planning the chunks the lake does not cover and stale raw files (N9), a cleaned site
re-listed through the real ``site list`` path (N4)."""

from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from neon_ducklake import coord, env, state
from neon_ducklake.site import chunks, fetch, site_lake, stream, upload

from .test_avu_fakes import FakeDataObjects
from .test_coord_fakes import DSIrods, DSSession
from .test_site_common import SITE, site_env  # noqa: F401 - fixtures
from .test_site_fetch import fake  # noqa: F401 - fixture (scripted NEON API)
from .test_site_stack import IS, OS, _q
from .test_stream_lake import (
    BIRD,
    CATALOG,
    POINTER,
    SAAT,
    VERSIONS,
    _copy_journal,
    _error,
    _files,
    _md5,
    _pointer,
    _use_root,
    _versions,
)
from .test_stream_stages import S, _chunk, pipe  # noqa: F401 - pipe is a fixture

REAL_PUT = FakeDataObjects.put


def _vm_b(pipe: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:  # noqa: F811
    """A second VM with a copy of this VM's journal (a journal restore) and no local lake."""
    vm_b = tmp_path / "vm-b"
    _copy_journal(pipe.root / "work" / "state.sqlite", vm_b / "work" / "state.sqlite")
    _use_root(monkeypatch, vm_b)
    return vm_b


def _tear(store: DSIrods, path: str, data: bytes) -> None:
    """``path`` as a killed put leaves it: part of the bytes, no good replica."""
    with store.lock:
        store.add_obj(path, content=data[:100], replicas=[{"number": 0, "size": 100, "checksum": "", "status": "0"}])


# ---------------------------------------------------------------- N3: verify(k) vs the push of chunk k+1

def test_verify_waits_for_the_catalog_push_of_the_next_chunk(pipe: SimpleNamespace,  # noqa: F811
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    a, b = _chunk(pipe, OS, "2023-05"), _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, a.chunk_id, stages=["download", "stack", "avus", "upload", "apply", "history"]) == 0
    assert stream.run_chunk(SITE, b.chunk_id, stages=["download", "stack", "avus"]) == 0
    mid, go = threading.Event(), threading.Event()

    def put(self: FakeDataObjects, local: str, irods_path: str, num_threads: int = 0, **opts: Any) -> None:
        if irods_path.endswith(".ducklake") and not mid.is_set():
            with self.store.lock:  # the catalog objects are being written: no good replica until the put ends
                for p in (irods_path, CATALOG):
                    for r in (self.store.objs.get(p) or {}).get("replicas", []):
                        r["status"] = "2"
            mid.set()
            assert go.wait(30)
        return REAL_PUT(self, local, irods_path, num_threads, **opts)

    monkeypatch.setattr(FakeDataObjects, "put", put)
    rcs: dict[str, int] = {}
    up = threading.Thread(target=lambda: rcs.__setitem__("upload", stream.upload_chunk(SITE, b.chunk_id)))
    up.start()
    assert mid.wait(30)
    ver = threading.Thread(target=lambda: rcs.__setitem__("verify", stream.verify_chunk(SITE, a.chunk_id)))
    ver.start()
    ver.join(1.0)
    waited = ver.is_alive()  # blocked on the host lock lake-push-<SITE> while the push publishes
    go.set()
    up.join(60)
    ver.join(60)
    assert rcs == {"upload": 0, "verify": 0}, (_error(a.chunk_id), _error(b.chunk_id))
    assert waited
    assert chunks.get_chunk(SITE, a.chunk_id).done == "verified"


def test_verify_retries_while_the_data_store_catalog_is_being_written(pipe: SimpleNamespace) -> None:  # noqa: F811
    a = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, a.chunk_id, stages=["download", "stack", "avus", "upload", "apply", "history"]) == 0
    pipe.site.con.execute("DELETE FROM lake_pushes")  # no push of this host vouches for the chunk
    whole = {p: pipe.store.content(p) for p in (_pointer(pipe)["path"], CATALOG)}
    for p, data in whole.items():  # both catalog copies mid-put: part of the bytes, no good replica
        o = pipe.store.objs[p]
        o["content"] = data[:64]
        for r in o["replicas"]:
            r["status"] = "2"
    assert stream.verify_chunk(SITE, a.chunk_id) == stream.RC_RETRY == 4
    ch = chunks.get_chunk(SITE, a.chunk_id)
    assert (ch.status, ch.done) == ("recorded", "recorded") and "being written" in ch.error
    for p, data in whole.items():  # the put finished
        pipe.store.set_content(p, data, register=True)
    assert stream.verify_chunk(SITE, a.chunk_id) == 0  # proven by the catalog's snapshot list


# ---------------------------------------------------------------- N5: versioned catalogs + CURRENT

def test_a_killed_catalog_put_leaves_the_previous_version_and_versions_are_never_overwritten(
        pipe: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    os_c, is_c = _chunk(pipe, OS, "2023-05"), _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, os_c.chunk_id) == 0
    v1 = _pointer(pipe)
    assert stream.run_chunk(SITE, is_c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    v2 = _pointer(pipe)
    assert v2["path"] != v1["path"] and v2["previous"] == v1["catalog"]
    assert _versions(pipe) == sorted([v1["path"], v2["path"]]) and v2["path"].startswith(f"{VERSIONS}neon_test.")
    assert _md5(pipe.store.content(v1["path"])) == v1["md5"]  # the first version is untouched
    # a later local change; the VM is killed while it puts the new versioned catalog
    lake = site_lake.open_site_lake(SITE)
    with site_lake.commit(lake, "a later local change"):
        lake.execute("CREATE TABLE lake.extra AS SELECT 1 AS x")
    lake.close()
    seen: list[str] = []

    def killed(self: FakeDataObjects, local: str, irods_path: str, num_threads: int = 0, **opts: Any) -> None:
        if irods_path.startswith(VERSIONS) and irods_path not in seen:
            seen.append(irods_path)
            _tear(self.store, irods_path, Path(local).read_bytes())
            raise RuntimeError("PUT_KILLED: the VM died mid-put")
        return REAL_PUT(self, local, irods_path, num_threads, **opts)

    monkeypatch.setattr(FakeDataObjects, "put", killed)
    res = upload.push_lake(SITE)
    assert res["failures"] and seen and _pointer(pipe) == v2  # CURRENT still names the previous version
    assert _md5(pipe.store.content(CATALOG)) == v2["md5"]
    # another VM restores from the Data Store meanwhile: it reads CURRENT, never the torn object
    root_a = env.ROOT
    _vm_b(pipe, tmp_path, monkeypatch)
    assert stream.restore_lake(SITE) == 0
    assert _md5(site_lake.site_paths(SITE).catalog_file.read_bytes()) == v2["md5"]
    assert _q(f"SELECT count(*) FROM lake.{SAAT}") == [(2004,)]
    _use_root(monkeypatch, root_a)
    # the retry writes the same bytes to the same name (the torn object is completed), then moves CURRENT
    res = upload.push_lake(SITE)
    assert not res["failures"], res
    v3 = _pointer(pipe)
    assert v3["path"] == seen[0] and v3["previous"] == v2["catalog"] and _md5(pipe.store.content(seen[0])) == v3["md5"]
    # a versioned name that already holds other bytes is never overwritten: the push is refused instead
    lake = site_lake.open_site_lake(SITE)
    with site_lake.commit(lake, "another local change"):
        lake.execute("CREATE TABLE lake.extra2 AS SELECT 2 AS x")
    lake.close()
    assert upload.push_lake(SITE)["failures"] and len(seen) == 2  # learns the next name (killed again)
    pipe.store.set_content(seen[1], b"someone else's bytes", register=True)
    res = upload.push_lake(SITE)
    assert res["failures"] and "never overwritten" in res["error"]
    assert pipe.store.content(seen[1]) == b"someone else's bytes" and _pointer(pipe) == v3


def test_restore_falls_back_to_the_newest_verifying_version(pipe: SimpleNamespace, tmp_path: Path,  # noqa: F811
                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    os_c, is_c = _chunk(pipe, OS, "2023-05"), _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, is_c.chunk_id) == 0
    v1 = _pointer(pipe)
    assert stream.run_chunk(SITE, os_c.chunk_id) == 0  # deleted: its raw files are evicted locally
    v2 = _pointer(pipe)
    # the version CURRENT names is damaged (its bytes no longer match the md5 its name pins), and the canonical
    # copy is torn: a VM taking the site over falls back to the newest version that verifies
    pipe.store.set_content(v2["path"], b"damaged", register=True)
    pipe.store.set_content(CATALOG, b"torn", register=True)
    _vm_b(pipe, tmp_path, monkeypatch)
    assert stream.restore_lake(SITE) == 0
    assert _md5(site_lake.site_paths(SITE).catalog_file.read_bytes()) == v1["md5"]
    report = json.loads((site_lake.site_paths(SITE).reports_dir / "restore_lake.json").read_text())
    assert report["used"]["source"] == "version" and report["used"]["path"] == v1["path"]
    # N9: that version lacks the OS chunk: it is re-planned, and its evicted raw files are fetched again
    ch = chunks.get_chunk(SITE, os_c.chunk_id)
    assert (ch.status, ch.done) == ("planned", "planned") and "lacks its stack" in ch.error
    assert report["uncovered"] == report["replanned"] == [os_c.chunk_id]
    con = state.connect(state.DB_PATH)
    try:
        rows = con.execute("SELECT DISTINCT dl_status, local_evicted FROM site_files WHERE product=?", (OS,)).fetchall()
    finally:
        con.close()
    assert [tuple(r) for r in rows] == [("stale", 1)]
    assert chunks.get_chunk(SITE, is_c.chunk_id).status == "deleted"  # covered: untouched


# ---------------------------------------------------------------- N7: catalog writes need the site's claim

def test_the_catalog_is_written_only_under_the_sites_claim(pipe: SimpleNamespace,  # noqa: F811
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEON_DUCKLAKE_VM_ID", "vm-a")
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus"]) == 0
    other = coord.claim_site(SITE, session=pipe.session, vm="vm-b", settle_s=0)
    assert other.acquired  # this VM lost the site to vm-b (its heartbeat has not noticed yet)
    assert stream.upload_chunk(SITE, c.chunk_id) == 1
    ch = chunks.get_chunk(SITE, c.chunk_id)
    assert (ch.status, ch.done) == ("avus", "avus") and "claim lost" in ch.error and "vm-b" in ch.error
    assert not _versions(pipe) and POINTER not in pipe.store.objs and CATALOG not in pipe.store.objs
    assert not chunks.lake_pushes(SITE)
    assert stream.push_lake(SITE) == 1  # the finalize and cleanup pushes go the same way
    # NEON_DUCKLAKE_REQUIRE_CLAIM=1: no claim at all is no permission either
    assert coord.release(other.claim, session=pipe.session)
    monkeypatch.setenv(upload.REQUIRE_CLAIM_ENV, "1")
    assert stream.push_lake(SITE) == 1 and not _versions(pipe)
    # this VM's live claim: the push goes through
    with coord.site_claim(SITE, session=pipe.session, vm="vm-a", settle_s=0, heartbeat_s=3600):
        assert stream.upload_chunk(SITE, c.chunk_id) == 0
    assert len(_versions(pipe)) == 1 and _pointer(pipe)["vm"] == "vm-a" and CATALOG in pipe.store.objs


# ---------------------------------------------------------------- N9: restore re-plans; stale raw files

def test_restore_replans_a_stacked_chunk_the_lake_lacks(pipe: SimpleNamespace) -> None:  # noqa: F811
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus"]) == 0
    # the lake holding this chunk never reached the Data Store, and this host lost it (a disk, or a new VM)
    shutil.rmtree(site_lake.site_paths(SITE).lake_dir)
    assert stream.restore_lake(SITE) == 0
    ch = chunks.get_chunk(SITE, c.chunk_id)
    assert (ch.status, ch.done) == ("planned", "planned") and "lacks its stack" in ch.error
    assert stream.run_chunk(SITE, c.chunk_id) == 0, _error(c.chunk_id)  # and it streams through again
    assert _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]


def test_stale_raw_files_are_downloaded_again(pipe: SimpleNamespace,  # noqa: F811
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    is_c, os_c = _chunk(pipe, IS, "2019-05"), _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, is_c.chunk_id, stages=["download", "stack"]) == 0
    root = pipe.root
    originals = {Path(rel).name: (root / rel).read_bytes() for rel in _files(root, IS, is_c.months) +
                 _files(root, OS, os_c.months)}
    # a raw file of the stacked chunk is not the one the journal describes (same size, other bytes)
    stale = next(root / rel for rel in _files(root, IS, is_c.months) if "SAAT_30min.2024-06" in rel)
    stale.write_bytes(originals[stale.name][::-1])
    assert stream.restore_lake(SITE) == 0
    ch = chunks.get_chunk(SITE, is_c.chunk_id)
    assert (ch.status, ch.done) == ("planned", "planned") and "stale" in ch.error
    con = pipe.site.con
    assert con.execute("SELECT dl_status FROM site_files WHERE name=?", (stale.name,)).fetchone()[0] is None
    assert not stale.exists()
    # the download of a chunk checks its own files the same way (md5 against dl_md5)
    other = next(root / rel for rel in _files(root, OS, os_c.months) if "brd_countdata.2023-05" in rel)
    other.write_bytes(originals[other.name][::-1])

    def neon_download(step: str, site: str, products: list[str] | None = None, months: list[str] | None = None,
                      threads: int = 8, lock_blocking: bool = False) -> int:  # NEON serves the listed bytes
        c2 = state.connect(state.DB_PATH)
        try:
            for r in c2.execute(f"SELECT name, local_path FROM site_files WHERE site=? AND dl_status IS NULL AND "
                                f"product IN ({','.join('?' * len(products or []))})", (site, *(products or []))):
                (env.ROOT / r["local_path"]).write_bytes(originals[r["name"]])
                c2.execute("UPDATE site_files SET dl_status='ok' WHERE name=?", (r["name"],))
            fetch.Fetcher(site=site, bundle=None, con=c2, pairs=[]).write_lake("download", products)
        finally:
            c2.close()
        return 0

    monkeypatch.setattr(fetch, "main", neon_download)
    assert stream.download_chunk(SITE, os_c.chunk_id) == 0, _error(os_c.chunk_id)
    assert other.read_bytes() == originals[other.name]
    assert stream.run_chunk(SITE, is_c.chunk_id, stages=["download"]) == 0
    assert stale.read_bytes() == originals[stale.name]


# ---------------------------------------------------------------- N4: a cleaned site is listed again

def test_a_cleaned_site_is_listed_again_through_the_real_list_path(site_env: Path, fake: Any,  # noqa: F811
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake import mirror
    from neon_ducklake import site as sitecli

    store = DSIrods()
    session = DSSession(store)
    push = mirror.push_tree
    monkeypatch.setattr(mirror, "push_tree", lambda items, threads=8, record_site=None, **kw: push(
        items, threads=threads, record_site=record_site, session=session, sleep=lambda _s: None))
    monkeypatch.setattr(env, "mesa", lambda: SimpleNamespace(session=session))
    args = SimpleNamespace(step="list", site=SITE, chunk_site=None, chunk=None, chunk_bytes=None, products=None,
                           months=None, sample=None, threads=2)
    assert sitecli.dispatch(args) == 0
    con = state.connect(state.DB_PATH)
    try:
        for c in chunks.plan_chunks(SITE, con=con):  # the site streamed through (no rows to stack) and finalized
            chunks.set_status(con, SITE, c.chunk_id, "deleted", done="deleted",
                              info={"stack": {"status": "empty", "files": 0}})
        for step in stream.FINALIZE_STEPS:
            chunks.set_step(con, SITE, step, "ok")
    finally:
        con.close()
    before = _q("SELECT count(*) FROM lake.files")
    assert stream.push_lake(SITE, session=session) == 0
    assert stream.cleanup_site(SITE, session=session) == 0 and chunks.site_cleaned(SITE)
    data = site_lake.site_paths(SITE).data_dir
    assert not [p for p in data.rglob("*") if p.is_file()]
    # a new NEON release: the site is listed again; the listing upserts into lake tables whose Parquet lives
    # only in the Data Store, so the lake data comes back first
    assert sitecli.dispatch(args) == 0
    assert not chunks.site_cleaned(SITE) and [p for p in data.rglob("*") if p.is_file()]
    assert _q("SELECT count(*) FROM lake.files") == before
    assert _q("SELECT count(*) FROM lake.site_months")[0][0] > 0
    first = _pointer(SimpleNamespace(store=store))  # nothing changed: the Data Store lake is still current
    # the finished site's cleanup runs again; lake rows written since finalize (a listing that changed something)
    # reach the Data Store before the local data is deleted, instead of being kept locally
    lake = site_lake.open_site_lake(SITE)
    with site_lake.commit(lake, "a listing change"):
        lake.execute("CREATE TABLE lake.relisted AS SELECT 1 AS x")
    lake.close()
    assert stream.cleanup_site(SITE, session=session) == 0
    now = _pointer(SimpleNamespace(store=store))
    assert now["catalog"] != first["catalog"] and now["previous"] == first["catalog"]
    assert not [p for p in data.rglob("*") if p.is_file()]


def test_old_catalog_versions_are_pruned_but_never_the_current_ones(pipe: SimpleNamespace,  # noqa: F811
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(upload.VERSIONS_KEEP_ENV, "2")
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    leftover = f"{S}/ducklake/CURRENT.new-deadbeef0000"  # an interrupted pointer update, long ago
    pipe.store.add_obj(leftover, content=b"{}")
    pipe.store.mtimes[leftover] = pipe.store.clock().replace(year=2020)
    pointers = [_pointer(pipe)]
    for i in range(3):
        lake = site_lake.open_site_lake(SITE)
        with site_lake.commit(lake, f"local change {i}"):
            lake.execute(f"CREATE TABLE lake.extra{i} AS SELECT {i} AS x")
        lake.close()
        assert stream.push_lake(SITE) == 0
        pointers.append(_pointer(pipe))
    assert [p["previous"] for p in pointers[1:]] == [p["catalog"] for p in pointers[:-1]]
    assert _versions(pipe) == sorted([pointers[-1]["path"], pointers[-2]["path"]])  # the newest two
    assert leftover not in pipe.store.objs
    assert _md5(pipe.store.content(pointers[-1]["path"])) == pointers[-1]["md5"]
