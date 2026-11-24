"""R2-1: ``restore_lake`` syncs only the data files the catalog being installed references, and only from good
replicas. A lake push interrupted while it uploads a data file leaves that object registered with a stale replica
(size 0 or partial, no checksum) that no published catalog references: it must neither wedge the restore (a locked
or unreadable replica) nor overwrite the good local file (a readable one), on this host or on a VM taking the site
over. The verified fetch refuses an object of size 0 or without a checksum."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from neon_ducklake import coord
from neon_ducklake.site import chunks, site_lake, stream, upload

from .test_avu_fakes import FakeDataObjects
from .test_coord_fakes import DSDataObjects, DSIrods
from .test_site_common import SITE, site_env  # noqa: F401 - fixtures
from .test_site_stack import IS, OS, _q
from .test_stream_lake import BIRD, CATALOG, POINTER, SAAT, VERSIONS, _error, _md5, _pointer
from .test_stream_lake_versions import _vm_b
from .test_stream_stages import S, _chunk, pipe  # noqa: F401 - pipe is a fixture

REAL_PUT = FakeDataObjects.put
REAL_GET = DSDataObjects.get
DATA = f"{S}/ducklake/data/"


def _data_files() -> dict[Path, bytes]:
    d = site_lake.site_paths(SITE).data_dir
    return {p: p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}


def _torn(store: DSIrods, path: str, data: bytes) -> None:
    """``path`` as an interrupted put leaves it: registered, part of the bytes, a stale replica of size 0 and no
    checksum."""
    with store.lock:
        store.add_obj(path, content=data[:100], replicas=[{"number": 0, "size": 0, "checksum": "", "status": "0"}])


def _report() -> dict[str, Any]:
    return json.loads((site_lake.site_paths(SITE).reports_dir / "restore_lake.json").read_text())


def _interrupted_push(pipe: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, *,  # noqa: F811
                      readable: bool) -> SimpleNamespace:
    """The OS chunk is pushed (CURRENT = v1). The IS chunk is stacked here, and its lake push is killed while it
    uploads the new data files: each is left torn in the Data Store, no catalog refers to them. ``readable``: a
    get of a torn object returns its partial bytes; else it fails (a locked / inaccessible replica)."""
    os_c, is_c = _chunk(pipe, OS, "2023-05"), _chunk(pipe, IS, "2019-05")
    assert stream.run_chunk(SITE, os_c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    v1 = _pointer(pipe)
    before = set(_data_files())
    assert stream.run_chunk(SITE, is_c.chunk_id, stages=["download", "stack", "avus"]) == 0
    new = {p: b for p, b in _data_files().items() if p not in before}
    assert new
    torn = {upload.irods_path_for(p): p for p in new}
    state = SimpleNamespace(killing=True, locked=not readable)

    def killed(self: FakeDataObjects, local: str, irods_path: str, num_threads: int = 0, **opts: Any) -> None:
        if state.killing and irods_path in torn:
            _torn(self.store, irods_path, Path(local).read_bytes())
            raise RuntimeError("PUT_KILLED: the VM died mid-put")
        return REAL_PUT(self, local, irods_path, num_threads, **opts)

    def get(self: DSDataObjects, path: str, local_path: str | None = None, num_threads: int = 0, **opts: Any) -> Any:
        if local_path and state.locked and path in torn:
            raise RuntimeError(f"LOCKED_DATA_OBJECT_ACCESS: {path}")
        return REAL_GET(self, path, local_path, num_threads, **opts)

    monkeypatch.setattr(FakeDataObjects, "put", killed)
    monkeypatch.setattr(DSDataObjects, "get", get)
    res = upload.push_lake(SITE)
    assert res["failures"] and _pointer(pipe) == v1  # no catalog refers to the torn objects
    for path in torn:
        assert pipe.store.objs[path]["replicas"][0]["status"] == "0"
    state.killing = False
    return SimpleNamespace(os_c=os_c, is_c=is_c, v1=v1, new=new, torn=torn, state=state)


@pytest.mark.parametrize("readable", [True, False], ids=["readable-torn-replica", "locked-torn-replica"])
def test_an_interrupted_push_neither_wedges_nor_corrupts_the_restore_on_this_host(
        pipe: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, readable: bool) -> None:  # noqa: F811
    x = _interrupted_push(pipe, monkeypatch, readable=readable)
    assert stream.restore_lake(SITE) == 0  # the old code: rc 2 (locked), or the good local files overwritten
    for p, data in x.new.items():
        assert p.read_bytes() == data, p  # never replaced by the torn copies
    rep = _report()
    assert rep["dataFetched"] == 0 and rep["data"]["referenced"] == len(_data_files())
    assert not rep["data"]["unavailable"] and not rep["data"]["keptLocal"]
    # the upload stage completes the torn objects (the local files are intact), and the chunk streams through
    x.state.locked = False
    assert stream.run_chunk(SITE, x.is_c.chunk_id, stages=["upload", "apply", "history", "verify"]) == 0, \
        _error(x.is_c.chunk_id)
    for path, p in x.torn.items():
        assert pipe.store.content(path) == p.read_bytes() and pipe.store.objs[path]["replicas"][0]["status"] == "1"
    assert _pointer(pipe) != x.v1 and _q(f"SELECT count(*) FROM lake.{SAAT}") == [(2004,)]


@pytest.mark.parametrize("readable", [True, False], ids=["readable-torn-replica", "locked-torn-replica"])
def test_a_takeover_vm_fetches_only_what_the_published_catalog_references(
        pipe: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, readable: bool) -> None:  # noqa: F811
    x = _interrupted_push(pipe, monkeypatch, readable=readable)
    new_rel = {str(p.relative_to(site_lake.site_paths(SITE).data_dir)) for p in x.new}
    _vm_b(pipe, tmp_path, monkeypatch)  # VM A died; VM B has its journal and no lake
    assert stream.restore_lake(SITE) == 0  # the old code: rc 2 on every VM (locked), or orphans fetched
    paths = site_lake.site_paths(SITE)
    assert _md5(paths.catalog_file.read_bytes()) == x.v1["md5"]
    refs = site_lake.catalog_data_files(paths.catalog_file)
    got = _data_files()
    assert {str(p.relative_to(paths.data_dir)): len(b) for p, b in got.items()} == refs  # exactly those
    for p, data in got.items():
        assert data == pipe.store.content(upload.irods_path_for(p))
    assert not any((paths.data_dir / rel).exists() for rel in new_rel)  # the torn orphans stay in the Data Store
    assert not list(paths.data_dir.rglob("*.part"))
    rep = _report()
    assert rep["data"]["remoteUnreferenced"] == 0 and rep["dataFetched"] == len(refs)  # torn: never listed
    assert _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]
    ch = chunks.get_chunk(SITE, x.is_c.chunk_id)  # its stack is not in the published lake: re-planned
    assert (ch.status, ch.done) == ("planned", "planned") and "lacks its stack" in ch.error


def test_the_verified_fetch_refuses_a_replica_without_size_or_checksum(
        pipe: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    v1 = _pointer(pipe)
    path, data = next((p, pipe.store.content(p)) for p in sorted(pipe.store.objs) if p.startswith(DATA))
    # a referenced data file whose only replica is marked good but has no size and no checksum (e.g. a put
    # without a registered checksum that was cut short): its partial bytes must never be taken
    with pipe.store.lock:
        pipe.store.objs[path]["content"] = data[:50]
        pipe.store.objs[path]["replicas"] = [{"number": 0, "size": 0, "checksum": "", "status": "1"}]
    _vm_b(pipe, tmp_path, monkeypatch)
    paths = site_lake.site_paths(SITE)
    assert stream.restore_lake(SITE) == stream.RC_RETRY  # the old code: rc 0 with a truncated data file
    assert not paths.catalog_file.exists()  # a catalog whose data is not all here is not installed
    rep = _report()
    assert "good Data Store replica" in rep["retry"] and rep["data"]["unavailable"] == [path[len(DATA):]]
    local = paths.data_dir / path[len(DATA):]
    assert not local.exists() and not local.with_name(local.name + ".part").exists()
    pipe.store.set_content(path, data, register=True)  # the object is completed
    assert stream.restore_lake(SITE) == 0
    assert local.read_bytes() == data and _md5(paths.catalog_file.read_bytes()) == v1["md5"]
    # the helpers themselves: good_objects leaves such objects out, fetch_verified refuses them
    pipe.store.add_obj(f"{DATA}x/empty.parquet", content=b"", replicas=[{"number": 0, "size": 0, "checksum": "",
                                                                         "status": "1"}])
    pipe.store.add_obj(f"{DATA}x/nosum.parquet", content=b"abc", replicas=[{"number": 0, "size": 3, "checksum": "",
                                                                            "status": "1"}])
    pipe.store.add_obj(f"{DATA}x/stale.parquet", content=b"abc", replicas=[
        {"number": 0, "size": 3, "checksum": hashlib.md5(b"abc").hexdigest(), "status": "0"}])
    good = upload.good_objects(pipe.session, DATA.rstrip("/"), recursive=True)
    assert path in good and not any(p.startswith(f"{DATA}x/") for p in good)
    assert set(coord.list_objects(pipe.session, f"{DATA}x")) == {f"{DATA}x/{n}.parquet"
                                                                for n in ("empty", "nosum", "stale")}
    for name, size, checksum in (("empty", 0, "d41d8cd98f00b204e9800998ecf8427e"), ("nosum", 3, "")):
        dest = tmp_path / f"{name}.parquet"
        dest.write_bytes(b"keep")
        with pytest.raises(coord.TransferMismatch):
            upload.fetch_verified(pipe.session, coord.RemoteObject(f"{DATA}x/{name}.parquet", size, checksum, None),
                                  dest)
        assert dest.read_bytes() == b"keep"


def test_a_local_data_file_is_replaced_only_by_a_verified_good_replica(pipe: SimpleNamespace) -> None:  # noqa: F811
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    paths = site_lake.site_paths(SITE)
    local, data = next(iter(_data_files().items()))
    remote = upload.irods_path_for(local)
    # a local data file of another size than the catalog records (a crash while this host wrote it): the good
    # replica of the catalog's size replaces it, after its checksum verified
    rel = str(local.relative_to(paths.data_dir))
    local.write_bytes(data[:10])
    assert stream.restore_lake(SITE) == 0
    assert local.read_bytes() == data
    replaced = _report()
    # without a good replica the local file is kept (and logged), never replaced by what the Data Store holds
    local.write_bytes(data[:10])
    with pipe.store.lock:
        for r in pipe.store.objs[remote]["replicas"]:
            r["status"] = "0"
    assert stream.restore_lake(SITE) == 0
    assert local.read_bytes() == data[:10]  # the old code: overwritten by the stale replica's bytes
    assert replaced["data"]["replaced"] == [rel] and _report()["data"]["keptLocal"] == [rel]


def test_a_local_catalog_that_references_lost_unpushed_data_yields_to_the_data_store_copy(
        pipe: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    x = _interrupted_push(pipe, monkeypatch, readable=True)
    for p in x.new:  # this host lost the new files too: they exist nowhere
        p.unlink()
    assert stream.restore_lake(SITE) == 0
    paths = site_lake.site_paths(SITE)
    rep = _report()
    assert rep["replaceCatalog"] and "neither this host nor the Data Store" in rep["why"]
    assert _md5(paths.catalog_file.read_bytes()) == x.v1["md5"] and list(paths.lake_dir.glob("*.replaced-*"))
    assert not any(p.exists() for p in x.new)  # the torn orphans were never fetched
    ch = chunks.get_chunk(SITE, x.is_c.chunk_id)
    assert (ch.status, ch.done) == ("planned", "planned") and "lacks its stack" in ch.error
    assert stream.run_chunk(SITE, x.is_c.chunk_id) == 0, _error(x.is_c.chunk_id)  # streams through again
    assert _q(f"SELECT count(*) FROM lake.{SAAT}") == [(2004,)] and _q(f"SELECT count(*) FROM lake.{BIRD}") == [(3,)]


def test_a_catalog_copy_without_a_good_replica_is_never_installed(
        pipe: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    c = _chunk(pipe, OS, "2023-05")
    assert stream.run_chunk(SITE, c.chunk_id, stages=["download", "stack", "avus", "upload"]) == 0
    # only the canonical copy is left, and its replica is stale (size 0, no checksum) though its bytes open
    with pipe.store.lock:
        for p in [p for p in pipe.store.objs if p == POINTER or p.startswith(VERSIONS)]:
            del pipe.store.objs[p]
        pipe.store.objs[CATALOG]["replicas"] = [{"number": 0, "size": 0, "checksum": "", "status": "0"}]
    _vm_b(pipe, tmp_path, monkeypatch)
    assert stream.restore_lake(SITE) == stream.RC_RETRY  # the old code installed it (rc 0)
    assert not site_lake.site_paths(SITE).catalog_file.exists()
    assert "no good replica" in _report()["retry"]


def test_catalog_data_files_are_the_lakes_files_with_their_sizes(pipe: SimpleNamespace) -> None:  # noqa: F811
    for product, month in ((OS, "2023-05"), (IS, "2019-05")):
        assert stream.run_chunk(SITE, _chunk(pipe, product, month).chunk_id, stages=["download", "stack"]) == 0
    paths = site_lake.site_paths(SITE)
    lake = site_lake.open_site_lake(SITE)
    site_lake.checkpoint_detach(lake, SITE)
    lake.close()
    refs = site_lake.catalog_data_files(paths.catalog_file)
    assert refs and refs == {str(p.relative_to(paths.data_dir)): len(b) for p, b in _data_files().items()}
    assert all(r.startswith("main/") and r.endswith(".parquet") for r in refs)
