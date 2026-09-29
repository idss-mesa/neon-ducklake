"""A fake Data Store for the multi-VM tests: FakeIrods plus create/open/get/unlink/move and modify times.

``DSDataObjects.create(path, force=False)`` mirrors python-irodsclient 3.3.0: a client-side ``exists``
check (``DataObjectExistsAtLogicalPath``), then the server-side create, which refuses an existing object
(``OVERWRITE_WITHOUT_FORCE_FLAG``). ``DSIrods.create_hook(path)`` runs between the two, so a test can hold
two clients in the race window. Modify times come from ``DSIrods.now`` (tz-aware UTC).
"""

from __future__ import annotations

import hashlib
import io
import posixpath
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from irods.exception import (
    CAT_NO_ROWS_FOUND,
    CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME,
    OVERWRITE_WITHOUT_FORCE_FLAG,
    DataObjectDoesNotExist,
    DataObjectExistsAtLogicalPath,
)

from neon_ducklake import env, state

from .test_avu_fakes import FakeDataObjects, FakeIrods, FakeSession

ROOT = "/iplant/home/testuser/neon-ducklake"


class DSIrods(FakeIrods):
    def __init__(self) -> None:
        super().__init__()
        self.mtimes: dict[str, datetime] = {}
        self.now: datetime | None = None
        self.create_hook: Any = None
        self.ops: list[tuple[str, str]] = []

    def clock(self) -> datetime:
        return self.now or datetime.now(UTC)

    def tick(self, seconds: float) -> None:
        self.now = self.clock() + timedelta(seconds=seconds)

    def add_obj(self, path: str, avus: tuple = (), *, content: bytes = b"x", replicas: list[dict] | None = None,
                mtime: datetime | None = None) -> None:
        with self.lock:
            super().add_obj(path, avus, content=content, replicas=replicas)
            self.mtimes[path] = mtime or self.clock()

    def set_content(self, path: str, data: bytes, *, register: bool = False) -> None:
        with self.lock:
            o = self.objs[path]
            o["content"] = data
            md5 = hashlib.md5(data).hexdigest() if register else ""
            o["replicas"] = [{"number": 0, "size": len(data), "checksum": md5, "status": "1"}]
            self.mtimes[path] = self.clock()

    def content(self, path: str) -> bytes:
        return self.objs[path]["content"]

    def rows(self, table: str) -> list[dict[str, Any]]:
        out = super().rows(table)
        if table in ("obj", "obj_meta"):
            with self.lock:
                for r in out:
                    p = f"{r['COLL_NAME']}/{r['DATA_NAME']}"
                    r["D_MODIFY_TIME"] = self.mtimes.get(p)
        return out


class FakeObj:
    def __init__(self, dobjs: DSDataObjects, path: str) -> None:
        o = dobjs.store.objs[path]
        good = [r for r in o["replicas"] if r["status"] == "1"] or o["replicas"]
        self.path = path
        self.name = posixpath.basename(path)
        self.size = good[0]["size"] if good else len(o["content"])
        self.checksum = good[0]["checksum"] if good else ""
        self.modify_time = dobjs.store.mtimes.get(path)
        self._dobjs = dobjs

    def open(self, mode: str = "r", **opts: Any) -> Any:
        return self._dobjs.open(self.path, mode, **opts)


class _Handle(io.BytesIO):
    def __init__(self, store: DSIrods, path: str, initial: bytes, *, writable: bool = True) -> None:
        super().__init__(initial)
        self.store, self.path, self._writable = store, path, writable

    def close(self) -> None:
        if not self.closed and self._writable:
            data = self.getvalue()
            with self.store.lock:
                if self.path in self.store.objs:
                    self.store.set_content(self.path, data)
        super().close()


class DSDataObjects(FakeDataObjects):
    store: DSIrods

    def exists(self, path: str) -> bool:
        return path in self.store.objs

    def _missing(self, path: str) -> None:
        if path not in self.store.objs:
            raise CAT_NO_ROWS_FOUND(f"no data object {path}")

    def get(self, path: str, local_path: str | None = None, num_threads: int = 0, **opts: Any) -> FakeObj:
        with self.store.lock:
            if path not in self.store.objs:
                raise DataObjectDoesNotExist(path)
            self.store.calls["get"] += 1
            if local_path:
                lp = Path(local_path)
                if lp.exists() and "forceFlag" not in opts:
                    raise OVERWRITE_WITHOUT_FORCE_FLAG(local_path)
                lp.write_bytes(self.store.objs[path]["content"])
                self.store.ops.append(("download", path))
            return FakeObj(self, path)

    def create(self, path: str, resource: str | None = None, force: bool = True, **opts: Any) -> FakeObj:
        if not force and self.exists(path):  # python-irodsclient's client-side check (racy)
            raise DataObjectExistsAtLogicalPath()
        if self.store.create_hook:
            self.store.create_hook(path)
        with self.store.lock:  # the server: refuses an existing object without FORCE_FLAG_KW
            if path in self.store.objs:
                raise OVERWRITE_WITHOUT_FORCE_FLAG(path)
            self.store.add_obj(path, content=b"", replicas=[{"number": 0, "size": 0, "checksum": "",
                                                             "status": "1"}])
            self.store.ops.append(("create", path))
            return FakeObj(self, path)

    def open(self, path: str, mode: str = "r", create: bool = True, **opts: Any) -> _Handle:
        with self.store.lock:
            exists = path in self.store.objs
            if mode in ("r", "r+"):
                self._missing(path)
                return _Handle(self.store, path, self.store.objs[path]["content"], writable=mode == "r+")
            if mode in ("w", "w+"):
                if not exists:
                    if not create:
                        raise CAT_NO_ROWS_FOUND(f"no data object {path} (open without create)")
                    self.store.add_obj(path, content=b"", replicas=[{"number": 0, "size": 0, "checksum": "",
                                                                     "status": "1"}])
                self.store.ops.append(("write", path))
                return _Handle(self.store, path, b"")
            raise NotImplementedError(mode)

    def unlink(self, path: str, force: bool = False, **opts: Any) -> None:
        with self.store.lock:
            self._missing(path)
            del self.store.objs[path]
            self.store.mtimes.pop(path, None)
            self.store.ops.append(("unlink", path))

    def move(self, src: str, dst: str) -> None:
        with self.store.lock:
            self._missing(src)
            if dst in self.store.objs:
                raise CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME(dst)
            if posixpath.dirname(dst) not in self.store.colls:
                self.store.add_coll(posixpath.dirname(dst))
            self.store.objs[dst] = self.store.objs.pop(src)
            self.store.mtimes[dst] = self.store.mtimes.pop(src, self.store.clock())
            self.store.ops.append(("move", f"{src} -> {dst}"))


class DSSession(FakeSession):
    def __init__(self, store: DSIrods) -> None:
        super().__init__(store)
        self.data_objects = DSDataObjects(store)

    def clone(self) -> DSSession:
        self.store.calls["clone"] += 1
        return DSSession(self.store)


def put(store: DSIrods, path: str, data: bytes, *, mtime: datetime | None = None) -> None:
    """Seed an object with a registered md5 checksum (as a checksum-verified upload leaves it)."""
    md5 = hashlib.md5(data).hexdigest()
    store.add_obj(path, content=data, replicas=[{"number": 0, "size": len(data), "checksum": md5, "status": "1"}],
                  mtime=mtime)


def hermetic_ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    """A fake Data Store plus a hermetic project root (paths, journal, locks) in tmp_path.

    Test modules wrap it in their own ``ds`` fixture.
    """
    root = tmp_path / "root"
    root.mkdir()
    for attr, rel in (("ROOT", ""), ("API_DIR", "api"), ("RUNS_DIR", "api/runs"), ("LAKE_DIR", "ducklake"),
                      ("SITES_DIR", "sites"), ("WORK_DIR", "work"), ("LOCK_DIR", "work/locks"),
                      ("MESA_CACHE_DIR", "work/mesa-cache"), ("QUARANTINE_DIR", "work/quarantine"),
                      ("LOG_DIR", "logs")):
        monkeypatch.setattr(env, attr, root / rel if rel else root)
    monkeypatch.setattr(env, "IRODS_ROOT", ROOT)
    monkeypatch.setattr(state, "DB_PATH", root / "work" / "state.sqlite")
    for var in ("NEON_DUCKLAKE_VM_ID", "NEON_DUCKLAKE_HISTORY_MODE", "NEON_DUCKLAKE_CURATION_SYNC",
                "NEON_DUCKLAKE_SECRETS"):
        monkeypatch.delenv(var, raising=False)
    store = DSIrods()
    store.add_coll(ROOT)
    return store


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)


def test_fake_create_is_exclusive(ds: DSIrods) -> None:
    s = DSSession(ds)
    s.data_objects.create(f"{ROOT}/a", force=False)
    with pytest.raises(DataObjectExistsAtLogicalPath):
        s.data_objects.create(f"{ROOT}/a", force=False)
    with s.data_objects.open(f"{ROOT}/a", "r+") as fh:
        fh.write(b"hello")
    assert ds.content(f"{ROOT}/a") == b"hello" and s.data_objects.get(f"{ROOT}/a").modify_time.tzinfo is not None
