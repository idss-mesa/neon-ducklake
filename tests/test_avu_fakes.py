"""In-memory fake of the python-irodsclient surface used by avu/bulk.py and mirror.py (no network).

Imported by the other ``test_avu_*`` / ``test_mirror_*`` modules. Queries are evaluated against the
real ``irods.models`` columns (matched by ``icat_key``), with GenQuery's DISTINCT semantics, so
replica rows collapse exactly as they would against iCAT.
"""

from __future__ import annotations

import fnmatch
import hashlib
import posixpath
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any

from irods.exception import (
    CAT_SUCCESS_BUT_WITH_NO_INFO,
    CAT_UNKNOWN_FILE,
    CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME,
    OVERWRITE_WITHOUT_FORCE_FLAG,
    CollectionDoesNotExist,
)
from irods.meta import iRODSMeta


class SYS_MAX_CONNECT_COUNT_EXCEEDED(Exception):
    pass


def test_fakes_module_loads() -> None:
    assert FakeSession(FakeIrods()).clone() is not None


class FakeIrods:
    """Shared server state for every FakeSession clone."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.colls: dict[str, dict[str, Any]] = {}
        self.objs: dict[str, dict[str, Any]] = {}
        self.calls: dict[str, int] = defaultdict(int)
        self.atomic_log: list[tuple[str, list[tuple[str, tuple[str, str, str]]]]] = []
        self.put_log: list[tuple[str, int, dict[str, Any]]] = []
        self.fail_atomic = 0
        self.on_atomic_fail: list[Any] = []  # callables run on each injected failure
        self.put_errors: list[BaseException] = []
        self.register_checksum = True
        self._next_id = 1000

    def _id(self) -> int:
        self._next_id += 1
        return self._next_id

    def add_coll(self, path: str, avus: tuple = ()) -> None:
        with self.lock:
            parts = path.rstrip("/").split("/")
            for i in range(2, len(parts) + 1):
                p = "/".join(parts[:i])
                self.colls.setdefault(p, {"id": self._id(), "avus": set()})
            self.colls[path]["avus"].update((a, v, u) for a, v, u in avus)

    def add_obj(self, path: str, avus: tuple = (), *, content: bytes = b"x", replicas: list[dict] | None = None) -> None:
        with self.lock:
            self.add_coll(posixpath.dirname(path))
            md5 = hashlib.md5(content).hexdigest()
            reps = replicas if replicas is not None else [{"number": 0, "size": len(content), "checksum": md5,
                                                           "status": "1"}]
            self.objs[path] = {"id": self._id(), "avus": {(a, v, u) for a, v, u in avus}, "replicas": reps,
                               "content": content}

    def avus(self, path: str) -> set[tuple[str, str, str]]:
        ent = self.objs.get(path) or self.colls.get(path)
        return set(ent["avus"]) if ent else set()

    # ------------------------------------------------------------ query tables
    def rows(self, table: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with self.lock:
            if table == "coll_meta":
                for p, c in self.colls.items():
                    for a, v, u in c["avus"]:
                        out.append({"COLL_ID": c["id"], "COLL_NAME": p, "COL_META_COLL_ATTR_NAME": a,
                                    "COL_META_COLL_ATTR_VALUE": v, "COL_META_COLL_ATTR_UNITS": u})
            elif table == "coll":
                out = [{"COLL_ID": c["id"], "COLL_NAME": p} for p, c in self.colls.items()]
            elif table in ("obj", "obj_meta"):
                for p, o in self.objs.items():
                    base = {"COLL_NAME": posixpath.dirname(p), "D_DATA_ID": o["id"], "DATA_NAME": posixpath.basename(p)}
                    for r in o["replicas"]:
                        rep = dict(base, DATA_REPL_NUM=r["number"], DATA_SIZE=r["size"], D_DATA_CHECKSUM=r["checksum"],
                                   D_REPL_STATUS=r["status"])
                        if table == "obj":
                            out.append(rep)
                            continue
                        for a, v, u in o["avus"]:
                            out.append(dict(rep, COL_META_DATA_ATTR_NAME=a, COL_META_DATA_ATTR_VALUE=v,
                                            COL_META_DATA_ATTR_UNITS=u))
            elif table == "user":
                out = [{"USER_ID": 7, "USER_NAME": "tswetnam"}]
        return out


def _table(cols: tuple[Any, ...]) -> str:
    keys = [c.icat_key for c in cols]
    if any(k.startswith("COL_META_COLL") for k in keys):
        return "coll_meta"
    if any(k.startswith("COL_META_DATA") for k in keys):
        return "obj_meta"
    if any(k.startswith(("D_", "DATA_")) for k in keys):
        return "obj"
    if any(k.startswith("COL_QUOTA") for k in keys):
        return "quota"
    if any(k.startswith("USER_") for k in keys):
        return "user"
    return "coll"


def _match(row: dict[str, Any], crit: Any) -> bool:
    val = row.get(crit.query_key.icat_key)
    if crit.op == "=":
        return val == crit.value
    if crit.op == "like":
        return val is not None and fnmatch.fnmatchcase(str(val), crit.value.replace("%", "*").replace("_", "?"))
    if crit.op == "in":
        return val in crit.value
    raise NotImplementedError(crit.op)


class FakeQuery:
    def __init__(self, store: FakeIrods, cols: tuple[Any, ...], criteria: list[Any]) -> None:
        self.store, self.cols, self.criteria = store, cols, criteria

    def filter(self, *crit: Any) -> FakeQuery:
        return FakeQuery(self.store, self.cols, self.criteria + list(crit))

    def __iter__(self):
        self.store.calls["query"] += 1
        table = _table(self.cols)
        seen: set[tuple] = set()
        out = []
        for row in self.store.rows(table):
            if all(_match(row, c) for c in self.criteria):
                key = tuple(row.get(c.icat_key) for c in self.cols)
                if key in seen:
                    continue
                seen.add(key)
                out.append({c: row.get(c.icat_key) for c in self.cols})
        return iter(out)


class FakeMeta:
    def __init__(self, store: FakeIrods) -> None:
        self.store = store

    def _entry(self, model: Any, path: str) -> dict[str, Any]:
        ent = (self.store.objs if model.__name__ == "DataObject" else self.store.colls).get(path)
        if ent is None:
            raise CAT_UNKNOWN_FILE(f"no such target {path}")
        return ent

    def apply_atomic_operations(self, model: Any, path: str, *ops: Any) -> None:
        s = self.store
        with s.lock:
            s.calls["atomic"] += 1
            assert len(ops) <= 500
            s.atomic_log.append((path, [(o.operation, (o.avu.name, o.avu.value, o.avu.units or "")) for o in ops]))
            if s.fail_atomic > 0:
                s.fail_atomic -= 1
                for hook in s.on_atomic_fail:
                    hook(s)
                raise RuntimeError("SYS_INTERNAL_ERR injected atomic failure")
            ent = self._entry(model, path)
            new = set(ent["avus"])
            for o in ops:
                t = (o.avu.name, o.avu.value, o.avu.units or "")
                if o.operation == "add":
                    if t in new:
                        raise CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME(str(t))
                    new.add(t)
                else:
                    if t not in new:
                        raise CAT_SUCCESS_BUT_WITH_NO_INFO(str(t))
                    new.discard(t)
            ent["avus"] = new

    def add(self, model: Any, path: str, meta: iRODSMeta) -> None:
        with self.store.lock:
            self.store.calls["meta_add"] += 1
            ent = self._entry(model, path)
            t = (meta.name, meta.value, meta.units or "")
            if t in ent["avus"]:
                raise CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME(str(t))
            ent["avus"].add(t)

    def remove(self, model: Any, path: str, meta: iRODSMeta) -> None:
        with self.store.lock:
            self.store.calls["meta_remove"] += 1
            ent = self._entry(model, path)
            t = (meta.name, meta.value, meta.units or "")
            if t not in ent["avus"]:
                raise CAT_SUCCESS_BUT_WITH_NO_INFO(str(t))
            ent["avus"].discard(t)

    def get(self, model: Any, path: str) -> list[iRODSMeta]:
        with self.store.lock:
            self.store.calls["meta_get"] += 1
            ent = self._entry(model, path)
            return [iRODSMeta(a, v, u or None) for a, v, u in ent["avus"]]


class FakeCollections:
    def __init__(self, store: FakeIrods) -> None:
        self.store = store

    def create(self, path: str, recurse: bool = True) -> None:
        self.store.calls["coll_create"] += 1
        self.store.add_coll(path)

    def get(self, path: str) -> Any:
        if path not in self.store.colls:
            raise CollectionDoesNotExist(path)
        return type("Coll", (), {"id": self.store.colls[path]["id"], "path": path})()

    def exists(self, path: str) -> bool:
        return path in self.store.colls


class FakeDataObjects:
    def __init__(self, store: FakeIrods) -> None:
        self.store = store

    def put(self, local: str, irods_path: str, num_threads: int = 0, **opts: Any) -> None:
        s = self.store
        with s.lock:
            s.calls["put"] += 1
            s.put_log.append((irods_path, num_threads, dict(opts)))
            if s.put_errors:
                raise s.put_errors.pop(0)
            if irods_path in s.objs and "forceFlag" not in opts:
                raise OVERWRITE_WITHOUT_FORCE_FLAG(irods_path)
            data = Path(local).read_bytes()
            md5 = hashlib.md5(data).hexdigest()
            reg = s.register_checksum and "regChksum" in opts
            s.add_obj(irods_path, tuple(s.objs[irods_path]["avus"]) if irods_path in s.objs else (), content=data,
                      replicas=[{"number": 0, "size": len(data), "checksum": md5 if reg else "", "status": "1"},
                                {"number": 1, "size": 0, "checksum": "", "status": "0"}])

    def chksum(self, path: str, **opts: Any) -> str:
        s = self.store
        with s.lock:
            s.calls["chksum"] += 1
            o = s.objs[path]
            md5 = hashlib.md5(o["content"]).hexdigest()
            o["replicas"][0]["checksum"] = md5
            return md5


class FakeSession:
    server_version = (4, 3, 1)

    def __init__(self, store: FakeIrods) -> None:
        self.store = store
        self.metadata = FakeMeta(store)
        self.collections = FakeCollections(store)
        self.data_objects = FakeDataObjects(store)

    def clone(self) -> FakeSession:
        self.store.calls["clone"] += 1
        return FakeSession(self.store)

    def cleanup(self) -> None:
        self.store.calls["cleanup"] += 1

    def query(self, *cols: Any) -> FakeQuery:
        return FakeQuery(self.store, cols, [])
