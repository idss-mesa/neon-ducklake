"""Coordination of several workers through the CyVerse Data Store (docs/STREAMING.md, "Coordination").

Workers on different CyVerse DE VMs share nothing but the Data Store, so every lock is an iRODS data object.

**Atomic create.** python-irodsclient 3.3.0 ``data_objects.create(path, force=False)`` first checks
``exists(path)`` on the client (raising ``DataObjectExistsAtLogicalPath``) and then sends
``DATA_OBJ_CREATE_AN`` *without* ``FORCE_FLAG_KW``. The client check alone is racy, but the server refuses
to create over an existing object without the force flag (``OVERWRITE_WITHOUT_FORCE_FLAG``), and two
concurrent creates at one logical path collide on iCAT's unique (collection, name, replica) index
(``CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME``). :func:`create_exclusive` treats all three as "exists".
``put()`` is NOT used for locks: it opens with ``O_CREAT|O_TRUNC`` and silently overwrites. The JSON is
written after the create through ``open(path, 'r+')``, which never creates, so an object that is briefly
empty counts as a live claim for ``EMPTY_GRACE_S``.

**Site claims** ``<IRODS_ROOT>/sites/<SITE>/.claim`` hold JSON ``{kind, key, vm, host, pid, token,
claimedAt, heartbeatAt, expiresAt, ttlS, info}``. :func:`try_claim`:

1. creates the object atomically; after ``SETTLE_S`` it re-reads the token (a racing breaker may have won);
2. if it exists and belongs to this VM on this host with a dead pid (a restarted worker), it is resumed;
3. if it is stale (``now > expiresAt``), it is broken under a second atomic lock ``<claim>.break``:
   re-read (the token must be the stale one), delete, create, release ``.break``, settle, re-read;
4. otherwise it is held.

**Identity.** A claim also records ``machine`` (``/etc/machine-id``) and ``boot``
(``/proc/sys/kernel/random/boot_id``). It is "ours" (:meth:`Claim.is_mine`) only for the same vm id, host
name and machine id. A claim of ours from the same boot is resumed when its pid is dead; one from another
boot (a reboot, or a twin VM sharing vm id, host name and machine id) only when it has stopped
heartbeating for ``2 * HEARTBEAT_S``. :func:`register_vm` claims ``<IRODS_ROOT>/.mesa/vms/<vm>.claim``
at worker start, so two live VMs with the same vm id are refused (``VmIdCollision``).

A :class:`Heartbeat` thread renews every ``HEARTBEAT_S`` (5 min; TTL ``CLAIM_TTL_S`` 2 h). A renew checks
the token, writes, then re-reads; a changed token means the claim is lost. A claim that could not be
renewed by ``expires_at - HEARTBEAT_S`` is declared lost by its own holder (self-expiry), before another VM
may steal it. Lost claims get ``claim.lost`` (a ``threading.Event``) set. :func:`release` deletes a claim
only when its token is still ours. :func:`site_claim` wraps all of this as a context manager for the
orchestrator and the worker: it yields the :class:`Claim` (watch ``claim.lost``) and raises
:class:`SiteDone` / :class:`SiteClaimed` when the site cannot be claimed.
``<IRODS_ROOT>/sites/<SITE>/.done`` (JSON summary) marks a finished site; :func:`claim_site` re-checks it
after winning the claim (another VM may have finished the site meanwhile).

**Reading a claim while it is renewed.** A renew rewrites the claim in place (``open 'w'`` truncates, then
writes): for a fraction of a second every ``HEARTBEAT_S`` a reader in another thread or process sees an empty
object, or gets a lock or replica error (``LOCKED_DATA_OBJECT_ACCESS``, ``SYS_REPLICA_INACCESSIBLE``). Every
decision that must not mistake that window for a lost claim (:func:`holds_site`, :func:`claim_is_current`,
:func:`renew`, :func:`release`, :func:`site_claims`, :func:`curation_claims`) reads through
:func:`read_claim_settled`: such a read is retried for up to ``CLAIM_READ_RETRY_S`` (8 s), and a claim that
stays unreadable raises :class:`ClaimUnreadable` (transient: unknown, never "lost"; the listings show it as an
unreadable claim that is not stale, i.e. held by somebody).

**Before a shared write** of a site (a site-lake catalog put, a journal pointer), :func:`holds_site` is
checked right before the write, also in stage subprocesses that have no :class:`Claim`. With
``NEON_DUCKLAKE_SITE_CLAIMS=on`` (the worker; ``run`` with claims exports it to its stages) the site's claim
must be live and this VM's; with ``off`` (``run --no-claims``) or unset (an operator) only a live claim of
another VM refuses the write. :func:`rename` (atomic, never onto an existing object) is the compare-and-swap
primitive of the journal pointer.

**Curation** claims ``<IRODS_ROOT>/curation/claims/<DP>.claim`` (TTL ``CURATION_TTL_S``, 6 h) guard the
two Opus replicates of a product; results are shared as ``<IRODS_ROOT>/curation/generic/<DP>.validated.json``.
Which copy of a result is newer is decided by its content version (:func:`generic_version`: ``validatedAt``,
then ``runId``), never by file modify times (a git checkout makes every file look new).
They are used only when :func:`curation_sync_enabled` (``NEON_DUCKLAKE_CURATION_SYNC=on|off|auto``; auto =
on for workers, i.e. when ``NEON_DUCKLAKE_VM_ID`` is set or ``NEON_DUCKLAKE_HISTORY_MODE=spool``).

**Operators:** ``neon-ducklake claims [list|release] [SITE|DP ...] [--force] [--json]`` (:func:`claims_main`).

Every function takes ``session=None`` (default: ``env.mesa().session``) so tests pass a fake.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import posixpath
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Self

from . import env

log = logging.getLogger("neon_ducklake.coord")


def _env_seconds(var: str, default: float) -> float:
    raw = (os.environ.get(var) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SystemExit(f"{var}={raw!r} is not a number of seconds") from exc


CLAIM_TTL_S = _env_seconds("NEON_DUCKLAKE_CLAIM_TTL_S", 2 * 3600.0)
HEARTBEAT_S = _env_seconds("NEON_DUCKLAKE_HEARTBEAT_S", 300.0)
CURATION_TTL_S = _env_seconds("NEON_DUCKLAKE_CURATION_TTL_S", 6 * 3600.0)
SETTLE_S = 2.0  # wait before re-reading a fresh claim's token (lets a concurrent breaker finish)
BREAK_TTL_S = 300.0  # a '.break' lock older than this is abandoned
EMPTY_GRACE_S = 300.0  # an empty / unparseable claim object younger than this is being written: live
# read_claim_settled: how long a claim being rewritten (empty, locked) is waited for, and the first pause
CLAIM_READ_RETRY_S = _env_seconds("NEON_DUCKLAKE_CLAIM_READ_RETRY_S", 8.0)
CLAIM_READ_PAUSE_S = 0.25
RENEW_WRITE_TRIES = 3  # a renew whose write failed rewrites the claim at once (it may have been left truncated)
CLAIM_NAME = ".claim"
DONE_NAME = ".done"
VM_DIR = ".mesa/vms"
CURATION_SYNC_ENV = "NEON_DUCKLAKE_CURATION_SYNC"
SITE_CLAIMS_ENV = "NEON_DUCKLAKE_SITE_CLAIMS"  # on | off (run.ENV_SITE_CLAIMS): is a site claim in force?

EXISTS_ERRORS = frozenset({"DataObjectExistsAtLogicalPath", "OVERWRITE_WITHOUT_FORCE_FLAG",
                           "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"})
MISSING_ERRORS = frozenset({"DoesNotExist", "DataObjectDoesNotExist", "CollectionDoesNotExist", "CAT_NO_ROWS_FOUND",
                            "CAT_UNKNOWN_FILE", "OBJ_PATH_DOES_NOT_EXIST", "USER_FILE_DOES_NOT_EXIST"})


def _is(exc: BaseException, names: frozenset[str]) -> bool:
    return any(k.__name__ in names for k in type(exc).__mro__)


def transient_error(exc: BaseException) -> bool:
    """A Data Store or transport error worth retrying: a lock or replica error of an object being written
    (``LOCKED_DATA_OBJECT_ACCESS``, ``SYS_REPLICA_INACCESSIBLE``, ...; the name varies by server version, so
    any iRODS error counts), a network error, or :class:`ClaimUnreadable`. A missing object is not one, and
    neither is a programming error (``TypeError``, ...)."""
    if isinstance(exc, ClaimUnreadable):
        return True
    if _is(exc, MISSING_ERRORS):
        return False
    try:
        from irods.exception import PycommandsException, iRODSException
    except ImportError:  # pragma: no cover - python-irodsclient is a dependency
        irods_errors: tuple[type[BaseException], ...] = ()
    else:
        irods_errors = (iRODSException, PycommandsException)
    if isinstance(exc, (*irods_errors, OSError, EOFError)):
        return True
    from .tuning import irods_error_kind

    return irods_error_kind(exc) is not None


class SiteUnavailable(RuntimeError):
    """:func:`site_claim` could not claim the site."""

    def __init__(self, site: str, msg: str, holder: Claim | None = None) -> None:
        super().__init__(msg)
        self.site = site
        self.holder = holder


class SiteDone(SiteUnavailable):
    """The site has a ``.done`` marker (possibly written while we were claiming it)."""


class SiteClaimed(SiteUnavailable):
    """Another live VM holds the site's claim (or won the race for it)."""


class TransferMismatch(RuntimeError):
    """A download did not match the iRODS size or checksum (a corrupt or half-written object, not an outage)."""


class VmIdCollision(SystemExit):
    """Another live worker is registered with this VM id (see :func:`register_vm`)."""


class ClaimUnreadable(RuntimeError):
    """A claim could not be read for the whole retry window of :func:`read_claim_settled`: every read raised a
    Data Store error, or the object stayed empty or unparseable while young (being written). Its owner is
    unknown, which is not the same as lost: a transient condition, retry later."""

    def __init__(self, path: str, msg: str) -> None:
        super().__init__(msg)
        self.path = path


def vm_id() -> str:
    """``$NEON_DUCKLAKE_VM_ID`` or the short host name (see :func:`env.vm_id`)."""
    return env.vm_id()


def host() -> str:
    return socket.gethostname()


def get_session(session: Any | None = None) -> Any:
    return session if session is not None else env.mesa().session


# ---------------------------------------------------------------- paths

def site_claim_path(site: str) -> str:
    return f"{env.IRODS_ROOT}/sites/{site.upper()}/{CLAIM_NAME}"


def site_done_path(site: str) -> str:
    return f"{env.IRODS_ROOT}/sites/{site.upper()}/{DONE_NAME}"


def curation_claim_path(product: str) -> str:
    return f"{env.IRODS_ROOT}/curation/claims/{product}{CLAIM_NAME}"


def generic_remote_path(product: str) -> str:
    return f"{env.IRODS_ROOT}/curation/generic/{product}.validated.json"


def spool_dir(vm: str | None = None) -> str:
    return f"{env.IRODS_ROOT}/.mesa/spool/{vm or vm_id()}"


def spool_done_dir(vm: str | None = None) -> str:
    return f"{env.IRODS_ROOT}/.mesa/spool-done/{vm or vm_id()}"


def journal_dir(site: str) -> str:
    return f"{env.IRODS_ROOT}/sites/{site.upper()}/.mesa-journal"


def vm_claim_path(vm: str | None = None) -> str:
    return f"{env.IRODS_ROOT}/{VM_DIR}/{vm or vm_id()}{CLAIM_NAME}"


# ---------------------------------------------------------------- Data Store primitives

@dataclass(frozen=True)
class RemoteObject:
    path: str
    size: int
    checksum: str
    modified: datetime | None

    @property
    def name(self) -> str:
        return posixpath.basename(self.path)


def ensure_collection(session: Any, coll: str) -> None:
    try:
        session.collections.create(coll, recurse=True)
    except Exception as exc:  # "exists" in its various spellings is fine
        if not (_is(exc, EXISTS_ERRORS) or "exist" in str(exc).lower()):
            raise


def stat(session: Any, path: str) -> RemoteObject | None:
    """Size, checksum and modify time of a data object; None when it does not exist."""
    try:
        obj = session.data_objects.get(path)
    except Exception as exc:
        if _is(exc, MISSING_ERRORS):
            return None
        raise
    mt = getattr(obj, "modify_time", None)
    if isinstance(mt, datetime) and mt.tzinfo is None:
        mt = mt.replace(tzinfo=UTC)
    return RemoteObject(path, int(getattr(obj, "size", 0) or 0), str(getattr(obj, "checksum", "") or ""), mt)


def read_bytes(session: Any, path: str) -> bytes | None:
    """The content of a (small) data object; None when it does not exist.

    The error iRODS returns for opening a missing object varies by server version, so any failure is
    followed by a ``stat``: a missing object gives None, anything else is re-raised.
    """
    try:
        with session.data_objects.open(path, "r") as fh:
            return fh.read()
    except Exception as exc:
        if _is(exc, MISSING_ERRORS) or stat(session, path) is None:
            return None
        raise


def create_exclusive(session: Any, path: str, data: bytes) -> bool:
    """Create ``path`` holding ``data`` only when nothing exists there. False when it already exists."""
    ensure_collection(session, posixpath.dirname(path))
    try:
        session.data_objects.create(path, force=False)
    except Exception as exc:
        if _is(exc, EXISTS_ERRORS):
            return False
        raise
    with session.data_objects.open(path, "r+") as fh:  # 'r+' never creates and never truncates
        fh.write(data)
    return True


def overwrite(session: Any, path: str, data: bytes, *, create: bool = False) -> None:
    """Replace the content of ``path``. With ``create=False`` it fails when the object is missing."""
    if create:
        ensure_collection(session, posixpath.dirname(path))
    with session.data_objects.open(path, "w", create=create) as fh:
        fh.write(data)


def unlink(session: Any, path: str) -> bool:
    """Delete a data object (no trash). False when it did not exist."""
    try:
        session.data_objects.unlink(path, force=True)
    except Exception as exc:
        if _is(exc, MISSING_ERRORS) or stat(session, path) is None:
            return False
        raise
    return True


RENAME_EXISTS_ERRORS = EXISTS_ERRORS | {"CAT_NAME_EXISTS_AS_DATAOBJ", "CAT_NAME_EXISTS_AS_COLLECTION"}


def rename(session: Any, src: str, dst: str) -> str:
    """Rename ``src`` to ``dst`` only when nothing exists at ``dst`` (atomic on the server: iRODS refuses to
    rename onto an existing object). Returns ``ok``, ``missing`` (``src`` is gone: someone else moved or deleted
    it) or ``exists`` (``dst`` is taken). Other errors propagate. This is the compare-and-swap primitive of the
    journal pointer (``journal_sync``)."""
    ensure_collection(session, posixpath.dirname(dst))
    try:
        session.data_objects.move(src, dst)
    except Exception as exc:
        if _is(exc, RENAME_EXISTS_ERRORS):
            return "exists"
        if _is(exc, MISSING_ERRORS):
            return "missing"
        if stat(session, src) is None:
            return "missing"
        if stat(session, dst) is not None:
            return "exists"
        raise
    return "ok"


def move(session: Any, src: str, dst: str) -> str:
    """Rename ``src`` to ``dst`` (creating the collection). An existing ``dst`` is never replaced: the
    object then goes to ``<dst>.dup-<utc>``. Returns the final path."""
    ensure_collection(session, posixpath.dirname(dst))
    if stat(session, dst) is not None:
        dst = f"{dst}.dup-{env.utcnow().strftime('%Y%m%dT%H%M%S%fZ')}"
    session.data_objects.move(src, dst)
    return dst


def list_objects(session: Any, coll: str, *, recursive: bool = False) -> dict[str, RemoteObject]:
    """path -> RemoteObject for the data objects in ``coll`` (and below). Good replicas are preferred."""
    from irods.column import Like
    from irods.models import Collection, DataObject

    cols = (Collection.name, DataObject.name, DataObject.size, DataObject.checksum, DataObject.modify_time,
            DataObject.replica_status)
    queries = [session.query(*cols).filter(Collection.name == coll)]
    if recursive:
        queries.append(session.query(*cols).filter(Like(Collection.name, coll.rstrip("/") + "/%")))
    out: dict[str, RemoteObject] = {}
    good: set[str] = set()
    for q in queries:
        for r in q:
            path = f"{r[Collection.name]}/{r[DataObject.name]}"
            ok = str(r[DataObject.replica_status]) == "1"
            if path in good and not ok:
                continue
            mt = r[DataObject.modify_time]
            if isinstance(mt, datetime) and mt.tzinfo is None:
                mt = mt.replace(tzinfo=UTC)
            out[path] = RemoteObject(path, int(r[DataObject.size] or 0), str(r[DataObject.checksum] or ""), mt)
            if ok:
                good.add(path)
    return out


def checksum_matches(remote_checksum: str, local: Path) -> bool | None:
    """Compare an iRODS checksum (md5 hex, ``md5:``/``sha2:`` prefixed) with a local file; None if unknown."""
    import base64

    c = (remote_checksum or "").strip()
    if not c:
        return None
    if c.lower().startswith("sha2:"):
        h = hashlib.sha256()
        want = c[5:]
        with local.open("rb") as fh:
            while chunk := fh.read(8 << 20):
                h.update(chunk)
        return base64.b64encode(h.digest()).decode() == want
    want = c[4:] if c.lower().startswith("md5:") else c
    h = hashlib.md5(usedforsecurity=False)
    with local.open("rb") as fh:
        while chunk := fh.read(8 << 20):
            h.update(chunk)
    return h.hexdigest() == want.lower()


def fetch(session: Any, remote: str, local: Path, *, obj: RemoteObject | None = None) -> RemoteObject:
    """Download ``remote`` to ``local`` through ``<local>.part``, verified against the iRODS checksum and size.

    The local file gets the remote modify time as its mtime. Raises :class:`TransferMismatch` (a
    ``RuntimeError``) on a mismatch and ``FileNotFoundError`` when ``remote`` does not exist.
    """
    import irods.keywords as kw

    obj = obj or stat(session, remote)
    if obj is None:
        raise FileNotFoundError(remote)
    local.parent.mkdir(parents=True, exist_ok=True)
    part = local.with_name(local.name + ".part")
    part.unlink(missing_ok=True)
    session.data_objects.get(remote, str(part), **{kw.FORCE_FLAG_KW: ""})
    size = part.stat().st_size
    if obj.size and size != obj.size:
        part.unlink(missing_ok=True)
        raise TransferMismatch(f"download of {remote}: {size} bytes, iRODS says {obj.size}")
    if checksum_matches(obj.checksum, part) is False:
        part.unlink(missing_ok=True)
        raise TransferMismatch(f"download of {remote}: checksum differs from iRODS {obj.checksum}")
    os.replace(part, local)
    if obj.modified is not None:
        ts = obj.modified.timestamp()
        os.utime(local, (ts, ts))
    return obj


def put_json(session: Any, path: str, doc: Any) -> None:
    """Write a small JSON document, creating or replacing it."""
    overwrite(session, path, (json.dumps(doc, indent=1, default=str, sort_keys=True) + "\n").encode(), create=True)


def read_json(session: Any, path: str) -> Any | None:
    raw = read_bytes(session, path)
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None


# ---------------------------------------------------------------- claims

def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="seconds")


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts


@dataclass
class Claim:
    path: str
    kind: str
    key: str
    vm: str
    host: str
    pid: int
    token: str
    claimed_at: datetime
    heartbeat_at: datetime
    expires_at: datetime
    ttl_s: float
    info: dict[str, Any] = field(default_factory=dict)
    valid: bool = True  # False: the object exists but holds no claim JSON (yet)
    modified: datetime | None = None  # iRODS modify time (decides staleness of an invalid claim)
    machine: str = ""  # /etc/machine-id of the holder ("" when unknown or an older claim)
    boot: str = ""  # the holder's kernel boot id
    # set (by the Heartbeat, or by the holder itself) when this claim is no longer ours
    lost: threading.Event = field(default_factory=threading.Event, compare=False, repr=False)
    status: str = ""  # how we got it: acquired | resumed | stolen (ours only)

    @classmethod
    def new(cls, path: str, kind: str, key: str, *, ttl_s: float, vm: str | None = None,
            info: dict[str, Any] | None = None, now: datetime | None = None) -> Claim:
        now = now or env.utcnow()
        return cls(path, kind, key, vm or vm_id(), host(), os.getpid(), uuid.uuid4().hex, now, now,
                   now + timedelta(seconds=ttl_s), ttl_s, dict(info or {}), machine=env.machine_id(),
                   boot=env.boot_id())

    def to_doc(self) -> dict[str, Any]:
        return {"kind": self.kind, "key": self.key, "vm": self.vm, "host": self.host, "pid": self.pid,
                "machine": self.machine, "boot": self.boot,
                "token": self.token, "claimedAt": _iso(self.claimed_at), "heartbeatAt": _iso(self.heartbeat_at),
                "expiresAt": _iso(self.expires_at), "ttlS": self.ttl_s, "info": self.info}

    def to_bytes(self) -> bytes:
        return (json.dumps(self.to_doc(), indent=1, sort_keys=True, default=str) + "\n").encode()

    @classmethod
    def from_bytes(cls, path: str, raw: bytes, modified: datetime | None = None) -> Claim:
        try:
            doc = json.loads(raw.decode("utf-8"))
            if not isinstance(doc, dict) or not doc.get("token"):
                raise ValueError("no token")
            exp = _parse_ts(doc.get("expiresAt"))
            if exp is None:
                raise ValueError("no expiresAt")
            return cls(path, str(doc.get("kind") or ""), str(doc.get("key") or ""), str(doc.get("vm") or ""),
                       str(doc.get("host") or ""), int(doc.get("pid") or 0), str(doc["token"]),
                       _parse_ts(doc.get("claimedAt")) or exp, _parse_ts(doc.get("heartbeatAt")) or exp, exp,
                       float(doc.get("ttlS") or 0), dict(doc.get("info") or {}), True, modified,
                       machine=str(doc.get("machine") or ""), boot=str(doc.get("boot") or ""))
        except (UnicodeDecodeError, ValueError, TypeError):
            epoch = datetime.fromtimestamp(0, UTC)
            return cls(path, "", "", "", "", 0, "", epoch, epoch, epoch, 0.0, {}, False, modified)

    def is_stale(self, now: datetime | None = None) -> bool:
        now = now or env.utcnow()
        if not self.valid:
            return self.modified is not None and (now - self.modified).total_seconds() > EMPTY_GRACE_S
        return now > self.expires_at

    def is_mine(self, vm: str | None = None) -> bool:
        """Written by this VM id (``vm``, default :func:`vm_id`) on this host and machine, possibly by an
        earlier process or boot. A claim without a machine id (older format) matches on vm id and host."""
        if not (self.valid and self.vm == (vm or vm_id()) and self.host == host()):
            return False
        mine = env.machine_id()
        return not self.machine or not mine or self.machine == mine

    def same_boot(self) -> bool | None:
        """True/False when both boot ids are known; None when either is unknown."""
        mine = env.boot_id()
        return None if not (self.boot and mine) else self.boot == mine

    def describe(self, now: datetime | None = None) -> str:
        now = now or env.utcnow()
        if not self.valid:
            return f"{self.path}: unreadable claim (being written or corrupt)"
        state = "STALE" if self.is_stale(now) else "live"
        left = (self.expires_at - now).total_seconds() / 60
        return (f"{self.kind} {self.key}: vm {self.vm} ({self.host} pid {self.pid}), claimed "
                f"{_iso(self.claimed_at)}, heartbeat {_iso(self.heartbeat_at)}, "
                f"{'expired' if left < 0 else 'expires in'} {abs(left):.0f} min [{state}]")


@dataclass
class ClaimResult:
    acquired: bool
    status: str  # acquired | resumed | stolen | held | lost-race | done
    claim: Claim | None  # ours when acquired, else the holder's (when known)
    previous: Claim | None = None  # the stale or resumed claim we replaced

    def __bool__(self) -> bool:
        return self.acquired


def read_claim(path: str, *, session: Any | None = None) -> Claim | None:
    s = get_session(session)
    raw = read_bytes(s, path)
    if raw is None:
        return None
    claim = Claim.from_bytes(path, raw)
    if not claim.valid:  # empty (being written) or corrupt: its age decides
        obj = stat(s, path)
        claim.modified = obj.modified if obj else None
    return claim


def _being_written(claim: Claim, now: datetime | None = None) -> bool:
    """An empty or unparseable claim object modified within ``EMPTY_GRACE_S`` (or gone between the read and
    the stat): a create or a renew is writing it right now."""
    if claim.valid:
        return False
    if claim.modified is None:
        return True
    return ((now or env.utcnow()) - claim.modified).total_seconds() <= EMPTY_GRACE_S


def read_claim_settled(path: str, *, session: Any | None = None, retry_s: float | None = None,
                       pause_s: float | None = None) -> Claim | None:
    """:func:`read_claim` for a decision that must not mistake a claim being rewritten for a lost one.

    A renew rewrites the claim in place (truncate, then write), so a reader in that window sees an empty object
    or gets a lock / replica error. Such a read (a :func:`transient_error`, or an empty or unparseable object
    modified within ``EMPTY_GRACE_S``) is retried with a growing pause (``pause_s``, default
    ``CLAIM_READ_PAUSE_S``, doubling up to 2 s) for up to ``retry_s`` (default ``CLAIM_READ_RETRY_S``, 8 s).

    Returns None (no claim), a valid claim, or an invalid one older than the grace (nobody's). Raises
    :class:`ClaimUnreadable` (the last error chained) when the window ends without such a read; a
    non-transient error (a programming error) propagates at once.
    """
    s = get_session(session)
    window = CLAIM_READ_RETRY_S if retry_s is None else max(0.0, retry_s)
    pause = CLAIM_READ_PAUSE_S if pause_s is None else max(0.0, pause_s)
    deadline = time.monotonic() + window
    tries = 0
    while True:
        tries += 1
        last: BaseException | None = None
        try:
            claim = read_claim(path, session=s)
        except Exception as exc:
            if not transient_error(exc):
                raise
            last, why = exc, f"{type(exc).__name__}: {exc}"
        else:
            if claim is None or not _being_written(claim):
                if tries > 1:
                    log.info("%s: claim read settled after %d attempt(s)", path, tries)
                return claim
            why = "empty or unparseable, modified " + (_iso(claim.modified) if claim.modified else "just now (gone)")
        left = deadline - time.monotonic()
        if left <= 0:
            raise ClaimUnreadable(path, f"{path}: claim unreadable for {window:.0f}s ({tries} attempt(s)): "
                                        f"{why}"[:500]) from last
        log.debug("%s: claim being written (%s); reading again", path, why)
        time.sleep(min(pause, left) if pause > 0 else 0)
        pause = min(max(pause * 2, 0.05), 2.0)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def quiet_s() -> float:
    """A claim whose heartbeat is older than this is not being renewed by anyone."""
    return max(2 * HEARTBEAT_S, 120.0)


def _resumable(holder: Claim, now: datetime) -> bool:
    """A claim of ours (:meth:`Claim.is_mine`) that no live process renews: resume it.

    Same boot: its pid is this process or dead. Another boot: it is our own claim from before a reboot, or a
    twin VM with the same vm id, host name and machine id; only a claim that stopped heartbeating is resumed.
    """
    same = holder.same_boot()
    if same is False:
        idle = (now - holder.heartbeat_at).total_seconds()
        if idle > quiet_s():
            return True
        log.error("%s %s: claimed under this VM id (%s) from another boot (%s) that heartbeated %.0fs ago: "
                  "is another VM using the same NEON_DUCKLAKE_VM_ID and host name?", holder.kind, holder.key,
                  holder.vm, holder.boot, idle)
        return False
    return holder.pid == os.getpid() or not _pid_alive(holder.pid)


def _confirm(s: Any, me: Claim, status: str, previous: Claim | None = None, settle_s: float | None = None
             ) -> ClaimResult:
    wait = SETTLE_S if settle_s is None else settle_s
    if wait > 0:
        time.sleep(wait)
    cur = read_claim(me.path, session=s)
    if cur is not None and cur.valid and cur.token == me.token:
        log.info("%s %s claimed by %s (%s)", me.kind, me.key, me.vm, status)
        me.status = status
        return ClaimResult(True, status, me, previous)
    log.warning("%s %s: lost the claim race to %s", me.kind, me.key, cur.vm if cur else "nobody (deleted)")
    return ClaimResult(False, "lost-race", cur, previous)


def _break_stale(s: Any, stale: Claim, me: Claim, now: datetime, settle_s: float | None) -> ClaimResult:
    brk = stale.path + ".break"
    lock = json.dumps({"vm": me.vm, "host": me.host, "pid": me.pid, "token": me.token, "at": _iso(now)}).encode()
    if not create_exclusive(s, brk, lock):
        other = stat(s, brk)
        if other is not None and other.modified is not None and \
                (now - other.modified).total_seconds() > BREAK_TTL_S:
            log.warning("%s: removing an abandoned break lock (%s)", brk, _iso(other.modified))
            unlink(s, brk)
        return ClaimResult(False, "held", stale)
    try:
        cur = read_claim(stale.path, session=s)
        if cur is not None and (cur.token != stale.token or not cur.is_stale(now)):
            return ClaimResult(False, "held", cur)  # renewed or replaced meanwhile
        if cur is not None:
            unlink(s, stale.path)
        if not create_exclusive(s, stale.path, me.to_bytes()):
            return ClaimResult(False, "lost-race", read_claim(stale.path, session=s))
    finally:
        unlink(s, brk)
    log.warning("%s %s: broke the stale claim of %s (heartbeat %s, expired %s)", me.kind, me.key,
                stale.vm or "?", _iso(stale.heartbeat_at) if stale.valid else "-",
                _iso(stale.expires_at) if stale.valid else "-")
    return _confirm(s, me, "stolen", stale, settle_s)


def try_claim(path: str, kind: str, key: str, *, ttl_s: float | None = None, session: Any | None = None,
              info: dict[str, Any] | None = None, now: datetime | None = None, settle_s: float | None = None,
              vm: str | None = None) -> ClaimResult:
    """Claim ``path`` for this VM (see the module docstring). ``bool(result)`` says whether we hold it."""
    s = get_session(session)
    now = now or env.utcnow()
    ttl = CLAIM_TTL_S if ttl_s is None else ttl_s
    me = Claim.new(path, kind, key, ttl_s=ttl, vm=vm, info=info, now=now)
    for _attempt in range(2):
        if create_exclusive(s, path, me.to_bytes()):
            return _confirm(s, me, "acquired", None, settle_s)
        holder = read_claim(path, session=s)
        if holder is None:
            continue  # released between our create and our read: try once more
        if holder.is_mine(me.vm) and _resumable(holder, now):
            cur = read_claim(path, session=s)
            if cur is None or cur.token != holder.token:
                return ClaimResult(False, "lost-race", cur)
            overwrite(s, path, me.to_bytes())
            log.info("%s %s: resumed this VM's earlier claim (pid %d)", kind, key, holder.pid)
            return _confirm(s, me, "resumed", holder, settle_s)
        if holder.is_stale(now):
            return _break_stale(s, holder, me, now, settle_s)
        return ClaimResult(False, "held", holder)
    return ClaimResult(False, "lost-race", read_claim(path, session=s))


def renew(claim: Claim, *, session: Any | None = None, now: datetime | None = None) -> bool:
    """Extend ``claim`` by its TTL: check the token, write, then re-read it.

    False when the claim is gone or no longer ours, before the write (nothing written) or after it (another
    VM replaced the claim between our read and our write, or right after it). Errors propagate (transient),
    as does :class:`ClaimUnreadable`. Both reads are :func:`read_claim_settled`. The write truncates the object
    first, so a write that failed may have left it empty: it is retried at once (``RENEW_WRITE_TRIES``), which
    is safe because nobody else writes a claim that is ours and not stale.
    """
    s = get_session(session)
    cur = read_claim_settled(claim.path, session=s)
    if cur is None or not cur.valid or cur.token != claim.token:
        return False
    now = now or env.utcnow()
    heartbeat_at, expires_at = claim.heartbeat_at, claim.expires_at
    claim.heartbeat_at = now
    claim.expires_at = now + timedelta(seconds=claim.ttl_s)
    data = claim.to_bytes()
    for attempt in range(1, RENEW_WRITE_TRIES + 1):
        try:
            overwrite(s, claim.path, data)
            break
        except Exception as exc:
            if attempt >= RENEW_WRITE_TRIES or not transient_error(exc):
                claim.heartbeat_at, claim.expires_at = heartbeat_at, expires_at  # not renewed
                raise
            log.warning("%s %s: claim write failed (%s); writing it again", claim.kind, claim.key, exc)
            time.sleep(CLAIM_READ_PAUSE_S * attempt)
    after = read_claim_settled(claim.path, session=s)
    if after is None or not after.valid or after.token != claim.token:
        log.error("%s %s: claim replaced by %s while renewing", claim.kind, claim.key,
                  (after.vm or "?") if after is not None else "nobody (deleted)")
        return False
    return True


def release(claim: Claim, *, session: Any | None = None) -> bool:
    """Delete ``claim`` when it is still ours (same token). Never deletes someone else's claim."""
    s = get_session(session)
    cur = read_claim_settled(claim.path, session=s)
    if cur is None:
        return False
    if cur.token != claim.token:
        log.warning("%s: not released, now held by %s", claim.path, cur.vm or "?")
        return False
    ok = unlink(s, claim.path)
    if ok:
        log.info("%s %s released", claim.kind, claim.key)
    return ok


class Heartbeat:
    """Renews claims every ``interval_s`` in a daemon thread; ``lost`` is set when one was taken away.

    Use as a context manager. A transient iRODS error is logged and retried at the next beat. A claim counts
    as lost when it is missing or carries another token, or (self-expiry) when it could not be renewed by
    ``expires_at - interval_s``: another VM may steal it right after it expires, so the holder must stop
    first. A lost claim gets ``claim.lost`` set, ``self.lost`` is set and ``on_lost(claim)`` is called once.
    """

    def __init__(self, claims: Iterable[Claim], *, interval_s: float | None = None, session: Any | None = None,
                 on_lost: Callable[[Claim], None] | None = None) -> None:
        self.claims = list(claims)
        self.interval_s = HEARTBEAT_S if interval_s is None else interval_s
        self._session = session
        self.on_lost = on_lost
        self.lost = threading.Event()
        self.lost_claims: list[Claim] = []
        self.beats = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _mark_lost(self, c: Claim, why: str) -> None:
        log.error("%s %s: claim lost (%s)", c.kind, c.key, why)
        self.lost_claims.append(c)
        c.lost.set()
        self.lost.set()
        if self.on_lost:
            with contextlib.suppress(Exception):
                self.on_lost(c)

    def _open_session(self, base: Any) -> Any:
        if base is None:
            base = get_session(self._session)
        return base, (base.clone() if hasattr(base, "clone") else base)

    def check_expiry(self, c: Claim, now: datetime | None = None) -> bool:
        """Self-expiry: True (and the claim marked lost) when it was not renewed by ``expires_at - interval``."""
        now = now or env.utcnow()
        if c not in self.lost_claims and now >= c.expires_at - timedelta(seconds=self.interval_s):
            self._mark_lost(c, f"not renewed since {_iso(c.heartbeat_at)}; it expires at {_iso(c.expires_at)}")
            return True
        return False

    def _run(self) -> None:
        base = s = None
        try:
            while not self._stop.wait(self.interval_s):
                if s is None:
                    try:
                        base, s = self._open_session(base)
                    except BaseException as exc:  # noqa: BLE001 - never kill the worker from this thread
                        log.error("heartbeat: no iRODS session (%s)", exc)
                for c in list(self.claims):
                    if c in self.lost_claims:
                        continue
                    ok: bool | None = None
                    if s is not None:
                        try:
                            ok = renew(c, session=s)
                        except Exception as exc:  # noqa: BLE001 - transient: retry at the next beat
                            log.warning("heartbeat %s %s failed: %s", c.kind, c.key, exc)
                    if ok is False:
                        self._mark_lost(c, "taken over or deleted")
                    elif ok is None:
                        self.check_expiry(c)
                self.beats += 1
        finally:
            if s is not None and s is not base and hasattr(s, "cleanup"):
                with contextlib.suppress(Exception):
                    s.cleanup()

    def start(self) -> Self:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="claim-heartbeat", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(5.0, self.interval_s))

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


# ---------------------------------------------------------------- sites

def claim_site(site: str, *, session: Any | None = None, ttl_s: float | None = None,
               info: dict[str, Any] | None = None, settle_s: float | None = None,
               vm: str | None = None) -> ClaimResult:
    """Claim a site (status ``done`` without claiming when its ``.done`` marker exists).

    ``.done`` is checked again after the claim is won: another VM may have finished the site and released
    its claim between our first check and our create. The claim is then released and ``done`` returned.
    """
    s = get_session(session)
    if stat(s, site_done_path(site)) is not None:
        return ClaimResult(False, "done", None)
    res = try_claim(site_claim_path(site), "site", site.upper(), ttl_s=ttl_s, session=s, info=info,
                    settle_s=settle_s, vm=vm)
    if res.acquired and res.claim is not None and stat(s, site_done_path(site)) is not None:
        log.warning("site %s: finished by another VM while we claimed it; releasing", site.upper())
        release(res.claim, session=s)
        return ClaimResult(False, "done", None, res.previous)
    return res


@contextlib.contextmanager
def site_claim(site: str, *, session: Any | None = None, ttl_s: float | None = None,
               heartbeat_s: float | None = None, info: dict[str, Any] | None = None,
               settle_s: float | None = None, vm: str | None = None,
               on_lost: Callable[[Claim], None] | None = None, release_on_exit: bool = True) -> Iterator[Claim]:
    """Hold the site's claim for the duration of the block, with a heartbeat thread.

    Yields our :class:`Claim`; ``claim.lost`` (a ``threading.Event``) is set when the claim is lost (taken
    over, deleted, or not renewed in time), and the caller must then stop working on the site (the
    orchestrator takes it as its ``stop_event``). ``claim.status`` says how it was won (acquired, resumed,
    stolen). Raises :class:`SiteDone` when the site has a ``.done`` marker and :class:`SiteClaimed` when
    another live VM holds it. On exit the claim is released (token-checked) unless it was lost or
    ``release_on_exit`` is False.
    """
    s = get_session(session)
    res = claim_site(site, session=s, ttl_s=ttl_s, info=info, settle_s=settle_s, vm=vm)
    if not res.acquired or res.claim is None:
        if res.status == "done":
            raise SiteDone(site.upper(), f"site {site.upper()} is done (.done marker)")
        who = res.claim.vm if res.claim is not None and res.claim.valid else "another VM"
        raise SiteClaimed(site.upper(), f"site {site.upper()} is claimed by {who} ({res.status})", res.claim)
    claim = res.claim
    hb = Heartbeat([claim], interval_s=heartbeat_s, session=s, on_lost=on_lost)
    try:
        with hb:
            yield claim
    finally:
        if release_on_exit and not claim.lost.is_set():
            try:
                release(claim, session=s)
            except Exception as exc:  # noqa: BLE001 - it expires after the TTL
                log.error("site %s: claim not released: %s", site.upper(), exc)


def site_claims_required(environ: dict[str, str] | None = None) -> bool | None:
    """``$NEON_DUCKLAKE_SITE_CLAIMS``: True (``on``: a site claim is in force for this process and its
    children, set by the worker and by ``run`` with claims), False (``off``: ``run --no-claims``), None (unset:
    an operator's command or a library call)."""
    e = os.environ if environ is None else environ
    raw = (e.get(SITE_CLAIMS_ENV) or "").strip().lower()
    if raw in ("1", "on", "true", "yes"):
        return True
    if raw in ("0", "off", "false", "no"):
        return False
    return None


def holds_site(site: str, *, session: Any | None = None, vm: str | None = None,
               required: bool | None = None, retry_s: float | None = None) -> bool:
    """May this process write the site's shared state on the Data Store right now (a site-lake catalog put,
    a journal pointer)? Called immediately before such a write, also from subprocesses that have no
    :class:`Claim` object; a False return aborts the write ("claim lost").

    - With a claim in force (``required``, default :func:`site_claims_required` = ``on``): True only when the
      site's claim is live and this VM's, on this host, machine and boot. False when it has no claim, another
      VM's, a stale one, or one written before this boot.
    - Without one (``off`` or unset: ``run --no-claims``, an operator): True unless another VM holds a live
      claim on the site (never write under a live owner); an absent or stale claim does not block.

    The claim is read with :func:`read_claim_settled`: a claim that the heartbeat (in this process or the
    run's) is rewriting right now, which reads as empty or raises a lock error for a fraction of a second, is
    read again for up to ``retry_s`` (``CLAIM_READ_RETRY_S``). False therefore always means a readable answer
    (no claim, another VM's, stale, another boot); a claim still unreadable after the window raises
    :class:`ClaimUnreadable` (transient: retry later; never write under an unknown owner).
    """
    if required is None:
        required = bool(site_claims_required())
    path = site_claim_path(site)
    c = read_claim_settled(path, session=session, retry_s=retry_s)
    if c is None or not c.valid:  # no claim, or an unparseable one older than the grace: nobody's
        if required:
            log.warning("site %s: %s claim: not this VM's to write", site.upper(), "no" if c is None else "a corrupt")
        return not required
    live_mine = c.is_mine(vm) and not c.is_stale() and c.same_boot() is not False
    if required:
        if not live_mine:
            log.warning("site %s: claim lost: %s", site.upper(), c.describe())
        return live_mine
    return live_mine or c.is_stale()


def claim_is_current(claim: Claim, *, session: Any | None = None, retry_s: float | None = None) -> bool:
    """Re-read the claim: True when it still carries our token (checked before irreversible steps).

    A settled read (:func:`read_claim_settled`): the heartbeat thread may be rewriting the claim right now.
    False (and ``claim.lost`` set) only for a readable answer; :class:`ClaimUnreadable` propagates."""
    cur = read_claim_settled(claim.path, session=session, retry_s=retry_s)
    ok = cur is not None and cur.valid and cur.token == claim.token
    if not ok:
        claim.lost.set()
    return ok


def register_vm(*, session: Any | None = None, vm: str | None = None, settle_s: float | None = None,
                ttl_s: float | None = None) -> Claim:
    """Claim ``<IRODS_ROOT>/.mesa/vms/<vm>.claim`` for this worker (heartbeat it; release it on exit).

    Raises :class:`VmIdCollision` when another live worker (another host, machine or boot that still
    heartbeats) is registered under the same VM id: its claims and spool paths would collide with ours.
    """
    v = vm or vm_id()
    res = try_claim(vm_claim_path(v), "vm", v, ttl_s=ttl_s, session=session, settle_s=settle_s, vm=v,
                    info={"machine": env.machine_id(), "boot": env.boot_id()})
    if res.acquired and res.claim is not None:
        return res.claim
    h = res.claim
    who = (f"host {h.host}, machine {h.machine or '?'}, boot {h.boot or '?'}, pid {h.pid}, heartbeat "
           f"{_iso(h.heartbeat_at)}") if h is not None and h.valid else "an unreadable registration"
    raise VmIdCollision(f"VM id {v!r} is already in use by a live worker ({who}); give every VM a unique "
                        f"NEON_DUCKLAKE_VM_ID (or wait until that registration expires)")


def write_done(site: str, summary: dict[str, Any], *, session: Any | None = None) -> str:
    doc = {"site": site.upper(), "vm": vm_id(), "host": host(), "finishedAt": _iso(env.utcnow()), **summary}
    path = site_done_path(site)
    put_json(get_session(session), path, doc)
    return path


def read_done(site: str, *, session: Any | None = None) -> dict[str, Any] | None:
    doc = read_json(get_session(session), site_done_path(site))
    return doc if isinstance(doc, dict) else ({} if doc is not None else None)


def _named_under(session: Any, name: str, coll_like: str) -> list[str]:
    from irods.column import Like
    from irods.models import Collection, DataObject

    q = session.query(Collection.name, DataObject.name).filter(DataObject.name == name) \
        .filter(Like(Collection.name, coll_like))
    return sorted({f"{r[Collection.name]}/{r[DataObject.name]}" for r in q})


def done_sites(*, session: Any | None = None) -> set[str]:
    """Sites with a ``.done`` marker (one GenQuery)."""
    s = get_session(session)
    return {posixpath.basename(posixpath.dirname(p)) for p in _named_under(s, DONE_NAME, f"{env.IRODS_ROOT}/sites/%")}


def _listed_claim(path: str, s: Any, retry_s: float | None) -> Claim | None:
    """A settled read for a claim listing; a claim still unreadable after the window is listed as an invalid
    claim of unknown age (``modified`` None: not stale, so it counts as held by somebody)."""
    try:
        return read_claim_settled(path, session=s, retry_s=retry_s)
    except ClaimUnreadable as exc:
        log.warning("%s", exc)
        c = Claim.from_bytes(path, b"")
        c.info = {"unreadable": str(exc)[:300]}
        return c


def site_claims(*, session: Any | None = None, retry_s: float | None = None) -> dict[str, Claim]:
    """site -> its current claim (live or stale).

    Each claim is a settled read (:func:`read_claim_settled`): a claim its VM's heartbeat is rewriting right now
    is read again instead of showing up empty, or raising a lock error. One still unreadable after the window is
    listed as an invalid claim that is not stale (held by somebody unknown). Errors of the listing itself
    propagate (the worker's :func:`worker.pick_next` retries its pass)."""
    s = get_session(session)
    out: dict[str, Claim] = {}
    for p in _named_under(s, CLAIM_NAME, f"{env.IRODS_ROOT}/sites/%"):
        c = _listed_claim(p, s, retry_s)
        if c is not None:
            out[posixpath.basename(posixpath.dirname(p))] = c
    return out


def curation_claims(*, session: Any | None = None, retry_s: float | None = None) -> dict[str, Claim]:
    """product -> its curation claim; settled reads, as :func:`site_claims` (a claim being renewed by the
    curating VM must not read as nobody's: ``validate.pending_generic`` would stop waiting for it)."""
    s = get_session(session)
    coll = f"{env.IRODS_ROOT}/curation/claims"
    out: dict[str, Claim] = {}
    for p in sorted(list_objects(s, coll)):
        if not p.endswith(CLAIM_NAME):
            continue
        c = _listed_claim(p, s, retry_s)
        if c is not None:
            out[posixpath.basename(p)[: -len(CLAIM_NAME)]] = c
    return out


# ---------------------------------------------------------------- curation

def curation_sync_enabled(environ: dict[str, str] | None = None) -> bool:
    """``NEON_DUCKLAKE_CURATION_SYNC``: on | off | auto (default: on for workers, off otherwise)."""
    e = os.environ if environ is None else environ
    raw = (e.get(CURATION_SYNC_ENV) or "auto").strip().lower()
    if raw in ("1", "on", "true", "yes"):
        return True
    if raw in ("0", "off", "false", "no"):
        return False
    return bool((e.get(env.VM_ID_ENV) or "").strip()) or \
        (e.get("NEON_DUCKLAKE_HISTORY_MODE") or "").strip().lower() == "spool"


def claim_curation(product: str, *, session: Any | None = None, ttl_s: float | None = None,
                   settle_s: float | None = None, vm: str | None = None) -> ClaimResult:
    return try_claim(curation_claim_path(product), "curation", product,
                     ttl_s=CURATION_TTL_S if ttl_s is None else ttl_s, session=session, settle_s=settle_s, vm=vm)


def release_curation(product: str, *, session: Any | None = None, vm: str | None = None) -> bool:
    """Release this VM's curation claim on ``product`` (it may have been taken by an earlier process)."""
    s = get_session(session)
    cur = read_claim(curation_claim_path(product), session=s)
    if cur is None or not cur.is_mine(vm):
        return False
    return release(cur, session=s)


_EPOCH = datetime.fromtimestamp(0, UTC)


def generic_version(doc: Any) -> tuple[datetime, str]:
    """The content version of a generic curation result: (``validatedAt``, ``runId``); oldest when absent."""
    if not isinstance(doc, dict):
        return _EPOCH, ""
    return _parse_ts(doc.get("validatedAt")) or _EPOCH, str(doc.get("runId") or "")


def validated_here(doc: Any) -> bool:
    """True when ``doc`` (a generic result) was validated by this VM (``validatedBy.vm``; results record the VM id
    only, never a host name, so each VM needs its own ``NEON_DUCKLAKE_VM_ID``)."""
    by = doc.get("validatedBy") if isinstance(doc, dict) else None
    return isinstance(by, dict) and by.get("vm") == vm_id()


def _local_doc(local: Path) -> Any:
    try:
        return json.loads(local.read_text("utf-8"))
    except (OSError, ValueError):
        return None


def fetch_generic(product: str, local: Path, *, session: Any | None = None) -> str:
    """Bring ``curation/generic/<DP>.validated.json`` from the Data Store to ``local``.

    Returns ``absent`` (not on the Data Store), ``same`` (identical bytes), ``kept`` (the local copy has a
    newer content version, :func:`generic_version`) or ``fetched`` (the Data Store copy is newer, of the same
    version, or the local copy is missing or unreadable). File modify times are never compared.
    """
    s = get_session(session)
    obj = stat(s, generic_remote_path(product))
    if obj is None:
        return "absent"
    if local.exists():
        if checksum_matches(obj.checksum, local):
            return "same"
        mine = _local_doc(local)
        if mine is not None and generic_version(mine) > generic_version(read_json(s, obj.path)):
            return "kept"
    fetch(s, obj.path, local, obj=obj)
    return "fetched"


def remote_generic_version(product: str, *, session: Any | None = None) -> tuple[datetime, str] | None:
    """The content version of the Data Store copy; None when there is none."""
    doc = read_json(get_session(session), generic_remote_path(product))
    return None if doc is None else generic_version(doc)


def publish_generic(product: str, local: Path, *, session: Any | None = None) -> dict[str, Any]:
    """Upload a validated generic curation result (checksum-verified, idempotent).

    Never replaces a Data Store copy with a newer content version (:func:`generic_version`); the result
    then has ``skipped: "remote-newer"`` and no upload happens.
    """
    from . import mirror

    s = get_session(session)
    remote = remote_generic_version(product, session=s)
    if remote is not None and remote > generic_version(_local_doc(local)):
        log.warning("%s: the Data Store has a newer generic result (%s); not replacing it", product,
                    _iso(remote[0]))
        return {"skipped": "remote-newer", "errors": 0}
    return mirror.push_tree([(local, generic_remote_path(product), None)], threads=1, session=s)


# ---------------------------------------------------------------- operators: claims list / release

def _resolve_target(target: str) -> tuple[str, str]:
    """A site code, a product code or a full claim path -> (kind, path)."""
    t = target.strip()
    if t.startswith("/"):
        return ("curation" if "/curation/claims/" in t else "site"), t
    if t.upper().startswith(("DP0.", "DP1.", "DP2.", "DP3.", "DP4.")):
        return "curation", curation_claim_path(t.upper())
    return "site", site_claim_path(t)


def claims_main(action: str | None = None, targets: list[str] | None = None, *, force: bool = False,
                as_json: bool = False, session: Any | None = None) -> int:
    """``neon-ducklake claims [--release SITE|DP|PATH]... [--force] [--json]``: ``action='list'|'release'``.

    Listing prints every site, curation and VM claim with its state, plus the finished sites. Releasing
    deletes the named claims (``targets``) when they are this VM's or stale; ``force`` deletes a live claim
    of another VM (only when that VM is known to be gone). There is deliberately no ``release=`` keyword:
    ``cli.py`` then calls ``claims_main("release"|"list", targets, force=..., as_json=...)``.
    """
    action = action or "list"
    s = get_session(session)
    now = env.utcnow()
    if action == "list":
        sites = site_claims(session=s)
        cur = curation_claims(session=s)
        done = done_sites(session=s)
        if as_json:
            print(json.dumps({"now": _iso(now), "done": sorted(done),
                              "sites": {k: c.to_doc() | {"stale": c.is_stale(now), "valid": c.valid}
                                        for k, c in sites.items()},
                              "curation": {k: c.to_doc() | {"stale": c.is_stale(now), "valid": c.valid}
                                           for k, c in cur.items()}}, indent=1, default=str))
            return 0
        print(f"== claims on {env.IRODS_ROOT} at {_iso(now)} (this VM: {vm_id()})")
        for c in list(sites.values()) + list(cur.values()):
            print("  " + c.describe(now))
        if not sites and not cur:
            print("  (none)")
        print(f"== finished sites (.done): {len(done)}" + (f": {', '.join(sorted(done))}" if done else ""))
        return 0
    if action != "release":
        raise SystemExit(f"unknown claims action {action!r} (list or release)")
    if not targets:
        raise SystemExit("claims release: name one or more SITE, DP code or claim path")
    rc = 0
    for t in targets:
        _kind, path = _resolve_target(t)
        c = read_claim(path, session=s)
        if c is None:
            print(f"{t}: no claim")
            continue
        if c.is_mine() or c.is_stale(now) or force:
            unlink(s, path)
            print(f"{t}: released ({c.describe(now)})")
        else:
            print(f"{t}: NOT released, live claim of another VM ({c.describe(now)}); use --force if it is gone")
            rc = 1
    return rc


@contextlib.contextmanager
def holding(result: ClaimResult, *, session: Any | None = None, interval_s: float | None = None,
            on_lost: Callable[[Claim], None] | None = None) -> Iterator[Heartbeat]:
    """Heartbeat an acquired claim for the duration of the block (does not release it)."""
    if not result.acquired or result.claim is None:
        raise ValueError("holding() needs an acquired claim")
    hb = Heartbeat([result.claim], interval_s=interval_s, session=session, on_lost=on_lost)
    with hb:
        yield hb
