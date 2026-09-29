"""`neon-ducklake run` and `neon-ducklake status`: the multi-site orchestrator (docs/STREAMING.md, docs/SCALING.md).

Every stage of every site runs as a SUBPROCESS of this interpreter (``python -m neon_ducklake.cli ...``),
inherits the environment (run the orchestrator through ``scripts/with-secrets.sh``) and logs to
``logs/run/<SITE>/<stage>.log`` (chunk stages: ``logs/run/<SITE>/chunks/<ID>/<stage>.log``). Children start
in their own session, so a terminal Ctrl-C reaches only the orchestrator: the first SIGINT/SIGTERM stops
scheduling and lets running stages finish, a second one terminates the children (SIGTERM, then SIGKILL after
a grace period). A library caller (worker.py) passes ``stop_event`` instead: once it is set the run stops
scheduling, terminates the children at once and returns 130. Every stage is idempotent.

**Stream mode (the default, docs/STREAMING.md "Orchestrator").** Per site:
``restore -> plan -> list -> chunk-plan -> chunks -> finalize -> cleanup``. ``restore`` (``site restore``)
runs first whenever the site has work: it restores the site's journal backup when another VM pushed a newer
one (journal_sync) and fetches the site lake from the Data Store when the local copy is missing or older
(stream.restore_lake). ``chunks`` is the chunk loop: for every chunk of ``stream_chunks`` (site/chunks.py),
in plan order, the per-chunk stages of site/stream.py run as ``site chunk <stage> <SITE> --chunk <ID>``:

    download net-in | stack, avus cpu | upload net-out | apply irods-meta | history local (spool) | verify, delete local

A chunk runs one stage at a time and chunk k+1 runs a stage only after chunk k finished it (pipeline order),
so chunk k+1 downloads while chunk k stacks, uploads or applies. At most ``chunk_depth`` (default 2) chunks
of a site are between download and delete, and a download is dispatched only while the local working set
plus this chunk fits ``NEON_DUCKLAKE_WORKSET_BYTES`` (default 50 GB; ``fetch.local_workset_bytes()``: raw
files on disk, superseded ones included, plus the site lakes' ``data/**``; a chunk runs alone when no other
chunk holds raw bytes) and free disk covers 1.3 x the chunk. ``finalize`` runs only after every chunk is
deleted and waits (retry later) while another VM curates one of the site's products
(``validate.pending_generic``); ``cleanup`` runs last and its rc 3 (some local files kept) counts as done
with a warning. Chunk progress is read from ``stream_chunks.status`` (planned -> downloaded -> stacked -> avus
-> uploaded -> applied -> recorded -> verified -> deleted), so a chunk resumes at the stage after its status;
a ``failed`` chunk resumes after ``stream_chunks.done``.

Retry later, not fail (RETRY_RC): ``RC_RETRY`` (4, site/stream.py) from any stream stage is a transient
condition (OLS taxa still pending, a site-lake push race that could not be resolved); a chunk download's rc 3
(the child's own working-set wait timed out) and finalize's rc 1 (another VM still curates) ask the same. The
task is dispatched again after 5 min, then 10, 20, 40 and 60 min (backoff capped at 1 h), at most 6 times; then
it fails and the next invocation starts over.

History: stream runs always spool (``NEON_DUCKLAKE_HISTORY_MODE=spool`` is exported to every child; the
central ``neon-ducklake recorder`` ingests the spool into MESA in few, large snapshots). ``direct`` is refused
in stream mode (per-chunk snapshots would spend the 900-snapshot MESA budget after a few sites) until DuckDB
2.0 multi-writer catalogs exist: ``NEON_DUCKLAKE_MESA_MULTIWRITER=1`` is that switch.

Data Store coordination (stream): ``run`` takes ``coord.site_claim(site)`` for each site before its first
stage when its stages include one that writes to the Data Store (restore, chunks, finalize, cleanup;
``--no-claims`` skips it; ``run_sites``, called by the worker that holds the claim itself, never takes
one). A site with a ``.done`` marker or a live claim of another VM is skipped; a site whose claim is lost
mid-run is stopped (its running stages are terminated). Every stream stage learns whether a claim is in force
(``NEON_DUCKLAKE_SITE_CLAIMS`` on|off, ``NEON_DUCKLAKE_REQUIRE_CLAIM`` 1|0): with one, ``coord.holds_site`` must
confirm this VM's live claim right before a site-lake catalog or journal pointer write. The site's journal is
pushed to the Data Store (``journal_sync.push_site_journal``: an immutable versioned set, its pointer written
last) after every chunk's ``apply``, ``history`` and ``delete`` and after ``finalize`` and ``cleanup``; a failed
push fails that stage. After ``finalize`` this VM's ontology export is published to ``ref/ontology_maps/vm/<vm>/``
(``bootstrap.publish_ontology``; a failure is a warning). A claimed site that finishes (cleanup ok, not a sample
run) gets its ``.done`` marker before the claim is released. A site this host finished whose marker is missing
(its write failed, or it finished under ``--no-claims``) plans nothing; ``run`` then claims it briefly and writes
the marker, unless another VM pushed a newer journal backup since: then the site runs again under the claim from
``restore`` (``RERUN_STAGES``) and gets its marker when that finishes.

**Whole-site mode (``--no-stream``, the previous behaviour).** Stages (in order) and resource classes. Each
class is a semaphore over sites, so site B downloads while site A uploads:

    plan local | list, download net-in (2) | stack, terms, cards, validate cpu (1) | curate llm (1)
    upload net-out (1) | apply irods-meta (1) | record mesa (1) | verify, evict local (unlimited)

``apply`` runs ``site build-avus`` then ``tag --apply-only``; ``record`` is ``tag --record-only``.

State: ``work/state.sqlite`` table ``site_runs(site, stage, status, attempt, started_at, finished_at, rc,
log_path, mode, info)`` for site-level stages and ``site_run_chunks(site, chunk_id, stage, ...)`` for chunk
stages; status is pending | running | ok | failed | skipped. A site resumes at its first stage not ``ok`` (in
a compatible mode: a sample run never satisfies a full run); a failed stage stops that site (the others
continue) and is retried by the next invocation.

Guards:
- disk: a download is dispatched only when free disk >= 1.3 x the bytes it will write plus the
  reservations of downloads in flight; otherwise it waits (and fails if nothing is running that could free
  space);
- working set (stream): see above;
- evicted inputs (whole-site): the download stage restores evicted files of products that changed, and the
  stack stage refuses to run while a product would be re-stacked from a partial input set (site/evict.py);
- MESA: ``record`` (and, with the direct backend, chunk ``history`` and ``finalize``) waits while another
  process holds the MESA catalog and is retried when it is free (``NEON_DUCKLAKE_WAIT_TIMEOUT_S``, default
  6 h, then it fails naming the process);
- curation: ``validate`` is ``ok`` without running when the site has no proposals (all products reuse
  ``curation/generic`` results or are external).
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import env, state
from .tuning import transient_irods

STAGES: tuple[str, ...] = ("plan", "list", "download", "stack", "terms", "cards", "curate", "validate", "upload",
                           "apply", "record", "verify", "evict")
STAGE_CLASS: dict[str, str] = {
    "plan": "local", "list": "net-in", "download": "net-in", "stack": "cpu", "terms": "cpu", "cards": "cpu",
    "curate": "llm", "validate": "cpu", "upload": "net-out", "apply": "irods-meta", "record": "mesa",
    "verify": "local", "evict": "local",
}
CLASS_LIMITS: dict[str, int | None] = {"net-in": 2, "cpu": 1, "llm": 1, "net-out": 1, "irods-meta": 1, "mesa": 1,
                                       "finalize": 1, "local": None}
STATUSES = ("pending", "running", "ok", "failed", "skipped")
SYMBOLS = {"ok": "+", "failed": "X", "running": ">", "pending": ".", "skipped": "-"}
SHORT = {"plan": "plan", "list": "list", "download": "dl", "stack": "stk", "terms": "trm", "cards": "crd",
         "curate": "cur", "validate": "val", "upload": "up", "apply": "app", "record": "rec", "verify": "ver",
         "evict": "evc", "restore": "rst", "chunk-plan": "cpl", "chunks": "chk", "finalize": "fin",
         "cleanup": "cln"}
DISK_FACTOR = 1.3
SNAPSHOT_BUDGET = 900
KILL_GRACE_S = 15.0
AQUATIC = ("Aquatic Wadeable Stream", "Aquatic Lake", "Aquatic Non-wadeable Stream")
HABITATS: dict[str, tuple[str, ...]] = {"terrestrial": ("Terrestrial",), "aquatic": AQUATIC,
                                        **{h.lower(): (h,) for h in AQUATIC}}
ORDERS = ("aquatic-first", "size", "name", "given")

# ---- streaming (docs/STREAMING.md)
STREAM_STAGES: tuple[str, ...] = ("restore", "plan", "list", "chunk-plan", "chunks", "finalize", "cleanup")
PRE_STAGES: tuple[str, ...] = ("plan", "list", "chunk-plan")
POST_STAGES: tuple[str, ...] = ("finalize", "cleanup")
STREAM_STAGE_CLASS: dict[str, str] = {"restore": "net-in", "plan": "local", "list": "net-in", "chunk-plan": "local",
                                      "chunks": "local", "finalize": "finalize", "cleanup": "local"}
CHUNK_STAGES: tuple[str, ...] = ("download", "stack", "avus", "upload", "apply", "history", "verify", "delete")
CHUNK_STAGE_CLASS: dict[str, str] = {"download": "net-in", "stack": "cpu", "avus": "cpu", "upload": "net-out",
                                     "apply": "irods-meta", "history": "mesa", "verify": "local", "delete": "local"}
# stream_chunks.status after each stage; the index of a chunk's status is the index of its next stage
CHUNK_STATUS_ORDER: tuple[str, ...] = ("planned", "downloaded", "stacked", "avus", "uploaded", "applied",
                                       "recorded", "verified", "deleted")
CHUNK_STATUS_ALIASES = {"spooled": "recorded"}
ALL_STAGES: tuple[str, ...] = STAGES + tuple(s for s in STREAM_STAGES if s not in STAGES)
HISTORY_MODES = ("direct", "spool")
ENV_CHUNK_BYTES = "NEON_DUCKLAKE_CHUNK_BYTES"
ENV_WORKSET_BYTES = "NEON_DUCKLAKE_WORKSET_BYTES"
ENV_HISTORY_MODE = "NEON_DUCKLAKE_HISTORY_MODE"
# the DuckDB 2.0 multi-writer switch: only with it may a stream run record history 'direct' (per chunk)
ENV_MULTIWRITER = "NEON_DUCKLAKE_MESA_MULTIWRITER"
ENV_WAIT_TIMEOUT_S = "NEON_DUCKLAKE_WAIT_TIMEOUT_S"
CHUNK_BYTES_DEFAULT = 8 * 10**9      # display only: site/chunks.py owns the chunk size (CHUNK_BYTES)
WORKSET_BYTES_DEFAULT = 50 * 10**9
CHUNK_DEPTH = 2
WAIT_TIMEOUT_S = 6 * 3600.0   # a task waiting for MESA or for another VM's curation fails after this long
GATE_RECHECK_S = 60.0         # how often the condition of a waiting task is checked again
RETRY_DELAY_S = 300.0         # first delay before a stage that asked to be retried later is dispatched again
RETRY_BACKOFF = 2.0           # each further retry of the same task waits this factor longer ...
RETRY_MAX_DELAY_S = 3600.0    # ... up to this delay (300, 600, 1200, 2400, 3600, 3600 s: ~3.3 h in all)
MAX_RETRIES = 6
# (stage, is a chunk stage) after which the site's journal is pushed to the Data Store (journal_sync)
JOURNAL_PUSH_AFTER: frozenset[tuple[str, bool]] = frozenset({("apply", True), ("history", True), ("delete", True),
                                                             ("finalize", False), ("cleanup", False)})
# site/stream.py RC_RETRY: a transient condition (OLS taxa still pending, a site-lake push race that could not be
# resolved): any stream stage may return it, and the task is dispatched again later (bounded, with backoff)
RC_RETRY = 4
# stage return codes that mean "retry later", by (stage, is a chunk stage): RC_RETRY from every stream stage, plus
# download 3 = blocked by the working set and finalize 1 = not ready (another VM still curates one of the site's
# products). cleanup 3 = some local files were kept: done, with a warning.
RETRY_RC: dict[tuple[str, bool], frozenset[int]] = {
    **{(s, True): frozenset({RC_RETRY}) for s in CHUNK_STAGES},
    **{(s, False): frozenset({RC_RETRY}) for s in STREAM_STAGES if s != "chunks"},
    ("download", True): frozenset({3, RC_RETRY}),
    ("finalize", False): frozenset({1, RC_RETRY}),
}
WARN_RC: dict[tuple[str, bool], int] = {("cleanup", False): 3}
# per-site results that are not failures
NOT_FAILED = ("ok", "nothing to do", "marked done", "done elsewhere", "claimed elsewhere")
# stream stages that write to the Data Store or the site's lake: a site runs them only under its claim
CLAIM_STAGES = frozenset({"restore", "chunks", "finalize", "cleanup"})
# a site this host finished (cleanup ok) whose journal backup on the Data Store is newer (another VM worked on it
# since): these stages run again under the claim instead of writing its .done marker
RERUN_STAGES: tuple[str, ...] = ("restore", "chunk-plan", "chunks", "finalize", "cleanup")
# exported to every stream stage (and set in the orchestrator for the run): is a site claim in force? 'on' when
# run or its caller (the worker) holds it: coord.holds_site, checked right before a site-lake catalog or journal
# pointer write, then requires this VM's live claim ("claim lost" otherwise); 'off' for `run --no-claims`: only a
# live claim of another VM blocks. ENV_REQUIRE_CLAIM (site/upload.py) says the same for the catalog guard.
ENV_SITE_CLAIMS = "NEON_DUCKLAKE_SITE_CLAIMS"
ENV_REQUIRE_CLAIM = "NEON_DUCKLAKE_REQUIRE_CLAIM"

SCHEMA = """
CREATE TABLE IF NOT EXISTS site_runs (
  site TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
  attempt INTEGER NOT NULL DEFAULT 0, started_at TEXT, finished_at TEXT, rc INTEGER, log_path TEXT,
  mode TEXT, info TEXT, PRIMARY KEY (site, stage)
);
CREATE TABLE IF NOT EXISTS site_run_chunks (
  site TEXT NOT NULL, chunk_id TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
  attempt INTEGER NOT NULL DEFAULT 0, started_at TEXT, finished_at TEXT, rc INTEGER, log_path TEXT,
  mode TEXT, info TEXT, PRIMARY KEY (site, chunk_id, stage)
);
"""


def say(msg: str) -> None:
    print(f"{env.utcnow().strftime('%H:%M:%S')} {msg}", flush=True)


def _gb(n: float | None) -> str:
    return f"{(n or 0) / 1e9:.1f}"


_SIZE_UNITS = {"": 1, "k": 10**3, "m": 10**6, "g": 10**9, "t": 10**12,
               "ki": 2**10, "mi": 2**20, "gi": 2**30, "ti": 2**40}


def parse_size(text: str | int) -> int:
    """Bytes from ``8000000000``, ``8G``, ``8GB``, ``500M``, ``1.5T`` (decimal) or ``8GiB`` (binary)."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]i?)?b?\s*", str(text).lower())
    if not m:
        raise ValueError(f"not a size: {text!r} (bytes, or a number with K/M/G/T, e.g. 8G or 50GB)")
    n = int(float(m.group(1)) * _SIZE_UNITS[m.group(2) or ""])
    if n < 1:
        raise ValueError(f"size must be positive: {text!r}")
    return n


def env_bytes(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return parse_size(raw)
    except ValueError as exc:
        raise SystemExit(f"${name}: {exc}") from exc


def env_seconds(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        v = float(raw)
    except ValueError as exc:
        raise SystemExit(f"${name}: not a number of seconds: {raw!r}") from exc
    if v <= 0:
        raise SystemExit(f"${name} must be positive")
    return v


def multiwriter_enabled() -> bool:
    """``$NEON_DUCKLAKE_MESA_MULTIWRITER`` is on: the MESA catalog takes parallel writers (DuckDB 2.0)."""
    return os.environ.get(ENV_MULTIWRITER, "").strip().lower() in ("1", "true", "yes", "on")


DIRECT_IN_STREAM = (
    "stream mode records MESA history through the spool and the central recorder (`neon-ducklake recorder "
    "--watch` on the MESA-catalog host): 'direct' history would take the single MESA lock and spend at least one "
    "of the 900 MESA snapshots per chunk (a few sites' worth). It returns with DuckDB 2.0 multi-writer catalogs: "
    f"set {ENV_MULTIWRITER}=1 once the MESA catalog supports them (docs/STREAMING.md, 'History backends'). "
    f"Drop --history-mode direct / unset {ENV_HISTORY_MODE}, or use --no-stream")


# ---------------------------------------------------------------- config

@dataclass
class RunConfig:
    stages: tuple[str, ...] = STAGES
    from_stage: str | None = None
    sample: int | None = None
    evict: bool = True
    max_in_flight: int = 3
    restack: bool = False
    threads: int | None = None
    poll_s: float = 5.0
    disk_factor: float = DISK_FACTOR
    stream: bool = False
    chunk_bytes: int | None = None
    workset_bytes: int | None = None
    history_mode: str | None = None
    chunk_depth: int = CHUNK_DEPTH
    claims: bool = False               # stream: take coord.site_claim per site (run; the worker holds its own)
    claims_held: bool = False          # stream: the caller (worker.py) holds the site's claim itself

    @property
    def mode(self) -> str:
        return f"sample:{self.sample}" if self.sample else "full"

    @property
    def all_stages(self) -> tuple[str, ...]:
        return STREAM_STAGES if self.stream else STAGES

    def selected(self) -> list[str]:
        """Stages this run may execute (whole-site: evict dropped for --sample / --no-evict)."""
        if self.stream:
            return [s for s in STREAM_STAGES if s in self.stages]
        return [s for s in STAGES if s in self.stages and not (s == "evict" and self.skips_evict)]

    @property
    def skips_evict(self) -> bool:
        return bool(self.sample) or not self.evict

    @property
    def workset(self) -> int:
        """Working-set cap in bytes: --workset-bytes, else $NEON_DUCKLAKE_WORKSET_BYTES, else 50 GB."""
        return self.workset_bytes or env_bytes(ENV_WORKSET_BYTES, WORKSET_BYTES_DEFAULT)

    @property
    def history(self) -> str:
        """History backend: --history-mode, else $NEON_DUCKLAKE_HISTORY_MODE, else spool (stream) / direct."""
        raw = self.history_mode or os.environ.get(ENV_HISTORY_MODE, "").strip()
        return (raw or ("spool" if self.stream else "direct")).lower()

    def child_env(self) -> dict[str, str]:
        """Settings passed to every stage subprocess (on top of the inherited environment).

        In stream mode the working-set cap, the history backend (spool) and whether a site claim is in force
        (``NEON_DUCKLAKE_SITE_CLAIMS``: off only for ``run --no-claims``) are always exported, so the children
        use exactly the orchestrator's values whatever their environment says."""
        out: dict[str, str] = {}
        if self.chunk_bytes:
            out[ENV_CHUNK_BYTES] = str(self.chunk_bytes)
        if self.stream or self.workset_bytes:
            out[ENV_WORKSET_BYTES] = str(self.workset)
        if self.stream or self.history_mode:
            out[ENV_HISTORY_MODE] = self.history
        if self.stream:
            held = self.claims or self.claims_held
            out[ENV_SITE_CLAIMS] = "on" if held else "off"
            out[ENV_REQUIRE_CLAIM] = "1" if held else "0"
        return out


def make_config(*, stages: str | Iterable[str] | None = None, from_stage: str | None = None,
                sample: int | None = None, evict: bool = True, max_in_flight: int = 3, restack: bool = False,
                threads: int | None = None, poll_s: float = 5.0, stream: bool = False,
                chunk_bytes: int | None = None, workset_bytes: int | None = None, history_mode: str | None = None,
                chunk_depth: int = CHUNK_DEPTH) -> RunConfig:
    """Validated RunConfig (SystemExit with a clear message on a bad combination)."""
    if isinstance(stages, str):
        stages = [s.strip() for s in stages.split(",") if s.strip()]
    universe = STREAM_STAGES if stream else STAGES
    chosen = tuple(stages) if stages else universe
    bad = [s for s in chosen if s not in universe]
    if bad:
        kind = "stream" if stream else "whole-site (--no-stream)"
        hint = ""
        if stream and any(s in STAGES for s in bad):
            hint = f"; per-chunk stages ({', '.join(CHUNK_STAGES)}) run inside 'chunks'"
        raise SystemExit(f"unknown stage(s) {', '.join(bad)}; {kind} stages are: {', '.join(universe)}{hint}")
    if sample is not None and sample < 1:
        raise SystemExit("--sample must be >= 1")
    if max_in_flight < 1:
        raise SystemExit("--max-sites-in-flight must be >= 1")
    if chunk_depth < 1:
        raise SystemExit("--chunks-in-flight must be >= 1")
    for name, v in (("--chunk-bytes", chunk_bytes), ("--workset-bytes", workset_bytes)):
        if v is not None and v < 1:
            raise SystemExit(f"{name} must be positive")
    if history_mode is not None and history_mode not in HISTORY_MODES:
        raise SystemExit(f"--history-mode must be one of {', '.join(HISTORY_MODES)}")
    if stream:
        if restack:
            raise SystemExit("--restack is a whole-site option: streamed chunks are stacked incrementally and "
                             "their raw files are deleted (use --no-stream --restack)")
        if not evict:
            raise SystemExit("--no-evict keeps raw files, but stream mode deletes every chunk once its Data Store "
                             "copy is verified (use --no-stream --no-evict to keep them)")
    cfg = RunConfig(stages=tuple(s for s in universe if s in chosen), from_stage=from_stage, sample=sample,
                    evict=evict, max_in_flight=max_in_flight, restack=restack, threads=threads, poll_s=poll_s,
                    stream=stream, chunk_bytes=chunk_bytes, workset_bytes=workset_bytes,
                    history_mode=history_mode, chunk_depth=chunk_depth)
    if cfg.history not in HISTORY_MODES:
        raise SystemExit(f"${ENV_HISTORY_MODE}={cfg.history!r}: must be one of {', '.join(HISTORY_MODES)}")
    if stream and cfg.history == "direct" and not multiwriter_enabled():
        raise SystemExit(DIRECT_IN_STREAM)
    sel = cfg.selected()
    if restack:
        if "download" not in sel or "stack" not in sel:
            raise SystemExit("--restack needs the download and stack stages (it re-downloads evicted inputs)")
        cfg.from_stage = from_stage or "download"
        if STAGES.index(cfg.from_stage) > STAGES.index("download"):
            raise SystemExit("--restack re-downloads evicted inputs: --from-stage must be download or earlier")
    if cfg.from_stage is not None and cfg.from_stage not in sel:
        raise SystemExit(f"--from-stage {cfg.from_stage} is not among the selected stages ({', '.join(sel)})")
    return cfg


def covers(have: str | None, want: str) -> bool:
    """Does a stage done in mode ``have`` satisfy mode ``want``? full covers all; sample:M covers sample:N<=M."""
    if have == "full" or have == want:
        return True
    if have and want and have.startswith("sample:") and want.startswith("sample:"):
        return int(have.split(":", 1)[1]) >= int(want.split(":", 1)[1])
    return False


# ---------------------------------------------------------------- persistence

class Store:
    """``site_runs`` + ``site_run_chunks`` in work/state.sqlite (one connection, serialised by a lock).

    Every method takes ``chunk=None`` for a site-level stage (``site_runs``) or a chunk id for a chunk stage
    (``site_run_chunks``)."""

    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        self.lock = threading.Lock()
        con.executescript(SCHEMA)

    def rows(self, site: str | None = None) -> dict[str, dict[str, dict[str, Any]]]:
        with self.lock:
            q = "SELECT * FROM site_runs" + (" WHERE site=?" if site else "") + " ORDER BY site"
            out: dict[str, dict[str, dict[str, Any]]] = {}
            for r in self.con.execute(q, (site,) if site else ()):
                out.setdefault(r["site"], {})[r["stage"]] = dict(r)
            return out

    def chunk_rows(self, site: str) -> dict[str, dict[str, dict[str, Any]]]:
        """chunk_id -> stage -> row, for one site."""
        with self.lock:
            out: dict[str, dict[str, dict[str, Any]]] = {}
            for r in self.con.execute("SELECT * FROM site_run_chunks WHERE site=?", (site,)):
                out.setdefault(r["chunk_id"], {})[r["stage"]] = dict(r)
            return out

    @staticmethod
    def _where(chunk: str | None) -> tuple[str, str, str]:
        if chunk is None:
            return "site_runs", "site, stage", "site=? AND stage=?"
        return "site_run_chunks", "site, chunk_id, stage", "site=? AND chunk_id=? AND stage=?"

    @staticmethod
    def _key(site: str, stage: str, chunk: str | None) -> tuple[str, ...]:
        return (site, stage) if chunk is None else (site, chunk, stage)

    def _get(self, site: str, stage: str, chunk: str | None, col: str) -> Any:
        table, _k, where = self._where(chunk)
        with self.lock:
            row = self.con.execute(f"SELECT {col} FROM {table} WHERE {where}", self._key(site, stage, chunk)).fetchone()
        return row[col] if row else None

    def _upsert(self, site: str, stage: str, chunk: str | None = None, **cols: Any) -> None:
        table, keys, _w = self._where(chunk)
        names = ", ".join(cols)
        marks = ", ".join("?" * len(cols))
        sets = ", ".join(f"{k}=excluded.{k}" for k in cols)
        nkeys = len(keys.split(","))
        with self.lock:
            self.con.execute(f"INSERT INTO {table}({keys}, {names}) VALUES ({', '.join('?' * nkeys)}, {marks}) "
                             f"ON CONFLICT({keys}) DO UPDATE SET {sets}",
                             (*self._key(site, stage, chunk), *cols.values()))

    def set_pending(self, site: str, stages: Iterable[str]) -> None:
        for s in stages:
            self._upsert(site, s, status="pending", rc=None, finished_at=None)

    def start(self, site: str, stage: str, log_path: str, mode: str, chunk: str | None = None) -> int:
        attempt = (self._get(site, stage, chunk, "attempt") or 0) + 1
        self._upsert(site, stage, chunk, status="running", attempt=attempt, started_at=env.utcnow().isoformat(),
                     finished_at=None, rc=None, log_path=log_path, mode=mode, info=None)
        return attempt

    def note_pid(self, site: str, stage: str, pid: int, chunk: str | None = None) -> None:
        info = _json(self._get(site, stage, chunk, "info"))
        info.setdefault("pids", []).append(pid)
        info["host"] = _host()  # site_run_chunks travel with the journal backup to other VMs
        self._upsert(site, stage, chunk, info=json.dumps(info))

    def finish(self, site: str, stage: str, status: str, rc: int | None, info: str = "",
               chunk: str | None = None) -> None:
        prev = _json(self._get(site, stage, chunk, "info"))
        prev.pop("pids", None)
        prev.pop("host", None)
        if info:
            prev["message"] = info
        self._upsert(site, stage, chunk, status=status, rc=rc, finished_at=env.utcnow().isoformat(),
                     info=json.dumps(prev) if prev else None)

    def skip(self, site: str, stage: str, info: str) -> None:
        self._upsert(site, stage, status="skipped", rc=None, finished_at=env.utcnow().isoformat(),
                     info=json.dumps({"message": info}))

    def note(self, site: str, stage: str, status: str, info: str) -> None:
        """Set a site-level row's status and message without touching attempt/timestamps."""
        self._upsert(site, stage, status=status, info=json.dumps({"message": info}))

    def reset_chunk(self, site: str, chunk: str, keep: str = "download") -> None:
        """A new pass of a chunk starts at ``keep``: forget its other stage rows (``keep`` retains its attempt)."""
        with self.lock:
            self.con.execute("DELETE FROM site_run_chunks WHERE site=? AND chunk_id=? AND stage != ?",
                             (site, chunk, keep))

    def recover_stale(self) -> list[tuple[str, str]]:
        """'running' rows left by a dead orchestrator -> failed; refuse while one of their children lives.

        Returns (site, stage) pairs; a chunk stage is reported as (site, 'chunk <ID> <stage>')."""
        with self.lock:
            rows = [(None, dict(r)) for r in self.con.execute("SELECT * FROM site_runs WHERE status='running'")]
            rows += [(r["chunk_id"], dict(r)) for r in
                     self.con.execute("SELECT * FROM site_run_chunks WHERE status='running'")]
        out = []
        for chunk, r in rows:
            what = r["stage"] if chunk is None else f"chunk {chunk} {r['stage']}"
            info = _json(r["info"])
            local = info.get("host") in (None, _host())  # a row restored from another VM's journal: not our pid
            live = [p for p in info.get("pids", []) if local and _alive(int(p))]
            if live:
                raise SystemExit(f"stage {what} of {r['site']} is still running as pid {live[0]} (left by a "
                                 f"previous orchestrator): wait for it or kill it, then re-run")
            self.finish(r["site"], r["stage"], "failed", None, "interrupted: the orchestrator exited mid-stage",
                        chunk=chunk)
            out.append((r["site"], what))
        return out


def _json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        v = json.loads(value)
    except (TypeError, ValueError):
        return {"message": str(value)}
    return v if isinstance(v, dict) else {"message": str(v)}


def _host() -> str:
    import socket

    return socket.gethostname()


def _alive(pid: int) -> bool:
    try:
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False
    return b"neon_ducklake" in cmd or b"neon-ducklake" in cmd


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def log_path(site: str, stage: str, chunk: str | None = None) -> Path:
    base = env.ROOT / "logs" / "run" / site
    return base / f"{stage}.log" if chunk is None else base / "chunks" / chunk / f"{stage}.log"


def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(env.ROOT))
    except ValueError:
        return str(p)


# ---------------------------------------------------------------- stage commands

def stage_commands(site: str, stage: str, *, sample: int | None = None, threads: int | None = None,
                   restack: bool = False, python: str | None = None) -> list[list[str]]:
    """argv list(s) of one whole-site stage (``apply`` is two commands)."""
    base = [python or sys.executable, "-m", "neon_ducklake.cli"]
    thr = ["--threads", str(threads)] if threads else []
    smp = ["--sample", str(sample)] if sample else []
    if stage == "plan":
        return [base + ["site", "plan", site]]
    if stage in ("list", "download"):
        return [base + ["site", stage, site, *smp, *thr]]
    if stage == "stack":
        return [base + ["site", "stack", site, *(["--force"] if restack else [])]]
    if stage in ("terms", "cards", "curate", "validate"):
        return [base + ["site", stage, site]]
    if stage == "upload":
        return [base + ["site", "upload", site, *thr]]
    if stage == "apply":
        return [base + ["site", "build-avus", site], base + ["tag", site, "--apply-only"]]
    if stage == "record":
        return [base + ["tag", site, "--record-only"]]
    if stage == "verify":
        return [base + ["verify", site]]
    if stage == "evict":
        return [base + ["site", "evict", site]]
    raise ValueError(f"unknown stage {stage}")


def stream_commands(site: str, stage: str, chunk: str | None = None, *, sample: int | None = None,
                    threads: int | None = None, python: str | None = None) -> list[list[str]]:
    """argv list(s) of one stream stage; ``chunks`` (the loop itself) has none."""
    base = [python or sys.executable, "-m", "neon_ducklake.cli"]
    thr = ["--threads", str(threads)] if threads else []
    if chunk is not None:
        if stage not in CHUNK_STAGES:
            raise ValueError(f"unknown chunk stage {stage}")
        return [base + ["site", "chunk", stage, site, "--chunk", chunk,
                        *(thr if stage in ("download", "upload", "apply") else [])]]
    if stage == "plan":
        return [base + ["site", "plan", site]]
    if stage == "list":
        return [base + ["site", "list", site, *(["--sample", str(sample)] if sample else []), *thr]]
    if stage == "finalize":
        return [base + ["site", "finalize", site, *thr]]
    if stage in ("restore", "chunk-plan", "cleanup"):
        return [base + ["site", stage, site]]
    if stage == "chunks":
        return []
    raise ValueError(f"unknown stream stage {stage}")


class SubprocessRunner:
    """Runs a stage's command(s) in their own session; output appended to the stage log (see log_path)."""

    def __init__(self, cfg: RunConfig, *, on_spawn: Callable[..., None] | None = None,
                 grace_s: float = KILL_GRACE_S) -> None:
        self.cfg = cfg
        self.on_spawn = on_spawn
        self.grace_s = grace_s
        self.killing = False
        self.killed_sites: set[str] = set()
        self._procs: dict[tuple[str, str, str | None], subprocess.Popen] = {}
        self._lock = threading.Lock()

    def commands(self, site: str, stage: str, chunk: str | None) -> list[list[str]]:
        if self.cfg.stream:
            return stream_commands(site, stage, chunk, sample=self.cfg.sample, threads=self.cfg.threads)
        return stage_commands(site, stage, sample=self.cfg.sample, threads=self.cfg.threads,
                              restack=self.cfg.restack)

    def __call__(self, site: str, stage: str, chunk: str | None = None) -> int:
        path = log_path(site, stage, chunk)
        path.parent.mkdir(parents=True, exist_ok=True)
        child_env = dict(os.environ)
        child_env["PYTHONUNBUFFERED"] = "1"
        child_env.update(self.cfg.child_env())
        rc = 0
        key = (site, stage, chunk)
        with path.open("a", encoding="utf-8") as fh:
            for argv in self.commands(site, stage, chunk):
                if self.killing or site in self.killed_sites:
                    return -int(signal.SIGTERM)
                shown = " ".join(["neon-ducklake", *argv[3:]]) if argv[1:3] == ["-m", "neon_ducklake.cli"] \
                    else " ".join(argv)
                fh.write(f"\n=== {env.utcnow().isoformat()} $ {shown}\n")
                fh.flush()
                t0 = time.monotonic()
                p = subprocess.Popen(argv, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                     cwd=str(env.ROOT), env=child_env, start_new_session=True)
                with self._lock:
                    self._procs[key] = p
                    late = self.killing or site in self.killed_sites  # a kill raced with the spawn
                if late:
                    self._terminate([p])
                if self.on_spawn:
                    self.on_spawn(site, stage, p.pid, *(() if chunk is None else (chunk,)))
                try:
                    rc = p.wait()
                finally:
                    with self._lock:
                        self._procs.pop(key, None)
                fh.write(f"=== exit {rc} after {time.monotonic() - t0:.1f}s\n")
                fh.flush()
                if rc != 0:
                    return rc
        return rc

    def kill(self) -> None:
        """Terminate every running child process group (SIGTERM now, SIGKILL after the grace period)."""
        self.killing = True
        with self._lock:
            procs = list(self._procs.values())
        self._terminate(procs)

    def kill_site(self, site: str) -> None:
        """Terminate the running children of one site (its claim was lost) and start no new ones for it."""
        with self._lock:
            self.killed_sites.add(site)
            procs = [p for k, p in self._procs.items() if k[0] == site]
        self._terminate(procs)

    def _terminate(self, procs: list[subprocess.Popen]) -> None:
        for p in procs:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGTERM)

        def finish() -> None:
            deadline = time.monotonic() + self.grace_s
            for p in procs:
                with contextlib.suppress(subprocess.TimeoutExpired):
                    p.wait(timeout=max(deadline - time.monotonic(), 0.01))
                if p.poll() is None:
                    with contextlib.suppress(ProcessLookupError, PermissionError):
                        os.killpg(p.pid, signal.SIGKILL)

        threading.Thread(target=finish, name="run-kill", daemon=True).start()


# ---------------------------------------------------------------- guards

@dataclass
class Outcome:
    status: str  # ok | failed | retry (not a failure: the task is dispatched again later)
    rc: int | None = None
    info: str = ""


def retry_reason(stage: str, chunk: str | None, rc: int) -> str:
    """Why a stream stage asked to be retried later (its rc is in RETRY_RC)."""
    if rc == RC_RETRY:
        return (f"transient (exit {rc}): e.g. OLS unavailable with taxa pending, or a site-lake push race; see the "
                "log; retrying later")
    if chunk is not None and stage == "download":
        return f"blocked (exit {rc}): the working set stayed full; retrying later"
    if chunk is None and stage == "finalize":
        return f"not ready (exit {rc}): another VM still curates one of the site's products; retrying later"
    return f"exit {rc}; retrying later"


class DiskGuard:
    """Free disk >= factor x (bytes the download will write) + reservations of downloads in flight."""

    def __init__(self, need_bytes: Callable[[str], int], free_bytes: Callable[[], int],
                 factor: float = DISK_FACTOR) -> None:
        self.need_bytes, self.free_bytes, self.factor = need_bytes, free_bytes, factor
        self.reserved: dict[str, int] = {}
        self._need: dict[str, int] = {}

    def try_reserve(self, site: str) -> tuple[bool, str]:
        if site not in self._need:
            self._need[site] = int(self.factor * max(int(self.need_bytes(site) or 0), 0))
        need = self._need[site]
        others = sum(v for k, v in self.reserved.items() if k != site)
        free = int(self.free_bytes())
        if need == 0 or free >= need + others:
            self.reserved[site] = need
            return True, ""
        return False, (f"disk guard: needs {_gb(need)} GB ({self.factor} x download) + {_gb(others)} GB reserved "
                       f"by downloads in flight, {_gb(free)} GB free")

    def release(self, site: str) -> None:
        self.reserved.pop(site, None)
        self._need.pop(site, None)


ChunkKey = tuple[str, str]  # (site, chunk_id)


class WorksetGuard:
    """Stream downloads: local working set + this chunk's download <= cap, and free disk >= factor x the download.

    ``held(verify=False)`` maps a key to local raw bytes: (site, chunk_id) for a stream chunk's files and any
    other key (``OTHER_KEY``) for raw files of no chunk (whole-site downloads, kept sample probes).
    ``total()`` (``fetch.local_workset_bytes()``, the measure of ``site/fetch.py``'s own wait, so a stage never
    blocks on what the orchestrator let through) is everything the working set holds on disk: raw files,
    superseded ones included, plus the site lakes' ``data/**``; what it holds beyond the tracked raw bytes counts
    as well. A chunk's own files never count against it, and only the bytes its download adds are checked.
    Downloads in flight count by their reservation until they finish. When no other chunk holds raw bytes, any
    chunk may start (a chunk larger than the cap runs alone; a lake alone never blocks the next chunk).
    """

    def __init__(self, cap: int, held: Callable[..., dict[Any, int]],
                 free_bytes: Callable[[], int] | None = None, factor: float = DISK_FACTOR,
                 total: Callable[[], int | None] | None = None) -> None:
        self.cap, self.held, self.free_bytes, self.factor = int(cap), held, free_bytes, factor
        self.total = total
        self.reserved: dict[ChunkKey, int] = {}   # bytes the chunk will hold once downloaded
        self._adding: dict[ChunkKey, int] = {}    # bytes its download writes
        try:
            import inspect

            self._verify_ok = "verify" in inspect.signature(held).parameters
        except (TypeError, ValueError):
            self._verify_ok = False

    def _held(self, verify: bool = False) -> dict[Any, int]:
        return dict(self.held(verify=verify) if self._verify_ok else self.held())

    def held_bytes(self, exclude: ChunkKey | None = None, *, verify: bool = False,
                   held: dict[Any, int] | None = None) -> int:
        held = self._held(verify) if held is None else held
        return (sum(int(v or 0) for k, v in held.items() if k != exclude and k not in self.reserved)
                + sum(v for k, v in self.reserved.items() if k != exclude))

    def untracked_bytes(self, held: dict[Any, int]) -> int:
        """Working-set bytes on disk beyond the tracked raw files: site lakes, superseded or stray files."""
        if self.total is None:
            return 0
        try:
            tot = self.total()
        except Exception as exc:  # noqa: BLE001 - the tracked bytes still bound the working set
            say(f"working set: cannot measure the local working set ({type(exc).__name__}: {exc})")
            return 0
        if tot is None:
            return 0
        return max(int(tot) - sum(int(v or 0) for v in held.values()), 0)

    def try_reserve(self, key: ChunkKey, need: int, *, verify: bool = False) -> tuple[bool, str]:
        held = self._held(verify)
        own = int(held.get(key, 0) or 0)
        add = max(int(need or 0) - own, 0)
        raw = self.held_bytes(exclude=key, held=held)
        extra = self.untracked_bytes(held)
        if raw > 0 and raw + extra + add > self.cap:
            lake = f" + {_gb(extra)} GB of site lakes and other files" if extra else ""
            return False, (f"working set: {_gb(raw)} GB of raw data on local disk{lake} + {_gb(add)} GB for this "
                           f"chunk > cap {_gb(self.cap)} GB")
        if self.free_bytes is not None and add:
            in_flight = sum(v for k, v in self._adding.items() if k != key)
            free = int(self.free_bytes())
            want = int(self.factor * add)
            if free < want + in_flight:
                return False, (f"disk guard: needs {_gb(want)} GB ({self.factor} x chunk) + {_gb(in_flight)} GB "
                               f"reserved by downloads in flight, {_gb(free)} GB free")
        self.reserved[key] = own + add
        self._adding[key] = add
        return True, ""

    def release(self, key: ChunkKey) -> None:
        self.reserved.pop(key, None)
        self._adding.pop(key, None)


def free_disk() -> int:
    import shutil

    return shutil.disk_usage(env.ROOT).free


def local_workset_total() -> int | None:
    """``fetch.local_workset_bytes()``: raw files on disk (superseded included) + site-lake ``data/**`` bytes,
    every site. None when this site/fetch.py has no such measure."""
    from .site import fetch

    fn = getattr(fetch, "local_workset_bytes", None)
    return None if fn is None else int(fn())


def download_bytes(site: str) -> int:
    """Bytes the site's next download writes (journal; plan.json before the first listing)."""
    from .site import evict

    con = state.connect(state.DB_PATH)
    try:
        n_rows = con.execute("SELECT count(*) FROM site_files WHERE site=?", (site,)).fetchone()[0]
        if n_rows:
            return evict.pending_bytes(con, site)
    finally:
        con.close()
    plan = env.ROOT / "sites" / site / "reports" / "plan.json"
    try:
        return int(json.loads(plan.read_text("utf-8"))["totals"]["bytes"])
    except (OSError, ValueError, KeyError, TypeError):
        return 0


def _note(site: str, stage: str, msg: str) -> None:
    p = log_path(site, stage)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(f"\n=== {env.utcnow().isoformat()} orchestrator: {msg}\n")


def mesa_busy_reason() -> str | None:
    """Why the MESA catalog cannot be opened for writing now (another process holds it), else None."""
    holders = env.catalog_lock_holders()
    if holders:
        who = "; ".join(f"pid {h['pid']} ({h['cmd'][:80].strip()})" for h in holders[:3])
        return f"MESA catalog is open by another process: {who}"
    return None


def _mesa_busy() -> Outcome | None:
    """A ``retry`` outcome while MESA is busy: the stage is dispatched again later, it does not fail."""
    why = mesa_busy_reason()
    return None if why is None else Outcome("retry", None, why)


def needs_mesa(cfg: RunConfig, stage: str, chunk: str | None) -> bool:
    """Does this task open the MESA catalog for writing? Whole-site ``record``; with the direct backend (only
    with the DuckDB 2.0 multi-writer switch in stream mode) chunk ``history`` and ``finalize``."""
    if not cfg.stream:
        return stage == "record"
    return cfg.history == "direct" and ((chunk is not None and stage == "history")
                                        or (chunk is None and stage == "finalize"))


def pending_generic(site: str) -> list[str]:
    """Products of the site whose curation another live VM holds and whose generic result is not on the Data
    Store yet (``validate.pending_generic``); finalize waits for them."""
    from .site import validate

    fn = getattr(validate, "pending_generic", None)
    return [] if fn is None else list(fn(site) or [])


def make_gate(cfg: RunConfig, *, curation: bool = True) -> Callable[[str, str, str | None], str | None]:
    """Pre-dispatch conditions: the reason a task must wait now (it is not dispatched and not failed), else None.

    - a task that writes MESA waits while another process holds the MESA catalog;
    - stream ``finalize`` waits while another VM curates one of the site's products (``curation``), so the
      site's product AVUs are never built without that product's curated terms."""

    def gate(site: str, stage: str, chunk: str | None = None) -> str | None:
        if needs_mesa(cfg, stage, chunk):
            why = mesa_busy_reason()
            if why:
                return why
        if curation and cfg.stream and chunk is None and stage == "finalize":
            try:
                pend = pending_generic(site)
            except Exception as exc:  # noqa: BLE001 - finalize's own curation step still checks
                say(f"{site:5} finalize   cannot check curation claims of other VMs ({type(exc).__name__}: {exc})")
                return None
            if pend:
                names = ", ".join(pend[:4]) + (" ..." if len(pend) > 4 else "")
                return f"another VM is curating {len(pend)} of the site's product(s) ({names})"
        return None

    return gate


def make_prep(cfg: RunConfig) -> Callable[[str, str], Outcome | None]:
    """Per-stage pre-steps and short-circuits run in the worker thread before the subprocess (whole-site)."""

    def prep(site: str, stage: str) -> Outcome | None:
        from .site import evict

        if stage in ("download", "stack"):
            con = state.connect(state.DB_PATH)
            try:
                evict.ensure_schema(con)
                if stage == "download":
                    products = None if cfg.restack else sorted(evict.restore_candidates(con, site))
                    n, b = evict.mark_restore(con, site, products)
                    if n:
                        what = "all products" if products is None else ", ".join(products)
                        _note(site, stage, f"restoring {n} evicted file(s) ({_gb(b)} GB) of {what}")
                    return None
                st = evict.input_state(con, site)
                if cfg.restack and st["missing"]:
                    return Outcome("failed", None, f"--restack needs every input on disk: {st['missing']} downloaded "
                                                   "file(s) are missing (re-run from download)")
                if not cfg.restack:
                    cands = evict.restore_candidates(con, site)
                    if cands:
                        names = ", ".join(list(cands)[:5]) + (" ..." if len(cands) > 5 else "")
                        return Outcome("failed", None, f"{len(cands)} product(s) would be re-stacked from partly "
                                                       f"evicted inputs ({names}): re-run from download (it restores "
                                                       "them) or use --restack")
                if st["local"] == 0 and st["missing"]:
                    return Outcome("ok", 0, f"nothing to stack on disk: {st['missing']} input(s) evicted, lake "
                                            "unchanged")
                return None
            finally:
                con.close()
        if stage == "validate":
            from .site.validate import has_proposals

            if not has_proposals(site):
                return Outcome("ok", 0, "no site proposals: every product reuses curation/generic or is external")
        if stage == "record":
            return _mesa_busy()
        return None

    return prep


def make_stream_prep(cfg: RunConfig) -> Callable[[str, str, str | None], Outcome | None]:
    """Stream pre-checks: with the direct history backend, chunk ``history`` and ``finalize`` need the MESA
    catalog free (the spool backend never opens it); while it is busy the task is retried later."""

    def prep(site: str, stage: str, chunk: str | None) -> Outcome | None:
        if needs_mesa(cfg, stage, chunk):
            return _mesa_busy()
        return None

    return prep


# ---------------------------------------------------------------- scheduler (whole-site)

@dataclass
class SitePlan:
    site: str
    stages: list[str]
    skip: list[str] = field(default_factory=list)  # marked 'skipped' when the site completes
    habitat: str | None = None
    bytes: int = 0


def plan_site(site: str, rows: dict[str, dict[str, Any]], cfg: RunConfig) -> SitePlan:
    """Stages to run: from --from-stage, else from the first selected stage not ok in a covering mode."""
    sel = cfg.selected()
    skip = ["evict"] if ("evict" in cfg.stages and cfg.skips_evict) else []
    if cfg.from_stage:
        return SitePlan(site, sel[sel.index(cfg.from_stage):], skip)
    for i, s in enumerate(sel):
        r = rows.get(s)
        if not (r and r.get("status") == "ok" and covers(r.get("mode"), cfg.mode)):
            return SitePlan(site, sel[i:], skip)
    return SitePlan(site, [], skip)


class _Signals:
    """First SIGINT/SIGTERM: stop scheduling; second: kill the running children (runner.kill)."""

    runner: Any
    out: Callable[[str], None]

    def _init_signals(self) -> None:
        self.stopping = False
        self.signals = 0

    def request_stop(self) -> None:
        if not self.stopping:
            self.stopping = True
            self.out("stop requested: no new stages; running stages finish (signal again to kill them)")

    def on_signal(self, signum: int, frame: Any) -> None:
        self.signals += 1
        if self.signals == 1:
            self.request_stop()
            return
        self.out("second signal: terminating running stages")
        kill = getattr(self.runner, "kill", None)
        if callable(kill):
            kill()


TaskKey = tuple[str, str | None, str]  # (site, chunk_id or None, stage)


class _Waits:
    """Tasks that wait before dispatch instead of failing: a gate condition (MESA busy, another VM curating one
    of the site's products) or a retry delay after a ``retry`` outcome (download rc 3, MESA busy seen by prep).

    ``gate(site, stage, chunk) -> reason | None`` is checked right before a dispatch and again at most every
    ``gate_recheck_s`` while the task waits; after ``wait_timeout_s`` of waiting the task fails with the reason.
    A ``retry`` outcome (RETRY_RC: RC_RETRY from any stream stage, download rc 3, finalize rc 1; MESA busy seen
    by prep) dispatches the task again after a delay that starts at ``retry_delay_s`` and grows by
    ``retry_backoff`` per retry of that task, up to ``retry_max_delay_s``; after ``max_retries`` retries the
    task fails (the next invocation starts over)."""

    out: Callable[[str], None]

    def _init_waits(self, gate: Callable[[str, str, str | None], str | None] | None, *,
                    wait_timeout_s: float | None = None, gate_recheck_s: float = GATE_RECHECK_S,
                    retry_delay_s: float = RETRY_DELAY_S, max_retries: int = MAX_RETRIES,
                    retry_backoff: float = RETRY_BACKOFF, retry_max_delay_s: float = RETRY_MAX_DELAY_S) -> None:
        self.gate = gate
        self.wait_timeout_s = env_seconds(ENV_WAIT_TIMEOUT_S, WAIT_TIMEOUT_S) if wait_timeout_s is None \
            else wait_timeout_s
        self.gate_recheck_s, self.retry_delay_s, self.max_retries = gate_recheck_s, retry_delay_s, max_retries
        self.retry_backoff, self.retry_max_delay_s = max(float(retry_backoff), 1.0), float(retry_max_delay_s)
        self.retry_delays: list[tuple[TaskKey, float]] = []  # (task, delay) of every retry queued, for diagnostics
        self._since: dict[TaskKey, float] = {}
        self._reason: dict[TaskKey, str] = {}
        self._recheck_at: dict[TaskKey, float] = {}
        self._not_before: dict[TaskKey, float] = {}
        self._retries: Counter = Counter()
        self.waiting: set[TaskKey] = set()  # tasks that waited in the current scheduling pass

    @staticmethod
    def _task_label(key: TaskKey) -> str:
        site, chunk, stage = key
        return f"{site:5} {stage:10}" + (f" {chunk}" if chunk else "")

    def _clear_wait(self, key: TaskKey) -> None:
        for d in (self._since, self._reason, self._recheck_at, self._not_before):
            d.pop(key, None)

    def _hold(self, key: TaskKey) -> tuple[str, str]:
        """``go``, ``wait`` or ``fail`` (with the reason) for a task that could be dispatched now."""
        now = time.monotonic()
        nb = self._not_before.get(key)
        if nb is not None and now < nb:
            self.waiting.add(key)
            return "wait", self._reason.get(key, "retry later")
        at = self._recheck_at.get(key)
        if at is not None and now < at:
            self.waiting.add(key)
            return "wait", self._reason.get(key, "")
        site, chunk, stage = key
        why = None
        if self.gate is not None:
            try:
                why = self.gate(site, stage, chunk)
            except Exception as exc:  # noqa: BLE001 - a broken check must not wedge the run
                self.out(f"{self._task_label(key)} wait check failed ({type(exc).__name__}: {exc}); proceeding")
        if not why:
            self._clear_wait(key)
            return "go", ""
        since = self._since.setdefault(key, now)
        if now - since >= self.wait_timeout_s:
            self._clear_wait(key)
            return "fail", (f"{why} (waited {now - since:.0f}s; {ENV_WAIT_TIMEOUT_S}="
                            f"{self.wait_timeout_s:.0f})")
        if self._reason.get(key) != why:
            self.out(f"{self._task_label(key)} waits: {why} (checked again every {self.gate_recheck_s:.0f}s)")
        self._reason[key] = why
        self._recheck_at[key] = now + self.gate_recheck_s
        self.waiting.add(key)
        return "wait", why

    def retry_delay(self, n: int) -> float:
        """Delay before the ``n``-th retry of a task (1-based): exponential backoff, capped."""
        cap = max(self.retry_max_delay_s, self.retry_delay_s)
        return min(self.retry_delay_s * self.retry_backoff ** max(n - 1, 0), cap)

    def _retry(self, key: TaskKey, out: Outcome) -> Outcome | None:
        """Queue a ``retry`` outcome for a later dispatch; a failed Outcome once the retries are used up."""
        self._retries[key] += 1
        n = self._retries[key]
        if n > self.max_retries:
            return Outcome("failed", out.rc, f"{out.info}; gave up after {n - 1} retries")
        delay = self.retry_delay(n)
        self._not_before[key] = time.monotonic() + delay
        self._reason[key] = f"retry later: {out.info}"
        self.retry_delays.append((key, delay))
        self.out(f"{self._task_label(key)} retry {n}/{self.max_retries} in {delay:.0f}s: {out.info}")
        return None


class Scheduler(_Signals, _Waits):
    """Dispatches site stages in priority order under the resource-class limits (one stage per site)."""

    def __init__(self, plans: list[SitePlan], runner: Callable[[str, str], int], store: Store | None = None, *,
                 mode: str = "full", limits: dict[str, int | None] | None = None, max_in_flight: int = 3,
                 disk_guard: DiskGuard | None = None, prep: Callable[[str, str], Outcome | None] | None = None,
                 poll_s: float = 5.0, out: Callable[[str], None] = say,
                 gate: Callable[[str, str, str | None], str | None] | None = None,
                 wait_timeout_s: float | None = None, gate_recheck_s: float = GATE_RECHECK_S,
                 retry_delay_s: float = RETRY_DELAY_S, max_retries: int = MAX_RETRIES,
                 retry_backoff: float = RETRY_BACKOFF, retry_max_delay_s: float = RETRY_MAX_DELAY_S) -> None:
        self.plans, self.runner, self.store, self.mode = plans, runner, store, mode
        self.limits = dict(CLASS_LIMITS if limits is None else limits)
        self.max_in_flight = max_in_flight
        self.disk_guard, self.prep, self.poll_s, self.out = disk_guard, prep, poll_s, out
        self._init_signals()
        self._init_waits(gate, wait_timeout_s=wait_timeout_s, gate_recheck_s=gate_recheck_s,
                         retry_delay_s=retry_delay_s, max_retries=max_retries, retry_backoff=retry_backoff,
                         retry_max_delay_s=retry_max_delay_s)
        self.events: list[tuple[str, str, str]] = []  # (site, stage, start|ok|failed) for tests/diagnostics

    # ------------------------------------------------------------ run
    def _work(self, site: str, stage: str) -> Outcome:
        try:
            if self.prep is not None:
                pre = self.prep(site, stage)
                if pre is not None:
                    return pre
            rc = int(self.runner(site, stage))
        except Exception as exc:  # noqa: BLE001 - a stage error must not stop the scheduler
            return Outcome("failed", None, f"{type(exc).__name__}: {exc}"[:500])
        if rc == 0:
            return Outcome("ok", 0)
        return Outcome("failed", rc, f"killed by signal {-rc}" if rc < 0 else f"exit {rc}")

    def _complete_site(self, p: SitePlan) -> None:
        if not self.store:
            return
        rows = self.store.rows(p.site).get(p.site, {})
        for s in p.skip:
            if (rows.get(s) or {}).get("status") != "ok":
                self.store.skip(p.site, s, "skipped: --sample or --no-evict")

    def run(self) -> dict[str, str]:
        idx = {p.site: 0 for p in self.plans}
        done: dict[str, str] = {}
        for p in self.plans:
            if not p.stages:
                done[p.site] = "nothing to do"
                self._complete_site(p)
        running: dict[Future, tuple[str, str, str, float]] = {}
        busy: set[str] = set()
        in_flight: set[str] = set()
        used: Counter = Counter()
        waiting_disk: dict[str, str] = {}
        ex = ThreadPoolExecutor(max_workers=max(1, self.max_in_flight), thread_name_prefix="run-stage")
        try:
            while True:
                blocked: dict[str, str] = {}
                self.waiting = set()
                if not self.stopping:
                    for p in self.plans:
                        s = p.site
                        if s in done or s in busy:
                            continue
                        if s not in in_flight and len(in_flight) >= self.max_in_flight:
                            continue
                        stage = p.stages[idx[s]]
                        cls = STAGE_CLASS[stage]
                        lim = self.limits.get(cls)
                        if lim is not None and used[cls] >= lim:
                            continue
                        verdict, why = self._hold((s, None, stage))
                        if verdict == "wait":
                            in_flight.add(s)
                            continue
                        if verdict == "fail":
                            self._finish(s, stage, Outcome("failed", None, why), 0.0)
                            done[s] = "failed"
                            in_flight.discard(s)
                            continue
                        if stage == "download" and self.disk_guard is not None:
                            ok, why = self.disk_guard.try_reserve(s)
                            if not ok:
                                blocked[s] = why
                                if waiting_disk.get(s) is None:
                                    self.out(f"{s:5} download waits: {why}")
                                waiting_disk[s] = why
                                continue
                            waiting_disk.pop(s, None)
                        used[cls] += 1
                        busy.add(s)
                        in_flight.add(s)
                        attempt = 1
                        if self.store:
                            attempt = self.store.start(s, stage, _rel(log_path(s, stage)), self.mode)
                        self.events.append((s, stage, "start"))
                        self.out(f"{s:5} {stage:9} start (attempt {attempt}, {cls})")
                        running[ex.submit(self._work, s, stage)] = (s, stage, cls, time.monotonic())
                if not running:
                    if self.stopping or len(done) == len(self.plans):
                        break
                    if self.waiting:  # a task waits for MESA or a retry: poll again
                        time.sleep(self.poll_s)
                        continue
                    if blocked:  # nothing runs, so nothing can free disk: fail the waiting downloads
                        for s, why in blocked.items():
                            self._finish(s, "download", Outcome("failed", None, why + "; nothing left to evict"),
                                         0.0)
                            done[s] = "failed"
                            in_flight.discard(s)
                            if self.disk_guard is not None:
                                self.disk_guard.release(s)
                        continue
                    for p in self.plans:  # safety net; not reachable with consistent limits
                        done.setdefault(p.site, "stopped")
                    break
                finished, _ = wait(list(running), timeout=self.poll_s, return_when=FIRST_COMPLETED)
                for fut in finished:
                    s, stage, cls, t0 = running.pop(fut)
                    used[cls] -= 1
                    busy.discard(s)
                    if stage == "download" and self.disk_guard is not None:
                        self.disk_guard.release(s)
                    out = fut.result()
                    if out.status == "retry":
                        gave_up = None if self.stopping else self._retry((s, None, stage), out)
                        if gave_up is None:
                            self._finish(s, stage, out, time.monotonic() - t0)
                            continue
                        out = gave_up
                    self._finish(s, stage, out, time.monotonic() - t0)
                    p = next(q for q in self.plans if q.site == s)
                    if out.status == "ok":
                        idx[s] += 1
                        if idx[s] >= len(p.stages):
                            done[s] = "ok"
                            in_flight.discard(s)
                            self._complete_site(p)
                    else:
                        done[s] = "failed"
                        in_flight.discard(s)
        finally:
            ex.shutdown(wait=True)
        for p in self.plans:
            done.setdefault(p.site, "stopped")
        return done

    def _finish(self, site: str, stage: str, out: Outcome, seconds: float) -> None:
        if self.store:
            self.store.finish(site, stage, "pending" if out.status == "retry" else out.status, out.rc, out.info)
        self.events.append((site, stage, out.status))
        extra = f": {out.info}" if out.info else ""
        self.out(f"{site:5} {stage:9} {out.status} ({seconds:.0f}s){extra}")


# ---------------------------------------------------------------- streaming: chunk journal

def journal_chunks(site: str, con: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """``stream_chunks`` rows of a site in plan order (the chunk id sorts by product, then first month)."""
    own = con is None
    con = con if con is not None else state.connect(state.DB_PATH)
    try:
        if not _has_table(con, "stream_chunks"):
            return []
        has_done = any(r[1] == "done" for r in con.execute("PRAGMA table_info(stream_chunks)"))
        return [dict(r) for r in con.execute(
            "SELECT chunk_id, product, months, n_files, bytes, status, attempt, error, "
            f"{'done' if has_done else 'NULL AS done'} FROM stream_chunks WHERE site=? ORDER BY chunk_id", (site,))]
    finally:
        if own:
            con.close()


def _norm_status(status: str | None) -> str:
    s = (status or "planned").strip().lower()
    return CHUNK_STATUS_ALIASES.get(s, s)


def chunk_resume_index(status: str | None, stage_rows: dict[str, dict[str, Any]] | None = None,
                       done: str | None = None) -> int:
    """Index into CHUNK_STAGES of a chunk's next stage (len(CHUNK_STAGES) = complete).

    ``stream_chunks.status`` decides when it is a progress status. A ``failed`` (or unknown) chunk resumes
    after ``stream_chunks.done`` (the last status it reached, site/chunks.py) or, without it, at the first
    stage this orchestrator has not seen succeed in the chunk's latest pass. A failed chunk is never complete."""
    s = _norm_status(status)
    if s in CHUNK_STATUS_ORDER:
        return CHUNK_STATUS_ORDER.index(s)
    if done:
        d = _norm_status(done)
        if d in CHUNK_STATUS_ORDER:
            return min(CHUNK_STATUS_ORDER.index(d), len(CHUNK_STAGES) - 1)
    rows = stage_rows or {}
    i = 0
    while i < len(CHUNK_STAGES) and (rows.get(CHUNK_STAGES[i]) or {}).get("status") == "ok":
        i += 1
    return min(i, len(CHUNK_STAGES) - 1)


@dataclass
class ChunkRun:
    chunk_id: str
    bytes: int = 0
    next: int = 0          # index into CHUNK_STAGES of the next stage
    running: bool = False

    @property
    def complete(self) -> bool:
        return self.next >= len(CHUNK_STAGES)

    @property
    def started(self) -> bool:
        return self.next > 0 or self.running

    @property
    def stage(self) -> str | None:
        return None if self.complete else CHUNK_STAGES[self.next]


def load_chunk_runs(site: str, store: Store) -> list[ChunkRun]:
    """The site's chunks from the journal, each positioned at its resume stage."""
    with store.lock:
        rows = journal_chunks(site, store.con)
    stage_rows = store.chunk_rows(site)
    return [ChunkRun(r["chunk_id"], int(r["bytes"] or 0),
                     chunk_resume_index(r["status"], stage_rows.get(r["chunk_id"]), r.get("done")))
            for r in rows]


OTHER_KEY = ("*", "*")  # raw bytes on local disk that belong to no stream chunk


def held_chunk_bytes(verify: bool = False) -> dict[Any, int]:
    """Raw bytes on local disk by owner: (site, chunk_id) for a stream chunk's months, ``OTHER_KEY`` for the
    rest (whole-site downloads, kept sample probes).

    Every site's downloaded, not evicted ``site_files`` rows (the measure of ``site/fetch.py``). ``verify``
    stats each file, so rows whose file is already gone do not count."""
    con = state.connect(state.DB_PATH)
    try:
        has_ev = any(r[1] == "local_evicted" for r in con.execute("PRAGMA table_info(site_files)"))
        where = "dl_status = 'ok'" + (" AND coalesce(local_evicted, 0) = 0" if has_ev else "")
        by_month: dict[tuple[str, str, str], int] = {}
        if verify:
            for r in con.execute(f"SELECT site, product, month, local_path, coalesce(dl_bytes, size, 0) AS b "
                                 f"FROM site_files WHERE {where}"):
                with contextlib.suppress(OSError):
                    if r["local_path"] and (env.ROOT / r["local_path"]).is_file():
                        k = (r["site"], r["product"], r["month"])
                        by_month[k] = by_month.get(k, 0) + int(r["b"])
        else:
            for r in con.execute(f"SELECT site, product, month, coalesce(sum(coalesce(dl_bytes, size, 0)), 0) AS b "
                                 f"FROM site_files WHERE {where} GROUP BY site, product, month"):
                by_month[(r["site"], r["product"], r["month"])] = int(r["b"])
        owner: dict[tuple[str, str, str], str] = {}
        if _has_table(con, "stream_chunks"):
            for r in con.execute("SELECT site, chunk_id, product, months FROM stream_chunks"):
                try:
                    months = [str(m) for m in json.loads(r["months"] or "[]")]
                except (TypeError, ValueError):
                    months = []
                for m in months:
                    owner[(r["site"], r["product"], m)] = r["chunk_id"]
        out: dict[Any, int] = {}
        for (site, product, month), b in by_month.items():
            if not b:
                continue
            cid = owner.get((site, product, month))
            key = (site, cid) if cid else OTHER_KEY
            out[key] = out.get(key, 0) + b
        return out
    finally:
        con.close()


# ---------------------------------------------------------------- streaming: plan

@dataclass
class StreamSitePlan:
    site: str
    stages: list[str]                  # site-level stages to run, in order (subset of STREAM_STAGES)
    habitat: str | None = None
    bytes: int = 0
    chunks: dict[str, int] = field(default_factory=dict)  # journal counts for display: total, done, started


def plan_stream_site(site: str, rows: dict[str, dict[str, Any]], cfg: RunConfig,
                     chunk_runs: list[ChunkRun] | None = None) -> StreamSitePlan:
    """Site-level stages to run in stream mode.

    --from-stage wins. Otherwise: from the first of plan/list/chunk-plan not ok (covering mode), then the
    chunk loop, finalize and cleanup; else the chunk loop when a chunk is not deleted (or the loop's row is
    not ok), then finalize and cleanup; else from the first of finalize/cleanup not ok. ``restore`` (when
    selected) runs first whenever anything else runs: another VM may have advanced the site since, so its
    journal backup and site lake are checked every time."""
    sel = cfg.selected()
    runs = chunk_runs or []
    counts = {"total": len(runs), "done": sum(c.complete for c in runs),
              "started": sum(c.started and not c.complete for c in runs)}

    def mk(stages: list[str]) -> StreamSitePlan:
        if stages and "restore" in sel and stages[0] != "restore":
            stages = ["restore", *(s for s in stages if s != "restore")]
        return StreamSitePlan(site, stages, chunks=counts)

    def ok(s: str) -> bool:
        r = rows.get(s)
        return bool(r and r.get("status") == "ok" and covers(r.get("mode"), cfg.mode))

    if cfg.from_stage:
        return mk(sel[sel.index(cfg.from_stage):])
    incomplete = any(not c.complete for c in runs)
    for i, s in enumerate(sel):
        if s == "restore":
            continue
        todo = (incomplete or not ok(s)) if s == "chunks" else not ok(s)
        if todo:
            return mk(sel[i:])
    return mk([])


def build_stream_plans(selected: list[dict[str, Any]], store: Store, cfg: RunConfig) -> list[StreamSitePlan]:
    all_rows = store.rows()
    plans = []
    for r in selected:
        p = plan_stream_site(r["site"], all_rows.get(r["site"], {}), cfg, load_chunk_runs(r["site"], store))
        p.habitat, p.bytes = r.get("habitat"), int(r.get("bytes") or 0)
        plans.append(p)
    return plans


# ---------------------------------------------------------------- streaming: scheduler

def stream_classes(stage: str, chunk: str | None = None, history_mode: str = "spool") -> tuple[str, ...]:
    """Resource classes a stream task holds while it runs.

    Spooled chunk history never opens MESA (it writes a batch to the Data Store), so it is ``local``; with the
    direct backend it takes ``mesa`` and finalize takes ``finalize`` + ``mesa``."""
    if chunk is not None:
        if stage == "history" and history_mode != "direct":
            return ("local",)
        return (CHUNK_STAGE_CLASS[stage],)
    if stage == "finalize" and history_mode == "direct":
        return ("finalize", "mesa")  # finalize records history under the MESA lock
    return (STREAM_STAGE_CLASS[stage],)


@dataclass
class _SiteRun:
    plan: StreamSitePlan
    pos: int = 0                       # index into plan.stages
    chunks: list[ChunkRun] | None = None
    running: int = 0                   # tasks of this site running now
    failed: bool = False
    loop_open: bool = False            # the 'chunks' row is marked running
    claim_cm: Any = None               # the entered coord.site_claim context manager
    claim: Any = None                  # its Claim (``.lost`` is a threading.Event)
    lost: bool = False                 # the claim was lost mid-run: the site is stopped
    check_done: bool = False           # nothing to do and finished here: its .done marker may be missing

    @property
    def stage(self) -> str | None:
        return self.plan.stages[self.pos] if self.pos < len(self.plan.stages) else None


def _exc_named(exc: BaseException, *names: str) -> bool:
    """Is ``exc`` an instance of a class with one of these names (coord.SiteDone / coord.SiteClaimed)?"""
    return any(k.__name__ in names for k in type(exc).__mro__)


class StreamScheduler(_Signals, _Waits):
    """Site-level stages plus the per-chunk pipeline, under the resource-class limits and the working set.

    ``runner(site, stage, chunk_id_or_None) -> rc``; ``load_chunks(site) -> list[ChunkRun]`` is called when a
    site enters its chunk loop (after chunk-plan) and before finalize. ``events`` records
    (site, chunk_id|None, stage, start|ok|failed|retry).

    Optional hooks:
    - ``claimer(site)`` returns a context manager (``coord.site_claim``) entered before the site's first task
      and exited when the site settles; it raises ``SiteDone`` / ``SiteClaimed`` (by class name) when the site
      is finished or claimed by another VM, and the site is then skipped. A claim whose ``.lost`` event is set
      stops its site: no new tasks, its running children are terminated (``runner.kill_site``).
    - ``after_ok(site, stage, chunk) -> error | None`` runs in the stage's worker thread after the stage
      succeeded (the journal push); an error fails the stage.
    - ``on_site_done(site) -> bool`` runs before the claim of a site that finished ok is released (the
      ``.done`` marker); it returns whether it wrote the marker.
    - ``finished(site) -> bool``: the local journal says this host finished the site (cleanup ok in a full
      run). Such a site with nothing to do is claimed briefly so its missing ``.done`` marker gets written
      (the claimer raises ``SiteDone`` when it exists: nothing to do; ``SiteClaimed``: left alone). Before
      writing it, ``journal_newer(site) -> bool`` (``journal_sync.needs_restore``) is asked: when another VM
      pushed a newer journal backup since, the site runs ``rerun_stages`` (restore first) under the claim
      instead, and gets its marker when they finish.
    - ``gate(site, stage, chunk) -> reason | None``: the task waits while it gives a reason (see _Waits).
    """

    def __init__(self, plans: list[StreamSitePlan], runner: Callable[..., int], store: Store | None = None, *,
                 load_chunks: Callable[[str], list[ChunkRun]], mode: str = "full",
                 limits: dict[str, int | None] | None = None, max_in_flight: int = 3, chunk_depth: int = CHUNK_DEPTH,
                 guard: WorksetGuard | None = None,
                 prep: Callable[[str, str, str | None], Outcome | None] | None = None,
                 history_mode: str = "spool", poll_s: float = 5.0, out: Callable[[str], None] = say,
                 claimer: Callable[[str], Any] | None = None,
                 after_ok: Callable[[str, str, str | None], str | None] | None = None,
                 on_site_done: Callable[[str], bool | None] | None = None,
                 finished: Callable[[str], bool] | None = None,
                 journal_newer: Callable[[str], bool] | None = None,
                 rerun_stages: Iterable[str] = RERUN_STAGES,
                 gate: Callable[[str, str, str | None], str | None] | None = None,
                 wait_timeout_s: float | None = None, gate_recheck_s: float = GATE_RECHECK_S,
                 retry_delay_s: float = RETRY_DELAY_S, max_retries: int = MAX_RETRIES,
                 retry_backoff: float = RETRY_BACKOFF, retry_max_delay_s: float = RETRY_MAX_DELAY_S) -> None:
        self.plans, self.runner, self.store, self.mode = plans, runner, store, mode
        self.load_chunks = load_chunks
        self.limits = dict(CLASS_LIMITS if limits is None else limits)
        self.max_in_flight, self.chunk_depth = max_in_flight, max(1, chunk_depth)
        self.guard, self.prep, self.history_mode = guard, prep, history_mode
        self.poll_s, self.out = poll_s, out
        self.claimer, self.after_ok, self.on_site_done = claimer, after_ok, on_site_done
        self.finished, self.journal_newer = finished, journal_newer
        self.rerun_stages = [s for s in STREAM_STAGES if s in set(rerun_stages)]
        self._init_signals()
        self._init_waits(gate, wait_timeout_s=wait_timeout_s, gate_recheck_s=gate_recheck_s,
                         retry_delay_s=retry_delay_s, max_retries=max_retries, retry_backoff=retry_backoff,
                         retry_max_delay_s=retry_max_delay_s)
        self.events: list[tuple[str, str | None, str, str]] = []
        self.claims: dict[str, Any] = {}   # site -> Claim, while held

    # ------------------------------------------------------------ helpers
    def classes(self, stage: str, chunk: str | None) -> tuple[str, ...]:
        return stream_classes(stage, chunk, self.history_mode)

    def _lost(self, site: str) -> bool:
        claim = self.claims.get(site)
        ev = getattr(claim, "lost", None)
        return bool(ev is not None and ev.is_set())

    def _outcome(self, stage: str, chunk: str | None, rc: int) -> Outcome:
        kind = (stage, chunk is not None)
        if rc == 0:
            return Outcome("ok", 0)
        if WARN_RC.get(kind) == rc:
            return Outcome("ok", rc, f"done with a warning: exit {rc} (some local files were kept; see the log)")
        if rc in RETRY_RC.get(kind, ()):
            return Outcome("retry", rc, retry_reason(stage, chunk, rc))
        return Outcome("failed", rc, f"killed by signal {-rc}" if rc < 0 else f"exit {rc}")

    def _work(self, site: str, stage: str, chunk: str | None) -> Outcome:
        try:
            if self.prep is not None:
                pre = self.prep(site, stage, chunk)
                if pre is not None:
                    return pre
            rc = int(self.runner(site, stage, chunk))
        except Exception as exc:  # noqa: BLE001 - a stage error must not stop the scheduler
            return Outcome("failed", None, f"{type(exc).__name__}: {exc}"[:500])
        out = self._outcome(stage, chunk, rc)
        if out.status == "ok" and self.after_ok is not None:
            if self._lost(site):
                return Outcome("failed", out.rc, "the site's claim was lost to another VM: journal not pushed")
            try:
                err = self.after_ok(site, stage, chunk)
            except Exception as exc:  # noqa: BLE001
                err = f"{type(exc).__name__}: {exc}"
            if err:
                return Outcome("failed", out.rc, str(err)[:500])
        return out

    @staticmethod
    def _label(site: str, stage: str, chunk: str | None) -> str:
        return f"{site:5} {stage:10}" if chunk is None else f"{site:5} {stage:10} {chunk}"

    def _load(self, st: _SiteRun) -> str | None:
        """Load the site's chunks; returns an error message on failure."""
        try:
            st.chunks = list(self.load_chunks(st.plan.site))
        except Exception as exc:  # noqa: BLE001
            return f"cannot read the chunk journal: {type(exc).__name__}: {exc}"[:500]
        return None

    def _open_loop(self, st: _SiteRun) -> bool:
        s = st.plan.site
        err = self._load(st)
        if err is None and st.chunks is not None:
            n = len(st.chunks)
            done = sum(c.complete for c in st.chunks)
            if self.store:
                self.store.start(s, "chunks", _rel(log_path(s, "chunks").with_suffix("")), self.mode)
            st.loop_open = True
            self.events.append((s, None, "chunks", "start"))
            self.out(f"{s:5} chunks     {done}/{n} chunk(s) deleted; {n - done} to process")
            return True
        self._fail_site_stage(st, "chunks", Outcome("failed", None, err or "no chunks"))
        return False

    def _close_loop(self, st: _SiteRun, status: str, info: str) -> None:
        s = st.plan.site
        if self.store:
            self.store.finish(s, "chunks", status, None if status != "ok" else 0, info)
        st.loop_open = False
        self.events.append((s, None, "chunks", status))
        self.out(f"{s:5} chunks     {status}: {info}")

    def _fail_site_stage(self, st: _SiteRun, stage: str, out: Outcome, chunk: str | None = None) -> None:
        self._finish(st.plan.site, stage, chunk, out, 0.0)
        st.failed = True
        if chunk is not None and st.loop_open:
            self._close_loop(st, "failed", f"chunk {chunk} {stage} failed: {out.info}")

    # ------------------------------------------------------------ claims
    def _claim(self, st: _SiteRun, done: dict[str, str]) -> bool:
        """Take the site's Data Store claim before its first task; False when the site is skipped or failed."""
        if self.claimer is None or st.claim_cm is not None or not CLAIM_STAGES & set(st.plan.stages):
            return True  # plan/list/chunk-plan only: local work, no claim needed
        s = st.plan.site
        try:
            cm = self.claimer(s)
            claim = cm.__enter__()
        except Exception as exc:  # noqa: BLE001 - SiteDone / SiteClaimed / Data Store errors
            first = st.stage or "restore"
            if _exc_named(exc, "SiteDone"):
                done[s] = "done elsewhere"
                why = f"skipped: the site is done on the Data Store (.done): {exc}"
            elif _exc_named(exc, "SiteClaimed"):
                done[s] = "claimed elsewhere"
                why = f"skipped: claimed by another VM: {exc}"
            else:
                done[s] = "failed"
                why = f"cannot claim the site on the Data Store: {type(exc).__name__}: {exc}"
            if self.store:
                if done[s] == "failed":
                    self.store.finish(s, first, "failed", None, why[:500])
                else:
                    self.store.skip(s, first, why[:500])
            self.events.append((s, None, first, "claim:" + done[s]))
            self.out(f"{s:5} {why}"[:300])
            return False
        st.claim_cm, st.claim = cm, claim
        self.claims[s] = claim
        self.out(f"{s:5} claimed on the Data Store")
        return True

    def _release(self, st: _SiteRun, result: str) -> bool:
        """Mark a finished site done (.done) and release its claim (exits the claim's context manager).

        Returns whether the ``.done`` marker was written."""
        if st.claim_cm is None:
            return False
        s = st.plan.site
        marked = False
        if result == "ok" and not st.lost and not self._lost(s) and self.on_site_done is not None:
            try:
                marked = self.on_site_done(s) is not False
            except Exception as exc:  # noqa: BLE001 - the site is finished; the next run writes the marker
                self.out(f"{s:5} could not write the .done marker (the next run writes it): "
                         f"{type(exc).__name__}: {exc}")
        cm, st.claim_cm, st.claim = st.claim_cm, None, None
        self.claims.pop(s, None)
        try:
            cm.__exit__(None, None, None)
        except Exception as exc:  # noqa: BLE001 - the claim expires after its TTL
            self.out(f"{s:5} could not release the claim: {type(exc).__name__}: {exc}")
        return marked

    def _wants_done_check(self, site: str) -> bool:
        """A site with nothing to do that this host finished: is its ``.done`` marker to be checked (claims on)?"""
        if self.claimer is None or self.on_site_done is None or self.finished is None:
            return False
        try:
            return bool(self.finished(site))
        except Exception as exc:  # noqa: BLE001 - a broken check must not stop the run
            self.out(f"{site:5} cannot tell whether the site is finished ({type(exc).__name__}: {exc})")
            return False

    def _check_done(self, st: _SiteRun, done: dict[str, str], in_flight: set[str]) -> None:
        """A finished site with nothing to do (cleanup ok here) whose ``.done`` marker may be missing, e.g. its
        write failed, or the site finished under ``--no-claims`` or before claims existed.

        Takes the claim (``SiteDone``: the marker exists; ``SiteClaimed``: another VM works on the site, which is
        left alone). Under the claim: when another VM pushed a newer journal backup since this host's last push
        or restore, this host's view is stale, so the site runs ``rerun_stages`` (restore first) under the claim
        and gets its marker when they finish; otherwise the marker is written now, then the claim is released."""
        s = st.plan.site
        st.check_done = False
        try:
            cm = self.claimer(s)  # type: ignore[misc]
            claim = cm.__enter__()
        except Exception as exc:  # noqa: BLE001 - SiteDone / SiteClaimed / Data Store errors
            if _exc_named(exc, "SiteDone"):
                done[s] = "nothing to do"
            elif _exc_named(exc, "SiteClaimed"):
                done[s] = "claimed elsewhere"
                self.out(f"{s:5} finished here, but another VM holds its claim now; not marking it done: {exc}"[:300])
            else:
                done[s] = "nothing to do"
                self.out(f"{s:5} finished here, but its .done marker could not be checked (the next run tries "
                         f"again): {type(exc).__name__}: {exc}"[:300])
            self.events.append((s, None, "-", "done-check:" + done[s]))
            return
        st.claim_cm, st.claim = cm, claim
        self.claims[s] = claim
        newer: bool | None = False
        if self.journal_newer is not None:
            try:
                newer = bool(self.journal_newer(s))
            except Exception as exc:  # noqa: BLE001 - unknown: never mark a site done over an unseen journal
                newer = None
                self.out(f"{s:5} finished here, but its journal backup on the Data Store could not be checked; not "
                         f"marking it done (the next run tries again): {type(exc).__name__}: {exc}"[:300])
        if newer and (not self.rerun_stages or self.rerun_stages[0] != "restore"):
            self.out(f"{s:5} finished here, but another VM pushed a newer journal backup of the site since; not "
                     f"marking it done (run it with the restore stage selected)")
        elif newer:
            st.plan.stages, st.pos = list(self.rerun_stages), 0
            if self.store:
                self.store.set_pending(s, st.plan.stages)
            in_flight.add(s)
            self.events.append((s, None, "-", "done-check:rerun"))
            self.out(f"{s:5} finished here, but another VM pushed a newer journal backup of the site since: running "
                     f"{', '.join(st.plan.stages)} under its claim before marking it done")
            return
        marked = self._release(st, "ok" if newer is False else "unchecked")
        done[s] = "marked done" if marked else "nothing to do"
        self.events.append((s, None, "-", "done-check:" + done[s]))
        if marked:
            self.out(f"{s:5} finished here without a .done marker on the Data Store: marked done")

    def _check_lost(self, sites: dict[str, _SiteRun], done: dict[str, str], in_flight: set[str]) -> None:
        for s, st in sites.items():
            if st.lost or st.claim_cm is None or not self._lost(s):
                continue
            st.lost = True
            st.failed = True
            self.out(f"{s:5} the site's Data Store claim was lost to another VM: stopping the site "
                     f"({st.running} running stage(s) terminated)")
            self.events.append((s, None, st.stage or "-", "claim lost"))
            kill_site = getattr(self.runner, "kill_site", None)
            if st.running and callable(kill_site):
                kill_site(s)
            if st.loop_open:
                self._close_loop(st, "failed", "the site's claim was lost to another VM")
            self._settle(st, done, in_flight)

    def _settle(self, st: _SiteRun, done: dict[str, str], in_flight: set[str]) -> None:
        s = st.plan.site
        if st.failed:
            if st.running == 0:
                done[s] = "claim lost" if st.lost else "failed"
                in_flight.discard(s)
                self._release(st, done[s])
            return
        while (st.stage == "chunks" and st.chunks is not None and st.loop_open and st.running == 0
               and all(c.complete for c in st.chunks)):
            self._close_loop(st, "ok", f"{len(st.chunks)} chunk(s) deleted")
            st.pos += 1
        if st.stage is None and st.running == 0:
            done[s] = "ok"
            in_flight.discard(s)
            self._release(st, "ok")

    def _candidates(self, st: _SiteRun) -> list[tuple[str, ChunkRun | None]]:
        """Tasks of this site that may start now, ignoring resource classes and the working set."""
        if st.failed or st.stage is None:
            return []
        if st.stage != "chunks":
            return [] if st.running else [(st.stage, None)]
        chunks = st.chunks or []
        active = sum(1 for c in chunks if c.started and not c.complete)
        out: list[tuple[str, ChunkRun | None]] = []
        for j, c in enumerate(chunks):
            if c.complete or c.running:
                continue
            s = c.next
            if any(not e.complete and e.next <= s for e in chunks[:j]):
                continue  # pipeline order: an earlier chunk has not finished this stage yet
            if not c.started and active >= self.chunk_depth:
                break  # no new chunk while chunk_depth chunks are between download and delete
            out.append((CHUNK_STAGES[s], c))
        return out

    # ------------------------------------------------------------ run
    def run(self) -> dict[str, str]:
        sites = {p.site: _SiteRun(p) for p in self.plans}
        done: dict[str, str] = {}
        for s, st in sites.items():
            if not st.plan.stages:
                if self._wants_done_check(s):
                    st.check_done = True  # checked (and its .done written) once it gets a slot
                else:
                    done[s] = "nothing to do"
        running: dict[Future, tuple[str, str, str | None, tuple[str, ...], float]] = {}
        in_flight: set[str] = set()
        used: Counter = Counter()
        waiting: dict[tuple[str, str], str] = {}
        # pipeline order allows at most one chunk per chunk stage of a site at a time
        workers = self.max_in_flight * len(CHUNK_STAGES) + 1
        ex = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="run-stage")
        try:
            while True:
                blocked: dict[tuple[str, str], tuple[_SiteRun, ChunkRun, str]] = {}
                self.waiting = set()
                self._check_lost(sites, done, in_flight)
                if not self.stopping:
                    for s, st in sites.items():
                        if s in done:
                            continue
                        if s not in in_flight and len(in_flight) >= self.max_in_flight:
                            continue
                        if st.check_done:
                            self._check_done(st, done, in_flight)
                            if s in done:
                                continue
                        if s not in in_flight and not st.failed:
                            if not self._claim(st, done):
                                continue
                            in_flight.add(s)  # a claimed site holds its slot until it settles
                        if st.stage == "chunks" and not st.loop_open and not st.failed:
                            in_flight.add(s)
                            if not self._open_loop(st):
                                self._settle(st, done, in_flight)
                                continue
                            self._settle(st, done, in_flight)
                            if s in done:
                                continue
                        for stage, c in self._candidates(st):
                            chunk = c.chunk_id if c is not None else None
                            classes = self.classes(stage, chunk)
                            if any((lim := self.limits.get(k)) is not None and used[k] >= lim for k in classes):
                                continue
                            if stage == "finalize" and chunk is None:
                                if st.chunks is None:
                                    err = self._load(st)
                                    if err:
                                        self._fail_site_stage(st, stage, Outcome("failed", None, err))
                                        break
                                left = [x for x in st.chunks or [] if not x.complete]
                                if left:
                                    self._fail_site_stage(st, stage, Outcome(
                                        "failed", None, f"{len(left)} of {len(st.chunks or [])} chunk(s) are not "
                                                        f"deleted yet (first: {left[0].chunk_id} at "
                                                        f"{left[0].stage}): run the chunk loop first"))
                                    break
                            verdict, why = self._hold((s, chunk, stage))
                            if verdict == "wait":
                                in_flight.add(s)
                                continue
                            if verdict == "fail":
                                self._fail_site_stage(st, stage, Outcome("failed", None, why), chunk)
                                break
                            gkey = None
                            if chunk is not None and stage == "download" and self.guard is not None:
                                gkey = (s, chunk)
                                ok, why = self.guard.try_reserve(gkey, c.bytes)
                                if not ok:
                                    blocked[gkey] = (st, c, why)
                                    if gkey not in waiting:
                                        self.out(f"{s:5} download   {chunk} waits: {why}")
                                    waiting[gkey] = why
                                    continue
                                waiting.pop(gkey, None)
                            self._dispatch(ex, running, used, st, stage, c, classes, gkey)
                            in_flight.add(s)
                        self._settle(st, done, in_flight)
                if not running:
                    if self.stopping or len(done) == len(sites):
                        break
                    if self.waiting:  # a task waits for MESA, another VM's curation or a retry: poll again
                        time.sleep(self.poll_s)
                        continue
                    if blocked:  # nothing runs, so nothing can free the working set: fail the waiting downloads
                        for (s, chunk), (st, c, why) in blocked.items():
                            if st.failed:
                                continue
                            if self.guard is not None:  # re-check against the files really on disk first
                                ok, why = self.guard.try_reserve((s, chunk), c.bytes, verify=True)
                                if ok:
                                    waiting.pop((s, chunk), None)
                                    classes = self.classes("download", chunk)
                                    self._dispatch(ex, running, used, st, "download", c, classes, (s, chunk))
                                    in_flight.add(s)
                                    continue
                            out = Outcome("failed", None, why + "; nothing running can free it (raw files of "
                                                                "earlier runs: `neon-ducklake site evict SITE`)")
                            self._fail_site_stage(st, "download", out, chunk)
                            self._settle(st, done, in_flight)
                        continue
                    for s in sites:  # safety net; not reachable with consistent limits
                        done.setdefault(s, "stopped")
                    break
                finished, _ = wait(list(running), timeout=self.poll_s, return_when=FIRST_COMPLETED)
                for fut in finished:
                    s, stage, chunk, classes, t0 = running.pop(fut)
                    st = sites[s]
                    for k in classes:
                        used[k] -= 1
                    st.running -= 1
                    if chunk is not None and stage == "download" and self.guard is not None:
                        self.guard.release((s, chunk))
                    out = fut.result()
                    c = next(x for x in st.chunks or [] if x.chunk_id == chunk) if chunk is not None else None
                    if c is not None:
                        c.running = False
                    if out.status == "retry":
                        # stopping or a stopped site: leave the stage pending for the next invocation
                        gave_up = None if (st.failed or self.stopping) else self._retry((s, chunk, stage), out)
                        if gave_up is None:
                            self._finish(s, stage, chunk, out, time.monotonic() - t0)
                            self._settle(st, done, in_flight)
                            continue
                        out = gave_up
                    self._finish(s, stage, chunk, out, time.monotonic() - t0)
                    if chunk is None:
                        if out.status == "ok":
                            st.pos += 1
                        else:
                            st.failed = True
                    else:
                        if out.status == "ok":
                            assert c is not None
                            c.next += 1
                        else:
                            st.failed = True
                            if st.loop_open:
                                self._close_loop(st, "failed", f"chunk {chunk} {stage} failed: {out.info}")
                    self._settle(st, done, in_flight)
        finally:
            ex.shutdown(wait=True)
            for s, st in sites.items():
                if s not in done:  # a finished site whose .done check never ran (stopped): nothing was run
                    done[s] = "nothing to do" if st.check_done else "stopped"
                if st.loop_open and self.store:  # stopped mid-loop: the next invocation resumes it
                    n = len(st.chunks or [])
                    k = sum(c.complete for c in st.chunks or [])
                    self.store.note(s, "chunks", "pending", f"stopped: {k}/{n} chunk(s) deleted")
                    st.loop_open = False
                self._release(st, done[s])
        return done

    def _dispatch(self, ex: ThreadPoolExecutor, running: dict, used: Counter, st: _SiteRun, stage: str,
                  c: ChunkRun | None, classes: tuple[str, ...], gkey: ChunkKey | None) -> None:
        s = st.plan.site
        chunk = c.chunk_id if c is not None else None
        for k in classes:
            used[k] += 1
        st.running += 1
        if c is not None:
            c.running = True
        attempt = 1
        if self.store:
            if chunk is not None and stage == CHUNK_STAGES[0] and (s, chunk, stage) not in self._retries:
                self.store.reset_chunk(s, chunk)
            attempt = self.store.start(s, stage, _rel(log_path(s, stage, chunk)), self.mode, chunk=chunk)
        self.events.append((s, chunk, stage, "start"))
        self.out(f"{self._label(s, stage, chunk)} start (attempt {attempt}, {'+'.join(classes)})")
        running[ex.submit(self._work, s, stage, chunk)] = (s, stage, chunk, classes, time.monotonic())

    def _finish(self, site: str, stage: str, chunk: str | None, out: Outcome, seconds: float) -> None:
        status = "pending" if out.status == "retry" else out.status
        if self.store:
            self.store.finish(site, stage, status, out.rc, out.info, chunk=chunk)
        self.events.append((site, chunk, stage, out.status))
        extra = f": {out.info}" if out.info else ""
        self.out(f"{self._label(site, stage, chunk)} {out.status} ({seconds:.0f}s){extra}")


# ---------------------------------------------------------------- site selection

def site_universe(*, con: Any | None = None) -> list[dict[str, Any]]:
    """Every site of the catalog with its habitat and non-AOP basic bytes."""
    from .site.sample import catalog_query

    return catalog_query(
        "SELECT s.siteCode AS site, s.siteProperties['HABITAT'] AS habitat, coalesce(i.bytes, 0) AS bytes "
        "FROM {cat}sites s LEFT JOIN (SELECT siteCode, sum(totalBytes) AS bytes FROM {cat}v_site_inventory "
        "GROUP BY siteCode) i ON i.siteCode = s.siteCode ORDER BY s.siteCode", con=con)


def order_sites(rows: list[dict[str, Any]], order: str) -> list[dict[str, Any]]:
    if order == "given":
        return list(rows)
    if order == "name":
        return sorted(rows, key=lambda r: r["site"])
    if order == "size":
        return sorted(rows, key=lambda r: (int(r.get("bytes") or 0), r["site"]))
    if order == "aquatic-first":
        return sorted(rows, key=lambda r: (not str(r.get("habitat") or "").startswith("Aquatic"),
                                           int(r.get("bytes") or 0), r["site"]))
    raise SystemExit(f"unknown order {order}; one of {', '.join(ORDERS)}")


def select_sites(sites: list[str] | None, *, all_sites: bool = False, habitat: str | None = None,
                 order: str | None = None, universe: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Ordered [{site, habitat, bytes}] for the requested sites / --all / --habitat."""
    codes = list(dict.fromkeys(s.strip().upper() for s in sites or [] if s.strip()))
    if not codes and not all_sites and not habitat:
        raise SystemExit("name one or more SITES, or use --all or --habitat")
    want: tuple[str, ...] | None = None
    if habitat:
        want = HABITATS.get(habitat.strip().lower())
        if want is None:
            raise SystemExit(f"unknown habitat {habitat!r}; one of Terrestrial, aquatic, " + ", ".join(AQUATIC))
    if universe is None:
        try:
            universe = site_universe()
        except (SystemExit, Exception):  # explicit site codes work without the catalog
            if all_sites or habitat:
                raise
            universe = None
    by_code = {r["site"]: r for r in universe or []}
    if codes:
        if by_code:
            unknown = [c for c in codes if c not in by_code]
            if unknown:
                raise SystemExit(f"unknown site(s): {', '.join(unknown)}")
        rows = [by_code.get(c, {"site": c, "habitat": None, "bytes": 0}) for c in codes]
    else:
        rows = list(by_code.values())
    if want is not None:
        rows = [r for r in rows if r.get("habitat") in want]
    return order_sites(rows, order or ("given" if codes else "aquatic-first"))


# ---------------------------------------------------------------- entry points

def build_plans(selected: list[dict[str, Any]], store: Store, cfg: RunConfig) -> list[SitePlan]:
    all_rows = store.rows()
    plans = []
    for r in selected:
        p = plan_site(r["site"], all_rows.get(r["site"], {}), cfg)
        p.habitat, p.bytes = r.get("habitat"), int(r.get("bytes") or 0)
        plans.append(p)
    return plans


def render_plan(plans: list[SitePlan], cfg: RunConfig) -> str:
    lines = [f"run plan: {len(plans)} site(s), mode {cfg.mode}, whole-site (--no-stream), max {cfg.max_in_flight} "
             f"in flight, evict {'off' if cfg.skips_evict else 'on'}" + (", restack" if cfg.restack else ""),
             f"{'#':>3} {'site':5} {'habitat':28} {'GB':>6}  {'resume at':10} stages"]
    for i, p in enumerate(plans, 1):
        st = f"{p.stages[0]}..{p.stages[-1]} ({len(p.stages)})" if p.stages else "nothing to do"
        lines.append(f"{i:>3} {p.site:5} {str(p.habitat or '?')[:28]:28} {_gb(p.bytes):>6}  "
                     f"{(p.stages[0] if p.stages else '-'):10} {st}")
    first = next((p for p in plans if p.stages), None)
    if first:
        lines.append(f"commands for {first.site}:")
        for s in first.stages:
            for argv in stage_commands(first.site, s, sample=cfg.sample, threads=cfg.threads, restack=cfg.restack):
                lines.append(f"  {s:9} [{STAGE_CLASS[s]:10}] neon-ducklake {' '.join(argv[3:])}")
    return "\n".join(lines)


def render_stream_plan(plans: list[StreamSitePlan], cfg: RunConfig) -> str:
    chunk_cap = f"{_gb(cfg.chunk_bytes)} GB" if cfg.chunk_bytes else \
        f"{_gb(env_bytes(ENV_CHUNK_BYTES, CHUNK_BYTES_DEFAULT))} GB (site/chunks.py default unless set)"
    lines = [(f"run plan: {len(plans)} site(s), mode {cfg.mode}, stream (chunks <= {chunk_cap}, working set "
              f"{_gb(cfg.workset)} GB, history {cfg.history}), max {cfg.max_in_flight} site(s) in flight, "
              f"{cfg.chunk_depth} chunk(s) per site, Data Store site claims "
              f"{'on' if cfg.claims else 'off (the caller holds them)'}"),
             f"{'#':>3} {'site':5} {'habitat':28} {'GB':>6}  {'resume at':10} stages"]
    for i, p in enumerate(plans, 1):
        st = f"{p.stages[0]}..{p.stages[-1]} ({len(p.stages)})" if p.stages else "nothing to do"
        resume = next((x for x in p.stages if x != "restore"), p.stages[0]) if p.stages else "-"
        lines.append(f"{i:>3} {p.site:5} {str(p.habitat or '?')[:28]:28} {_gb(p.bytes):>6}  {resume:10} {st}")
        c = p.chunks or {}
        if c.get("total"):
            lines.append(f"      chunks: {c['done']}/{c['total']} deleted, {c['started']} in progress (journal)")
    first = next((p for p in plans if p.stages), None)
    if first:
        lines.append(f"commands for {first.site}:")
        for s in first.stages:
            if s == "chunks":
                lines.append(f"  {'chunks':10} for each chunk <ID> of stream_chunks, in order "
                             f"(chunk k+1 overlaps chunk k):")
                for cs in CHUNK_STAGES:
                    argv = stream_commands(first.site, cs, "<ID>")[0]
                    cls = "+".join(stream_classes(cs, "<ID>", cfg.history))
                    lines.append(f"    {cs:8} [{cls:10}] neon-ducklake {' '.join(argv[3:])}")
                continue
            cls = "+".join(stream_classes(s, None, cfg.history))
            for argv in stream_commands(first.site, s, sample=cfg.sample, threads=cfg.threads):
                lines.append(f"  {s:10} [{cls:10}] neon-ducklake {' '.join(argv[3:])}")
    return "\n".join(lines)


@contextlib.contextmanager
def _env_overlay(values: dict[str, str]) -> Any:
    """Set these environment variables in this process for the block (then restore them), so in-process calls
    (curation claims, the journal push, the working-set measure) see exactly what the stage children see."""
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _install_signals(handler: Callable[[int, Any], None]) -> dict[int, Any]:
    """Install the stop handler (main thread only: a library caller may run us from a worker thread)."""
    if threading.current_thread() is not threading.main_thread():
        return {}
    return {sig: signal.signal(sig, handler) for sig in (signal.SIGINT, signal.SIGTERM)}


def _site_claim(site: str) -> Any:
    """``coord.site_claim(site)``: a context manager yielding the Claim (``.lost``); raises SiteDone/SiteClaimed."""
    from . import coord

    return coord.site_claim(site)


def _push_site_journal(site: str) -> Any:
    from . import journal_sync

    return journal_sync.push_site_journal(site)


JOURNAL_PUSH_TRIES = 4  # a push that fails with a transient iRODS error is retried after 30, 60, 90 s
JOURNAL_PUSH_WAIT_S = 30.0


def make_journal_pusher(stop_event: threading.Event | None = None,
                        push: Callable[[str], Any] | None = None,
                        sleep: Callable[[float], None] = time.sleep) -> Callable[[str, str, str | None], str | None]:
    """``after_ok`` hook: push the site's journal to the Data Store after the stages of JOURNAL_PUSH_AFTER.

    Pushes of one site are serialised. Returns an error message (the stage then fails) when the push fails, or
    when ``stop_event`` is set (the caller lost the site's claim: never overwrite the new owner's backup)."""
    locks: dict[str, threading.Lock] = {}
    guard = threading.Lock()

    def after_ok(site: str, stage: str, chunk: str | None) -> str | None:
        if (stage, chunk is not None) not in JOURNAL_PUSH_AFTER:
            return None
        if stop_event is not None and stop_event.is_set():
            return "stopped by the caller (claim lost?): journal not pushed"
        with guard:
            lock = locks.setdefault(site, threading.Lock())
        for attempt in range(1, JOURNAL_PUSH_TRIES + 1):
            try:
                with lock:
                    res = (push or _push_site_journal)(site)
                break
            except Exception as exc:  # noqa: BLE001
                # a refused or dropped connection is retried here: failing the stage for it fails the site
                if attempt < JOURNAL_PUSH_TRIES and transient_irods(exc) and \
                        not (stop_event is not None and stop_event.is_set()):
                    say(f"{site:5} {stage:10} journal push: {type(exc).__name__}: {exc}; "
                        f"retry {attempt}/{JOURNAL_PUSH_TRIES - 1} in {JOURNAL_PUSH_WAIT_S * attempt:.0f}s"[:400])
                    sleep(JOURNAL_PUSH_WAIT_S * attempt)
                    continue
                return f"journal push failed: {type(exc).__name__}: {exc}"
        if isinstance(res, dict) and res.get("error"):
            return f"journal push failed: {res['error']}"
        return None

    return after_ok


def publish_ontology_export() -> str | None:
    """``bootstrap.publish_ontology()``: this VM's ontology export to ``<IRODS_ROOT>/ref/ontology_maps/vm/<vm>/``
    (each VM writes only its own; bootstrap merges them). Returns an error message, or None."""
    from . import bootstrap

    fn = getattr(bootstrap, "publish_ontology", None)
    if fn is None:
        return None
    out = fn()
    if isinstance(out, dict) and out.get("status") == "error":
        return str(out.get("error") or "error")
    return None


def make_after_ok(stop_event: threading.Event | None = None, *, push: Callable[[str], Any] | None = None,
                  publish: Callable[[], str | None] | None = None) -> Callable[[str, str, str | None], str | None]:
    """The stream ``after_ok`` hook: the journal push (``make_journal_pusher``; its failure fails the stage), then
    after ``finalize`` this VM's ontology export (a failure is only a warning: the next finalize publishes it)."""
    journal = make_journal_pusher(stop_event, push=push)

    def after_ok(site: str, stage: str, chunk: str | None) -> str | None:
        err = journal(site, stage, chunk)
        if err or chunk is not None or stage != "finalize" or (stop_event is not None and stop_event.is_set()):
            return err
        try:
            warn = (publish or publish_ontology_export)()
        except Exception as exc:  # noqa: BLE001
            warn = f"{type(exc).__name__}: {exc}"
        if warn:
            say(f"{site:5} finalize   warning: this VM's ontology export was not published ({warn})"[:400])
        return None

    return after_ok


def make_done_writer(store: Store, cfg: RunConfig) -> Callable[[str], bool]:
    """``on_site_done`` hook: write the site's ``.done`` marker when a full (not sample) run finished it (its
    cleanup is ok in a full run). Returns whether it wrote the marker."""

    def write(site: str) -> bool:
        if cfg.sample:
            return False
        rows = store.rows(site).get(site, {})
        cleanup = rows.get("cleanup") or {}
        if cleanup.get("status") != "ok" or not covers(cleanup.get("mode"), "full"):
            return False
        from . import coord

        with store.lock:
            chunks = {r["status"]: int(r["n"]) for r in store.con.execute(
                "SELECT status, count(*) AS n FROM stream_chunks WHERE site=? GROUP BY status", (site,))} \
                if _has_table(store.con, "stream_chunks") else {}
        coord.write_done(site, {"rc": 0, "by": "run", "chunks": chunks,
                                "cleanup": _json(cleanup.get("info")).get("message")})
        say(f"{site:5} marked done on the Data Store (.done)")
        return True

    return write


def make_finished_check(store: Store, cfg: RunConfig) -> Callable[[str], bool]:
    """``finished`` hook: this host's journal says the site is finished, so its ``.done`` marker is checked.

    Its ``cleanup`` stage is ok in a full run of a stream configuration that selects cleanup; and in the stream
    journal of site/chunks.py every chunk is deleted and the ``cleanup`` step is ok or partial (that row is
    dropped when the lake is restored for writing again, e.g. after a re-list)."""

    def finished(site: str) -> bool:
        if cfg.sample or not cfg.stream or "cleanup" not in cfg.selected():
            return False
        row = store.rows(site).get(site, {}).get("cleanup") or {}
        if row.get("status") != "ok" or not covers(row.get("mode"), "full"):
            return False
        with store.lock:
            con = store.con
            if _has_table(con, "stream_chunks") and con.execute(
                    "SELECT count(*) FROM stream_chunks WHERE site=? AND coalesce(status, '') != 'deleted'",
                    (site,)).fetchone()[0]:
                return False
            if _has_table(con, "stream_sites"):
                step = con.execute("SELECT status FROM stream_sites WHERE site=? AND step='cleanup'",
                                   (site,)).fetchone()
                if step is None or step[0] not in ("ok", "partial"):
                    return False
        return True

    return finished


def _journal_newer(site: str) -> bool:
    """``journal_sync.needs_restore(site)``: the Data Store holds a journal backup of the site this host has not
    seen (another VM pushed it after this host's last push or restore)."""
    from . import journal_sync

    return bool(journal_sync.needs_restore(site))


def _watch_stop(stop_event: threading.Event, sched: _Signals, runner: Any, finished: threading.Event) -> None:
    """Library stop: once ``stop_event`` is set, stop scheduling and terminate the running children."""
    while not finished.is_set():
        if stop_event.wait(0.2):
            if finished.is_set():
                return
            sched.out("stop requested by the caller (the site's claim was lost?): no new stages; terminating "
                      "running stages")
            sched.request_stop()
            kill = getattr(runner, "kill", None)
            if callable(kill):
                kill()
            return


def main(sites: list[str] | None = None, *, all_sites: bool = False, habitat: str | None = None,
         sample: int | None = None, stages: str | Iterable[str] | None = None, from_stage: str | None = None,
         evict: bool = True, max_in_flight: int = 3, dry_run: bool = False, restack: bool = False,
         order: str | None = None, threads: int | None = None, poll_s: float = 5.0, stream: bool = True,
         chunk_bytes: int | None = None, workset_bytes: int | None = None, history_mode: str | None = None,
         chunk_depth: int = CHUNK_DEPTH, claims: bool | None = None,
         stop_event: threading.Event | None = None, claims_held: bool = False) -> int:
    """``neon-ducklake run`` (see the module docstring). ``claims`` (stream only; default on) takes a Data Store
    claim per site; ``claims_held`` says the caller holds the claim instead (``run_sites``: the worker), so the
    stages still run with ``NEON_DUCKLAKE_SITE_CLAIMS=on``; ``stop_event`` (library callers) stops the run like a
    second signal and returns 130."""
    cfg = make_config(stages=stages, from_stage=from_stage, sample=sample, evict=evict, max_in_flight=max_in_flight,
                      restack=restack, threads=threads, poll_s=poll_s, stream=stream, chunk_bytes=chunk_bytes,
                      workset_bytes=workset_bytes, history_mode=history_mode, chunk_depth=chunk_depth)
    cfg.claims = bool(stream and (True if claims is None else claims))
    cfg.claims_held = bool(stream and claims_held and not cfg.claims)
    if stop_event is not None and stop_event.is_set():
        say("stop requested before the run started: nothing run")
        return 130
    selected = select_sites(sites, all_sites=all_sites, habitat=habitat, order=order)
    if not selected:
        print("no sites selected")
        return 1

    def plans_for(store: Store) -> list[Any]:
        return build_stream_plans(selected, store, cfg) if cfg.stream else build_plans(selected, store, cfg)

    def render(plans: list[Any]) -> str:
        return render_stream_plan(plans, cfg) if cfg.stream else render_plan(plans, cfg)

    con = state.connect(state.DB_PATH)
    try:
        store = Store(con)
        if dry_run:
            print(render(plans_for(store)))
            return 0
        with env.file_lock("run"), _env_overlay(cfg.child_env() if cfg.stream else {}):
            for site, stage in store.recover_stale():
                say(f"{site:5} {stage:9} marked failed (interrupted by a previous orchestrator)")
            plans = plans_for(store)
            for p in plans:
                store.set_pending(p.site, p.stages)
            print(render(plans), flush=True)
            runner = SubprocessRunner(cfg, on_spawn=store.note_pid)
            sched: Scheduler | StreamScheduler
            if cfg.stream:
                guard = WorksetGuard(cfg.workset, held_chunk_bytes, free_disk, cfg.disk_factor,
                                     total=local_workset_total)
                sched = StreamScheduler(plans, runner, store, load_chunks=lambda s: load_chunk_runs(s, store),
                                        mode=cfg.mode, max_in_flight=cfg.max_in_flight, chunk_depth=cfg.chunk_depth,
                                        guard=guard, prep=make_stream_prep(cfg), history_mode=cfg.history,
                                        poll_s=cfg.poll_s, gate=make_gate(cfg),
                                        claimer=_site_claim if cfg.claims else None,
                                        after_ok=make_after_ok(stop_event),
                                        on_site_done=make_done_writer(store, cfg) if cfg.claims else None,
                                        finished=make_finished_check(store, cfg) if cfg.claims else None,
                                        journal_newer=_journal_newer if cfg.claims else None,
                                        rerun_stages=[x for x in RERUN_STAGES if x in cfg.selected()])
            else:
                guard_ws = DiskGuard(download_bytes, free_disk, cfg.disk_factor)
                sched = Scheduler(plans, runner, store, mode=cfg.mode, max_in_flight=cfg.max_in_flight,
                                  disk_guard=guard_ws, prep=make_prep(cfg), poll_s=cfg.poll_s, gate=make_gate(cfg))
            old = _install_signals(sched.on_signal)
            finished = threading.Event()
            watcher = None
            if stop_event is not None:
                watcher = threading.Thread(target=_watch_stop, args=(stop_event, sched, runner, finished),
                                           name="run-stop", daemon=True)
                watcher.start()
            try:
                results = sched.run()
            finally:
                finished.set()
                if watcher is not None:
                    watcher.join(2)
                for sig, handler in old.items():
                    signal.signal(sig, handler)
        counts = Counter(results.values())
        say("run done: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
        for s, r in results.items():
            if r not in ("ok", "nothing to do"):
                say(f"{s:5} {r}")
        with contextlib.suppress(Exception):  # the status file is a convenience
            write_status(collect_status(con, [r["site"] for r in selected]))
    finally:
        con.close()
    if sched.stopping or (stop_event is not None and stop_event.is_set()):
        return 130
    return 0 if all(r in NOT_FAILED for r in results.values()) else 1


def run_sites(sites: Iterable[str], *, stream: bool = True, sample: int | None = None,
              stages: str | Iterable[str] | None = None, chunk_bytes: int | None = None,
              workset_bytes: int | None = None, history_mode: str | None = None,
              stop_event: threading.Event | None = None, claims: bool = False) -> int:
    """Library entry point (worker.py): run these sites, in this order, to completion or failure.

    Returns 0 when every site finished (or had nothing to do), 1 on a failure, 130 when stopped: by a signal
    (only handled when called from the main thread) or by ``stop_event`` (set it when the caller's site claim
    is lost: scheduling stops, the running children are terminated and no journal is pushed after that). The
    caller holds the site claims (``claims=False``); the journal is still pushed after every chunk. Holds
    ``work/locks/run.lock`` while running."""
    return main(list(sites), stream=stream, sample=sample, stages=stages, chunk_bytes=chunk_bytes,
                workset_bytes=workset_bytes, history_mode=history_mode, claims=claims, stop_event=stop_event,
                claims_held=not claims)


# ---------------------------------------------------------------- status

def _lock_held(name: str) -> bool:
    path = env.LOCK_DIR / f"{name}.lock"
    if not path.exists():
        return False
    with path.open("a+") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
    return False


def chunk_progress(con: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """site -> {total, done, failed, inProgress, planned, byStatus} from ``stream_chunks``."""
    if not _has_table(con, "stream_chunks"):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for r in con.execute("SELECT site, status, count(*) AS n FROM stream_chunks GROUP BY site, status"):
        d = out.setdefault(r["site"], {"total": 0, "done": 0, "failed": 0, "inProgress": 0, "planned": 0,
                                       "byStatus": {}})
        n, status = int(r["n"]), str(r["status"] or "planned")
        d["total"] += n
        d["byStatus"][status] = n
        if status == "deleted":
            d["done"] += n
        elif status == "failed":
            d["failed"] += n
        elif status == "planned":
            d["planned"] += n
        else:
            d["inProgress"] += n
    return out


def local_raw_bytes(con: sqlite3.Connection) -> dict[str, int]:
    """site -> raw bytes held on local disk (downloaded ok, not evicted/deleted, not superseded)."""
    has_ev = any(r[1] == "local_evicted" for r in con.execute("PRAGMA table_info(site_files)"))
    ev = " AND coalesce(local_evicted, 0) = 0" if has_ev else ""
    return {r[0]: int(r[1]) for r in con.execute(
        "SELECT site, coalesce(sum(coalesce(dl_bytes, size, 0)), 0) FROM site_files WHERE dl_status = 'ok'"
        f"{ev} GROUP BY site")}


def spool_backlog(con: sqlite3.Connection) -> dict[str, Any]:
    """Local batches spooled to the Data Store for the recorder (``batches.status = 'spooled'``), plus
    ``spooling`` ones whose upload was interrupted (the next history call completes them)."""
    by_scope: dict[str, dict[str, int]] = {}
    for r in con.execute("SELECT scope, count(*), coalesce(sum(n_rows), 0) FROM batches WHERE status = 'spooled' "
                         "GROUP BY scope"):
        by_scope[r[0]] = {"batches": int(r[1]), "rows": int(r[2])}
    spooling = con.execute("SELECT count(*) FROM batches WHERE status = 'spooling'").fetchone()[0]
    return {"batches": sum(v["batches"] for v in by_scope.values()),
            "rows": sum(v["rows"] for v in by_scope.values()), "byScope": by_scope, "spooling": int(spooling)}


def collect_status(con: sqlite3.Connection, sites: list[str] | None = None) -> dict[str, Any]:
    """Per-site stage statuses, chunk progress + journal totals (files, bytes, evicted, local bytes held, AVUs
    recorded, snapshots) and the spool backlog."""
    from .site import evict

    store = Store(con)
    runs = store.rows()
    files: dict[str, dict[str, int]] = {}
    for r in con.execute(
            "SELECT site, count(*) AS n, coalesce(sum(coalesce(size, 0)), 0) AS bytes,"
            " coalesce(sum(dl_status = 'ok'), 0) AS dl_ok,"
            " coalesce(sum(CASE WHEN dl_status = 'ok' THEN coalesce(dl_bytes, size, 0) END), 0) AS dl_bytes,"
            " coalesce(sum(up_status IN ('uploaded', 'present')), 0) AS up_ok"
            " FROM site_files WHERE coalesce(dl_status, '') != 'superseded' GROUP BY site"):
        files[r["site"]] = {k: int(r[k]) for k in ("n", "bytes", "dl_ok", "dl_bytes", "up_ok")}
    evicted = evict.evicted_totals(con)
    held = local_raw_bytes(con)
    chunks = chunk_progress(con)
    spool = spool_backlog(con)
    avus = {r[0]: int(r[1]) for r in con.execute(
        "SELECT scope, count(*) FROM avu_journal WHERE status = 'recorded' GROUP BY scope")}
    snaps = {r[0]: int(r[1]) for r in con.execute(
        "SELECT scope, count(*) FROM batches WHERE status = 'recorded' GROUP BY scope")}
    chunk_tasks: dict[str, list[dict[str, Any]]] = {}
    for r in con.execute("SELECT * FROM site_run_chunks WHERE status IN ('running', 'failed') "
                         "ORDER BY site, chunk_id, started_at"):
        chunk_tasks.setdefault(r["site"], []).append(
            {k: r[k] for k in ("chunk_id", "stage", "status", "attempt", "rc", "started_at", "finished_at",
                               "log_path")} | {"info": _json(r["info"]).get("message")})
    names = sorted(set(runs) | set(files) | set(chunks)) if not sites else [s.upper() for s in sites]
    out_sites: dict[str, Any] = {}
    for s in names:
        f = files.get(s, {})
        c = chunks.get(s) or {"total": 0, "done": 0, "failed": 0, "inProgress": 0, "planned": 0, "byStatus": {}}
        tasks = chunk_tasks.get(s, [])
        out_sites[s] = {
            "stages": {st: {k: (runs.get(s, {}).get(st) or {}).get(k) for k in
                            ("status", "attempt", "rc", "started_at", "finished_at", "log_path", "mode")}
                       | {"info": _json((runs.get(s, {}).get(st) or {}).get("info")).get("message")}
                       for st in ALL_STAGES if st in runs.get(s, {})},
            "files": f.get("n", 0), "bytes": f.get("bytes", 0), "downloaded": f.get("dl_ok", 0),
            "downloadedBytes": f.get("dl_bytes", 0), "uploaded": f.get("up_ok", 0),
            "evictedFiles": evicted.get(s, {}).get("files", 0), "evictedBytes": evicted.get(s, {}).get("bytes", 0),
            "localBytes": held.get(s, 0), "chunks": c,
            "runningChunks": [t for t in tasks if t["status"] == "running"],
            "failedChunks": [t for t in tasks if t["status"] == "failed"],
            "spooledBatches": (spool["byScope"].get(s) or {}).get("batches", 0),
            "avusRecorded": avus.get(s, 0), "snapshots": snaps.get(s, 0)}
    totals = {k: sum(v[k] for v in out_sites.values()) for k in
              ("files", "bytes", "downloaded", "downloadedBytes", "uploaded", "evictedFiles", "evictedBytes",
               "localBytes", "avusRecorded", "snapshots")}
    totals["chunks"] = sum(v["chunks"]["total"] for v in out_sites.values())
    totals["chunksDone"] = sum(v["chunks"]["done"] for v in out_sites.values())
    stage_counts = Counter(st["status"] for v in out_sites.values() for st in v["stages"].values())
    workset: int | None = None
    with contextlib.suppress(Exception):  # fetch.local_workset_bytes(): raw files + site lakes on disk
        workset = local_workset_total()
    return {"generatedAt": env.utcnow().isoformat(), "orchestratorRunning": _lock_held("run"),
            "sites": out_sites, "totals": totals, "stageCounts": dict(stage_counts),
            "localBytesAllSites": sum(held.values()), "worksetBytes": workset,
            "worksetCap": env_bytes(ENV_WORKSET_BYTES, WORKSET_BYTES_DEFAULT), "spool": spool,
            "allScopes": {"avusRecorded": sum(avus.values()), "snapshots": sum(snaps.values()),
                          "snapshotBudget": SNAPSHOT_BUDGET}}


def render_status(rep: dict[str, Any]) -> str:
    present = {st for v in rep["sites"].values() for st in v["stages"]}
    cols = [s for s in ALL_STAGES if s in present] or list(STAGES)
    streaming = any(v["chunks"]["total"] or any(s in v["stages"] for s in ("chunk-plan", "chunks"))
                    for v in rep["sites"].values())
    head = f"{'site':5} " + " ".join(f"{SHORT[s]:>4}" for s in cols) + \
        (f" {'chunks':>9} {'heldGB':>6}" if streaming else "") + \
        f" {'files':>8} {'GB':>7} {'evictGB':>7} {'AVUs':>10} {'snaps':>5}"
    title = (f"neon-ducklake status ({rep['generatedAt'][:19]}Z; orchestrator "
             f"{'running' if rep['orchestratorRunning'] else 'not running'})")
    lines = [title, head, "-" * len(head)]
    problems = []
    for s, v in rep["sites"].items():
        cells = []
        for st in cols:
            info = v["stages"].get(st)
            status = (info or {}).get("status")
            if status == "running" and not rep["orchestratorRunning"]:
                status = "failed"  # a dead orchestrator's stage
            cells.append(f"{SYMBOLS.get(status, ' ') if status else ' ':>4}")
            if info and status == "failed":
                problems.append(f"{s} {st} failed (attempt {info.get('attempt')}, rc {info.get('rc')}): "
                                f"{info.get('info') or ''} [{info.get('log_path') or ''}]")
        for t in v.get("failedChunks", []):
            problems.append(f"{s} chunk {t['chunk_id']} {t['stage']} failed (attempt {t.get('attempt')}, rc "
                            f"{t.get('rc')}): {t.get('info') or ''} [{t.get('log_path') or ''}]")
        c = v["chunks"]
        chunk_cells = (f" {str(c['done']) + '/' + str(c['total']) if c['total'] else '-':>9} "
                       f"{_gb(v['localBytes']):>6}") if streaming else ""
        lines.append(f"{s:5} " + " ".join(cells) + chunk_cells + f" {v['files']:>8} {_gb(v['bytes']):>7} "
                     f"{_gb(v['evictedBytes']):>7} {v['avusRecorded']:>10} {v['snapshots']:>5}")
    t = rep["totals"]
    lines.append("-" * len(head))
    tot_chunks = (f" {str(t['chunksDone']) + '/' + str(t['chunks']):>9} {_gb(t['localBytes']):>6}"
                  if streaming else "")
    lines.append(f"{'total':5} " + " " * (5 * len(cols) - 1) + tot_chunks + f" {t['files']:>8} {_gb(t['bytes']):>7} "
                 f"{_gb(t['evictedBytes']):>7} {t['avusRecorded']:>10} {t['snapshots']:>5}")
    a = rep["allScopes"]
    lines.append(f"all scopes: {a['avusRecorded']} AVU rows recorded, {a['snapshots']} MESA snapshots "
                 f"(budget {a['snapshotBudget']})")
    sp = rep.get("spool") or {}
    lines.append(f"spool backlog: {sp.get('batches', 0)} batch(es), {sp.get('rows', 0)} row(s) spooled by this host "
                 f"and not seen recorded" + (f" ({sp['spooling']} interrupted upload(s))" if sp.get("spooling") else "")
                 + f"; raw bytes on local disk: {_gb(rep.get('localBytesAllSites'))} GB")
    if sp.get("batches"):
        lines.append("  (a recorder on another host does not update this journal: the Data Store backlog is "
                     "`neon-ducklake recorder --dry-run`)")
    if rep.get("worksetBytes") is not None:
        lines.append(f"working set on local disk: {_gb(rep['worksetBytes'])} GB (raw files incl. superseded + site "
                     f"lakes) of the {_gb(rep.get('worksetCap'))} GB cap")
    live = rep["orchestratorRunning"]
    for s, v in rep["sites"].items():
        for task in v.get("runningChunks", []):
            head = "running:" if live else "stale (orchestrator not running; the next run retries it):"
            lines.append(f"{head} {s} chunk {task['chunk_id']} {task['stage']} (attempt {task.get('attempt')}, "
                         f"since {(task.get('started_at') or '')[:19]}Z) [{task.get('log_path') or ''}]")
    lines.append("legend: + ok  X failed  > running  . pending  - skipped  (blank) never run"
                 + ("; chunks = deleted/total, heldGB = raw bytes on local disk" if streaming else ""))
    lines += problems
    return "\n".join(lines)


def write_status(rep: dict[str, Any]) -> Path:
    path = env.ROOT / "logs" / "status.json"
    env.write_json(path, rep)
    return path


def status_main(sites: list[str] | None = None, *, as_json: bool = False) -> int:
    con = state.connect(state.DB_PATH)
    try:
        rep = collect_status(con, sites or None)
    finally:
        con.close()
    path = write_status(rep)
    print(json.dumps(rep, indent=1, default=str) if as_json else render_status(rep))
    if not as_json:
        print(f"\nwrote {_rel(path)}")
    return 0
