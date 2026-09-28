"""work/state.sqlite — the resumable journal shared by every stage (SQLite WAL: many processes).

Signed URLs live only here (file mode 0600, never mirrored) and are nulled after download.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .env import WORK_DIR, utcnow

DB_PATH = WORK_DIR / "state.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT NOT NULL, stage TEXT NOT NULL, site TEXT, argv TEXT, started_at TEXT, finished_at TEXT,
  status TEXT, info TEXT, PRIMARY KEY (run_id, stage, site)
);
CREATE TABLE IF NOT EXISTS requests (
  run_id TEXT NOT NULL, family TEXT NOT NULL, key TEXT NOT NULL,
  method TEXT, path TEXT, params TEXT, http_status INTEGER, attempts INTEGER, elapsed_ms REAL,
  response_bytes INTEGER, landed_path TEXT, landed_bytes INTEGER, sha256 TEXT,
  status TEXT, error TEXT, fetched_at TEXT, PRIMARY KEY (run_id, family, key)
);
CREATE TABLE IF NOT EXISTS site_files (
  site TEXT NOT NULL, product TEXT NOT NULL, month TEXT NOT NULL, name TEXT NOT NULL,
  release TEXT, release_only INTEGER DEFAULT 0, kind TEXT, size INTEGER, md5 TEXT, crc32c TEXT,
  url TEXT, listed_at TEXT, local_path TEXT,
  storage_md5 TEXT, neon_md5 TEXT, transfer_verified INTEGER,
  dl_status TEXT, dl_attempts INTEGER DEFAULT 0, dl_error TEXT, dl_md5 TEXT, dl_bytes INTEGER, md5_verified INTEGER,
  downloaded_at TEXT,
  irods_path TEXT, up_status TEXT, up_attempts INTEGER DEFAULT 0, up_error TEXT, irods_size INTEGER,
  irods_checksum TEXT, uploaded_at TEXT,
  PRIMARY KEY (site, product, month, name)
);
CREATE INDEX IF NOT EXISTS site_files_dl ON site_files(site, dl_status);
CREATE INDEX IF NOT EXISTS site_files_up ON site_files(site, up_status);
CREATE TABLE IF NOT EXISTS listings (
  site TEXT NOT NULL, product TEXT NOT NULL, part TEXT NOT NULL, release TEXT, start_month TEXT, end_month TEXT,
  listed_at TEXT, status TEXT, n_files INTEGER, n_bytes INTEGER, error TEXT, PRIMARY KEY (site, product, part)
);
CREATE TABLE IF NOT EXISTS avu_journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scope TEXT NOT NULL, unit_id TEXT NOT NULL, path TEXT NOT NULL, target_type TEXT NOT NULL,
  attribute TEXT NOT NULL, value TEXT NOT NULL, unit TEXT NOT NULL DEFAULT '', op TEXT NOT NULL,
  generator TEXT, status TEXT NOT NULL, applied_at TEXT, batch_id TEXT, snapshot_id INTEGER, error TEXT
);
CREATE INDEX IF NOT EXISTS avu_journal_status ON avu_journal(scope, status);
CREATE INDEX IF NOT EXISTS avu_journal_path ON avu_journal(path);
CREATE INDEX IF NOT EXISTS avu_journal_batch ON avu_journal(batch_id);
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY, scope TEXT NOT NULL, seq INTEGER, total INTEGER, note TEXT, n_rows INTEGER,
  status TEXT NOT NULL, snapshot_id INTEGER, created_at TEXT, recorded_at TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS uploads (
  local_path TEXT PRIMARY KEY, irods_path TEXT NOT NULL, size INTEGER, md5 TEXT, status TEXT,
  irods_checksum TEXT, uploaded_at TEXT, error TEXT, attempts INTEGER DEFAULT 0
);
"""


def connect(path: Path | None = None) -> sqlite3.Connection:
    path = path if path is not None else DB_PATH  # resolved at call time so tests can patch DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    con = sqlite3.connect(path, timeout=60, isolation_level=None, check_same_thread=False)
    if new:
        os.chmod(path, 0o600)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=60000")
    con.executescript(SCHEMA)
    return con


def start_run(con: sqlite3.Connection, run_id: str, stage: str, site: str | None, argv: list[str]) -> None:
    con.execute(
        "INSERT OR REPLACE INTO runs(run_id, stage, site, argv, started_at, status) VALUES (?,?,?,?,?,?)",
        (run_id, stage, site or "", json.dumps(argv), utcnow().isoformat(), "running"),
    )


def finish_run(con: sqlite3.Connection, run_id: str, stage: str, site: str | None, status: str,
               info: dict[str, Any] | None = None) -> None:
    con.execute(
        "UPDATE runs SET finished_at=?, status=?, info=? WHERE run_id=? AND stage=? AND site=?",
        (utcnow().isoformat(), status, json.dumps(info or {}, default=str), run_id, stage, site or ""),
    )
