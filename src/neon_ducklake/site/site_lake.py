"""The per-site DuckLake: ``sites/<SITE>/ducklake/neon_<site>.ducklake`` + ``data/``.

DATA_PATH decision (chdir-free): every attach passes the **absolute** ``sites/<SITE>/ducklake/data/``
with ``OVERRIDE_DATA_PATH true``. DuckLake records the data path given at creation (absolute here) and
stores each Parquet file relative to it, so the directory stays portable: readers (``sql/attach_all.sql``
style, or an iRODS copy) always attach with their own absolute ``DATA_PATH`` + ``OVERRIDE_DATA_PATH true``.
A relative ``'data/'`` would be resolved against the process cwd at write time, which is why the catalog
lake chdirs; the site code never changes directory.

Writes go through :func:`commit` (one DuckLake snapshot, author ``env.ACTOR`` + message). Upserts use
``MERGE INTO`` on logical keys and only touch changed rows, so ``table_changes`` shows real diffs.
"""

from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .. import env

log = logging.getLogger("neon_ducklake.site.site_lake")

ALIAS = "lake"
CATALOG_ALIAS = "neon"
LAKE_OPTIONS: dict[str, Any] = {"parquet_compression": "zstd", "data_inlining_row_limit": 0,
                                "require_commit_message": True}

# Contracted site-lake tables written by site/fetch.py (column -> DuckDB type).
# Download integrity (contract (b), nullable): storageMd5 = md5 hex from the GCS GET ``x-goog-hash``,
# neonMd5 = the md5 NEON lists, transferVerified = local md5 == storageMd5, md5Verified = local md5 == neonMd5.
FILES_COLUMNS: dict[str, str] = {
    "productCode": "VARCHAR", "siteCode": "VARCHAR", "domainCode": "VARCHAR", "month": "VARCHAR",
    "release": "VARCHAR", "isProvisional": "BOOLEAN", "isReleaseOnly": "BOOLEAN", "name": "VARCHAR",
    "size": "BIGINT", "md5": "VARCHAR", "crc32c": "VARCHAR", "kind": "VARCHAR", "tableName": "VARCHAR",
    "hor": "VARCHAR", "ver": "VARCHAR", "tmi": "VARCHAR", "publicationTimestamp": "TIMESTAMP", "ext": "VARCHAR",
    "localRelPath": "VARCHAR", "irodsPath": "VARCHAR", "dlMd5": "VARCHAR", "md5Verified": "BOOLEAN",
    "storageMd5": "VARCHAR", "neonMd5": "VARCHAR", "transferVerified": "BOOLEAN",
}
FILES_KEYS = ("productCode", "month", "name")
SITE_MONTHS_COLUMNS: dict[str, str] = {
    "productCode": "VARCHAR", "month": "VARCHAR", "release": "VARCHAR", "isProvisional": "BOOLEAN",
    "isReleaseOnly": "BOOLEAN", "releaseGenerationDate": "TIMESTAMPTZ", "packageGenerationDate": "TIMESTAMPTZ",
    "fileCount": "BIGINT", "totalBytes": "BIGINT", "isExternal": "BOOLEAN",
}
SITE_MONTHS_KEYS = ("productCode", "month")
EXTERNAL_LINKS_COLUMNS: dict[str, str] = {
    "productCode": "VARCHAR", "url": "VARCHAR", "name": "VARCHAR", "type": "VARCHAR",
    "monthProbed": "VARCHAR", "months": "VARCHAR[]",
}
EXTERNAL_LINKS_KEYS = ("productCode", "url")

# Tables written by site/stack.py that must exist even when empty (contract).
STACK_SCHEMAS: dict[str, dict[str, str]] = {
    "file_stats": {
        "sourceFile": "VARCHAR", "productCode": "VARCHAR", "tableName": "VARCHAR", "lakeTable": "VARCHAR",
        "month": "VARCHAR", "rowCount": "BIGINT", "timeMin": "TIMESTAMPTZ", "timeMax": "TIMESTAMPTZ",
        "timeColumn": "VARCHAR", "qfFailPct": "DOUBLE", "plotIDs": "VARCHAR[]", "plotCount": "BIGINT",
        "protocolVersions": "VARCHAR[]", "taxonCount": "BIGINT", "hor": "VARCHAR", "ver": "VARCHAR", "tmi": "VARCHAR"},
    "file_taxa": {
        "sourceFile": "VARCHAR", "productCode": "VARCHAR", "tableName": "VARCHAR", "taxonID": "VARCHAR",
        "scientificName": "VARCHAR", "taxonRank": "VARCHAR", "vernacularName": "VARCHAR", "nRecords": "BIGINT"},
    "cast_errors": {
        "productCode": "VARCHAR", "tableName": "VARCHAR", "neonTable": "VARCHAR", "columnName": "VARCHAR",
        "targetType": "VARCHAR", "nonNull": "BIGINT", "failures": "BIGINT", "failPct": "DOUBLE", "action": "VARCHAR",
        "samples": "VARCHAR[]", "builtAt": "TIMESTAMPTZ"},
    # one row per header group of a data / lab table, and of sensor_positions (tableName='sensor_positions',
    # unknownColumns = columns outside the stacker's known sensor_positions schema, accepted and unioned)
    "schema_drift": {
        "productCode": "VARCHAR", "tableName": "VARCHAR", "lakeTable": "VARCHAR", "headerSha1": "VARCHAR",
        "nColumns": "BIGINT", "nFiles": "BIGINT", "firstMonth": "VARCHAR", "lastMonth": "VARCHAR",
        "columns": "VARCHAR[]", "missingVsUnion": "VARCHAR[]", "notInVariables": "VARCHAR[]", "builtAt": "TIMESTAMPTZ",
        "unknownColumns": "VARCHAR[]", "firstPublication": "TIMESTAMPTZ", "lastPublication": "TIMESTAMPTZ"},
    # one row per variables file: its record count (``neon.variableCount``, read from the lake, not the CSV)
    "variables_files": {
        "productCode": "VARCHAR", "month": "VARCHAR", "sourceFile": "VARCHAR", "publicationDate": "TIMESTAMPTZ",
        "nVariables": "BIGINT"},
    "table_catalog": {
        "tableName": "VARCHAR", "productCode": "VARCHAR", "neonTable": "VARCHAR", "kind": "VARCHAR",
        "nFiles": "BIGINT", "nRows": "BIGINT", "nColumns": "BIGINT", "firstMonth": "VARCHAR", "lastMonth": "VARCHAR",
        "timeColumn": "VARCHAR", "inputSha256": "VARCHAR", "builtAt": "TIMESTAMPTZ", "nTypedColumns": "BIGINT",
        "nVarcharColumns": "BIGINT", "rowsBeforeDedup": "BIGINT", "dedupRemoved": "BIGINT"},
}


def arrow_type(duck_type: str) -> Any:
    import pyarrow as pa

    if duck_type.endswith("[]"):
        return pa.list_(arrow_type(duck_type[:-2]))
    return {"VARCHAR": pa.string(), "BIGINT": pa.int64(), "INTEGER": pa.int32(), "BOOLEAN": pa.bool_(),
            "DOUBLE": pa.float64(),
            "TIMESTAMP": pa.timestamp("us"), "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"), "DATE": pa.date32()}[duck_type]


def arrow_table(rows: list[dict[str, Any]], columns: dict[str, str]) -> Any:
    """Rows -> pyarrow Table with the exact column order/types of ``columns`` (missing keys -> NULL)."""
    import pyarrow as pa

    schema = pa.schema([(c, arrow_type(t)) for c, t in columns.items()])
    return pa.Table.from_pylist([{c: r.get(c) for c in columns} for r in rows], schema=schema)


@dataclass(frozen=True)
class SitePaths:
    """Every local path of one site subtree (resolved from env at call time, so tests can redirect)."""

    site: str
    root: Path
    site_dir: Path

    @property
    def raw_dir(self) -> Path:
        return self.site_dir / "raw"

    @property
    def lake_dir(self) -> Path:
        return self.site_dir / "ducklake"

    @property
    def data_dir(self) -> Path:
        return self.lake_dir / "data"

    @property
    def catalog_file(self) -> Path:
        return self.lake_dir / f"neon_{self.site.lower()}.ducklake"

    @property
    def wal_file(self) -> Path:
        return self.catalog_file.with_name(self.catalog_file.name + ".wal")

    @property
    def tmp_dir(self) -> Path:
        return self.site_dir / "tmp"

    @property
    def reports_dir(self) -> Path:
        return self.site_dir / "reports"

    def raw_rel(self, product: str, month: str, name: str) -> str:
        """Local path relative to the project root (the journal's ``local_path`` / lake ``localRelPath``)."""
        return f"sites/{self.site}/raw/{product}/{month}/{name}"

    def irods_raw(self, product: str, month: str, name: str) -> str:
        return f"{env.IRODS_ROOT}/sites/{self.site}/raw/{product}/{month}/{name}"


def site_paths(site: str) -> SitePaths:
    site = site.upper()
    return SitePaths(site=site, root=env.ROOT, site_dir=env.SITES_DIR / site)


def sql_str(value: str | Path) -> str:
    """Single-quoted SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def ident(name: str) -> str:
    """Double-quoted SQL identifier (NEON column names such as ``HOR.VER`` need quoting)."""
    return '"' + name.replace('"', '""') + '"'


def connect(site: str, *, memory_limit: str | None = None, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with ducklake/spatial LOADed (never INSTALLed) and the project settings."""
    p = site_paths(site)
    p.tmp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    settings = dict(env.DUCKDB_SETTINGS)
    if memory_limit:
        settings["memory_limit"] = memory_limit
    if threads:
        settings["threads"] = threads
    con.execute(f"SET memory_limit={sql_str(settings['memory_limit'])}")
    con.execute(f"SET threads={int(settings['threads'])}")
    con.execute(f"SET temp_directory={sql_str(p.tmp_dir)}")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET TimeZone='UTC'")
    return con


def attach(con: duckdb.DuckDBPyConnection, site: str, *, read_only: bool = False, alias: str = ALIAS) -> SitePaths:
    """ATTACH the site lake with an absolute DATA_PATH (+ OVERRIDE_DATA_PATH); set lake options when writable."""
    p = site_paths(site)
    if read_only and not p.catalog_file.exists():
        raise FileNotFoundError(p.catalog_file)
    if not read_only:
        p.data_dir.mkdir(parents=True, exist_ok=True)
    opts = [f"DATA_PATH {sql_str(str(p.data_dir) + '/')}", "OVERRIDE_DATA_PATH true"]
    if read_only:
        opts.insert(0, "READ_ONLY")
    con.execute(f"ATTACH {sql_str('ducklake:' + str(p.catalog_file))} AS {alias} ({', '.join(opts)})")
    if not read_only:
        current = dict(con.execute(f"SELECT option_name, value FROM {alias}.options()").fetchall())
        for key, value in LAKE_OPTIONS.items():
            if str(current.get(key, "")).lower() != str(value).lower():
                con.execute(f"CALL {alias}.set_option({sql_str(key)}, ?)", [value])
    return p


def open_site_lake(site: str, *, read_only: bool = False, **kw: Any) -> duckdb.DuckDBPyConnection:
    con = connect(site, **kw)
    attach(con, site, read_only=read_only)
    return con


def catalog_lake_file() -> Path:
    return env.LAKE_DIR / env.CATALOG_LAKE


def attach_catalog(con: duckdb.DuckDBPyConnection, alias: str = CATALOG_ALIAS) -> bool:
    """Attach the Phase-1 catalog lake read-only; False (logged) when it is missing or locked by a writer."""
    path = catalog_lake_file()
    if not path.exists():
        return False
    try:
        con.execute(f"ATTACH {sql_str('ducklake:' + str(path))} AS {alias} "
                    f"(READ_ONLY, DATA_PATH {sql_str(str(env.LAKE_DIR / 'data') + '/')}, OVERRIDE_DATA_PATH true)")
    except duckdb.Error as exc:
        log.warning("catalog lake not attachable (%s); continuing without it", str(exc).splitlines()[0][:160])
        return False
    return True


@contextlib.contextmanager
def commit(con: duckdb.DuckDBPyConnection, message: str, extra: dict[str, Any] | None = None,
           alias: str = ALIAS) -> Iterator[None]:
    """One DuckLake snapshot: BEGIN … commit message (author env.ACTOR) … COMMIT; ROLLBACK on error."""
    con.execute("BEGIN")
    try:
        yield
        con.execute("CALL ducklake_set_commit_message(?, ?, ?, extra_info => ?)",
                    [alias, env.ACTOR, message, json.dumps(extra or {}, default=str)])
        con.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(duckdb.Error):
            con.execute("ROLLBACK")
        raise


def checkpoint_detach(con: duckdb.DuckDBPyConnection, site: str, alias: str = ALIAS) -> None:
    """Checkpoint the lake's DuckDB metadata file, DETACH, and fail if a ``.wal`` remains.

    This is a DuckDB-level checkpoint of ``__ducklake_metadata_<alias>`` (it folds the WAL into the
    ``.ducklake`` file); it does not run DuckLake maintenance, so no snapshot or data file changes.
    """
    p = site_paths(site)
    con.execute(f"CHECKPOINT __ducklake_metadata_{alias}")
    con.execute(f"DETACH {alias}")
    if p.wal_file.exists():
        raise RuntimeError(f"{p.wal_file} still exists after CHECKPOINT/DETACH")


# ---------------------------------------------------------------- schema helpers

def table_exists(con: duckdb.DuckDBPyConnection, table: str, alias: str = ALIAS) -> bool:
    row = con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name='main' "
                      "AND table_name=?", [alias, table]).fetchone()
    return bool(row and row[0])


def table_columns(con: duckdb.DuckDBPyConnection, table: str, alias: str = ALIAS) -> dict[str, str]:
    rows = con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name=? "
                       "AND schema_name='main' AND table_name=? ORDER BY column_index", [alias, table]).fetchall()
    return {r[0]: r[1] for r in rows}


def _canonical_type(con: duckdb.DuckDBPyConnection, t: str) -> str:
    return str(con.execute(f"SELECT typeof(CAST(NULL AS {t}))").fetchone()[0])


def column_types(con: duckdb.DuckDBPyConnection, table: str, alias: str = ALIAS) -> dict[str, str]:
    rows = con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name=? AND table_name=? "
                       "ORDER BY column_index", [alias, table]).fetchall()
    return {c: str(t) for c, t in rows}


def ensure_table(con: duckdb.DuckDBPyConnection, table: str, columns: dict[str, str], alias: str = ALIAS,
                 *, reconcile: bool = True) -> None:
    """CREATE TABLE IF NOT EXISTS, ADD any missing columns (schema grows, never shrinks) and, when
    ``reconcile``, rebuild the table if an existing column's type differs from the declared one.

    Reconciliation matters for module tables created by an older release (e.g. ``schema_drift`` list
    columns inferred as INTEGER[] from empty lists and now declared VARCHAR[]): without it an
    ``INSERT BY NAME`` fails with a conversion error. Only callers passing a *declared* schema reconcile.
    """
    if not table_exists(con, table, alias):
        cols = ", ".join(f"{ident(c)} {t}" for c, t in columns.items())
        con.execute(f"CREATE TABLE {alias}.{ident(table)} ({cols})")
        return
    have = column_types(con, table, alias)
    for c, t in columns.items():
        if c not in have:
            con.execute(f"ALTER TABLE {alias}.{ident(table)} ADD COLUMN {ident(c)} {t}")
    if not reconcile:
        return
    mismatched = {c: t for c, t in columns.items()
                  if c in have and _canonical_type(con, have[c]) != _canonical_type(con, t)}
    if mismatched:
        log.warning("%s.%s: reconciling column types %s", alias, table,
                    {c: f"{have[c]} -> {t}" for c, t in mismatched.items()})
        cur = column_types(con, table, alias)
        sel = ", ".join(f"TRY_CAST({ident(c)} AS {mismatched[c]}) AS {ident(c)}" if c in mismatched else ident(c)
                        for c in cur)
        con.execute(f"CREATE OR REPLACE TABLE {alias}.{ident(table)} AS SELECT {sel} FROM {alias}.{ident(table)}")


def relation_columns(con: duckdb.DuckDBPyConnection, source: str) -> dict[str, str]:
    """Column -> type of a registered view/table (or any SELECT-able name)."""
    rel = con.sql(f"SELECT * FROM {source} LIMIT 0")
    return {c: str(t) for c, t in zip(rel.columns, rel.types, strict=True)}


def upsert(con: duckdb.DuckDBPyConnection, table: str, source: str, keys: Sequence[str],
           columns: dict[str, str] | None = None, alias: str = ALIAS) -> dict[str, int]:
    """Key-based upsert of ``source`` (a registered view or table) into ``alias.table`` via MERGE.

    Only rows whose non-key values differ are updated; new keys are inserted; nothing is deleted.
    Call inside :func:`commit` (the MERGE is part of that snapshot). Returns {'changed', 'source'}.
    """
    src_cols = relation_columns(con, source)
    ensure_table(con, table, columns or src_cols, alias, reconcile=columns is not None)
    tgt_cols = table_columns(con, table, alias)
    for c, t in src_cols.items():
        if c not in tgt_cols:
            con.execute(f"ALTER TABLE {alias}.{ident(table)} ADD COLUMN {ident(c)} {t}")
    cols = list(src_cols)
    non_key = [c for c in cols if c not in keys]
    on = " AND ".join(f"t.{ident(k)} IS NOT DISTINCT FROM s.{ident(k)}" for k in keys)
    differs = " OR ".join(f"t.{ident(c)} IS DISTINCT FROM s.{ident(c)}" for c in non_key) or "false"
    n_src = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    changed = con.execute(
        f"SELECT count(*) FROM {source} s LEFT JOIN (SELECT *, true AS __hit FROM {alias}.{ident(table)}) t "
        f"ON {on} WHERE t.__hit IS NULL OR ({differs})").fetchone()[0]
    if changed:
        sets = ", ".join(f"{ident(c)} = s.{ident(c)}" for c in non_key)
        col_list = ", ".join(ident(c) for c in cols)
        val_list = ", ".join(f"s.{ident(c)}" for c in cols)
        matched = f"WHEN MATCHED AND ({differs}) THEN UPDATE SET {sets} " if non_key else ""
        con.execute(f"MERGE INTO {alias}.{ident(table)} t USING (SELECT * FROM {source}) s ON {on} "
                    f"{matched}WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})")
    return {"changed": int(changed), "source": int(n_src)}


def replace_where(con: duckdb.DuckDBPyConnection, table: str, source: str, where: str,
                  params: Sequence[Any] = (), alias: str = ALIAS) -> int:
    """Replace the slice ``WHERE <where>`` of ``alias.table`` by ``source`` (DELETE + INSERT BY NAME).

    Creates the table from the source's schema when missing and adds new source columns. Call inside
    :func:`commit`. Returns the number of inserted rows.
    """
    src_cols = relation_columns(con, source)
    declared = STACK_SCHEMAS.get(table) or (FILES_COLUMNS if table == "files" else None)
    ensure_table(con, table, declared or src_cols, alias, reconcile=declared is not None)
    have = table_columns(con, table, alias)
    for c, t in src_cols.items():
        if c not in have:
            con.execute(f"ALTER TABLE {alias}.{ident(table)} ADD COLUMN {ident(c)} {t}")
    con.execute(f"DELETE FROM {alias}.{ident(table)} WHERE {where}", list(params))
    con.execute(f"INSERT INTO {alias}.{ident(table)} BY NAME SELECT * FROM {source}")
    return int(con.execute(f"SELECT count(*) FROM {source}").fetchone()[0])


def snapshot_count(con: duckdb.DuckDBPyConnection, alias: str = ALIAS) -> int:
    return int(con.execute(f"SELECT count(*) FROM {alias}.snapshots()").fetchone()[0])


# ---------------------------------------------------------------- catalog lineage (Data Store copy vs local)

Snapshot = tuple[int, str, str]  # (snapshot_id, snapshot_time, commit_message)


def catalog_snapshots(catalog: Path, site: str) -> list[Snapshot]:
    """Every snapshot of a ``.ducklake`` catalog file (attached READ_ONLY; its data files are never read)."""
    con = duckdb.connect()
    try:
        con.execute("LOAD ducklake")
        data = site_paths(site).data_dir
        con.execute(f"ATTACH {sql_str('ducklake:' + str(catalog))} AS cat "
                    f"(READ_ONLY, DATA_PATH {sql_str(str(data) + '/')}, OVERRIDE_DATA_PATH true)")
        rows = con.execute("SELECT snapshot_id, CAST(snapshot_time AS VARCHAR), coalesce(commit_message, '') "
                           "FROM cat.snapshots() ORDER BY snapshot_id").fetchall()
    finally:
        con.close()
    return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]


def _live_row(rows: list[tuple[Any, ...]], at: int | None) -> tuple[Any, ...] | None:
    """The version of a schema or table row (``begin_snapshot, end_snapshot, ...``) live at snapshot ``at``; the
    newest one when no version covers it."""
    if not rows:
        return None
    if at is not None:
        for r in rows:
            if (r[0] is None or r[0] <= at) and (r[1] is None or r[1] > at):
                return r
    return max(rows, key=lambda r: r[0] or 0)


def _join_path(prefix: str, part: Any, relative: Any) -> str:
    part = str(part or "")
    return prefix + part if relative is None or bool(relative) else part


def catalog_data_files(catalog: Path) -> dict[str, int | None]:
    """Every data file a ``.ducklake`` catalog file references (``ducklake_data_file`` and ``ducklake_delete_file``,
    ended ones included: older snapshots still read them), as ``path relative to the lake's data/`` -> the size the
    catalog records (``file_size_bytes``; None when unknown).

    The catalog is opened as a plain DuckDB database, read-only (checkpoint the lake first: no ``.wal``). Paths
    resolve the DuckLake way: data path + schema path + table path + file path, each part relative to the one
    before unless absolute. An absolute path outside the catalog's data path cannot be mapped to ``data/`` and is
    left out (logged)."""
    con = duckdb.connect(str(catalog), read_only=True)
    try:
        def cols(table: str) -> set[str]:
            return {str(r[0]) for r in con.execute(f"DESCRIBE {table}").fetchall()}

        scoped = "scope" in cols("ducklake_metadata")
        row = con.execute("SELECT value FROM ducklake_metadata WHERE key = 'data_path'" +
                          (" ORDER BY scope NULLS FIRST" if scoped else "") + " LIMIT 1").fetchone()
        base = str(row[0] or "") if row else ""
        if base and not base.endswith("/"):
            base += "/"
        versions: dict[str, dict[int, list[tuple[Any, ...]]]] = {}
        for table, key, extra, name in (("ducklake_schema", "schema_id", "", "schema_name"),
                                        ("ducklake_table", "table_id", "schema_id, ", "table_name")):
            path = "path, path_is_relative" if "path" in cols(table) else f"{name} || '/', true"
            got = versions.setdefault(table, {})
            for rid, *rest in con.execute(f"SELECT {key}, begin_snapshot, end_snapshot, {extra}{path} "
                                          f"FROM {table}").fetchall():
                got.setdefault(int(rid), []).append(tuple(rest))
        files = con.execute(
            "SELECT table_id, begin_snapshot, path, path_is_relative, file_size_bytes FROM ducklake_data_file "
            "UNION ALL SELECT table_id, begin_snapshot, path, path_is_relative, file_size_bytes "
            "FROM ducklake_delete_file").fetchall()
    finally:
        con.close()
    out: dict[str, int | None] = {}
    for tid, begin, path, relative, size in files:
        prefix = base
        t = _live_row(versions["ducklake_table"].get(int(tid), []), begin)
        if t is not None:  # (begin, end, schema_id, path, path_is_relative)
            s = _live_row(versions["ducklake_schema"].get(int(t[2]), []), begin)
            if s is not None:  # (begin, end, path, path_is_relative)
                prefix = _join_path(prefix, s[2], s[3])
            prefix = _join_path(prefix, t[3], t[4])
        full = _join_path(prefix, path, relative)
        if base and full.startswith(base):
            rel = full[len(base):]
        elif not base and not full.startswith("/"):
            rel = full
        else:
            log.warning("catalog %s: data file %s lies outside its data path %s; left out", catalog.name, full, base)
            continue
        out[rel.lstrip("/")] = int(size) if size is not None else None
    return out


def snapshot_key(snap: Snapshot) -> str:
    """``id|time|message``: identifies one snapshot of one lake history (``lake_pushes.snapshot_key``)."""
    return f"{snap[0]}|{snap[1]}|{snap[2]}"


def lineage(local: list[Snapshot], remote: list[Snapshot]) -> str:
    """How two snapshot histories of one lake relate: ``same``, ``local_ahead`` (the remote history is a prefix of
    the local one), ``remote_ahead`` (the reverse) or ``diverged`` (each has snapshots the other lacks)."""
    if local == remote:
        return "same"
    n = min(len(local), len(remote))
    if local[:n] != remote[:n]:
        return "diverged"
    return "local_ahead" if len(local) > len(remote) else "remote_ahead"


def last_snapshot_id(con: duckdb.DuckDBPyConnection, alias: str = ALIAS) -> int | None:
    row = con.execute(f"SELECT max(snapshot_id) FROM {alias}.snapshots()").fetchone()
    return None if not row or row[0] is None else int(row[0])


def stream_chunk_ids(con: duckdb.DuckDBPyConnection, site: str, alias: str = ALIAS) -> dict[str, int]:
    """chunk id -> last snapshot id of the lake's ``stream stack <SITE> <chunk> <unit>`` commits."""
    prefix = f"stream stack {site.upper()} "
    out: dict[str, int] = {}
    for sid, msg in con.execute(f"SELECT snapshot_id, commit_message FROM {alias}.snapshots() "
                                "WHERE starts_with(commit_message, ?)", [prefix]).fetchall():
        parts = str(msg)[len(prefix):].split(" ", 1)
        if parts and parts[0]:
            out[parts[0]] = max(out.get(parts[0], 0), int(sid))
    return out


def data_bytes(site: str | None = None) -> int:
    """Bytes of the local site-lake ``data/**`` files (one site, or every site under ``sites/``)."""
    import os

    roots = [site_paths(site).data_dir] if site else sorted(env.SITES_DIR.glob("*/ducklake/data"))
    total = 0
    for root in roots:
        if not root.is_dir():
            continue
        for dirpath, _dirs, names in os.walk(root):
            for n in names:
                with contextlib.suppress(OSError):
                    total += os.lstat(os.path.join(dirpath, n)).st_size
    return total
