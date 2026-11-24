"""Phase 1 — build or refresh the catalog lake ``ducklake/neon_catalog.ducklake`` from a sealed run.

One ``BEGIN … COMMIT`` per build, i.e. exactly one DuckLake snapshot (``catalog_builds`` gets one row per
build so even a no-change refresh commits). Every table is a hash-keyed upsert: ``rowHash`` = md5 of the
row's JSON (meta columns excluded); lake rows whose hash is not staged are deleted, staged rows whose hash
is not in the lake are inserted, so refreshes produce real ``ducklake_table_changes`` diffs. Key uniqueness,
NOT NULL keys and zero ``X-Goog`` (signed URLs) are asserted inside the transaction; a failure rolls back.
The 16 published ``neon_*`` spatial layers are copied once from the neon-downloads lake pinned
``AT (VERSION => 90)``. After COMMIT the metadata database is checkpointed and detached; a leftover
``.wal`` is an error.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa

from . import duck, env, flatten
from .flatten import TABLES, TableSpec

log = logging.getLogger("neon_ducklake.catalog_lake")

CODE_ROOT = Path(__file__).resolve().parents[2]
VIEWS_SQL = CODE_ROOT / "sql" / "catalog_views.sql"
ALIAS = "neon"
META_COLS = (("rowHash", "VARCHAR"), ("loadedRun", "VARCHAR"))
LAKE_OPTIONS: dict[str, Any] = {"parquet_compression": "zstd", "data_inlining_row_limit": 0,
                                "require_commit_message": True}
SPATIAL_PREFIX = "neon_"
# tables staged/upserted before the bookkeeping tables (catalog_builds, table_catalog) that summarise them
DATA_TABLES = [n for n in TABLES if n not in ("catalog_builds", "table_catalog")]
TYPE_ALIASES = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------- connection


def connect(temp_dir: Path | None = None) -> Any:
    """In-memory DuckDB with ducklake + spatial LOADed (never INSTALLed) and the project settings."""
    import duckdb

    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute("LOAD spatial")
    con.execute(f"SET memory_limit='{env.DUCKDB_SETTINGS['memory_limit']}'")
    con.execute(f"SET threads={int(env.DUCKDB_SETTINGS['threads'])}")
    con.execute("SET TimeZone='UTC'")  # to_json(TIMESTAMPTZ) -> row hashes must not depend on the host zone
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir}'")
    return con


@contextlib.contextmanager
def chdir(path: Path) -> Iterator[None]:
    old = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


def attach_catalog(con: Any, alias: str = ALIAS) -> bool:
    """ATTACH the catalog lake from the current directory (relative DATA_PATH is recorded). True if new."""
    created = not Path(env.CATALOG_LAKE).exists()
    con.execute(f"ATTACH 'ducklake:{env.CATALOG_LAKE}' AS {alias} (DATA_PATH 'data/')")
    if created:
        for opt, val in LAKE_OPTIONS.items():
            con.execute(f"CALL ducklake_set_option('{alias}', '{opt}', ?)", [val])
    return created


def attach_all(con: Any, root: Path | None = None) -> list[str]:
    """Attach the catalog lake and every site lake READ_ONLY with absolute paths (see sql/attach_all.sql)."""
    root = root or env.ROOT
    out = []
    cat = root / "ducklake" / env.CATALOG_LAKE
    if cat.exists():
        con.execute(f"ATTACH 'ducklake:{cat}' AS neon (READ_ONLY, DATA_PATH '{cat.parent / 'data'}/', "
                    "OVERRIDE_DATA_PATH true)")
        out.append("neon")
    for lake in sorted((root / "sites").glob("*/ducklake/neon_*.ducklake")):
        alias = lake.stem.lower()
        con.execute(f"ATTACH 'ducklake:{lake}' AS {alias} (READ_ONLY, DATA_PATH '{lake.parent / 'data'}/', "
                    "OVERRIDE_DATA_PATH true)")
        out.append(alias)
    return out


# ---------------------------------------------------------------- staging + upsert


def lake_columns(spec: TableSpec) -> list[tuple[str, str]]:
    return spec.sql_columns() + list(META_COLS) + [(n, t) for n, t, _e in spec.derived]


def stage(con: Any, spec: TableSpec, source_sql: str, run_id: str) -> str:
    """Typed temp table ``stage_<name>`` with rowHash (md5 of the row JSON), loadedRun and derived columns."""
    raw, out = f"stage_raw_{spec.name}", f"stage_{spec.name}"
    casts = ", ".join(f"CAST({q(c)} AS {t}) AS {q(c)}" for c, t in spec.sql_columns())
    con.execute(f"CREATE OR REPLACE TEMP TABLE {raw} AS SELECT {casts} FROM ({source_sql})")
    derived = "".join(f", {expr} AS {q(n)}" for n, _t, expr in spec.derived)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {out} AS SELECT r.*, md5(to_json(r)::VARCHAR) AS rowHash, "
                f"?::VARCHAR AS loadedRun{derived} FROM {raw} r", [run_id])
    con.execute(f"DROP TABLE {raw}")
    return out


def stage_arrow(con: Any, spec: TableSpec, tbl: pa.Table, run_id: str) -> str:
    view = f"arrow_{spec.name}"
    duck.register(con, view, tbl)
    try:
        return stage(con, spec, f"SELECT * FROM {view}", run_id)
    finally:
        duck.unregister(con, view)


def stage_parquet(con: Any, spec: TableSpec, paths: list[Path], run_id: str) -> str:
    if not paths:
        return stage_arrow(con, spec, flatten.empty_table(spec.name), run_id)
    files = "[" + ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths) + "]"
    return stage(con, spec, f"SELECT * FROM read_parquet({files})", run_id)


def existing_columns(con: Any, alias: str, table: str) -> dict[str, str]:
    rows = con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name=? "
                       "AND schema_name='main' AND table_name=? ORDER BY column_index", [alias, table]).fetchall()
    return {c: t for c, t in rows}


def _norm(t: str) -> str:
    return TYPE_ALIASES.get(t.upper(), t.upper())


def ensure_table(con: Any, alias: str, spec: TableSpec) -> str:
    """Create the lake table, or evolve it to the spec. Returns created/ok/altered/recreated."""
    want = lake_columns(spec)
    have = existing_columns(con, alias, spec.name)
    ddl = ", ".join(f"{q(c)} {t}" for c, t in want)
    if not have:
        con.execute(f"CREATE TABLE {alias}.{q(spec.name)} ({ddl})")
        return "created"
    if [(c, _norm(t)) for c, t in want] == [(c, _norm(t)) for c, t in have.items()]:
        return "ok"
    if any(c in have and _norm(have[c]) != _norm(t) for c, t in want):
        con.execute(f"DROP TABLE {alias}.{q(spec.name)}")
        con.execute(f"CREATE TABLE {alias}.{q(spec.name)} ({ddl})")
        return "recreated"
    for c, t in want:
        if c not in have:
            con.execute(f"ALTER TABLE {alias}.{q(spec.name)} ADD COLUMN {q(c)} {t}")
    for c in have:
        if c not in dict(want):
            con.execute(f"ALTER TABLE {alias}.{q(spec.name)} DROP COLUMN {q(c)}")
    return "altered"


def upsert(con: Any, alias: str, spec: TableSpec, stage_table: str) -> tuple[int, int]:
    """Hash-keyed upsert of a staged table; returns (inserted, deleted)."""
    t = f"{alias}.{q(spec.name)}"
    scope = ""
    if spec.scope_col:
        scope = f" AND {q(spec.scope_col)} IN (SELECT DISTINCT {q(spec.scope_col)} FROM {stage_table})"
    deleted = con.execute(f"DELETE FROM {t} WHERE rowHash NOT IN (SELECT rowHash FROM {stage_table}){scope}"
                          ).fetchone()[0]
    cols = ", ".join(q(c) for c, _t in lake_columns(spec))
    inserted = con.execute(f"INSERT INTO {t} ({cols}) SELECT {cols} FROM {stage_table} "
                           f"WHERE rowHash NOT IN (SELECT rowHash FROM {t})").fetchone()[0]
    return int(inserted or 0), int(deleted or 0)


def check_table(con: Any, alias: str, spec: TableSpec) -> list[str]:
    """Key uniqueness, NOT NULL keys and zero signed URLs in any text column."""
    t = f"{alias}.{q(spec.name)}"
    keys = ", ".join(q(k) for k in spec.key)
    problems = []
    dup = con.execute(f"SELECT count(*) FROM (SELECT {keys} FROM {t} GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    if dup:
        problems.append(f"{spec.name}: {dup} duplicate keys ({keys})")
    nulls = con.execute(f"SELECT count(*) FROM {t} WHERE " + " OR ".join(f"{q(k)} IS NULL" for k in spec.key)
                        ).fetchone()[0]
    if nulls:
        problems.append(f"{spec.name}: {nulls} rows with a NULL key column")
    text = [c for c, typ in spec.columns if typ in ("str", "json", "list", "map")]
    if text:
        cond = " OR ".join(f"contains(CAST({q(c)} AS VARCHAR), '{flatten.SIGNED_MARK}')" for c in text)
        signed = con.execute(f"SELECT count(*) FROM {t} WHERE {cond}").fetchone()[0]
        if signed:
            problems.append(f"{spec.name}: {signed} rows contain a signed URL ({flatten.SIGNED_MARK})")
    return problems


# ---------------------------------------------------------------- views + spatial


@dataclass
class ViewDef:
    name: str
    description: str
    sql: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def load_views(path: Path = VIEWS_SQL) -> list[ViewDef]:
    """``-- view: <name>: <description>`` blocks of sql/catalog_views.sql, in file order."""
    text = path.read_text()
    parts = re.split(r"^-- view: ", text, flags=re.MULTILINE)[1:]
    out = []
    for part in parts:
        head, _, body = part.partition("\n")
        name, _, desc = head.partition(":")
        out.append(ViewDef(name.strip(), desc.strip(), body.strip().rstrip(";").strip()))
    return out


def create_view(con: Any, alias: str, view: ViewDef) -> None:
    sql = re.sub(r"^\s*CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+(\w+)", rf"CREATE OR REPLACE VIEW {alias}.\2",
                 view.sql, count=1, flags=re.IGNORECASE)
    con.execute(sql)


def lake_tables(con: Any, alias: str) -> list[str]:
    return [r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name=? "
                                      "AND schema_name='main' ORDER BY 1", [alias]).fetchall()]


def lake_views(con: Any, alias: str) -> list[str]:
    return [r[0] for r in con.execute("SELECT view_name FROM duckdb_views() WHERE database_name=? "
                                      "AND schema_name='main' ORDER BY 1", [alias]).fetchall()]


@dataclass
class SpatialSource:
    lake: Path
    data_path: Path
    version: int

    @classmethod
    def default(cls) -> SpatialSource | None:
        """The configured precedent lake (env.PRECEDENT_*), or None when it is disabled."""
        if env.PRECEDENT_LAKE is None or env.PRECEDENT_DATA is None:
            log.warning("no precedent lake configured (NEON_DUCKLAKE_PRECEDENT_LAKE); "
                        "the spatial-layer copy is skipped")
            return None
        return cls(env.PRECEDENT_LAKE, env.PRECEDENT_DATA, env.PRECEDENT_VERSION)


def spatial_layers(con: Any, src_alias: str, version: int) -> list[dict[str, Any]]:
    rows = con.execute(f"SELECT * FROM {src_alias}.layer_catalog AT (VERSION => {int(version)}) "
                       f"WHERE table_name LIKE '{SPATIAL_PREFIX}%' ORDER BY table_name").fetchall()
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r, strict=True)) for r in rows]


# ---------------------------------------------------------------- build


@dataclass
class BuildResult:
    run_id: str
    status: str
    snapshot_id: int | None = None
    inserted: dict[str, int] = field(default_factory=dict)
    deleted: dict[str, int] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    exit_code: int = 0


def remove_empty_dirs(root: Path) -> None:
    """Remove ``root`` and its sub-directories if they hold no files (rolled-back DuckLake table dirs)."""
    if not root.is_dir():
        return
    for d in sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        with contextlib.suppress(OSError):
            d.rmdir()
    with contextlib.suppress(OSError):
        root.rmdir()


def _now_id() -> str:
    return env.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")


def build(run_id: str, *, run_dir: Path, lake_dir: Path, dry_run: bool = False, allow_partial: bool = False,
          ledger_db: Path | None = None, spatial: SpatialSource | None = None, temp_dir: Path | None = None,
          workers: int = 12, lock: bool = True) -> BuildResult:
    """Flatten ``run_dir/sealed`` and upsert it into the catalog lake in ``lake_dir`` (one snapshot)."""
    from .seal import read_ledger

    t0 = time.time()
    fm_path = run_dir / "fetch_manifest.json"
    fm = json.loads(fm_path.read_text()) if fm_path.exists() else {}
    status = fm.get("status")
    if status != "complete" and not allow_partial:
        log.error("run %s fetch status is %r (not complete); pass --allow-partial to build anyway", run_id, status)
        return BuildResult(run_id, "refused", exit_code=2)
    sealed_manifest = run_dir / "sealed" / "MANIFEST.json"
    if sealed_manifest.exists() and json.loads(sealed_manifest.read_text()).get("errors"):
        log.error("sealed/MANIFEST.json of %s lists refused outputs (signed URLs); not building", run_id)
        return BuildResult(run_id, "refused", exit_code=2)
    ledger = read_ledger(run_id, ledger_db)
    flat = flatten.flatten_run(run_dir, ledger, workers=workers, fetch_manifest=fm)
    res = BuildResult(run_id, "running", timings={"flatten": round(time.time() - t0, 1)})

    lake_dir.mkdir(parents=True, exist_ok=True)
    lock_name = "catalog-lake" if lake_dir.resolve() == env.LAKE_DIR.resolve() else \
        "catalog-lake-" + hashlib.md5(str(lake_dir.resolve()).encode(), usedforsecurity=False).hexdigest()[:8]
    lock_cm = env.file_lock(lock_name) if lock else contextlib.nullcontext()
    con = connect(temp_dir)
    try:
        with lock_cm, chdir(lake_dir):
            t1 = time.time()
            staged: dict[str, str] = {}
            for name in DATA_TABLES:
                spec = TABLES[name]
                if name in flatten.PARQUET_TABLES:
                    staged[name] = stage_parquet(con, spec, flat.parquet.get(name, []), run_id)
                else:
                    staged[name] = stage_arrow(con, spec, flat.tables.get(name, flatten.empty_table(name)), run_id)
            res.timings["stage"] = round(time.time() - t1, 1)

            created = attach_catalog(con)
            existing = set(lake_tables(con, ALIAS))
            src_alias = None
            layers: list[dict[str, Any]] = []
            if spatial is not None and spatial.lake.exists():
                src_alias = "neon_precedent"
                con.execute(f"ATTACH 'ducklake:{spatial.lake}' AS {src_alias} (READ_ONLY, DATA_PATH "
                            f"'{str(spatial.data_path).rstrip('/')}/', OVERRIDE_DATA_PATH true)")
                layers = spatial_layers(con, src_alias, spatial.version)
            elif spatial is not None:
                log.warning("precedent lake %s not found (NEON_DUCKLAKE_PRECEDENT_LAKE); "
                            "the spatial-layer copy is skipped", spatial.lake)
            old_catalog = {}
            if "table_catalog" in existing:
                cur = con.execute(f"SELECT * FROM {ALIAS}.table_catalog")
                cols = [d[0] for d in cur.description]
                old_catalog = {r[0]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}
            if not layers:  # precedent unavailable: keep describing the layers already copied
                layers = [{"table_name": t, "kind": (old_catalog.get(t) or {}).get("description"),
                           "_source": (old_catalog.get(t) or {}).get("source")}
                          for t in sorted(existing) if t.startswith(SPATIAL_PREFIX) and t not in TABLES]

            t2 = time.time()
            con.execute("BEGIN")
            try:
                for name in DATA_TABLES:
                    spec = TABLES[name]
                    state = ensure_table(con, ALIAS, spec)
                    ins, dele = upsert(con, ALIAS, spec, staged[name])
                    res.inserted[name], res.deleted[name] = ins, dele
                    if state not in ("ok", "created"):
                        log.warning("table %s %s", name, state)
                spatial_rows = []
                for layer in layers:
                    tname = layer["table_name"]
                    if tname not in existing and src_alias:
                        con.execute(f"CREATE TABLE {ALIAS}.{q(tname)} AS SELECT * FROM {src_alias}.{q(tname)} "
                                    f"AT (VERSION => {int(spatial.version)})")
                        res.inserted[tname] = con.execute(f"SELECT count(*) FROM {ALIAS}.{q(tname)}").fetchone()[0]
                    spatial_rows.append(layer)
                views = load_views()
                have_views = set(lake_views(con, ALIAS))
                for v in views:
                    old = old_catalog.get(v.name) or {}
                    if v.name not in have_views or old.get("definitionSha256") != v.sha256:
                        create_view(con, ALIAS, v)
                        res.inserted[v.name] = 1
                res.timings["upsert"] = round(time.time() - t2, 1)

                t3 = time.time()
                for name in DATA_TABLES:
                    res.problems += check_table(con, ALIAS, TABLES[name])
                res.timings["assert"] = round(time.time() - t3, 1)

                # bookkeeping: one catalog_builds row, then the table catalog of everything in the lake
                for tname in lake_tables(con, ALIAS):
                    res.rows[tname] = con.execute(f"SELECT count(*) FROM {ALIAS}.{q(tname)}").fetchone()[0]
                build_id = _now_id()
                builds = flatten.to_table("catalog_builds", [{
                    "buildId": build_id, "builtAt": env.utcnow(), "runId": run_id, "runStatus": status,
                    "allowPartial": allow_partial, "inserted": {k: v for k, v in res.inserted.items() if v},
                    "deleted": {k: v for k, v in res.deleted.items() if v}, "rowCounts": res.rows,
                    "versions": env.repo_versions(), "tools": env.tool_versions()}])
                spec_b = TABLES["catalog_builds"]
                ensure_table(con, ALIAS, spec_b)
                res.inserted["catalog_builds"], res.deleted["catalog_builds"] = upsert(
                    con, ALIAS, spec_b, stage_arrow(con, spec_b, builds, run_id))
                res.rows["catalog_builds"] = con.execute(f"SELECT count(*) FROM {ALIAS}.catalog_builds").fetchone()[0]
                res.rows["table_catalog"] = len(TABLES) + len(spatial_rows) + len(views)
                cat_rows = table_catalog_rows(res.rows, spatial_rows, views, old_catalog, spatial)
                spec_c = TABLES["table_catalog"]
                ensure_table(con, ALIAS, spec_c)
                res.inserted["table_catalog"], res.deleted["table_catalog"] = upsert(
                    con, ALIAS, spec_c, stage_arrow(con, spec_c, flatten.to_table("table_catalog", cat_rows), run_id))
                for name in ("catalog_builds", "table_catalog"):
                    res.problems += check_table(con, ALIAS, TABLES[name])

                if res.problems:
                    con.execute("ROLLBACK")
                    for p in res.problems:
                        log.error("assertion failed: %s", p)
                    res.status, res.exit_code = "assertion-failed", 3
                    return res
                if dry_run:
                    con.execute("ROLLBACK")
                    res.status = "dry-run"
                    if src_alias:
                        con.execute(f"DETACH {src_alias}")
                    con.execute(f"DETACH {ALIAS}")
                    if created:  # a dry run against a new lake leaves nothing behind
                        for leftover in (Path(env.CATALOG_LAKE), Path(env.CATALOG_LAKE + ".wal")):
                            leftover.unlink(missing_ok=True)
                        remove_empty_dirs(Path("data"))
                    return res
                extra = {"runId": run_id, "buildId": build_id, "runStatus": status, "created": created,
                         "inserted": {k: v for k, v in res.inserted.items() if v},
                         "deleted": {k: v for k, v in res.deleted.items() if v}}
                con.execute(f"CALL ducklake_set_commit_message('{ALIAS}', ?, ?, extra_info => ?)",
                            [env.ACTOR, f"catalog build {run_id}", json.dumps(extra, separators=(",", ":"))])
                con.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    con.execute("ROLLBACK")
                raise
            res.snapshot_id = con.execute(f"SELECT max(snapshot_id) FROM {ALIAS}.snapshots()").fetchone()[0]
            if src_alias:
                con.execute(f"DETACH {src_alias}")
            con.execute(f"CHECKPOINT __ducklake_metadata_{ALIAS}")
            con.execute(f"DETACH {ALIAS}")
            wal = Path(env.CATALOG_LAKE + ".wal")
            if wal.exists():
                log.error("%s remains after CHECKPOINT/DETACH", lake_dir / wal)
                res.status, res.exit_code = "wal-remains", 4
                return res
            res.status = "committed"
    finally:
        con.close()
        res.timings["total"] = round(time.time() - t0, 1)
    return res


def table_catalog_rows(rows: dict[str, int], layers: list[dict[str, Any]], views: list[ViewDef],
                       old: dict[str, dict[str, Any]], spatial: SpatialSource | None) -> list[dict[str, Any]]:
    out = []
    for name, spec in TABLES.items():
        out.append({"tableName": name, "tableGroup": spec.group, "description": spec.description,
                    "keyColumns": list(spec.key), "columnCount": len(spec.columns) + len(spec.derived),
                    "rowCount": rows.get(name, 0), "sourceFamilies": list(spec.sources),
                    "source": "NEON Data API via neon-mcp (sealed harvest run)" if spec.sources else "neon-ducklake",
                    "definitionSha256": hashlib.sha256(repr((spec.columns, spec.key, spec.derived)).encode()).hexdigest()})
    for layer in layers:
        name = layer["table_name"]
        prev = old.get(name) or {}
        src = layer.get("_source") or (
            f"{layer.get('source_url') or layer.get('source_file')} via neon-downloads "
            f"{spatial.lake.name if spatial else 'neon_spatial.ducklake'}@v{spatial.version if spatial else '?'}")
        out.append({"tableName": name, "tableGroup": "spatial",
                    "description": layer.get("kind") or prev.get("description"), "keyColumns": [],
                    "columnCount": None, "rowCount": rows.get(name, 0), "sourceFamilies": [],
                    "source": src or prev.get("source"), "definitionSha256": None})
    for v in views:
        out.append({"tableName": v.name, "tableGroup": "view", "description": v.description, "keyColumns": [],
                    "columnCount": None, "rowCount": None, "sourceFamilies": [], "source": "sql/catalog_views.sql",
                    "definitionSha256": v.sha256})
    return out


def print_counts(res: BuildResult) -> None:
    names = sorted(set(res.inserted) | set(res.deleted) | set(res.rows))
    print(f"{'table':34s} {'rows':>10s} {'insert':>10s} {'delete':>10s}")
    for n in names:
        print(f"{n:34s} {res.rows.get(n, 0):>10,d} {res.inserted.get(n, 0):>10,d} {res.deleted.get(n, 0):>10,d}")
    print(f"status={res.status} snapshot={res.snapshot_id} timings={res.timings}")


def main(run_id: str | None = None, dry_run: bool = False, allow_partial: bool = False,
         lake_dir: Path | None = None) -> int:
    from .seal import resolve_run

    rid = resolve_run(run_id)
    res = build(rid, run_dir=env.RUNS_DIR / rid, lake_dir=Path(lake_dir) if lake_dir else env.LAKE_DIR,
                dry_run=dry_run, allow_partial=allow_partial, spatial=SpatialSource.default(),
                temp_dir=env.WORK_DIR / "duckdb_tmp", workers=min(12, os.cpu_count() or 4))
    if res.status != "refused":
        print_counts(res)
    return res.exit_code
