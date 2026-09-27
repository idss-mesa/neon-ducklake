"""Phase 1 — seal a harvest run: bundle the raw landings into one gz/jsonl per family.

``api/runs/<runId>/raw/<family>/<key>.json.gz`` -> ``api/runs/<runId>/sealed/<family>.jsonl.gz`` (one line per
landing: ``{"key", "request": {method, path, params}, "fetchedAt", "payload"}``, sorted by key);
``data_query`` goes to ``sealed/data_query/<DP>.jsonl.gz``, one file per product. The harvest's stage files
(``stage/{pairs,zero_pairs,location_nodes}.json.gz``) are copied to ``sealed/stage/`` so ``sealed/`` is the
complete, self-describing build input (``raw/`` is never mirrored). ``sealed/MANIFEST.json`` lists every file
with lines, bytes and md5.

Idempotent: an output whose inputs (names, sizes, mtimes, ledger fetch times) are unchanged is not rewritten,
and gzip output is deterministic (mtime 0, no embedded name). A byte stream containing ``X-Goog`` (a signed
URL) aborts the seal for that output, which is never written.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import shutil
import sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

from . import env
from .env import utcnow, write_json

log = logging.getLogger("neon_ducklake.seal")

SEAL_VERSION = 1
SIGNED_MARK = b"X-Goog"
STAGE_FILES = ("pairs", "zero_pairs", "location_nodes")
PER_PRODUCT_FAMILIES = ("data_query",)


class SignedUrlError(RuntimeError):
    """A signed URL (X-Goog query string) was found in a byte stream bound for sealed/."""


# ---------------------------------------------------------------- run + ledger

def resolve_run(run_id: str | None, runs_dir: Path | None = None) -> str:
    """Explicit run id, else api/LATEST, else the newest run directory."""
    if run_id:
        return run_id
    runs_dir = runs_dir or env.RUNS_DIR
    latest = env.API_DIR / "LATEST"
    if runs_dir == env.RUNS_DIR and latest.exists() and latest.read_text().strip():
        return latest.read_text().strip()
    cands = sorted(d.name for d in runs_dir.iterdir() if d.is_dir()) if runs_dir.exists() else []
    if not cands:
        raise SystemExit(f"no harvest runs under {runs_dir}")
    return cands[-1]


def read_ledger(run_id: str, db_path: Path | None = None) -> list[dict[str, Any]]:
    """All ledger rows of a run, read-only (never creates or migrates work/state.sqlite)."""
    from .state import DB_PATH

    path = db_path or DB_PATH
    if not path.exists():
        return []
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
    try:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute("SELECT * FROM requests WHERE run_id=? ORDER BY family, key",
                                             (run_id,))]
    finally:
        con.close()


def ledger_by_landing(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Ledger rows keyed by their landed path relative to the run dir (``raw/<family>/<file>``)."""
    return {r["landed_path"]: r for r in rows if r.get("landed_path")}


# ---------------------------------------------------------------- planning

def output_name(family: str, landing: Path) -> str:
    """sealed/ relative output path of one landing."""
    if family in PER_PRODUCT_FAMILIES:
        return f"{family}/{landing.name.split('__', 1)[0]}.jsonl.gz"
    return f"{family}.jsonl.gz"


def plan_outputs(raw_dir: Path) -> dict[str, list[Path]]:
    """Group complete landings (``*.json.gz``; never ``.part``) by sealed output file."""
    out: dict[str, list[Path]] = defaultdict(list)
    if not raw_dir.exists():
        return {}
    for fam_dir in sorted(p for p in raw_dir.iterdir() if p.is_dir()):
        for f in sorted(fam_dir.glob("*.json.gz")):
            out[output_name(fam_dir.name, f)].append(f)
    return dict(out)


def inputs_signature(files: list[Path], ledger: dict[str, dict[str, Any]], run_dir: Path) -> str:
    h = hashlib.sha256(f"v{SEAL_VERSION}".encode())
    for f in sorted(files):
        st = f.stat()
        rel = str(f.relative_to(run_dir))
        led = ledger.get(rel) or {}
        h.update(f"{rel}\0{st.st_size}\0{st.st_mtime_ns}\0{led.get('fetched_at')}\0{led.get('sha256')}\n".encode())
    return h.hexdigest()


# ---------------------------------------------------------------- writing

def _params(value: Any) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def seal_line(rel: str, raw_json: bytes, led: dict[str, Any] | None) -> tuple[str, bytes]:
    """(key, one sealed jsonl line); the payload bytes are embedded verbatim (already compact JSON)."""
    led = led or {}
    key = led.get("key") or Path(rel).name.removesuffix(".json.gz")
    request = {"method": led.get("method"), "path": led.get("path"), "params": _params(led.get("params"))}
    head = json.dumps({"key": key, "request": request, "fetchedAt": led.get("fetched_at")},
                      separators=(",", ":"), ensure_ascii=False)
    body = raw_json.strip()
    return key, head[:-1].encode() + b',"payload":' + body + b"}\n"


def write_sealed(out: Path, files: list[Path], ledger: dict[str, dict[str, Any]], run_dir: Path) -> dict[str, Any]:
    """Write one sealed output atomically; returns {lines, bytes, md5}. Raises SignedUrlError."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".part")
    lines = []
    for f in files:
        rel = str(f.relative_to(run_dir))
        raw = gzip.decompress(f.read_bytes())
        led = ledger.get(rel)
        key, line = seal_line(rel, raw, led)
        if SIGNED_MARK in line:
            raise SignedUrlError(f"signed URL in {rel}")
        lines.append((key, line))
    lines.sort(key=lambda kv: kv[0])
    md5 = hashlib.md5(usedforsecurity=False)
    try:
        with tmp.open("wb") as fh, gzip.GzipFile(filename="", mode="wb", fileobj=fh, compresslevel=6, mtime=0) as gz:
            for _k, line in lines:
                gz.write(line)
        data = tmp.read_bytes()
        md5.update(data)
        os.replace(tmp, out)
    finally:
        tmp.unlink(missing_ok=True)
    return {"lines": len(lines), "bytes": len(data), "md5": md5.hexdigest()}


def _seal_one(args: tuple[str, str, list[str], dict[str, dict[str, Any]], str]) -> dict[str, Any]:
    name, sealed_dir, files, ledger, run_dir = args
    try:
        info = write_sealed(Path(sealed_dir) / name, [Path(f) for f in files], ledger, Path(run_dir))
        return {"file": name, **info}
    except SignedUrlError as exc:
        return {"file": name, "error": str(exc)}


def _file_md5(path: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def seal_run(run_dir: Path, ledger_rows: list[dict[str, Any]], *, workers: int = 8,
             force: bool = False) -> dict[str, Any]:
    """Seal one run directory; returns the MANIFEST dict (also written to sealed/MANIFEST.json)."""
    raw_dir, sealed_dir = run_dir / "raw", run_dir / "sealed"
    sealed_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = sealed_dir / "MANIFEST.json"
    old = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    old_files = {f["file"]: f for f in old.get("files", [])}
    ledger = ledger_by_landing(ledger_rows)
    plan = plan_outputs(raw_dir)
    entries: dict[str, dict[str, Any]] = {}
    todo = []
    for name, files in plan.items():
        sig = inputs_signature(files, ledger, run_dir)
        prev = old_files.get(name)
        out = sealed_dir / name
        if (not force and prev and prev.get("inputsSig") == sig and out.exists()
                and out.stat().st_size == prev.get("bytes")):
            entries[name] = prev
            continue
        sub = {str(f.relative_to(run_dir)): ledger[str(f.relative_to(run_dir))]
               for f in files if str(f.relative_to(run_dir)) in ledger}
        todo.append((name, sig, files, sub))
    errors = []
    if todo:
        log.info("sealing %d outputs (%d unchanged)", len(todo), len(entries))
        args = [(n, str(sealed_dir), [str(f) for f in fs], sub, str(run_dir)) for n, _s, fs, sub in todo]
        sigs = {n: (s, len(fs)) for n, s, fs, _sub in todo}
        if workers > 1 and len(args) > 1:
            with ProcessPoolExecutor(max_workers=workers) as ex:
                results = list(ex.map(_seal_one, args))
        else:
            results = [_seal_one(a) for a in args]
        for res in results:
            if "error" in res:
                errors.append(res)
                (sealed_dir / res["file"]).unlink(missing_ok=True)  # never leave a stale copy to be built
                log.error("seal refused %s: %s", res["file"], res["error"])
                continue
            sig, n_in = sigs[res["file"]]
            entries[res["file"]] = {**res, "family": res["file"].split("/")[0].removesuffix(".jsonl.gz"),
                                    "inputs": n_in, "inputsSig": sig}
    # stage files, copied verbatim (deterministic names, checked for signed URLs)
    for stem in STAGE_FILES:
        src = run_dir / "stage" / f"{stem}.json.gz"
        if not src.exists():
            continue
        name = f"stage/{stem}.json.gz"
        dst = sealed_dir / name
        src_md5 = _file_md5(src)
        prev = old_files.get(name)
        if not force and prev and prev.get("md5") == src_md5 and dst.exists():
            entries[name] = prev
            continue
        if SIGNED_MARK in gzip.decompress(src.read_bytes()):
            errors.append({"file": name, "error": "signed URL in stage file"})
            dst.unlink(missing_ok=True)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(dst.name + ".part")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dst)
        entries[name] = {"file": name, "family": "stage", "lines": None, "bytes": dst.stat().st_size,
                         "md5": src_md5, "inputs": 1, "inputsSig": src_md5}
    files = [entries[k] for k in sorted(entries)]
    fetch_status = None
    fm = run_dir / "fetch_manifest.json"
    if fm.exists():
        fetch_status = json.loads(fm.read_text()).get("status")
    manifest = {"runId": run_dir.name, "sealVersion": SEAL_VERSION, "fetchStatus": fetch_status,
                "sealedAt": old.get("sealedAt"), "files": files,
                "totals": {"files": len(files), "lines": sum(f.get("lines") or 0 for f in files),
                           "bytes": sum(f.get("bytes") or 0 for f in files)},
                "errors": errors}
    comparable = {k: v for k, v in manifest.items() if k != "sealedAt"}
    if comparable != {k: v for k, v in old.items() if k != "sealedAt"}:
        manifest["sealedAt"] = utcnow().isoformat()
        write_json(manifest_path, manifest)
    return manifest


def scan_signed(sealed_dir: Path) -> list[str]:
    """Sealed files whose decompressed bytes contain a signed-URL marker (should be empty)."""
    bad = []
    for f in sorted(sealed_dir.rglob("*.gz")):
        with gzip.open(f, "rb") as gz:
            tail = b""
            for chunk in iter(lambda gz=gz: gz.read(1 << 22), b""):
                if SIGNED_MARK in tail + chunk:
                    bad.append(str(f.relative_to(sealed_dir)))
                    break
                tail = chunk[-16:]
    return bad


def main(run_id: str | None = None) -> int:
    rid = resolve_run(run_id)
    run_dir = env.RUNS_DIR / rid
    if not (run_dir / "raw").exists():
        log.error("run %s has no raw/ directory", rid)
        return 1
    ledger = read_ledger(rid)
    man = seal_run(run_dir, ledger, workers=min(16, os.cpu_count() or 4))
    t = man["totals"]
    log.info("sealed %s: %d files, %d lines, %.1f MB (fetch status %s)", rid, t["files"], t["lines"],
             t["bytes"] / 1e6, man["fetchStatus"])
    if man["errors"]:
        log.error("%d outputs refused (signed URLs); see sealed/MANIFEST.json", len(man["errors"]))
        return 2
    return 0
