"""seal.py: bundling raw landings into sealed/ (hermetic)."""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from neon_ducklake import seal
from tests.test_catalog_fakes import fake_env, make_run  # noqa: F401 - fixture


def _lines(path: Path) -> list[dict]:
    with gzip.open(path, "rt") as fh:
        return [json.loads(x) for x in fh]


def test_seal_bundles_every_family(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    man = seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    sealed = run.run_dir / "sealed"
    names = {f["file"] for f in man["files"]}
    assert {"products.jsonl.gz", "taxonomy.jsonl.gz", "manifests.jsonl.gz", "external.jsonl.gz"} <= names
    # data_query: one file per product
    assert {"data_query/DP1.00001.001.jsonl.gz", "data_query/DP1.00006.001.jsonl.gz",
            "data_query/DP1.10003.001.jsonl.gz"} <= names
    assert {"stage/pairs.json.gz", "stage/location_nodes.json.gz"} <= names
    assert not man["errors"]
    lines = _lines(sealed / "data_query" / "DP1.00001.001.jsonl.gz")
    assert [ln["key"] for ln in lines] == sorted(ln["key"] for ln in lines)
    first = lines[0]
    assert set(first) == {"key", "request", "fetchedAt", "payload"}
    assert first["request"]["method"] == "POST" and first["request"]["params"] == {"k": first["key"]}
    assert first["payload"]["_request"]["pair"]["product"] == "DP1.00001.001"
    for entry in man["files"]:
        data = (sealed / entry["file"]).read_bytes()
        assert entry["bytes"] == len(data)
        assert entry["md5"] == hashlib.md5(data).hexdigest()
    assert man["totals"]["files"] == len(man["files"])
    assert json.loads((sealed / "MANIFEST.json").read_text())["files"] == man["files"]


def test_seal_is_idempotent_and_incremental(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    led = seal.read_ledger(run.run_id, run.ledger_db)
    seal.seal_run(run.run_dir, led, workers=1)
    sealed = run.run_dir / "sealed"
    before = {p: p.stat().st_mtime_ns for p in sealed.rglob("*") if p.is_file()}
    man2 = seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    after = {p: p.stat().st_mtime_ns for p in sealed.rglob("*") if p.is_file()}
    assert before == after  # nothing rewritten, MANIFEST untouched
    # a new landing only re-seals its own output
    run.land("taxonomy", "BIRD__500", {"count": 0, "total": 2, "data": []}, fetched_at="2026-01-02T00:00:00+00:00")
    man3 = seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    changed = {p for p in sealed.rglob("*") if p.is_file() and after.get(p) != p.stat().st_mtime_ns}
    assert {p.name for p in changed} == {"taxonomy.jsonl.gz", "MANIFEST.json"}
    tax = next(f for f in man3["files"] if f["file"] == "taxonomy.jsonl.gz")
    assert tax["lines"] == 2 and tax["md5"] != next(f for f in man2["files"] if f["file"] == "taxonomy.jsonl.gz")["md5"]


def test_seal_output_is_deterministic(tmp_path: Path) -> None:
    a = make_run(tmp_path / "a")
    b = make_run(tmp_path / "b")
    ma = seal.seal_run(a.run_dir, seal.read_ledger(a.run_id, a.ledger_db), workers=1)
    mb = seal.seal_run(b.run_dir, seal.read_ledger(b.run_id, b.ledger_db), workers=2)
    # jsonl bundles are byte-identical; stage/ copies are verbatim (their gzip header carries the harvest's mtime)
    def bundles(man: dict) -> list[tuple[str, str]]:
        return [(f["file"], f["md5"]) for f in man["files"] if not f["file"].startswith("stage/")]
    assert bundles(ma) == bundles(mb) and len(bundles(ma)) == len(ma["files"]) - 3


def test_seal_ignores_part_files(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    part = run.run_dir / "raw" / "data_query" / "DP9.99999.001__SRER__2020-01_2020-01.json.gz.part"
    part.write_bytes(b"not gzip")
    man = seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    assert not any("DP9.99999" in f["file"] for f in man["files"])


def test_seal_refuses_signed_urls(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    assert (run.run_dir / "sealed" / "external.jsonl.gz").exists()
    run.land("external", "DP1.00001.001__SRER__2020-01", {"externalData": [
        {"url": "https://storage.googleapis.com/x.csv?X-Goog-Algorithm=GOOG4&X-Goog-Signature=abc"}]})
    man = seal.seal_run(run.run_dir, seal.read_ledger(run.run_id, run.ledger_db), workers=1)
    assert [e["file"] for e in man["errors"]] == ["external.jsonl.gz"]
    assert not (run.run_dir / "sealed" / "external.jsonl.gz").exists()
    assert not list((run.run_dir / "sealed").glob("*.part"))
    assert seal.scan_signed(run.run_dir / "sealed") == []


def test_scan_signed_finds_marker(tmp_path: Path) -> None:
    d = tmp_path / "sealed"
    d.mkdir()
    (d / "ok.jsonl.gz").write_bytes(gzip.compress(b'{"url":"https://x/y.csv"}\n'))
    (d / "bad.jsonl.gz").write_bytes(gzip.compress(b'{"url":"https://x/y.csv?X-Goog-Signature=1"}\n'))
    assert seal.scan_signed(d) == ["bad.jsonl.gz"]


def test_main_uses_env_paths(fake_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    from neon_ducklake import state

    run = make_run(fake_env)
    monkeypatch.setattr(state, "DB_PATH", run.ledger_db)
    assert seal.resolve_run(None, fake_env / "api" / "runs") == run.run_id
    assert seal.main(run.run_id) == 0
    assert (run.run_dir / "sealed" / "MANIFEST.json").exists()
