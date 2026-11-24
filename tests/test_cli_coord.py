"""cli.py wiring of the multi-VM commands against the real modules: `claims` with the real coord.claims_main and
a fake Data Store session, `worker --no-bootstrap`, `journal push|restore`, `recorder` coalescing flags,
`run --no-claims` and `site restore`."""

from __future__ import annotations

import inspect
import json
import sys
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import cli, coord, journal_sync, run

from .test_coord_fakes import ROOT, DSIrods, DSSession, hermetic_ds


@pytest.fixture
def ds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DSIrods:
    return hermetic_ds(tmp_path, monkeypatch)


def _fake_module(monkeypatch: pytest.MonkeyPatch, name: str, **attrs: Any) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, name, mod)
    return mod


def _stale_claim(store: DSIrods, site: str) -> str:
    past = datetime.now(UTC) - timedelta(hours=3)
    path = f"{ROOT}/sites/{site}/.claim"
    doc = {"kind": "site", "key": site, "vm": "vm-gone", "host": "gone-host", "pid": 999999, "token": "stale",
           "claimedAt": past.isoformat(), "heartbeatAt": past.isoformat(),
           "expiresAt": (datetime.now(UTC) - timedelta(minutes=5)).isoformat(), "ttlS": 7200}
    store.add_obj(path, content=json.dumps(doc).encode())
    return path


# ---------------------------------------------------------------- m1: claims --json and repeated --release

def test_claims_cli_with_the_real_claims_main(ds: DSIrods, monkeypatch: pytest.MonkeyPatch,
                                              capsys: pytest.CaptureFixture[str]) -> None:
    s = DSSession(ds)
    monkeypatch.setattr(coord, "get_session", lambda session=None: s)
    assert coord.claim_site("HARV", session=s, settle_s=0, vm="vm-a")  # a live claim of another VM
    assert coord.claim_site("ARIK", session=s, settle_s=0)  # ours
    stale = _stale_claim(ds, "SRER")
    assert cli.main(["claims", "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert {"HARV", "ARIK", "SRER"} <= set(doc["sites"]) and doc["sites"]["SRER"]["stale"]
    assert cli.main(["claims"]) == 0 and "HARV" in capsys.readouterr().out
    # repeated --release: a stale claim and our own go; a live claim of another VM only with --force
    assert cli.main(["claims", "--release", "srer", "--release", "arik"]) == 0
    assert stale not in ds.objs and f"{ROOT}/sites/ARIK/.claim" not in ds.objs
    assert cli.main(["claims", "--release", "HARV"]) == 1 and f"{ROOT}/sites/HARV/.claim" in ds.objs
    assert cli.main(["claims", "--release", "HARV", "--force"]) == 0 and f"{ROOT}/sites/HARV/.claim" not in ds.objs
    with pytest.raises(SystemExit, match="--json lists"):
        cli.main(["claims", "--json", "--release", "HARV"])


# ---------------------------------------------------------------- m2: worker --no-bootstrap, journal

def test_worker_no_bootstrap_reaches_the_real_worker_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake import worker

    assert "bootstrap" in inspect.signature(worker.main).parameters
    got: dict[str, Any] = {}
    real = inspect.signature(worker.main)

    def fake_main(*args: Any, **kw: Any) -> int:
        got.update(real.bind(*args, **kw).arguments)  # the call must fit the real signature
        return 0

    fake_main.__signature__ = real  # type: ignore[attr-defined]
    monkeypatch.setattr(worker, "main", fake_main)
    assert cli.main(["worker", "--sites", "srer", "--no-bootstrap", "--once"]) == 0
    assert got["bootstrap"] is False and got["sites"] == ["SRER"] and got["once"] is True
    got.clear()
    assert cli.main(["worker"]) == 0 and "bootstrap" not in got  # the worker's own default (on)
    _fake_module(monkeypatch, "neon_ducklake.worker", main=lambda sites, habitat, max_sites, chunk_bytes,
                 workset_bytes, once=False: 0)
    with pytest.raises(SystemExit, match="no --no-bootstrap"):
        cli.main(["worker", "--no-bootstrap"])


def test_journal_push_and_restore_commands(monkeypatch: pytest.MonkeyPatch,
                                           capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(journal_sync, "push_site_journal",
                        lambda site: calls.append(("push", site)) or {"site": site, "tables": {"site_files": 3}})
    restored = {"v": True}
    monkeypatch.setattr(journal_sync, "restore_site_journal",
                        lambda site, *, force=False: calls.append(("restore", site, force)) or restored["v"])
    assert cli.main(["journal", "push", "srer"]) == 0 and calls[-1] == ("push", "SRER")
    assert json.loads(capsys.readouterr().out)["tables"] == {"site_files": 3}
    assert cli.main(["journal", "restore", "srer"]) == 0 and calls[-1] == ("restore", "SRER", False)
    assert "restored" in capsys.readouterr().out
    restored["v"] = False
    assert cli.main(["journal", "restore", "SRER", "--force"]) == 1 and calls[-1] == ("restore", "SRER", True)
    assert "not restored" in capsys.readouterr().out
    monkeypatch.setattr(journal_sync, "push_site_journal", lambda site: {"error": "iRODS down"})
    assert cli.main(["journal", "push", "SRER"]) == 1
    with pytest.raises(SystemExit, match="--force applies"):
        cli.main(["journal", "push", "SRER", "--force"])
    with pytest.raises(SystemExit):
        cli.main(["journal", "status", "SRER"])


# ---------------------------------------------------------------- recorder coalescing flags

def test_recorder_min_rows_max_age_and_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    got: dict[str, Any] = {}

    def recorder_main(*, watch: bool = False, interval: float = 60.0, max_rows: int | None = None,
                      min_rows: int | None = None, max_age_h: float | None = None, flush: bool = False,
                      dry_run: bool = False) -> int:
        got.clear()
        got.update(watch=watch, interval=interval, max_rows=max_rows, min_rows=min_rows, max_age_h=max_age_h,
                   flush=flush, dry_run=dry_run)
        return 0

    _fake_module(monkeypatch, "neon_ducklake.avu.recorder", main=recorder_main)
    assert cli.main(["recorder", "--watch", "--interval", "300", "--min-rows", "250000", "--max-age-h", "12"]) == 0
    assert got == {"watch": True, "interval": 300, "max_rows": None, "min_rows": 250_000, "max_age_h": 12.0,
                   "flush": False, "dry_run": False}
    assert cli.main(["recorder", "--flush"]) == 0 and got["flush"] is True and got["min_rows"] is None
    for argv, msg in [(["recorder", "--flush", "--min-rows", "5"], "drop --min-rows"),
                      (["recorder", "--min-rows", "0"], "--min-rows must be >= 1"),
                      (["recorder", "--max-age-h", "0"], "--max-age-h must be positive")]:
        with pytest.raises(SystemExit, match=msg):
            cli.main(argv)

    def recorder_seconds(*, watch: bool = False, interval: float = 60.0, max_age_s: float | None = None) -> int:
        got.clear()
        got.update(max_age_s=max_age_s)
        return 0

    _fake_module(monkeypatch, "neon_ducklake.avu.recorder", main=recorder_seconds)
    assert cli.main(["recorder", "--max-age-h", "2"]) == 0 and got == {"max_age_s": 7200.0}
    with pytest.raises(SystemExit, match="this recorder has no --min-rows"):
        cli.main(["recorder", "--min-rows", "10"])
    with pytest.raises(SystemExit, match="this recorder has no --flush"):
        cli.main(["recorder", "--flush"])


# ---------------------------------------------------------------- run --no-claims

def test_run_claims_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    got: dict[str, Any] = {}
    monkeypatch.setattr(run, "main", lambda sites, **kw: got.update(kw) or 0)
    assert cli.main(["run", "SRER"]) == 0 and got["claims"] is True
    assert cli.main(["run", "SRER", "--no-claims"]) == 0 and got["claims"] is False


# ---------------------------------------------------------------- site restore

def test_site_restore_restores_a_newer_journal_then_the_lake(monkeypatch: pytest.MonkeyPatch,
                                                             capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[Any, ...]] = []
    state = {"needs": False, "restored": True, "lake_rc": 0}
    monkeypatch.setattr(journal_sync, "needs_restore", lambda site: calls.append(("needs", site)) or state["needs"],
                        raising=False)
    monkeypatch.setattr(journal_sync, "restore_site_journal",
                        lambda site, *, force=False: calls.append(("restore", site, force)) or state["restored"])
    _fake_module(monkeypatch, "neon_ducklake.site.stream",
                 restore_lake=lambda site: calls.append(("lake", site)) or state["lake_rc"])
    assert cli.main(["site", "restore", "srer"]) == 0
    assert calls == [("needs", "SRER"), ("lake", "SRER")] and "local journal is current" in capsys.readouterr().out
    calls.clear()
    state["needs"] = True  # another VM pushed a newer journal: restore it before the lake
    assert cli.main(["site", "restore", "SRER"]) == 0
    assert calls == [("needs", "SRER"), ("restore", "SRER", False), ("lake", "SRER")]
    calls.clear()
    state["restored"] = False  # a newer backup that could not be restored stops the site before any stage
    assert cli.main(["site", "restore", "SRER"]) == 2 and ("lake", "SRER") not in calls
    calls.clear()
    state.update(needs=False, restored=True, lake_rc=2)  # the lake does not cover the stacked chunks
    assert cli.main(["site", "restore", "SRER", "--force"]) == 2
    assert calls == [("restore", "SRER", True), ("lake", "SRER")]


# ---------------------------------------------------------------- stream stages run by hand spool too

def test_site_chunk_and_finalize_by_hand_spool_their_history(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    seen: list[str | None] = []
    _fake_module(monkeypatch, "neon_ducklake.site.stream",
                 history_chunk=lambda site, chunk_id: seen.append(os.environ.get(run.ENV_HISTORY_MODE)) or 0,
                 finalize_site=lambda site: seen.append(os.environ.get(run.ENV_HISTORY_MODE)) or 0)
    monkeypatch.delenv(run.ENV_HISTORY_MODE, raising=False)
    monkeypatch.delenv(run.ENV_MULTIWRITER, raising=False)
    assert cli.main(["site", "chunk", "history", "SRER", "--chunk", "C1"]) == 0
    assert cli.main(["site", "finalize", "SRER"]) == 0
    assert seen == ["spool", "spool"] and run.ENV_HISTORY_MODE not in os.environ  # only for the stage
    monkeypatch.setenv(run.ENV_HISTORY_MODE, "direct")
    with pytest.raises(SystemExit, match="MESA_MULTIWRITER"):
        cli.main(["site", "chunk", "history", "SRER", "--chunk", "C1"])
    monkeypatch.setenv(run.ENV_MULTIWRITER, "1")  # the DuckDB 2.0 switch
    assert cli.main(["site", "finalize", "SRER"]) == 0 and seen[-1] == "direct"


# ---------------------------------------------------------------- N12: the sweep's recorder really coalesces

def _script_recorder_args() -> tuple[list[str], list[str]]:
    """RECORDER_ARGS of scripts/full_sweep.sh (evaluated by bash) and the hint printed by scripts/sweep_status.sh."""
    import re
    import subprocess

    root = Path(__file__).resolve().parents[1]
    line = next(x for x in (root / "scripts/full_sweep.sh").read_text().splitlines()
                if x.startswith("readonly RECORDER_ARGS="))
    out = subprocess.run(["bash", "-c", f'{line}; printf "%s\\n" "${{RECORDER_ARGS[@]}}"'], check=True,
                         capture_output=True, text=True).stdout.split()
    hint = re.search(r"neon-ducklake (recorder --watch[^)\"]*)", (root / "scripts/sweep_status.sh").read_text())
    assert hint is not None
    return out, hint.group(1).split()


def test_full_sweep_recorder_min_rows_is_below_max_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from neon_ducklake.avu import recorder

    sweep, hint = _script_recorder_args()
    assert sweep == hint  # the status hint starts the same recorder
    got: dict[str, Any] = {}

    def capture(**kw: Any) -> int:
        got.update(kw)
        return 0

    capture.__signature__ = inspect.signature(recorder.main)  # type: ignore[attr-defined]  # the real options
    monkeypatch.setattr(recorder, "main", capture)
    assert cli.main(sweep) == 0
    assert got["watch"] is True and got["max_age_h"] == 12.0
    assert got["max_rows"] == 250_000 and got["min_rows"] == recorder.DEFAULT_MIN_ROWS == 200_000
    assert got["min_rows"] < got["max_rows"]
    # why: groups never exceed max_rows, so with min_rows == max_rows a group of 60k-row batches (4 x 60k = 240k)
    # would never be due and every snapshot would wait the 12 h max age
    now = datetime.now(UTC)
    batches = [recorder.SpoolBatch("vm-a", f"b{i}", "", "", {"rows": 60_000, "createdAt": now.isoformat()},
                                   None)  # type: ignore[arg-type]
               for i in range(8)]
    groups = recorder.plan_groups(batches, got["max_rows"])
    assert [sum(b.rows for b in g) for g in groups] == [240_000, 240_000]
    due, wait = recorder.eligible_groups(groups, min_rows=got["min_rows"], max_age_h=12, flush=False, now=now)
    assert len(due) == 2 and wait == []
    due, wait = recorder.eligible_groups(groups, min_rows=got["max_rows"], max_age_h=12, flush=False, now=now)
    assert due == [] and len(wait) == 2
