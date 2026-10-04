"""run.py: stage commands, subprocess runner, pre-stage guards, site selection, status, CLI wiring, dry run."""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from typing import Any

import duckdb
import pytest

from neon_ducklake import cli, env, run, state

from .test_site_common import SITE, site_env  # noqa: F401 - fixture

# ---------------------------------------------------------------- commands

def test_stage_commands_map_to_existing_cli_commands() -> None:
    cmd = {s: [argv[3:] for argv in run.stage_commands("SRER", s, python="py")] for s in run.STAGES}
    assert cmd["plan"] == [["site", "plan", "SRER"]]
    assert cmd["list"] == [["site", "list", "SRER"]]
    assert cmd["apply"] == [["site", "build-avus", "SRER"], ["tag", "SRER", "--apply-only"]]
    assert cmd["record"] == [["tag", "SRER", "--record-only"]]
    assert cmd["verify"] == [["verify", "SRER"]]
    assert cmd["evict"] == [["site", "evict", "SRER"]]
    assert run.stage_commands("X", "plan", python="py")[0][:3] == ["py", "-m", "neon_ducklake.cli"]
    assert run.stage_commands("X", "download", sample=2, threads=4)[0][3:] == \
        ["site", "download", "X", "--sample", "2", "--threads", "4"]
    assert run.stage_commands("X", "stack", restack=True)[0][3:] == ["site", "stack", "X", "--force"]


def test_every_stage_command_parses_with_the_real_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    import neon_ducklake.site as site_pkg
    from neon_ducklake import verify
    from neon_ducklake.avu import tag

    seen: list[tuple[str, Any]] = []
    monkeypatch.setattr(site_pkg, "dispatch", lambda a: seen.append(("site", (a.step, a.site, a.sample,
                                                                               a.threads, a.force))) or 0)
    monkeypatch.setattr(tag, "main", lambda scope, **kw: seen.append(("tag", (scope, kw["apply_only"],
                                                                               kw["record_only"]))) or 0)
    monkeypatch.setattr(verify, "main", lambda scope, **kw: seen.append(("verify", scope)) or 0)
    for s in run.STAGES:
        for argv in run.stage_commands("XYZW", s, sample=2, threads=4, restack=True):
            assert cli.main(argv[3:]) == 0, argv
    assert ("site", ("list", "XYZW", 2, 4, False)) in seen
    assert ("site", ("stack", "XYZW", None, 8, True)) in seen
    assert ("tag", ("XYZW", True, False)) in seen and ("tag", ("XYZW", False, True)) in seen
    assert ("verify", "XYZW") in seen and ("site", ("evict", "XYZW", None, 8, False)) in seen
    assert len(seen) == len(run.STAGES) + 1  # apply is two commands


# ---------------------------------------------------------------- subprocess runner

def test_subprocess_runner_logs_output_and_stops_on_failure(site_env: Path,  # noqa: F811
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    progs = {"ok": [[sys.executable, "-c", "print('hello from child')"],
                    [sys.executable, "-c", "print('second')"]],
             "bad": [[sys.executable, "-c", "import sys; print('boom'); sys.exit(3)"],
                     [sys.executable, "-c", "print('never')"]]}
    monkeypatch.setattr(run, "stage_commands", lambda site, stage, **kw: progs[stage])
    spawned: list[tuple[str, str, int]] = []
    r = run.SubprocessRunner(run.make_config(), on_spawn=lambda s, st, pid: spawned.append((s, st, pid)))
    assert r("S1", "ok") == 0
    text = (site_env / "logs/run/S1/ok.log").read_text()
    assert "hello from child" in text and "second" in text and "=== exit 0" in text
    assert r("S1", "bad") == 3
    text = (site_env / "logs/run/S1/bad.log").read_text()
    assert "boom" in text and "never" not in text
    assert [x[:2] for x in spawned] == [("S1", "ok"), ("S1", "ok"), ("S1", "bad")]


def test_subprocess_runner_kill_terminates_children(site_env: Path,  # noqa: F811
                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run, "stage_commands",
                        lambda site, stage, **kw: [[sys.executable, "-c", "import time; time.sleep(30)"]])
    r = run.SubprocessRunner(run.make_config(), grace_s=2)
    out: dict[str, int] = {}
    t = threading.Thread(target=lambda: out.update(rc=r("S1", "upload")))
    t.start()
    deadline = time.monotonic() + 10
    while not r._procs and time.monotonic() < deadline:
        time.sleep(0.01)
    r.kill()
    t.join(10)
    assert not t.is_alive() and out["rc"] < 0


# ---------------------------------------------------------------- pre-stage guards

def _journal(root: Path, rows: list[dict[str, Any]], evicted: set[str] = frozenset()) -> None:
    from neon_ducklake.site import evict

    con = state.connect(state.DB_PATH)
    evict.ensure_schema(con)
    for r in rows:
        rel = f"sites/{SITE}/raw/{r['product']}/2024-05/{r['name']}"
        if r.get("on_disk", True) and r.get("dl_status", "ok") == "ok" and r["name"] not in evicted:
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_bytes(b"x" * 10)
        con.execute("INSERT INTO site_files(site, product, month, name, kind, size, local_path, dl_status, dl_bytes,"
                    " local_evicted) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (SITE, r["product"], "2024-05", r["name"], r.get("kind", "data"), 10, rel,
                     r.get("dl_status", "ok"), 10, int(r["name"] in evicted)))
    con.close()


def test_prep_download_restores_changed_products_and_stack_guard(site_env: Path) -> None:  # noqa: F811
    _journal(site_env, [
        {"product": "DP1.A", "name": "a1.csv"}, {"product": "DP1.A", "name": "a2.csv"},
        {"product": "DP1.A", "name": "a.variables.csv", "kind": "variables"},
        {"product": "DP1.A", "name": "a3.csv", "dl_status": None},  # new month listed after eviction
        {"product": "DP1.B", "name": "b1.csv"}, {"product": "DP1.B", "name": "b.variables.csv", "kind": "variables"},
    ], evicted={"a1.csv", "a2.csv", "b1.csv"})
    prep = run.make_prep(run.make_config())
    # stack before download: DP1.A would be re-stacked from a partial input set -> refused
    out = prep(SITE, "stack")
    assert out is not None and out.status == "failed" and "DP1.A" in out.info and "DP1.B" not in out.info
    # download restores DP1.A only (DP1.B is fully evicted and unchanged)
    assert prep(SITE, "download") is None
    con = state.connect(state.DB_PATH)
    flags = dict(con.execute("SELECT name, local_evicted FROM site_files").fetchall())
    con.close()
    assert flags["a1.csv"] == 0 and flags["a2.csv"] == 0 and flags["b1.csv"] == 1
    assert "restoring 2 evicted file(s)" in (site_env / f"logs/run/{SITE}/download.log").read_text()


def test_prep_stack_nothing_local_is_ok_and_restack_needs_every_input(site_env: Path) -> None:  # noqa: F811
    _journal(site_env, [{"product": "DP1.A", "name": "a1.csv"}, {"product": "DP1.B", "name": "b1.csv"}],
             evicted={"a1.csv", "b1.csv"})
    out = run.make_prep(run.make_config())(SITE, "stack")
    assert out is not None and out.status == "ok" and "evicted" in out.info
    out = run.make_prep(run.make_config(restack=True))(SITE, "stack")
    assert out is not None and out.status == "failed" and "--restack" in out.info
    # restack marks every evicted file for re-download
    assert run.make_prep(run.make_config(restack=True))(SITE, "download") is None
    con = state.connect(state.DB_PATH)
    assert con.execute("SELECT sum(local_evicted) FROM site_files").fetchone()[0] == 0
    con.close()


def test_prep_stack_proceeds_normally_without_evictions(site_env: Path) -> None:  # noqa: F811
    _journal(site_env, [{"product": "DP1.A", "name": "a1.csv"}])
    assert run.make_prep(run.make_config())(SITE, "stack") is None


def test_prep_validate_without_proposals_and_record_lock(site_env: Path,  # noqa: F811
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    prep = run.make_prep(run.make_config())
    out = prep(SITE, "validate")
    assert out is not None and out.status == "ok" and "curation/generic" in out.info
    monkeypatch.setattr(env, "catalog_lock_holders", lambda: [{"pid": "4242", "cmd": "mesa-mcp --transport stdio"}])
    out = prep(SITE, "record")
    assert out is not None and out.status == "retry" and "pid 4242" in out.info  # MESA busy: retried later
    monkeypatch.setattr(env, "catalog_lock_holders", list)
    assert prep(SITE, "record") is None
    assert prep(SITE, "terms") is None


def test_download_bytes_counts_pending_and_missing_files(site_env: Path) -> None:  # noqa: F811
    _journal(site_env, [{"product": "DP1.A", "name": "a1.csv"}, {"product": "DP1.A", "name": "a2.csv"},
                        {"product": "DP1.A", "name": "a3.csv", "dl_status": None}], evicted={"a2.csv"})
    assert run.download_bytes(SITE) == 20
    assert run.download_bytes("NONE") == 0
    env.write_json(site_env / "sites/NONE/reports/plan.json", {"totals": {"bytes": 1234}})
    assert run.download_bytes("NONE") == 1234


# ---------------------------------------------------------------- site selection

def _fake_catalog() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE sites (siteCode VARCHAR, siteProperties MAP(VARCHAR, VARCHAR))")
    for code, hab in [("HARV", "Terrestrial"), ("SRER", "Terrestrial"), ("ARIK", "Aquatic Wadeable Stream"),
                      ("BARC", "Aquatic Lake"), ("FLNT", "Aquatic Non-wadeable Stream")]:
        con.execute("INSERT INTO sites VALUES (?, MAP {'HABITAT': ?, 'TYPEECO': 'x'})", [code, hab])
    con.execute("CREATE TABLE v_site_inventory (productCode VARCHAR, siteCode VARCHAR, month VARCHAR, "
                "isProvisional BOOLEAN, isReleaseOnly BOOLEAN, totalBytes BIGINT)")
    for code, gb in [("HARV", 80), ("SRER", 82), ("ARIK", 9), ("BARC", 8), ("FLNT", 5)]:
        con.execute("INSERT INTO v_site_inventory VALUES ('DP1.1', ?, '2024-01', false, false, ?)",
                    [code, gb * 10**9])
    return con


def test_site_universe_and_ordering() -> None:
    uni = run.site_universe(con=_fake_catalog())
    assert {r["site"]: r["habitat"] for r in uni}["BARC"] == "Aquatic Lake"
    order = [r["site"] for r in run.select_sites(None, all_sites=True, universe=uni)]
    assert order == ["FLNT", "BARC", "ARIK", "HARV", "SRER"]  # aquatic (small first), then terrestrial by size
    assert [r["site"] for r in run.select_sites(None, habitat="aquatic", universe=uni)] == ["FLNT", "BARC", "ARIK"]
    assert [r["site"] for r in run.select_sites(None, habitat="Terrestrial", universe=uni)] == ["HARV", "SRER"]
    assert [r["site"] for r in run.select_sites(None, habitat="aquatic lake", universe=uni)] == ["BARC"]
    assert [r["site"] for r in run.select_sites(["srer", "ARIK"], universe=uni)] == ["SRER", "ARIK"]
    assert [r["site"] for r in run.select_sites(["SRER", "ARIK"], order="size", universe=uni)] == ["ARIK", "SRER"]
    assert run.select_sites(None, all_sites=True, order="name", universe=uni)[0]["site"] == "ARIK"
    with pytest.raises(SystemExit, match="unknown site"):
        run.select_sites(["NOPE"], universe=uni)
    with pytest.raises(SystemExit, match="unknown habitat"):
        run.select_sites(None, habitat="marine", universe=uni)
    with pytest.raises(SystemExit, match="SITES"):
        run.select_sites(None, universe=uni)


def test_explicit_sites_work_without_a_catalog(site_env: Path) -> None:  # noqa: F811
    rows = run.select_sites(["SRER", "HARV"])  # the tmp root has no catalog lake
    assert [r["site"] for r in rows] == ["SRER", "HARV"]
    with pytest.raises(SystemExit, match="catalog lake missing"):
        run.select_sites(None, all_sites=True)


# ---------------------------------------------------------------- status

def test_status_table_totals_and_json(site_env: Path, capsys: pytest.CaptureFixture[str]) -> None:  # noqa: F811
    con = state.connect(state.DB_PATH)
    store = run.Store(con)
    store.set_pending("SRER", run.STAGES)
    for s in run.STAGES[:5]:
        store.start("SRER", s, f"logs/run/SRER/{s}.log", "full")
        store.finish("SRER", s, "ok", 0)
    store.start("SRER", "cards", "logs/run/SRER/cards.log", "full")
    store.finish("SRER", "cards", "failed", 2, "exit 2")
    for i, (dl, up) in enumerate([("ok", "present"), ("ok", "uploaded"), (None, None), ("superseded", None)]):
        con.execute("INSERT INTO site_files(site, product, month, name, size, dl_status, dl_bytes, up_status) "
                    "VALUES ('SRER','DP1.A','2024-05',?,?,?,?,?)", (f"f{i}", 10**9, dl, 10**9 if dl else None, up))
    for st in ("recorded", "recorded", "applied"):
        con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, op, status) "
                    "VALUES ('SRER','u','/p','data_object','neon.x','1','add',?)", (st,))
    con.execute("INSERT INTO batches(batch_id, scope, status) VALUES ('b1','SRER','recorded'), ('b2','catalog',"
                "'recorded'), ('b3','SRER','failed')")
    rep = run.collect_status(con)
    con.close()
    s = rep["sites"]["SRER"]
    assert s["files"] == 3 and s["downloaded"] == 2 and s["uploaded"] == 2 and s["bytes"] == 3 * 10**9
    assert s["avusRecorded"] == 2 and s["snapshots"] == 1
    assert rep["allScopes"]["snapshots"] == 2 and rep["allScopes"]["snapshotBudget"] == run.SNAPSHOT_BUDGET
    assert s["stages"]["cards"]["status"] == "failed" and s["stages"]["cards"]["info"] == "exit 2"
    assert rep["stageCounts"] == {"ok": 5, "failed": 1, "pending": 7}
    text = run.render_status(rep)
    assert "SRER" in text and " X" in text and "exit 2" in text and "legend" in text
    assert run.status_main() == 0
    out = capsys.readouterr().out
    assert "wrote logs/status.json" in out
    assert json.loads((site_env / "logs/status.json").read_text())["sites"]["SRER"]["files"] == 3
    assert run.status_main(["SRER"], as_json=True) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["files"] == 3


def test_status_counts_mesa_snapshots_not_spooled_batches(site_env: Path) -> None:  # noqa: F811
    """The spool recorder coalesces many batches (of several scopes) into one MESA snapshot: the snapshot budget
    line must count snapshots, as scripts/full_sweep.sh reads it against the 900 budget."""
    con = state.connect(state.DB_PATH)
    store = run.Store(con)
    for site in ("KONZ", "SRER"):
        store.set_pending(site, run.STAGES)
    con.executemany("INSERT INTO batches(batch_id, scope, status, snapshot_id) VALUES (?,?,?,?)",
                    [(f"k{i}", "KONZ", "recorded", 10) for i in range(57)]
                    + [("s1", "SRER", "recorded", 3), ("s2", "SRER", "recorded", 4), ("c1", "catalog", "recorded", 10),
                       ("x1", "KONZ", "spooled", None)])
    rep = run.collect_status(con)
    con.close()
    assert rep["sites"]["KONZ"]["snapshots"] == 1 and rep["sites"]["SRER"]["snapshots"] == 2
    assert rep["allScopes"]["snapshots"] == 3  # 3, 4 and 10 (shared by KONZ and catalog)


# ---------------------------------------------------------------- entry points

def test_main_dry_run_prints_plan_and_runs_nothing(site_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                                                   capsys: pytest.CaptureFixture[str]) -> None:
    def no_spawn(*a: Any, **k: Any) -> None:
        raise AssertionError("dry run must not spawn")

    monkeypatch.setattr(run.subprocess, "Popen", no_spawn)
    assert run.main(["SRER", "HARV"], dry_run=True, sample=2, stream=False) == 0
    out = capsys.readouterr().out
    assert "run plan: 2 site(s), mode sample:2" in out and "evict off" in out and "whole-site" in out
    assert "neon-ducklake site list SRER --sample 2" in out
    assert "neon-ducklake tag SRER --apply-only" in out
    con = state.connect(state.DB_PATH)
    assert con.execute("SELECT count(*) FROM site_runs").fetchone()[0] == 0
    con.close()


def test_main_runs_scheduler_with_real_wiring(site_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
                                              capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[tuple[str, str]] = []

    def fake_call(self: run.SubprocessRunner, site: str, stage: str) -> int:
        calls.append((site, stage))
        return 0

    monkeypatch.setattr(run.SubprocessRunner, "__call__", fake_call)
    monkeypatch.setattr(run, "make_prep", lambda cfg: (lambda site, stage: None))
    rc = run.main(["SRER"], stages="plan,list,verify", poll_s=0.01, stream=False)
    assert rc == 0
    assert calls == [("SRER", "plan"), ("SRER", "list"), ("SRER", "verify")]
    assert (site_env / "logs/status.json").exists()
    # a second invocation has nothing to do
    calls.clear()
    assert run.main(["SRER"], stages="plan,list,verify", poll_s=0.01, stream=False) == 0
    assert calls == []
    assert "nothing to do" in capsys.readouterr().out


def test_cli_wires_run_status_evict_and_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    got: dict[str, Any] = {}
    monkeypatch.setattr(run, "main", lambda sites, **kw: got.update(run=(sites, kw)) or 0)
    monkeypatch.setattr(run, "status_main", lambda sites, **kw: got.update(status=(sites, kw)) or 0)
    assert cli.main(["run", "SRER", "HARV", "--sample", "2", "--stages", "plan,list", "--no-evict",
                     "--max-sites-in-flight", "2", "--dry-run", "--order", "size"]) == 0
    sites, kw = got["run"]
    assert sites == ["SRER", "HARV"] and kw["sample"] == 2 and kw["stages"] == "plan,list"
    assert kw["evict"] is False and kw["max_in_flight"] == 2 and kw["dry_run"] is True and kw["order"] == "size"
    assert kw["stream"] is True and kw["chunk_bytes"] is None and kw["history_mode"] is None  # stream is the default
    assert cli.main(["run", "--habitat", "aquatic", "--from-stage", "upload", "--restack", "--no-stream"]) == 0
    assert got["run"][1]["habitat"] == "aquatic" and got["run"][1]["restack"] is True
    assert got["run"][1]["stream"] is False
    assert cli.main(["status", "--json"]) == 0 and got["status"] == ([], {"as_json": True})

    from neon_ducklake.site import evict, sample

    monkeypatch.setattr(evict, "main", lambda site, dry_run=False: got.update(evict=(site, dry_run)) or 0)
    monkeypatch.setattr(sample, "run_list", lambda site, n, **kw: got.update(list=(site, n, kw)) or 0)
    monkeypatch.setattr(sample, "run_download", lambda site, n, **kw: got.update(dl=(site, n, kw)) or 0)
    assert cli.main(["site", "evict", "srer", "--dry-run"]) == 0 and got["evict"] == ("SRER", True)
    assert cli.main(["site", "list", "ARIK", "--sample", "2", "--threads", "4"]) == 0
    assert got["list"] == ("ARIK", 2, {"products": None, "threads": 4})
    assert cli.main(["site", "download", "ARIK", "--sample", "3", "--products", "DP1.1"]) == 0
    assert got["dl"] == ("ARIK", 3, {"products": ["DP1.1"], "threads": 8})
    with pytest.raises(SystemExit, match="mutually exclusive"):
        cli.main(["site", "list", "ARIK", "--sample", "2", "--months", "2024-01"])
    with pytest.raises(SystemExit, match="--sample applies"):
        cli.main(["site", "stack", "ARIK", "--sample", "2"])
