"""site/evict.py: a raw file is deleted only when every safety rule holds; nothing else is ever touched."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import env, state
from neon_ducklake.site import evict

from .test_site_common import SITE, md5, site_env  # noqa: F401 - fixture

P = "DP1.00002.001"


class Site:
    """A tmp site with journal rows, files on disk, AVU journal rows and a verify report."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.con = state.connect(state.DB_PATH)
        from neon_ducklake.site.fetch import SUPERSEDED_SCHEMA

        self.con.executescript(SUPERSEDED_SCHEMA)
        self.verified_at = env.utcnow()

    def rel(self, name: str, month: str = "2024-05", product: str = P) -> str:
        return f"sites/{SITE}/raw/{product}/{month}/{name}"

    def add(self, name: str, *, kind: str = "data", month: str = "2024-05", product: str = P,
            dl_status: str | None = "ok", up_status: str | None = "present", checksum: str | None = "same",
            recorded: str | None = "recorded", on_disk: bool = True, dl_bytes: int | None = None,
            uploaded_delta: float = -60.0) -> Path:
        body = f"content of {name}".encode()
        rel = self.rel(name, month, product)
        path = self.root / rel
        if on_disk:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        digest = md5(body)
        irods = f"{env.IRODS_ROOT}/{rel}"
        chk = {"same": f"md5:{digest.upper()}", "other": "0" * 32, None: None}[checksum]
        up_at = (self.verified_at + timedelta(seconds=uploaded_delta)).isoformat()
        self.con.execute(
            "INSERT INTO site_files(site, product, month, name, kind, size, local_path, dl_status, dl_md5, dl_bytes,"
            " irods_path, up_status, irods_checksum, uploaded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (SITE, product, month, name, kind, len(body), rel, dl_status, digest,
             len(body) if dl_bytes is None else dl_bytes, irods, up_status, chk, up_at))
        if recorded:
            self.avu(irods, "neon.productCode", recorded)
            self.avu(irods, "neon.siteCode", "recorded")
        return path

    def avu(self, path: str, attribute: str, status: str, op: str = "add") -> None:
        self.con.execute("INSERT INTO avu_journal(scope, unit_id, path, target_type, attribute, value, op, status) "
                         "VALUES (?,?,?,?,?,?,?,?)", (SITE, "u", path, "data_object", attribute, P, op, status))

    def verify(self, *, ok: bool = True, irods: bool = True, site: str = SITE, filters: dict | None = None,
               fail_check: bool = False) -> None:
        checks = [{"name": "local files", "status": "pass"}, {"name": "irods objects", "status": "pass"}]
        if fail_check:
            checks.append({"name": "avu core", "status": "fail"})
        env.write_json(self.root / "sites" / SITE / "reports" / "verify.json", {
            "site": site, "generatedAt": self.verified_at.isoformat(), "irods": irods,
            "filters": filters or {"products": None, "months": None},
            "summary": {"pass": 2, **({"fail": 1} if fail_check else {})}, "ok": ok, "checks": checks})

    def flags(self) -> dict[str, int]:
        return {r[0]: r[1] for r in self.con.execute("SELECT name, coalesce(local_evicted, 0) FROM site_files")}


@pytest.fixture
def site(site_env: Path) -> Site:  # noqa: F811
    s = Site(site_env)
    yield s
    s.con.close()


def _protected(root: Path) -> list[Path]:
    """Files evict must never delete."""
    out = []
    for rel in (f"sites/{SITE}/ducklake/neon_test.ducklake", f"sites/{SITE}/ducklake/data/main/t/x.parquet",
                f"sites/{SITE}/cards/{P}.md", f"sites/{SITE}/curation/review.md", f"sites/{SITE}/reports/plan.json",
                "ref/ontology_maps/taxa.parquet", f"sites/{SITE}/tmp/stage.duckdb",
                f"sites/{SITE}/raw/{P}/2024-05/unjournaled.csv", f"sites/{SITE}/raw/{P}/2024-05/partial.csv.part"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"keep me")
        out.append(p)
    return out


def test_evicts_only_when_every_rule_holds(site: Site, capsys: pytest.CaptureFixture[str]) -> None:
    good = site.add("good.csv")
    good2 = site.add("good2.csv", month="2024-06")
    protected = _protected(site.root)
    site.verify()
    assert evict.main(SITE) == 0
    assert not good.exists() and not good2.exists()
    assert all(p.exists() for p in protected)
    assert site.flags() == {"good.csv": 1, "good2.csv": 1}
    log_rows = site.con.execute("SELECT site, local_path, bytes FROM evict_log ORDER BY local_path").fetchall()
    assert [tuple(r) for r in log_rows] == [(SITE, site.rel("good.csv"), len(b"content of good.csv")),
                                            (SITE, site.rel("good2.csv", "2024-06"), len(b"content of good2.csv"))]
    out = capsys.readouterr().out
    assert "evicted 2 file(s), freed" in out
    assert not good2.parent.exists()  # empty month directory pruned
    summary = json.loads((site.root / "logs" / f"evict_summary_{SITE}.json").read_text())
    assert summary["evicted"] == 2 and summary["bytesFreed"] > 0
    # idempotent: a second run deletes nothing more and reports them as already evicted
    assert evict.main(SITE) == 0
    assert json.loads((site.root / "logs" / f"evict_summary_{SITE}.json").read_text())["alreadyEvicted"] == 2


FAILING_CASES: dict[str, dict[str, Any]] = {
    "not downloaded ok": {"dl_status": "error"},
    "stale download": {"dl_status": "stale"},
    "not uploaded": {"up_status": None},
    "upload failed": {"up_status": "error"},
    "checksum differs": {"checksum": "other"},
    "no irods checksum": {"checksum": None},
    "avus only applied": {"recorded": "applied"},
    "avus noop": {"recorded": "noop"},
    "no avus": {"recorded": None},
    "size changed on disk": {"dl_bytes": 3},
    "uploaded after verify": {"uploaded_delta": 60.0},
    "variables kept": {"kind": "variables"},
}


@pytest.mark.parametrize("case", sorted(FAILING_CASES))
def test_never_deletes_when_a_rule_fails(site: Site, case: str) -> None:
    good = site.add("good.csv")
    bad = site.add("bad.csv", **FAILING_CASES[case])
    site.verify()
    rc = evict.main(SITE)
    assert bad.exists(), case
    assert site.flags()["bad.csv"] == 0
    assert not good.exists()  # the good file still goes
    kept_ok = case in ("variables kept", "not downloaded ok", "stale download")
    assert rc == (0 if kept_ok else 3), case


def test_recorded_then_removed_avu_blocks(site: Site) -> None:
    f = site.add("x.csv")
    site.avu(f"{env.IRODS_ROOT}/{site.rel('x.csv')}", "neon.productCode", "applied", op="remove")
    site.verify()
    assert evict.main(SITE) == 3 and f.exists()


def test_superseded_and_symlinked_files_stay(site: Site) -> None:
    sup = site.add("old.csv")
    site.con.execute("INSERT INTO fetch_superseded VALUES (?,?,?,?,?,?)", (SITE, P, "2024-05", "old.csv", 1, "x"))
    target = site.root / "elsewhere.csv"
    target.write_bytes(b"content of link.csv")
    link = site.add("link.csv", on_disk=False)
    link.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(target, link)
    site.verify()
    assert evict.main(SITE) == 3  # the symlink is blocked
    assert sup.exists() and link.is_symlink() and target.exists()


@pytest.mark.parametrize("gate", ["missing", "not ok", "failed check", "no irods", "other site"])
def test_verify_gate_closed_deletes_nothing(site: Site, gate: str, capsys: pytest.CaptureFixture[str]) -> None:
    f = site.add("good.csv")
    if gate == "not ok":
        site.verify(ok=False)
    elif gate == "failed check":
        site.verify(fail_check=True, ok=True)
    elif gate == "no irods":
        site.verify(irods=False)
    elif gate == "other site":
        site.verify(site="HARV")
    assert evict.main(SITE) == 1
    assert f.exists() and "nothing deleted" in capsys.readouterr().out
    assert not evict.has_evict_column(site.con)  # a closed gate writes nothing at all


def test_verify_filters_limit_the_scope(site: Site) -> None:
    inside = site.add("in.csv", month="2024-05")
    outside = site.add("out.csv", month="2024-06")
    other_product = site.add("p.csv", product="DP1.10003.001")
    site.verify(filters={"products": [P], "months": ["2024-05"]})
    assert evict.main(SITE) == 3
    assert not inside.exists() and outside.exists() and other_product.exists()


def test_dry_run_changes_nothing(site: Site, capsys: pytest.CaptureFixture[str]) -> None:
    f = site.add("good.csv")
    site.verify()
    assert evict.main(SITE, dry_run=True) == 0
    assert f.exists()
    assert "would free" in capsys.readouterr().out
    assert not evict.has_evict_column(site.con)  # not even the schema is touched
    assert not (site.root / "logs" / f"evict_summary_{SITE}.json").exists()


def test_flagged_file_back_on_disk_is_reflagged_and_evaluated(site: Site) -> None:
    f = site.add("good.csv")
    blocked = site.add("blocked.csv", up_status=None)
    evict.ensure_schema(site.con)
    site.con.execute("UPDATE site_files SET local_evicted=1")  # e.g. crash before the unlink, or a re-download
    site.verify()
    assert evict.main(SITE) == 3
    assert not f.exists() and blocked.exists()
    assert site.flags() == {"good.csv": 1, "blocked.csv": 0}


def test_ensure_schema_is_idempotent(site: Site) -> None:
    evict.ensure_schema(site.con)
    evict.ensure_schema(site.con)
    cols = [r[1] for r in site.con.execute("PRAGMA table_info(site_files)")]
    assert cols.count("local_evicted") == 1
    assert site.con.execute("SELECT count(*) FROM sqlite_master WHERE name='evict_log'").fetchone()[0] == 1


def test_lock_held_by_a_fetch_blocks_eviction(site: Site) -> None:
    f = site.add("good.csv")
    site.verify()
    with env.file_lock(f"site-fetch-{SITE}"), pytest.raises(SystemExit, match="site-fetch"):
        evict.main(SITE)
    assert f.exists()


def test_restore_helpers(site: Site) -> None:
    a1 = site.add("a1.csv")
    site.add("a.variables.csv", kind="variables")
    site.add("b1.csv", product="DP1.10003.001")
    site.add("b.variables.csv", product="DP1.10003.001", kind="variables")
    site.verify()
    assert evict.main(SITE) == 0
    assert not a1.exists()
    st = evict.input_state(site.con, SITE)
    assert st == {"local": 2, "missing": 2, "evicted": 2, "notDownloaded": 0}
    assert evict.restore_candidates(site.con, SITE) == {}  # fully evicted + unchanged: stack skips them
    assert evict.pending_bytes(site.con, SITE) == len(b"content of a1.csv") + len(b"content of b1.csv")
    # a new month of DP1.00002 is listed: that product must be restored before a re-stack
    site.add("a2.csv", month="2024-07", dl_status=None, on_disk=False, recorded=None)
    assert evict.restore_candidates(site.con, SITE) == {P: 1}
    assert evict.mark_restore(site.con, SITE, [P]) == (1, len(b"content of a1.csv"))
    assert site.flags()["a1.csv"] == 0 and site.flags()["b1.csv"] == 1
    assert evict.mark_restore(site.con, SITE, []) == (0, 0)
    assert evict.mark_restore(site.con, SITE) == (1, len(b"content of b1.csv"))
    assert evict.evicted_totals(site.con) == {}
