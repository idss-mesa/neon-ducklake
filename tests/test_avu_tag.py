from __future__ import annotations

import sys
import types

import pytest

from neon_ducklake import state
from neon_ducklake.avu import bulk, history, tag, vocab


@pytest.fixture
def calls(tmp_path, monkeypatch):
    seen: list[tuple] = []
    con = state.connect(tmp_path / "s.sqlite")
    monkeypatch.setattr(state, "connect", lambda *a, **k: con)
    monkeypatch.setattr(vocab, "build_catalog_desired", lambda rid, con=None: seen.append(("build", rid)) or {"rows": 1})
    monkeypatch.setattr(bulk, "apply_scope", lambda scope, **k: seen.append(("apply", scope, k["dry_run"],
                                                                               k["threads"], k["rate"])) or
                        {"adds": 1, "errors": 0})
    monkeypatch.setattr(history, "record_scope", lambda scope, **k: seen.append(("record", scope)) or
                        {"batches_recorded": 1, "blocked_rows": 0})
    return seen


def test_catalog_full_flow(calls) -> None:
    assert tag.main("catalog", threads=4, rate=10.0) == 0
    assert calls == [("build", None), ("apply", "catalog", False, 4, 10.0), ("record", "catalog")]


def test_dry_run_and_apply_only_skip_recording(calls) -> None:
    tag.main("catalog", dry_run=True)
    tag.main("catalog", apply_only=True)
    assert [c[0] for c in calls] == ["build", "apply", "build", "apply"]
    assert calls[1][2] is True


def test_record_only(calls) -> None:
    tag.main("catalog", record_only=True)
    assert calls == [("record", "catalog")]


def test_site_scope_uses_lazy_builder(calls, monkeypatch) -> None:
    got = {}
    mod = types.ModuleType("neon_ducklake.site.avus")
    mod.build_desired = lambda site, products, months: got.update(site=site, products=products, months=months) or {}
    monkeypatch.setitem(sys.modules, "neon_ducklake.site.avus", mod)
    import neon_ducklake.site as site_pkg

    monkeypatch.setattr(site_pkg, "avus", mod, raising=False)
    tag.main("srer", products="DP1.00002.001, ", months="2024-06,2024-07")
    assert got == {"site": "SRER", "products": ["DP1.00002.001"], "months": ["2024-06", "2024-07"]}
    assert ("apply", "SRER", False, 8, 20.0) in calls and ("record", "SRER") in calls


def test_missing_site_builder_is_a_clear_error(calls, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "neon_ducklake.site.avus", None)  # import fails
    with pytest.raises(SystemExit, match="site AVU builder unavailable"):
        tag.main("HARV")


def test_errors_give_nonzero_exit(calls, monkeypatch) -> None:
    monkeypatch.setattr(bulk, "apply_scope", lambda scope, **k: {"errors": 2})
    assert tag.main("catalog") == 1
    with pytest.raises(SystemExit):
        tag.main("catalog", apply_only=True, record_only=True)
