"""site/h5.py: NEON eddy4R HDF5 reading, alignment and graceful degradation."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from neon_ducklake.site import h5

from .test_site_common import SITE
from .test_site_stack import make_h5


def test_read_h5_columns_units_and_missing(tmp_path: Path) -> None:
    p = tmp_path / "a.h5"
    make_h5(p)
    r = h5.read_h5(str(p), SITE)
    assert r.columns["timeBgn"].dtype == np.dtype("datetime64[ms]")
    assert str(r.columns["timeBgn"][1]) == "2024-06-01T00:30:00.000"
    assert str(r.columns["timeEnd"][0]) == "2024-06-01T00:29:59.950"
    assert np.isnan(r.columns["fluxCo2Turb"][1]) and list(r.columns["qfFinlCo2Turb"]) == [0, 1, 0]
    assert list(r.columns["footDistXaxs90"]) == [100.0, 110.0, 120.0]
    assert r.units["fluxCo2Turb"] == "umolCo2 m-2 s-1"
    assert f"/{SITE}/dp04/data/fluxH2o/turb" in r.missing  # absent paths are reported, not fatal
    assert {a["attr"] for a in r.attrs} == {"SciVer", "LatTow", "DistZaxsLvlMeasTow"}
    assert any(d["path"] == f"/{SITE}/dp04/qfqm/foot/turb" and d["fields"] == ["timeBgn", "timeEnd", "qfFinl"]
               for d in r.datasets)


def test_read_h5_without_dp04_registers_only(tmp_path: Path) -> None:
    p = tmp_path / "b.h5"
    make_h5(p, with_dp04=False)
    r = h5.read_h5(str(p), SITE)
    assert r.columns == {} and r.missing == [f"/{SITE}/dp04"]
    assert [d["path"] for d in r.datasets] == ["/readMe"]


def test_misaligned_time_axes_merge_by_key(tmp_path: Path) -> None:
    import h5py

    p = tmp_path / "c.h5"
    t = np.dtype([("timeBgn", "S24"), ("timeEnd", "S24"), ("flux", "<f8")])
    q = np.dtype([("timeBgn", "S24"), ("timeEnd", "S24"), ("qfFinl", "<i4")])
    a, b = b"2024-06-01T00:00:00.000Z", b"2024-06-01T00:30:00.000Z"
    with h5py.File(p, "w") as f:
        g = f.create_group(SITE)
        g.create_dataset("dp04/data/fluxCo2/turb", data=np.array([(a, a, 1.0), (b, b, 2.0)], dtype=t))
        g.create_dataset("dp04/qfqm/fluxCo2/turb", data=np.array([(b, b, 1)], dtype=q))
    r = h5.read_h5(str(p), SITE)
    assert list(r.columns["fluxCo2Turb"]) == [1.0, 2.0]
    assert np.isnan(r.columns["qfFinlCo2Turb"][0]) and r.columns["qfFinlCo2Turb"][1] == 1
