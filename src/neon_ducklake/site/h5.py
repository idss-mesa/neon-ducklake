"""DP4.00200.001 (eddy-covariance bundle) HDF5 -> site lake, called from site/stack.py.

Per monthly ``nsae`` .h5 file:
  * ``h5_attrs``    (productCode, sourceFile, month, path, attr, value) for the root and ``/<SITE>`` groups;
  * ``h5_datasets`` (productCode, sourceFile, month, path, shape, nRows, dtype, fields, unit) for every dataset;
  * ``dp4_00200_001__nsae_dp04``: one row per 30-min window (timeBgn/timeEnd) with the dp04 fluxes
    ``/<SITE>/dp04/data/<flux>/<level>/flux`` (turb, plus nsae/stor when present), the footprint
    statistics ``/<SITE>/dp04/data/foot/stat`` and the matching ``/<SITE>/dp04/qfqm/.../qfFinl`` flags;
  * ``file_stats`` rows (rowCount, timeMin/timeMax on timeBgn, qfFailPct over the turb qfFinl flags).

A file whose dp04 paths are missing is still registered (attrs, datasets, file_stats with rowCount NULL).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import numpy as np

log = logging.getLogger("neon_ducklake.site.h5")

FLUXES = ("fluxCo2", "fluxH2o", "fluxTemp")
LEVELS = ("turb", "nsae", "stor")
LAKE_TABLE = "dp4_00200_001__nsae_dp04"
NEON_TABLE = "nsae"


@dataclass
class H5Result:
    attrs: list[dict[str, Any]]
    datasets: list[dict[str, Any]]
    columns: dict[str, np.ndarray]  # dp04 columns (timeBgn/timeEnd as datetime64[ms]) or {} when missing
    units: dict[str, str]
    missing: list[str]


def _text(value: Any) -> str:
    """Attribute value -> text (byte strings decoded, arrays comma-joined)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, np.ndarray):
        return ",".join(_text(v) for v in value.tolist())
    if isinstance(value, (list, tuple)):
        return ",".join(_text(v) for v in value)
    return str(value)


def _times(raw: np.ndarray) -> np.ndarray:
    """NEON ``b'2024-06-01T00:00:00.000Z'`` strings -> datetime64[ms] (UTC)."""
    txt = np.char.rstrip(np.char.decode(raw.astype("S"), "ascii"), "Z")
    return txt.astype("datetime64[ms]")


def _site_group(f: Any, site: str) -> Any | None:
    if site in f:
        return f[site]
    for key in f:
        if key not in ("objDesc", "readMe") and hasattr(f[key], "keys"):
            return f[key]
    return None


def read_h5(path: str, site: str) -> H5Result:
    import h5py

    attrs: list[dict[str, Any]] = []
    datasets: list[dict[str, Any]] = []
    columns: dict[str, np.ndarray] = {}
    units: dict[str, str] = {}
    missing: list[str] = []
    with h5py.File(path, "r") as f:
        for k, v in f.attrs.items():
            attrs.append({"path": "/", "attr": k, "value": _text(v)})
        grp = _site_group(f, site)
        if grp is not None:
            for k, v in grp.attrs.items():
                attrs.append({"path": grp.name, "attr": k, "value": _text(v)})

        def visit(name: str, obj: Any) -> None:
            if isinstance(obj, h5py.Dataset):
                unit = obj.attrs.get("unit")
                datasets.append({"path": "/" + name, "shape": str(tuple(obj.shape)),
                                 "nRows": int(obj.shape[0]) if obj.shape else None, "dtype": str(obj.dtype),
                                 "fields": list(obj.dtype.names) if obj.dtype.names else None,
                                 "unit": _text(unit) if unit is not None else None})
        f.visititems(visit)
        if grp is None or "dp04" not in grp:
            missing.append(f"/{site}/dp04")
            return H5Result(attrs, datasets, columns, units, missing)
        base: np.ndarray | None = None
        parts: list[tuple[str, np.ndarray, np.ndarray]] = []  # (column, timeBgn, values)
        time_end: dict[bytes, bytes] = {}

        def take(dpath: str, field: str, col: str) -> None:
            nonlocal base
            full = f"dp04/{dpath}"
            if full not in grp:
                missing.append(f"/{site}/{full}")
                return
            ds = grp[full][()]
            if ds.dtype.names is None or field not in ds.dtype.names or "timeBgn" not in ds.dtype.names:
                missing.append(f"/{site}/{full}:{field}")
                return
            tb = ds["timeBgn"]
            if base is None:
                base = tb
            if "timeEnd" in ds.dtype.names:
                time_end.update(zip(tb.tolist(), ds["timeEnd"].tolist(), strict=True))
            unit = grp[full].attrs.get("unit")
            if unit is not None:
                try:
                    units[col] = _text(np.asarray(unit)[list(ds.dtype.names).index(field)])
                except (IndexError, ValueError):
                    units[col] = _text(unit)
            parts.append((col, tb, ds[field]))

        for flux in FLUXES:
            short = flux[4:]  # Co2 / H2o / Temp
            for level in LEVELS:
                take(f"data/{flux}/{level}", "flux", f"{flux}{level.capitalize()}")
                take(f"qfqm/{flux}/{level}", "qfFinl", f"qfFinl{short}{level.capitalize()}")
        foot = "dp04/data/foot/stat"
        if foot in grp and grp[foot].dtype.names:
            for field in grp[foot].dtype.names:
                if field not in ("timeBgn", "timeEnd"):
                    take("data/foot/stat", field, "foot" + field[0].upper() + field[1:])
        else:
            missing.append(f"/{site}/{foot}")
        take("qfqm/foot/turb", "qfFinl", "qfFinlFoot")
        if base is None:
            return H5Result(attrs, datasets, columns, units, missing)
        # align every part on timeBgn (identical axes in practice; merge by key otherwise)
        keys = list(dict.fromkeys(k for _c, tb, _v in parts for k in tb.tolist()))
        aligned = all(len(tb) == len(keys) and tb.tolist() == keys for _c, tb, _v in parts)
        key_arr = np.array(keys, dtype=base.dtype)
        columns["timeBgn"] = _times(key_arr)
        columns["timeEnd"] = _times(np.array([time_end.get(k, k) for k in keys], dtype=base.dtype))
        for col, tb, vals in parts:
            if aligned:
                columns[col] = np.asarray(vals)
            else:
                pos = {k: i for i, k in enumerate(tb.tolist())}
                out = np.full(len(keys), np.nan, dtype="float64")
                for i, k in enumerate(keys):
                    j = pos.get(k)
                    if j is not None:
                        out[i] = vals[j]
                columns[col] = out
    return H5Result(attrs, datasets, columns, units, missing)


def _ts_ms(arr: np.ndarray) -> Any:
    import pyarrow as pa

    return pa.array(arr.astype("datetime64[ms]"), type=pa.timestamp("ms")).cast(pa.timestamp("ms", tz="UTC"))


def build_tables(site: str, domain: str | None, product: str, files: Iterable[Any]) -> dict[str, Any]:
    """Read every .h5 file -> pyarrow tables for h5_attrs, h5_datasets, the dp04 table and file_stats."""
    import pyarrow as pa

    attrs_rows: list[dict] = []
    ds_rows: list[dict] = []
    stats_rows: list[dict] = []
    flux_tables: list[Any] = []
    missing: dict[str, list[str]] = {}
    units: dict[str, str] = {}
    for sf in files:
        res = read_h5(str(sf.path), site)
        for a in res.attrs:
            attrs_rows.append({"productCode": product, "sourceFile": sf.name, "month": sf.month, **a})
        for d in res.datasets:
            ds_rows.append({"productCode": product, "sourceFile": sf.name, "month": sf.month, **d})
        if res.missing:
            missing[sf.name] = res.missing
        units.update(res.units)
        stat: dict[str, Any] = {"sourceFile": sf.name, "productCode": product, "tableName": NEON_TABLE,
                                "lakeTable": LAKE_TABLE, "month": sf.month, "rowCount": None, "timeMin": None,
                                "timeMax": None, "timeColumn": "timeBgn", "qfFailPct": None}
        if res.columns:
            n = len(res.columns["timeBgn"])
            cols: dict[str, Any] = {
                "siteID": pa.array([site] * n), "domainID": pa.array([domain] * n),
                "month": pa.array([sf.month] * n), "release": pa.array([sf.release] * n),
                "publicationDate": pa.array([sf.pub_ts] * n, type=pa.timestamp("us", tz="UTC")),
                "sourceFile": pa.array([sf.name] * n),
                "timeBgn": _ts_ms(res.columns["timeBgn"]), "timeEnd": _ts_ms(res.columns["timeEnd"]),
            }
            for col, vals in res.columns.items():
                if col in ("timeBgn", "timeEnd"):
                    continue
                if col.startswith("qfFinl"):
                    v = np.asarray(vals, dtype="float64")
                    cols[col] = pa.array(np.where(np.isnan(v), 0, v).astype("int64"), mask=np.isnan(v))
                else:
                    cols[col] = pa.array(np.asarray(vals, dtype="float64"), from_pandas=True)
            flux_tables.append(pa.table(cols))
            qf = [np.asarray(res.columns[c], dtype="float64") for c in res.columns
                  if c.startswith("qfFinl") and c.endswith("Turb")]
            qf_all = np.concatenate(qf) if qf else np.array([])
            qf_all = qf_all[~np.isnan(qf_all)]
            tb = res.columns["timeBgn"]
            stat.update({"rowCount": n,
                         "timeMin": _as_utc(tb.min()) if n else None, "timeMax": _as_utc(tb.max()) if n else None,
                         "qfFailPct": float(100.0 * (qf_all == 1).sum() / len(qf_all)) if len(qf_all) else None})
        stats_rows.append(stat)
    flux = pa.concat_tables(flux_tables, promote_options="default") if flux_tables else None
    return {"h5_attrs": pa.Table.from_pylist(attrs_rows) if attrs_rows else None,
            "h5_datasets": pa.Table.from_pylist(ds_rows, schema=_ds_schema()) if ds_rows else None,
            "flux": flux, "stats": stats_rows, "missing": missing, "units": units}


def _as_utc(v: np.datetime64) -> datetime:
    from datetime import UTC

    return datetime.fromtimestamp(v.astype("datetime64[ms]").astype("int64") / 1000, tz=UTC)


def _ds_schema() -> Any:
    import pyarrow as pa

    return pa.schema([("productCode", pa.string()), ("sourceFile", pa.string()), ("month", pa.string()),
                      ("path", pa.string()), ("shape", pa.string()), ("nRows", pa.int64()), ("dtype", pa.string()),
                      ("fields", pa.list_(pa.string())), ("unit", pa.string())])
