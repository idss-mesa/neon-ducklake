"""The one NEON file-name classifier shared by the catalog flatten, site fetch, stack and AVU builders.

:func:`classify` wraps ``neon_mcp.neon.filenames.parse_neon_filename`` and adds the external-lab grammar
that parser reports as ``other``::

    NEON.<Lab_Name>.<table>.<YYYYMMDDTHHMMSSZ>.<ext>     e.g. NEON.NEON_HQ.mct_taxonTableLabSummary.20251206T221502Z.csv

-> kind ``lab``, ``lab_name`` = ``<Lab_Name>``, ``table``, ``timestamp``, ``ext``. Lab files carry no
domain, site, product or month by design.

``product_hint`` is the product the file was listed under (a DP1 code). Supporting files published under
a ``DP0.*`` code (``NEON.D10.ARIK.DP0.10003.001.validation...``) and lab files report ``product_code =
product_hint``; without a hint the parsed code is kept (``DP0.*`` for DP0 files, ``None`` for lab files).

It never raises: unknown shapes are kind ``other``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

KINDS: frozenset[str] = frozenset({"data", "variables", "readme", "sensor_positions", "eml", "science_review_flags",
                                   "categorical_codes", "validation", "lab", "package", "other"})

# Supporting files NEON regenerates in place (same name, new bytes) without always refreshing the md5 it
# lists in /data/query (probe finding F1). Their NEON md5 is advisory; the storage md5 is authoritative.
REGENERATED_KINDS: frozenset[str] = frozenset({"variables", "sensor_positions", "readme", "eml", "categorical_codes",
                                               "validation", "science_review_flags"})

LAB_FILE_RE = re.compile(r"^NEON\.(?P<lab>[A-Za-z0-9_-]+)\.(?P<table>[A-Za-z0-9_]+)\.(?P<ts>\d{8}T\d{6}Z)\."
                         r"(?P<ext>[A-Za-z0-9]+)$")


@dataclass(frozen=True)
class Classified:
    kind: str
    table: str | None = None
    hor: str | None = None
    ver: str | None = None
    tmi: str | None = None
    domain: str | None = None
    site: str | None = None
    product_code: str | None = None
    month: str | None = None
    package: str | None = None
    timestamp: str | None = None
    ext: str | None = None
    lab_name: str | None = None


def basename(name: str) -> str:
    return (name or "").strip().rsplit("/", 1)[-1]


# IS data names whose temporal index is alphanumeric (e.g. DP4.00001.001 ``01D`` daily / ``01M`` monthly):
# neon-mcp's parser only accepts three digits for HOR/VER/TMI and would take ``000`` as the table name.
ALNUM_TMI_RE = re.compile(
    r"^NEON\.(?P<domain>D\d{2})\.(?P<site>[A-Z]{4})\.(?P<product>DP\d\.\d{5}\.\d{3})\.(?P<hor>\d{3})\.(?P<ver>\d{3})\."
    r"(?P<tmi>[0-9A-Z]{3})\.(?P<table>[^.]+)\.(?P<month>\d{4}-\d{2})\.(?P<package>basic|expanded)\."
    r"(?P<ts>\d{8}T\d{6}Z)\.(?P<ext>\w+)$")


def classify(name: str, product_hint: str | None = None) -> Classified:
    """Kind and name parts of one NEON file name (see the module docstring)."""
    from neon_mcp.neon.filenames import parse_neon_filename

    m = ALNUM_TMI_RE.match(basename(name))
    if m and not m["tmi"].isdigit():
        code = product_hint if (product_hint and m["product"].startswith("DP0.")) else m["product"]
        return Classified(kind="data", table=m["table"], hor=m["hor"], ver=m["ver"], tmi=m["tmi"],
                          domain=m["domain"], site=m["site"], product_code=code, month=m["month"],
                          package=m["package"], timestamp=m["ts"], ext=m["ext"].lower())
    p = parse_neon_filename(name)
    if p.kind == "other":
        m = LAB_FILE_RE.match(basename(name))
        if m:
            return Classified(kind="lab", table=m["table"], timestamp=m["ts"], ext=m["ext"].lower(),
                              lab_name=m["lab"], product_code=product_hint)
    code = p.product_code
    if product_hint and code and code.startswith("DP0."):
        code = product_hint
    return Classified(kind=p.kind, table=p.table, hor=p.hor, ver=p.ver, tmi=p.tmi, domain=p.domain, site=p.site,
                      product_code=code, month=p.month, package=p.package, timestamp=p.timestamp, ext=p.ext)


def is_regenerated(kind: str | None) -> bool:
    """A supporting kind whose NEON-listed md5 may be stale (verify it against storage instead)."""
    return (kind or "") in REGENERATED_KINDS
