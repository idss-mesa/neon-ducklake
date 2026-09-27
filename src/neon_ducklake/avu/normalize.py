"""AVU value normalisation, mirroring mesa-mcp's ``add_avu_to_irods`` rules.

``add_avu_to_irods`` strips attribute/value/unit and rejects an empty attribute or value. We do the
same (returning ``None`` instead of raising, so builders can simply drop empties), and additionally
cap values at 2,000 UTF-8 bytes (cut on a sentence, else word, boundary and marked with `` […]``) —
long text stays in the lakes. Units longer than 250 bytes are a hard error: a unit is a CURIE, URL or
short label, so an overlong one is a builder bug.
"""

from __future__ import annotations

import re
from typing import Any

MAX_VALUE_BYTES = 2000
MAX_UNIT_BYTES = 250
ELLIPSIS = " […]"
PROTECTED_PREFIX = "ipc"
PROTECTED_ATTRS = frozenset({"mesa.enabled"})

_SENTENCE_END = re.compile(r"[.!?;](?=\s)")

Triple = tuple[str, str, str]


class UnitTooLong(ValueError):
    """A unit exceeded MAX_UNIT_BYTES (hard error)."""


def is_protected(attribute: str) -> bool:
    """AVUs the bulk writer never adds or removes (CyVerse ``ipc*`` and the MESA marker)."""
    return attribute.startswith(PROTECTED_PREFIX) or attribute in PROTECTED_ATTRS


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def cap_value(value: str, limit: int = MAX_VALUE_BYTES) -> str:
    """Return ``value`` unchanged if it fits in ``limit`` UTF-8 bytes, else a boundary-cut prefix + `` […]``."""
    raw = value.encode("utf-8")
    if len(raw) <= limit:
        return value
    budget = limit - len(ELLIPSIS.encode("utf-8"))
    cut = raw[:budget].decode("utf-8", errors="ignore")
    floor = len(cut) // 2  # never throw away more than half the budget looking for a boundary
    ends = [m.end() for m in _SENTENCE_END.finditer(cut) if m.end() >= floor]
    if ends:
        cut = cut[: ends[-1]]
    else:
        ws = max(cut.rfind(" "), cut.rfind("\n"), cut.rfind("\t"))
        if ws >= floor:
            cut = cut[:ws]
    out = cut.rstrip() + ELLIPSIS
    assert len(out.encode("utf-8")) <= limit
    return out


def normalize(attribute: Any, value: Any, unit: Any = "") -> Triple | None:
    """Normalise one AVU; ``None`` when attribute or value is empty after stripping.

    Raises :class:`UnitTooLong` when the unit exceeds 250 UTF-8 bytes.
    """
    a = _as_text(attribute).strip()
    v = _as_text(value).strip()
    u = _as_text(unit).strip()
    if not a or not v:
        return None
    if len(u.encode("utf-8")) > MAX_UNIT_BYTES:
        raise UnitTooLong(f"unit for {a!r} is {len(u.encode('utf-8'))} bytes (> {MAX_UNIT_BYTES}): {u[:60]!r}…")
    return a, cap_value(v), u


def normalize_many(avus: list[tuple[Any, ...]]) -> list[Triple]:
    """Normalise ``(a, v[, u])`` tuples, dropping empties and exact duplicates (order kept)."""
    out: list[Triple] = []
    seen: set[Triple] = set()
    for t in avus:
        a, v = t[0], t[1]
        u = t[2] if len(t) > 2 else ""
        n = normalize(a, v, u)
        if n is not None and n not in seen:
            seen.add(n)
            out.append(n)
    return out
