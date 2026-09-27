from __future__ import annotations

import pytest

from neon_ducklake.avu.normalize import (
    ELLIPSIS,
    MAX_VALUE_BYTES,
    UnitTooLong,
    cap_value,
    is_protected,
    normalize,
    normalize_many,
)


def test_strip_and_drop_empties() -> None:
    assert normalize("  neon.siteCode ", " SRER\n", " ") == ("neon.siteCode", "SRER", "")
    assert normalize("", "x") is None
    assert normalize("a", "   ") is None
    assert normalize("a", None) is None
    assert normalize(None, "x") is None


def test_scalars_to_text() -> None:
    assert normalize("neon.releaseOnly", True) == ("neon.releaseOnly", "true", "")
    assert normalize("neon.rowCount", 12, "count") == ("neon.rowCount", "12", "count")
    assert normalize("neon.rowCount", 0) == ("neon.rowCount", "0", "")


def test_unit_limit_is_hard_error() -> None:
    normalize("a", "v", "u" * 250)
    with pytest.raises(UnitTooLong):
        normalize("a", "v", "u" * 251)
    with pytest.raises(UnitTooLong):
        normalize("a", "v", "é" * 126)  # 252 bytes


def test_short_value_untouched() -> None:
    v = "x" * MAX_VALUE_BYTES
    assert cap_value(v) == v


def test_cap_on_sentence_boundary() -> None:
    sentence = "This is a sentence about NEON data. "
    v = sentence * 100
    out = cap_value(v)
    assert len(out.encode()) <= MAX_VALUE_BYTES
    assert out.endswith("data." + ELLIPSIS)
    assert v.startswith(out[: -len(ELLIPSIS)])


def test_cap_on_word_boundary_and_multibyte() -> None:
    v = "Ωmega " * 600  # 7 bytes per word, no sentence ends
    out = cap_value(v)
    raw = out.encode()
    assert len(raw) <= MAX_VALUE_BYTES
    assert out.endswith("Ωmega" + ELLIPSIS)
    raw.decode("utf-8")  # never split a code point


def test_cap_without_any_boundary() -> None:
    v = "é" * 1500  # 3000 bytes, no whitespace
    out = cap_value(v)
    assert len(out.encode()) <= MAX_VALUE_BYTES
    assert out.endswith(ELLIPSIS)


def test_normalize_many_dedupes_and_keeps_order() -> None:
    out = normalize_many([("b", "1"), ("a", "2", "u"), (" b", "1 "), ("c", "")])
    assert out == [("b", "1", ""), ("a", "2", "u")]


def test_protected() -> None:
    assert is_protected("ipc_UUID")
    assert is_protected("ipc-filetype")
    assert is_protected("mesa.enabled")
    assert not is_protected("mesa.ducklake.catalog")
    assert not is_protected("neon.siteCode")
