"""Tests for shared span normalization helpers."""

from __future__ import annotations

from datafusion_engine.expr.span import (
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    encoding_from_unit,
    normalize_col_unit_value,
)


def test_normalize_col_unit_value() -> None:
    """Normalize col-unit values using canonical rules."""
    assert normalize_col_unit_value(None) == "utf32"
    assert normalize_col_unit_value(ENC_UTF8) == "utf8"
    assert normalize_col_unit_value(ENC_UTF16) == "utf16"
    assert normalize_col_unit_value(ENC_UTF32) == "utf32"
    assert normalize_col_unit_value("UTF8") == "utf8"
    assert normalize_col_unit_value(" 2 ") == "utf16"
    assert normalize_col_unit_value("byte_offset") == "byte"
    assert normalize_col_unit_value("unknown") == "utf32"


def test_encoding_from_unit() -> None:
    """Map normalized col-unit strings back to encoding ids."""
    assert encoding_from_unit("utf8") == ENC_UTF8
    assert encoding_from_unit("utf16") == ENC_UTF16
    assert encoding_from_unit("utf32") == ENC_UTF32
    assert encoding_from_unit("byte") == ENC_UTF32
