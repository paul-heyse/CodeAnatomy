"""Tests for canonical coercion helpers."""

from __future__ import annotations

import pytest

from utils.coercion import coerce_bool, coerce_int, coerce_opt_int

EXPECTED_INT_VALUE = 12


def test_coerce_int_accepts_numeric_strings() -> None:
    """Numeric strings are coerced to integers."""
    assert coerce_int("12", label="x") == EXPECTED_INT_VALUE


def test_coerce_opt_int_none_passthrough() -> None:
    """Optional integer coercion preserves None values."""
    assert coerce_opt_int(None, label="x") is None


def test_coerce_bool_string_values() -> None:
    """Boolean coercion handles true/false string forms."""
    assert coerce_bool("true", default=False, label="x") is True
    assert coerce_bool("0", default=True, label="x") is False


def test_coerce_bool_rejects_unsupported_values() -> None:
    """Unsupported boolean coercion inputs raise TypeError."""
    with pytest.raises(TypeError, match="bool"):
        coerce_bool(object(), default=False, label="x")
