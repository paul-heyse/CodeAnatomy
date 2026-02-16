# ruff: noqa: D103, PLR2004
"""Tests for canonical coercion helpers."""

from __future__ import annotations

import pytest

from utils.coercion import coerce_bool, coerce_int, coerce_opt_int


def test_coerce_int_accepts_numeric_strings() -> None:
    assert coerce_int("12", label="x") == 12


def test_coerce_opt_int_none_passthrough() -> None:
    assert coerce_opt_int(None, label="x") is None


def test_coerce_bool_string_values() -> None:
    assert coerce_bool("true", default=False, label="x") is True
    assert coerce_bool("0", default=True, label="x") is False


def test_coerce_bool_rejects_unsupported_values() -> None:
    with pytest.raises(TypeError):
        coerce_bool(object(), default=False, label="x")
