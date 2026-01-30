"""Tests for environment variable helpers."""

from __future__ import annotations

import pytest

from utils.env_utils import (
    env_bool,
    env_bool_strict,
    env_float,
    env_int,
    env_text,
    env_truthy,
    env_value,
)

INT_SAMPLE = 42
INT_DEFAULT = 7
FLOAT_SAMPLE = 3.14
FLOAT_DEFAULT = 1.5


def test_env_value_strips(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_value trims whitespace and treats blanks as missing."""
    monkeypatch.setenv("TEST_ENV_VALUE", "  hello ")
    assert env_value("TEST_ENV_VALUE") == "hello"
    monkeypatch.setenv("TEST_ENV_VALUE", "   ")
    assert env_value("TEST_ENV_VALUE") is None


def test_env_text_allows_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_text supports empty values when configured."""
    monkeypatch.delenv("TEXT_VALUE", raising=False)
    assert env_text("TEXT_VALUE", default="fallback") == "fallback"
    monkeypatch.setenv("TEXT_VALUE", "  hello ")
    assert env_text("TEXT_VALUE") == "hello"
    monkeypatch.setenv("TEXT_VALUE", "   ")
    assert env_text("TEXT_VALUE", default="fallback") == "fallback"
    assert not env_text("TEXT_VALUE", allow_empty=True)
    monkeypatch.setenv("TEXT_VALUE", "  spaced  ")
    assert env_text("TEXT_VALUE", strip=False) == "  spaced  "


def test_env_bool_valid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_bool handles canonical truthy/falsey values."""
    monkeypatch.setenv("BOOL_TRUE", "true")
    assert env_bool("BOOL_TRUE") is True
    monkeypatch.setenv("BOOL_FALSE", "0")
    assert env_bool("BOOL_FALSE") is False


def test_env_bool_invalid_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_bool respects defaults when values are invalid."""
    monkeypatch.setenv("BOOL_INVALID", "maybe")
    assert env_bool("BOOL_INVALID", default=True) is True
    assert env_bool("BOOL_INVALID", default=False, on_invalid="false") is False
    assert env_bool("BOOL_INVALID", default=None, on_invalid="none") is None


def test_env_bool_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_bool_strict only accepts strict boolean strings."""
    monkeypatch.setenv("STRICT_BOOL", "true")
    assert env_bool_strict("STRICT_BOOL", default=False) is True
    monkeypatch.setenv("STRICT_BOOL", "false")
    assert env_bool_strict("STRICT_BOOL", default=True) is False
    monkeypatch.setenv("STRICT_BOOL", "1")
    assert env_bool_strict("STRICT_BOOL", default=True) is True


def test_env_int_and_float(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_int/env_float parse values or fall back to defaults."""
    monkeypatch.setenv("INT_VALUE", "42")
    assert env_int("INT_VALUE") == INT_SAMPLE
    monkeypatch.setenv("INT_VALUE", "nope")
    assert env_int("INT_VALUE", default=INT_DEFAULT) == INT_DEFAULT

    monkeypatch.setenv("FLOAT_VALUE", "3.14")
    assert env_float("FLOAT_VALUE") == FLOAT_SAMPLE
    monkeypatch.setenv("FLOAT_VALUE", "bad")
    assert env_float("FLOAT_VALUE", default=FLOAT_DEFAULT) == FLOAT_DEFAULT


def test_env_truthy() -> None:
    """Ensure env_truthy handles common truthy/falsey strings."""
    assert env_truthy("true") is True
    assert env_truthy("TRUE") is True
    assert env_truthy("false") is False
    assert env_truthy(None) is False
