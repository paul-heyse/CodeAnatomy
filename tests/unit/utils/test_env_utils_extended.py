"""Tests for extended environment helpers."""

from __future__ import annotations

from enum import StrEnum

import pytest

from utils.env_utils import env_enum, env_list


class SampleEnum(StrEnum):
    """Sample enum for env_enum tests."""

    FOO = "foo"
    BAR = "bar"


def test_env_list_default_and_strip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_list returns defaults and strips items."""
    monkeypatch.delenv("LIST_VALUE", raising=False)
    assert env_list("LIST_VALUE") == []
    assert env_list("LIST_VALUE", default=["a"]) == ["a"]

    monkeypatch.setenv("LIST_VALUE", " a, b , ,c ")
    assert env_list("LIST_VALUE") == ["a", "b", "c"]


def test_env_list_custom_separator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_list honors custom separators and strip flags."""
    monkeypatch.setenv("LIST_VALUE", "a|b| |c")
    assert env_list("LIST_VALUE", separator="|") == ["a", "b", "c"]
    assert env_list("LIST_VALUE", separator="|", strip=False) == ["a", "b", " ", "c"]


def test_env_enum_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure env_enum parses enum values and respects defaults."""
    monkeypatch.setenv("ENUM_VALUE", "FOO")
    assert env_enum("ENUM_VALUE", SampleEnum) is SampleEnum.FOO

    monkeypatch.setenv("ENUM_VALUE", "unknown")
    assert env_enum("ENUM_VALUE", SampleEnum) is None
    assert env_enum("ENUM_VALUE", SampleEnum, default=SampleEnum.BAR) is SampleEnum.BAR
