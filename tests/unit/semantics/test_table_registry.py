"""Tests for semantic TableRegistry extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from semantics.table_registry import TableRegistry

if TYPE_CHECKING:
    from semantics.compiler import TableInfo


def test_table_registry_register_get_and_names() -> None:
    """TableRegistry stores and returns table entries deterministically."""
    registry = TableRegistry()
    sentinel = cast("TableInfo", object())
    registry.register("b", sentinel)
    registry.register("a", sentinel)

    assert registry.get("a") is sentinel
    assert registry.get("missing") is None
    assert registry.names() == ("a", "b")
    assert registry.all_names() == ("a", "b")


def test_table_registry_rejects_duplicate_registration() -> None:
    """Registering the same table name twice raises ValueError."""
    registry = TableRegistry()
    sentinel = cast("TableInfo", object())
    registry.register("dup", sentinel)
    with pytest.raises(ValueError, match="Table already registered"):
        registry.register("dup", sentinel)


def test_table_registry_ensure_and_get_registers_once() -> None:
    """ensure_and_get should call factory only for first lookup."""
    registry = TableRegistry()
    calls = {"count": 0}

    def _factory() -> TableInfo:
        calls["count"] += 1
        return cast("TableInfo", object())

    first = registry.ensure_and_get("node", _factory)
    second = registry.ensure_and_get("node", _factory)

    assert first is second
    assert calls["count"] == 1
