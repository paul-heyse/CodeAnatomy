"""Tests for semantic TableRegistry extraction."""

from __future__ import annotations

import pytest

from semantics.table_registry import TableRegistry


def test_table_registry_register_get_and_names() -> None:
    """TableRegistry stores and returns table entries deterministically."""
    registry = TableRegistry()
    sentinel = object()
    registry.register("b", sentinel)  # type: ignore[arg-type]
    registry.register("a", sentinel)  # type: ignore[arg-type]

    assert registry.get("a") is sentinel
    assert registry.get("missing") is None
    assert registry.names() == ("a", "b")
    assert registry.all_names() == ("a", "b")


def test_table_registry_rejects_duplicate_registration() -> None:
    """Registering the same table name twice raises ValueError."""
    registry = TableRegistry()
    sentinel = object()
    registry.register("dup", sentinel)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Table already registered"):
        registry.register("dup", sentinel)  # type: ignore[arg-type]


def test_table_registry_resolve_registers_once() -> None:
    """Resolve should call factory only for first lookup."""
    registry = TableRegistry()
    calls = {"count": 0}

    def _factory() -> object:
        calls["count"] += 1
        return object()

    first = registry.resolve("node", _factory)  # type: ignore[arg-type]
    second = registry.resolve("node", _factory)  # type: ignore[arg-type]

    assert first is second
    assert calls["count"] == 1
