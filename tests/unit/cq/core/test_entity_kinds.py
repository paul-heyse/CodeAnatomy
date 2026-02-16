"""Tests for canonical entity-kind registry."""

from __future__ import annotations

from tools.cq.core.entity_kinds import ENTITY_KINDS


def test_entity_kind_registry_sets_are_frozensets() -> None:
    """Entity kind category sets are immutable frozensets."""
    assert isinstance(ENTITY_KINDS.function_kinds, frozenset)
    assert isinstance(ENTITY_KINDS.class_kinds, frozenset)
    assert isinstance(ENTITY_KINDS.import_kinds, frozenset)


def test_entity_kind_registry_decorator_union() -> None:
    """Decorator kinds match function/class union and retain core kinds."""
    assert ENTITY_KINDS.decorator_kinds == (ENTITY_KINDS.function_kinds | ENTITY_KINDS.class_kinds)
    assert "function" in ENTITY_KINDS.function_kinds
    assert "class" in ENTITY_KINDS.class_kinds
    assert "import" in ENTITY_KINDS.import_kinds
