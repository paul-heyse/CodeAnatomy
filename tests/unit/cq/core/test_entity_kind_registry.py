"""Tests for entity-kind registry mappings and match semantics."""

from __future__ import annotations

from tools.cq.core.entity_kinds import ENTITY_KINDS


def test_record_type_maps_exposed() -> None:
    """Entity selectors expose base and extra record-type requirements."""
    assert ENTITY_KINDS.record_types_for_entity("function") == frozenset({"def"})
    assert ENTITY_KINDS.extra_record_types_for_entity("function") == frozenset({"call"})
    assert ENTITY_KINDS.record_types_for_entity("unknown") == frozenset()
    assert ENTITY_KINDS.extra_record_types_for_entity("unknown") == frozenset()


def test_matches_function_and_class_kinds() -> None:
    """Function/class matching is delegated to canonical kind registries."""
    assert ENTITY_KINDS.matches(
        entity_type="function",
        record_kind="function",
        record_type="def",
    )
    assert ENTITY_KINDS.matches(
        entity_type="class",
        record_kind="class",
        record_type="def",
    )
    assert not ENTITY_KINDS.matches(
        entity_type="class",
        record_kind="function",
        record_type="def",
    )


def test_matches_import_decorator_and_callsite() -> None:
    """Non-definition selectors use canonical import/decorator/call routing."""
    assert ENTITY_KINDS.matches(
        entity_type="import",
        record_kind="import",
        record_type="import",
    )
    assert ENTITY_KINDS.matches(
        entity_type="decorator",
        record_kind="class",
        record_type="def",
    )
    assert ENTITY_KINDS.matches(
        entity_type="callsite",
        record_kind="call_expression",
        record_type="call",
    )
    assert not ENTITY_KINDS.matches(
        entity_type="callsite",
        record_kind="call_expression",
        record_type="def",
    )
