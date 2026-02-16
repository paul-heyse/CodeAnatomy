"""Tests for runtime schema-id helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.schema.node_schema import (
    build_runtime_field_ids,
    build_runtime_ids,
)

CALL_NODE_KIND_ID = 2
BODY_FIELD_ID = 2
FUNCTION_FIELD_ID = 4


class _LanguageStub:
    @staticmethod
    def id_for_node_kind(name: str, named: object) -> int:
        _ = named
        return {"identifier": 1, "call": 2, "function_definition": 3}[name]

    @staticmethod
    def field_id_for_name(name: str) -> int:
        return {"name": 1, "body": 2, "parameters": 3, "function": 4}[name]


def test_build_runtime_ids_and_field_ids() -> None:
    """Test build runtime ids and field ids."""
    language = _LanguageStub()
    assert build_runtime_ids(language)["call"] == CALL_NODE_KIND_ID
    assert build_runtime_field_ids(language)["body"] == BODY_FIELD_ID


def test_build_runtime_field_ids_with_custom_candidates() -> None:
    """Test build runtime field ids with custom candidates."""
    language = _LanguageStub()
    out = build_runtime_field_ids(language, field_names=("function",))
    assert out["name"] == 1
    assert out["function"] == FUNCTION_FIELD_ID
