"""Tests for runtime schema-id helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.schema.node_schema import (
    build_runtime_field_ids,
    build_runtime_ids,
)


class _LanguageStub:
    def id_for_node_kind(self, name: str, named: object) -> int:
        _ = named
        return {"identifier": 1, "call": 2, "function_definition": 3}[name]

    def field_id_for_name(self, name: str) -> int:
        return {"name": 1, "body": 2, "parameters": 3}[name]


def test_build_runtime_ids_and_field_ids() -> None:
    language = _LanguageStub()
    assert build_runtime_ids(language)["call"] == 2
    assert build_runtime_field_ids(language)["body"] == 2
