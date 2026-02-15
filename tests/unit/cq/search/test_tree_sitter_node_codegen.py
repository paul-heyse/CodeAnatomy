"""Tests for node-type code generation helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.schema.node_codegen import render_node_types_module
from tools.cq.search.tree_sitter.schema.node_schema import GrammarNodeTypeV1, GrammarSchemaV1


def test_render_node_types_module_emits_python_source() -> None:
    schema = GrammarSchemaV1(
        language="python",
        node_types=(
            GrammarNodeTypeV1(type="function_definition", named=True, fields=("name", "body")),
        ),
    )
    source = render_node_types_module(schema)
    assert "NODE_TYPES" in source
    assert "function_definition" in source
