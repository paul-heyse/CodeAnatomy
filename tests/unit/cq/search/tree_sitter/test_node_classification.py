"""Tests for visibility/supertype node classification surfaces."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tools.cq.search.tree_sitter.schema import node_schema
from tools.cq.search.tree_sitter.structural.export import export_structural_rows

try:
    import tree_sitter_python
    from tree_sitter import Language, Parser
except ImportError:  # pragma: no cover - environment dependent
    tree_sitter_python = None
    Language = None
    Parser = None


def test_runtime_node_types_include_visibility_and_supertype_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test runtime node types include visibility and supertype flags."""
    runtime_language = SimpleNamespace(
        node_kind_count=2,
        field_count=0,
        node_kind_for_id=lambda kind_id: "module" if kind_id == 0 else "identifier",
        node_kind_is_named=lambda _kind_id: True,
        node_kind_is_visible=lambda _kind_id: True,
        node_kind_is_supertype=lambda kind_id: kind_id == 0,
        field_name_for_id=lambda _field_id: None,
    )
    monkeypatch.setattr(node_schema, "_runtime_language", lambda _language: runtime_language)
    schema = node_schema.load_grammar_schema("python")
    assert schema is not None
    node_rows = tuple(row for row in schema.node_types if row.type != "__field_registry__")
    assert node_rows
    assert node_rows[0].is_visible is True
    assert node_rows[0].is_supertype is True
    assert node_rows[1].is_visible is True
    assert node_rows[1].is_supertype is False


@pytest.mark.skipif(tree_sitter_python is None, reason="tree-sitter-python is unavailable")
def test_structural_export_emits_node_classification_fields() -> None:
    """Test structural export emits node classification fields."""
    assert Language is not None
    assert Parser is not None
    assert tree_sitter_python is not None
    parser = Parser(Language(tree_sitter_python.language()))
    source = b"x = 1\n"
    tree = parser.parse(source)
    assert tree is not None
    exported = export_structural_rows(
        file_path="sample.py", root=tree.root_node, source_bytes=source
    )
    assert exported.nodes
    first = exported.nodes[0]
    assert first.is_visible in {True, False, None}
    assert first.is_supertype in {True, False, None}
