"""Neighborhood structural export token coverage tests."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.structural.export import export_structural_rows

try:
    import tree_sitter_python
    from tree_sitter import Language, Parser
except ImportError:  # pragma: no cover - optional dependency
    tree_sitter_python = None
    Language = None
    Parser = None


@pytest.mark.skipif(tree_sitter_python is None, reason="tree-sitter-python is unavailable")
def test_structural_export_includes_tokens() -> None:
    """Test structural export includes tokens."""
    assert Language is not None
    assert Parser is not None
    assert tree_sitter_python is not None
    parser = Parser(Language(tree_sitter_python.language()))
    source = b"class C:\n    pass\n"
    tree = parser.parse(source)
    assert tree is not None
    export = export_structural_rows(
        file_path="sample.py",
        root=tree.root_node,
        source_bytes=source,
    )
    assert export.nodes
    assert export.tokens
