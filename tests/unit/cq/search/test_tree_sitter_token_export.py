"""Tests for CST token export helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.structural.token_export import export_cst_tokens

try:
    import tree_sitter_python
    from tree_sitter import Language, Parser
except ImportError:  # pragma: no cover - optional dependency
    tree_sitter_python = None
    Language = None
    Parser = None


@pytest.mark.skipif(tree_sitter_python is None, reason="tree-sitter-python is unavailable")
def test_export_cst_tokens_returns_leaf_tokens() -> None:
    assert Language is not None
    assert Parser is not None
    assert tree_sitter_python is not None
    parser = Parser(Language(tree_sitter_python.language()))
    source = b"def f(x):\n    return x\n"
    tree = parser.parse(source)
    assert tree is not None
    tokens = export_cst_tokens(file_path="sample.py", root=tree.root_node, source_bytes=source)
    assert tokens
    assert any(token.text == "f" for token in tokens)
