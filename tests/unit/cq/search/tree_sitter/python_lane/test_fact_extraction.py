"""Unit tests for Python lane fact-extraction helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.python_lane.fact_extraction import (
    extract_python_facts,
    extract_scope_facts,
)


@dataclass(slots=True)
class _Node:
    type: str
    parent: object | None = None


def test_extract_scope_facts_collects_scope_chain() -> None:
    """Scope extraction should record nearest scope and ancestor chain kinds."""
    root = _Node("module")
    cls = _Node("class_definition", root)
    fn = _Node("function_definition", cls)

    facts = extract_scope_facts(fn, source_bytes=b"")

    assert facts.scope_kind == "function_definition"
    assert "function_definition" in facts.scope_chain


def test_extract_python_facts_includes_node_and_scope() -> None:
    """Fact extraction should include node kind and nearest scope metadata."""
    node = _Node("identifier", _Node("function_definition", _Node("module")))
    facts = extract_python_facts(node, source_bytes=b"")

    assert facts["node_kind"] == "identifier"
    assert facts["scope_kind"] == "function_definition"
