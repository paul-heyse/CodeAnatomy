"""Tests for extractor predicate-based helper guards."""

from __future__ import annotations

from ast_grep_py import SgNode, SgRoot
from tools.cq.search.python.extractors_classification import (
    _unwrap_decorated,
)
from tools.cq.search.python.extractors_classification import (
    classify_function_role_by_parent as _classify_function_role_by_parent,
)


def _first_named(root: SgRoot, kind: str) -> SgNode:
    return next(iter(root.root().find_all(kind=kind)))


def test_classify_function_role_by_parent_outside_class() -> None:
    """Test classify function role by parent outside class."""
    root = SgRoot("def helper(x):\n    return x\n", "python")
    fn = _first_named(root, "function_definition")
    assert _classify_function_role_by_parent(fn, _unwrap_decorated(fn)) == "free_function"


def test_classify_function_role_by_parent_inside_class() -> None:
    """Test classify function role by parent inside class."""
    root = SgRoot("class C:\n    def m(self):\n        return 1\n", "python")
    fn = _first_named(root, "function_definition")
    assert _classify_function_role_by_parent(fn, _unwrap_decorated(fn)) == "method"
