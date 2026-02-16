"""Tests for classifier runtime predicate helpers."""

from __future__ import annotations

from ast_grep_py import SgRoot
from tools.cq.search.pipeline.classifier_runtime import _is_docstring_context


def test_is_docstring_context_detects_expression_statement_docstring() -> None:
    root = SgRoot('def f():\n    """doc"""\n    return 1\n', "python")
    node = next(iter(root.root().find_all(kind="string")))
    assert _is_docstring_context(node) is True


def test_is_docstring_context_rejects_assignment_string() -> None:
    root = SgRoot('x = "value"\n', "python")
    node = next(iter(root.root().find_all(kind="string")))
    assert _is_docstring_context(node) is False
