"""Tests for Python-specific query constants."""

from __future__ import annotations

from tools.cq.query.python_constants import PYTHON_AST_FIELDS


def test_python_ast_fields_contains_expected_keys() -> None:
    """Python AST field set includes stable high-signal field names."""
    assert "name" in PYTHON_AST_FIELDS
    assert "args" in PYTHON_AST_FIELDS
    assert "decorator_list" in PYTHON_AST_FIELDS
    assert "func" in PYTHON_AST_FIELDS
    assert "subject" in PYTHON_AST_FIELDS


def test_python_ast_fields_is_frozenset() -> None:
    """Field registry is immutable."""
    assert isinstance(PYTHON_AST_FIELDS, frozenset)
