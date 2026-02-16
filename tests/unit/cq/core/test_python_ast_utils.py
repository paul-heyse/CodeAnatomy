"""Tests for tools.cq.core.python_ast_utils."""

from __future__ import annotations

import ast

from tools.cq.core.python_ast_utils import get_call_name, safe_unparse


def test_safe_unparse_with_valid_node() -> None:
    """Test safe_unparse with a valid AST node."""
    node = ast.parse("x + y", mode="eval").body
    assert safe_unparse(node, default="<fail>") == "x + y"


def test_safe_unparse_with_default() -> None:
    """Test safe_unparse returns default on failure."""
    node = ast.Name(id="test", ctx=ast.Load())
    result = safe_unparse(node, default="<fallback>")
    assert result in {"test", "<fallback>"}


def test_safe_unparse_default_empty_string() -> None:
    """Test safe_unparse default parameter defaults to empty string."""
    node = ast.Name(id="test", ctx=ast.Load())
    result = safe_unparse(node)
    assert isinstance(result, str)


def test_get_call_name_with_name_node() -> None:
    """Test get_call_name with ast.Name node."""
    func = ast.Name(id="print", ctx=ast.Load())
    name, is_attribute, receiver = get_call_name(func)
    assert name == "print"
    assert is_attribute is False
    assert receiver is None


def test_get_call_name_with_attribute_and_name_receiver() -> None:
    """Test get_call_name with ast.Attribute node with Name receiver."""
    receiver_node = ast.Name(id="obj", ctx=ast.Load())
    func = ast.Attribute(value=receiver_node, attr="method", ctx=ast.Load())
    name, is_attribute, receiver = get_call_name(func)
    assert name == "obj.method"
    assert is_attribute is True
    assert receiver == "obj"


def test_get_call_name_with_attribute_without_name_receiver() -> None:
    """Test get_call_name with ast.Attribute node without Name receiver."""
    call_node = ast.Call(
        func=ast.Name(id="func", ctx=ast.Load()),
        args=[],
        keywords=[],
    )
    func = ast.Attribute(value=call_node, attr="method", ctx=ast.Load())
    name, is_attribute, receiver = get_call_name(func)
    assert name == "method"
    assert is_attribute is True
    assert receiver is None


def test_get_call_name_with_other_node_type() -> None:
    """Test get_call_name with other node type returns empty tuple."""
    func = ast.Lambda(
        args=ast.arguments(
            args=[],
            posonlyargs=[],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=ast.Constant(value=1),
    )
    name, is_attribute, receiver = get_call_name(func)
    assert not name
    assert is_attribute is False
    assert receiver is None
