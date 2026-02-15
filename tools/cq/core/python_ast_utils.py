"""Shared Python AST utility helpers for CQ tools.

This module provides canonical implementations of commonly-used AST helper
functions to avoid duplication across the codebase.
"""

from __future__ import annotations

import ast


def safe_unparse(node: ast.AST, *, default: str = "") -> str:
    """Safely unparse an AST node to source text, returning default on failure.

    Parameters
    ----------
    node : ast.AST
        AST node to unparse.
    default : str
        Default value to return if unparsing fails. Defaults to empty string.

    Returns:
    -------
    str
        Unparsed source code, or default on failure.
    """
    try:
        return ast.unparse(node)
    except (ValueError, TypeError):
        return default


def get_call_name(func: ast.expr) -> tuple[str, bool, str | None]:
    """Extract call name, whether it's an attribute call, and optional receiver.

    Parameters
    ----------
    func : ast.expr
        AST expression node representing the called function.

    Returns:
    -------
    tuple[str, bool, str | None]
        Tuple of (name, is_attribute, receiver_or_none):
        - name: The extracted function/method name
        - is_attribute: True if this is an attribute call (obj.method())
        - receiver_or_none: Receiver name (like 'obj') if available, else None
    """
    if isinstance(func, ast.Name):
        return func.id, False, None
    if isinstance(func, ast.Attribute):
        if isinstance(func.value, ast.Name):
            receiver = func.value.id
            return f"{receiver}.{func.attr}", True, receiver
        return func.attr, True, None
    return "", False, None
