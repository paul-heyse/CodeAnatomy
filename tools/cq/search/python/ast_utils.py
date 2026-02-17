"""Shared AST utilities for Python analysis."""

from __future__ import annotations

import ast

from tools.cq.search._shared.helpers import line_col_to_byte_offset


def node_byte_span(node: ast.AST, source_bytes: bytes) -> tuple[int, int] | None:
    """Return byte span (start, end) for an AST node.

    Parameters
    ----------
    node : ast.AST
        AST node with position information.
    source_bytes : bytes
        Source file bytes for offset calculation.

    Returns:
    -------
    tuple[int, int] | None
        Byte span as (start, end) or None if node lacks position info.
    """
    lineno = getattr(node, "lineno", None)
    col_offset = getattr(node, "col_offset", None)
    end_lineno = getattr(node, "end_lineno", None)
    end_col_offset = getattr(node, "end_col_offset", None)
    if not isinstance(lineno, int):
        return None
    if not isinstance(col_offset, int):
        return None
    if not isinstance(end_lineno, int):
        return None
    if not isinstance(end_col_offset, int):
        return None
    start = line_col_to_byte_offset(source_bytes, lineno, col_offset)
    end = line_col_to_byte_offset(source_bytes, end_lineno, end_col_offset)
    if start is None or end is None or end <= start:
        return None
    return start, end


def ast_node_priority(node: ast.AST) -> int:
    """Return priority score for AST node type.

    Lower scores indicate higher priority for matching.

    Parameters
    ----------
    node : ast.AST
        AST node to score.

    Returns:
    -------
    int
        Priority score (0 = highest priority).
    """
    if isinstance(node, ast.Name):
        return 0
    if isinstance(node, ast.Attribute):
        return 1
    if isinstance(node, ast.alias):
        return 2
    if isinstance(node, ast.Call):
        return 3
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return 4
    return 10


__all__ = [
    "ast_node_priority",
    "node_byte_span",
]
