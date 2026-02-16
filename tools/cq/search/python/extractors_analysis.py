"""Python behavior and import analysis functions for enrichment pipeline.

This module provides analysis functions for extracting behavioral patterns
(yields, awaits, returns) and import details from Python code.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode

# ---------------------------------------------------------------------------
# Python ast scope boundary types
# ---------------------------------------------------------------------------

_AST_SCOPE_BOUNDARY_TYPES: tuple[type, ...] = (
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.Lambda,
)

# ---------------------------------------------------------------------------
# Behavior type mapping
# ---------------------------------------------------------------------------

# Map from AST node type to the behavior flag name it sets.
_BEHAVIOR_TYPE_MAP: dict[type, str] = {
    ast.Raise: "raises_exception",
    ast.Yield: "yields",
    ast.YieldFrom: "yields",
    ast.Await: "awaits",
    ast.With: "has_context_manager",
    ast.AsyncWith: "has_context_manager",
}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_IMPORT_NAMES = 12

# ---------------------------------------------------------------------------
# Scope-safe walker
# ---------------------------------------------------------------------------


def walk_function_body(
    func_node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> Iterator[ast.AST]:
    """Walk function body nodes, skipping nested scopes.

    Parameters
    ----------
    func_node
        A function definition AST node.

    Yields:
    ------
    ast.AST
        Body nodes excluding nested function/class/lambda definitions.
    """
    stack: list[ast.AST] = list(reversed(func_node.body))
    while stack:
        current = stack.pop()
        yield current
        # Do not recurse into nested scope boundaries
        if isinstance(current, _AST_SCOPE_BOUNDARY_TYPES):
            continue
        stack.extend(
            child
            for child in ast.iter_child_nodes(current)
            if not isinstance(child, _AST_SCOPE_BOUNDARY_TYPES)
        )


# ---------------------------------------------------------------------------
# Behavior analysis
# ---------------------------------------------------------------------------


def extract_behavior_summary(
    func_ast: ast.FunctionDef | ast.AsyncFunctionDef,
) -> dict[str, object]:
    """Extract behavioral flags from a function body.

    Parameters
    ----------
    func_ast
        A function definition AST node.

    Returns:
    -------
    dict[str, object]
        Behavior flags: returns_value, raises_exception, yields, awaits,
        has_context_manager.
    """
    flags: dict[str, bool] = {}

    for child in walk_function_body(func_ast):
        if isinstance(child, ast.Return) and child.value is not None:
            flags["returns_value"] = True
        else:
            flag_name = _BEHAVIOR_TYPE_MAP.get(type(child))
            if flag_name is not None:
                flags[flag_name] = True

    return dict(flags)


def extract_generator_flag(
    func_ast: ast.FunctionDef | ast.AsyncFunctionDef,
) -> dict[str, object]:
    """Detect whether a function is a generator (scope-safe).

    Parameters
    ----------
    func_ast
        A function definition AST node.

    Returns:
    -------
    dict[str, object]
        Dict with ``is_generator`` key.
    """
    for child in walk_function_body(func_ast):
        if isinstance(child, (ast.Yield, ast.YieldFrom)):
            return {"is_generator": True}
    return {"is_generator": False}


# ---------------------------------------------------------------------------
# AST function finder
# ---------------------------------------------------------------------------


def find_ast_function(
    tree: ast.Module,
    line: int,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Find a function definition starting on the given line.

    Parameters
    ----------
    tree
        Parsed AST module.
    line
        1-indexed line number.

    Returns:
    -------
    ast.FunctionDef | ast.AsyncFunctionDef | None
        The matching function node, or None.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.lineno == line:
                return node
            # Check for decorated functions (decorator may be on a different line)
            if (
                hasattr(node, "decorator_list")
                and node.decorator_list
                and node.decorator_list[0].lineno <= line <= node.lineno
            ):
                return node
    return None


# ---------------------------------------------------------------------------
# Import analysis
# ---------------------------------------------------------------------------


def find_ast_import(
    tree: ast.Module,
    line: int,
) -> ast.Import | ast.ImportFrom | None:
    """Find an import statement on the given line.

    Parameters
    ----------
    tree
        Parsed AST module.
    line
        1-indexed line number.

    Returns:
    -------
    ast.Import | ast.ImportFrom | None
        The matching import node, or None.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)) and node.lineno == line:
            return node
    return None


def is_type_checking_import(tree: ast.Module, import_node: ast.Import | ast.ImportFrom) -> bool:
    """Detect whether an import is inside a TYPE_CHECKING guard.

    Parameters
    ----------
    tree
        Parsed AST module.
    import_node
        The import node to check.

    Returns:
    -------
    bool
        True if inside an ``if TYPE_CHECKING:`` block.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        is_tc = (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
            isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
        )
        if is_tc:
            for child in ast.walk(node):
                if child is import_node:
                    return True
    return False


def extract_import_detail(
    _node: SgNode,
    source_bytes: bytes,
    ast_tree: ast.Module,
    line: int,
) -> dict[str, object]:
    """Extract normalized import details using Python ``ast``.

    Parameters
    ----------
    _node
        An import_statement or import_from_statement ast-grep node (unused).
    source_bytes
        Full source bytes (unused, kept for compatibility).
    ast_tree
        Parsed AST module.
    line
        1-indexed line number.

    Returns:
    -------
    dict[str, object]
        Import detail fields.
    """
    result: dict[str, object] = {}

    import_node = find_ast_import(ast_tree, line)
    if import_node is None:
        return result

    if isinstance(import_node, ast.Import):
        if import_node.names:
            result["import_module"] = import_node.names[0].name
            if import_node.names[0].asname:
                result["import_alias"] = import_node.names[0].asname
        result["import_level"] = 0

    elif isinstance(import_node, ast.ImportFrom):
        if import_node.module:
            result["import_module"] = import_node.module
        names = [alias.name for alias in import_node.names[:_MAX_IMPORT_NAMES]]
        result["import_names"] = names
        if len(import_node.names) == 1 and import_node.names[0].asname:
            result["import_alias"] = import_node.names[0].asname
        result["import_level"] = import_node.level or 0

    # TYPE_CHECKING detection
    if is_type_checking_import(ast_tree, import_node):
        result["is_type_import"] = True

    return result


__all__ = [
    "extract_behavior_summary",
    "extract_generator_flag",
    "extract_import_detail",
    "find_ast_function",
    "find_ast_import",
    "is_type_checking_import",
    "walk_function_body",
]
