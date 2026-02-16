"""Shared utilities for tree-sitter lane operations.

This module consolidates duplicated helper functions used across both
python_lane and rust_lane implementations. All functions are fail-open
and never affect core search behavior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Tree

    from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1

# Shared error tuple for enrichment operations
# Combined from both python_lane and rust_lane - includes all exceptions
# that should be caught and degraded gracefully
ENRICHMENT_ERRORS: tuple[type[Exception], ...] = (
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
    UnicodeError,
    KeyError,
    IndexError,
)


def build_query_windows(
    *,
    anchor_window: QueryWindowV1,
    source_byte_len: int,
    changed_ranges: tuple[object, ...],
) -> tuple[QueryWindowV1, ...]:
    """Build query windows from changed ranges with anchor window fallback.

    This function creates query windows from incremental parse change ranges,
    ensuring the anchor window is always included even if not in changed ranges.

    Parameters
    ----------
    anchor_window
        The primary window to query, derived from the match location.
    source_byte_len
        Total byte length of the source file.
    changed_ranges
        Tuple of changed ranges from incremental parse, or empty tuple.

    Returns:
    -------
    tuple[QueryWindowV1, ...]
        Query windows to execute, always including the anchor window.
    """
    # Import here to avoid circular dependency
    from tools.cq.search.tree_sitter.core.change_windows import (
        contains_window,
        ensure_query_windows,
        windows_from_changed_ranges,
    )

    windows = windows_from_changed_ranges(
        changed_ranges,
        source_byte_len=source_byte_len,
        pad_bytes=96,
    )
    windows = ensure_query_windows(windows, fallback=anchor_window)
    if windows and not contains_window(
        windows,
        value=anchor_window.start_byte,
        width=anchor_window.end_byte - anchor_window.start_byte,
    ):
        return (*windows, anchor_window)
    return windows


def lift_anchor(node: Node, *, parent_types: frozenset[str]) -> Node:
    """Lift a node to the nearest semantically meaningful parent.

    Walk up the tree from the given node to find the first parent that
    matches one of the provided parent types. This is used to expand
    match locations to their containing statement or definition.

    Parameters
    ----------
    node
        The starting node to lift.
    parent_types
        Set of node types that should terminate the upward walk.

    Returns:
    -------
    Node
        The lifted parent node, or the original node if no match found.
    """
    current = node
    while current.parent is not None:
        parent = current.parent
        if parent.type in parent_types:
            return parent
        current = parent
    return node


def make_parser_from_language(language: Language) -> Parser:
    """Create a tree-sitter Parser for the given language.

    Parameters
    ----------
    language
        The tree-sitter Language object to parse with.

    Returns:
    -------
    Parser
        A configured parser instance.

    Raises:
        RuntimeError: When tree-sitter parser bindings are unavailable.
    """
    try:
        from tree_sitter import Parser as _TreeSitterParser
    except ImportError as exc:  # pragma: no cover
        msg = "tree_sitter parser bindings are unavailable"
        raise RuntimeError(msg) from exc

    return _TreeSitterParser(language)


def parse_tree(source_bytes: bytes, *, language: Language) -> Tree:
    """Parse source bytes into a tree-sitter Tree.

    Parameters
    ----------
    source_bytes
        UTF-8 encoded source bytes to parse.
    language
        The tree-sitter Language object to parse with.

    Returns:
    -------
    Tree
        The parsed syntax tree.

    Raises:
        RuntimeError: When parser bindings are unavailable or parse returns no tree.
    """
    parser = make_parser_from_language(language)
    tree = parser.parse(source_bytes)
    if tree is None:
        msg = "tree-sitter parser returned no tree"
        raise RuntimeError(msg)
    return tree


__all__ = [
    "ENRICHMENT_ERRORS",
    "build_query_windows",
    "lift_anchor",
    "make_parser_from_language",
    "parse_tree",
]
