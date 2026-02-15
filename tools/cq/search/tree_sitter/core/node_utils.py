"""Canonical tree-sitter node utilities.

This module provides shared helpers for extracting text and byte spans from
tree-sitter-like node objects. It supersedes local `_node_text` duplicates
scattered across the codebase.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class NodeLike(Protocol):
    """Structural protocol for tree-sitter-like node objects.

    This protocol matches the interface of tree-sitter Node objects,
    allowing generic code to work with any node-like structure.
    """

    @property
    def start_byte(self) -> int:
        """Return the byte offset where this node starts in the source."""
        ...

    @property
    def end_byte(self) -> int:
        """Return the byte offset where this node ends in the source."""
        ...

    @property
    def start_point(self) -> tuple[int, int]:
        """Return the (row, column) position where this node starts."""
        ...

    @property
    def end_point(self) -> tuple[int, int]:
        """Return the (row, column) position where this node ends."""
        ...


def node_text(
    node: NodeLike | object | None,
    source_bytes: bytes,
    *,
    strip: bool = True,
    max_len: int | None = None,
) -> str:
    """Extract UTF-8 text from a node's byte span.

    Parameters
    ----------
    node : NodeLike | object | None
        The node to extract text from. If None, returns empty string.
        Uses getattr() to safely access start_byte/end_byte.
    source_bytes : bytes
        The source code as bytes.
    strip : bool, default True
        Whether to strip leading/trailing whitespace from the result.
    max_len : int | None, default None
        Maximum length of returned text. If exceeded, truncates with "...".

    Returns:
    -------
    str
        The decoded UTF-8 text from the node's byte span, or empty string
        if the node is None or has an empty span.

    Examples:
    --------
    >>> node_text(some_node, source_bytes)
    'def foo():'

    >>> node_text(some_node, source_bytes, strip=False, max_len=10)
    '  def fo...'
    """
    if node is None:
        return ""
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    text = source_bytes[start:end].decode("utf-8", errors="replace")
    out = text.strip() if strip else text
    if max_len is not None and len(out) >= max_len:
        return out[: max(1, max_len - 3)] + "..."
    return out


def node_byte_span(node: NodeLike | object | None) -> tuple[int, int]:
    """Extract (start_byte, end_byte) from a node, defaulting to (0, 0).

    Parameters
    ----------
    node : NodeLike | object | None
        The node to extract byte span from. If None, returns (0, 0).
        Uses getattr() to safely access start_byte/end_byte.

    Returns:
    -------
    tuple[int, int]
        A tuple of (start_byte, end_byte) representing the byte span.
        If the node is None or missing attributes, returns (0, 0).
        Ensures end_byte >= start_byte.

    Examples:
    --------
    >>> node_byte_span(some_node)
    (42, 56)

    >>> node_byte_span(None)
    (0, 0)
    """
    if node is None:
        return 0, 0
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    return start, max(start, end)


__all__ = ["NodeLike", "node_byte_span", "node_text"]
