"""Rust tree-sitter availability helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

try:
    from tree_sitter import Point as _TreeSitterPoint
except ImportError:  # pragma: no cover - availability guard
    _TreeSitterPoint = None


__all__ = ["is_tree_sitter_rust_available"]


def is_tree_sitter_rust_available() -> bool:
    """Check whether Rust tree-sitter runtime dependencies are available.

    Returns:
        bool: True when required Rust tree-sitter runtime dependencies are importable.
    """
    try:
        from tree_sitter import Parser as _TreeSitterParser
    except ImportError:  # pragma: no cover
        return False

    return all(
        obj is not None
        for obj in (
            load_tree_sitter_language("rust"),
            _TreeSitterParser,
            _TreeSitterPoint,
        )
    )
