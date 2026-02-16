"""Canonical constants and field-id helpers for the Python tree-sitter lane."""

from __future__ import annotations

from functools import lru_cache

from tools.cq.search.tree_sitter.core import infrastructure

MAX_CAPTURE_ITEMS = 8
DEFAULT_MATCH_LIMIT = 4_096
STOP_CONTEXT_KINDS: frozenset[str] = frozenset({"module", "source_file"})
PYTHON_LIFT_ANCHOR_TYPES: frozenset[str] = frozenset(
    {
        "call",
        "attribute",
        "assignment",
        "import_statement",
        "import_from_statement",
        "function_definition",
        "class_definition",
    }
)


@lru_cache(maxsize=1)
def get_python_field_ids() -> dict[str, int]:
    """Return cached Python grammar field IDs."""
    return infrastructure.cached_field_ids("python")


__all__ = [
    "DEFAULT_MATCH_LIMIT",
    "MAX_CAPTURE_ITEMS",
    "PYTHON_LIFT_ANCHOR_TYPES",
    "STOP_CONTEXT_KINDS",
    "get_python_field_ids",
]
