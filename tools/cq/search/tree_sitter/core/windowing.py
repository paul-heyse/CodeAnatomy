"""Query cursor window application helpers with containment-mode support."""

from __future__ import annotations

from typing import Any

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryPointWindowV1,
    QueryWindowV1,
)


def apply_point_window(
    *,
    cursor: Any,
    window: QueryPointWindowV1,
    mode: str,
) -> bool:
    """Apply a point window to a query cursor.

    Returns:
        bool: ``True`` when a window API was applied.
    """
    if mode in {"containment_preferred", "containment_required"}:
        containing = getattr(cursor, "set_containing_point_range", None)
        if callable(containing):
            containing(
                (window.start_row, window.start_col),
                (window.end_row, window.end_col),
            )
            return True
        if mode == "containment_required":
            return False
    set_point_range = getattr(cursor, "set_point_range", None)
    if callable(set_point_range):
        set_point_range(
            (window.start_row, window.start_col),
            (window.end_row, window.end_col),
        )
        return True
    return False


def apply_byte_window(
    *,
    cursor: Any,
    window: QueryWindowV1,
    mode: str,
) -> bool:
    """Apply a byte window to a query cursor.

    Returns:
        bool: ``True`` when a window API was applied.
    """
    if mode in {"containment_preferred", "containment_required"}:
        containing = getattr(cursor, "set_containing_byte_range", None)
        if callable(containing):
            containing(window.start_byte, window.end_byte)
            return True
        if mode == "containment_required":
            return False
    set_byte_range = getattr(cursor, "set_byte_range", None)
    if callable(set_byte_range):
        set_byte_range(window.start_byte, window.end_byte)
        return True
    return False


__all__ = ["apply_byte_window", "apply_point_window"]
