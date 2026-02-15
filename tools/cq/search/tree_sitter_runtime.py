"""Shared bounded query execution helpers for tree-sitter-based lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_runtime_contracts import (
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryWindowV1,
)

if TYPE_CHECKING:
    from tree_sitter import Node, Query

try:
    from tree_sitter import QueryCursor as _TreeSitterQueryCursor
except ImportError:  # pragma: no cover - optional dependency guard
    _TreeSitterQueryCursor = None


def _default_window(root: Node) -> QueryWindowV1:
    return QueryWindowV1(
        start_byte=int(getattr(root, "start_byte", 0)),
        end_byte=int(getattr(root, "end_byte", 0)),
    )


def _normalized_windows(
    root: Node,
    windows: Iterable[QueryWindowV1] | None,
) -> tuple[QueryWindowV1, ...]:
    if windows is None:
        return (_default_window(root),)
    normalized = tuple(window for window in windows if window.end_byte > window.start_byte)
    if normalized:
        return normalized
    return (_default_window(root),)


def run_bounded_query_captures(
    query: Query,
    root: Node,
    *,
    windows: Iterable[QueryWindowV1] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    progress_callback: Callable[[object], bool] | None = None,
) -> tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]:
    """Run `captures` over bounded windows with defensive limits.

    Returns:
        tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]: Capture buckets
            merged across windows and execution telemetry.

    Raises:
        RuntimeError: Tree-sitter query cursor bindings are unavailable.
    """
    if _TreeSitterQueryCursor is None:
        msg = "tree_sitter query cursor bindings are unavailable"
        raise RuntimeError(msg)

    effective_settings = settings or QueryExecutionSettingsV1()
    cursor = _TreeSitterQueryCursor(query, match_limit=effective_settings.match_limit)
    if effective_settings.max_start_depth is not None and hasattr(cursor, "set_max_start_depth"):
        cursor.set_max_start_depth(effective_settings.max_start_depth)

    windows_list = _normalized_windows(root, windows)
    merged: dict[str, list[Node]] = {}
    windows_executed = 0
    _ = progress_callback
    cancelled = False

    for window in windows_list:
        cursor.set_byte_range(window.start_byte, window.end_byte)
        captures = cursor.captures(root)
        windows_executed += 1
        if isinstance(captures, dict):
            for capture_name, nodes in captures.items():
                if not isinstance(capture_name, str):
                    continue
                bucket = merged.setdefault(capture_name, [])
                if isinstance(nodes, list):
                    bucket.extend(node for node in nodes if hasattr(node, "start_byte"))
        if bool(getattr(cursor, "did_exceed_match_limit", False)):
            break

    telemetry = QueryExecutionTelemetryV1(
        windows_total=len(windows_list),
        windows_executed=windows_executed,
        capture_count=sum(len(nodes) for nodes in merged.values()),
        exceeded_match_limit=bool(getattr(cursor, "did_exceed_match_limit", False)),
        cancelled=cancelled,
    )
    return merged, telemetry


def run_bounded_query_matches(
    query: Query,
    root: Node,
    *,
    windows: Iterable[QueryWindowV1] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    progress_callback: Callable[[object], bool] | None = None,
) -> tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
    """Run `matches` over bounded windows with defensive limits.

    Returns:
        tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
            Query matches merged across windows and execution telemetry.

    Raises:
        RuntimeError: Tree-sitter query cursor bindings are unavailable.
    """
    if _TreeSitterQueryCursor is None:
        msg = "tree_sitter query cursor bindings are unavailable"
        raise RuntimeError(msg)

    effective_settings = settings or QueryExecutionSettingsV1()
    cursor = _TreeSitterQueryCursor(query, match_limit=effective_settings.match_limit)
    if effective_settings.max_start_depth is not None and hasattr(cursor, "set_max_start_depth"):
        cursor.set_max_start_depth(effective_settings.max_start_depth)

    windows_list = _normalized_windows(root, windows)
    merged: list[tuple[int, dict[str, list[Node]]]] = []
    windows_executed = 0
    _ = progress_callback
    cancelled = False

    for window in windows_list:
        cursor.set_byte_range(window.start_byte, window.end_byte)
        matches = cursor.matches(root)
        windows_executed += 1
        if isinstance(matches, list):
            merged.extend(
                (pattern_idx, capture_map)
                for pattern_idx, capture_map in matches
                if isinstance(pattern_idx, int) and isinstance(capture_map, dict)
            )
        if bool(getattr(cursor, "did_exceed_match_limit", False)):
            break

    telemetry = QueryExecutionTelemetryV1(
        windows_total=len(windows_list),
        windows_executed=windows_executed,
        match_count=len(merged),
        exceeded_match_limit=bool(getattr(cursor, "did_exceed_match_limit", False)),
        cancelled=cancelled,
    )
    return merged, telemetry


__all__ = [
    "run_bounded_query_captures",
    "run_bounded_query_matches",
]
