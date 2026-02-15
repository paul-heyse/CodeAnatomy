"""Shared bounded query execution helpers for tree-sitter-based lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from time import monotonic
from typing import TYPE_CHECKING, Any

from tools.cq.search.tree_sitter_runtime_contracts import (
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryPointWindowV1,
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


def _normalized_point_windows(
    point_windows: Iterable[QueryPointWindowV1] | None,
) -> tuple[QueryPointWindowV1, ...] | None:
    if point_windows is None:
        return None
    normalized = tuple(
        window
        for window in point_windows
        if (
            window.end_row > window.start_row
            or (window.end_row == window.start_row and window.end_col > window.start_col)
        )
    )
    return normalized if normalized else None


def _combined_progress_callback(
    *,
    budget_ms: int | None,
    progress_callback: Callable[[object], bool] | None,
) -> Callable[[object], bool] | None:
    callback = progress_callback if callable(progress_callback) else None
    if budget_ms is None or budget_ms <= 0:
        return callback

    deadline = monotonic() + (float(budget_ms) / 1000.0)

    def _budget_callback(state: object) -> bool:
        if monotonic() >= deadline:
            return False
        if callback is None:
            return True
        return bool(callback(state))

    return _budget_callback


def _is_match_contained(
    *,
    capture_map: dict[str, list[Node]],
    window: QueryWindowV1,
) -> bool:
    for values in capture_map.values():
        for node in values:
            node_start = int(getattr(node, "start_byte", 0))
            node_end = int(getattr(node, "end_byte", node_start))
            if node_start < window.start_byte or node_end > window.end_byte:
                return False
    return True


def _build_cursor(query: Query, settings: QueryExecutionSettingsV1) -> Any:
    if _TreeSitterQueryCursor is None:
        msg = "tree_sitter query cursor bindings are unavailable"
        raise RuntimeError(msg)
    cursor = _TreeSitterQueryCursor(query, match_limit=settings.match_limit)
    if settings.max_start_depth is not None and hasattr(cursor, "set_max_start_depth"):
        cursor.set_max_start_depth(settings.max_start_depth)
    return cursor


def _window_plan(
    *,
    root: Node,
    windows: Iterable[QueryWindowV1] | None,
    point_windows: Iterable[QueryPointWindowV1] | None,
) -> tuple[tuple[QueryWindowV1, QueryPointWindowV1 | None], ...]:
    windows_list = _normalized_windows(root, windows)
    point_windows_list = _normalized_point_windows(point_windows)
    total = len(point_windows_list) if point_windows_list is not None else len(windows_list)
    rows: list[tuple[QueryWindowV1, QueryPointWindowV1 | None]] = []
    for index in range(total):
        window = windows_list[index] if index < len(windows_list) else windows_list[-1]
        point_window = None
        if point_windows_list is not None and index < len(point_windows_list):
            point_window = point_windows_list[index]
        rows.append((window, point_window))
    return tuple(rows)


def _apply_window(
    *,
    cursor: Any,
    window: QueryWindowV1,
    point_window: QueryPointWindowV1 | None,
) -> None:
    if point_window is not None:
        cursor.set_point_range(
            (point_window.start_row, point_window.start_col),
            (point_window.end_row, point_window.end_col),
        )
        return
    cursor.set_byte_range(window.start_byte, window.end_byte)


def _cancelled(
    *,
    progress: Callable[[object], bool] | None,
    windows_executed: int,
    windows_total: int,
    exceeded_match_limit: bool,
) -> bool:
    return not exceeded_match_limit and progress is not None and windows_executed < windows_total


def _progress_allows(progress: Callable[[object], bool] | None) -> bool:
    if progress is None:
        return True
    return bool(progress(None))


def run_bounded_query_captures(
    query: Query,
    root: Node,
    *,
    windows: Iterable[QueryWindowV1] | None = None,
    point_windows: Iterable[QueryPointWindowV1] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    progress_callback: Callable[[object], bool] | None = None,
) -> tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]:
    """Run `captures` over bounded windows with defensive limits.

    Returns:
        tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]: Capture map and
            execution telemetry.
    """
    effective_settings = settings or QueryExecutionSettingsV1()
    cursor = _build_cursor(query, effective_settings)
    plan = _window_plan(root=root, windows=windows, point_windows=point_windows)
    progress = _combined_progress_callback(
        budget_ms=effective_settings.budget_ms,
        progress_callback=progress_callback,
    )

    merged: dict[str, list[Node]] = {}
    windows_executed = 0
    for window, point_window in plan:
        _apply_window(cursor=cursor, window=window, point_window=point_window)
        if not _progress_allows(progress):
            break
        captures = cursor.captures(root)
        windows_executed += 1
        if isinstance(captures, dict):
            for capture_name, nodes in captures.items():
                if not isinstance(capture_name, str) or not isinstance(nodes, list):
                    continue
                merged.setdefault(capture_name, []).extend(
                    node for node in nodes if hasattr(node, "start_byte")
                )
        if bool(getattr(cursor, "did_exceed_match_limit", False)):
            break

    exceeded = bool(getattr(cursor, "did_exceed_match_limit", False))
    telemetry = QueryExecutionTelemetryV1(
        windows_total=len(plan),
        windows_executed=windows_executed,
        capture_count=sum(len(nodes) for nodes in merged.values()),
        exceeded_match_limit=exceeded,
        cancelled=_cancelled(
            progress=progress,
            windows_executed=windows_executed,
            windows_total=len(plan),
            exceeded_match_limit=exceeded,
        ),
    )
    return merged, telemetry


def run_bounded_query_matches(
    query: Query,
    root: Node,
    *,
    windows: Iterable[QueryWindowV1] | None = None,
    point_windows: Iterable[QueryPointWindowV1] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    progress_callback: Callable[[object], bool] | None = None,
) -> tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
    """Run `matches` over bounded windows with defensive limits.

    Returns:
        tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
            Match rows and execution telemetry.
    """
    effective_settings = settings or QueryExecutionSettingsV1()
    cursor = _build_cursor(query, effective_settings)
    plan = _window_plan(root=root, windows=windows, point_windows=point_windows)
    progress = _combined_progress_callback(
        budget_ms=effective_settings.budget_ms,
        progress_callback=progress_callback,
    )

    merged: list[tuple[int, dict[str, list[Node]]]] = []
    windows_executed = 0
    for window, point_window in plan:
        _apply_window(cursor=cursor, window=window, point_window=point_window)
        if not _progress_allows(progress):
            break
        matches = cursor.matches(root)
        windows_executed += 1
        if isinstance(matches, list):
            for pattern_idx, capture_map in matches:
                if not isinstance(pattern_idx, int) or not isinstance(capture_map, dict):
                    continue
                if effective_settings.require_containment and not _is_match_contained(
                    capture_map=capture_map,
                    window=window,
                ):
                    continue
                merged.append((pattern_idx, capture_map))
        if bool(getattr(cursor, "did_exceed_match_limit", False)):
            break

    exceeded = bool(getattr(cursor, "did_exceed_match_limit", False))
    telemetry = QueryExecutionTelemetryV1(
        windows_total=len(plan),
        windows_executed=windows_executed,
        match_count=len(merged),
        exceeded_match_limit=exceeded,
        cancelled=_cancelled(
            progress=progress,
            windows_executed=windows_executed,
            windows_total=len(plan),
            exceeded_match_limit=exceeded,
        ),
    )
    return merged, telemetry


__all__ = [
    "run_bounded_query_captures",
    "run_bounded_query_matches",
]
