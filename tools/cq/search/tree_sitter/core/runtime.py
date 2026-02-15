"""Shared bounded query execution helpers for tree-sitter-based lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Any, cast

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryPointWindowV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import record_runtime_sample

if TYPE_CHECKING:
    from tree_sitter import Node, Query

try:
    from tree_sitter import QueryCursor as _TreeSitterQueryCursor
except ImportError:  # pragma: no cover - optional dependency guard
    _TreeSitterQueryCursor = None


@dataclass(frozen=True, slots=True)
class QueryExecutionCallbacksV1:
    """Optional callback bundle for query runtime execution."""

    progress_callback: Callable[[object], bool] | None = None
    predicate_callback: (
        Callable[[str, object, int, Mapping[str, Sequence[object]]], bool] | None
    ) = None


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
    if callback is None:
        # QueryCursor progress callbacks can be unstable across versions.
        # Keep the hot path callback-free unless an explicit callback was provided.
        return None
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
    cursor_any = cast("Any", cursor)
    if settings.max_start_depth is not None and hasattr(cursor_any, "set_max_start_depth"):
        cursor_any.set_max_start_depth(settings.max_start_depth)
    timeout_micros: int | None = settings.timeout_micros
    if timeout_micros is None and settings.budget_ms is not None and settings.budget_ms > 0:
        timeout_micros = int(settings.budget_ms) * 1_000
    if timeout_micros is not None and hasattr(cursor_any, "set_timeout_micros"):
        cursor_any.set_timeout_micros(timeout_micros)
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
    _ = window
    # Avoid QueryCursor.set_byte_range due native instability on some trees.
    # We apply window filtering in Python after query execution instead.
    return


def _node_overlaps_window(node: Node, window: QueryWindowV1) -> bool:
    node_start = int(getattr(node, "start_byte", 0))
    node_end = int(getattr(node, "end_byte", node_start))
    return node_end > window.start_byte and node_start < window.end_byte


def _capture_map_overlaps_window(
    *,
    capture_map: dict[str, list[Node]],
    window: QueryWindowV1,
) -> bool:
    for values in capture_map.values():
        for node in values:
            if _node_overlaps_window(node, window):
                return True
    return False


def _filter_captures_by_window(
    *,
    captures: dict[str, list[Node]],
    window: QueryWindowV1,
) -> dict[str, list[Node]]:
    out: dict[str, list[Node]] = {}
    for capture_name, nodes in captures.items():
        filtered = [node for node in nodes if _node_overlaps_window(node, window)]
        if filtered:
            out[capture_name] = filtered
    return out


def _match_fingerprint(pattern_idx: int, capture_map: dict[str, list[Node]]) -> tuple[object, ...]:
    rows: list[tuple[str, tuple[tuple[int, int], ...]]] = []
    for capture_name in sorted(capture_map):
        nodes = capture_map[capture_name]
        spans = tuple(
            (int(getattr(node, "start_byte", 0)), int(getattr(node, "end_byte", 0)))
            for node in nodes
        )
        rows.append((capture_name, spans))
    return (pattern_idx, tuple(rows))


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


def _cursor_captures(
    *,
    cursor: Any,
    root: Node,
    predicate_callback: Callable[[str, object, int, Mapping[str, Sequence[object]]], bool] | None,
    progress: Callable[[object], bool] | None,
) -> dict[str, list[Node]]:
    if predicate_callback is not None or progress is not None:
        try:
            return cursor.captures(
                root,
                predicate=predicate_callback,
                progress_callback=progress,
            )
        except TypeError:
            pass
    if progress is not None:
        try:
            return cursor.captures(root, progress_callback=progress)
        except TypeError:
            pass
    return cursor.captures(root)


def _cursor_matches(
    *,
    cursor: Any,
    root: Node,
    predicate_callback: Callable[[str, object, int, Mapping[str, Sequence[object]]], bool] | None,
    progress: Callable[[object], bool] | None,
) -> list[tuple[int, dict[str, list[Node]]]]:
    if predicate_callback is not None or progress is not None:
        try:
            return cursor.matches(
                root,
                predicate=predicate_callback,
                progress_callback=progress,
            )
        except TypeError:
            pass
    if progress is not None:
        try:
            return cursor.matches(root, progress_callback=progress)
        except TypeError:
            pass
    return cursor.matches(root)


def run_bounded_query_captures(
    query: Query,
    root: Node,
    *,
    windows: Iterable[QueryWindowV1] | None = None,
    point_windows: Iterable[QueryPointWindowV1] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    callbacks: QueryExecutionCallbacksV1 | None = None,
) -> tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]:
    """Run `captures` over bounded windows with defensive limits.

    Returns:
        tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]: Capture map and
            execution telemetry.
    """
    effective_settings = settings or QueryExecutionSettingsV1()
    callback_bundle = callbacks or QueryExecutionCallbacksV1()
    cursor = _build_cursor(query, effective_settings)
    plan = _window_plan(root=root, windows=windows, point_windows=point_windows)
    progress = _combined_progress_callback(
        budget_ms=effective_settings.budget_ms,
        progress_callback=callback_bundle.progress_callback,
    )
    started = monotonic()

    merged: dict[str, list[Node]] = {}
    windows_executed = 0
    for window, point_window in plan:
        _apply_window(cursor=cursor, window=window, point_window=point_window)
        if not _progress_allows(progress):
            break
        raw_captures = _cursor_captures(
            cursor=cursor,
            root=root,
            predicate_callback=callback_bundle.predicate_callback,
            progress=progress,
        )
        captures = _filter_captures_by_window(captures=raw_captures, window=window)
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
    record_runtime_sample("query_captures", (monotonic() - started) * 1000.0)
    return merged, telemetry


def run_bounded_query_matches(
    query: Query,
    root: Node,
    *,
    windows: Iterable[QueryWindowV1] | None = None,
    point_windows: Iterable[QueryPointWindowV1] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    callbacks: QueryExecutionCallbacksV1 | None = None,
) -> tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
    """Run `matches` over bounded windows with defensive limits.

    Returns:
        tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
            Match rows and execution telemetry.
    """
    effective_settings = settings or QueryExecutionSettingsV1()
    callback_bundle = callbacks or QueryExecutionCallbacksV1()
    cursor = _build_cursor(query, effective_settings)
    plan = _window_plan(root=root, windows=windows, point_windows=point_windows)
    progress = _combined_progress_callback(
        budget_ms=effective_settings.budget_ms,
        progress_callback=callback_bundle.progress_callback,
    )
    started = monotonic()

    merged: list[tuple[int, dict[str, list[Node]]]] = []
    seen_match_fingerprints: set[tuple[object, ...]] = set()
    windows_executed = 0
    for window, point_window in plan:
        _apply_window(cursor=cursor, window=window, point_window=point_window)
        if not _progress_allows(progress):
            break
        matches = _cursor_matches(
            cursor=cursor,
            root=root,
            predicate_callback=callback_bundle.predicate_callback,
            progress=progress,
        )
        windows_executed += 1
        _merge_window_matches(
            matches=matches,
            require_containment=effective_settings.require_containment,
            window=window,
            seen_match_fingerprints=seen_match_fingerprints,
            merged=merged,
        )
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
    record_runtime_sample("query_matches", (monotonic() - started) * 1000.0)
    return merged, telemetry


def _merge_window_matches(
    *,
    matches: object,
    require_containment: bool,
    window: QueryWindowV1 | None,
    seen_match_fingerprints: set[tuple[object, ...]],
    merged: list[tuple[int, dict[str, list[Node]]]],
) -> None:
    if not isinstance(matches, list):
        return
    for pattern_idx, capture_map in matches:
        if not isinstance(pattern_idx, int) or not isinstance(capture_map, dict):
            continue
        if window is not None:
            if require_containment:
                if not _is_match_contained(capture_map=capture_map, window=window):
                    continue
            elif not _capture_map_overlaps_window(capture_map=capture_map, window=window):
                continue
        fingerprint = _match_fingerprint(pattern_idx, capture_map)
        if fingerprint in seen_match_fingerprints:
            continue
        seen_match_fingerprints.add(fingerprint)
        merged.append((pattern_idx, capture_map))


__all__ = [
    "QueryExecutionCallbacksV1",
    "run_bounded_query_captures",
    "run_bounded_query_matches",
]
