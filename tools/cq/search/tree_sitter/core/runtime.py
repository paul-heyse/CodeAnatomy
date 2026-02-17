"""Shared bounded query execution helpers for tree-sitter-based lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Any, cast

from tools.cq.search.tree_sitter.contracts.core_models import (
    NodeLike,
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryPointWindowV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import (
    derive_degrade_reason,
    record_runtime_sample,
    runtime_snapshot,
)
from tools.cq.search.tree_sitter.core.runtime_support import (
    QueryAutotunePlanV1,
    apply_byte_window,
    apply_point_window,
    build_autotune_plan,
)

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


@dataclass(frozen=True, slots=True)
class _BoundedQueryStateV1:
    effective_settings: QueryExecutionSettingsV1
    cursor: object
    windows: tuple[tuple[QueryWindowV1, QueryPointWindowV1 | None], ...]
    base_window_count: int
    progress: Callable[[object], bool] | None
    predicate_callback: Callable[[str, object, int, Mapping[str, Sequence[object]]], bool] | None
    started: float
    deadline: float | None


@dataclass(frozen=True, slots=True)
class _BoundedQueryRequestV1:
    query: Query
    root: Node
    runtime_label: str
    windows: Iterable[QueryWindowV1] | None = None
    point_windows: Iterable[QueryPointWindowV1] | None = None
    settings: QueryExecutionSettingsV1 | None = None
    callbacks: QueryExecutionCallbacksV1 | None = None


def _default_window(root: Node) -> QueryWindowV1:
    start_byte, end_byte = _node_bounds(root)
    return QueryWindowV1(start_byte=start_byte, end_byte=end_byte)


def _node_bounds(node: NodeLike) -> tuple[int, int]:
    return int(node.start_byte), int(node.end_byte)


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
    has_budget = budget_ms is not None and budget_ms > 0
    if callback is None:
        return None
    if not has_budget:
        return callback

    budget_value = budget_ms
    if budget_value is None:
        return callback
    budget_seconds = float(budget_value) / 1000.0
    deadline = monotonic() + budget_seconds

    def _budget_callback(state: object) -> bool:
        if monotonic() >= deadline:
            return False
        assert callback is not None
        return bool(callback(state))

    return _budget_callback


def _autotuned_settings(
    settings: QueryExecutionSettingsV1,
    *,
    runtime_label: str,
) -> tuple[QueryExecutionSettingsV1, QueryAutotunePlanV1]:
    fallback_budget_ms = settings.budget_ms if settings.budget_ms is not None else 200
    snapshot = runtime_snapshot(runtime_label, fallback_budget_ms=fallback_budget_ms)
    plan = build_autotune_plan(
        snapshot=snapshot,
        default_budget_ms=fallback_budget_ms,
        default_match_limit=settings.match_limit,
    )
    default_settings = QueryExecutionSettingsV1()
    tuned_match_limit = (
        plan.match_limit
        if settings.match_limit == default_settings.match_limit
        else settings.match_limit
    )
    tuned_budget = plan.budget_ms if settings.budget_ms is None else settings.budget_ms
    return (
        QueryExecutionSettingsV1(
            match_limit=tuned_match_limit,
            max_start_depth=settings.max_start_depth,
            budget_ms=tuned_budget,
            require_containment=settings.require_containment,
            window_mode=settings.window_mode,
        ),
        plan,
    )


def _is_match_contained(
    *,
    capture_map: dict[str, list[Node]],
    window: QueryWindowV1,
) -> bool:
    for values in capture_map.values():
        for node in values:
            node_start, node_end = _node_bounds(node)
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
    return cursor


def _window_plan(
    *,
    root: Node,
    windows: Iterable[QueryWindowV1] | None,
    point_windows: Iterable[QueryPointWindowV1] | None,
    split_target: int = 1,
) -> tuple[tuple[QueryWindowV1, QueryPointWindowV1 | None], ...]:
    windows_list = _normalized_windows(root, windows)
    point_windows_list = _normalized_point_windows(point_windows)
    if point_windows_list is None and split_target > 1:
        split_windows: list[QueryWindowV1] = []
        for window in windows_list:
            split_windows.extend(_split_window(window, split_target))
        if split_windows:
            windows_list = tuple(split_windows)
    total = len(point_windows_list) if point_windows_list is not None else len(windows_list)
    rows: list[tuple[QueryWindowV1, QueryPointWindowV1 | None]] = []
    for index in range(total):
        window = windows_list[index] if index < len(windows_list) else windows_list[-1]
        point_window = None
        if point_windows_list is not None and index < len(point_windows_list):
            point_window = point_windows_list[index]
        rows.append((window, point_window))
    return tuple(rows)


def _split_window(window: QueryWindowV1, split_target: int) -> tuple[QueryWindowV1, ...]:
    if split_target <= 1:
        return (window,)
    start = int(window.start_byte)
    end = int(window.end_byte)
    width = end - start
    if width <= 1:
        return (window,)
    step = max(1, width // split_target)
    rows: list[QueryWindowV1] = []
    cursor = start
    while cursor < end:
        next_cursor = min(end, cursor + step)
        if next_cursor <= cursor:
            break
        rows.append(QueryWindowV1(start_byte=cursor, end_byte=next_cursor))
        cursor = next_cursor
    if not rows:
        return (window,)
    last = rows[-1]
    if last.end_byte < end:
        rows[-1] = QueryWindowV1(start_byte=last.start_byte, end_byte=end)
    return tuple(rows)


def _base_window_count(
    *,
    root: Node,
    windows: Iterable[QueryWindowV1] | None,
    point_windows: Iterable[QueryPointWindowV1] | None,
) -> int:
    point_rows = _normalized_point_windows(point_windows)
    if point_rows is not None:
        return len(point_rows)
    return len(_normalized_windows(root, windows))


def _apply_window(
    *,
    cursor: Any,
    window: QueryWindowV1,
    point_window: QueryPointWindowV1 | None,
    window_mode: str,
) -> bool:
    if point_window is not None:
        return apply_point_window(
            cursor=cursor,
            window=point_window,
            mode=window_mode,
        )
    return apply_byte_window(cursor=cursor, window=window, mode=window_mode)


def _match_fingerprint(pattern_idx: int, capture_map: dict[str, list[Node]]) -> tuple[object, ...]:
    rows: list[tuple[str, tuple[tuple[int, int], ...]]] = []
    for capture_name in sorted(capture_map):
        nodes = capture_map[capture_name]
        spans = tuple(_node_bounds(node) for node in nodes)
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


def _include_capture_node(
    node: object,
    *,
    has_change_context: bool,
) -> bool:
    if not has_change_context:
        return True
    has_changes = getattr(node, "has_changes", None)
    if has_changes is None:
        return True
    try:
        return bool(has_changes)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return True


def _has_byte_span(node: object) -> bool:
    try:
        start, end = _node_bounds(cast("NodeLike", node))
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return False
    return end >= start


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


def _merge_capture_map(
    *,
    merged: dict[str, list[Node]],
    captures: object,
    has_change_context: bool,
) -> None:
    if not isinstance(captures, dict):
        return
    for capture_name, nodes in captures.items():
        if not isinstance(capture_name, str) or not isinstance(nodes, list):
            continue
        filtered = [
            node
            for node in nodes
            if _has_byte_span(node)
            and _include_capture_node(node, has_change_context=has_change_context)
        ]
        if filtered:
            merged.setdefault(capture_name, []).extend(cast("list[Node]", filtered))


def _prepare_bounded_query_state(
    *,
    request: _BoundedQueryRequestV1,
) -> _BoundedQueryStateV1:
    effective_settings, tune_plan = _autotuned_settings(
        request.settings or QueryExecutionSettingsV1(),
        runtime_label=request.runtime_label,
    )
    callback_bundle = request.callbacks or QueryExecutionCallbacksV1()
    started = monotonic()
    deadline = (
        started + (float(effective_settings.budget_ms) / 1000.0)
        if effective_settings.budget_ms is not None and effective_settings.budget_ms > 0
        else None
    )
    plan = _window_plan(
        root=request.root,
        windows=request.windows,
        point_windows=request.point_windows,
        split_target=tune_plan.window_split_target,
    )
    return _BoundedQueryStateV1(
        effective_settings=effective_settings,
        cursor=_build_cursor(request.query, effective_settings),
        windows=plan,
        base_window_count=_base_window_count(
            root=request.root,
            windows=request.windows,
            point_windows=request.point_windows,
        ),
        progress=_combined_progress_callback(
            budget_ms=effective_settings.budget_ms,
            progress_callback=callback_bundle.progress_callback,
        ),
        predicate_callback=callback_bundle.predicate_callback,
        started=started,
        deadline=deadline,
    )


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
    state = _prepare_bounded_query_state(
        request=_BoundedQueryRequestV1(
            query=query,
            root=root,
            windows=windows,
            point_windows=point_windows,
            settings=settings,
            callbacks=callbacks,
            runtime_label="query_captures",
        )
    )

    merged: dict[str, list[Node]] = {}
    windows_executed = 0
    degrade_reason: str | None = None
    budget_cancelled = False
    for window, point_window in state.windows:
        if state.deadline is not None and monotonic() >= state.deadline:
            budget_cancelled = True
            break
        window_applied = _apply_window(
            cursor=state.cursor,
            window=window,
            point_window=point_window,
            window_mode=state.effective_settings.window_mode,
        )
        if not window_applied and state.effective_settings.window_mode == "containment_required":
            degrade_reason = derive_degrade_reason(
                exceeded_match_limit=False,
                cancelled=False,
                containment_required=True,
                window_applied=False,
            )
            break
        if not _progress_allows(state.progress):
            break
        captures = _cursor_captures(
            cursor=state.cursor,
            root=root,
            predicate_callback=state.predicate_callback,
            progress=state.progress,
        )
        windows_executed += 1
        _merge_capture_map(
            merged=merged,
            captures=captures,
            has_change_context=state.effective_settings.has_change_context,
        )
        if bool(getattr(state.cursor, "did_exceed_match_limit", False)):
            break

    exceeded = bool(getattr(state.cursor, "did_exceed_match_limit", False))
    cancelled = _cancelled(
        progress=state.progress,
        windows_executed=windows_executed,
        windows_total=len(state.windows),
        exceeded_match_limit=exceeded,
    )
    cancelled = cancelled or budget_cancelled
    if degrade_reason is None:
        degrade_reason = derive_degrade_reason(
            exceeded_match_limit=exceeded,
            cancelled=cancelled,
        )
    telemetry = QueryExecutionTelemetryV1(
        windows_total=len(state.windows),
        windows_executed=windows_executed,
        capture_count=sum(len(nodes) for nodes in merged.values()),
        exceeded_match_limit=exceeded,
        cancelled=cancelled,
        window_split_count=max(0, len(state.windows) - state.base_window_count),
        degrade_reason=degrade_reason,
    )
    record_runtime_sample("query_captures", (monotonic() - state.started) * 1000.0)
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
    state = _prepare_bounded_query_state(
        request=_BoundedQueryRequestV1(
            query=query,
            root=root,
            windows=windows,
            point_windows=point_windows,
            settings=settings,
            callbacks=callbacks,
            runtime_label="query_matches",
        )
    )

    merged: list[tuple[int, dict[str, list[Node]]]] = []
    seen_match_fingerprints: set[tuple[object, ...]] = set()
    windows_executed = 0
    degrade_reason: str | None = None
    budget_cancelled = False
    for window, point_window in state.windows:
        if state.deadline is not None and monotonic() >= state.deadline:
            budget_cancelled = True
            break
        window_applied = _apply_window(
            cursor=state.cursor,
            window=window,
            point_window=point_window,
            window_mode=state.effective_settings.window_mode,
        )
        if not window_applied and state.effective_settings.window_mode == "containment_required":
            degrade_reason = derive_degrade_reason(
                exceeded_match_limit=False,
                cancelled=False,
                containment_required=True,
                window_applied=False,
            )
            break
        if not _progress_allows(state.progress):
            break
        matches = _cursor_matches(
            cursor=state.cursor,
            root=root,
            predicate_callback=state.predicate_callback,
            progress=state.progress,
        )
        windows_executed += 1
        _merge_window_matches(
            matches=matches,
            require_containment=state.effective_settings.require_containment,
            window=window,
            seen_match_fingerprints=seen_match_fingerprints,
            merged=merged,
        )
        if bool(getattr(state.cursor, "did_exceed_match_limit", False)):
            break

    exceeded = bool(getattr(state.cursor, "did_exceed_match_limit", False))
    cancelled = _cancelled(
        progress=state.progress,
        windows_executed=windows_executed,
        windows_total=len(state.windows),
        exceeded_match_limit=exceeded,
    )
    cancelled = cancelled or budget_cancelled
    if degrade_reason is None:
        degrade_reason = derive_degrade_reason(
            exceeded_match_limit=exceeded,
            cancelled=cancelled,
        )
    telemetry = QueryExecutionTelemetryV1(
        windows_total=len(state.windows),
        windows_executed=windows_executed,
        match_count=len(merged),
        exceeded_match_limit=exceeded,
        cancelled=cancelled,
        window_split_count=max(0, len(state.windows) - state.base_window_count),
        degrade_reason=degrade_reason,
    )
    record_runtime_sample("query_matches", (monotonic() - state.started) * 1000.0)
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
        if (
            window is not None
            and require_containment
            and not _is_match_contained(capture_map=capture_map, window=window)
        ):
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
