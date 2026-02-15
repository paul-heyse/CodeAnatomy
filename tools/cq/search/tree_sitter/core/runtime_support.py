"""Runtime support utilities for tree-sitter query execution.

This module consolidates query autotuning, windowing constraint helpers,
and budget/deadline management for tree-sitter-based search lanes.
"""

from __future__ import annotations

from time import monotonic
from typing import Any

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import (
    AdaptiveRuntimeSnapshotV1,
    QueryPointWindowV1,
    QueryWindowV1,
)

# ── Query Autotuning ──


_HIGH_LATENCY_MS = 120.0
_LIGHT_SPLIT_LATENCY_MS = 100.0


class QueryAutotunePlanV1(CqStruct, frozen=True):
    """Derived runtime tuning plan for one query execution lane."""

    budget_ms: int
    match_limit: int
    window_split_target: int


def build_autotune_plan(
    *,
    snapshot: AdaptiveRuntimeSnapshotV1,
    default_budget_ms: int,
    default_match_limit: int,
) -> QueryAutotunePlanV1:
    """Build an execution tuning plan from runtime snapshot telemetry."""
    average_latency_ms = max(0.0, float(snapshot.average_latency_ms))
    budget = max(50, min(2_000, int(max(float(default_budget_ms), average_latency_ms * 4.0))))
    if average_latency_ms >= _HIGH_LATENCY_MS:
        tuned_match_limit = max(512, min(16_384, int(default_match_limit // 2)))
    else:
        tuned_match_limit = max(512, min(16_384, int(default_match_limit)))
    split_target = 1 if average_latency_ms < _LIGHT_SPLIT_LATENCY_MS else 4
    return QueryAutotunePlanV1(
        budget_ms=budget,
        match_limit=tuned_match_limit,
        window_split_target=split_target,
    )


# ── Window Constraint Helpers ──


def apply_point_window(
    *,
    cursor: Any,
    window: QueryPointWindowV1,
    mode: str,
) -> bool:
    """Apply a point window to a query cursor.

    Returns:
    -------
    bool
        ``True`` when a window API was applied.
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
    -------
    bool
        ``True`` when a window API was applied.
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


# ── Budget and Deadline Helpers ──


def budget_ms_per_anchor(
    *,
    timeout_seconds: float,
    max_anchors: int,
    reserve_fraction: float = 0.5,
    min_budget_ms: int = 25,
    max_budget_ms: int = 2_000,
) -> int:
    """Derive a bounded per-anchor query budget from request timeout bounds.

    Returns:
    -------
    int
        Per-anchor budget in milliseconds.
    """
    safe_timeout = max(0.1, float(timeout_seconds))
    safe_anchors = max(1, int(max_anchors))
    usable_seconds = safe_timeout * max(0.05, min(0.95, reserve_fraction))
    budget = int((usable_seconds * 1000.0) / safe_anchors)
    return max(min_budget_ms, min(max_budget_ms, budget))


def deadline_from_budget_ms(budget_ms: int | None) -> float | None:
    """Return a monotonic deadline, or ``None`` when no budget is enforced."""
    if budget_ms is None or budget_ms <= 0:
        return None
    return monotonic() + (float(budget_ms) / 1000.0)


def is_deadline_expired(deadline: float | None) -> bool:
    """Return whether a monotonic deadline has elapsed."""
    if deadline is None:
        return False
    return monotonic() >= deadline


__all__ = [
    "QueryAutotunePlanV1",
    "apply_byte_window",
    "apply_point_window",
    "budget_ms_per_anchor",
    "build_autotune_plan",
    "deadline_from_budget_ms",
    "is_deadline_expired",
]
