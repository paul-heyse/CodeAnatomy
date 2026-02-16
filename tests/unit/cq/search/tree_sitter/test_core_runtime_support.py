"""Tests for tree-sitter runtime support utilities."""

from __future__ import annotations

from types import SimpleNamespace

from tools.cq.search.tree_sitter.contracts.core_models import (
    AdaptiveRuntimeSnapshotV1,
    QueryPointWindowV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core.runtime_support import (
    apply_byte_window,
    apply_point_window,
    budget_ms_per_anchor,
    build_autotune_plan,
    deadline_from_budget_ms,
    is_deadline_expired,
)

DEFAULT_BUDGET_MS = 200
DEFAULT_MATCH_LIMIT = 4096
HIGH_SPLIT_TARGET = 4
MIN_MATCH_LIMIT = 512
MAX_MATCH_LIMIT = 16_384
BUDGET_LOWER_BOUND = 25
BUDGET_UPPER_BOUND = 2_000

# ── Autotune Tests ──


def test_build_autotune_plan_scales_budget_and_split_target() -> None:
    """Test build autotune plan scales budget and split target."""
    snapshot = AdaptiveRuntimeSnapshotV1(language="query_matches", average_latency_ms=180.0)
    plan = build_autotune_plan(
        snapshot=snapshot,
        default_budget_ms=DEFAULT_BUDGET_MS,
        default_match_limit=DEFAULT_MATCH_LIMIT,
    )
    assert plan.budget_ms >= DEFAULT_BUDGET_MS
    assert plan.window_split_target == HIGH_SPLIT_TARGET
    assert MIN_MATCH_LIMIT <= plan.match_limit <= MAX_MATCH_LIMIT


def test_build_autotune_plan_low_latency_keeps_small_split() -> None:
    """Test build autotune plan low latency keeps small split."""
    snapshot = AdaptiveRuntimeSnapshotV1(language="query_captures", average_latency_ms=40.0)
    plan = build_autotune_plan(
        snapshot=snapshot,
        default_budget_ms=DEFAULT_BUDGET_MS,
        default_match_limit=DEFAULT_MATCH_LIMIT,
    )
    assert plan.window_split_target == 1
    assert plan.match_limit == DEFAULT_MATCH_LIMIT


# ── Window Application Tests ──


def test_apply_byte_window_prefers_containing_range_when_supported() -> None:
    """Test apply byte window prefers containing range when supported."""
    calls: list[tuple[str, int, int]] = []
    cursor = SimpleNamespace(
        set_containing_byte_range=lambda start, end: calls.append(("containing", start, end)),
        set_byte_range=lambda start, end: calls.append(("intersection", start, end)),
    )
    applied = apply_byte_window(
        cursor=cursor,
        window=QueryWindowV1(start_byte=1, end_byte=5),
        mode="containment_preferred",
    )
    assert applied is True
    assert calls == [("containing", 1, 5)]


def test_apply_byte_window_required_without_support_returns_false() -> None:
    """Test apply byte window required without support returns false."""
    calls: list[tuple[int, int]] = []
    cursor = SimpleNamespace(
        set_byte_range=lambda start, end: calls.append((start, end)),
    )
    applied = apply_byte_window(
        cursor=cursor,
        window=QueryWindowV1(start_byte=2, end_byte=8),
        mode="containment_required",
    )
    assert applied is False
    assert calls == []


def test_apply_point_window_falls_back_to_intersection_when_preferred() -> None:
    """Test apply point window falls back to intersection when preferred."""
    calls: list[tuple[tuple[int, int], tuple[int, int]]] = []
    cursor = SimpleNamespace(
        set_point_range=lambda start, end: calls.append((start, end)),
    )
    applied = apply_point_window(
        cursor=cursor,
        window=QueryPointWindowV1(start_row=1, start_col=0, end_row=1, end_col=9),
        mode="containment_preferred",
    )
    assert applied is True
    assert calls == [((1, 0), (1, 9))]


# ── Budget and Deadline Tests ──


def test_budget_ms_per_anchor_is_bounded() -> None:
    """Test budget ms per anchor is bounded."""
    budget = budget_ms_per_anchor(timeout_seconds=30.0, max_anchors=500)
    assert BUDGET_LOWER_BOUND <= budget <= BUDGET_UPPER_BOUND


def test_budget_ms_per_anchor_respects_reserve_fraction() -> None:
    """Test budget ms per anchor respects reserve fraction."""
    large_budget = budget_ms_per_anchor(
        timeout_seconds=10.0,
        max_anchors=10,
        reserve_fraction=0.8,
    )
    small_budget = budget_ms_per_anchor(
        timeout_seconds=10.0,
        max_anchors=10,
        reserve_fraction=0.2,
    )
    assert large_budget > small_budget


def test_deadline_from_budget_ms_returns_none_for_no_budget() -> None:
    """Test deadline from budget ms returns none for no budget."""
    assert deadline_from_budget_ms(None) is None
    assert deadline_from_budget_ms(0) is None
    assert deadline_from_budget_ms(-5) is None


def test_deadline_from_budget_ms_returns_monotonic_time() -> None:
    """Test deadline from budget ms returns monotonic time."""
    deadline = deadline_from_budget_ms(50)
    assert deadline is not None
    assert isinstance(deadline, float)


def test_is_deadline_expired_returns_false_for_none() -> None:
    """Test is deadline expired returns false for none."""
    assert is_deadline_expired(None) is False


def test_is_deadline_expired_detects_past_deadline() -> None:
    """Test is deadline expired detects past deadline."""
    past_deadline = 0.0
    assert is_deadline_expired(past_deadline) is True
