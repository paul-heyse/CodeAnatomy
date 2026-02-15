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

# ── Autotune Tests ──


def test_build_autotune_plan_scales_budget_and_split_target() -> None:
    snapshot = AdaptiveRuntimeSnapshotV1(language="query_matches", average_latency_ms=180.0)
    plan = build_autotune_plan(
        snapshot=snapshot,
        default_budget_ms=200,
        default_match_limit=4096,
    )
    assert plan.budget_ms >= 200
    assert plan.window_split_target == 4
    assert 512 <= plan.match_limit <= 16_384


def test_build_autotune_plan_low_latency_keeps_small_split() -> None:
    snapshot = AdaptiveRuntimeSnapshotV1(language="query_captures", average_latency_ms=40.0)
    plan = build_autotune_plan(
        snapshot=snapshot,
        default_budget_ms=200,
        default_match_limit=4096,
    )
    assert plan.window_split_target == 1
    assert plan.match_limit == 4096


# ── Window Application Tests ──


def test_apply_byte_window_prefers_containing_range_when_supported() -> None:
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
    budget = budget_ms_per_anchor(timeout_seconds=30.0, max_anchors=500)
    assert 25 <= budget <= 2_000


def test_budget_ms_per_anchor_respects_reserve_fraction() -> None:
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
    assert deadline_from_budget_ms(None) is None
    assert deadline_from_budget_ms(0) is None
    assert deadline_from_budget_ms(-5) is None


def test_deadline_from_budget_ms_returns_monotonic_time() -> None:
    deadline = deadline_from_budget_ms(50)
    assert deadline is not None
    assert isinstance(deadline, float)


def test_is_deadline_expired_returns_false_for_none() -> None:
    assert is_deadline_expired(None) is False


def test_is_deadline_expired_detects_past_deadline() -> None:
    past_deadline = 0.0
    assert is_deadline_expired(past_deadline) is True
