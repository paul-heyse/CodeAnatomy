"""Tests for tree-sitter runtime autotune helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import AdaptiveRuntimeSnapshotV1
from tools.cq.search.tree_sitter.core.autotune import build_autotune_plan


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
