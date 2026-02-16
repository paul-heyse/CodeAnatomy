"""Tests for tree-sitter runtime autotune helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import AdaptiveRuntimeSnapshotV1
from tools.cq.search.tree_sitter.core.runtime_support import build_autotune_plan

DEFAULT_BUDGET_MS = 200
EXPECTED_WINDOW_SPLIT_TARGET = 4
MIN_MATCH_LIMIT = 512
MAX_MATCH_LIMIT = 16_384
DEFAULT_MATCH_LIMIT = 4096


def test_build_autotune_plan_scales_budget_and_split_target() -> None:
    """Test build autotune plan scales budget and split target."""
    snapshot = AdaptiveRuntimeSnapshotV1(language="query_matches", average_latency_ms=180.0)
    plan = build_autotune_plan(
        snapshot=snapshot,
        default_budget_ms=DEFAULT_BUDGET_MS,
        default_match_limit=DEFAULT_MATCH_LIMIT,
    )
    assert plan.budget_ms >= DEFAULT_BUDGET_MS
    assert plan.window_split_target == EXPECTED_WINDOW_SPLIT_TARGET
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
