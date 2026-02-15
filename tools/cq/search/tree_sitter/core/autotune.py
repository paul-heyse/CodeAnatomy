"""Telemetry-driven runtime autotuning helpers for tree-sitter queries."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import AdaptiveRuntimeSnapshotV1

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


__all__ = ["QueryAutotunePlanV1", "build_autotune_plan"]
