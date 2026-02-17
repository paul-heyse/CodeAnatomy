"""Front-door risk helpers."""

from __future__ import annotations

from tools.cq.core.front_door_contracts import (
    DEFAULT_INSIGHT_THRESHOLDS,
    InsightRiskCountersV1,
    InsightRiskV1,
    RiskLevel,
)


def risk_from_counters(counters: InsightRiskCountersV1) -> InsightRiskV1:
    """Build risk payload from deterministic counters.

    Returns:
        Risk level, drivers, and counters derived from threshold policy.
    """
    drivers: list[str] = []
    if counters.callers >= DEFAULT_INSIGHT_THRESHOLDS.high_caller_threshold:
        drivers.append("high_call_surface")
    elif counters.callers >= DEFAULT_INSIGHT_THRESHOLDS.medium_caller_threshold:
        drivers.append("medium_call_surface")

    if counters.forwarding_count > 0:
        drivers.append("argument_forwarding")

    if counters.hazard_count > 0:
        drivers.append("dynamic_hazards")

    if counters.arg_shape_count > DEFAULT_INSIGHT_THRESHOLDS.arg_variance_threshold:
        drivers.append("arg_shape_variance")

    if counters.closure_capture_count > 0:
        drivers.append("closure_capture")

    level = _risk_level_from_counters(counters)
    return InsightRiskV1(level=level, drivers=tuple(drivers), counters=counters)


def _risk_level_from_counters(counters: InsightRiskCountersV1) -> RiskLevel:
    if (
        counters.callers > DEFAULT_INSIGHT_THRESHOLDS.high_caller_strict_threshold
        or counters.hazard_count > 0
        or (counters.forwarding_count > 0 and counters.callers > 0)
    ):
        return "high"
    if (
        counters.callers > DEFAULT_INSIGHT_THRESHOLDS.medium_caller_strict_threshold
        or counters.arg_shape_count > DEFAULT_INSIGHT_THRESHOLDS.arg_variance_threshold
        or counters.files_with_calls > DEFAULT_INSIGHT_THRESHOLDS.files_with_calls_threshold
        or counters.closure_capture_count > 0
    ):
        return "med"
    return "low"


__all__ = ["risk_from_counters"]
