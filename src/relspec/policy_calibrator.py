"""Closed-loop policy calibration from execution outcome metrics.

Read post-execution metrics (predicted vs actual costs, timing data)
and produce bounded threshold adjustments for the next compilation
cycle.  The calibrator uses an exponential moving average (EMA) to
smooth adjustments, clamped within ``CalibrationBounds``.

Three modes control rollout:

- ``"off"``     -- return current thresholds unchanged
- ``"warn"``    -- compute recommendations without applying
- ``"enforce"`` -- produce adjusted thresholds for next cycle

Legacy aliases are accepted for compatibility:
``"observe" -> "warn"`` and ``"apply" -> "enforce"``.
"""

from __future__ import annotations

from typing import Literal

from relspec.calibration_bounds import CalibrationBounds, validate_calibration_bounds
from relspec.inference_confidence import InferenceConfidence, high_confidence, low_confidence
from serde_msgspec import StructBaseStrict

CalibrationMode = Literal["off", "warn", "enforce", "observe", "apply"]

# EMA smoothing factor.  A value of 0.3 gives recent observations 30%
# weight while retaining 70% of the prior estimate.
_EMA_ALPHA = 0.3

# Minimum number of observations needed before calibration can produce
# a high-confidence adjustment.
_MIN_EVIDENCE_COUNT = 5


class CalibrationThresholds(StructBaseStrict, frozen=True):
    """Snapshot of tunable policy thresholds subject to calibration.

    Parameters
    ----------
    high_fanout_threshold
        Fan-out degree above which an intermediate node is promoted to
        ``delta_staging`` cache policy.
    small_table_row_threshold
        Row count below which a table is classified as small (full scan).
    large_table_row_threshold
        Row count above which a table is classified as large.
    """

    high_fanout_threshold: int = 2
    small_table_row_threshold: int = 10_000
    large_table_row_threshold: int = 1_000_000


class ExecutionMetricsSummary(StructBaseStrict, frozen=True):
    """Summarised execution metrics for calibration input.

    Parameters
    ----------
    predicted_cost
        Total predicted cost from the scheduling cost model.
    actual_cost
        Total actual cost observed during execution.
    observation_count
        Number of task executions contributing to this summary.
    mean_duration_ms
        Mean task duration in milliseconds.
    mean_row_count
        Mean output row count across tasks, or ``None`` when unavailable.
    """

    predicted_cost: float
    actual_cost: float
    observation_count: int
    mean_duration_ms: float | None = None
    mean_row_count: float | None = None


class PolicyCalibrationResult(StructBaseStrict, frozen=True):
    """Result of a single calibration cycle.

    Parameters
    ----------
    adjusted_thresholds
        Thresholds after calibration (clamped within bounds).
    evidence_summary
        Human-readable summary of the evidence used.
    calibration_confidence
        Confidence metadata for the calibration decision.
    mode
        Calibration mode that produced this result.
    cost_ratio
        Ratio of actual to predicted cost (``actual / predicted``).
        Values above 1.0 indicate under-estimation; below 1.0
        indicate over-estimation.
    bounded
        True when any threshold was clamped to a bound limit.
    """

    adjusted_thresholds: CalibrationThresholds
    evidence_summary: str
    calibration_confidence: InferenceConfidence
    mode: CalibrationMode
    cost_ratio: float | None = None
    bounded: bool = False


def calibrate_from_execution_metrics(
    *,
    metrics: ExecutionMetricsSummary,
    current_thresholds: CalibrationThresholds,
    bounds: CalibrationBounds,
    mode: CalibrationMode = "off",
) -> PolicyCalibrationResult:
    """Produce adjusted thresholds from execution feedback.

    Compare predicted vs actual costs and derive threshold adjustments
    using an exponential moving average, clamped within bounds.

    Args:
        metrics: Summarised execution outcome metrics.
        current_thresholds: Currently active policy thresholds.
        bounds: Permissible adjustment ranges.
        mode: Calibration rollout mode.

    Returns:
        Calibration outcome with adjusted thresholds and confidence.

    Raises:
        ValueError: When ``bounds`` are not well-ordered.
    """
    bound_errors = validate_calibration_bounds(bounds)
    if bound_errors:
        msg = f"Invalid calibration bounds: {'; '.join(bound_errors)}"
        raise ValueError(msg)

    if mode == "off":
        return _result_unchanged(current_thresholds, mode=mode)

    if metrics.predicted_cost <= 0.0:
        return _result_unchanged(
            current_thresholds,
            mode=mode,
            evidence_summary="predicted_cost is zero or negative; skipping calibration",
        )

    cost_ratio = metrics.actual_cost / metrics.predicted_cost
    direction = _calibration_direction(cost_ratio)
    evidence_parts = [
        f"cost_ratio={cost_ratio:.3f}",
        f"observations={metrics.observation_count}",
        f"direction={direction}",
    ]

    adjusted, was_bounded = _adjust_thresholds(
        current=current_thresholds,
        bounds=bounds,
        cost_ratio=cost_ratio,
    )

    if mode in {"observe", "warn"}:
        resolved_mode = "warn" if mode == "warn" else "observe"
        confidence = _build_calibration_confidence(
            metrics=metrics,
            decision_value=resolved_mode,
        )
        return PolicyCalibrationResult(
            adjusted_thresholds=adjusted,
            evidence_summary="; ".join(evidence_parts),
            calibration_confidence=confidence,
            mode=resolved_mode,
            cost_ratio=cost_ratio,
            bounded=was_bounded,
        )

    resolved_mode = "enforce" if mode == "enforce" else "apply"
    confidence = _build_calibration_confidence(
        metrics=metrics,
        decision_value=resolved_mode,
    )
    return PolicyCalibrationResult(
        adjusted_thresholds=adjusted,
        evidence_summary="; ".join(evidence_parts),
        calibration_confidence=confidence,
        mode=resolved_mode,
        cost_ratio=cost_ratio,
        bounded=was_bounded,
    )


def _result_unchanged(
    thresholds: CalibrationThresholds,
    *,
    mode: CalibrationMode,
    evidence_summary: str = "calibration disabled",
) -> PolicyCalibrationResult:
    return PolicyCalibrationResult(
        adjusted_thresholds=thresholds,
        evidence_summary=evidence_summary,
        calibration_confidence=low_confidence(
            "calibration",
            "unchanged",
            evidence_summary,
            (),
            score=0.0,
        ),
        mode=mode,
        cost_ratio=None,
        bounded=False,
    )


_UNDER_ESTIMATION_THRESHOLD = 1.1
_OVER_ESTIMATION_THRESHOLD = 0.9


def _calibration_direction(cost_ratio: float) -> str:
    """Classify the cost ratio into a human-readable direction label.

    Returns:
    -------
    str
        One of ``"under_estimated"``, ``"over_estimated"``, or
        ``"well_calibrated"``.
    """
    if cost_ratio > _UNDER_ESTIMATION_THRESHOLD:
        return "under_estimated"
    if cost_ratio < _OVER_ESTIMATION_THRESHOLD:
        return "over_estimated"
    return "well_calibrated"


def _adjust_thresholds(
    *,
    current: CalibrationThresholds,
    bounds: CalibrationBounds,
    cost_ratio: float,
) -> tuple[CalibrationThresholds, bool]:
    """Apply EMA adjustment and clamp within bounds.

    When actual cost exceeds predicted (under-estimation), raise
    thresholds to be more conservative.  When actual cost is lower
    (over-estimation), lower thresholds to be more aggressive.

    Returns:
    -------
    tuple[CalibrationThresholds, bool]
        Adjusted thresholds and whether any value was clamped.
    """
    # Scale factor: >1 when under-estimating, <1 when over-estimating.
    scale = cost_ratio

    new_high_fanout, hf_bounded = _ema_int(
        current.high_fanout_threshold,
        scale,
        bounds.min_high_fanout_threshold,
        bounds.max_high_fanout_threshold,
    )
    new_small_table, st_bounded = _ema_int(
        current.small_table_row_threshold,
        scale,
        bounds.min_small_table_row_threshold,
        bounds.max_small_table_row_threshold,
    )
    new_large_table, lt_bounded = _ema_int(
        current.large_table_row_threshold,
        scale,
        bounds.min_large_table_row_threshold,
        bounds.max_large_table_row_threshold,
    )

    was_bounded = hf_bounded or st_bounded or lt_bounded
    adjusted = CalibrationThresholds(
        high_fanout_threshold=new_high_fanout,
        small_table_row_threshold=new_small_table,
        large_table_row_threshold=new_large_table,
    )
    return adjusted, was_bounded


def _ema_int(
    current: int,
    scale: float,
    lower: int,
    upper: int,
) -> tuple[int, bool]:
    """Apply exponential moving average with integer rounding and clamping.

    The target value is ``current * scale``.  The EMA blends the target
    toward the current value, then rounds and clamps to ``[lower, upper]``.

    Returns:
    -------
    tuple[int, bool]
        Clamped value and whether clamping was applied.
    """
    target = current * scale
    blended = _EMA_ALPHA * target + (1 - _EMA_ALPHA) * current
    rounded = round(blended)
    clamped = max(lower, min(upper, rounded))
    return clamped, clamped != rounded


def _build_calibration_confidence(
    *,
    metrics: ExecutionMetricsSummary,
    decision_value: str,
) -> InferenceConfidence:
    """Build confidence metadata from observation evidence.

    Returns:
    -------
    InferenceConfidence
        High confidence when observations exceed threshold, low otherwise.
    """
    sources: list[str] = ["execution_metrics"]
    if metrics.mean_duration_ms is not None:
        sources.append("duration")
    if metrics.mean_row_count is not None:
        sources.append("row_count")

    evidence = tuple(sources)

    if metrics.observation_count >= _MIN_EVIDENCE_COUNT:
        return high_confidence(
            "calibration",
            decision_value,
            evidence,
            score=min(0.8 + 0.02 * metrics.observation_count, 1.0),
        )

    return low_confidence(
        "calibration",
        decision_value,
        f"insufficient observations ({metrics.observation_count} < {_MIN_EVIDENCE_COUNT})",
        evidence,
        score=0.3 + 0.03 * metrics.observation_count,
    )


__all__ = [
    "CalibrationMode",
    "CalibrationThresholds",
    "ExecutionMetricsSummary",
    "PolicyCalibrationResult",
    "calibrate_from_execution_metrics",
]
