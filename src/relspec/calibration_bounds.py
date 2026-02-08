"""Bounded threshold ranges for adaptive policy calibration.

Define permissible min/max ranges for each tunable policy threshold.
The calibrator adjusts thresholds within these bounds, ensuring that
adaptive optimization never produces pathological values.
"""

from __future__ import annotations

from serde_msgspec import StructBaseStrict


class CalibrationBounds(StructBaseStrict, frozen=True):
    """Permissible range for each tunable policy threshold.

    Each pair ``(min_*, max_*)`` defines the closed interval within which
    the calibrator is allowed to adjust the corresponding threshold.  The
    calibrator clamps adjusted values to this interval after applying the
    exponential moving average update.

    Parameters
    ----------
    min_high_fanout_threshold
        Lower bound for the high-fanout promotion threshold.
    max_high_fanout_threshold
        Upper bound for the high-fanout promotion threshold.
    min_small_table_row_threshold
        Lower bound for the small-table row count threshold.
    max_small_table_row_threshold
        Upper bound for the small-table row count threshold.
    min_large_table_row_threshold
        Lower bound for the large-table row count threshold.
    max_large_table_row_threshold
        Upper bound for the large-table row count threshold.
    """

    min_high_fanout_threshold: int = 1
    max_high_fanout_threshold: int = 10
    min_small_table_row_threshold: int = 1_000
    max_small_table_row_threshold: int = 100_000
    min_large_table_row_threshold: int = 100_000
    max_large_table_row_threshold: int = 10_000_000


def validate_calibration_bounds(bounds: CalibrationBounds) -> list[str]:
    """Validate that all bound pairs are well-ordered.

    Parameters
    ----------
    bounds
        Calibration bounds to validate.

    Returns:
    -------
    list[str]
        Validation error messages.  Empty when all bounds are valid.
    """
    errors: list[str] = []
    if bounds.min_high_fanout_threshold >= bounds.max_high_fanout_threshold:
        errors.append(
            f"high_fanout_threshold: min ({bounds.min_high_fanout_threshold}) "
            f"must be less than max ({bounds.max_high_fanout_threshold})"
        )
    if bounds.min_small_table_row_threshold >= bounds.max_small_table_row_threshold:
        errors.append(
            f"small_table_row_threshold: min ({bounds.min_small_table_row_threshold}) "
            f"must be less than max ({bounds.max_small_table_row_threshold})"
        )
    if bounds.min_large_table_row_threshold >= bounds.max_large_table_row_threshold:
        errors.append(
            f"large_table_row_threshold: min ({bounds.min_large_table_row_threshold}) "
            f"must be less than max ({bounds.max_large_table_row_threshold})"
        )
    return errors


DEFAULT_CALIBRATION_BOUNDS: CalibrationBounds = CalibrationBounds()
"""Module-level default bounds for callers that need canonical ranges."""


__all__ = [
    "DEFAULT_CALIBRATION_BOUNDS",
    "CalibrationBounds",
    "validate_calibration_bounds",
]
