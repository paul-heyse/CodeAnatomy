"""Tests for calibration bounds and policy calibrator."""

from __future__ import annotations

import pytest

from relspec.calibration_bounds import (
    DEFAULT_CALIBRATION_BOUNDS,
    CalibrationBounds,
    validate_calibration_bounds,
)
from relspec.inference_confidence import InferenceConfidence
from relspec.policy_calibrator import (
    CalibrationThresholds,
    ExecutionMetricsSummary,
    calibrate_from_execution_metrics,
)
from tests.test_helpers.immutability import assert_immutable_assignment

DEFAULT_HIGH_FANOUT_MAX = 10
DEFAULT_SMALL_TABLE_MIN = 1_000
DEFAULT_SMALL_TABLE_MAX = 100_000
DEFAULT_LARGE_TABLE_MIN = 100_000
DEFAULT_LARGE_TABLE_MAX = 10_000_000
THREE_ERRORS = 3
DEFAULT_HIGH_FANOUT_THRESHOLD = 2
DEFAULT_ROW_THRESHOLD_SMALL = 10_000
DEFAULT_ROW_THRESHOLD_LARGE = 1_000_000
CUSTOM_HIGH_FANOUT_THRESHOLD = 5
CUSTOM_ROW_THRESHOLD_SMALL = 5_000
CUSTOM_ROW_THRESHOLD_LARGE = 500_000
HIGH_CONFIDENCE_FLOOR = 0.8
LOW_CONFIDENCE_CEILING = 0.5
LOW_CONFIDENCE_OBSERVE_CEILING = 0.5

# ---------------------------------------------------------------------------
# CalibrationBounds
# ---------------------------------------------------------------------------


class TestCalibrationBoundsDefaults:
    """Verify default calibration bound values."""

    def test_high_fanout_defaults(self) -> None:
        """Default high-fanout bounds are [1, 10]."""
        b = CalibrationBounds()
        assert b.min_high_fanout_threshold == 1
        assert b.max_high_fanout_threshold == DEFAULT_HIGH_FANOUT_MAX

    def test_small_table_defaults(self) -> None:
        """Default small-table bounds are [1_000, 100_000]."""
        b = CalibrationBounds()
        assert b.min_small_table_row_threshold == DEFAULT_SMALL_TABLE_MIN
        assert b.max_small_table_row_threshold == DEFAULT_SMALL_TABLE_MAX

    def test_large_table_defaults(self) -> None:
        """Default large-table bounds are [100_000, 10_000_000]."""
        b = CalibrationBounds()
        assert b.min_large_table_row_threshold == DEFAULT_LARGE_TABLE_MIN
        assert b.max_large_table_row_threshold == DEFAULT_LARGE_TABLE_MAX

    def test_frozen(self) -> None:
        """CalibrationBounds struct is immutable."""
        b = CalibrationBounds()
        assert_immutable_assignment(
            factory=lambda: b,
            attribute="min_high_fanout_threshold",
            attempted_value=99,
            expected_exception=AttributeError,
        )


class TestDefaultCalibrationBounds:
    """Verify module-level default constant."""

    def test_matches_fresh_instance(self) -> None:
        """DEFAULT_CALIBRATION_BOUNDS matches a fresh CalibrationBounds."""
        fresh = CalibrationBounds()
        assert (
            DEFAULT_CALIBRATION_BOUNDS.min_high_fanout_threshold == fresh.min_high_fanout_threshold
        )
        assert (
            DEFAULT_CALIBRATION_BOUNDS.max_high_fanout_threshold == fresh.max_high_fanout_threshold
        )
        assert (
            DEFAULT_CALIBRATION_BOUNDS.min_small_table_row_threshold
            == fresh.min_small_table_row_threshold
        )
        assert (
            DEFAULT_CALIBRATION_BOUNDS.max_small_table_row_threshold
            == fresh.max_small_table_row_threshold
        )

    def test_is_valid(self) -> None:
        """DEFAULT_CALIBRATION_BOUNDS passes validation."""
        errors = validate_calibration_bounds(DEFAULT_CALIBRATION_BOUNDS)
        assert errors == []


class TestValidateCalibrationBounds:
    """Verify bounds validation logic."""

    def test_valid_bounds_no_errors(self) -> None:
        """Well-ordered bounds produce no errors."""
        bounds = CalibrationBounds(
            min_high_fanout_threshold=1,
            max_high_fanout_threshold=5,
            min_small_table_row_threshold=1_000,
            max_small_table_row_threshold=50_000,
            min_large_table_row_threshold=100_000,
            max_large_table_row_threshold=5_000_000,
        )
        assert validate_calibration_bounds(bounds) == []

    def test_equal_high_fanout_bounds_error(self) -> None:
        """Equal min and max for high-fanout produces an error."""
        bounds = CalibrationBounds(
            min_high_fanout_threshold=3,
            max_high_fanout_threshold=3,
        )
        errors = validate_calibration_bounds(bounds)
        assert len(errors) == 1
        assert "high_fanout_threshold" in errors[0]

    def test_inverted_small_table_bounds_error(self) -> None:
        """Inverted small-table bounds produce an error."""
        bounds = CalibrationBounds(
            min_small_table_row_threshold=50_000,
            max_small_table_row_threshold=1_000,
        )
        errors = validate_calibration_bounds(bounds)
        assert len(errors) == 1
        assert "small_table_row_threshold" in errors[0]

    def test_inverted_large_table_bounds_error(self) -> None:
        """Inverted large-table bounds produce an error."""
        bounds = CalibrationBounds(
            min_large_table_row_threshold=10_000_000,
            max_large_table_row_threshold=100_000,
        )
        errors = validate_calibration_bounds(bounds)
        assert len(errors) == 1
        assert "large_table_row_threshold" in errors[0]

    def test_multiple_invalid_bounds(self) -> None:
        """All three pairs invalid produces three errors."""
        bounds = CalibrationBounds(
            min_high_fanout_threshold=10,
            max_high_fanout_threshold=1,
            min_small_table_row_threshold=100_000,
            max_small_table_row_threshold=1_000,
            min_large_table_row_threshold=10_000_000,
            max_large_table_row_threshold=100_000,
        )
        errors = validate_calibration_bounds(bounds)
        assert len(errors) == THREE_ERRORS


# ---------------------------------------------------------------------------
# CalibrationThresholds
# ---------------------------------------------------------------------------


class TestCalibrationThresholds:
    """Verify CalibrationThresholds struct."""

    def test_defaults(self) -> None:
        """Default thresholds match canonical project values."""
        t = CalibrationThresholds()
        assert t.high_fanout_threshold == DEFAULT_HIGH_FANOUT_THRESHOLD
        assert t.small_table_row_threshold == DEFAULT_ROW_THRESHOLD_SMALL
        assert t.large_table_row_threshold == DEFAULT_ROW_THRESHOLD_LARGE

    def test_custom_values(self) -> None:
        """Custom thresholds are preserved."""
        t = CalibrationThresholds(
            high_fanout_threshold=CUSTOM_HIGH_FANOUT_THRESHOLD,
            small_table_row_threshold=CUSTOM_ROW_THRESHOLD_SMALL,
            large_table_row_threshold=CUSTOM_ROW_THRESHOLD_LARGE,
        )
        assert t.high_fanout_threshold == CUSTOM_HIGH_FANOUT_THRESHOLD
        assert t.small_table_row_threshold == CUSTOM_ROW_THRESHOLD_SMALL
        assert t.large_table_row_threshold == CUSTOM_ROW_THRESHOLD_LARGE

    def test_frozen(self) -> None:
        """CalibrationThresholds is immutable."""
        t = CalibrationThresholds()
        assert_immutable_assignment(
            factory=lambda: t,
            attribute="high_fanout_threshold",
            attempted_value=99,
            expected_exception=AttributeError,
        )


# ---------------------------------------------------------------------------
# calibrate_from_execution_metrics — mode="off"
# ---------------------------------------------------------------------------


class TestCalibrateOff:
    """Verify mode='off' returns thresholds unchanged."""

    def test_off_returns_current_thresholds(self) -> None:
        """Mode 'off' preserves current thresholds exactly."""
        current = CalibrationThresholds(
            high_fanout_threshold=3,
            small_table_row_threshold=5_000,
            large_table_row_threshold=500_000,
        )
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=200.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=current,
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="off",
        )
        assert result.mode == "off"
        assert result.adjusted_thresholds == current
        assert result.cost_ratio is None
        assert result.bounded is False

    def test_off_confidence_is_low(self) -> None:
        """Mode 'off' produces zero-confidence result."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=100.0,
                observation_count=10,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="off",
        )
        assert result.calibration_confidence.confidence_score == 0.0


# ---------------------------------------------------------------------------
# calibrate_from_execution_metrics — mode="observe"
# ---------------------------------------------------------------------------


class TestCalibrateObserve:
    """Verify mode='observe' produces recommendations without side effects."""

    def test_observe_returns_adjusted_thresholds(self) -> None:
        """Mode 'observe' computes adjusted thresholds."""
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=200.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="observe",
        )
        assert result.mode == "observe"
        assert result.cost_ratio is not None
        assert result.cost_ratio == pytest.approx(2.0)

    def test_observe_evidence_summary_contains_ratio(self) -> None:
        """Evidence summary includes cost ratio."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=150.0,
                observation_count=3,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="observe",
        )
        assert "cost_ratio" in result.evidence_summary

    def test_observe_confidence_with_many_observations(self) -> None:
        """Many observations produce high confidence."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=100.0,
                observation_count=20,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="observe",
        )
        assert result.calibration_confidence.confidence_score >= HIGH_CONFIDENCE_FLOOR

    def test_observe_confidence_with_few_observations(self) -> None:
        """Few observations produce low confidence."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=100.0,
                observation_count=2,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="observe",
        )
        assert result.calibration_confidence.confidence_score < LOW_CONFIDENCE_OBSERVE_CEILING


# ---------------------------------------------------------------------------
# calibrate_from_execution_metrics — mode="apply"
# ---------------------------------------------------------------------------


class TestCalibrateApply:
    """Verify mode='apply' produces adjusted thresholds."""

    def test_apply_under_estimation_raises_thresholds(self) -> None:
        """When actual >> predicted (under-estimation), thresholds increase."""
        current = CalibrationThresholds(
            high_fanout_threshold=5,
            small_table_row_threshold=10_000,
            large_table_row_threshold=1_000_000,
        )
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=300.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=current,
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        assert result.mode == "apply"
        # All thresholds should be >= current (EMA toward higher target)
        assert result.adjusted_thresholds.high_fanout_threshold >= current.high_fanout_threshold
        assert (
            result.adjusted_thresholds.small_table_row_threshold
            >= current.small_table_row_threshold
        )
        assert (
            result.adjusted_thresholds.large_table_row_threshold
            >= current.large_table_row_threshold
        )

    def test_apply_over_estimation_lowers_thresholds(self) -> None:
        """When actual << predicted (over-estimation), thresholds decrease."""
        current = CalibrationThresholds(
            high_fanout_threshold=5,
            small_table_row_threshold=50_000,
            large_table_row_threshold=5_000_000,
        )
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=30.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=current,
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        assert result.adjusted_thresholds.high_fanout_threshold <= current.high_fanout_threshold
        assert (
            result.adjusted_thresholds.small_table_row_threshold
            <= current.small_table_row_threshold
        )
        assert (
            result.adjusted_thresholds.large_table_row_threshold
            <= current.large_table_row_threshold
        )

    def test_apply_well_calibrated_minimal_change(self) -> None:
        """When actual ~= predicted, thresholds change minimally."""
        current = CalibrationThresholds()
        metrics = ExecutionMetricsSummary(
            predicted_cost=100.0,
            actual_cost=100.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=current,
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        assert result.adjusted_thresholds == current

    def test_apply_mode_in_result(self) -> None:
        """Result mode is 'apply'."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=100.0,
                observation_count=1,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        assert result.mode == "apply"


# ---------------------------------------------------------------------------
# Bounds clamping
# ---------------------------------------------------------------------------


class TestBoundsClamping:
    """Verify EMA adjustments are clamped within bounds."""

    def test_extreme_under_estimation_clamped(self) -> None:
        """Extreme under-estimation does not exceed max bounds."""
        current = CalibrationThresholds(
            high_fanout_threshold=5,
            small_table_row_threshold=50_000,
            large_table_row_threshold=5_000_000,
        )
        bounds = CalibrationBounds(
            min_high_fanout_threshold=1,
            max_high_fanout_threshold=8,
            min_small_table_row_threshold=1_000,
            max_small_table_row_threshold=60_000,
            min_large_table_row_threshold=100_000,
            max_large_table_row_threshold=6_000_000,
        )
        metrics = ExecutionMetricsSummary(
            predicted_cost=1.0,
            actual_cost=1000.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=current,
            bounds=bounds,
            mode="apply",
        )
        assert result.adjusted_thresholds.high_fanout_threshold <= bounds.max_high_fanout_threshold
        assert (
            result.adjusted_thresholds.small_table_row_threshold
            <= bounds.max_small_table_row_threshold
        )
        assert (
            result.adjusted_thresholds.large_table_row_threshold
            <= bounds.max_large_table_row_threshold
        )
        assert result.bounded is True

    def test_extreme_over_estimation_clamped(self) -> None:
        """Extreme over-estimation does not go below min bounds."""
        current = CalibrationThresholds(
            high_fanout_threshold=2,
            small_table_row_threshold=2_000,
            large_table_row_threshold=200_000,
        )
        bounds = CalibrationBounds(
            min_high_fanout_threshold=2,
            max_high_fanout_threshold=10,
            min_small_table_row_threshold=1_500,
            max_small_table_row_threshold=100_000,
            min_large_table_row_threshold=150_000,
            max_large_table_row_threshold=10_000_000,
        )
        metrics = ExecutionMetricsSummary(
            predicted_cost=1000.0,
            actual_cost=1.0,
            observation_count=10,
        )
        result = calibrate_from_execution_metrics(
            metrics=metrics,
            current_thresholds=current,
            bounds=bounds,
            mode="apply",
        )
        assert result.adjusted_thresholds.high_fanout_threshold >= bounds.min_high_fanout_threshold
        assert (
            result.adjusted_thresholds.small_table_row_threshold
            >= bounds.min_small_table_row_threshold
        )
        assert (
            result.adjusted_thresholds.large_table_row_threshold
            >= bounds.min_large_table_row_threshold
        )
        assert result.bounded is True


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestCalibrationValidation:
    """Verify validation of calibration inputs."""

    def test_invalid_bounds_raises_value_error(self) -> None:
        """Invalid bounds raise ValueError."""
        bad_bounds = CalibrationBounds(
            min_high_fanout_threshold=10,
            max_high_fanout_threshold=1,
        )
        with pytest.raises(ValueError, match="Invalid calibration bounds"):
            calibrate_from_execution_metrics(
                metrics=ExecutionMetricsSummary(
                    predicted_cost=100.0,
                    actual_cost=100.0,
                    observation_count=5,
                ),
                current_thresholds=CalibrationThresholds(),
                bounds=bad_bounds,
                mode="apply",
            )

    def test_zero_predicted_cost_returns_unchanged(self) -> None:
        """Zero predicted cost skips calibration gracefully."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=0.0,
                actual_cost=100.0,
                observation_count=10,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        assert result.adjusted_thresholds == CalibrationThresholds()
        assert "zero or negative" in result.evidence_summary


# ---------------------------------------------------------------------------
# InferenceConfidence construction
# ---------------------------------------------------------------------------


class TestCalibrationConfidence:
    """Verify InferenceConfidence metadata from calibration."""

    def test_high_observation_count_yields_high_confidence(self) -> None:
        """Many observations produce high-confidence result."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=120.0,
                observation_count=20,
                mean_duration_ms=50.0,
                mean_row_count=1000.0,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        conf = result.calibration_confidence
        assert conf.confidence_score >= HIGH_CONFIDENCE_FLOOR
        assert "execution_metrics" in conf.evidence_sources
        assert "duration" in conf.evidence_sources
        assert "row_count" in conf.evidence_sources
        assert conf.decision_type == "calibration"

    def test_low_observation_count_yields_low_confidence(self) -> None:
        """Few observations produce low-confidence result."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=120.0,
                observation_count=2,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        conf = result.calibration_confidence
        assert conf.confidence_score < LOW_CONFIDENCE_CEILING
        assert conf.fallback_reason is not None
        assert "insufficient" in conf.fallback_reason

    def test_confidence_evidence_sources_minimal(self) -> None:
        """Minimal metrics produce only execution_metrics source."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=100.0,
                observation_count=10,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        conf = result.calibration_confidence
        assert "execution_metrics" in conf.evidence_sources
        assert "duration" not in conf.evidence_sources
        assert "row_count" not in conf.evidence_sources


# ---------------------------------------------------------------------------
# PolicyCalibrationResult struct
# ---------------------------------------------------------------------------


class TestPolicyCalibrationResult:
    """Verify PolicyCalibrationResult struct properties."""

    def test_frozen(self) -> None:
        """PolicyCalibrationResult is immutable."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=100.0,
                observation_count=1,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="off",
        )
        assert_immutable_assignment(
            factory=lambda: result,
            attribute="mode",
            attempted_value="apply",
            expected_exception=AttributeError,
        )

    def test_result_has_all_fields(self) -> None:
        """Result contains all expected fields."""
        result = calibrate_from_execution_metrics(
            metrics=ExecutionMetricsSummary(
                predicted_cost=100.0,
                actual_cost=150.0,
                observation_count=10,
            ),
            current_thresholds=CalibrationThresholds(),
            bounds=DEFAULT_CALIBRATION_BOUNDS,
            mode="apply",
        )
        assert isinstance(result.adjusted_thresholds, CalibrationThresholds)
        assert isinstance(result.evidence_summary, str)
        assert isinstance(result.calibration_confidence, InferenceConfidence)
        assert result.mode == "apply"
        assert isinstance(result.cost_ratio, float)
        assert isinstance(result.bounded, bool)
