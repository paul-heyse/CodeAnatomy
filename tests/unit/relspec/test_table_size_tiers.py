"""Unit tests for relspec.table_size_tiers."""

from __future__ import annotations

import pytest

from relspec.table_size_tiers import (
    _DEFAULT_THRESHOLDS,
    TableSizeThresholds,
    TableSizeTier,
    classify_table_size,
)
from tests.test_helpers.immutability import assert_immutable_assignment


class TestTableSizeThresholdsDefaults:
    """Verify canonical default threshold values."""

    def test_small_threshold_default(self) -> None:
        """Default small threshold is 10,000."""
        assert TableSizeThresholds().small_threshold == 10_000

    def test_large_threshold_default(self) -> None:
        """Default large threshold is 1,000,000."""
        assert TableSizeThresholds().large_threshold == 1_000_000

    def test_streaming_threshold_default(self) -> None:
        """Default streaming threshold is 100,000."""
        assert TableSizeThresholds().streaming_threshold == 100_000

    def test_module_level_default_matches(self) -> None:
        """Module-level _DEFAULT_THRESHOLDS matches fresh instance."""
        fresh = TableSizeThresholds()
        assert _DEFAULT_THRESHOLDS.small_threshold == fresh.small_threshold
        assert _DEFAULT_THRESHOLDS.large_threshold == fresh.large_threshold
        assert _DEFAULT_THRESHOLDS.streaming_threshold == fresh.streaming_threshold

    def test_frozen(self) -> None:
        """Thresholds struct is frozen (immutable)."""
        t = TableSizeThresholds()
        assert_immutable_assignment(
            factory=lambda: t,
            attribute="small_threshold",
            attempted_value=42,
            expected_exception=AttributeError,
        )


class TestClassifyTableSizeNone:
    """Verify behaviour when row_count is None."""

    def test_none_returns_medium(self) -> None:
        """None row_count conservatively classifies as MEDIUM."""
        assert classify_table_size(None) == TableSizeTier.MEDIUM


class TestClassifyTableSizeBoundaries:
    """Verify boundary conditions around small and large thresholds."""

    @pytest.mark.parametrize(
        ("row_count", "expected"),
        [
            (0, TableSizeTier.SMALL),
            (1, TableSizeTier.SMALL),
            (9_999, TableSizeTier.SMALL),
            (10_000, TableSizeTier.MEDIUM),
            (10_001, TableSizeTier.MEDIUM),
            (500_000, TableSizeTier.MEDIUM),
            (999_999, TableSizeTier.MEDIUM),
            (1_000_000, TableSizeTier.MEDIUM),
            (1_000_001, TableSizeTier.LARGE),
            (10_000_000, TableSizeTier.LARGE),
        ],
    )
    def test_boundary_classification(
        self,
        row_count: int,
        expected: TableSizeTier,
    ) -> None:
        """Classify at boundary values using default thresholds."""
        assert classify_table_size(row_count) == expected


class TestClassifyTableSizeCustomThresholds:
    """Verify that custom thresholds override defaults."""

    def test_custom_small_threshold(self) -> None:
        """Custom small threshold shifts the boundary."""
        custom = TableSizeThresholds(small_threshold=100, large_threshold=1_000)
        assert classify_table_size(99, custom) == TableSizeTier.SMALL
        assert classify_table_size(100, custom) == TableSizeTier.MEDIUM

    def test_custom_large_threshold(self) -> None:
        """Custom large threshold shifts the boundary."""
        custom = TableSizeThresholds(small_threshold=100, large_threshold=1_000)
        assert classify_table_size(1_000, custom) == TableSizeTier.MEDIUM
        assert classify_table_size(1_001, custom) == TableSizeTier.LARGE


class TestTableSizeTierValues:
    """Verify TableSizeTier enum values are stable strings."""

    def test_tier_values(self) -> None:
        """Tier string values match expected labels."""
        assert TableSizeTier.SMALL == "small"
        assert TableSizeTier.MEDIUM == "medium"
        assert TableSizeTier.LARGE == "large"

    def test_tier_is_str(self) -> None:
        """Tiers are usable as plain strings (StrEnum)."""
        assert isinstance(TableSizeTier.SMALL, str)
