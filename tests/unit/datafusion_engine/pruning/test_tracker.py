"""Test PruningTracker accumulation and summary computation."""

from __future__ import annotations

import pytest

from datafusion_engine.pruning.metrics import PruningMetrics
from datafusion_engine.pruning.tracker import PruningSummary, PruningTracker


class TestPruningSummaryDefaults:
    """Test PruningSummary default values."""

    def test_default_summary(self) -> None:
        """Default summary has all-zero fields."""
        s = PruningSummary()
        assert s.total_queries == 0
        assert s.queries_with_pruning == 0
        assert s.avg_effectiveness == 0.0
        assert s.total_row_groups_pruned == 0
        assert s.total_row_groups_total == 0


class TestPruningTrackerEmpty:
    """Test tracker with no entries."""

    def test_empty_tracker_summary(self) -> None:
        """Empty tracker returns zero-valued summary."""
        tracker = PruningTracker()
        summary = tracker.summary()
        assert summary.total_queries == 0
        assert summary.queries_with_pruning == 0
        assert summary.avg_effectiveness == 0.0


class TestPruningTrackerRecord:
    """Test recording and summarizing pruning metrics."""

    def test_single_entry(self) -> None:
        """Single entry produces correct summary."""
        tracker = PruningTracker()
        tracker.record(
            "view_a",
            PruningMetrics(
                row_groups_total=100,
                row_groups_pruned=50,
                pruning_effectiveness=0.5,
            ),
        )
        summary = tracker.summary()
        assert summary.total_queries == 1
        assert summary.queries_with_pruning == 1
        assert summary.avg_effectiveness == pytest.approx(0.5)
        assert summary.total_row_groups_pruned == 50
        assert summary.total_row_groups_total == 100

    def test_multiple_entries(self) -> None:
        """Multiple entries are accumulated correctly."""
        tracker = PruningTracker()
        tracker.record(
            "view_a",
            PruningMetrics(
                row_groups_total=100,
                row_groups_pruned=50,
                pruning_effectiveness=0.5,
            ),
        )
        tracker.record(
            "view_b",
            PruningMetrics(
                row_groups_total=200,
                row_groups_pruned=100,
                pruning_effectiveness=0.5,
            ),
        )
        summary = tracker.summary()
        assert summary.total_queries == 2
        assert summary.queries_with_pruning == 2
        assert summary.avg_effectiveness == pytest.approx(0.5)
        assert summary.total_row_groups_pruned == 150
        assert summary.total_row_groups_total == 300

    def test_mixed_pruning_and_no_pruning(self) -> None:
        """Entries with and without pruning are handled correctly."""
        tracker = PruningTracker()
        tracker.record(
            "view_a",
            PruningMetrics(
                row_groups_total=100,
                row_groups_pruned=40,
                pruning_effectiveness=0.4,
            ),
        )
        tracker.record(
            "view_b",
            PruningMetrics(
                row_groups_total=50,
                row_groups_pruned=0,
                pruning_effectiveness=0.0,
            ),
        )
        summary = tracker.summary()
        assert summary.total_queries == 2
        assert summary.queries_with_pruning == 1
        assert summary.avg_effectiveness == pytest.approx(0.4)
        assert summary.total_row_groups_pruned == 40
        assert summary.total_row_groups_total == 150

    def test_no_pruning_entries(self) -> None:
        """All entries with zero pruning produce zero average effectiveness."""
        tracker = PruningTracker()
        tracker.record("view_a", PruningMetrics(row_groups_total=50))
        tracker.record("view_b", PruningMetrics(row_groups_total=30))
        summary = tracker.summary()
        assert summary.total_queries == 2
        assert summary.queries_with_pruning == 0
        assert summary.avg_effectiveness == 0.0


class TestPruningTrackerDefaults:
    """Test tracker with default-valued metrics."""

    def test_default_metrics_entry(self) -> None:
        """Recording default PruningMetrics does not cause errors."""
        tracker = PruningTracker()
        tracker.record("view_empty", PruningMetrics())
        summary = tracker.summary()
        assert summary.total_queries == 1
        assert summary.queries_with_pruning == 0
        assert summary.total_row_groups_total == 0
