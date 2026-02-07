"""Test pruning metrics parsing from explain-analyze output."""

from __future__ import annotations

import pytest

from datafusion_engine.pruning.explain_parser import parse_pruning_metrics
from datafusion_engine.pruning.metrics import PruningMetrics


class TestParseEmptyInput:
    """Test graceful degradation with empty or missing input."""

    def test_empty_string(self) -> None:
        """Empty string returns zero-valued metrics."""
        m = parse_pruning_metrics("")
        assert m.row_groups_total == 0
        assert m.row_groups_pruned == 0
        assert m.pruning_effectiveness == 0.0

    def test_no_pruning_info(self) -> None:
        """Explain output without pruning info returns zero-valued metrics."""
        text = "SortExec: sort_mode=FullSort\n  ProjectionExec: expr=[a]\n"
        m = parse_pruning_metrics(text)
        assert m.row_groups_total == 0
        assert m.row_groups_pruned == 0

    def test_unrelated_content(self) -> None:
        """Random content returns zero-valued metrics."""
        m = parse_pruning_metrics("This is not explain output at all.")
        assert isinstance(m, PruningMetrics)
        assert m.pruning_effectiveness == 0.0


class TestParseRowGroupMetrics:
    """Test row-group pruning extraction."""

    def test_pruned_of_format(self) -> None:
        """Parse 'pruned N of M row groups' format."""
        text = "ParquetExec: pruned 30 of 100 row groups"
        m = parse_pruning_metrics(text)
        assert m.row_groups_total == 100
        assert m.row_groups_pruned == 30
        assert m.pruning_effectiveness == pytest.approx(0.3)

    def test_pruned_groups_eq_format(self) -> None:
        """Parse 'pruned_groups=N' key-value format."""
        text = "ParquetExec: pruned_groups=15 total_groups=50"
        m = parse_pruning_metrics(text)
        assert m.row_groups_pruned == 15
        assert m.row_groups_total == 50

    def test_stats_pruned_format(self) -> None:
        """Parse 'RowGroupsStatisticsPruned: N' format."""
        text = "RowGroupsStatisticsPruned: 42\ntotal_groups=60"
        m = parse_pruning_metrics(text)
        assert m.row_groups_pruned == 42
        assert m.row_groups_total == 60

    def test_pruned_exceeds_total_corrected(self) -> None:
        """When pruned exceeds total, total is corrected upward."""
        text = "pruned_groups=20 total_groups=10"
        m = parse_pruning_metrics(text)
        assert m.row_groups_total >= m.row_groups_pruned

    def test_effectiveness_computation(self) -> None:
        """Effectiveness is pruned / total ratio."""
        text = "ParquetExec: pruned 50 of 200 row groups"
        m = parse_pruning_metrics(text)
        assert m.pruning_effectiveness == pytest.approx(0.25)

    def test_zero_total_zero_effectiveness(self) -> None:
        """Zero total row groups produces zero effectiveness."""
        m = parse_pruning_metrics("SortExec: no pruning info")
        assert m.pruning_effectiveness == 0.0


class TestParsePageMetrics:
    """Test page-level pruning extraction."""

    def test_pruned_of_pages_format(self) -> None:
        """Parse 'pruned N of M pages' format."""
        text = "pruned 10 of 40 pages"
        m = parse_pruning_metrics(text)
        assert m.pages_total == 40
        assert m.pages_pruned == 10

    def test_page_eq_format(self) -> None:
        """Parse 'pruned_pages=N' and 'total_pages=M' format."""
        text = "pruned_pages=5 total_pages=20"
        m = parse_pruning_metrics(text)
        assert m.pages_pruned == 5
        assert m.pages_total == 20


class TestParseFilterMetrics:
    """Test filter pushdown extraction."""

    def test_pushed_filters_eq(self) -> None:
        """Parse 'pushed_filters=N' format."""
        text = "pushed_filters=3"
        m = parse_pruning_metrics(text)
        assert m.filters_pushed == 3

    def test_filter_list(self) -> None:
        """Parse filter list format 'filters=[a, b, c]'."""
        text = "filters=[col1 > 5, col2 = 'abc', col3 IS NOT NULL]"
        m = parse_pruning_metrics(text)
        assert m.filters_pushed == 3

    def test_empty_filter_list(self) -> None:
        """Parse empty filter list."""
        text = "filters=[]"
        m = parse_pruning_metrics(text)
        assert m.filters_pushed == 0


class TestParseStatisticsAvailability:
    """Test statistics availability detection."""

    def test_stats_available_true(self) -> None:
        """Detect statistics availability from key=true."""
        text = "statistics_available=true"
        m = parse_pruning_metrics(text)
        assert m.statistics_available is True

    def test_parquet_with_pruning_implies_stats(self) -> None:
        """ParquetExec with pruning data implies statistics available."""
        text = "ParquetExec: pruned 5 of 10 row groups"
        m = parse_pruning_metrics(text)
        assert m.statistics_available is True

    def test_no_parquet_no_stats(self) -> None:
        """Without ParquetExec or explicit flag, statistics are unavailable."""
        text = "SortExec: expr=[a]"
        m = parse_pruning_metrics(text)
        assert m.statistics_available is False


class TestParseRealisticOutput:
    """Test with realistic DataFusion explain-analyze output samples."""

    def test_full_parquet_explain(self) -> None:
        """Parse a realistic ParquetExec explain-analyze fragment."""
        text = (
            "| ParquetExec: file_groups={4 groups: [[...]]}, "
            "pruned 12 of 48 row groups, "
            "pruned 30 of 192 pages, "
            "pushed_filters=2, "
            "statistics_available=true |"
        )
        m = parse_pruning_metrics(text)
        assert m.row_groups_total == 48
        assert m.row_groups_pruned == 12
        assert m.pages_total == 192
        assert m.pages_pruned == 30
        assert m.filters_pushed == 2
        assert m.statistics_available is True
        assert m.pruning_effectiveness == pytest.approx(0.25)
