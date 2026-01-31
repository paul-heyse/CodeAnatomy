"""Tests for semantics.stats.collector module."""

from __future__ import annotations

from typing import cast

import pytest

from semantics.stats.collector import (
    ColumnStats,
    ViewStats,
    ViewStatsCache,
    _parse_row_count_from_line,
)


class TestColumnStats:
    """Tests for ColumnStats dataclass."""

    def test_basic_creation(self) -> None:
        """Create ColumnStats with just name."""
        stats = ColumnStats(name="file_path")
        assert stats.name == "file_path"
        assert stats.null_count is None
        assert stats.distinct_count is None

    def test_full_creation(self) -> None:
        """Create ColumnStats with all fields."""
        stats = ColumnStats(
            name="bstart",
            null_count=0,
            distinct_count=100,
            min_value="0",
            max_value="9999",
        )
        assert stats.name == "bstart"
        assert stats.null_count == 0
        assert stats.distinct_count == 100
        assert stats.min_value == "0"
        assert stats.max_value == "9999"

    def test_as_dict(self) -> None:
        """Serialize ColumnStats to dictionary."""
        stats = ColumnStats(name="col", null_count=5)
        result = stats.as_dict()
        assert result["name"] == "col"
        assert result["null_count"] == 5
        assert result["distinct_count"] is None

    def test_frozen(self) -> None:
        """ColumnStats is immutable."""
        stats = ColumnStats(name="test")
        with pytest.raises(AttributeError):
            stats.name = "changed"  # type: ignore[misc]


class TestViewStats:
    """Tests for ViewStats dataclass."""

    def test_basic_creation(self) -> None:
        """Create ViewStats with just name."""
        stats = ViewStats(view_name="my_view")
        assert stats.view_name == "my_view"
        assert stats.row_count is None
        assert stats.row_count_exact is False
        assert stats.column_stats == {}

    def test_add_column_stats(self) -> None:
        """Add column statistics to view."""
        stats = ViewStats(view_name="test")
        col_stats = ColumnStats(name="col1")
        stats.add_column_stats(col_stats)
        assert "col1" in stats.column_stats
        assert stats.column_stats["col1"] is col_stats

    def test_column_names(self) -> None:
        """Get column names from stats."""
        stats = ViewStats(view_name="test")
        stats.add_column_stats(ColumnStats(name="a"))
        stats.add_column_stats(ColumnStats(name="b"))
        names = stats.column_names()
        assert "a" in names
        assert "b" in names

    def test_has_exact_count_false(self) -> None:
        """Check has_exact_count when estimated."""
        stats = ViewStats(view_name="test", row_count=100, row_count_exact=False)
        assert stats.has_exact_count() is False

    def test_has_exact_count_true(self) -> None:
        """Check has_exact_count when exact."""
        stats = ViewStats(view_name="test", row_count=100, row_count_exact=True)
        assert stats.has_exact_count() is True

    def test_has_exact_count_none(self) -> None:
        """Check has_exact_count when row_count is None."""
        stats = ViewStats(view_name="test", row_count_exact=True)
        assert stats.has_exact_count() is False

    def test_as_dict(self) -> None:
        """Serialize ViewStats to dictionary."""
        stats = ViewStats(
            view_name="my_view",
            row_count=1000,
            row_count_exact=True,
            schema_fingerprint="abc123",
        )
        stats.add_column_stats(ColumnStats(name="col1"))
        result = stats.as_dict()
        column_stats = cast("dict[str, object]", result["column_stats"])
        assert result["view_name"] == "my_view"
        assert result["row_count"] == 1000
        assert result["row_count_exact"] is True
        assert result["schema_fingerprint"] == "abc123"
        assert "col1" in column_stats


class TestViewStatsCache:
    """Tests for ViewStatsCache."""

    def test_empty_cache(self) -> None:
        """Empty cache has no entries."""
        cache = ViewStatsCache()
        assert len(cache) == 0
        assert cache.get("missing") is None
        assert "missing" not in cache

    def test_put_and_get(self) -> None:
        """Store and retrieve stats."""
        cache = ViewStatsCache()
        stats = ViewStats(view_name="view1", row_count=100)
        cache.put(stats)
        assert len(cache) == 1
        assert "view1" in cache
        retrieved = cache.get("view1")
        assert retrieved is stats
        assert retrieved is not None
        assert retrieved.row_count == 100

    def test_invalidate(self) -> None:
        """Remove stats from cache."""
        cache = ViewStatsCache()
        cache.put(ViewStats(view_name="view1"))
        cache.put(ViewStats(view_name="view2"))
        assert len(cache) == 2
        cache.invalidate("view1")
        assert len(cache) == 1
        assert "view1" not in cache
        assert "view2" in cache

    def test_invalidate_missing(self) -> None:
        """Invalidate non-existent entry is no-op."""
        cache = ViewStatsCache()
        cache.invalidate("missing")  # Should not raise

    def test_clear(self) -> None:
        """Clear all cache entries."""
        cache = ViewStatsCache()
        cache.put(ViewStats(view_name="v1"))
        cache.put(ViewStats(view_name="v2"))
        assert len(cache) == 2
        cache.clear()
        assert len(cache) == 0

    def test_overwrite(self) -> None:
        """Putting same view name overwrites."""
        cache = ViewStatsCache()
        cache.put(ViewStats(view_name="v", row_count=1))
        cache.put(ViewStats(view_name="v", row_count=2))
        assert len(cache) == 1
        retrieved = cache.get("v")
        assert retrieved is not None
        assert retrieved.row_count == 2


class TestParseRowCountFromLine:
    """Tests for _parse_row_count_from_line helper."""

    def test_valid_line(self) -> None:
        """Parse row count from valid line."""
        line = "TableScan: my_table projection=[a, b], rows=1234"
        result = _parse_row_count_from_line(line)
        assert result == 1234

    def test_no_rows_marker(self) -> None:
        """Return None if no rows= marker."""
        line = "TableScan: my_table projection=[a, b]"
        result = _parse_row_count_from_line(line)
        assert result is None

    def test_non_numeric_rows(self) -> None:
        """Return None if rows value is not numeric."""
        line = "rows=unknown"
        result = _parse_row_count_from_line(line)
        assert result is None

    def test_rows_with_trailing_chars(self) -> None:
        """Parse rows with trailing punctuation."""
        line = "rows=100, size=500"
        result = _parse_row_count_from_line(line)
        assert result == 100

    def test_rows_with_paren(self) -> None:
        """Parse rows with closing paren."""
        line = "stats=(rows=50)"
        result = _parse_row_count_from_line(line)
        assert result == 50
