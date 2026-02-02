"""Tests for filter options handling.

These tests verify that:
1. Filter options are correctly parsed from CLI
2. _build_filters creates proper FilterConfig
"""

from __future__ import annotations

from tools.cq.cli_app.app import app
from tools.cq.cli_app.commands.analysis import _build_filters


class TestBuildFilters:
    """Tests for _build_filters helper function."""

    def test_empty_filters(self) -> None:
        """Test building filters with all None values."""
        filters = _build_filters(
            include=None,
            exclude=None,
            impact=None,
            confidence=None,
            severity=None,
            limit=None,
        )
        assert not filters.has_filters

    def test_include_patterns(self) -> None:
        """Test building filters with include patterns."""
        filters = _build_filters(
            include=["src/", "tools/"],
            exclude=None,
            impact=None,
            confidence=None,
            severity=None,
            limit=None,
        )
        assert filters.include == ["src/", "tools/"]

    def test_exclude_patterns(self) -> None:
        """Test building filters with exclude patterns."""
        filters = _build_filters(
            include=None,
            exclude=["tests/", "docs/"],
            impact=None,
            confidence=None,
            severity=None,
            limit=None,
        )
        assert filters.exclude == ["tests/", "docs/"]

    def test_impact_comma_separated(self) -> None:
        """Test building filters with comma-separated impact values."""
        filters = _build_filters(
            include=None,
            exclude=None,
            impact="low,high",
            confidence=None,
            severity=None,
            limit=None,
        )
        assert filters.impact == ["low", "high"]

    def test_confidence_comma_separated(self) -> None:
        """Test building filters with comma-separated confidence values."""
        filters = _build_filters(
            include=None,
            exclude=None,
            impact=None,
            confidence="med,high",
            severity=None,
            limit=None,
        )
        assert filters.confidence == ["med", "high"]

    def test_severity_comma_separated(self) -> None:
        """Test building filters with comma-separated severity values."""
        filters = _build_filters(
            include=None,
            exclude=None,
            impact=None,
            confidence=None,
            severity="error,warning",
            limit=None,
        )
        assert filters.severity == ["error", "warning"]

    def test_limit(self) -> None:
        """Test building filters with limit."""
        filters = _build_filters(
            include=None,
            exclude=None,
            impact=None,
            confidence=None,
            severity=None,
            limit=50,
        )
        assert filters.limit == 50

    def test_strips_whitespace(self) -> None:
        """Test that whitespace is stripped from comma-separated values."""
        filters = _build_filters(
            include=None,
            exclude=None,
            impact="low , high",
            confidence=None,
            severity=None,
            limit=None,
        )
        assert filters.impact == ["low", "high"]


class TestFilterOptionsFromCLI:
    """Tests for filter options parsed from CLI."""

    def test_include_repeated_flag(self) -> None:
        """Test include with repeated flags."""
        _cmd, bound, _extra = app.parse_args(
            [
                "calls",
                "foo",
                "--include",
                "src/",
                "--include",
                "tools/",
            ]
        )
        assert bound.kwargs["include"] == ["src/", "tools/"]

    def test_limit_from_cli(self) -> None:
        """Test limit parsed from CLI."""
        _cmd, bound, _extra = app.parse_args(["calls", "foo", "--limit", "25"])
        assert bound.kwargs["limit"] == 25

    def test_impact_filter_flag(self) -> None:
        """Test --impact flag."""
        _cmd, bound, _extra = app.parse_args(["calls", "foo", "--impact", "high,med"])
        assert bound.kwargs["impact_filter"] == "high,med"
