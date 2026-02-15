"""Tests for filter options handling.

These tests verify that:
1. Filter options are correctly converted into CommonFilters
2. Filter params are parsed from CLI into flattened options
"""

from __future__ import annotations

from tools.cq.cli_app.app import app
from tools.cq.cli_app.options import CommonFilters, options_from_params
from tools.cq.cli_app.params import FilterParams


class TestFilterOptionsConversion:
    """Tests for converting filter params into CommonFilters."""

    def test_empty_filters(self) -> None:
        """Test building filters with all defaults."""
        filters = options_from_params(FilterParams(), type_=CommonFilters)
        assert not filters.has_filters

    def test_include_patterns(self) -> None:
        """Test building filters with include patterns."""
        filters = options_from_params(
            FilterParams(include=["src/", "tools/"]),
            type_=CommonFilters,
        )
        assert filters.include == ["src/", "tools/"]

    def test_exclude_patterns(self) -> None:
        """Test building filters with exclude patterns."""
        filters = options_from_params(
            FilterParams(exclude=["tests/", "docs/"]),
            type_=CommonFilters,
        )
        assert filters.exclude == ["tests/", "docs/"]

    def test_limit(self) -> None:
        """Test building filters with limit."""
        filters = options_from_params(FilterParams(limit=50), type_=CommonFilters)
        assert filters.limit == 50


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
        opts = bound.kwargs["opts"]
        assert opts.include == ["src/", "tools/"]

    def test_include_consume_multiple_tokens(self) -> None:
        """Test include parsing with consume_multiple token lists."""
        _cmd, bound, _extra = app.parse_args(
            [
                "calls",
                "foo",
                "--include",
                "src/",
                "tools/",
                "--exclude",
                "tests/",
            ]
        )
        opts = bound.kwargs["opts"]
        assert opts.include == ["src/", "tools/"]
        assert opts.exclude == ["tests/"]

    def test_limit_from_cli(self) -> None:
        """Test limit parsed from CLI."""
        _cmd, bound, _extra = app.parse_args(["calls", "foo", "--limit", "25"])
        opts = bound.kwargs["opts"]
        assert opts.limit == 25

    def test_impact_filter_flag(self) -> None:
        """Test --impact flag."""
        _cmd, bound, _extra = app.parse_args(["calls", "foo", "--impact", "high,med"])
        opts = bound.kwargs["opts"]
        assert [str(value) for value in opts.impact] == ["high", "med"]

    def test_impact_filter_multi_token(self) -> None:
        """Test --impact supports multiple tokens after one flag."""
        _cmd, bound, _extra = app.parse_args(
            ["calls", "foo", "--impact", "high", "med", "--confidence", "high"]
        )
        opts = bound.kwargs["opts"]
        assert [str(value) for value in opts.impact] == ["high", "med"]
