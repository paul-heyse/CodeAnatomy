"""Tests for filter options handling.

These tests verify that:
1. Filter options are correctly converted into CommonFilters
2. Filter params are parsed from CLI into flattened options
"""

from __future__ import annotations

from tools.cq.cli_app.app import app
from tools.cq.cli_app.options import CommonFilters, options_from_params
from tools.cq.cli_app.params import FilterParams

DEFAULT_LIMIT = 50
CLI_LIMIT = 25


class TestFilterOptionsConversion:
    """Tests for converting filter params into CommonFilters."""

    @staticmethod
    def test_empty_filters() -> None:
        """Test building filters with all defaults."""
        filters = options_from_params(FilterParams(), type_=CommonFilters)
        assert not filters.has_filters

    @staticmethod
    def test_include_patterns() -> None:
        """Test building filters with include patterns."""
        filters = options_from_params(
            FilterParams(include=["src/", "tools/"]),
            type_=CommonFilters,
        )
        assert filters.include == ["src/", "tools/"]

    @staticmethod
    def test_exclude_patterns() -> None:
        """Test building filters with exclude patterns."""
        filters = options_from_params(
            FilterParams(exclude=["tests/", "docs/"]),
            type_=CommonFilters,
        )
        assert filters.exclude == ["tests/", "docs/"]

    @staticmethod
    def test_limit() -> None:
        """Test building filters with limit."""
        filters = options_from_params(FilterParams(limit=DEFAULT_LIMIT), type_=CommonFilters)
        assert filters.limit == DEFAULT_LIMIT


class TestFilterOptionsFromCLI:
    """Tests for filter options parsed from CLI."""

    @staticmethod
    def test_include_repeated_flag() -> None:
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

    @staticmethod
    def test_include_consume_multiple_tokens() -> None:
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

    @staticmethod
    def test_limit_from_cli() -> None:
        """Test limit parsed from CLI."""
        _cmd, bound, _extra = app.parse_args(["calls", "foo", "--limit", str(CLI_LIMIT)])
        opts = bound.kwargs["opts"]
        assert opts.limit == CLI_LIMIT

    @staticmethod
    def test_impact_filter_flag() -> None:
        """Test --impact flag."""
        _cmd, bound, _extra = app.parse_args(["calls", "foo", "--impact", "high,med"])
        opts = bound.kwargs["opts"]
        assert [str(value) for value in opts.impact] == ["high", "med"]

    @staticmethod
    def test_impact_filter_multi_token() -> None:
        """Test --impact supports multiple tokens after one flag."""
        _cmd, bound, _extra = app.parse_args(
            ["calls", "foo", "--impact", "high", "med", "--confidence", "high"]
        )
        opts = bound.kwargs["opts"]
        assert [str(value) for value in opts.impact] == ["high", "med"]
