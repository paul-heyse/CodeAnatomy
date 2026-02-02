"""Tests for cyclopts CLI parsing.

These tests verify that command arguments are correctly parsed without
actually executing the commands.
"""

from __future__ import annotations

import pytest
from tools.cq.cli_app.app import app
from tools.cq.cli_app.types import OutputFormat


class TestImpactCommandParsing:
    """Tests for impact command argument parsing."""

    def test_impact_required_args(self) -> None:
        """Test parsing impact command with required arguments."""
        _cmd, bound, _extra = app.parse_args(["impact", "build_graph", "--param", "root"])
        assert bound.args[0] == "build_graph"  # positional arg
        assert bound.kwargs["param"] == "root"

    def test_impact_with_depth(self) -> None:
        """Test parsing impact command with depth option."""
        _cmd, bound, _extra = app.parse_args(["impact", "foo", "--param", "bar", "--depth", "10"])
        assert bound.args[0] == "foo"  # positional arg
        assert bound.kwargs["param"] == "bar"
        assert bound.kwargs["depth"] == 10

    def test_impact_with_format(self) -> None:
        """Test parsing impact command with format option (global option via meta app)."""
        # --format is a global option handled by the meta launcher
        _cmd, bound, _extra = app.meta.parse_args(
            ["impact", "foo", "--param", "bar", "--format", "json"]
        )
        assert bound.kwargs["output_format"] == OutputFormat.json

    def test_impact_missing_param_fails(self) -> None:
        """Test that impact command fails without --param."""
        with pytest.raises(SystemExit):
            app.parse_args(["impact", "foo"], exit_on_error=True, print_error=False)


class TestCallsCommandParsing:
    """Tests for calls command argument parsing."""

    def test_calls_basic(self) -> None:
        """Test parsing calls command."""
        _cmd, bound, _extra = app.parse_args(["calls", "my_function"])
        assert bound.args[0] == "my_function"  # positional arg

    def test_calls_with_filters(self) -> None:
        """Test parsing calls command with filter options."""
        _cmd, bound, _extra = app.parse_args(
            [
                "calls",
                "foo",
                "--include",
                "src/",
                "--exclude",
                "tests/",
                "--limit",
                "50",
            ]
        )
        assert bound.args[0] == "foo"  # positional arg
        assert bound.kwargs["include"] == ["src/"]
        assert bound.kwargs["exclude"] == ["tests/"]
        assert bound.kwargs["limit"] == 50


class TestQueryCommandParsing:
    """Tests for q (query) command argument parsing."""

    def test_query_basic(self) -> None:
        """Test parsing query command with query string."""
        _cmd, bound, _extra = app.parse_args(["q", "entity=function name=main"])
        assert bound.args[0] == "entity=function name=main"  # positional arg

    def test_query_with_explain(self) -> None:
        """Test parsing query command with explain option."""
        _cmd, bound, _extra = app.parse_args(["q", "entity=function", "--explain-files"])
        assert bound.kwargs["explain_files"] is True

    def test_query_with_no_cache(self) -> None:
        """Test parsing query command with no-cache option."""
        _cmd, bound, _extra = app.parse_args(["q", "entity=function", "--no-cache"])
        assert bound.kwargs["no_cache"] is True


class TestSigImpactCommandParsing:
    """Tests for sig-impact command argument parsing."""

    def test_sig_impact_basic(self) -> None:
        """Test parsing sig-impact command."""
        _cmd, bound, _extra = app.parse_args(
            [
                "sig-impact",
                "foo",
                "--to",
                "foo(a, b, *, c=None)",
            ]
        )
        assert bound.args[0] == "foo"  # positional arg
        assert bound.kwargs["to"] == "foo(a, b, *, c=None)"


class TestIndexCommandParsing:
    """Tests for index command argument parsing."""

    def test_index_rebuild(self) -> None:
        """Test parsing index command with rebuild option."""
        _cmd, bound, _extra = app.parse_args(["index", "--rebuild"])
        assert bound.kwargs["rebuild"] is True

    def test_index_stats(self) -> None:
        """Test parsing index command with stats option."""
        _cmd, bound, _extra = app.parse_args(["index", "--stats"])
        assert bound.kwargs["stats"] is True

    def test_index_clear(self) -> None:
        """Test parsing index command with clear option."""
        _cmd, bound, _extra = app.parse_args(["index", "--clear"])
        assert bound.kwargs["clear"] is True


class TestCacheCommandParsing:
    """Tests for cache command argument parsing."""

    def test_cache_stats(self) -> None:
        """Test parsing cache command with stats option."""
        _cmd, bound, _extra = app.parse_args(["cache", "--stats"])
        assert bound.kwargs["stats"] is True

    def test_cache_clear(self) -> None:
        """Test parsing cache command with clear option."""
        _cmd, bound, _extra = app.parse_args(["cache", "--clear"])
        assert bound.kwargs["clear"] is True


class TestOutputFormatParsing:
    """Tests for output format enum parsing.

    Note: --format is a global option handled by the meta launcher,
    so we use app.meta.parse_args() for these tests.
    """

    @pytest.mark.parametrize(
        ("format_arg", "expected"),
        [
            ("md", OutputFormat.md),
            ("json", OutputFormat.json),
            ("both", OutputFormat.both),
            ("summary", OutputFormat.summary),
            ("mermaid", OutputFormat.mermaid),
            ("mermaid-class", OutputFormat.mermaid_class),
            ("dot", OutputFormat.dot),
        ],
    )
    def test_format_options(self, format_arg: str, expected: OutputFormat) -> None:
        """Test parsing all format options."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--format", format_arg])
        assert bound.kwargs["output_format"] == expected


class TestReportCommandParsing:
    """Tests for report command argument parsing."""

    def test_report_basic(self) -> None:
        """Test parsing report command."""
        _cmd, bound, _extra = app.parse_args(
            [
                "report",
                "refactor-impact",
                "--target",
                "function:build_graph",
            ]
        )
        assert bound.args[0] == "refactor-impact"  # positional arg
        assert bound.kwargs["target"] == "function:build_graph"

    def test_report_with_options(self) -> None:
        """Test parsing report command with various options."""
        _cmd, bound, _extra = app.parse_args(
            [
                "report",
                "safety-reliability",
                "--target",
                "class:MyClass",
                "--in",
                "src/",
                "--param",
                "config",
                "--to",
                "foo(a, b)",
            ]
        )
        assert bound.args[0] == "safety-reliability"  # positional arg
        assert bound.kwargs["target"] == "class:MyClass"
        assert bound.kwargs["in_dir"] == "src/"
        assert bound.kwargs["param"] == "config"
        assert bound.kwargs["signature"] == "foo(a, b)"
