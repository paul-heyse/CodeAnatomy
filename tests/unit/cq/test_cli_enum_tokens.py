"""Tests for enum token parsing with name_transform.

These tests verify that format tokens like 'mermaid-class' are correctly
parsed to their corresponding enum values.
"""

from __future__ import annotations

import pytest
from tools.cq.cli_app.app import app
from tools.cq.cli_app.types import OutputFormat


class TestOutputFormatTokens:
    """Tests for output format enum parsing with name_transform.

    Note: --format is a global option handled by the meta launcher,
    so we use app.meta.parse_args() for these tests.
    """

    @pytest.mark.parametrize(
        ("token", "expected"),
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
    def test_format_token_parsing(self, token: str, expected: OutputFormat) -> None:
        """Test that CLI tokens are correctly parsed to enum values."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--format", token])
        global_opts = bound.kwargs["global_opts"]
        assert global_opts.output_format == expected

    def test_mermaid_class_hyphen_form(self) -> None:
        """Test that mermaid-class (with hyphen) works.

        This is the critical test for name_transform working correctly.
        The enum member is mermaid_class (underscore) but CLI token is
        mermaid-class (hyphen).
        """
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--format", "mermaid-class"])
        global_opts = bound.kwargs["global_opts"]
        assert global_opts.output_format == OutputFormat.mermaid_class
        assert str(global_opts.output_format) == "mermaid-class"

    def test_invalid_format_fails(self) -> None:
        """Test that invalid format token fails parsing."""
        with pytest.raises(SystemExit):
            app.meta.parse_args(
                ["calls", "foo", "--format", "invalid"], exit_on_error=True, print_error=False
            )


class TestEnumValuePreservation:
    """Tests that enum values are preserved correctly."""

    def test_output_format_str_returns_value(self) -> None:
        """Test that str(OutputFormat) returns the CLI token."""
        assert str(OutputFormat.md) == "md"
        assert str(OutputFormat.mermaid_class) == "mermaid-class"

    def test_output_format_value_lookup(self) -> None:
        """Test creating enum from value string."""
        assert OutputFormat("md") == OutputFormat.md
        assert OutputFormat("mermaid-class") == OutputFormat.mermaid_class
