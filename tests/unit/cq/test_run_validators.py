"""Tests for run input and search validators."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.app import app


def test_run_requires_input() -> None:
    """Ensure run requires at least one input source."""
    with pytest.raises(SystemExit):
        app.parse_args(["run"], exit_on_error=True, print_error=False)


def test_search_regex_literal_mutually_exclusive() -> None:
    """Ensure --regex and --literal cannot both be set."""
    with pytest.raises(SystemExit):
        app.parse_args(
            ["search", "foo", "--regex", "--literal"],
            exit_on_error=True,
            print_error=False,
        )
