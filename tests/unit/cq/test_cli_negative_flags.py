"""Tests for negative flag behavior in cq CLI.

These tests verify that:
1. --no-cache works for the query command (global behavior removed)
2. --no-rebuild, --no-stats, --no-clear do NOT work for admin commands
"""

from __future__ import annotations

import pytest
from tools.cq.cli_app.app import app


class TestNegativeFlagsQuery:
    """Tests for negative flags on query command."""

    def test_no_cache_works(self) -> None:
        """Test that --no-cache is parsed correctly for query command."""
        _cmd, bound, _extra = app.parse_args(["q", "entity=function", "--no-cache"])
        assert bound.kwargs["no_cache"] is True

    def test_no_save_artifact_works(self) -> None:
        """Test that --no-save-artifact is parsed correctly.

        Note: --no-save-artifact is a global option handled by the meta launcher,
        so we use app.meta.parse_args() for this test.
        """
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--no-save-artifact"])
        assert bound.kwargs["no_save_artifact"] is True


class TestNegativeFlagsAdmin:
    """Tests for negative flags on admin commands.

    Admin command boolean flags should NOT have negative forms.
    """

    def test_index_no_rebuild_fails(self) -> None:
        """Test that --no-rebuild is NOT a valid option for index."""
        with pytest.raises(SystemExit):
            app.parse_args(["index", "--no-rebuild"], exit_on_error=True, print_error=False)

    def test_index_no_stats_fails(self) -> None:
        """Test that --no-stats is NOT a valid option for index."""
        with pytest.raises(SystemExit):
            app.parse_args(["index", "--no-stats"], exit_on_error=True, print_error=False)

    def test_index_no_clear_fails(self) -> None:
        """Test that --no-clear is NOT a valid option for index."""
        with pytest.raises(SystemExit):
            app.parse_args(["index", "--no-clear"], exit_on_error=True, print_error=False)

    def test_cache_no_stats_fails(self) -> None:
        """Test that --no-stats is NOT a valid option for cache."""
        with pytest.raises(SystemExit):
            app.parse_args(["cache", "--no-stats"], exit_on_error=True, print_error=False)

    def test_cache_no_clear_fails(self) -> None:
        """Test that --no-clear is NOT a valid option for cache."""
        with pytest.raises(SystemExit):
            app.parse_args(["cache", "--no-clear"], exit_on_error=True, print_error=False)


class TestAdminPositiveFlags:
    """Tests that positive admin flags still work."""

    def test_index_rebuild(self) -> None:
        """Test that --rebuild is parsed correctly."""
        _cmd, bound, _extra = app.parse_args(["index", "--rebuild"])
        assert bound.kwargs["rebuild"] is True

    def test_index_stats(self) -> None:
        """Test that --stats is parsed correctly."""
        _cmd, bound, _extra = app.parse_args(["index", "--stats"])
        assert bound.kwargs["stats"] is True

    def test_index_clear(self) -> None:
        """Test that --clear is parsed correctly."""
        _cmd, bound, _extra = app.parse_args(["index", "--clear"])
        assert bound.kwargs["clear"] is True

    def test_cache_stats(self) -> None:
        """Test that --stats is parsed correctly for cache."""
        _cmd, bound, _extra = app.parse_args(["cache", "--stats"])
        assert bound.kwargs["stats"] is True

    def test_cache_clear(self) -> None:
        """Test that --clear is parsed correctly for cache."""
        _cmd, bound, _extra = app.parse_args(["cache", "--clear"])
        assert bound.kwargs["clear"] is True
