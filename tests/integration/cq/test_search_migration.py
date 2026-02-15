"""Integration tests for native rg search migration.

Verifies that the native rg-based search functions work correctly
with the cq macro commands.
"""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.pipeline.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.rg.adapter import (
    find_call_candidates,
    find_callers,
    find_files_with_pattern,
    search_content,
)


class TestSearchAdapterIntegration:
    """Test native rg adapter functions work with real codebase."""

    def test_find_files_with_pattern_in_repo(self) -> None:
        """Test finding files in the actual repository."""
        repo_root = Path(__file__).parent.parent.parent.parent

        # Find Python files containing "def test_"
        files = find_files_with_pattern(
            repo_root,
            pattern=r"def test_",
            include_globs=["**/tests/**/*.py"],
            limits=SearchLimits(max_files=100),
        )

        assert len(files) > 0
        assert all(f.suffix == ".py" for f in files)
        assert all("tests" in str(f) for f in files)

    def test_find_call_candidates_finds_real_calls(self) -> None:
        """Test finding call candidates for a known function."""
        repo_root = Path(__file__).parent.parent.parent.parent

        # find_files_with_pattern is called in various places
        results = find_call_candidates(
            repo_root,
            function_name="find_files_with_pattern",
            limits=SearchLimits(max_total_matches=50),
        )

        # Should find at least the calls in tests
        assert len(results) > 0
        # Results should be (Path, int) tuples
        for file_path, line_num in results:
            assert isinstance(file_path, Path)
            assert file_path.exists()
            assert isinstance(line_num, int)
            assert line_num > 0

    def test_find_callers_matches_call_candidates(self) -> None:
        """Test that find_callers returns same results as find_call_candidates."""
        repo_root = Path(__file__).parent.parent.parent.parent
        limits = SearchLimits(max_total_matches=20)

        candidates = find_call_candidates(
            repo_root,
            function_name="SearchLimits",
            limits=limits,
        )

        callers = find_callers(
            repo_root,
            function_name="SearchLimits",
            limits=limits,
        )

        # They should return the same results (for now they're identical)
        assert len(candidates) == len(callers)

    def test_search_content_returns_line_content(self) -> None:
        """Test that search_content returns actual line content."""
        repo_root = Path(__file__).parent.parent.parent.parent

        results = search_content(
            repo_root,
            pattern="class SearchLimits",
            limits=SearchLimits(max_total_matches=10),
        )

        assert len(results) > 0
        for file_path, line_num, content in results:
            assert isinstance(file_path, Path)
            assert file_path.exists()
            assert isinstance(line_num, int)
            assert isinstance(content, str)
            assert "SearchLimits" in content


class TestMacroIntegration:
    """Test that macros work with the new search adapter."""

    def test_calls_macro_import(self) -> None:
        """Test that calls macro imports successfully."""
        from tools.cq.macros.calls import _rg_find_candidates, cmd_calls

        # Should not raise
        assert callable(cmd_calls)
        assert callable(_rg_find_candidates)

    def test_impact_macro_import(self) -> None:
        """Test that impact macro imports successfully."""
        from tools.cq.macros.impact import _find_callers_via_search, cmd_impact

        # Should not raise
        assert callable(cmd_impact)
        assert callable(_find_callers_via_search)

    def test_sig_impact_macro_import(self) -> None:
        """Test that sig_impact macro imports successfully."""
        from tools.cq.macros.sig_impact import _collect_sites, cmd_sig_impact

        # Should not raise
        assert callable(cmd_sig_impact)
        assert callable(_collect_sites)

    def test_toolchain_detects_rg(self) -> None:
        """Test that toolchain correctly detects ripgrep."""
        from tools.cq.core.toolchain import Toolchain

        tc = Toolchain.detect()

        assert tc.has_rg
        assert tc.rg_available
        assert tc.rg_version is not None
        # Should not raise
        tc.require_rg()


class TestSearchLimitsProfiles:
    """Test that search limit profiles work correctly."""

    def test_interactive_profile_is_fast(self) -> None:
        """Test INTERACTIVE profile has reasonable limits for quick queries."""
        assert INTERACTIVE.timeout_seconds <= 10.0
        assert INTERACTIVE.max_files <= 1000

    def test_custom_limits_work(self) -> None:
        """Test custom SearchLimits work with adapter."""
        repo_root = Path(__file__).parent.parent.parent.parent

        # Very restrictive limits
        limits = SearchLimits(
            max_files=5,
            max_total_matches=3,
            max_matches_per_file=2,
            timeout_seconds=5.0,
        )

        results = search_content(
            repo_root,
            pattern="import",
            limits=limits,
        )

        # Should respect the limits
        assert len(results) <= 3
