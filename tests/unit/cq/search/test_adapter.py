"""Tests for native ripgrep adapter integration."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.adapter import (
    find_call_candidates,
    find_callers,
    find_files_with_pattern,
    search_content,
)
from tools.cq.search.profiles import SearchLimits


@pytest.fixture
def sample_repo(tmp_path: Path) -> Path:
    """Create a temporary directory with sample Python files.

    Returns
    -------
    Path
        Path to the sample repository root.

    Structure:
        repo/
        ├── src/
        │   ├── __init__.py
        │   ├── module_a.py
        │   └── utils/
        │       ├── __init__.py
        │       └── helpers.py
        ├── tests/
        │   ├── __init__.py
        │   └── test_module.py
        └── excluded/
            └── ignore_me.py
    """
    repo = tmp_path / "repo"
    repo.mkdir()

    # Create src directory structure
    src = repo / "src"
    src.mkdir()
    (src / "__init__.py").write_text("")

    (src / "module_a.py").write_text(
        """\
def process_data(data: list) -> dict:
    '''Process input data and return results.'''
    return {"processed": data}

def calculate_sum(numbers: list[int]) -> int:
    '''Calculate sum of numbers.'''
    return sum(numbers)

class DataProcessor:
    def __init__(self):
        self.data = []

    def add_item(self, item):
        self.data.append(item)
"""
    )

    # Create utils subdirectory
    utils = src / "utils"
    utils.mkdir()
    (utils / "__init__.py").write_text("")

    (utils / "helpers.py").write_text(
        """\
from ..module_a import process_data, calculate_sum

def helper_function():
    '''Helper function that calls module_a functions.'''
    data = [1, 2, 3]
    result = process_data(data)
    total = calculate_sum(data)
    return result, total

def another_helper():
    '''Another helper function.'''
    return process_data([])
"""
    )

    # Create tests directory
    tests = repo / "tests"
    tests.mkdir()
    (tests / "__init__.py").write_text("")

    (tests / "test_module.py").write_text(
        """\
from src.module_a import process_data, DataProcessor

def test_process_data():
    '''Test process_data function.'''
    result = process_data([1, 2, 3])
    assert result["processed"] == [1, 2, 3]

def test_data_processor():
    '''Test DataProcessor class.'''
    processor = DataProcessor()
    processor.add_item(1)
    assert len(processor.data) == 1
"""
    )

    # Create excluded directory
    excluded = repo / "excluded"
    excluded.mkdir()
    (excluded / "ignore_me.py").write_text(
        """\
def ignored_function():
    '''This should be excluded from searches.'''
    pass
"""
    )

    return repo


class TestFindFilesWithPattern:
    """Test file search with pattern matching."""

    def test_finds_python_files_with_def(self, sample_repo: Path) -> None:
        """Test finding Python files containing 'def'."""
        files = find_files_with_pattern(sample_repo, pattern=r"def ")
        file_names = {f.name for f in files}

        assert "module_a.py" in file_names
        assert "helpers.py" in file_names
        assert "test_module.py" in file_names

    def test_respects_include_globs(self, sample_repo: Path) -> None:
        """Test include_globs filters to matching files."""
        files = find_files_with_pattern(
            sample_repo,
            pattern=r"def ",
            include_globs=["**/src/**/*.py"],
        )
        file_names = {f.name for f in files}

        # Should only include src files with 'def'
        assert "module_a.py" in file_names
        assert "helpers.py" in file_names
        # Should not include test files
        assert "test_module.py" not in file_names

    def test_respects_exclude_globs(self, sample_repo: Path) -> None:
        """Test exclude_globs filters out matching files."""
        files = find_files_with_pattern(
            sample_repo,
            pattern=r"def ",
            exclude_globs=["**/excluded/**", "**/__init__.py"],
        )
        file_names = {f.name for f in files}

        assert "module_a.py" in file_names
        assert "helpers.py" in file_names
        # Should exclude __init__.py (no 'def' anyway) and excluded directory
        assert "ignore_me.py" not in file_names

    def test_combined_include_exclude(self, sample_repo: Path) -> None:
        """Test combining include and exclude globs."""
        files = find_files_with_pattern(
            sample_repo,
            pattern=r"def ",
            include_globs=["**/src/**/*.py"],
            exclude_globs=["**/__init__.py"],
        )
        file_names = {f.name for f in files}

        assert "module_a.py" in file_names
        assert "helpers.py" in file_names
        assert "test_module.py" not in file_names

    def test_pattern_matching(self, sample_repo: Path) -> None:
        """Test pattern matching in file content."""
        # Find files containing test function definitions
        files = find_files_with_pattern(sample_repo, pattern=r"def test_")
        file_names = {f.name for f in files}
        assert "test_module.py" in file_names
        # Should not match non-test files
        assert "module_a.py" not in file_names

    def test_empty_result(self, sample_repo: Path) -> None:
        """Test search with no matching files."""
        files = find_files_with_pattern(sample_repo, pattern=r"nonexistent_pattern_xyz")
        assert len(files) == 0

    def test_respects_max_files(self, sample_repo: Path) -> None:
        """Test max_files limit is respected."""
        limits = SearchLimits(max_files=1, max_total_matches=100)
        files = find_files_with_pattern(sample_repo, pattern=r"def ", limits=limits)
        assert len(files) <= 1

    def test_timeout_returns_empty(
        self, sample_repo: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test timeout handling returns empty results."""
        from tools.cq.search import adapter as adapter_module

        def _raise_timeout(*_args: object, **_kwargs: object) -> list[object]:
            msg = "timeout"
            raise TimeoutError(msg)

        monkeypatch.setattr(adapter_module, "search_sync_with_timeout", _raise_timeout)
        limits = SearchLimits(timeout_seconds=0.001)
        files = find_files_with_pattern(sample_repo, pattern=r"def ", limits=limits)
        assert files == []


class TestFindCallCandidates:
    """Test function call candidate detection."""

    def test_finds_function_calls(self, sample_repo: Path) -> None:
        """Test finding calls to a function."""
        limits = SearchLimits(max_total_matches=100)
        results = find_call_candidates(
            sample_repo,
            function_name="process_data",
            limits=limits,
        )

        # Should find calls in helpers.py and test_module.py
        assert len(results) >= 2
        file_names = {r[0].name for r in results}
        assert "helpers.py" in file_names
        assert "test_module.py" in file_names

    def test_finds_method_calls(self, sample_repo: Path) -> None:
        """Test finding method calls."""
        limits = SearchLimits(max_total_matches=100)
        results = find_call_candidates(
            sample_repo,
            function_name="add_item",
            limits=limits,
        )

        # Should find call in test_module.py
        assert len(results) >= 1
        assert any(r[0].name == "test_module.py" for r in results)

    def test_respects_limits(self, sample_repo: Path) -> None:
        """Test result limits are respected."""
        limits = SearchLimits(max_total_matches=1)
        results = find_call_candidates(
            sample_repo,
            function_name="process_data",
            limits=limits,
        )

        # Should respect max_total_matches
        assert len(results) <= 1


class TestFindCallers:
    """Test caller location detection."""

    def test_finds_callers(self, sample_repo: Path) -> None:
        """Test finding caller locations."""
        limits = SearchLimits(max_total_matches=100)
        results = find_callers(
            sample_repo,
            function_name="process_data",
            limits=limits,
        )

        # Should find callers in helpers.py and test_module.py
        assert len(results) >= 2
        file_names = {r[0].name for r in results}
        assert "helpers.py" in file_names
        assert "test_module.py" in file_names

    def test_caller_result_format(self, sample_repo: Path) -> None:
        """Test caller result contains expected format (Path, line_number)."""
        limits = SearchLimits(max_total_matches=100)
        results = find_callers(
            sample_repo,
            function_name="process_data",
            limits=limits,
        )

        for file_path, line_number in results:
            assert isinstance(file_path, Path)
            assert file_path.exists()
            assert isinstance(line_number, int)
            assert line_number > 0

    def test_respects_limits(self, sample_repo: Path) -> None:
        """Test caller search respects limits."""
        limits = SearchLimits(max_total_matches=1)
        results = find_callers(
            sample_repo,
            function_name="process_data",
            limits=limits,
        )

        assert len(results) <= 1


class TestSearchContent:
    """Test content search functionality."""

    def test_searches_content(self, sample_repo: Path) -> None:
        """Test searching file contents."""
        limits = SearchLimits(max_total_matches=100)
        results = search_content(
            sample_repo,
            pattern="process_data",
            limits=limits,
        )

        # Should find matches in multiple files
        assert len(results) > 0
        file_names = {r[0].name for r in results}
        assert "module_a.py" in file_names
        assert "helpers.py" in file_names

    def test_result_format(self, sample_repo: Path) -> None:
        """Test search results have correct format."""
        limits = SearchLimits(max_total_matches=100)
        results = search_content(
            sample_repo,
            pattern="def process_data",
            limits=limits,
        )

        # Results should be (path, line_number, content) tuples
        for path, line_num, content in results:
            assert isinstance(path, Path)
            assert path.exists()
            assert isinstance(line_num, int)
            assert line_num > 0
            assert isinstance(content, str)
            assert "process_data" in content

    def test_respects_file_globs(self, sample_repo: Path) -> None:
        """Test file_globs filters content search."""
        limits = SearchLimits(max_total_matches=100)
        results = search_content(
            sample_repo,
            pattern="process_data",
            limits=limits,
            file_globs=["**/utils/**/*.py"],
        )

        # Should only find matches in utils directory
        file_names = {r[0].name for r in results}
        assert "helpers.py" in file_names
        assert "module_a.py" not in file_names

    def test_respects_limits(self, sample_repo: Path) -> None:
        """Test content search respects result limits."""
        limits = SearchLimits(max_total_matches=2)
        results = search_content(
            sample_repo,
            pattern="def ",
            limits=limits,
        )

        assert len(results) <= 2

    def test_empty_result(self, sample_repo: Path) -> None:
        """Test search with no matches."""
        limits = SearchLimits(max_total_matches=100)
        results = search_content(
            sample_repo,
            pattern="nonexistent_pattern_xyz",
            limits=limits,
        )

        assert len(results) == 0


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_nonexistent_directory(self) -> None:
        """Test search in non-existent directory."""
        nonexistent = Path("/nonexistent/path")
        limits = SearchLimits(max_total_matches=100)

        # Should return empty results, not raise
        files = find_files_with_pattern(nonexistent, pattern=r"\.py$")
        assert len(files) == 0

        results = search_content(
            nonexistent,
            pattern="test",
            limits=limits,
        )
        assert len(results) == 0

    def test_empty_directory(self, tmp_path: Path) -> None:
        """Test search in empty directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        limits = SearchLimits(max_total_matches=100)

        files = find_files_with_pattern(empty_dir, pattern=r"def ")
        assert len(files) == 0

        results = search_content(
            empty_dir,
            pattern="test",
            limits=limits,
        )
        assert len(results) == 0

    def test_special_characters_in_pattern(self, sample_repo: Path) -> None:
        """Test patterns with special regex characters."""
        limits = SearchLimits(max_total_matches=100)

        # Pattern with brackets (class definition)
        results = search_content(
            sample_repo,
            pattern=r"def.*\(.*\):",
            limits=limits,
        )
        assert len(results) > 0

    @pytest.mark.parametrize(
        "glob_pattern",
        [
            "**/*.py",
            "**/src/**/*.py",
            "*.py",
        ],
    )
    def test_various_glob_patterns(
        self,
        sample_repo: Path,
        glob_pattern: str,
    ) -> None:
        """Test various glob patterns work correctly."""
        files = find_files_with_pattern(
            sample_repo,
            pattern=r"def ",
            include_globs=[glob_pattern],
        )
        # Should not raise, and should return some results for valid patterns
        assert isinstance(files, list)
