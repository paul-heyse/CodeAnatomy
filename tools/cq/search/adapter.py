"""Adapter layer for rpygrep search operations.

Provides high-level search functions that integrate rpygrep with search profiles.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from rpygrep import RipGrepSearch

if TYPE_CHECKING:
    from tools.cq.search.profiles import SearchLimits

from tools.cq.search.profiles import DEFAULT
from tools.cq.search.timeout import search_sync_with_timeout

_CASE_SENSITIVE_DEFAULT = True


class _RgLines(Protocol):
    text: str | None


class _RgMatchData(Protocol):
    line_number: int
    lines: _RgLines


class _RgMatch(Protocol):
    data: _RgMatchData


class _RgResult(Protocol):
    path: str
    matches: Sequence[_RgMatch]


def find_def_lines(file_path: Path) -> list[tuple[int, int]]:
    """Find all def/async def lines in a file with their indentation.

    Parameters
    ----------
    file_path
        Path to the Python file to scan.

    Returns
    -------
    list[tuple[int, int]]
        List of (line_number, indent_level) tuples. Line numbers are 1-indexed.
    """
    if not file_path.exists():
        return []

    results: list[tuple[int, int]] = []
    try:
        content = file_path.read_text(encoding="utf-8")
        for i, line in enumerate(content.splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith(("def ", "async def ")):
                indent = len(line) - len(stripped)
                results.append((i, indent))
    except (OSError, UnicodeDecodeError):
        return []

    return results


def find_files_with_pattern(
    root: Path,
    pattern: str,
    *,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Find files containing a pattern using rpygrep.

    Parameters
    ----------
    root : Path
        Root directory to search from
    pattern : str
        Regex pattern to search for
    include_globs : list[str] | None, optional
        File glob patterns to include (e.g., ["*.py", "*.pyi"])
    exclude_globs : list[str] | None, optional
        File glob patterns to exclude
    limits : SearchLimits | None, optional
        Search limits, defaults to DEFAULT profile

    Returns
    -------
    list[Path]
        List of file paths containing matches

    """
    # Return empty list for non-existent directories
    if not root.exists():
        return []

    limits = limits or DEFAULT
    include_globs = include_globs or ["*.py", "*.pyi"]
    exclude_globs = exclude_globs or []

    # Build rpygrep command
    searcher = (
        RipGrepSearch()
        .set_working_directory(root)
        .add_pattern(pattern)
        .case_sensitive(_CASE_SENSITIVE_DEFAULT)
        .max_count(limits.max_matches_per_file)
        .max_depth(limits.max_depth)
        .max_file_size(limits.max_file_size_bytes)
        .add_safe_defaults()
        .as_json()
    )

    # Add include globs
    for glob in include_globs:
        searcher = searcher.include_glob(glob)

    # Add exclude globs
    for glob in exclude_globs:
        searcher = searcher.exclude_glob(glob)

    # Collect unique file paths (convert to absolute)
    try:
        results = search_sync_with_timeout(
            lambda: list(searcher.run()),
            limits.timeout_seconds,
        )
    except TimeoutError:
        return []
    seen_files: set[Path] = set()
    for result in results:
        if len(seen_files) >= limits.max_files:
            break
        # rpygrep returns paths relative to working_directory
        abs_path = (root / result.path).resolve()
        seen_files.add(abs_path)

    return sorted(seen_files)


def find_call_candidates(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits | None = None,
) -> list[tuple[Path, int]]:
    """Find potential call sites for a function.

    Searches for patterns like `function_name(` to locate call sites.

    Parameters
    ----------
    root : Path
        Root directory to search from
    function_name : str
        Name of the function to find callers for
    limits : SearchLimits | None, optional
        Search limits, defaults to DEFAULT profile

    Returns
    -------
    list[tuple[Path, int]]
        List of (file_path, line_number) tuples for potential call sites

    """
    # Return empty list for non-existent directories
    if not root.exists():
        return []

    limits = limits or DEFAULT

    # Pattern to find function calls: function_name(
    pattern = rf"\b{function_name}\s*\("

    searcher = (
        RipGrepSearch()
        .set_working_directory(root)
        .add_pattern(pattern)
        .include_type("py")
        .case_sensitive(_CASE_SENSITIVE_DEFAULT)
        .max_count(limits.max_matches_per_file)
        .max_depth(limits.max_depth)
        .max_file_size(limits.max_file_size_bytes)
        .add_safe_defaults()
        .as_json()
    )

    candidates: list[tuple[Path, int]] = []
    seen_files: set[Path] = set()
    try:
        results = search_sync_with_timeout(
            lambda: list(searcher.run()),
            limits.timeout_seconds,
        )
    except TimeoutError:
        return []
    for result in results:
        # rpygrep returns paths relative to working_directory
        abs_path = (root / result.path).resolve()
        if abs_path not in seen_files:
            if len(seen_files) >= limits.max_files:
                break
            seen_files.add(abs_path)
        for match in result.matches:
            if len(candidates) >= limits.max_total_matches:
                break
            candidates.append((abs_path, match.data.line_number))

        if len(candidates) >= limits.max_total_matches:
            break

    return candidates


def find_callers(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits | None = None,
) -> list[tuple[Path, int]]:
    """Find callers of a function.

    Similar to find_call_candidates but may apply additional filtering.

    Parameters
    ----------
    root : Path
        Root directory to search from
    function_name : str
        Name of the function to find callers for
    limits : SearchLimits | None, optional
        Search limits, defaults to DEFAULT profile

    Returns
    -------
    list[tuple[Path, int]]
        List of (file_path, line_number) tuples for callers

    """
    # For now, this is identical to find_call_candidates
    # In future phases, this may incorporate AST validation
    return find_call_candidates(root, function_name, limits=limits)


def search_content(
    root: Path,
    pattern: str,
    *,
    file_globs: list[str] | None = None,
    limits: SearchLimits | None = None,
) -> list[tuple[Path, int, str]]:
    """General content search across files.

    Parameters
    ----------
    root : Path
        Root directory to search from
    pattern : str
        Regex pattern to search for
    file_globs : list[str] | None, optional
        File glob patterns to include, defaults to ["*.py", "*.pyi"]
    limits : SearchLimits | None, optional
        Search limits, defaults to DEFAULT profile

    Returns
    -------
    list[tuple[Path, int, str]]
        List of (file_path, line_number, matched_line) tuples

    """
    # Return empty list for non-existent directories
    if not root.exists():
        return []

    limits = limits or DEFAULT
    file_globs = file_globs or ["*.py", "*.pyi"]

    searcher = _build_content_searcher(root, pattern, file_globs, limits)
    return _collect_content_matches(root, searcher, limits)


def _build_content_searcher(
    root: Path,
    pattern: str,
    file_globs: list[str],
    limits: SearchLimits,
) -> RipGrepSearch:
    searcher = (
        RipGrepSearch()
        .set_working_directory(root)
        .add_pattern(pattern)
        .case_sensitive(_CASE_SENSITIVE_DEFAULT)
        .max_count(limits.max_matches_per_file)
        .max_depth(limits.max_depth)
        .max_file_size(limits.max_file_size_bytes)
        .add_safe_defaults()
        .as_json()
    )
    for glob in file_globs:
        searcher = searcher.include_glob(glob)
    return searcher


def _collect_content_matches(
    root: Path,
    searcher: RipGrepSearch,
    limits: SearchLimits,
) -> list[tuple[Path, int, str]]:
    try:
        results = search_sync_with_timeout(
            lambda: list(searcher.run()),
            limits.timeout_seconds,
        )
    except TimeoutError:
        return []
    results = cast("list[_RgResult]", results)
    return _collect_content_results(root, results, limits)


def _collect_content_results(
    root: Path,
    results: Sequence[_RgResult],
    limits: SearchLimits,
) -> list[tuple[Path, int, str]]:
    matches: list[tuple[Path, int, str]] = []
    file_match_counts: dict[Path, int] = {}
    seen_files: set[Path] = set()
    for result in results:
        file_path = (root / result.path).resolve()
        if not _should_include_file(file_path, seen_files, limits):
            break
        for match in result.matches:
            if _should_stop_content_matches(matches, file_match_counts, file_path, limits):
                break
            line_content = match.data.lines.text or ""
            matches.append((file_path, match.data.line_number, line_content))
            file_match_counts[file_path] = file_match_counts.get(file_path, 0) + 1
        if len(matches) >= limits.max_total_matches:
            break
    return matches


def _should_include_file(file_path: Path, seen_files: set[Path], limits: SearchLimits) -> bool:
    if file_path in seen_files:
        return True
    if len(seen_files) >= limits.max_files:
        return False
    seen_files.add(file_path)
    return True


def _should_stop_content_matches(
    matches: list[tuple[Path, int, str]],
    file_match_counts: dict[Path, int],
    file_path: Path,
    limits: SearchLimits,
) -> bool:
    if len(matches) >= limits.max_total_matches:
        return True
    return file_match_counts.get(file_path, 0) >= limits.max_matches_per_file
