"""Adapter layer for rpygrep search operations.

Provides high-level search functions that integrate rpygrep with search profiles.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from rpygrep import RipGrepSearch

if TYPE_CHECKING:
    from tools.cq.search.profiles import SearchLimits

from tools.cq.search.profiles import DEFAULT


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
        .case_sensitive(True)
        .as_json()
    )

    # Add include globs
    for glob in include_globs:
        searcher = searcher.include_glob(glob)

    # Add exclude globs
    for glob in exclude_globs:
        searcher = searcher.exclude_glob(glob)

    # Collect unique file paths (convert to absolute)
    seen_files: set[Path] = set()
    for result in searcher.run():
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
        .case_sensitive(True)
        .as_json()
    )

    candidates: list[tuple[Path, int]] = []
    for result in searcher.run():
        # rpygrep returns paths relative to working_directory
        abs_path = (root / result.path).resolve()
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

    # Build rpygrep command
    searcher = (
        RipGrepSearch()
        .set_working_directory(root)
        .add_pattern(pattern)
        .case_sensitive(True)
        .as_json()
    )

    # Add file globs
    for glob in file_globs:
        searcher = searcher.include_glob(glob)

    matches: list[tuple[Path, int, str]] = []
    file_match_counts: dict[Path, int] = {}

    for result in searcher.run():
        # rpygrep returns paths relative to working_directory
        file_path = (root / result.path).resolve()

        for match in result.matches:
            # Enforce per-file limit
            current_count = file_match_counts.get(file_path, 0)
            if current_count >= limits.max_matches_per_file:
                continue

            # Enforce total limit
            if len(matches) >= limits.max_total_matches:
                break

            # Extract line content from match data
            line_content = match.data.lines.text or ""

            matches.append((file_path, match.data.line_number, line_content))
            file_match_counts[file_path] = current_count + 1

        if len(matches) >= limits.max_total_matches:
            break

    return matches
