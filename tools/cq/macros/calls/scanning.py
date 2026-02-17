"""Ripgrep-based candidate finding for call sites.

Provides efficient call-site candidate discovery using ripgrep
pattern matching with language-aware scoping.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from tools.cq.core.types import QueryLanguageScope
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import find_call_candidates

if TYPE_CHECKING:
    from tools.cq.search._shared.types import SearchLimits


def rg_find_candidates(
    function_name: str,
    root: Path,
    *,
    limits: SearchLimits | None = None,
) -> list[tuple[Path, int]]:
    """Public wrapper for ripgrep candidate discovery.

    Returns:
        Candidate locations as ``(path, line)`` tuples.
    """
    return _rg_find_candidates(function_name, root, limits=limits)


def _rg_find_candidates(
    function_name: str,
    root: Path,
    *,
    limits: SearchLimits | None = None,
    lang_scope: str = "auto",
) -> list[tuple[Path, int]]:
    """Use ripgrep to find candidate files/lines.

    Parameters
    ----------
    function_name : str
        Name of the function to find calls for.
    root : Path
        Repository root.
    limits : SearchLimits | None, optional
        Search limits, defaults to INTERACTIVE profile.

    Returns:
    -------
    list[tuple[Path, int]]
        Candidate (file, line) pairs with absolute paths.
    """
    limits = limits or INTERACTIVE
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    return find_call_candidates(
        root,
        search_name,
        limits=limits,
        lang_scope=cast("QueryLanguageScope", lang_scope),
    )


def group_candidates(candidates: list[tuple[Path, int]]) -> dict[Path, list[int]]:
    """Public wrapper for candidate grouping.

    Returns:
        Candidate line numbers grouped by file path.
    """
    return _group_candidates(candidates)


def _group_candidates(candidates: list[tuple[Path, int]]) -> dict[Path, list[int]]:
    """Group candidate matches by file.

    Parameters
    ----------
    candidates : list[tuple[Path, int]]
        List of (file, line) candidate pairs.

    Returns:
    -------
    dict[Path, list[int]]
        Mapping of file to candidate line numbers.
    """
    by_file: dict[Path, list[int]] = {}
    for file, line in candidates:
        by_file.setdefault(file, []).append(line)
    return by_file


__all__ = [
    "group_candidates",
    "rg_find_candidates",
]
