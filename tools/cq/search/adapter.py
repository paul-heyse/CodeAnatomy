"""Native ripgrep-backed search helper functions."""

from __future__ import annotations

from pathlib import Path

from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguageScope,
    ripgrep_types_for_scope,
)
from tools.cq.search.classifier import QueryMode
from tools.cq.search.profiles import DEFAULT, SearchLimits
from tools.cq.search.rg_native import run_rg_json
from tools.cq.search.timeout import search_sync_with_timeout


def _extract_path(data: dict[str, object]) -> str | None:
    path_obj = data.get("path")
    if not isinstance(path_obj, dict):
        return None
    text = path_obj.get("text")
    if isinstance(text, str):
        return text
    return None


def _extract_line_number(data: dict[str, object]) -> int | None:
    value = data.get("line_number")
    if isinstance(value, int):
        return value
    return None


def _extract_line_text(data: dict[str, object]) -> str:
    lines_obj = data.get("lines")
    if not isinstance(lines_obj, dict):
        return ""
    text = lines_obj.get("text")
    return text if isinstance(text, str) else ""


def find_def_lines(file_path: Path) -> list[tuple[int, int]]:
    """Find all top-level def/async/class and Rust type lines with indentation.

    Returns:
    -------
    list[tuple[int, int]]
        `(line_number, indent)` tuples.
    """
    if not file_path.exists():
        return []
    try:
        content = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []

    results: list[tuple[int, int]] = []
    for i, line in enumerate(content.splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith(
            (
                "def ",
                "async def ",
                "class ",
                "fn ",
                "pub fn ",
                "struct ",
                "enum ",
                "trait ",
                "impl ",
            )
        ):
            indent = len(line) - len(stripped)
            results.append((i, indent))
    return results


def find_files_with_pattern(
    root: Path,
    pattern: str,
    *,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    limits: SearchLimits | None = None,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
) -> list[Path]:
    """Find files containing pattern with native ripgrep.

    Returns:
    -------
    list[Path]
        Absolute file paths containing at least one match.
    """
    if not root.exists():
        return []
    limits = limits or DEFAULT
    include_globs = include_globs or []
    exclude_globs = exclude_globs or []

    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            limits.timeout_seconds,
            kwargs={
                "root": root,
                "pattern": pattern,
                "mode": QueryMode.REGEX,
                "lang_types": tuple(ripgrep_types_for_scope(lang_scope)),
                "include_globs": include_globs,
                "exclude_globs": exclude_globs,
                "limits": limits,
            },
        )
    except TimeoutError:
        return []
    seen: set[Path] = set()
    for event in proc.events:
        if event.type != "match" or not isinstance(event.data, dict):
            continue
        rel_path = _extract_path(event.data)
        if rel_path is None:
            continue
        seen.add((root / rel_path).resolve())
        if len(seen) >= limits.max_files:
            break
    return sorted(seen)


def find_call_candidates(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits | None = None,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
) -> list[tuple[Path, int]]:
    """Find candidate call sites for function/method symbols.

    Returns:
    -------
    list[tuple[Path, int]]
        Absolute path and 1-based line number tuples.
    """
    if not root.exists():
        return []
    limits = limits or DEFAULT
    symbol = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\b{symbol}\s*\("
    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            limits.timeout_seconds,
            kwargs={
                "root": root,
                "pattern": pattern,
                "mode": QueryMode.REGEX,
                "lang_types": tuple(ripgrep_types_for_scope(lang_scope)),
                "include_globs": [],
                "exclude_globs": [],
                "limits": limits,
            },
        )
    except TimeoutError:
        return []

    results: list[tuple[Path, int]] = []
    for event in proc.events:
        if event.type != "match" or not isinstance(event.data, dict):
            continue
        rel_path = _extract_path(event.data)
        line = _extract_line_number(event.data)
        if rel_path is None or line is None:
            continue
        results.append(((root / rel_path).resolve(), line))
        if len(results) >= limits.max_total_matches:
            break
    return results


def find_callers(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits | None = None,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
) -> list[tuple[Path, int]]:
    """Find callers for a symbol.

    Returns:
    -------
    list[tuple[Path, int]]
        Absolute path and 1-based line number tuples.
    """
    return find_call_candidates(root, function_name, limits=limits, lang_scope=lang_scope)


def search_content(
    root: Path,
    pattern: str,
    *,
    file_globs: list[str] | None = None,
    limits: SearchLimits | None = None,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
) -> list[tuple[Path, int, str]]:
    """Search file contents and return `(path, line, text)` triples.

    Returns:
    -------
    list[tuple[Path, int, str]]
        Absolute path, 1-based line number, and line text triples.
    """
    if not root.exists():
        return []
    limits = limits or DEFAULT
    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            limits.timeout_seconds,
            kwargs={
                "root": root,
                "pattern": pattern,
                "mode": QueryMode.REGEX,
                "lang_types": tuple(ripgrep_types_for_scope(lang_scope)),
                "include_globs": file_globs or [],
                "exclude_globs": [],
                "limits": limits,
            },
        )
    except TimeoutError:
        return []
    results: list[tuple[Path, int, str]] = []
    for event in proc.events:
        if event.type != "match" or not isinstance(event.data, dict):
            continue
        rel_path = _extract_path(event.data)
        line = _extract_line_number(event.data)
        if rel_path is None or line is None:
            continue
        results.append(((root / rel_path).resolve(), line, _extract_line_text(event.data)))
        if len(results) >= limits.max_total_matches:
            break
    return results


__all__ = [
    "find_call_candidates",
    "find_callers",
    "find_def_lines",
    "find_files_with_pattern",
    "search_content",
]
