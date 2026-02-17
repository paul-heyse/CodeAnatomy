"""Native ripgrep-backed search helper functions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.core.types import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguageScope,
    is_path_in_lang_scope,
    ripgrep_types_for_scope,
)
from tools.cq.search._shared.profiles import DEFAULT, INTERACTIVE
from tools.cq.search._shared.requests import RgRunRequest
from tools.cq.search._shared.timeouts import search_sync_with_timeout
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.rg.codec import as_match_data, match_line_number, match_line_text, match_path
from tools.cq.search.rg.contracts import RgRunSettingsV1
from tools.cq.search.rg.runner import RgProcessResult, run_rg_json, run_with_settings

_DEF_LINE_PATTERN = r"^\s*(def |async def |class |fn |pub fn |struct |enum |trait |impl )"


@dataclass(frozen=True, slots=True)
class FilePatternSearchOptions:
    """Configuration surface for ripgrep-backed file pattern scans."""

    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    limits: SearchLimits | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
    paths: tuple[str, ...] = (".",)
    mode: QueryMode = QueryMode.REGEX
    extra_patterns: tuple[str, ...] = ()


def _as_absolute(root: Path, row: str) -> Path:
    candidate = Path(row.strip())
    if candidate.is_absolute():
        return candidate.resolve()
    return (root / row.strip()).resolve()


def find_def_lines(file_path: Path) -> list[tuple[int, int]]:
    """Find def/class/type lines with indentation using ripgrep.

    Returns:
        list[tuple[int, int]]: Function return value.
    """
    resolved = file_path.resolve()
    if not resolved.exists():
        return []

    root = resolved.parent
    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            DEFAULT.timeout_seconds,
            kwargs={
                "request": RgRunRequest(
                    root=root,
                    pattern=_DEF_LINE_PATTERN,
                    mode=QueryMode.REGEX,
                    lang_types=(),
                    include_globs=[],
                    exclude_globs=[],
                    limits=DEFAULT,
                    paths=(resolved.name,),
                )
            },
        )
    except TimeoutError:
        return []

    lines: list[tuple[int, int]] = []
    for event in proc.events:
        data = as_match_data(event)
        if data is None:
            continue
        line_number = match_line_number(data)
        if line_number is None:
            continue
        text = match_line_text(data)
        stripped = text.lstrip()
        indent = len(text) - len(stripped)
        lines.append((line_number, indent))
    return sorted(set(lines))


def find_files_with_pattern(
    root: Path,
    pattern: str,
    *,
    options: FilePatternSearchOptions | None = None,
) -> list[Path]:
    """Find files containing pattern with native ripgrep.

    Returns:
        list[Path]: Function return value.
    """
    if not root.exists():
        return []
    resolved = options or FilePatternSearchOptions()
    limits = resolved.limits or DEFAULT

    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            limits.timeout_seconds,
            kwargs={
                "request": RgRunRequest(
                    root=root,
                    pattern=pattern,
                    mode=resolved.mode,
                    lang_types=tuple(ripgrep_types_for_scope(resolved.lang_scope)),
                    include_globs=list(resolved.include_globs),
                    exclude_globs=list(resolved.exclude_globs),
                    limits=limits,
                    paths=resolved.paths,
                    extra_patterns=resolved.extra_patterns,
                )
            },
        )
    except TimeoutError:
        return []
    seen: set[Path] = set()
    for event in proc.events:
        data = as_match_data(event)
        if data is None:
            continue
        rel_path = match_path(data)
        if rel_path is None:
            continue
        if not is_path_in_lang_scope(rel_path, resolved.lang_scope):
            continue
        seen.add((root / rel_path).resolve())
        if len(seen) >= limits.max_files:
            break
    return sorted(seen)


def list_candidate_files(
    root: Path,
    *,
    lang_types: tuple[str, ...] = (),
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    paths: tuple[str, ...] = (".",),
    limits: SearchLimits | None = None,
) -> list[Path]:
    """List files ripgrep would scan under current scope configuration.

    Returns:
        list[Path]: Function return value.
    """
    settings = RgRunSettingsV1(
        pattern="",
        mode=QueryMode.REGEX.value,
        lang_types=lang_types,
        include_globs=tuple(include_globs or ()),
        exclude_globs=tuple(exclude_globs or ()),
        operation="files",
        paths=paths,
    )
    result = run_with_settings(root=root, limits=limits or INTERACTIVE, settings=settings)
    if result.returncode not in {0, 1}:
        return []
    rel_rows = [line.strip() for line in result.stdout_lines if line.strip()]
    return sorted(_as_absolute(root, row) for row in rel_rows)


def find_call_candidates(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits | None = None,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
) -> list[tuple[Path, int]]:
    """Find candidate call sites for function/method symbols.

    Returns:
        list[tuple[Path, int]]: Function return value.
    """
    if not root.exists():
        return []
    limits = limits or DEFAULT
    symbol = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"{re.escape(symbol)}\s*\("
    call_pattern = re.compile(rf"\b{pattern}")

    try:
        proc = _run_call_candidate_search(
            root=root,
            pattern=pattern,
            mode=QueryMode.IDENTIFIER,
            limits=limits,
            lang_scope=lang_scope,
        )
    except TimeoutError:
        return []
    identifier_rows = _collect_call_candidate_rows(
        proc,
        root=root,
        call_pattern=call_pattern,
        limits=limits,
        lang_scope=lang_scope,
    )
    if identifier_rows:
        return identifier_rows
    try:
        proc = _run_call_candidate_search(
            root=root,
            pattern=pattern,
            mode=QueryMode.REGEX,
            limits=limits,
            lang_scope=lang_scope,
        )
    except TimeoutError:
        return []
    return _collect_call_candidate_rows(
        proc,
        root=root,
        call_pattern=call_pattern,
        limits=limits,
        lang_scope=lang_scope,
    )


def _run_call_candidate_search(
    *,
    root: Path,
    pattern: str,
    mode: QueryMode,
    limits: SearchLimits,
    lang_scope: QueryLanguageScope,
) -> RgProcessResult:
    return search_sync_with_timeout(
        run_rg_json,
        limits.timeout_seconds,
        kwargs={
            "request": RgRunRequest(
                root=root,
                pattern=pattern,
                mode=mode,
                lang_types=tuple(ripgrep_types_for_scope(lang_scope)),
                include_globs=[],
                exclude_globs=[],
                limits=limits,
            )
        },
    )


def _collect_call_candidate_rows(
    proc: RgProcessResult,
    *,
    root: Path,
    call_pattern: re.Pattern[str],
    limits: SearchLimits,
    lang_scope: QueryLanguageScope,
) -> list[tuple[Path, int]]:
    rows: list[tuple[Path, int]] = []
    for event in proc.events:
        data = as_match_data(event)
        if data is None:
            continue
        rel_path = match_path(data)
        line = match_line_number(data)
        if rel_path is None or line is None:
            continue
        if not call_pattern.search(match_line_text(data)):
            continue
        if not is_path_in_lang_scope(rel_path, lang_scope):
            continue
        rows.append(((root / rel_path).resolve(), line))
        if len(rows) >= limits.max_total_matches:
            break
    return rows


def find_callers(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits | None = None,
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE,
) -> list[tuple[Path, int]]:
    """Find callers for a symbol.

    Returns:
        list[tuple[Path, int]]: Function return value.
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
    """Search file contents and return ``(path, line, text)`` triples.

    Returns:
        list[tuple[Path, int, str]]: Function return value.
    """
    if not root.exists():
        return []
    limits = limits or DEFAULT
    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            limits.timeout_seconds,
            kwargs={
                "request": RgRunRequest(
                    root=root,
                    pattern=pattern,
                    mode=QueryMode.REGEX,
                    lang_types=tuple(ripgrep_types_for_scope(lang_scope)),
                    include_globs=file_globs or [],
                    exclude_globs=[],
                    limits=limits,
                )
            },
        )
    except TimeoutError:
        return []
    results: list[tuple[Path, int, str]] = []
    for event in proc.events:
        data = as_match_data(event)
        if data is None:
            continue
        rel_path = match_path(data)
        line = match_line_number(data)
        if rel_path is None or line is None:
            continue
        if not is_path_in_lang_scope(rel_path, lang_scope):
            continue
        results.append(((root / rel_path).resolve(), line, match_line_text(data)))
        if len(results) >= limits.max_total_matches:
            break
    return results


def find_symbol_candidates(
    root: Path,
    symbol_name: str,
    *,
    lang_scope: QueryLanguageScope,
    limits: SearchLimits,
) -> list[tuple[str, int, str]]:
    """Find symbol candidates via shared rg lane for neighborhood fallback.

    Returns:
        list[tuple[str, int, str]]: Function return value.
    """
    request = RgRunRequest(
        root=root,
        pattern=re.escape(symbol_name),
        mode=QueryMode.IDENTIFIER,
        lang_types=tuple(ripgrep_types_for_scope(lang_scope)),
        include_globs=[],
        exclude_globs=[],
        limits=limits,
    )
    try:
        proc = search_sync_with_timeout(
            run_rg_json,
            limits.timeout_seconds,
            kwargs={"request": request},
        )
    except TimeoutError:
        return []

    rows: list[tuple[str, int, str]] = []
    for event in proc.events:
        data = as_match_data(event)
        if data is None:
            continue
        rel = match_path(data)
        line = match_line_number(data)
        if rel is None or line is None:
            continue
        rel_norm = normalize_repo_relative_path(rel, root=root)
        if not is_path_in_lang_scope(rel_norm, lang_scope):
            continue
        rows.append((rel_norm, line, match_line_text(data)))
        if len(rows) >= limits.max_total_matches:
            break
    return rows


def find_symbol_definition_files(
    root: Path,
    symbol_name: str,
    *,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Find Python definition files for a symbol using the shared rg lane.

    Returns:
        list[Path]: Function return value.
    """
    escaped = re.escape(symbol_name)
    pattern = rf"(def|class)\s+{escaped}\b"
    return find_files_with_pattern(
        root,
        pattern,
        options=FilePatternSearchOptions(
            include_globs=tuple(include_globs or ()),
            exclude_globs=tuple(exclude_globs or ()),
            limits=limits,
            lang_scope="python",
            mode=QueryMode.REGEX,
        ),
    )


__all__ = [
    "FilePatternSearchOptions",
    "find_call_candidates",
    "find_callers",
    "find_def_lines",
    "find_files_with_pattern",
    "find_symbol_candidates",
    "find_symbol_definition_files",
    "list_candidate_files",
    "search_content",
]
