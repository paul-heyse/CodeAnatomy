"""Search-specific context windowing helpers."""

from __future__ import annotations

_TOP_LEVEL_PROLOGUE_LINE_COUNT = 3


def _line_indent(line: str) -> int:
    expanded = line.expandtabs(4)
    return len(expanded) - len(expanded.lstrip(" "))


def _find_nonblank_start(source_lines: list[str], anchor_idx: int, lower_idx: int) -> int:
    idx = anchor_idx
    while idx > lower_idx and not source_lines[idx].strip():
        idx -= 1
    while idx > lower_idx and source_lines[idx - 1].strip():
        idx -= 1
    return idx


def _find_nonblank_end(source_lines: list[str], anchor_idx: int, upper_idx: int) -> int:
    idx = anchor_idx
    while idx < upper_idx and not source_lines[idx].strip():
        idx += 1
    while idx < upper_idx and source_lines[idx + 1].strip():
        idx += 1
    return idx


def _compact_top_level_window(
    source_lines: list[str],
    *,
    match_line: int,
    def_lines: list[tuple[int, int]],
    max_lines: int = 20,
) -> dict[str, int]:
    total_lines = len(source_lines)
    first_def_line = def_lines[0][0] if def_lines else total_lines + 1
    top_level_end = min(total_lines, max(1, first_def_line - 1))
    anchor_line = min(max(1, match_line), top_level_end)
    anchor_idx = anchor_line - 1
    lower_idx = 0
    upper_idx = max(lower_idx, top_level_end - 1)
    block_start = _find_nonblank_start(source_lines, anchor_idx, lower_idx)
    block_end = _find_nonblank_end(source_lines, anchor_idx, upper_idx)

    start_idx = 0 if block_start <= _TOP_LEVEL_PROLOGUE_LINE_COUNT else block_start
    end_idx = max(block_end, min(upper_idx, anchor_idx + 4))
    if end_idx - start_idx + 1 > max_lines:
        if anchor_idx - start_idx >= max_lines:
            start_idx = max(lower_idx, anchor_idx - (max_lines // 2))
        end_idx = min(upper_idx, start_idx + max_lines - 1)

    return {"start_line": start_idx + 1, "end_line": end_idx + 1}


def compute_search_context_window(
    source_lines: list[str],
    *,
    match_line: int,
    def_lines: list[tuple[int, int]],
) -> dict[str, int]:
    """Compute context windows for smart-search findings.

    For matches inside definitions, this reuses macro callsite context behavior.
    For top-level/import matches, it emits a compact prologue block instead of
    spanning to EOF.

    Returns:
    -------
    dict[str, int]
        Inclusive one-based context window with ``start_line`` and ``end_line``.
    """
    total_lines = len(source_lines)
    if total_lines == 0:
        return {"start_line": 1, "end_line": 1}

    first_def_line = def_lines[0][0] if def_lines else total_lines + 1
    if match_line < first_def_line:
        return _compact_top_level_window(
            source_lines,
            match_line=match_line,
            def_lines=def_lines,
        )

    from tools.cq.macros.calls import compute_calls_context_window

    return compute_calls_context_window(match_line, def_lines, total_lines)


def extract_search_context_snippet(
    source_lines: list[str],
    *,
    context_window: dict[str, int],
    match_line: int,
) -> str | None:
    """Render context snippet for smart-search findings.

    Returns:
    -------
    str | None
        Extracted snippet text or ``None`` when rendering is unavailable.
    """
    from tools.cq.macros.calls import extract_calls_context_snippet

    return extract_calls_context_snippet(
        source_lines,
        context_window["start_line"],
        context_window["end_line"],
        match_line=match_line,
    )


__all__ = [
    "compute_search_context_window",
    "extract_search_context_snippet",
]
