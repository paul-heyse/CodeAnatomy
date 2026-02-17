"""Search-specific context windowing helpers."""

from __future__ import annotations

import msgspec

_TOP_LEVEL_PROLOGUE_LINE_COUNT = 3


class ContextWindow(msgspec.Struct, frozen=True):
    """Immutable inclusive line window."""

    start_line: int
    end_line: int

    def __post_init__(self) -> None:
        """Validate line-window invariants.

        Raises:
            ValueError: If window bounds are invalid.
        """
        if self.start_line < 1:
            msg = f"start_line must be >= 1 (got {self.start_line})"
            raise ValueError(msg)
        if self.end_line < self.start_line:
            msg = (
                f"Invalid context window: end_line {self.end_line} < "
                f"start_line {self.start_line}"
            )
            raise ValueError(msg)

    @classmethod
    def around(
        cls,
        *,
        center_line: int,
        radius: int = 5,
        total_lines: int | None = None,
    ) -> ContextWindow:
        """Create a window around a center line.

        Returns:
            ContextWindow: Bounded window centered on the requested line.
        """
        bounded_center = max(1, center_line)
        start = max(1, bounded_center - max(0, radius))
        end = bounded_center + max(0, radius)
        if isinstance(total_lines, int):
            capped_total = max(1, total_lines)
            end = min(capped_total, end)
            start = min(start, capped_total)
        return cls(start_line=start, end_line=end)

    def line_count(self) -> int:
        """Return inclusive line count for this window."""
        return self.end_line - self.start_line + 1


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
) -> ContextWindow:
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

    return ContextWindow(start_line=start_idx + 1, end_line=end_idx + 1)


def compute_search_context_window(
    source_lines: list[str],
    *,
    match_line: int,
    def_lines: list[tuple[int, int]],
) -> ContextWindow:
    """Compute context windows for smart-search findings.

    For matches inside definitions, this reuses macro callsite context behavior.
    For top-level/import matches, it emits a compact prologue block instead of
    spanning to EOF.

    Returns:
    -------
    ContextWindow
        Inclusive one-based context window.
    """
    total_lines = len(source_lines)
    if total_lines == 0:
        return ContextWindow(start_line=1, end_line=1)

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
    context_window: ContextWindow,
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
        context_window.start_line,
        context_window.end_line,
        match_line=match_line,
    )


__all__ = [
    "ContextWindow",
    "compute_search_context_window",
    "extract_search_context_snippet",
]
