"""Code snippet extraction for call contexts.

Handles context window selection and snippet rendering with smart
truncation based on indentation and docstring skipping.
"""

from __future__ import annotations

import re

_MAX_CONTEXT_SNIPPET_LINES = 30
_TRIPLE_QUOTE_RE = re.compile(r"^[rRuUbBfF]*(?P<quote>'''|\"\"\")")


def extract_calls_context_snippet(
    source_lines: list[str],
    start_line: int,
    end_line: int,
    *,
    match_line: int | None = None,
) -> str | None:
    """Public wrapper for callsite context snippet extraction.

    Returns:
        Context snippet when available, otherwise ``None``.
    """
    return _extract_context_snippet(
        source_lines,
        start_line,
        end_line,
        match_line=match_line,
    )


def _extract_context_snippet(
    source_lines: list[str],
    start_line: int,
    end_line: int,
    *,
    match_line: int | None = None,
    max_lines: int = _MAX_CONTEXT_SNIPPET_LINES,
) -> str | None:
    """Extract source snippet for context window with truncation.

    Parameters
    ----------
    source_lines
        All lines of the source file.
    start_line
        1-indexed start line of the context.
    end_line
        1-indexed end line of the context.
    match_line
        1-indexed line of the matched term, used to anchor block selection.
    max_lines
        Maximum lines before truncation.

    Returns:
    -------
    str | None
        The extracted snippet, or None on errors.
    """
    try:
        start_idx = max(0, start_line - 1)
        end_idx = min(len(source_lines) - 1, end_line - 1)
        if start_idx > end_idx:
            return None

        header_indices = _collect_function_header_indices(source_lines, start_idx, end_idx)
        resolved_match_line = match_line if match_line is not None else start_line
        match_idx = min(max(start_idx, resolved_match_line - 1), end_idx)
        anchor_indices = _collect_anchor_block_indices(source_lines, start_idx, end_idx, match_idx)
        selected = _select_context_indices(
            start_idx,
            end_idx,
            header_indices=header_indices,
            anchor_indices=anchor_indices,
            max_lines=max_lines,
        )
        rendered_lines = _render_selected_context_lines(source_lines, selected)
        if not rendered_lines:
            return None
        return "\n".join(rendered_lines)
    except (IndexError, TypeError):
        return None


def _line_indent(line: str) -> int:
    expanded = line.expandtabs(4)
    return len(expanded) - len(expanded.lstrip(" "))


def _is_blank(line: str) -> bool:
    return not line.strip()


def _first_nonblank_index(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
) -> int | None:
    for idx in range(start_idx, end_idx + 1):
        if not _is_blank(source_lines[idx]):
            return idx
    return None


def _skip_docstring_block(
    source_lines: list[str],
    body_idx: int | None,
    end_idx: int,
) -> int | None:
    if body_idx is None:
        return None
    stripped = source_lines[body_idx].lstrip()
    match = _TRIPLE_QUOTE_RE.match(stripped)
    if match is None:
        return body_idx
    quote = match.group("quote")
    tail = stripped[match.end() :]
    if quote in tail:
        return _first_nonblank_index(source_lines, body_idx + 1, end_idx)
    for idx in range(body_idx + 1, end_idx + 1):
        if quote in source_lines[idx]:
            return _first_nonblank_index(source_lines, idx + 1, end_idx)
    return None


def _collect_function_header_indices(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
) -> set[int]:
    if start_idx > end_idx:
        return set()
    indices: set[int] = {start_idx}
    def_indent = _line_indent(source_lines[start_idx])
    first_body_idx = _first_nonblank_index(source_lines, start_idx + 1, end_idx)
    first_body_idx = _skip_docstring_block(source_lines, first_body_idx, end_idx)
    if first_body_idx is None:
        return indices
    body_indent = _line_indent(source_lines[first_body_idx])
    pending_blanks: list[int] = []
    for idx in range(first_body_idx, end_idx + 1):
        line = source_lines[idx]
        if _is_blank(line):
            pending_blanks.append(idx)
            continue
        indent = _line_indent(line)
        if indent <= def_indent:
            break
        if indent == body_indent:
            indices.update(pending_blanks)
            pending_blanks.clear()
            indices.add(idx)
        else:
            pending_blanks.clear()
    return indices


def _resolve_nonblank_match_index(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
    match_idx: int,
) -> int:
    for idx in range(match_idx, start_idx - 1, -1):
        if not _is_blank(source_lines[idx]):
            return idx
    first_nonblank = _first_nonblank_index(source_lines, match_idx, end_idx)
    if first_nonblank is not None:
        return first_nonblank
    return match_idx


def _collect_anchor_block_indices(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
    match_idx: int,
) -> set[int]:
    if start_idx > end_idx:
        return set()
    anchor_idx = _resolve_nonblank_match_index(source_lines, start_idx, end_idx, match_idx)
    anchor_indent = _line_indent(source_lines[anchor_idx])
    block_start = anchor_idx
    block_indent = anchor_indent
    for idx in range(anchor_idx - 1, start_idx - 1, -1):
        line = source_lines[idx]
        if _is_blank(line):
            continue
        indent = _line_indent(line)
        if indent < anchor_indent:
            block_start = idx
            block_indent = indent
            break
    block_end = end_idx
    for idx in range(block_start + 1, end_idx + 1):
        line = source_lines[idx]
        if _is_blank(line):
            continue
        indent = _line_indent(line)
        if indent <= block_indent:
            block_end = idx - 1
            break
    block_end = max(block_end, block_start)
    return set(range(block_start, block_end + 1))


def _select_context_indices(
    start_idx: int,
    end_idx: int,
    *,
    header_indices: set[int],
    anchor_indices: set[int],
    max_lines: int,
) -> list[int]:
    mandatory = set(header_indices)
    mandatory.update(anchor_indices)
    selected: set[int] = {idx for idx in mandatory if start_idx <= idx <= end_idx}
    if len(selected) < max_lines:
        for idx in range(end_idx, start_idx - 1, -1):
            if idx in selected:
                continue
            selected.add(idx)
            if len(selected) >= max_lines:
                break
    return sorted(selected)


def _render_selected_context_lines(source_lines: list[str], selected: list[int]) -> list[str]:
    if not selected:
        return []
    rendered: list[str] = []
    previous: int | None = None
    for idx in selected:
        if previous is not None and idx - previous > 1:
            omitted = idx - previous - 1
            rendered.append(f"    # ... omitted ({omitted} lines) ...")
        rendered.append(source_lines[idx])
        previous = idx
    return rendered


__all__ = [
    "extract_calls_context_snippet",
]
