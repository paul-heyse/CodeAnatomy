# ruff: noqa: DOC201,DOC501,TRY003,EM102,PGH003,TRY300
"""LDMD strict parser with stack validation and byte-offset correctness."""

from __future__ import annotations

import re
from dataclasses import dataclass


class LdmdParseError(Exception):
    """Raised on invalid LDMD structure."""


_BEGIN_RE = re.compile(
    r'^<!--LDMD:BEGIN\s+id="([^"]+)"'
    r'(?:\s+title="([^"]*)")?'
    r'(?:\s+level="(\d+)")?'
    r'(?:\s+parent="([^"]*)")?'
    r'(?:\s+tags="([^"]*)")?'
    r"\s*-->$"
)
_END_RE = re.compile(r'^<!--LDMD:END\s+id="([^"]+)"\s*-->$')


@dataclass(frozen=True)
class SectionMeta:
    """Metadata for a single LDMD section."""

    id: str
    start_offset: int
    end_offset: int
    depth: int
    collapsed: bool


@dataclass(frozen=True)
class LdmdIndex:
    """Index of LDMD sections with byte offsets."""

    sections: list[SectionMeta]
    total_bytes: int


def _is_collapsed(section_id: str) -> bool:
    """Determine if section should be collapsed by default.

    Uses lazy import to avoid dependency on R5 section_layout module.
    Falls back to True if the module isn't available yet.
    """
    try:
        from tools.cq.neighborhood.section_layout import (
            _DYNAMIC_COLLAPSE_SECTIONS,
            _UNCOLLAPSED_SECTIONS,
        )

        if section_id in _UNCOLLAPSED_SECTIONS:
            return False
        if section_id in _DYNAMIC_COLLAPSE_SECTIONS:
            return True
    except ImportError:
        pass
    return True


def build_index(content: bytes) -> LdmdIndex:
    """Build section index in single forward pass over raw bytes.

    Validates:
    - BEGIN/END nesting via stack
    - Duplicate section IDs
    - Byte offsets from raw content (not decoded text)

    Uses attribute-based marker grammar:
    - BEGIN: <!--LDMD:BEGIN id="..." title="..." level="..." parent="..." tags="..."-->
    - END: <!--LDMD:END id="..."-->

    Parameters
    ----------
    content
        Raw LDMD document as bytes.

    Returns:
    -------
    LdmdIndex
        Index of sections with byte offsets.

    Raises:
    ------
    LdmdParseError
        If structure is invalid (mismatched nesting, duplicate IDs, unclosed sections).
    """
    open_stack: list[str] = []
    seen_ids: set[str] = set()
    sections: list[SectionMeta] = []
    open_sections: dict[str, dict[str, object]] = {}

    byte_offset = 0
    for raw_line in content.splitlines(keepends=True):
        line_byte_len = len(raw_line)
        # Normalize EOL markers for regex matching while preserving raw byte offsets.
        line = raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace")

        begin_match = _BEGIN_RE.match(line)
        if begin_match:
            sid = begin_match.group(1)
            if sid in seen_ids:
                raise LdmdParseError(f"Duplicate section ID: {sid}")
            seen_ids.add(sid)
            open_stack.append(sid)
            open_sections[sid] = {
                "byte_start": byte_offset,
                "title": begin_match.group(2) or "",
                "level": int(begin_match.group(3)) if begin_match.group(3) else 0,
                "parent": begin_match.group(4) or "",
                "tags": begin_match.group(5) or "",
            }

        end_match = _END_RE.match(line)
        if end_match:
            sid = end_match.group(1)
            if not open_stack or open_stack[-1] != sid:
                expected = open_stack[-1] if open_stack else "none"
                raise LdmdParseError(f"Mismatched END: expected '{expected}', got '{sid}'")
            open_stack.pop()
            section_meta = open_sections.pop(sid)

            sections.append(
                SectionMeta(
                    id=sid,
                    start_offset=section_meta["byte_start"],  # type: ignore
                    end_offset=byte_offset + line_byte_len,
                    depth=len(open_stack),
                    collapsed=_is_collapsed(sid),
                )
            )

        byte_offset += line_byte_len

    if open_stack:
        raise LdmdParseError(f"Unclosed sections: {open_stack}")

    return LdmdIndex(
        sections=sections,
        total_bytes=len(content),
    )


def _safe_utf8_truncate(data: bytes, limit: int) -> bytes:
    """Truncate bytes at limit, preserving UTF-8 boundaries.

    Uses strict decode validation after boundary adjustment.

    Parameters
    ----------
    data
        Bytes to truncate.
    limit
        Maximum byte length.

    Returns:
    -------
    bytes
        Truncated data with valid UTF-8 boundaries.
    """
    if len(data) <= limit:
        return data

    # Back up from limit to find valid boundary
    candidate = data[:limit]
    while limit > 0:
        try:
            candidate.decode("utf-8", errors="strict")
            return candidate
        except UnicodeDecodeError:
            limit -= 1
            candidate = data[:limit]
    return b""


def get_slice(
    content: bytes,
    index: LdmdIndex,
    *,
    section_id: str,
    mode: str = "full",
    depth: int = 0,
    limit_bytes: int = 0,
) -> bytes:
    """Extract content from a section by ID.

    Parameters
    ----------
    content
        Full LDMD document bytes.
    index
        Pre-built section index.
    section_id
        Target section ID.
    mode
        Extraction mode: "full", "preview", or "tldr".
    depth
        Include nested sections up to this depth (0 = no nesting).
    limit_bytes
        Max bytes to return (0 = unlimited, uses safe UTF-8 truncation).

    Returns:
    -------
    bytes
        Section content as bytes.

    Raises:
    ------
    LdmdParseError
        If section_id is not found.
    """
    if mode not in {"full", "preview", "tldr"}:
        raise LdmdParseError(f"Unsupported mode: {mode}")

    section = next((s for s in index.sections if s.id == section_id), None)
    if section is None:
        raise LdmdParseError(f"Section not found: {section_id}")

    slice_data = content[section.start_offset : section.end_offset]

    if mode == "tldr":
        tldr_id = f"{section_id}_tldr"
        try:
            local_index = build_index(slice_data)
            tldr_section = next((s for s in local_index.sections if s.id == tldr_id), None)
            if tldr_section is not None:
                slice_data = slice_data[tldr_section.start_offset : tldr_section.end_offset]
                depth = 0
            else:
                mode = "preview"
        except LdmdParseError:
            mode = "preview"

    if mode == "preview":
        depth = max(0, min(depth if depth > 0 else 1, 1))
        if limit_bytes <= 0:
            limit_bytes = 4096

    slice_data = _apply_depth_filter(slice_data, max_depth=max(0, depth))

    if limit_bytes > 0:
        slice_data = _safe_utf8_truncate(slice_data, limit_bytes)

    return slice_data


def _apply_depth_filter(content: bytes, *, max_depth: int) -> bytes:
    """Filter LDMD content to include markers/content up to max nested depth.

    Depth is relative to the first section marker in the provided slice:
    - depth 0 keeps only the target section body
    - depth 1 includes direct child sections
    """
    if max_depth < 0:
        return b""

    lines = content.splitlines(keepends=True)
    stack: list[str] = []
    kept: list[bytes] = []

    for raw_line in lines:
        line = raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace")
        begin_match = _BEGIN_RE.match(line)
        if begin_match:
            relative_depth = max(0, len(stack) - 1)
            if relative_depth <= max_depth:
                kept.append(raw_line)
            stack.append(begin_match.group(1))
            continue

        end_match = _END_RE.match(line)
        if end_match:
            relative_depth = max(0, len(stack) - 1)
            if relative_depth <= max_depth:
                kept.append(raw_line)
            if stack and stack[-1] == end_match.group(1):
                stack.pop()
            continue

        relative_depth = max(0, len(stack) - 1)
        if relative_depth <= max_depth:
            kept.append(raw_line)

    return b"".join(kept)


def search_sections(
    content: bytes,
    index: LdmdIndex,
    *,
    query: str,
) -> list[dict[str, object]]:
    """Search within LDMD sections for text matching query.

    Parameters
    ----------
    content
        Full LDMD document bytes.
    index
        Pre-built section index.
    query
        Search query string (simple text search).

    Returns:
    -------
    list[dict]
        List of matches with section_id and match context.
    """
    matches: list[dict[str, object]] = []
    query_lower = query.lower()

    for section in index.sections:
        section_content = content[section.start_offset : section.end_offset]
        section_text = section_content.decode("utf-8", errors="replace")

        if query_lower in section_text.lower():
            # Find all occurrences in this section
            lines = section_text.split("\n")
            for line_idx, line in enumerate(lines):
                if query_lower in line.lower():
                    matches.append(
                        {
                            "section_id": section.id,
                            "line": line_idx,
                            "text": line.strip(),
                        }
                    )

    return matches


def get_neighbors(
    index: LdmdIndex,
    *,
    section_id: str,
) -> dict[str, str | None]:
    """Get neighboring sections for navigation.

    Parameters
    ----------
    index
        Pre-built section index.
    section_id
        Target section ID.

    Returns:
    -------
    dict
        Navigation info with prev/next section IDs.

    Raises:
    ------
    LdmdParseError
        If section_id is not found.
    """
    # Find section index
    section_idx = None
    for idx, s in enumerate(index.sections):
        if s.id == section_id:
            section_idx = idx
            break

    if section_idx is None:
        raise LdmdParseError(f"Section not found: {section_id}")

    prev_id = None
    next_id = None

    if section_idx > 0:
        prev_id = index.sections[section_idx - 1].id

    if section_idx < len(index.sections) - 1:
        next_id = index.sections[section_idx + 1].id

    return {
        "section_id": section_id,
        "prev": prev_id,
        "next": next_id,
    }
