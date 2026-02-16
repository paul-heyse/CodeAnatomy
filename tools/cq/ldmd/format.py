"""LDMD strict parser with stack validation and byte-offset correctness."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from tools.cq.core.types import LdmdSliceMode


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
logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class _OpenSectionMeta:
    byte_start: int
    title: str
    level: int
    parent: str
    tags: str


def _is_collapsed(section_id: str) -> bool:
    """Determine if section should be collapsed by default.

    Uses lazy import to avoid dependency on R5 section_layout module.
    Falls back to True if the module isn't available yet.

    Returns:
        `True` when section should be collapsed by default.
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

    Returns:
        Index of sections with byte offsets.

    Raises:
        LdmdParseError: If structure is invalid (mismatched nesting, duplicate IDs, or
            unclosed sections).
    """
    open_stack: list[str] = []
    seen_ids: set[str] = set()
    sections: list[SectionMeta] = []
    open_sections: dict[str, _OpenSectionMeta] = {}

    byte_offset = 0
    for raw_line in content.splitlines(keepends=True):
        line_byte_len = len(raw_line)
        # Normalize EOL markers for regex matching while preserving raw byte offsets.
        line = raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace")

        begin_match = _BEGIN_RE.match(line)
        if begin_match:
            sid = begin_match.group(1)
            if sid in seen_ids:
                msg = f"Duplicate section ID: {sid}"
                logger.warning("LDMD parse failure: %s", msg)
                raise LdmdParseError(msg)
            seen_ids.add(sid)
            open_stack.append(sid)
            open_sections[sid] = _OpenSectionMeta(
                byte_start=byte_offset,
                title=begin_match.group(2) or "",
                level=int(begin_match.group(3)) if begin_match.group(3) else 0,
                parent=begin_match.group(4) or "",
                tags=begin_match.group(5) or "",
            )

        end_match = _END_RE.match(line)
        if end_match:
            sid = end_match.group(1)
            if not open_stack or open_stack[-1] != sid:
                expected = open_stack[-1] if open_stack else "none"
                msg = f"Mismatched END: expected '{expected}', got '{sid}'"
                logger.warning("LDMD parse failure: %s", msg)
                raise LdmdParseError(msg)
            open_stack.pop()
            section_meta = open_sections.pop(sid)

            sections.append(
                SectionMeta(
                    id=sid,
                    start_offset=section_meta.byte_start,
                    end_offset=byte_offset + line_byte_len,
                    depth=len(open_stack),
                    collapsed=_is_collapsed(sid),
                )
            )

        byte_offset += line_byte_len

    if open_stack:
        msg = f"Unclosed sections: {open_stack}"
        logger.warning("LDMD parse failure: %s", msg)
        raise LdmdParseError(msg)

    return LdmdIndex(
        sections=sections,
        total_bytes=len(content),
    )


def _resolve_section_id(index: LdmdIndex, section_id: str) -> str:
    if section_id != "root":
        return section_id
    if not index.sections:
        msg = "Document has no sections"
        raise LdmdParseError(msg)
    return min(index.sections, key=lambda section: section.start_offset).id


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
    while limit > 0:
        candidate = data[:limit]
        try:
            candidate.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            limit -= 1
        else:
            return candidate
    return b""


def get_slice(
    content: bytes,
    index: LdmdIndex,
    *,
    section_id: str,
    mode: LdmdSliceMode = LdmdSliceMode.full,
    depth: int = 0,
    limit_bytes: int = 0,
) -> bytes:
    """Extract content from a section by ID.

    Returns:
        Section content as bytes.

    Raises:
        LdmdParseError: If `section_id` is not found or mode is unsupported.
    """
    mode_value = str(mode)
    if mode_value not in {"full", "preview", "tldr"}:
        msg = f"Unsupported mode: {mode_value}"
        logger.warning("LDMD slice failure: %s", msg)
        raise LdmdParseError(msg)

    resolved_id = _resolve_section_id(index, section_id)
    section = next((s for s in index.sections if s.id == resolved_id), None)
    if section is None:
        msg = f"Section not found: {resolved_id}"
        logger.warning("LDMD slice failure: %s", msg)
        raise LdmdParseError(msg)

    slice_data = content[section.start_offset : section.end_offset]

    if mode_value == "tldr":
        tldr_id = f"{resolved_id}_tldr"
        try:
            local_index = build_index(slice_data)
            tldr_section = next((s for s in local_index.sections if s.id == tldr_id), None)
            if tldr_section is not None:
                slice_data = slice_data[tldr_section.start_offset : tldr_section.end_offset]
                depth = 0
            else:
                mode_value = "preview"
        except LdmdParseError:
            logger.warning(
                "LDMD TLDR parse failed for section '%s'; falling back to preview", resolved_id
            )
            mode_value = "preview"

    if mode_value == "preview":
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

    Returns:
        Filtered LDMD bytes that respect the requested maximum depth.
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

    Returns:
        Navigation info with previous/next section IDs.

    Raises:
        LdmdParseError: If `section_id` is not found.
    """
    resolved_id = _resolve_section_id(index, section_id)
    # Find section index
    section_idx = None
    for idx, s in enumerate(index.sections):
        if s.id == resolved_id:
            section_idx = idx
            break

    if section_idx is None:
        msg = f"Section not found: {resolved_id}"
        raise LdmdParseError(msg)

    prev_id = None
    next_id = None

    if section_idx > 0:
        prev_id = index.sections[section_idx - 1].id

    if section_idx < len(index.sections) - 1:
        next_id = index.sections[section_idx + 1].id

    return {
        "section_id": resolved_id,
        "prev": prev_id,
        "next": next_id,
    }
