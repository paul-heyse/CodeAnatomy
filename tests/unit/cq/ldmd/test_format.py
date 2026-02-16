"""Tests for LDMD strict parser."""

from __future__ import annotations

import pytest
from tools.cq.core.types import LdmdSliceMode
from tools.cq.ldmd.format import (
    LdmdParseError,
    _safe_utf8_truncate,
    build_index,
    get_neighbors,
    get_slice,
    search_sections,
)

SECTION_COUNT_TWO = 2
CAFE_UTF8_BYTES = 5
SNOWMAN_UTF8_BYTES = 5
EMOJI_UTF8_BYTES = 6
SLICE_LIMIT_BYTES = 50


def test_valid_ldmd_parses_correctly() -> None:
    """Test that a valid LDMD document parses correctly."""
    content = b"""<!--LDMD:BEGIN id="section1" title="First Section" level="1"-->
# First Section
Some content here.
<!--LDMD:END id="section1"-->

<!--LDMD:BEGIN id="section2" title="Second Section" level="1"-->
# Second Section
More content here.
<!--LDMD:END id="section2"-->
"""
    index = build_index(content)
    assert len(index.sections) == SECTION_COUNT_TWO
    assert index.sections[0].id == "section1"
    assert index.sections[1].id == "section2"
    assert index.total_bytes == len(content)


def test_mismatched_begin_end_raises_error() -> None:
    """Test that mismatched BEGIN/END markers raise LdmdParseError."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
Content
<!--LDMD:END id="section2"-->
"""
    with pytest.raises(LdmdParseError, match="Mismatched END"):
        build_index(content)


def test_duplicate_ids_raise_error() -> None:
    """Test that duplicate section IDs raise LdmdParseError."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
Content 1
<!--LDMD:END id="section1"-->

<!--LDMD:BEGIN id="section1"-->
Content 2
<!--LDMD:END id="section1"-->
"""
    with pytest.raises(LdmdParseError, match="Duplicate section ID"):
        build_index(content)


def test_unclosed_sections_raise_error() -> None:
    """Test that unclosed sections raise LdmdParseError."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
Content
"""
    with pytest.raises(LdmdParseError, match="Unclosed sections"):
        build_index(content)


def test_safe_utf8_truncate_preserves_2byte_chars() -> None:
    """Test that _safe_utf8_truncate preserves 2-byte UTF-8 characters."""
    # U+00E9 (é) is 2 bytes in UTF-8: 0xC3 0xA9
    data = "café".encode()  # b'caf\xc3\xa9'
    assert len(data) == CAFE_UTF8_BYTES

    # Truncating at 4 would split the é character
    result = _safe_utf8_truncate(data, 4)
    # Should truncate to 3 bytes to avoid splitting
    assert result == b"caf"
    assert result.decode("utf-8") == "caf"


def test_safe_utf8_truncate_preserves_3byte_chars() -> None:
    """Test that _safe_utf8_truncate preserves 3-byte UTF-8 characters."""
    # U+2603 (☃) is 3 bytes in UTF-8: 0xE2 0x98 0x83
    data = "hi☃".encode()  # b'hi\xe2\x98\x83'
    assert len(data) == SNOWMAN_UTF8_BYTES

    # Truncating at 4 would split the snowman
    result = _safe_utf8_truncate(data, 4)
    assert result == b"hi"
    assert result.decode("utf-8") == "hi"


def test_safe_utf8_truncate_preserves_4byte_chars() -> None:
    """Test that _safe_utf8_truncate preserves 4-byte UTF-8 characters."""
    # U+1F600 (😀) is 4 bytes in UTF-8: 0xF0 0x9F 0x98 0x80
    data = "hi😀".encode()  # b'hi\xf0\x9f\x98\x80'
    assert len(data) == EMOJI_UTF8_BYTES

    # Truncating at 4 would split the emoji
    result = _safe_utf8_truncate(data, 4)
    assert result == b"hi"
    assert result.decode("utf-8") == "hi"


def test_byte_offsets_correct_for_ascii() -> None:
    """Test that byte offsets are correct for ASCII content."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
Line 1
Line 2
<!--LDMD:END id="section1"-->
"""
    index = build_index(content)
    assert len(index.sections) == 1
    section = index.sections[0]

    # Extract the section and verify it's complete
    extracted = content[section.start_offset : section.end_offset]
    assert extracted.startswith(b"<!--LDMD:BEGIN")
    assert extracted.endswith(b'id="section1"-->\n')


def test_byte_offsets_correct_for_crlf() -> None:
    """Test that byte offsets are correct for CRLF line endings."""
    content = b'<!--LDMD:BEGIN id="section1"-->\r\nLine 1\r\n<!--LDMD:END id="section1"-->\r\n'
    index = build_index(content)
    assert len(index.sections) == 1
    section = index.sections[0]

    # Extract and verify
    extracted = content[section.start_offset : section.end_offset]
    assert extracted.startswith(b"<!--LDMD:BEGIN")
    assert extracted.endswith(b"-->\r\n")


def test_get_slice_returns_correct_content() -> None:
    """Test that get_slice returns correct section content."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
# Section 1
Content here.
<!--LDMD:END id="section1"-->

<!--LDMD:BEGIN id="section2"-->
# Section 2
More content.
<!--LDMD:END id="section2"-->
"""
    index = build_index(content)

    slice_data = get_slice(content, index, section_id="section1")
    assert b"Section 1" in slice_data
    assert b"Section 2" not in slice_data


def test_get_slice_with_limit_bytes() -> None:
    """Test that get_slice with limit_bytes truncates safely."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
This is a long section with lots of content that should be truncated.
<!--LDMD:END id="section1"-->
"""
    index = build_index(content)

    slice_data = get_slice(content, index, section_id="section1", limit_bytes=50)
    assert len(slice_data) <= SLICE_LIMIT_BYTES
    # Should be valid UTF-8
    slice_data.decode("utf-8")


def test_get_slice_preview_mode_uses_depth_filter() -> None:
    """Test get slice preview mode uses depth filter."""
    content = b"""<!--LDMD:BEGIN id=\"root\"-->
Root line
<!--LDMD:BEGIN id=\"child\" parent=\"root\"-->
Child line
<!--LDMD:BEGIN id=\"grandchild\" parent=\"child\"-->
Grandchild line
<!--LDMD:END id=\"grandchild\"-->
<!--LDMD:END id=\"child\"-->
<!--LDMD:END id=\"root\"-->
"""
    index = build_index(content)
    preview = get_slice(content, index, section_id="root", mode=LdmdSliceMode.preview, depth=0)
    assert b"Root line" in preview
    assert b"Child line" in preview
    assert b"Grandchild line" not in preview


def test_get_slice_tldr_mode_prefers_tldr_child() -> None:
    """Test get slice tldr mode prefers tldr child."""
    content = b"""<!--LDMD:BEGIN id=\"section\"-->
<!--LDMD:BEGIN id=\"section_tldr\" parent=\"section\"-->
TLDR text
<!--LDMD:END id=\"section_tldr\"-->
<!--LDMD:BEGIN id=\"section_body\" parent=\"section\"-->
Body text
<!--LDMD:END id=\"section_body\"-->
<!--LDMD:END id=\"section\"-->
"""
    index = build_index(content)
    tldr = get_slice(content, index, section_id="section", mode=LdmdSliceMode.tldr)
    assert b"TLDR text" in tldr
    assert b"Body text" not in tldr


def test_search_sections_finds_text() -> None:
    """Test that search_sections finds text within sections."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
This section contains the word apple.
<!--LDMD:END id="section1"-->

<!--LDMD:BEGIN id="section2"-->
This section contains the word banana.
<!--LDMD:END id="section2"-->
"""
    index = build_index(content)

    matches = search_sections(content, index, query="apple")
    assert len(matches) > 0
    assert any(m["section_id"] == "section1" for m in matches)

    matches = search_sections(content, index, query="banana")
    assert len(matches) > 0
    assert any(m["section_id"] == "section2" for m in matches)


def test_get_neighbors_returns_prev_next() -> None:
    """Test that get_neighbors returns prev/next section IDs."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
First
<!--LDMD:END id="section1"-->

<!--LDMD:BEGIN id="section2"-->
Second
<!--LDMD:END id="section2"-->

<!--LDMD:BEGIN id="section3"-->
Third
<!--LDMD:END id="section3"-->
"""
    index = build_index(content)

    # Middle section should have both prev and next
    nav = get_neighbors(index, section_id="section2")
    assert nav["prev"] == "section1"
    assert nav["next"] == "section3"

    # First section should have no prev
    nav = get_neighbors(index, section_id="section1")
    assert nav["prev"] is None
    assert nav["next"] == "section2"

    # Last section should have no next
    nav = get_neighbors(index, section_id="section3")
    assert nav["prev"] == "section2"
    assert nav["next"] is None


def test_nested_sections_parse_correctly() -> None:
    """Test that nested sections parse with correct depth."""
    content = b"""<!--LDMD:BEGIN id="outer"-->
Outer content
<!--LDMD:BEGIN id="inner"-->
Inner content
<!--LDMD:END id="inner"-->
More outer content
<!--LDMD:END id="outer"-->
"""
    index = build_index(content)
    assert len(index.sections) == SECTION_COUNT_TWO

    # Find the sections
    outer = next(s for s in index.sections if s.id == "outer")
    inner = next(s for s in index.sections if s.id == "inner")

    # Inner should have greater depth
    assert inner.depth > outer.depth


def test_get_slice_raises_on_missing_section() -> None:
    """Test that get_slice raises LdmdParseError for missing section."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
Content
<!--LDMD:END id="section1"-->
"""
    index = build_index(content)

    with pytest.raises(LdmdParseError, match="Section not found"):
        get_slice(content, index, section_id="nonexistent")


def test_get_neighbors_raises_on_missing_section() -> None:
    """Test that get_neighbors raises LdmdParseError for missing section."""
    content = b"""<!--LDMD:BEGIN id="section1"-->
Content
<!--LDMD:END id="section1"-->
"""
    index = build_index(content)

    with pytest.raises(LdmdParseError, match="Section not found"):
        get_neighbors(index, section_id="nonexistent")
