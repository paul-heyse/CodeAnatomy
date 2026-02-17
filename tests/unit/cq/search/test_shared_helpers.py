"""Tests for shared CQ search helper utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from tools.cq.search._shared.helpers import (
    line_col_to_byte_offset,
    node_text,
    source_hash,
    truncate,
)

UTF8_BYTE_OFFSET = 8


@dataclass(frozen=True)
class _NodeStub:
    start_byte: int
    end_byte: int


def test_source_hash_is_stable() -> None:
    """Test source hash is stable."""
    payload = b"print('hello')\n"
    assert source_hash(payload) == source_hash(payload)


def test_truncate_appends_ellipsis() -> None:
    """Test truncate appends ellipsis."""
    assert truncate("abcdef", 5) == "ab..."
    assert truncate("abc", 5) == "abc"


def test_line_col_to_byte_offset_handles_utf8() -> None:
    """Test line col to byte offset handles utf8."""
    source = "x = 'héllo'\n".encode()
    # "x = '" is 5 chars/bytes; "h" is 1 byte; "é" is 2 bytes.
    assert line_col_to_byte_offset(source, 1, 7) == UTF8_BYTE_OFFSET


def test_node_text_extracts_slice() -> None:
    """Test node text extracts slice."""
    source = b"abcdef"
    assert node_text(cast("Any", _NodeStub(start_byte=1, end_byte=4)), source) == "bcd"
