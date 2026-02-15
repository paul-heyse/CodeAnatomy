"""Tests for shared CQ search helper utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from tools.cq.search._shared.core import line_col_to_byte_offset, node_text, source_hash, truncate


@dataclass(frozen=True)
class _NodeStub:
    start_byte: int
    end_byte: int


def test_source_hash_is_stable() -> None:
    payload = b"print('hello')\n"
    assert source_hash(payload) == source_hash(payload)


def test_truncate_appends_ellipsis() -> None:
    assert truncate("abcdef", 5) == "ab..."
    assert truncate("abc", 5) == "abc"


def test_line_col_to_byte_offset_handles_utf8() -> None:
    source = "x = 'héllo'\n".encode()
    # "x = '" is 5 chars/bytes; "h" is 1 byte; "é" is 2 bytes.
    assert line_col_to_byte_offset(source, 1, 7) == 8


def test_node_text_extracts_slice() -> None:
    source = b"abcdef"
    assert node_text(cast("Any", _NodeStub(start_byte=1, end_byte=4)), source) == "bcd"
