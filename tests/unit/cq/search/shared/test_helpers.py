"""Tests for shared helper module split from _shared.core."""

from __future__ import annotations

import pytest
from tools.cq.search._shared.helpers import (
    assert_no_runtime_only_keys,
    line_col_to_byte_offset,
    source_hash,
    truncate,
)

LINE2_START = 4
LINE2_FIRST_CHAR = 6


def test_line_col_to_byte_offset_handles_utf8_columns() -> None:
    """Byte offsets should account for UTF-8 character width."""
    source = "a=1\néx\n".encode()
    assert line_col_to_byte_offset(source, 2, 0) == LINE2_START
    assert line_col_to_byte_offset(source, 2, 1) == LINE2_FIRST_CHAR


def test_assert_no_runtime_only_keys_raises_for_runtime_payload() -> None:
    """Runtime-only keys must be rejected for serializable payloads."""
    with pytest.raises(TypeError):
        assert_no_runtime_only_keys({"node": object()})


def test_source_hash_and_truncate_are_stable() -> None:
    """Helper utilities should provide deterministic outputs."""
    text = "abcdefghijklmnopqrstuvwxyz"
    assert source_hash(text.encode("utf-8")) == source_hash(text.encode("utf-8"))
    assert truncate(text, 8) == "abcde..."
