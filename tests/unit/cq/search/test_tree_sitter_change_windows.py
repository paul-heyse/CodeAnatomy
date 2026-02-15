"""Tests for changed-range window derivation helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter_change_windows import (
    contains_window,
    ensure_query_windows,
    windows_from_changed_ranges,
)
from tools.cq.search.tree_sitter_runtime_contracts import QueryWindowV1


@dataclass(frozen=True)
class _Range:
    start_byte: int
    end_byte: int


def test_windows_from_changed_ranges_merges_overlaps_and_applies_padding() -> None:
    windows = windows_from_changed_ranges(
        (_Range(10, 20), _Range(18, 30)),
        source_byte_len=200,
        pad_bytes=5,
    )
    assert len(windows) == 1
    assert windows[0] == QueryWindowV1(start_byte=5, end_byte=35)


def test_ensure_query_windows_uses_fallback_when_empty() -> None:
    fallback = QueryWindowV1(start_byte=2, end_byte=4)
    assert ensure_query_windows((), fallback=fallback) == (fallback,)


def test_contains_window_detects_contained_range() -> None:
    windows = (QueryWindowV1(start_byte=100, end_byte=200),)
    assert contains_window(windows, value=120, width=8) is True
    assert contains_window(windows, value=90, width=5) is False
