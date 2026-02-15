"""Tests for tree-sitter cursor window application helpers."""

from __future__ import annotations

from types import SimpleNamespace

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryPointWindowV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core.windowing import apply_byte_window, apply_point_window


def test_apply_byte_window_prefers_containing_range_when_supported() -> None:
    calls: list[tuple[str, int, int]] = []
    cursor = SimpleNamespace(
        set_containing_byte_range=lambda start, end: calls.append(("containing", start, end)),
        set_byte_range=lambda start, end: calls.append(("intersection", start, end)),
    )
    applied = apply_byte_window(
        cursor=cursor,
        window=QueryWindowV1(start_byte=1, end_byte=5),
        mode="containment_preferred",
    )
    assert applied is True
    assert calls == [("containing", 1, 5)]


def test_apply_byte_window_required_without_support_returns_false() -> None:
    calls: list[tuple[int, int]] = []
    cursor = SimpleNamespace(
        set_byte_range=lambda start, end: calls.append((start, end)),
    )
    applied = apply_byte_window(
        cursor=cursor,
        window=QueryWindowV1(start_byte=2, end_byte=8),
        mode="containment_required",
    )
    assert applied is False
    assert calls == []


def test_apply_point_window_falls_back_to_intersection_when_preferred() -> None:
    calls: list[tuple[tuple[int, int], tuple[int, int]]] = []
    cursor = SimpleNamespace(
        set_point_range=lambda start, end: calls.append((start, end)),
    )
    applied = apply_point_window(
        cursor=cursor,
        window=QueryPointWindowV1(start_row=1, start_col=0, end_row=1, end_col=9),
        mode="containment_preferred",
    )
    assert applied is True
    assert calls == [((1, 0), (1, 9))]
