"""Tests for test_tree_sitter_runtime_windowing."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryPointWindowV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core import runtime as tree_sitter_runtime

if TYPE_CHECKING:
    from tree_sitter import Node, Query


@dataclass
class _FakeNode:
    start_byte: int
    end_byte: int


_CAPTURED_POINT_RANGES: list[tuple[tuple[int, int], tuple[int, int]]] = []


def _fake_cursor_factory(
    _query: Query,
    match_limit: int = 0,
) -> SimpleNamespace:
    _ = _query
    point_ranges: list[tuple[tuple[int, int], tuple[int, int]]] = []

    def set_max_start_depth(_depth: int) -> None:
        _ = _depth

    def set_byte_range(_start: int, _end: int) -> None:
        _ = (_start, _end)

    def set_point_range(start: tuple[int, int], end: tuple[int, int]) -> None:
        point_ranges.append((start, end))
        _CAPTURED_POINT_RANGES.append((start, end))

    def captures(
        _root: object, progress_callback: object | None = None
    ) -> dict[str, list[_FakeNode]]:
        _ = progress_callback
        return {"cap": [_FakeNode(1, 2)]}

    def matches(
        _root: object,
        progress_callback: object | None = None,
    ) -> list[tuple[int, dict[str, list[_FakeNode]]]]:
        _ = progress_callback
        return [(0, {"cap": [_FakeNode(5, 10)]})]

    return SimpleNamespace(
        match_limit=match_limit,
        did_exceed_match_limit=False,
        point_ranges=point_ranges,
        set_max_start_depth=set_max_start_depth,
        set_byte_range=set_byte_range,
        set_point_range=set_point_range,
        captures=captures,
        matches=matches,
    )


@pytest.fixture(autouse=True)
def _clear_ranges() -> None:
    _CAPTURED_POINT_RANGES.clear()



def test_run_bounded_query_uses_point_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Forward point window boundaries to the cursor during bounded query."""
    monkeypatch.setattr(tree_sitter_runtime, "_TreeSitterQueryCursor", _fake_cursor_factory)

    root = SimpleNamespace(start_byte=0, end_byte=20)
    query = cast("Query", object())
    root_node = cast("Node", root)
    _, telemetry = tree_sitter_runtime.run_bounded_query_captures(
        query=query,
        root=root_node,
        windows=(QueryWindowV1(start_byte=0, end_byte=20),),
        point_windows=(QueryPointWindowV1(start_row=1, start_col=0, end_row=1, end_col=5),),
        settings=QueryExecutionSettingsV1(match_limit=8),
    )

    assert _CAPTURED_POINT_RANGES == [((1, 0), (1, 5))]
    assert telemetry.windows_executed == 1



def test_run_bounded_query_enforces_containment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify containment filtering removes out-of-window matches when enabled."""
    monkeypatch.setattr(tree_sitter_runtime, "_TreeSitterQueryCursor", _fake_cursor_factory)

    root = SimpleNamespace(start_byte=0, end_byte=20)
    query = cast("Query", object())
    root_node = cast("Node", root)
    matches, _ = tree_sitter_runtime.run_bounded_query_matches(
        query=query,
        root=root_node,
        windows=(QueryWindowV1(start_byte=0, end_byte=8),),
        settings=QueryExecutionSettingsV1(match_limit=8, require_containment=True),
    )
    assert matches == []

    matches_unfiltered, _ = tree_sitter_runtime.run_bounded_query_matches(
        query=query,
        root=root_node,
        windows=(QueryWindowV1(start_byte=0, end_byte=8),),
        settings=QueryExecutionSettingsV1(match_limit=8, require_containment=False),
    )
    assert len(matches_unfiltered) == 1
