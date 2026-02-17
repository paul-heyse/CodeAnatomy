"""Unit tests for Python lane capture-processing helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.python_lane.capture_processing import (
    group_captures_by_scope,
    process_captures,
)


class _Node:
    def __init__(self, kind: str, parent: object | None = None) -> None:
        self.type = kind
        self.parent = parent
        self.start_byte = 1
        self.end_byte = 3


def test_process_captures_emits_typed_rows() -> None:
    """Capture processing should emit typed rows with original capture names."""
    node = _Node("identifier", _Node("function_definition"))
    rows = process_captures([("name", node)], source_bytes=b"")

    assert len(rows) == 1
    assert rows[0].capture_name == "name"


def test_group_captures_by_scope_buckets_rows() -> None:
    """Grouped capture rows should be keyed by enclosing scope kind."""
    node = _Node("identifier", _Node("function_definition"))
    grouped = group_captures_by_scope([("name", node)], source_bytes=b"")

    assert "function_definition" in grouped
