"""Tests for Python locals scope-index helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.python_lane.locals_index import (
    build_locals_index,
    scope_chain_for_anchor,
)

INNER_SCOPE_START_BYTE = 17
INNER_SCOPE_END_BYTE = 46


@dataclass(frozen=True)
class _FakeNode:
    type: str
    start_byte: int
    end_byte: int
    text: bytes
    name_node: _FakeNode | None = None

    def child_by_field_name(self, name: str) -> _FakeNode | None:
        if name == "name":
            return self.name_node
        return None


def test_build_locals_index_selects_nearest_scope() -> None:
    """Test build locals index selects nearest scope."""
    source = b"def outer():\n    def inner():\n        x = 1\n"
    x_start = source.index(b"x =")
    definition = _FakeNode(type="identifier", start_byte=x_start, end_byte=x_start + 1, text=b"x")
    inner_scope = _FakeNode(
        type="function_definition",
        start_byte=17,
        end_byte=46,
        text=b"def inner():\n        x = 1",
    )
    outer_scope = _FakeNode(
        type="function_definition",
        start_byte=0,
        end_byte=46,
        text=b"def outer():\n    def inner():\n        x = 1",
    )

    rows = build_locals_index(
        definitions=[definition],
        scopes=[outer_scope, inner_scope],
        source_bytes=source,
    )

    assert len(rows) == 1
    assert rows[0].name == "x"
    assert rows[0].scope_start == INNER_SCOPE_START_BYTE
    assert rows[0].scope_end == INNER_SCOPE_END_BYTE


def test_scope_chain_for_anchor_falls_back_to_module() -> None:
    """Test scope chain for anchor falls back to module."""
    source = b"value = 1\n"
    anchor = _FakeNode(type="identifier", start_byte=0, end_byte=5, text=b"value")

    chain = scope_chain_for_anchor(anchor=anchor, scopes=[], source_bytes=source)

    assert chain == ["<module>"]
