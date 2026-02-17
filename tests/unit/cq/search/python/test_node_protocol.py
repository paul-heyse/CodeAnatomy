"""Tests for Python node access protocol."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.python.node_protocol import PythonNodeAccess


@dataclass(frozen=True)
class _Pos:
    line: int
    column: int
    byte: int


@dataclass(frozen=True)
class _Range:
    start: _Pos
    end: _Pos


class _Node:
    def __init__(self, *, kind: str, text: str = "") -> None:
        self._kind = kind
        self._text = text

    def kind(self) -> str:
        return self._kind

    def text(self) -> str:
        return self._text

    def field(self, name: str) -> _Node | None:
        _ = (self, name)
        return None

    def parent(self) -> _Node | None:
        _ = self
        return None

    def children(self) -> list[_Node]:
        _ = self
        return []

    def is_named(self) -> bool:
        _ = self
        return True

    def range(self) -> _Range:
        _ = self
        return _Range(start=_Pos(1, 0, 0), end=_Pos(1, 1, 1))


def _uses_protocol(node: PythonNodeAccess) -> tuple[str, int]:
    span = node.range()
    return node.kind(), span.start.line


def test_node_stub_satisfies_protocol_contract() -> None:
    """Verify node stub satisfies PythonNodeAccess protocol expectations."""
    kind, start_line = _uses_protocol(_Node(kind="function_definition", text="def x(): ..."))
    assert kind == "function_definition"
    assert start_line == 1
