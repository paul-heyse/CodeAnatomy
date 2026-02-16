"""Tests for cursor pooling behavior through structural export APIs."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.structural import export as export_module


@dataclass
class _Node:
    type: str
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]
    tree: object | None = None

    def walk(self) -> Any:
        child = _Node("identifier", 4, 8, (0, 4), (0, 8), tree=self.tree)
        return _Cursor(nodes=[self, child])


class _Cursor:
    copy_calls = 0
    reset_to_calls = 0
    reset_calls = 0

    def __init__(self, *, nodes: list[_Node], index: int = 0) -> None:
        self._nodes = nodes
        self._index = index

    @property
    def node(self) -> _Node | None:
        if 0 <= self._index < len(self._nodes):
            return self._nodes[self._index]
        return None

    @property
    def field_name(self) -> str | None:
        return "name" if self._index == 1 else None

    def copy(self) -> _Cursor:
        _Cursor.copy_calls += 1
        return _Cursor(nodes=self._nodes, index=self._index)

    def reset_to(self, other: _Cursor) -> None:
        _Cursor.reset_to_calls += 1
        self._index = other._index

    def reset(self, node: object) -> None:
        _Cursor.reset_calls += 1
        for idx, candidate in enumerate(self._nodes):
            if candidate is node:
                self._index = idx
                return
        self._index = -1

    def goto_first_child_for_byte(self, _target_byte: int) -> int | None:
        if self._index == 0:
            self._index = 1
            return 0
        return None

    def goto_first_child(self) -> bool:
        return False

    def goto_next_sibling(self) -> bool:
        return False

    def goto_parent(self) -> bool:
        if self._index == 1:
            self._index = 0
            return True
        return False


def test_export_structural_rows_uses_cursor_copy_and_resets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test export structural rows uses cursor copy and resets."""
    _Cursor.copy_calls = 0
    _Cursor.reset_to_calls = 0
    _Cursor.reset_calls = 0
    tree = SimpleNamespace(language=SimpleNamespace(name="python"))
    root = _Node("module", 0, 8, (0, 0), (0, 8), tree=tree)
    monkeypatch.setattr(export_module, "export_cst_tokens", lambda **_kwargs: [])
    monkeypatch.setattr(export_module, "_classification_map", lambda _language: {})
    exported = export_module.export_structural_rows(
        file_path="sample.py",
        root=cast("Any", root),
        source_bytes=b"x = y\n",
    )
    assert exported.nodes
    assert _Cursor.copy_calls >= 1
    assert _Cursor.reset_calls >= 1
    assert _Cursor.reset_to_calls >= 1
