"""Tests for tree-sitter runtime capability snapshots."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tools.cq.search.tree_sitter.core import language_registry as registry


def test_load_tree_sitter_capabilities_defaults_when_language_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load tree sitter capabilities defaults when language unavailable."""
    registry.load_tree_sitter_capabilities.cache_clear()
    monkeypatch.setattr(registry, "load_tree_sitter_language", lambda _language: None)
    caps = registry.load_tree_sitter_capabilities("python")
    assert caps.language == "python"
    assert caps.has_cursor_copy is False
    assert caps.has_cursor_reset is False
    assert caps.has_cursor_reset_to is False
    assert caps.has_goto_first_child_for_byte is False
    assert caps.has_query_cursor_containing_byte_range is False
    assert caps.has_query_cursor_containing_point_range is False


def test_load_tree_sitter_capabilities_detects_runtime_methods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detect parser and cursor feature availability from runtime classes."""
    class _Cursor:
        def copy(self) -> _Cursor:
            return self

        def reset(self, _node: object) -> None:
            return None

        def reset_to(self, _cursor: object) -> None:
            return None

        def goto_first_child_for_byte(self, _byte_offset: int) -> int | None:
            return 0

    class _Root:
        @staticmethod
        def walk() -> _Cursor:
            return _Cursor()

    class _Tree:
        root_node = _Root()

    class _Parser:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        @staticmethod
        def parse(_source: bytes) -> _Tree:
            return _Tree()

    class _QueryCursor:
        @staticmethod
        def set_containing_byte_range(_start: int, _end: int) -> None:
            return None

        @staticmethod
        def set_containing_point_range(
            _start: tuple[int, int],
            _end: tuple[int, int],
        ) -> None:
            return None

    registry.load_tree_sitter_capabilities.cache_clear()
    monkeypatch.setattr(
        registry,
        "load_tree_sitter_language",
        lambda _language: SimpleNamespace(name="python"),
    )
    monkeypatch.setattr(registry, "_TreeSitterParser", _Parser)
    monkeypatch.setattr(registry, "_TreeSitterQueryCursor", _QueryCursor)
    caps = registry.load_tree_sitter_capabilities("python")
    assert caps.has_cursor_copy is True
    assert caps.has_cursor_reset is True
    assert caps.has_cursor_reset_to is True
    assert caps.has_goto_first_child_for_byte is True
    assert caps.has_query_cursor_containing_byte_range is True
    assert caps.has_query_cursor_containing_point_range is True
