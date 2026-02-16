"""Tests for Python ast-grep rule wrappers."""

from __future__ import annotations

from typing import Any, cast

from tools.cq.search.python.pipeline_support import find_import_aliases


class _Node:
    def __init__(self) -> None:
        self.last_query: object = None

    def find_all(self, query: object) -> list[str]:
        self.last_query = query
        return ["match"]


def test_find_import_aliases_uses_config_rules() -> None:
    """Test find import aliases uses config rules."""
    node = _Node()
    rows = find_import_aliases(cast("Any", node))
    assert rows == ["match"]
    assert isinstance(node.last_query, dict)
