"""Tests for tree-sitter runtime contracts wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)

WINDOW_END_BYTE = 10
DEFAULT_MATCH_LIMIT = 32


def test_runtime_contract_instantiation() -> None:
    """Test runtime contract instantiation."""
    window = QueryWindowV1(start_byte=0, end_byte=WINDOW_END_BYTE)
    settings = QueryExecutionSettingsV1(match_limit=DEFAULT_MATCH_LIMIT)
    assert window.end_byte == WINDOW_END_BYTE
    assert settings.match_limit == DEFAULT_MATCH_LIMIT
