"""Tests for tree-sitter runtime contracts wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)


def test_runtime_contract_instantiation() -> None:
    window = QueryWindowV1(start_byte=0, end_byte=10)
    settings = QueryExecutionSettingsV1(match_limit=32)
    assert window.end_byte == 10
    assert settings.match_limit == 32
