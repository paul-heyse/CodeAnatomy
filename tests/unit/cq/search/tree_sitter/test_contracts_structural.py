"""Tests for tree-sitter structural contracts wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import TreeSitterStructuralExportV1


def test_structural_contract_defaults() -> None:
    export = TreeSitterStructuralExportV1()
    assert export.nodes == []
    assert export.edges == []
