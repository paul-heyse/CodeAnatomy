"""Tests for tree-sitter structural contracts wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import (
    TreeSitterStructuralExportV1,
    TreeSitterStructuralNodeV1,
)


def test_structural_contract_defaults() -> None:
    """Test structural contract defaults."""
    export = TreeSitterStructuralExportV1()
    assert export.nodes == []
    assert export.edges == []


def test_structural_node_classification_defaults() -> None:
    """Test structural node classification defaults."""
    row = TreeSitterStructuralNodeV1(
        node_id="sample.py:0:1:identifier",
        kind="identifier",
        start_byte=0,
        end_byte=1,
        start_line=1,
        start_col=0,
        end_line=1,
        end_col=1,
    )
    assert row.is_visible is None
    assert row.is_supertype is None
