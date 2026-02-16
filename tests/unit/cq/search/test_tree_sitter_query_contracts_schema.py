"""Tests for tree-sitter query contracts.yaml schema parsing."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.query_models import load_pack_rules


def test_load_pack_rules_reads_required_metadata_contracts() -> None:
    """Test load pack rules reads required metadata contracts."""
    rust_rules = load_pack_rules("rust")
    python_rules = load_pack_rules("python")

    assert rust_rules.required_metadata_keys == ("cq.emit", "cq.kind", "cq.anchor")
    assert python_rules.required_metadata_keys == ("cq.emit", "cq.kind", "cq.anchor")
    assert "injection.combined" in rust_rules.forbidden_capture_names
    assert "injection.combined" in python_rules.forbidden_capture_names
