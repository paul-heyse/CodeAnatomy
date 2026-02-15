"""Tests for canonical tree-sitter lane payload contracts."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.lane_payloads import (
    canonicalize_python_lane_payload,
    canonicalize_rust_lane_payload,
)


def test_python_lane_payload_canonicalizes_legacy_diagnostics_key() -> None:
    payload: dict[str, object] = {
        "tree_sitter_diagnostics": [{"kind": "ERROR"}],
        "node_kind": "identifier",
    }
    normalized = canonicalize_python_lane_payload(payload)
    assert "tree_sitter_diagnostics" not in normalized
    assert normalized["cst_diagnostics"] == [{"kind": "ERROR"}]
    assert normalized["cst_query_hits"] == []


def test_rust_lane_payload_defaults_empty_cst_fields() -> None:
    payload: dict[str, object] = {"language": "rust", "scope_chain": ["function_item:main"]}
    normalized = canonicalize_rust_lane_payload(payload)
    assert normalized["cst_diagnostics"] == []
    assert normalized["cst_query_hits"] == []
