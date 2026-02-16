"""Tests for rust evidence helpers."""

from __future__ import annotations

from tools.cq.search.rust.evidence import attach_rust_evidence, build_macro_evidence, macro_rows

EXPECTED_MACRO_ROWS = 2


def test_macro_rows_filters_macro_names() -> None:
    """Test macro rows filters macro names."""
    rows = macro_rows(["println!", "dbg!", "not_macro"])
    assert [row.macro_name for row in rows] == ["println", "dbg"]


def test_build_macro_evidence_reads_tree_sitter_facts() -> None:
    """Test build macro evidence reads tree sitter facts."""
    payload: dict[str, object] = {"rust_tree_sitter_facts": {"calls": ["println!", "foo!"]}}
    rows = build_macro_evidence(payload)
    assert len(rows) == EXPECTED_MACRO_ROWS


def test_attach_rust_evidence_sets_payload_fields() -> None:
    """Test attach rust evidence sets payload fields."""
    payload: dict[str, object] = {
        "rust_tree_sitter_facts": {
            "calls": ["println!"],
            "modules": ["crate::m"],
            "imports": ["crate::m::dep"],
        }
    }
    attach_rust_evidence(payload)
    assert "macro_expansions" in payload
    assert "rust_module_graph" in payload
