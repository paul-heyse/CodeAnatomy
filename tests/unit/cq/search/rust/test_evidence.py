"""Tests for rust evidence helpers."""

from __future__ import annotations

from tools.cq.search.rust.evidence import attach_rust_evidence, build_macro_evidence, macro_rows


def test_macro_rows_filters_macro_names() -> None:
    rows = macro_rows(["println!", "dbg!", "not_macro"])
    assert [row.macro_name for row in rows] == ["println", "dbg"]


def test_build_macro_evidence_reads_tree_sitter_facts() -> None:
    payload: dict[str, object] = {"rust_tree_sitter_facts": {"calls": ["println!", "foo!"]}}
    rows = build_macro_evidence(payload)
    assert len(rows) == 2


def test_attach_rust_evidence_sets_payload_fields() -> None:
    payload: dict[str, object] = {
        "rust_tree_sitter_facts": {
            "calls": ["println!"],
            "modules": ["crate::m"],
            "imports": ["crate::m::dep"],
        }
    }
    enriched = attach_rust_evidence(payload)
    assert "macro_expansions" in enriched
    assert "rust_module_graph" in enriched
