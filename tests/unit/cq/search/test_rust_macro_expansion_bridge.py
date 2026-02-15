"""Tests for Rust macro expansion evidence bridge."""

from __future__ import annotations

from tools.cq.search.rust_macro_expansion_bridge import build_macro_expansion_evidence


def test_build_macro_expansion_evidence_from_call_facts() -> None:
    payload = {
        "rust_tree_sitter_facts": {
            "calls": ["sql!", "helper_fn", "regex!"],
        }
    }
    evidence = build_macro_expansion_evidence(payload)
    assert evidence
    names = {row.macro_name for row in evidence}
    assert "sql" in names
    assert "regex" in names
