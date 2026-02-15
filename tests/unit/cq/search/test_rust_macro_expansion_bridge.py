"""Tests for Rust macro expansion evidence bridge."""

from __future__ import annotations

from tools.cq.search.rust.evidence import build_macro_expansion_evidence


def test_build_macro_expansion_evidence_from_call_facts() -> None:
    payload: dict[str, object] = {
        "rust_tree_sitter_facts": {
            "calls": ["sql!", "helper_fn", "regex!"],
        }
    }
    evidence = build_macro_expansion_evidence(payload)
    assert evidence
    names = {row.macro_name for row in evidence}
    assert "sql" in names
    assert "regex" in names


def test_build_macro_expansion_evidence_prefers_bridge_results() -> None:
    payload: dict[str, object] = {
        "macro_expansion_results": [
            {
                "macro_call_id": "src/lib.rs:10:4:sql",
                "name": "sql",
                "expansion": "SELECT 1",
                "applied": True,
            }
        ]
    }

    evidence = build_macro_expansion_evidence(payload)

    assert len(evidence) == 1
    assert evidence[0].macro_name == "sql"
    assert evidence[0].applied is True
    assert evidence[0].expansion == "SELECT 1"
