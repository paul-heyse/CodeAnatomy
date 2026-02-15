"""Tests for rust enrichment orchestration and schema lint helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.rust import enrichment as rust_enrichment


def test_schema_lint_has_no_deprecated_key_errors() -> None:
    errors = rust_enrichment.lint_rust_enrichment_schema()
    assert all("deprecated key emitted" not in err for err in errors)


def test_ast_grep_precedence_over_tree_sitter(monkeypatch: pytest.MonkeyPatch) -> None:
    source = 'fn build_graph() { println!("x"); }\n'

    def _fake_ast(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "language": "rust",
            "enrichment_status": "applied",
            "enrichment_sources": ["ast_grep"],
            "node_kind": "call_expression",
            "call_target": "build_graph",
        }

    def _fake_ts(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "language": "rust",
            "enrichment_status": "applied",
            "enrichment_sources": ["tree_sitter"],
            "call_target": "println",
            "macro_name": "println",
        }

    monkeypatch.setattr(rust_enrichment, "_build_ast_grep_payload", _fake_ast)
    monkeypatch.setattr(rust_enrichment, "_ts_enrich", _fake_ts)

    payload = rust_enrichment.enrich_rust_context_by_byte_range(
        source,
        byte_start=3,
        byte_end=14,
        cache_key="precedence",
    )
    assert payload is not None
    # ast-grep value should win for non-empty overlaps.
    assert payload.get("call_target") == "build_graph"
    # tree-sitter should still gap-fill missing keys.
    assert payload.get("macro_name") == "println"
    enrichment_sources = payload.get("enrichment_sources")
    assert isinstance(enrichment_sources, list)
    assert "tree_sitter" in enrichment_sources


def test_crosscheck_mode_emits_mismatch_details(monkeypatch: pytest.MonkeyPatch) -> None:
    source = 'fn build_graph() { println!("x"); }\n'

    def _fake_ast(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "language": "rust",
            "enrichment_status": "applied",
            "enrichment_sources": ["ast_grep"],
            "node_kind": "call_expression",
            "call_target": "build_graph",
        }

    def _fake_ts(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "language": "rust",
            "enrichment_status": "applied",
            "enrichment_sources": ["tree_sitter"],
            "call_target": "println",
        }

    monkeypatch.setattr(rust_enrichment, "_build_ast_grep_payload", _fake_ast)
    monkeypatch.setattr(rust_enrichment, "_ts_enrich", _fake_ts)
    monkeypatch.setenv("CQ_RUST_ENRICHMENT_CROSSCHECK", "1")
    payload = rust_enrichment.enrich_rust_context_by_byte_range(
        source,
        byte_start=3,
        byte_end=14,
        cache_key="crosscheck",
    )
    monkeypatch.delenv("CQ_RUST_ENRICHMENT_CROSSCHECK", raising=False)

    assert payload is not None
    assert payload.get("enrichment_status") == "degraded"
    mismatches = payload.get("crosscheck_mismatches")
    assert isinstance(mismatches, list)
    assert mismatches
