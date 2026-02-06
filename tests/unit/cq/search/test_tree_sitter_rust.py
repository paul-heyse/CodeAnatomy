"""Tests for optional Rust tree-sitter enrichment helpers."""

from __future__ import annotations

import pytest
from tools.cq.search import tree_sitter_rust

_RUST_SAMPLE = 'mod outer {\n    fn build_graph() {\n        println!("hello");\n    }\n}\n'


@pytest.mark.skipif(
    not tree_sitter_rust.is_tree_sitter_rust_available(),
    reason="tree-sitter-rust is not available in this environment",
)
def test_enrich_rust_context_returns_scope_chain() -> None:
    """Enrichment should return stable context fields for Rust source."""
    tree_sitter_rust.clear_tree_sitter_rust_cache()
    payload = tree_sitter_rust.enrich_rust_context(_RUST_SAMPLE, line=3, col=10, cache_key="sample")
    assert payload is not None
    assert isinstance(payload.get("node_kind"), str)
    chain = payload.get("scope_chain")
    assert isinstance(chain, list)
    assert any("function_item" in str(item) for item in chain)


def test_enrich_rust_context_fail_open_on_parser_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unexpected parser failures should degrade to None."""

    def _boom(_source: bytes) -> object:
        msg = "forced parse failure"
        raise RuntimeError(msg)

    tree_sitter_rust.clear_tree_sitter_rust_cache()
    monkeypatch.setattr(tree_sitter_rust, "_parse_tree", _boom)
    payload = tree_sitter_rust.enrich_rust_context(_RUST_SAMPLE, line=2, col=8, cache_key="boom")
    assert payload is None


def test_enrich_rust_context_returns_none_when_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unavailable runtime should return None without raising."""
    monkeypatch.setattr(tree_sitter_rust, "is_tree_sitter_rust_available", lambda: False)
    payload = tree_sitter_rust.enrich_rust_context(_RUST_SAMPLE, line=2, col=8, cache_key="missing")
    assert payload is None
