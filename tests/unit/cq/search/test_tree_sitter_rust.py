"""Tests for optional Rust tree-sitter enrichment helpers."""

from __future__ import annotations

import pytest
from tools.cq.search import tree_sitter_rust
from tools.cq.search.tree_sitter_rust import MAX_SOURCE_BYTES

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


def test_enrich_skips_large_source() -> None:
    """Enrichment should return None for sources exceeding MAX_SOURCE_BYTES."""
    large_source = "x" * (MAX_SOURCE_BYTES + 1)
    payload = tree_sitter_rust.enrich_rust_context(
        large_source,
        line=1,
        col=0,
        cache_key="large",
    )
    assert payload is None


def test_enrich_handles_negative_col() -> None:
    """Enrichment should return None when col is negative."""
    payload = tree_sitter_rust.enrich_rust_context(
        _RUST_SAMPLE,
        line=2,
        col=-1,
        cache_key="negcol",
    )
    assert payload is None


@pytest.mark.skipif(
    not tree_sitter_rust.is_tree_sitter_rust_available(),
    reason="tree-sitter-rust is not available in this environment",
)
def test_scope_chain_bounded_depth() -> None:
    """Scope chain traversal should terminate within the node count limit.

    Construct a deeply nested Rust source with many ``mod`` blocks and
    verify that ``_scope_chain`` returns without hanging, producing a
    bounded result list.
    """
    nesting = 300
    lines = []
    for i in range(nesting):
        indent = "    " * i
        lines.append(f"{indent}mod m{i} {{")
    # Place a function at the innermost level
    inner_indent = "    " * nesting
    lines.append(f"{inner_indent}fn leaf() {{}}")
    for i in reversed(range(nesting)):
        indent = "    " * i
        lines.append(f"{indent}}}")
    deep_source = "\n".join(lines) + "\n"

    tree_sitter_rust.clear_tree_sitter_rust_cache()
    # Target the leaf function (line is 1-based; it sits at line nesting+1)
    payload = tree_sitter_rust.enrich_rust_context(
        deep_source,
        line=nesting + 1,
        col=len(inner_indent) + 3,
        cache_key="deep",
    )
    # The call must complete (not hang). The result may be a dict or None
    # depending on parser resolution, but it must terminate.
    if payload is not None:
        chain = payload.get("scope_chain")
        assert isinstance(chain, list)
        # Chain length must be bounded by the internal node-visit cap (256)
        assert len(chain) <= 256
