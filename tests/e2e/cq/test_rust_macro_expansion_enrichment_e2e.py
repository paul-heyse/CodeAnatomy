"""E2E coverage for Rust macro expansion evidence bridge."""

from __future__ import annotations

import pytest
from tools.cq.search.rust_enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.tree_sitter_rust import is_tree_sitter_rust_available


@pytest.mark.skipif(not is_tree_sitter_rust_available(), reason="tree-sitter-rust unavailable")
def test_rust_macro_expansion_bridge_attaches_evidence() -> None:
    source = 'fn main() { regex!("a+"); }\n'
    byte_start = source.index("regex")
    byte_end = byte_start + len("regex")
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key="tests/e2e/regex_macro.rs",
    )
    assert isinstance(payload, dict)
    expansions = payload.get("macro_expansions")
    assert isinstance(expansions, list)
