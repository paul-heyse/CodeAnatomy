"""E2E coverage for Rust module/import graph enrichment."""

from __future__ import annotations

import pytest
from tools.cq.search.rust.enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.tree_sitter.rust_lane.runtime import is_tree_sitter_rust_available


@pytest.mark.skipif(not is_tree_sitter_rust_available(), reason="tree-sitter-rust unavailable")
def test_rust_module_graph_is_attached_from_tree_sitter_facts() -> None:
    """Test rust module graph is attached from tree sitter facts."""
    source = "mod corelib { pub fn run() {} }\nuse corelib::run;\nfn main() { run(); }\n"
    byte_start = source.index("run();")
    byte_end = byte_start + len("run")
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key="tests/e2e/module_graph.rs",
    )
    assert isinstance(payload, dict)
    module_graph = payload.get("rust_module_graph")
    assert isinstance(module_graph, dict)
