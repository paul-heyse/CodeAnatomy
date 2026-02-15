"""E2E coverage for Rust injection enrichment payloads."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.rust_lane.runtime import (
    enrich_rust_context_by_byte_range,
    is_tree_sitter_rust_available,
)


@pytest.mark.skipif(not is_tree_sitter_rust_available(), reason="tree-sitter-rust unavailable")
def test_rust_injection_plan_is_emitted_for_macro_invocation() -> None:
    source = 'fn main() { sql!("select 1"); }\n'
    byte_start = source.index("sql")
    byte_end = byte_start + len("sql")
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key="tests/e2e/sql_macro.rs",
    )
    assert isinstance(payload, dict)
    injections = payload.get("query_pack_injections")
    assert isinstance(injections, list)
