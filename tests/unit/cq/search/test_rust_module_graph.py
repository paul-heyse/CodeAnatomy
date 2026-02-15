"""Tests for Rust module/import graph builder."""

from __future__ import annotations

from tools.cq.search.rust.evidence import build_rust_module_graph


def test_build_rust_module_graph_uses_structured_rows() -> None:
    payload = {
        "rust_module_rows": [
            {"module_id": "module:corelib", "module_name": "corelib"},
            {"module_id": "module:utils", "module_name": "utils"},
        ],
        "rust_import_rows": [
            {
                "source_module_id": "module:corelib",
                "target_path": "corelib::io::read",
                "visibility": "public",
                "is_reexport": True,
            },
            {
                "source_module_id": "module:utils",
                "target_path": "serde::Serialize",
                "visibility": "private",
                "is_reexport": False,
            },
        ],
    }
    graph = build_rust_module_graph(payload)
    assert {row.module_name for row in graph.modules} == {"corelib", "utils"}
    assert any(edge.source_module_id == "module:corelib" for edge in graph.edges)
    assert any(edge.is_reexport for edge in graph.edges)
