"""Tests for Rust module/import graph builder."""

from __future__ import annotations

from tools.cq.search.rust_module_graph import build_rust_module_graph


def test_build_rust_module_graph_links_module_prefixed_imports() -> None:
    payload = {
        "rust_tree_sitter_facts": {
            "modules": ["corelib", "utils"],
            "imports": ["corelib::io::read", "serde::Serialize"],
        }
    }
    graph = build_rust_module_graph(payload)
    assert graph.modules == ("corelib", "utils")
    assert graph.imports == ("corelib::io::read", "serde::Serialize")
    assert any(edge.source == "corelib" for edge in graph.edges)
