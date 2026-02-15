"""Tests for Rust module graph builder normalization."""

from __future__ import annotations

from tools.cq.search.rust.module_graph_builder import build_module_graph


def test_build_module_graph_normalizes_nodes_and_edges() -> None:
    graph = build_module_graph(
        module_rows=[
            {"module_name": "core", "module_id": "module:core", "file_path": "src/lib.rs"},
            {"module_name": "core", "module_id": "module:core", "file_path": "src/lib.rs"},
        ],
        import_rows=[
            {
                "source_module_id": "module:core",
                "target_path": "core::io::read",
                "visibility": "public",
                "is_reexport": True,
            },
            {
                "source_module_id": "module:core",
                "target_path": "core::io::read",
                "visibility": "public",
                "is_reexport": True,
            },
        ],
    )
    modules = graph.get("modules")
    edges = graph.get("edges")
    metadata = graph.get("metadata")

    assert isinstance(modules, (list, tuple))
    assert isinstance(edges, (list, tuple))
    assert isinstance(metadata, dict)
    assert len(modules) == 1
    assert len(edges) == 1
    assert metadata.get("module_count") == 1
    assert metadata.get("edge_count") == 1
