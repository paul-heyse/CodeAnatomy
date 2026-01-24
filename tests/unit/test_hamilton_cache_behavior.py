"""Unit tests for Hamilton cache behavior tags."""

from __future__ import annotations

from hamilton import driver
from hamilton import graph as hgraph

from hamilton_pipeline.modules import inputs


def _inputs_graph() -> hgraph.FunctionGraph:
    return driver.Builder().with_modules(inputs).build().graph


def test_inputs_nodes_marked_ignore_for_cache() -> None:
    """Ensure ephemeral runtime nodes are marked with cache ignore behavior."""
    graph = _inputs_graph()
    ignore_nodes = (
        "ctx",
        "diagnostics_collector",
        "engine_session",
        "ibis_backend",
        "ibis_execution",
        "streaming_table_provider",
    )
    for name in ignore_nodes:
        node = graph.nodes[name]
        assert node.tags.get("cache.behavior") == "ignore"
