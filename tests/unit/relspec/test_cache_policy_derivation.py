"""Tests for graph-derived cache policy derivation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import rustworkx as rx

from relspec.policy_compiler import _derive_cache_policies
from relspec.rustworkx_graph import (
    GraphEdge,
    GraphNode,
    TaskGraph,
    TaskNode,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def _build_test_task_graph(
    tasks: list[tuple[str, str]],
    edges: list[tuple[int, int]],
) -> TaskGraph:
    """Build a minimal TaskGraph for testing.

    Parameters
    ----------
    tasks
        List of (task_name, output_name) pairs.  Each pair becomes a
        TaskNode in the graph.
    edges
        List of (source_idx, target_idx) pairs referencing positions in
        ``tasks``.  Edges use a generic ``requires`` kind.

    Returns:
    -------
    TaskGraph
        Minimal task graph suitable for cache policy derivation tests.
    """
    graph: rx.PyDiGraph = rx.PyDiGraph()
    task_idx: dict[str, int] = {}

    for task_name, output_name in tasks:
        node = GraphNode(
            kind="task",
            payload=TaskNode(
                name=task_name,
                output=output_name,
                inputs=(),
                sources=(),
                priority=0,
                task_kind="semantic",
            ),
        )
        idx = graph.add_node(node)
        task_idx[task_name] = idx

    for src, tgt in edges:
        src_name = tasks[src][0]
        tgt_name = tasks[tgt][0]
        graph.add_edge(
            task_idx[src_name],
            task_idx[tgt_name],
            GraphEdge(kind="requires", name=f"{src_name}->{tgt_name}"),
        )

    return TaskGraph(
        graph=graph,
        evidence_idx={},
        task_idx=task_idx,
        output_policy="all_producers",
    )


@dataclass(frozen=True)
class _FakeDatasetLocation:
    """Minimal stand-in for DatasetLocation in tests."""

    path: str


class TestDeriveCachePolicies:
    """Test _derive_cache_policies with various graph topologies."""

    def test_terminal_node_in_output_locations(self) -> None:
        """Terminal node in output_locations gets delta_output."""
        graph = _build_test_task_graph(
            tasks=[("cpg_nodes", "cpg_nodes_v1")],
            edges=[],
        )
        locations: Mapping[str, object] = {
            "cpg_nodes": _FakeDatasetLocation(path="/out/cpg_nodes"),
        }
        result = _derive_cache_policies(graph, locations)
        assert result["cpg_nodes"] == "delta_output"

    def test_terminal_node_not_in_output_locations(self) -> None:
        """Terminal node (out_degree 0) not in output_locations gets none."""
        graph = _build_test_task_graph(
            tasks=[("temp_view", "temp_view_v1")],
            edges=[],
        )
        result = _derive_cache_policies(graph, {})
        assert result["temp_view"] == "none"

    def test_high_fanout_gets_staging(self) -> None:
        """Node with out_degree > 2 gets delta_staging."""
        # hub -> child_a, child_b, child_c  (fan-out = 3)
        graph = _build_test_task_graph(
            tasks=[
                ("hub", "hub_v1"),
                ("child_a", "child_a_v1"),
                ("child_b", "child_b_v1"),
                ("child_c", "child_c_v1"),
            ],
            edges=[(0, 1), (0, 2), (0, 3)],
        )
        result = _derive_cache_policies(graph, {})
        assert result["hub"] == "delta_staging"

    def test_low_fanout_intermediate_gets_staging(self) -> None:
        """Node with out_degree 1-2 gets delta_staging."""
        graph = _build_test_task_graph(
            tasks=[
                ("parent", "parent_v1"),
                ("child", "child_v1"),
            ],
            edges=[(0, 1)],
        )
        result = _derive_cache_policies(graph, {})
        assert result["parent"] == "delta_staging"
        assert result["child"] == "none"  # leaf

    def test_output_location_overrides_fanout(self) -> None:
        """Output location takes precedence over fan-out classification."""
        graph = _build_test_task_graph(
            tasks=[
                ("cpg_edges", "cpg_edges_v1"),
                ("child_a", "child_a_v1"),
                ("child_b", "child_b_v1"),
                ("child_c", "child_c_v1"),
            ],
            edges=[(0, 1), (0, 2), (0, 3)],
        )
        locations: Mapping[str, object] = {
            "cpg_edges": _FakeDatasetLocation(path="/out/cpg_edges"),
        }
        result = _derive_cache_policies(graph, locations)
        assert result["cpg_edges"] == "delta_output"

    def test_empty_graph(self) -> None:
        """Empty task graph produces empty policies."""
        graph = _build_test_task_graph(tasks=[], edges=[])
        result = _derive_cache_policies(graph, {})
        assert result == {}

    def test_chain_topology(self) -> None:
        """Linear chain: intermediate nodes are staging, leaf is none."""
        graph = _build_test_task_graph(
            tasks=[
                ("step_1", "step_1_v1"),
                ("step_2", "step_2_v1"),
                ("step_3", "step_3_v1"),
            ],
            edges=[(0, 1), (1, 2)],
        )
        result = _derive_cache_policies(graph, {})
        assert result["step_1"] == "delta_staging"
        assert result["step_2"] == "delta_staging"
        assert result["step_3"] == "none"

    def test_diamond_topology(self) -> None:
        """Diamond: root -> mid_a, mid_b -> leaf."""
        graph = _build_test_task_graph(
            tasks=[
                ("root", "root_v1"),
                ("mid_a", "mid_a_v1"),
                ("mid_b", "mid_b_v1"),
                ("leaf", "leaf_v1"),
            ],
            edges=[(0, 1), (0, 2), (1, 3), (2, 3)],
        )
        result = _derive_cache_policies(graph, {})
        assert result["root"] == "delta_staging"  # out_degree=2
        assert result["mid_a"] == "delta_staging"  # out_degree=1
        assert result["mid_b"] == "delta_staging"  # out_degree=1
        assert result["leaf"] == "none"  # out_degree=0

    def test_mixed_output_and_staging(self) -> None:
        """Mix of output locations and graph-derived policies."""
        graph = _build_test_task_graph(
            tasks=[
                ("cpg_nodes", "cpg_nodes_v1"),
                ("rel_calls", "rel_calls_v1"),
                ("temp", "temp_v1"),
            ],
            edges=[(1, 0), (1, 2)],
        )
        locations: Mapping[str, object] = {
            "cpg_nodes": _FakeDatasetLocation(path="/out/cpg_nodes"),
        }
        result = _derive_cache_policies(graph, locations)
        assert result["cpg_nodes"] == "delta_output"
        assert result["rel_calls"] == "delta_staging"  # out_degree=2
        assert result["temp"] == "none"  # leaf
