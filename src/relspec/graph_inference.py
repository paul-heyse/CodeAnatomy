"""Task graph construction from view-driven SQLGlot dependencies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.view_graph_registry import ViewNode
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from relspec.rustworkx_graph import TaskGraph, build_task_graph_from_inferred_deps
from schema_spec.system import DatasetSpec


def build_task_graph_from_views(
    nodes: Sequence[ViewNode],
    *,
    priorities: Mapping[str, int] | None = None,
    datasets: Sequence[DatasetSpec] | None = None,
) -> TaskGraph:
    """Build a task graph from view nodes.

    Parameters
    ----------
    nodes : Sequence[ViewNode]
        View registry nodes with SQLGlot ASTs.
    priorities : Mapping[str, int] | None
        Optional per-view priority overrides.
    datasets : Sequence[DatasetSpec] | None
        Optional dataset specs to seed evidence nodes even when no view references them.

    Returns
    -------
    TaskGraph
        Task graph built from inferred dependencies.
    """
    inferred: Sequence[InferredDeps] = infer_deps_from_view_nodes(nodes)
    resolved_priorities = priorities or {}
    extra_evidence = tuple(spec.name for spec in datasets or ())
    return build_task_graph_from_inferred_deps(
        inferred,
        priorities=resolved_priorities,
        extra_evidence=extra_evidence,
    )


__all__ = [
    "TaskGraph",
    "build_task_graph_from_views",
]
