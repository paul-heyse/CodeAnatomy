"""Task graph construction from view-driven SQLGlot dependencies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.view_graph_registry import ViewNode
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from relspec.plan_catalog import PlanArtifact, PlanCatalog
from relspec.rustworkx_graph import TaskGraph, build_task_graph_from_inferred_deps


def build_task_graph(
    artifacts: Sequence[PlanArtifact],
    *,
    priorities: Mapping[str, int] | None = None,
) -> TaskGraph:
    """Build a task graph from compiled plan artifacts.

    Parameters
    ----------
    artifacts : Sequence[PlanArtifact]
        Compiled plan artifacts.
    priorities : Mapping[str, int] | None
        Optional per-task priority overrides.

    Returns
    -------
    TaskGraph
        Task graph built from inferred dependencies.
    """
    deps = tuple(artifact.deps for artifact in artifacts)
    resolved_priorities = priorities or {
        artifact.task.name: artifact.task.priority for artifact in artifacts
    }
    return build_task_graph_from_inferred_deps(deps, priorities=resolved_priorities)


def build_task_graph_from_views(
    nodes: Sequence[ViewNode],
    *,
    priorities: Mapping[str, int] | None = None,
) -> TaskGraph:
    """Build a task graph from view nodes.

    Parameters
    ----------
    nodes : Sequence[ViewNode]
        View registry nodes with SQLGlot ASTs.
    priorities : Mapping[str, int] | None
        Optional per-view priority overrides.

    Returns
    -------
    TaskGraph
        Task graph built from inferred dependencies.
    """
    inferred: Sequence[InferredDeps] = infer_deps_from_view_nodes(nodes)
    resolved_priorities = priorities or {}
    return build_task_graph_from_inferred_deps(inferred, priorities=resolved_priorities)


def task_graph_from_catalog(catalog: PlanCatalog) -> TaskGraph:
    """Build a task graph from a plan catalog.

    Parameters
    ----------
    catalog : PlanCatalog
        Plan catalog with compiled artifacts.

    Returns
    -------
    TaskGraph
        Task graph built from inferred dependencies.
    """
    return build_task_graph(catalog.artifacts)


__all__ = [
    "TaskGraph",
    "build_task_graph",
    "build_task_graph_from_views",
    "task_graph_from_catalog",
]
