"""Task graph construction from inferred plan dependencies."""

from __future__ import annotations

from collections.abc import Sequence

from relspec.plan_catalog import PlanArtifact, PlanCatalog
from relspec.rustworkx_graph import TaskGraph, build_task_graph_from_inferred_deps


def build_task_graph(artifacts: Sequence[PlanArtifact]) -> TaskGraph:
    """Build a task graph from compiled plan artifacts.

    Parameters
    ----------
    artifacts : Sequence[PlanArtifact]
        Compiled plan artifacts.

    Returns
    -------
    TaskGraph
        Task graph built from inferred dependencies.
    """
    deps = tuple(artifact.deps for artifact in artifacts)
    return build_task_graph_from_inferred_deps(deps)


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


__all__ = ["TaskGraph", "build_task_graph", "task_graph_from_catalog"]
