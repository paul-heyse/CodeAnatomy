"""Hamilton nodes for task graph construction."""

from __future__ import annotations

from hamilton.function_modifiers import tag

from relspec.graph_inference import TaskGraph, task_graph_from_catalog
from relspec.plan_catalog import PlanCatalog


@tag(layer="graph", artifact="task_graph", kind="graph")
def task_graph(plan_catalog: PlanCatalog) -> TaskGraph:
    """Build the task graph from plan catalog.

    Returns
    -------
    TaskGraph
        Task graph derived from inferred dependencies.
    """
    return task_graph_from_catalog(plan_catalog)


__all__ = ["task_graph"]
