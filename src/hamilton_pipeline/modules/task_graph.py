"""Hamilton nodes for task graph construction."""

from __future__ import annotations

from typing import TYPE_CHECKING

from hamilton.function_modifiers import tag

from relspec.graph_inference import TaskGraph, build_task_graph_from_views
from relspec.rustworkx_schedule import TaskSchedule, schedule_tasks

if TYPE_CHECKING:
    from datafusion_engine.view_graph_registry import ViewNode
    from relspec.evidence import EvidenceCatalog
    from schema_spec.system import DatasetSpec


@tag(layer="graph", artifact="task_graph", kind="graph")
def task_graph(
    view_nodes: tuple[ViewNode, ...],
    dataset_specs: tuple[DatasetSpec, ...] | None = None,
) -> TaskGraph:
    """Build the task graph from view nodes and optional dataset specs.

    Returns
    -------
    TaskGraph
        Task graph derived from inferred dependencies.
    """
    return build_task_graph_from_views(view_nodes, datasets=dataset_specs)


@tag(layer="graph", artifact="task_schedule", kind="schedule")
def task_schedule(
    task_graph: TaskGraph,
    evidence_catalog: EvidenceCatalog,
) -> TaskSchedule:
    """Build a task schedule from the inferred graph.

    Returns
    -------
    TaskSchedule
        Schedule with ordered tasks and generations.
    """
    return schedule_tasks(task_graph, evidence=evidence_catalog, allow_partial=True)


@tag(layer="graph", artifact="task_generations", kind="schedule")
def task_generations(task_schedule: TaskSchedule) -> tuple[tuple[str, ...], ...]:
    """Return task generations for scheduling diagnostics.

    Returns
    -------
    tuple[tuple[str, ...], ...]
        Generation waves of task names.
    """
    return task_schedule.generations


__all__ = ["task_generations", "task_graph", "task_schedule"]
