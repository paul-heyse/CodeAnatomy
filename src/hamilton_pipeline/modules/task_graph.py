"""Hamilton nodes for task graph construction."""

from __future__ import annotations

from typing import TYPE_CHECKING

from hamilton.function_modifiers import tag

from relspec.graph_inference import TaskGraph, task_graph_from_catalog
from relspec.rustworkx_schedule import TaskSchedule, schedule_tasks

if TYPE_CHECKING:
    from relspec.evidence import EvidenceCatalog
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
