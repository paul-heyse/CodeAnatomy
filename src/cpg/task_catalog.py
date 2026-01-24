"""Task catalog for CPG build plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cpg.plan_builders import build_cpg_edges_plan, build_cpg_nodes_plan, build_cpg_props_plan
from cpg.specs import TaskIdentity
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    from ibis_engine.plan import IbisPlan


def cpg_task_catalog() -> TaskCatalog:
    """Return CPG tasks for the inference pipeline.

    Returns
    -------
    TaskCatalog
        Catalog with task specs for CPG outputs.
    """
    cpg_task_priority = 100
    cpg_nodes_task = "cpg.nodes"
    cpg_edges_task = "cpg.edges"
    cpg_props_task = "cpg.props"

    nodes_identity = TaskIdentity(name=cpg_nodes_task, priority=cpg_task_priority)
    props_identity = TaskIdentity(name=cpg_props_task, priority=cpg_task_priority)

    def _build_nodes(context: TaskBuildContext) -> IbisPlan:
        if context.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        return build_cpg_nodes_plan(
            context.ibis_catalog,
            context.ctx,
            context.backend,
            task_identity=nodes_identity,
        )

    def _build_edges(context: TaskBuildContext) -> IbisPlan:
        if context.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        return build_cpg_edges_plan(
            context.ibis_catalog,
            context.ctx,
            context.backend,
        )

    def _build_props(context: TaskBuildContext) -> IbisPlan:
        if context.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        return build_cpg_props_plan(
            context.ibis_catalog,
            context.ctx,
            context.backend,
            task_identity=props_identity,
        )

    tasks = (
        TaskSpec(
            name=cpg_nodes_task,
            output="cpg_nodes_v1",
            build=_build_nodes,
            kind="compute",
            priority=cpg_task_priority,
        ),
        TaskSpec(
            name=cpg_edges_task,
            output="cpg_edges_v1",
            build=_build_edges,
            kind="compute",
            priority=cpg_task_priority,
        ),
        TaskSpec(
            name=cpg_props_task,
            output="cpg_props_v1",
            build=_build_props,
            kind="compute",
            priority=cpg_task_priority,
        ),
    )
    return TaskCatalog(tasks=tasks)


__all__ = ["cpg_task_catalog"]
