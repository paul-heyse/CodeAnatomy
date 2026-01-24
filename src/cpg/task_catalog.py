"""Task catalog for CPG build plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cpg.plan_builders import build_cpg_edges_plan, build_cpg_nodes_plan, build_cpg_props_plan
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

    def _build_nodes(context: TaskBuildContext) -> IbisPlan:
        if context.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        return build_cpg_nodes_plan(
            context.ibis_catalog,
            context.ctx,
            context.backend,
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
        )

    tasks = (
        TaskSpec(name="cpg.nodes", output="cpg_nodes_v1", build=_build_nodes, kind="compute"),
        TaskSpec(name="cpg.edges", output="cpg_edges_v1", build=_build_edges, kind="compute"),
        TaskSpec(name="cpg.props", output="cpg_props_v1", build=_build_props, kind="compute"),
    )
    return TaskCatalog(tasks=tasks)


__all__ = ["cpg_task_catalog"]
