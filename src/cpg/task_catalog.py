"""Task catalog for CPG view outputs."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from arrowdsl.core.ordering import Ordering
from datafusion_engine.view_registry import ensure_view_graph
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import bind_expr_schema
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext


def cpg_task_catalog() -> TaskCatalog:
    """Return CPG tasks for the view-driven pipeline.

    Returns
    -------
    TaskCatalog
        Catalog with task specs for CPG outputs.
    """
    tasks = (
        TaskSpec(
            name="cpg.nodes",
            output="cpg_nodes_v1",
            build=_view_task_builder("cpg_nodes_v1"),
            kind="compute",
            priority=100,
        ),
        TaskSpec(
            name="cpg.edges",
            output="cpg_edges_v1",
            build=_view_task_builder("cpg_edges_v1"),
            kind="compute",
            priority=100,
        ),
        TaskSpec(
            name="cpg.props",
            output="cpg_props_v1",
            build=_view_task_builder("cpg_props_v1"),
            kind="compute",
            priority=100,
        ),
    )
    return TaskCatalog(tasks=tasks)


def _view_task_builder(output: str) -> Callable[[TaskBuildContext], IbisPlan]:
    def _build(context: TaskBuildContext) -> IbisPlan:
        return _view_plan(context, output=output)

    return _build


def _view_plan(context: TaskBuildContext, *, output: str) -> IbisPlan:
    session = _session_context(context)
    ensure_view_graph(
        session,
        runtime_profile=context.ctx.runtime.datafusion,
        include_registry_views=False,
    )
    expr = context.backend.table(output)
    schema = _schema_for(output)
    bound = bind_expr_schema(expr, schema=schema, allow_extra_columns=False)
    return IbisPlan(expr=bound, ordering=Ordering.unordered())


def _schema_for(name: str) -> pa.Schema:
    from datafusion_engine.schema_registry import schema_for

    return schema_for(name)


def _session_context(context: TaskBuildContext) -> SessionContext:
    if context.facade is not None:
        return context.facade.ctx
    profile = context.ctx.runtime.datafusion
    if profile is None:
        msg = "ExecutionContext.runtime.datafusion is required for view registration."
        raise ValueError(msg)
    return profile.session_context()


__all__ = ["cpg_task_catalog"]
