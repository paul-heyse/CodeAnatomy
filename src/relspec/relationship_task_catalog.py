"""Task catalog for relationship view outputs."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from arrowdsl.core.ordering import Ordering
from datafusion_engine.view_registry import ensure_view_graph
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import bind_expr_schema
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec
from relspec.view_defs import (
    REL_CALLSITE_QNAME_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_NAME_SYMBOL_OUTPUT,
    RELATION_OUTPUT_NAME,
)

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext


def relationship_task_catalog() -> TaskCatalog:
    """Return the task catalog for relationship view outputs.

    Returns
    -------
    TaskCatalog
        Catalog for relationship output plans.
    """
    tasks = (
        TaskSpec(
            name="rel.name_symbol",
            output=REL_NAME_SYMBOL_OUTPUT,
            build=_view_task_builder(REL_NAME_SYMBOL_OUTPUT),
            kind="view",
            priority=100,
        ),
        TaskSpec(
            name="rel.import_symbol",
            output=REL_IMPORT_SYMBOL_OUTPUT,
            build=_view_task_builder(REL_IMPORT_SYMBOL_OUTPUT),
            kind="view",
            priority=100,
        ),
        TaskSpec(
            name="rel.def_symbol",
            output=REL_DEF_SYMBOL_OUTPUT,
            build=_view_task_builder(REL_DEF_SYMBOL_OUTPUT),
            kind="view",
            priority=100,
        ),
        TaskSpec(
            name="rel.callsite_symbol",
            output=REL_CALLSITE_SYMBOL_OUTPUT,
            build=_view_task_builder(REL_CALLSITE_SYMBOL_OUTPUT),
            kind="view",
            priority=100,
        ),
        TaskSpec(
            name="rel.callsite_qname",
            output=REL_CALLSITE_QNAME_OUTPUT,
            build=_view_task_builder(REL_CALLSITE_QNAME_OUTPUT),
            kind="view",
            priority=100,
        ),
        TaskSpec(
            name="rel.output",
            output=RELATION_OUTPUT_NAME,
            build=_view_task_builder(RELATION_OUTPUT_NAME),
            kind="view",
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


__all__ = ["relationship_task_catalog"]
