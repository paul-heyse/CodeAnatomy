"""Default task catalog builders for the view-driven pipeline."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from arrowdsl.core.ordering import Ordering
from datafusion_engine.schema_registry import schema_for
from datafusion_engine.view_registry import ensure_view_graph
from datafusion_engine.view_registry_specs import view_graph_output_names
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import bind_expr_schema
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec
from relspec.view_defs import (
    DEFAULT_REL_TASK_PRIORITY,
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


_RELSPEC_OUTPUTS: frozenset[str] = frozenset(
    {
        REL_NAME_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_CALLSITE_QNAME_OUTPUT,
        RELATION_OUTPUT_NAME,
    }
)


def _priority_for_output(output: str) -> int:
    if output in _RELSPEC_OUTPUTS:
        return DEFAULT_REL_TASK_PRIORITY
    if output.startswith("cpg_"):
        return 100
    return 100


def _view_task_builder(output: str) -> Callable[[TaskBuildContext], IbisPlan]:
    def _build(context: TaskBuildContext) -> IbisPlan:
        return _view_plan(context, output=output)

    return _build


def _view_plan(context: TaskBuildContext, *, output: str) -> IbisPlan:
    session = _session_context(context)
    ensure_view_graph(
        session,
        runtime_profile=context.ctx.runtime.datafusion,
        include_registry_views=True,
    )
    expr = context.backend.table(output)
    schema = _schema_for(output)
    bound = bind_expr_schema(expr, schema=schema, allow_extra_columns=False)
    return IbisPlan(expr=bound, ordering=Ordering.unordered())


def _schema_for(name: str) -> pa.Schema:
    return schema_for(name)


def _session_context(context: TaskBuildContext) -> SessionContext:
    if context.facade is not None:
        return context.facade.ctx
    profile = context.ctx.runtime.datafusion
    if profile is None:
        msg = "ExecutionContext.runtime.datafusion is required for view registration."
        raise ValueError(msg)
    return profile.session_context()


def default_task_catalog() -> TaskCatalog:
    """Return the default task catalog for the pipeline.

    Returns
    -------
    TaskCatalog
        Combined task catalog for normalization + CPG outputs.
    """
    tasks = tuple(
        TaskSpec(
            name=f"view.{output_name}",
            output=output_name,
            build=_view_task_builder(output_name),
            kind="view",
            priority=_priority_for_output(output_name),
        )
        for output_name in view_graph_output_names()
    )
    return TaskCatalog(tasks=tasks)


def build_task_catalog() -> TaskCatalog:
    """Return the default task catalog (compatibility alias).

    Returns
    -------
    TaskCatalog
        Combined task catalog for normalization + CPG outputs.
    """
    return default_task_catalog()


__all__ = ["build_task_catalog", "default_task_catalog"]
