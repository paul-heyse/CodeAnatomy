"""Task catalog for normalization view outputs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.core.ordering import Ordering
from datafusion_engine.view_registry import ensure_view_graph
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import bind_expr_schema
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext


@dataclass(frozen=True)
class NormalizeTaskContext:
    """Build context wrapper for normalize tasks."""

    ibis_catalog: object
    ctx: ExecutionContext
    backend: BaseBackend


_NORMALIZE_OUTPUTS: tuple[str, ...] = (
    "type_exprs_norm_v1",
    "type_nodes_v1",
    "py_bc_blocks_norm_v1",
    "py_bc_cfg_edges_norm_v1",
    "py_bc_def_use_events_v1",
    "py_bc_reaches_v1",
    "diagnostics_norm_v1",
    "span_errors_v1",
)


def _view_plan(
    context: TaskBuildContext,
    *,
    output: str,
) -> IbisPlan:
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


def normalize_task_catalog() -> TaskCatalog:
    """Return the normalize task catalog derived from registered views.

    Returns
    -------
    TaskCatalog
        Normalize task catalog.
    """
    tasks = [
        TaskSpec(
            name=f"normalize.{output_name}",
            output=output_name,
            build=_view_task_builder(output_name),
            kind="view",
            cache_policy="none",
        )
        for output_name in _NORMALIZE_OUTPUTS
    ]
    return TaskCatalog(tasks=tuple(tasks))


def _view_task_builder(output: str) -> Callable[[TaskBuildContext], IbisPlan]:
    def _build(context: TaskBuildContext) -> IbisPlan:
        return _view_plan(context, output=output)

    return _build


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


__all__ = ["NormalizeTaskContext", "normalize_task_catalog"]
