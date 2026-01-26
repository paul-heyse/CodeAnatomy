"""Hamilton nodes for task catalog construction."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from hamilton.function_modifiers import tag
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.view_registry import ensure_view_graph
from datafusion_engine.view_registry_specs import view_graph_nodes
from ibis_engine.catalog import IbisPlanCatalog, IbisPlanSource
from relspec.context import ensure_task_build_context
from relspec.task_catalog import TaskBuildContext, TaskCatalog
from relspec.task_catalog_builders import task_catalog_from_view_nodes

if TYPE_CHECKING:
    from datafusion_engine.view_graph_registry import ViewNode


@tag(layer="tasks", artifact="task_catalog", kind="catalog")
def task_catalog(ctx: ExecutionContext) -> TaskCatalog:
    """Return the default task catalog.

    Returns
    -------
    TaskCatalog
        Default task catalog for the pipeline.

    Raises
    ------
    ValueError
        Raised when the DataFusion runtime profile is missing.
    """
    nodes = view_nodes(ctx)
    return task_catalog_from_view_nodes(nodes)


@tag(layer="views", artifact="view_nodes", kind="graph")
def view_nodes(ctx: ExecutionContext) -> tuple[ViewNode, ...]:
    """Return view graph nodes with SQLGlot ASTs attached.

    Returns
    -------
    tuple[ViewNode, ...]
        View nodes derived from registered views.
    """
    profile = ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view graph nodes."
        raise ValueError(msg)
    session = profile.session_context()
    snapshot = ensure_view_graph(
        session,
        runtime_profile=profile,
        include_registry_views=True,
    )
    nodes = view_graph_nodes(session, snapshot=snapshot)
    return tuple(node for node in nodes if node.sqlglot_ast is not None)


@tag(layer="tasks", artifact="task_build_context", kind="context")
def task_build_context(
    ctx: ExecutionContext,
    ibis_backend: BaseBackend,
    source_catalog_inputs: Mapping[str, IbisPlanSource],
) -> TaskBuildContext:
    """Build the TaskBuildContext for plan compilation.

    Returns
    -------
    TaskBuildContext
        Build context for task plan compilation.
    """
    ibis_catalog = IbisPlanCatalog(backend=ibis_backend, tables=dict(source_catalog_inputs))
    return ensure_task_build_context(
        ctx,
        ibis_backend,
        ibis_catalog=ibis_catalog,
    )


__all__ = ["task_build_context", "task_catalog", "view_nodes"]
