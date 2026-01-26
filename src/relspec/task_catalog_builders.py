"""Default task catalog builders for the view-driven pipeline."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from arrowdsl.core.ordering import Ordering
from datafusion_engine.view_registry import ensure_view_graph
from ibis_engine.plan import IbisPlan
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
    from datafusion import SessionContext

    from datafusion_engine.view_graph_registry import ViewNode
    from sqlglot_tools.compat import Expression


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
    return IbisPlan(expr=expr, ordering=Ordering.unordered())


def _sqlglot_builder_from_ast(
    ast: Expression | None,
) -> Callable[[TaskBuildContext], Expression] | None:
    if ast is None:
        return None

    def _build(_context: TaskBuildContext) -> Expression:
        return ast

    return _build


def _session_context(context: TaskBuildContext) -> SessionContext:
    if context.facade is not None:
        return context.facade.ctx
    profile = context.ctx.runtime.datafusion
    if profile is None:
        msg = "ExecutionContext.runtime.datafusion is required for view registration."
        raise ValueError(msg)
    return profile.session_context()


def default_task_catalog() -> TaskCatalog:
    """Raise because the default catalog is deprecated.

    Raises
    ------
    ValueError
        Always raised to enforce view-driven task catalogs.
    """
    msg = "default_task_catalog is deprecated; use task_catalog_from_view_nodes."
    raise ValueError(msg)


def task_catalog_from_view_nodes(nodes: Sequence[ViewNode]) -> TaskCatalog:
    """Return a task catalog derived from view registry nodes.

    Returns
    -------
    TaskCatalog
        Task catalog with one view task per node.
    """
    tasks = tuple(
        TaskSpec(
            name=node.name,
            output=node.name,
            build=_view_task_builder(node.name),
            kind="view",
            priority=_priority_for_output(node.name),
            sqlglot_builder=_sqlglot_builder_from_ast(node.sqlglot_ast),
        )
        for node in nodes
        if node.sqlglot_ast is not None
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


__all__ = ["build_task_catalog", "default_task_catalog", "task_catalog_from_view_nodes"]
