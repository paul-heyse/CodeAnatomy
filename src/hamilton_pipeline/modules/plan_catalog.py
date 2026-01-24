"""Hamilton nodes for plan catalog compilation."""

from __future__ import annotations

from hamilton.function_modifiers import tag
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from relspec.plan_catalog import PlanCatalog, compile_task_catalog
from relspec.task_catalog import TaskBuildContext, TaskCatalog


@tag(layer="plans", artifact="plan_catalog", kind="catalog")
def plan_catalog(
    task_catalog: TaskCatalog,
    task_build_context: TaskBuildContext,
    ctx: ExecutionContext,
    ibis_backend: BaseBackend,
) -> PlanCatalog:
    """Compile the plan catalog for all tasks.

    Returns
    -------
    PlanCatalog
        Catalog of compiled plan artifacts.
    """
    return compile_task_catalog(
        task_catalog,
        backend=ibis_backend,
        ctx=ctx,
        build_context=task_build_context,
    )


__all__ = ["plan_catalog"]
