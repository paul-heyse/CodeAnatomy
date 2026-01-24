"""Hamilton nodes for task catalog construction."""

from __future__ import annotations

from hamilton.function_modifiers import tag
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from ibis_engine.catalog import IbisPlanCatalog
from relspec.task_catalog import TaskBuildContext, TaskCatalog
from relspec.task_catalog_builders import build_task_catalog


@tag(layer="tasks", artifact="task_catalog", kind="catalog")
def task_catalog() -> TaskCatalog:
    """Return the default task catalog.

    Returns
    -------
    TaskCatalog
        Default task catalog for the pipeline.
    """
    return build_task_catalog()


@tag(layer="tasks", artifact="task_build_context", kind="context")
def task_build_context(
    ctx: ExecutionContext,
    ibis_backend: BaseBackend,
) -> TaskBuildContext:
    """Build the TaskBuildContext for plan compilation.

    Returns
    -------
    TaskBuildContext
        Build context for task plan compilation.
    """
    ibis_catalog = IbisPlanCatalog(backend=ibis_backend)
    return TaskBuildContext(ctx=ctx, backend=ibis_backend, ibis_catalog=ibis_catalog)


__all__ = ["task_build_context", "task_catalog"]
