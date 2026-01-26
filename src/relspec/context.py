"""Helpers for building relspec execution contexts."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from ibis_engine.execution_factory import datafusion_facade_from_ctx
from relspec.task_catalog import TaskBuildContext

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.catalog import IbisPlanCatalog
    from normalize.runtime import NormalizeRuntime


def ensure_task_build_context(
    ctx: ExecutionContext,
    backend: BaseBackend,
    *,
    build_context: TaskBuildContext | None = None,
    ibis_catalog: IbisPlanCatalog | None = None,
    runtime: NormalizeRuntime | None = None,
) -> TaskBuildContext:
    """Return a TaskBuildContext with a DataFusion facade when available.

    Returns
    -------
    TaskBuildContext
        Build context with a facade attached when DataFusion is configured.
    """
    resolved = build_context or TaskBuildContext(
        ctx=ctx,
        backend=backend,
        ibis_catalog=ibis_catalog,
        runtime=runtime,
    )
    if resolved.facade is None:
        facade = datafusion_facade_from_ctx(ctx, backend=backend)
        if facade is not None:
            resolved = replace(resolved, facade=facade)
    return resolved


__all__ = ["ensure_task_build_context"]
