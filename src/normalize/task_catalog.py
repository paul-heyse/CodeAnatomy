"""Task catalog for normalization plan builders."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ibis.backends import BaseBackend

import normalize.dataset_specs as dataset_specs_module
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from normalize.ibis_plan_builders import plan_builders_ibis
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec


@dataclass(frozen=True)
class NormalizeTaskContext:
    """Build context wrapper for normalize tasks."""

    ibis_catalog: IbisPlanCatalog
    ctx: ExecutionContext
    backend: BaseBackend


def _empty_plan(output: str, *, backend: BaseBackend) -> IbisPlan:
    schema = dataset_specs_module.dataset_schema(output)
    return register_ibis_table(
        empty_table(schema),
        options=SourceToIbisOptions(
            backend=backend,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )


def normalize_task_catalog() -> TaskCatalog:
    """Return the normalize task catalog derived from plan builders.

    Returns
    -------
    TaskCatalog
        Normalize task catalog.
    """
    tasks: list[TaskSpec] = []
    for output_name, builder in plan_builders_ibis().items():

        def _build(
            context: TaskBuildContext,
            *,
            _builder: Callable[
                [IbisPlanCatalog, ExecutionContext, BaseBackend], IbisPlan | None
            ] = builder,
            _output: str = output_name,
        ) -> IbisPlan:
            if context.ibis_catalog is None:
                msg = "TaskBuildContext.ibis_catalog is required for normalize tasks."
                raise ValueError(msg)
            plan = _builder(context.ibis_catalog, context.ctx, context.backend)
            if plan is None:
                return _empty_plan(_output, backend=context.backend)
            return plan

        tasks.append(
            TaskSpec(
                name=f"normalize.{output_name}",
                output=output_name,
                build=_build,
                kind="view",
                cache_policy="none",
            )
        )
    return TaskCatalog(tasks=tuple(tasks))


__all__ = ["NormalizeTaskContext", "normalize_task_catalog"]
