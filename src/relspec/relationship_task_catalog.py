"""Task catalog for relationship outputs."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.plan import IbisPlan
from relspec.relationship_plans import (
    REL_CALLSITE_QNAME_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_NAME_SYMBOL_OUTPUT,
    RELATION_OUTPUT_NAME,
    build_rel_callsite_qname_plan,
    build_rel_callsite_symbol_plan,
    build_rel_def_symbol_plan,
    build_rel_import_symbol_plan,
    build_rel_name_symbol_plan,
    build_relation_output_plan,
)
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext


def relationship_task_catalog() -> TaskCatalog:
    """Return the task catalog for relationship outputs.

    Returns
    -------
    TaskCatalog
        Catalog for relationship output plans.
    """
    tasks = (
        TaskSpec(
            name="rel.name_symbol",
            output=REL_NAME_SYMBOL_OUTPUT,
            build=_plan_builder(build_rel_name_symbol_plan),
            kind="view",
        ),
        TaskSpec(
            name="rel.import_symbol",
            output=REL_IMPORT_SYMBOL_OUTPUT,
            build=_plan_builder(build_rel_import_symbol_plan),
            kind="view",
        ),
        TaskSpec(
            name="rel.def_symbol",
            output=REL_DEF_SYMBOL_OUTPUT,
            build=_plan_builder(build_rel_def_symbol_plan),
            kind="view",
        ),
        TaskSpec(
            name="rel.callsite_symbol",
            output=REL_CALLSITE_SYMBOL_OUTPUT,
            build=_plan_builder(build_rel_callsite_symbol_plan),
            kind="view",
        ),
        TaskSpec(
            name="rel.callsite_qname",
            output=REL_CALLSITE_QNAME_OUTPUT,
            build=_plan_builder(build_rel_callsite_qname_plan),
            kind="view",
        ),
        TaskSpec(
            name="rel.output",
            output=RELATION_OUTPUT_NAME,
            build=_plan_builder(build_relation_output_plan),
            kind="view",
        ),
    )
    return TaskCatalog(tasks=tasks)


def _plan_builder(
    builder: Callable[[IbisPlanCatalog, ExecutionContext, BaseBackend], IbisPlan],
) -> Callable[[TaskBuildContext], IbisPlan]:
    def _build(context: TaskBuildContext) -> IbisPlan:
        if context.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for relationship tasks."
            raise ValueError(msg)
        return builder(context.ibis_catalog, context.ctx, context.backend)

    return _build


__all__ = ["relationship_task_catalog"]
