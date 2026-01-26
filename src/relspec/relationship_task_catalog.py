"""Task catalog for relationship outputs."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

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
from relspec.relationship_sql import (
    build_rel_callsite_qname_sql,
    build_rel_callsite_symbol_sql,
    build_rel_def_symbol_sql,
    build_rel_import_symbol_sql,
    build_rel_name_symbol_sql,
    build_relation_output_sql,
)
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    from sqlglot_tools.compat import Expression


def relationship_task_catalog() -> TaskCatalog:
    """Return the task catalog for relationship outputs.

    Returns
    -------
    TaskCatalog
        Catalog for relationship output plans.
    """
    default_priority = 100
    tasks = (
        TaskSpec(
            name="rel.name_symbol",
            output=REL_NAME_SYMBOL_OUTPUT,
            build=_plan_builder(
                build_rel_name_symbol_plan,
                task_name="rel.name_symbol",
                task_priority=default_priority,
            ),
            sqlglot_builder=_sql_builder(
                build_rel_name_symbol_sql,
                task_name="rel.name_symbol",
                task_priority=default_priority,
            ),
            kind="view",
            priority=default_priority,
        ),
        TaskSpec(
            name="rel.import_symbol",
            output=REL_IMPORT_SYMBOL_OUTPUT,
            build=_plan_builder(
                build_rel_import_symbol_plan,
                task_name="rel.import_symbol",
                task_priority=default_priority,
            ),
            sqlglot_builder=_sql_builder(
                build_rel_import_symbol_sql,
                task_name="rel.import_symbol",
                task_priority=default_priority,
            ),
            kind="view",
            priority=default_priority,
        ),
        TaskSpec(
            name="rel.def_symbol",
            output=REL_DEF_SYMBOL_OUTPUT,
            build=_plan_builder(
                build_rel_def_symbol_plan,
                task_name="rel.def_symbol",
                task_priority=default_priority,
            ),
            sqlglot_builder=_sql_builder(
                build_rel_def_symbol_sql,
                task_name="rel.def_symbol",
                task_priority=default_priority,
            ),
            kind="view",
            priority=default_priority,
        ),
        TaskSpec(
            name="rel.callsite_symbol",
            output=REL_CALLSITE_SYMBOL_OUTPUT,
            build=_plan_builder(
                build_rel_callsite_symbol_plan,
                task_name="rel.callsite_symbol",
                task_priority=default_priority,
            ),
            sqlglot_builder=_sql_builder(
                build_rel_callsite_symbol_sql,
                task_name="rel.callsite_symbol",
                task_priority=default_priority,
            ),
            kind="view",
            priority=default_priority,
        ),
        TaskSpec(
            name="rel.callsite_qname",
            output=REL_CALLSITE_QNAME_OUTPUT,
            build=_plan_builder(
                build_rel_callsite_qname_plan,
                task_name="rel.callsite_qname",
                task_priority=default_priority,
            ),
            sqlglot_builder=_sql_builder(
                build_rel_callsite_qname_sql,
                task_name="rel.callsite_qname",
                task_priority=default_priority,
            ),
            kind="view",
            priority=default_priority,
        ),
        TaskSpec(
            name="rel.output",
            output=RELATION_OUTPUT_NAME,
            build=_plan_builder(build_relation_output_plan),
            sqlglot_builder=_sql_builder(build_relation_output_sql),
            kind="view",
            priority=default_priority,
        ),
    )
    return TaskCatalog(tasks=tasks)


def _plan_builder(
    builder: Callable[..., IbisPlan],
    *,
    task_name: str | None = None,
    task_priority: int | None = None,
) -> Callable[[TaskBuildContext], IbisPlan]:
    def _build(context: TaskBuildContext) -> IbisPlan:
        if context.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for relationship tasks."
            raise ValueError(msg)
        if task_name is not None and task_priority is not None:
            return builder(
                context.ibis_catalog,
                context.ctx,
                context.backend,
                task_name=task_name,
                task_priority=task_priority,
            )
        return builder(context.ibis_catalog, context.ctx, context.backend)

    return _build


def _sql_builder(
    builder: Callable[..., Expression],
    *,
    task_name: str | None = None,
    task_priority: int | None = None,
) -> Callable[[TaskBuildContext], Expression]:
    def _build(_context: TaskBuildContext) -> Expression:
        if task_name is not None and task_priority is not None:
            return builder(task_name=task_name, task_priority=task_priority)
        return builder()

    return _build


__all__ = ["relationship_task_catalog"]
