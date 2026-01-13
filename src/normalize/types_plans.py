"""Plan builders for normalized type tables."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow as pa

from arrowdsl.compute.expr_core import HashExprSpec, trimmed_non_empty_expr
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan
from arrowdsl.plan_helpers import project_to_schema
from arrowdsl.schema.schema import empty_table
from normalize.registry_specs import (
    dataset_input_columns,
    dataset_input_schema,
    dataset_query,
    dataset_schema,
)
from normalize.utils import TYPE_ID_SPEC, PlanSource, plan_source, project_columns

TYPE_EXPRS_NAME = "type_exprs_norm_v1"
TYPE_NODES_NAME = "type_nodes_v1"


def _to_plan(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
) -> Plan:
    return plan_source(source, ctx=ctx, columns=columns)


def type_exprs_plan(
    cst_type_exprs: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane normalized type expression table.

    Returns
    -------
    Plan
        Plan producing normalized type expression rows.
    """
    base_names = dataset_input_columns(TYPE_EXPRS_NAME)
    plan = _to_plan(cst_type_exprs, ctx=ctx, columns=base_names)
    plan = project_to_schema(
        plan,
        schema=dataset_input_schema(TYPE_EXPRS_NAME),
        ctx=ctx,
    )

    _, non_empty = trimmed_non_empty_expr("expr_text")
    plan = plan.filter(non_empty, ctx=ctx)
    return dataset_query(TYPE_EXPRS_NAME).apply_to_plan(plan, ctx=ctx)


def type_nodes_plan_from_scip(
    scip_symbol_information: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane type node table from SCIP symbol information.

    Returns
    -------
    Plan
        Plan producing normalized type node rows.
    """
    plan = _to_plan(scip_symbol_information, ctx=ctx, columns=["type_repr"])
    available = set(plan.schema(ctx=ctx).names)
    if "type_repr" not in available:
        return Plan.table_source(empty_table(dataset_schema(TYPE_NODES_NAME)))

    trimmed, non_empty = trimmed_non_empty_expr("type_repr")
    plan = plan.filter(non_empty, ctx=ctx)
    plan = plan.project([trimmed], ["type_repr"], ctx=ctx)

    type_hash = HashExprSpec(spec=TYPE_ID_SPEC).to_expression()
    plan = project_columns(
        plan,
        base=["type_repr"],
        extras=[
            (type_hash, "type_id"),
            (pc.scalar("scip"), "type_form"),
            (pc.scalar("inferred"), "origin"),
        ],
        ctx=ctx,
    )
    return dataset_query(TYPE_NODES_NAME).apply_to_plan(plan, ctx=ctx)


def type_nodes_plan_from_exprs(
    type_exprs_norm: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane type node table from type expressions.

    Returns
    -------
    Plan
        Plan producing normalized type node rows.
    """
    plan = _to_plan(type_exprs_norm, ctx=ctx, columns=["type_id", "type_repr"])
    available = set(plan.schema(ctx=ctx).names)
    if "type_repr" not in available or "type_id" not in available:
        return Plan.table_source(empty_table(dataset_schema(TYPE_NODES_NAME)))

    trimmed, non_empty = trimmed_non_empty_expr("type_repr")
    valid = ensure_expression(pc.and_(non_empty, pc.is_valid(pc.field("type_id"))))
    plan = plan.filter(valid, ctx=ctx)
    plan = plan.project([pc.field("type_id"), trimmed], ["type_id", "type_repr"], ctx=ctx)
    plan = project_columns(
        plan,
        base=["type_id", "type_repr"],
        extras=[
            (pc.scalar("annotation"), "type_form"),
            (pc.scalar("annotation"), "origin"),
        ],
        ctx=ctx,
    )
    return dataset_query(TYPE_NODES_NAME).apply_to_plan(plan, ctx=ctx)


def type_nodes_plan(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None = None,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane type node table from normalized inputs.

    Returns
    -------
    Plan
        Plan producing normalized type node rows.
    """
    return _type_nodes_plan(type_exprs_norm, scip_symbol_information, ctx=ctx)


def _type_nodes_plan(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None,
    *,
    ctx: ExecutionContext,
) -> Plan:
    scip_table: TableLike | None = None
    if scip_symbol_information is not None:
        scip_plan = _to_plan(scip_symbol_information, ctx=ctx, columns=["type_repr"])
        scip_result = run_plan(scip_plan, ctx=ctx, prefer_reader=False)
        if isinstance(scip_result.value, pa.RecordBatchReader):
            msg = "Expected table result from run_plan."
            raise TypeError(msg)
        scip_table = cast("TableLike", scip_result.value)
    if (
        scip_table is not None
        and scip_table.num_rows > 0
        and "type_repr" in scip_table.column_names
    ):
        return type_nodes_plan_from_scip(scip_table, ctx=ctx)
    return type_nodes_plan_from_exprs(type_exprs_norm, ctx=ctx)


__all__ = [
    "TYPE_EXPRS_NAME",
    "TYPE_NODES_NAME",
    "type_exprs_plan",
    "type_nodes_plan",
    "type_nodes_plan_from_exprs",
    "type_nodes_plan_from_scip",
]
