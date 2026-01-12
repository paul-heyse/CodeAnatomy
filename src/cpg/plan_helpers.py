"""Plan helpers for CPG plan-lane builders."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, SchemaLike, TableLike, pc
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.schema.schema import (
    EncodingSpec,
    empty_table,
    encode_expression,
    projection_for_schema,
)


def ensure_plan(source: Plan | TableLike, *, label: str = "") -> Plan:
    """Return a plan backed by an Acero table source.

    Returns
    -------
    Plan
        Plan for the source value.
    """
    if isinstance(source, Plan):
        return source
    return Plan.table_source(source, label=label)


def empty_plan(schema: SchemaLike, *, label: str = "") -> Plan:
    """Return a plan backed by an empty table with the provided schema.

    Returns
    -------
    Plan
        Plan with an empty table source.
    """
    return Plan.table_source(empty_table(schema), label=label)


def align_plan(plan: Plan, *, schema: SchemaLike, ctx: ExecutionContext) -> Plan:
    """Align a plan to a target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting to the schema.
    """
    available = plan.schema(ctx=ctx).names
    exprs, names = projection_for_schema(schema, available=available, safe_cast=ctx.safe_cast)
    return plan.project(exprs, names, ctx=ctx)


def encode_plan(
    plan: Plan,
    *,
    specs: Sequence[EncodingSpec],
    ctx: ExecutionContext,
) -> Plan:
    """Return a plan with dictionary encoding applied.

    Returns
    -------
    Plan
        Plan with dictionary-encoded columns.
    """
    encode_cols = {spec.column for spec in specs}
    names = plan.schema(ctx=ctx).names
    exprs: list[ComputeExpression] = []
    for name in names:
        if name in encode_cols:
            exprs.append(encode_expression(name))
        else:
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def set_or_append_column(
    plan: Plan,
    *,
    name: str,
    expr: ComputeExpression,
    ctx: ExecutionContext,
) -> Plan:
    """Replace or append a column with the provided expression.

    Returns
    -------
    Plan
        Updated plan with the column set.
    """
    names = list(plan.schema(ctx=ctx).names)
    exprs = [pc.field(col) for col in names]
    if name in names:
        idx = names.index(name)
        exprs[idx] = expr
    else:
        names.append(name)
        exprs.append(expr)
    return plan.project(exprs, names, ctx=ctx)


def finalize_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    """Materialize a plan as a table.

    Returns
    -------
    TableLike
        Materialized plan output.
    """
    return PlanSpec.from_plan(plan).to_table(ctx=ctx)


__all__ = [
    "align_plan",
    "empty_plan",
    "encode_plan",
    "ensure_plan",
    "finalize_plan",
    "set_or_append_column",
]
