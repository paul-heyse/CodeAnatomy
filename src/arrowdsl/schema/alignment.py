"""Alignment and encoding helpers for tables and plans."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, SchemaLike, TableLike, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.schema import (
    EncodingPolicy,
    EncodingSpec,
    align_to_schema,
    encode_expression,
    projection_for_schema,
)


def encoding_projection(
    columns: Sequence[str],
    *,
    available: Sequence[str],
) -> tuple[list[ComputeExpression], list[str]]:
    """Return projection expressions to apply dictionary encoding.

    Returns
    -------
    tuple[list[ComputeExpression], list[str]]
        Expressions and column names for encoding projection.
    """
    encode_set = set(columns)
    expressions: list[ComputeExpression] = []
    names: list[str] = []
    for name in available:
        expr = encode_expression(name) if name in encode_set else pc.field(name)
        expressions.append(expr)
        names.append(name)
    return expressions, names


def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    """Return columns marked for dictionary encoding via field metadata.

    Returns
    -------
    list[str]
        Column names marked for dictionary encoding.
    """
    encoding_columns: list[str] = []
    for field in schema:
        meta = field.metadata or {}
        if meta.get(b"encoding") == b"dictionary":
            encoding_columns.append(field.name)
    return encoding_columns


def align_table(
    table: TableLike,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
) -> TableLike:
    """Align a table to a schema using context casting policy.

    Returns
    -------
    TableLike
        Aligned table.
    """
    aligned, _ = align_to_schema(
        table,
        schema=schema,
        safe_cast=ctx.safe_cast,
        keep_extra_columns=keep_extra_columns,
    )
    return aligned


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
) -> Plan:
    """Align a plan to a target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting to the schema.
    """
    available = plan.schema(ctx=ctx).names
    exprs, names = projection_for_schema(schema, available=available, safe_cast=ctx.safe_cast)
    if keep_extra_columns:
        extras = [name for name in available if name not in names]
        for name in extras:
            names.append(name)
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def encode_plan(plan: Plan, *, columns: Sequence[str], ctx: ExecutionContext) -> Plan:
    """Return a plan with dictionary encoding applied.

    Returns
    -------
    Plan
        Plan with dictionary-encoded columns.
    """
    exprs, names = encoding_projection(columns, available=plan.schema(ctx=ctx).names)
    return plan.project(exprs, names, ctx=ctx)


def encode_table(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified columns on a table.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    specs = tuple(EncodingSpec(column=col) for col in columns)
    return EncodingPolicy(specs=specs).apply(table)


def encode_any(
    value: TableLike | Plan,
    *,
    policy: EncodingPolicy,
    ctx: ExecutionContext,
) -> TableLike | Plan:
    """Apply encoding policy to a plan or table.

    Returns
    -------
    TableLike | Plan
        Encoded plan or table.
    """
    if isinstance(value, Plan):
        columns = tuple(spec.column for spec in policy.specs)
        return encode_plan(value, columns=columns, ctx=ctx)
    return policy.apply(value)


__all__ = [
    "align_plan",
    "align_table",
    "encode_any",
    "encode_plan",
    "encode_table",
    "encoding_columns_from_metadata",
    "encoding_projection",
]
