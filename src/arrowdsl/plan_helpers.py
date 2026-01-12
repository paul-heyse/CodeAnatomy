"""Shared plan-lane helper utilities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ComputeExpression,
    DataTypeLike,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.plan.source import PlanSource, plan_from_source
from arrowdsl.schema.schema import EncodingSpec, encode_columns, encode_expression
from arrowdsl.schema.structs import flatten_struct_field


def query_for_schema(schema: SchemaLike) -> QuerySpec:
    """Return a QuerySpec projecting the schema columns.

    Returns
    -------
    QuerySpec
        QuerySpec with base columns set to the schema names.
    """
    return QuerySpec.simple(*schema.names)


def column_or_null_expr(
    name: str,
    dtype: DataTypeLike,
    *,
    available: set[str],
    cast: bool = False,
    safe: bool = False,
) -> ComputeExpression:
    """Return a field expression or typed null when missing.

    Returns
    -------
    ComputeExpression
        Expression for the field or a typed null literal.
    """
    expr = pc.field(name) if name in available else pc.scalar(pa.scalar(None, type=dtype))
    if cast:
        return ensure_expression(pc.cast(expr, dtype, safe=safe))
    return ensure_expression(expr)


def coalesce_expr(
    cols: Sequence[str],
    *,
    dtype: DataTypeLike,
    available: set[str],
    cast: bool = False,
    safe: bool = False,
) -> ComputeExpression:
    """Return a coalesced expression over available columns.

    Returns
    -------
    ComputeExpression
        Coalesced expression or typed null when none are available.
    """
    exprs: list[ComputeExpression] = []
    for col in cols:
        if col not in available:
            continue
        expr = pc.field(col)
        if cast:
            expr = ensure_expression(pc.cast(expr, dtype, safe=safe))
        else:
            expr = ensure_expression(expr)
        exprs.append(expr)
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(None, type=dtype)))
    if len(exprs) == 1:
        return exprs[0]
    return ensure_expression(pc.coalesce(*exprs))


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


def project_columns(
    plan: Plan,
    *,
    base: Sequence[str],
    rename: Mapping[str, str] | None = None,
    extras: Sequence[tuple[ComputeExpression, str]] = (),
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Project columns with optional renames and appended expressions.

    Returns
    -------
    Plan
        Plan with projected columns and appended expressions.
    """
    rename = rename or {}
    names = [rename.get(name, name) for name in base]
    expressions: list[ComputeExpression] = [pc.field(name) for name in base]
    for expr, name in extras:
        expressions.append(expr)
        names.append(name)
    return plan.project(expressions, names, ctx=ctx)


def project_to_schema(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
) -> Plan:
    """Project a plan to the schema, filling missing columns with typed nulls.

    Returns
    -------
    Plan
        Plan aligned to the schema.
    """
    available = set(plan.schema(ctx=ctx).names)
    names: list[str] = []
    exprs: list[ComputeExpression] = []
    for field in schema:
        names.append(field.name)
        exprs.append(
            column_or_null_expr(
                field.name,
                field.type,
                available=available,
                cast=True,
                safe=False,
            )
        )
    if keep_extra_columns:
        for name in plan.schema(ctx=ctx).names:
            if name in names:
                continue
            names.append(name)
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def plan_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    """Return a plan for tables, readers, or dataset-backed sources.

    Returns
    -------
    Plan
        Acero-backed plan for dataset/table sources.
    """
    return plan_from_source(source, ctx=ctx, columns=columns, label=label)


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
    """Dictionary-encode specified string columns.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    specs = tuple(EncodingSpec(column=col) for col in columns)
    return encode_columns(table, specs=specs)


__all__ = [
    "PlanSource",
    "coalesce_expr",
    "column_or_null_expr",
    "encode_plan",
    "encode_table",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "flatten_struct_field",
    "plan_source",
    "project_columns",
    "project_to_schema",
    "query_for_schema",
]
