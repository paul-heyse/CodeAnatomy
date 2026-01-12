"""Shared plan-lane helper utilities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.core.context import ExecutionContext, Ordering, OrderingEffect
from arrowdsl.core.interop import (
    ComputeExpression,
    DataTypeLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.plan.ops import ScanOp
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.schema import encode_expression


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


def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    """Flatten a struct field into child fields with parent-name prefixes.

    Returns
    -------
    list[pyarrow.Field]
        Flattened fields with parent-name prefixes.
    """
    return list(field.flatten())


PlanSource = TableLike | RecordBatchReaderLike | ds.Dataset | ds.Scanner | Plan


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
    if isinstance(source, Plan):
        return source
    if isinstance(source, ds.Dataset):
        available = set(source.schema.names)
        scan_cols = list(columns) if columns is not None else list(source.schema.names)
        scan_cols = [name for name in scan_cols if name in available]
        scan_ordering = (
            OrderingEffect.IMPLICIT
            if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
            else OrderingEffect.UNORDERED
        )
        scan_op = ScanOp(
            dataset=source,
            columns=scan_cols,
            predicate=None,
            ordering_effect=scan_ordering,
        )
        decl = scan_op.to_declaration([], ctx=ctx)
        ordering = scan_op.apply_ordering(Ordering.unordered())
        return Plan(decl=decl, label=label, ordering=ordering, pipeline_breakers=())
    if isinstance(source, ds.Scanner):
        scanner = cast("ds.Scanner", source)
        return Plan.table_source(scanner.to_table(), label=label)
    if isinstance(source, RecordBatchReaderLike):
        return Plan.table_source(source.read_all(), label=label)
    return Plan.table_source(source, label=label)


__all__ = [
    "PlanSource",
    "coalesce_expr",
    "column_or_null_expr",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "flatten_struct_field",
    "plan_source",
    "project_columns",
    "query_for_schema",
]
