"""Plan-lane helper utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.plan.plan import Plan, PlanSpec
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


def apply_query_spec(
    plan: Plan,
    *,
    spec: QuerySpec,
    ctx: ExecutionContext,
    provenance: bool = False,
) -> Plan:
    """Apply QuerySpec filters and projections to a plan.

    Returns
    -------
    Plan
        Updated plan with filters/projections applied.
    """
    predicate = spec.predicate_expression()
    if predicate is not None:
        plan = plan.filter(predicate, ctx=ctx)
    cols = spec.scan_columns(provenance=provenance)
    if isinstance(cols, Mapping):
        names = list(cols.keys())
        expressions = list(cols.values())
    else:
        names = list(cols)
        expressions = [pc.field(name) for name in cols]
    return plan.project(expressions, names, ctx=ctx)


def append_projection(
    plan: Plan,
    *,
    base: Sequence[str],
    extras: Sequence[tuple[ComputeExpression, str]],
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Append extra expressions to a projection of base columns.

    Returns
    -------
    Plan
        Plan with appended projection expressions.
    """
    expressions: list[ComputeExpression] = [pc.field(name) for name in base]
    names = list(base)
    for expr, name in extras:
        expressions.append(expr)
        names.append(name)
    return plan.project(expressions, names, ctx=ctx)


def rename_plan_columns(
    plan: Plan,
    *,
    columns: Sequence[str],
    rename: Mapping[str, str],
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Rename columns via a plan projection.

    Returns
    -------
    Plan
        Plan with renamed columns.
    """
    names = [rename.get(name, name) for name in columns]
    expressions = [pc.field(name) for name in columns]
    return plan.project(expressions, names, ctx=ctx)


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


def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    """Flatten a struct field into child fields with parent-name prefixes.

    Returns
    -------
    list[pa.Field]
        Flattened fields with parent-name prefixes.
    """
    return list(field.flatten())


def materialize_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    """Materialize a plan as a table.

    Returns
    -------
    TableLike
        Materialized table.
    """
    return PlanSpec.from_plan(plan).to_table(ctx=ctx)


def stream_plan(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)


def finalize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when allowed, otherwise a table.
    """
    spec = PlanSpec.from_plan(plan)
    if prefer_reader and not spec.pipeline_breakers:
        return spec.to_reader(ctx=ctx)
    return spec.to_table(ctx=ctx)


def finalize_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Finalize a bundle of plans into tables or readers.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Finalized plan outputs keyed by name.
    """
    return {
        name: finalize_plan(plan, ctx=ctx, prefer_reader=prefer_reader)
        for name, plan in plans.items()
    }


__all__ = [
    "append_projection",
    "apply_query_spec",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "finalize_plan",
    "finalize_plan_bundle",
    "flatten_struct_field",
    "materialize_plan",
    "query_for_schema",
    "rename_plan_columns",
    "stream_plan",
]
