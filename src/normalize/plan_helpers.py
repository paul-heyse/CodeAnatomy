"""Plan-lane helper utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan_helpers import (
    append_projection,
    apply_query_spec,
    flatten_struct_field,
    query_for_schema,
    rename_plan_columns,
)
from arrowdsl.schema.schema import encode_expression


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
