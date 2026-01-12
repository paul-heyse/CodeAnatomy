"""Plan-lane helper utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike, pc
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan_helpers import (
    PlanSource,
    encoding_columns_from_metadata,
    encoding_projection,
    flatten_struct_field,
    plan_source,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.schema import SchemaTransform, projection_for_schema


@dataclass(frozen=True)
class PlanOutput:
    """Plan output bundled with materialization metadata."""

    value: TableLike | RecordBatchReaderLike
    kind: Literal["reader", "table"]


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


def align_plan_to_schema(
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
        Plan projecting/casting to match the schema.
    """
    available = plan.schema(ctx=ctx).names
    exprs, names = projection_for_schema(schema, available=available, safe_cast=ctx.safe_cast)
    if keep_extra_columns:
        seen = set(names)
        for name in available:
            if name in seen:
                continue
            exprs.append(pc.field(name))
            names.append(name)
    return plan.project(exprs, names, ctx=ctx)


def _align_table_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool,
) -> TableLike:
    transform = SchemaTransform(
        schema=schema,
        safe_cast=ctx.safe_cast,
        keep_extra_columns=keep_extra_columns,
        on_error="unsafe" if ctx.safe_cast else "raise",
    )
    aligned, _ = transform.apply_with_info(table)
    return aligned


def finalize_plan_result(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    schema: SchemaLike | None = None,
    keep_extra_columns: bool = False,
) -> PlanOutput:
    """Return a reader or table plus materialization metadata.

    Returns
    -------
    PlanOutput
        Plan output and the materialization kind.
    """
    if schema is not None:
        if plan.decl is None:
            aligned = _align_table_to_schema(
                PlanSpec.from_plan(plan).to_table(ctx=ctx),
                schema=schema,
                ctx=ctx,
                keep_extra_columns=keep_extra_columns,
            )
            return PlanOutput(value=aligned, kind="table")
        plan = align_plan_to_schema(
            plan,
            schema=schema,
            ctx=ctx,
            keep_extra_columns=keep_extra_columns,
        )

    spec = PlanSpec.from_plan(plan)
    if prefer_reader and not spec.pipeline_breakers:
        return PlanOutput(value=spec.to_reader(ctx=ctx), kind="reader")
    return PlanOutput(value=spec.to_table(ctx=ctx), kind="table")


def finalize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    schema: SchemaLike | None = None,
    keep_extra_columns: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when allowed, otherwise a table.
    """
    return finalize_plan_result(
        plan,
        ctx=ctx,
        prefer_reader=prefer_reader,
        schema=schema,
        keep_extra_columns=keep_extra_columns,
    ).value


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
    "PlanOutput",
    "PlanSource",
    "align_plan_to_schema",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "finalize_plan",
    "finalize_plan_bundle",
    "finalize_plan_result",
    "flatten_struct_field",
    "materialize_plan",
    "plan_source",
    "project_columns",
    "query_for_schema",
    "stream_plan",
]
