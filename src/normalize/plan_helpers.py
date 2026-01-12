"""Plan-lane helper utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import (
    PlanRunResult,
    run_plan,
    run_plan_bundle,
)
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
) -> PlanRunResult:
    """Return a reader or table plus materialization metadata.

    Returns
    -------
    PlanRunResult
        Plan output and the materialization kind.

    Raises
    ------
    TypeError
        Raised when run_plan returns a reader instead of a table.
    """
    if schema is not None:
        if plan.decl is None:
            result = run_plan(plan, ctx=ctx, prefer_reader=False)
            if isinstance(result.value, pa.RecordBatchReader):
                msg = "Expected table result from run_plan."
                raise TypeError(msg)
            aligned = _align_table_to_schema(
                cast("TableLike", result.value),
                schema=schema,
                ctx=ctx,
                keep_extra_columns=keep_extra_columns,
            )
            return PlanRunResult(value=aligned, kind="table")
        plan = align_plan_to_schema(
            plan,
            schema=schema,
            ctx=ctx,
            keep_extra_columns=keep_extra_columns,
        )

    return run_plan(plan, ctx=ctx, prefer_reader=prefer_reader)


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
    return run_plan_bundle(plans, ctx=ctx, prefer_reader=prefer_reader)


__all__ = [
    "PlanSource",
    "align_plan_to_schema",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "finalize_plan",
    "finalize_plan_bundle",
    "finalize_plan_result",
    "flatten_struct_field",
    "plan_source",
    "project_columns",
    "query_for_schema",
]
