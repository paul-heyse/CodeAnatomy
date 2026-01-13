"""Plan-lane helper utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan_bundle
from arrowdsl.plan_helpers import (
    PlanSource,
    encoding_columns_from_metadata,
    encoding_projection,
    finalize_plan,
    finalize_plan_result,
    flatten_struct_field,
    plan_source,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.alignment import align_plan as align_plan_to_schema_helper


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
    return align_plan_to_schema_helper(
        plan,
        schema=schema,
        ctx=ctx,
        keep_extra_columns=keep_extra_columns,
    )


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
