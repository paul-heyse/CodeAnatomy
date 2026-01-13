"""Shared helpers for building extract plans."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.scan_io import plan_from_rows
from arrowdsl.schema.schema import empty_table
from extract.registry_specs import dataset_query, dataset_schema
from extract.schema_ops import ExtractNormalizeOptions, normalize_extract_plan


def empty_plan_for_dataset(name: str) -> Plan:
    """Return an empty plan for a dataset name.

    Returns
    -------
    Plan
        Plan sourcing an empty table for the dataset.
    """
    return Plan.table_source(empty_table(dataset_schema(name)))


def apply_query_and_normalize(
    name: str,
    plan: Plan,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
) -> Plan:
    """Apply the dataset query and normalize plan alignment.

    Returns
    -------
    Plan
        Query-filtered and normalized plan.
    """
    plan = dataset_query(name).apply_to_plan(plan, ctx=ctx)
    return normalize_extract_plan(name, plan, ctx=ctx, normalize=normalize)


def plan_from_rows_for_dataset(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    row_schema: SchemaLike,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
) -> Plan:
    """Build a plan from row data with dataset query and normalization.

    Returns
    -------
    Plan
        Plan for the dataset rows with query + normalization applied.
    """
    plan = plan_from_rows(rows, schema=row_schema, label=name)
    return apply_query_and_normalize(
        name,
        plan,
        ctx=ctx,
        normalize=normalize,
    )


__all__ = [
    "apply_query_and_normalize",
    "empty_plan_for_dataset",
    "plan_from_rows_for_dataset",
]
