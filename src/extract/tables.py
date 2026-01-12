"""Shared table construction helpers for extractors."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import (
    flatten_struct_field,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.schema import (
    SchemaTransform,
    projection_for_schema,
)


def align_table(table: TableLike, *, schema: SchemaLike) -> TableLike:
    """Align a table to a target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    return SchemaTransform(schema=schema).apply(table)


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    available: Sequence[str] | None = None,
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Return a plan aligned to the target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting columns to the schema.
    """
    if available is None:
        available = schema.names if ctx is None else plan.schema(ctx=ctx).names
    safe_cast = True if ctx is None else ctx.safe_cast
    exprs, names = projection_for_schema(schema, available=available, safe_cast=safe_cast)
    return plan.project(exprs, names, ctx=ctx)


__all__ = [
    "align_plan",
    "align_table",
    "flatten_struct_field",
    "project_columns",
    "query_for_schema",
]
