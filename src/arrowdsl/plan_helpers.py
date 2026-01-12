"""Shared plan-lane helper utilities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, SchemaLike, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec


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


def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    """Flatten a struct field into child fields with parent-name prefixes.

    Returns
    -------
    list[pyarrow.Field]
        Flattened fields with parent-name prefixes.
    """
    return list(field.flatten())


__all__ = [
    "append_projection",
    "apply_query_spec",
    "flatten_struct_field",
    "query_for_schema",
    "rename_plan_columns",
]
