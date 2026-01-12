"""Quality capture helpers for invalid IDs in CPG tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.arrays import const_array
from cpg.plan_exprs import invalid_id_expr

type ValuesLike = ArrayLike | ChunkedArrayLike

QUALITY_SCHEMA = pa.schema(
    [
        pa.field("entity_kind", pa.string(), nullable=False),
        pa.field("entity_id", pa.string()),
        pa.field("issue", pa.string(), nullable=False),
        pa.field("source_table", pa.string()),
    ]
)


def empty_quality_table() -> TableLike:
    """Return an empty quality table.

    Returns
    -------
    TableLike
        Empty quality table with the canonical schema.
    """
    return pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in QUALITY_SCHEMA],
        schema=QUALITY_SCHEMA,
    )


@dataclass(frozen=True)
class QualityPlanSpec:
    """Specification for building quality plans from IDs."""

    id_col: str
    entity_kind: str
    issue: str
    source_table: str | None = None


def _is_zero(values: ValuesLike) -> ValuesLike:
    dtype = values.type
    if patypes.is_dictionary(dtype):
        values = pc.cast(values, pa.string(), safe=False)
        dtype = values.type
    if patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return pc.equal(values, pa.scalar("0"))
    if patypes.is_integer(dtype):
        return pc.equal(values, pa.scalar(0, type=dtype))
    if patypes.is_floating(dtype):
        return pc.equal(values, pa.scalar(0.0, type=dtype))
    return pc.equal(pc.cast(values, pa.string(), safe=False), pa.scalar("0"))


def _invalid_id_mask(values: ValuesLike) -> ValuesLike:
    return pc.or_(pc.is_null(values), _is_zero(values))


def _quality_table_from_ids(
    ids: ValuesLike,
    *,
    entity_kind: str,
    issue: str,
    source_table: str | None,
) -> TableLike:
    if ids.null_count != 0 or not patypes.is_string(ids.type):
        ids = pc.cast(ids, pa.string(), safe=False)
    n = len(ids)
    kind_arr = const_array(n, entity_kind, dtype=pa.string())
    issue_arr = const_array(n, issue, dtype=pa.string())
    if source_table is None:
        source_arr = pa.nulls(n, type=pa.string())
    else:
        source_arr = const_array(n, source_table, dtype=pa.string())
    return pa.Table.from_arrays(
        [kind_arr, ids, issue_arr, source_arr],
        schema=QUALITY_SCHEMA,
    )


def quality_from_ids(
    table: TableLike,
    *,
    id_col: str,
    entity_kind: str,
    issue: str,
    source_table: str | None = None,
) -> TableLike:
    """Return quality rows for invalid IDs in the specified column.

    Returns
    -------
    TableLike
        Quality rows for invalid identifiers.
    """
    if id_col not in table.column_names:
        if table.num_rows == 0:
            return empty_quality_table()
        ids = pa.nulls(table.num_rows, type=pa.string())
        return _quality_table_from_ids(
            ids, entity_kind=entity_kind, issue=issue, source_table=source_table
        )
    values = table[id_col]
    mask = _invalid_id_mask(values)
    invalid = table.filter(mask)
    if invalid.num_rows == 0:
        return empty_quality_table()
    return _quality_table_from_ids(
        invalid[id_col], entity_kind=entity_kind, issue=issue, source_table=source_table
    )


def concat_quality_tables(tables: Sequence[TableLike]) -> TableLike:
    """Concatenate quality tables, returning an empty table when none exist.

    Returns
    -------
    TableLike
        Concatenated quality table.
    """
    parts = [table for table in tables if table.num_rows]
    if not parts:
        return empty_quality_table()
    return pa.concat_tables(parts)


def quality_plan_from_ids(
    plan: Plan,
    *,
    spec: QualityPlanSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Return a plan producing quality rows for invalid IDs.

    Returns
    -------
    Plan
        Plan emitting quality rows for invalid identifiers.
    """
    schema = plan.schema(ctx=ctx)
    available = set(schema.names)
    if spec.id_col in available:
        dtype = schema.field(spec.id_col).type
        id_expr = ensure_expression(pc.field(spec.id_col))
    else:
        dtype = pa.string()
        id_expr = ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))

    id_str = ensure_expression(pc.cast(id_expr, pa.string(), safe=False))
    invalid = invalid_id_expr(id_expr, dtype=dtype)
    issue_expr = ensure_expression(pc.cast(pc.scalar(spec.issue), pa.string(), safe=False))
    kind_expr = ensure_expression(pc.cast(pc.scalar(spec.entity_kind), pa.string(), safe=False))
    if spec.source_table is None:
        source_expr = ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False))
    else:
        source_expr = ensure_expression(
            pc.cast(pc.scalar(spec.source_table), pa.string(), safe=False)
        )
    filtered = plan.filter(invalid, ctx=ctx)
    exprs = [kind_expr, id_str, issue_expr, source_expr]
    names = ["entity_kind", "entity_id", "issue", "source_table"]
    return filtered.project(exprs, names, ctx=ctx)


__all__ = [
    "QUALITY_SCHEMA",
    "concat_quality_tables",
    "empty_quality_table",
    "quality_from_ids",
    "quality_plan_from_ids",
]
