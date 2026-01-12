"""Shared hashing helpers for extractors."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import arrowdsl.core.interop as pa
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import HashSpec, hash_column_values, hash_expression
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.arrays import set_or_append_column


def valid_mask(table: TableLike, cols: Sequence[str]) -> ArrayLike:
    """Return a validity mask for the provided columns.

    Returns
    -------
    ArrayLike
        Boolean mask of rows with all columns valid.
    """
    mask = pc.is_valid(table[cols[0]])
    for col in cols[1:]:
        mask = pc.and_(mask, pc.is_valid(table[col]))
    return mask


def apply_hash_column(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> TableLike:
    """Hash columns into a new id column.

    Returns
    -------
    TableLike
        Updated table with hashed id column.
    """
    hashed = hash_column_values(table, spec=spec)
    out_col = spec.out_col or f"{spec.prefix}_id"
    if required:
        mask = valid_mask(table, required)
        hashed = pc.if_else(mask, hashed, pa.scalar(None, type=hashed.type))
    return set_or_append_column(table, out_col, hashed)


def apply_hash_columns(
    table: TableLike,
    *,
    specs: Sequence[HashSpec],
    required: Mapping[str, Sequence[str]] | None = None,
) -> TableLike:
    """Apply multiple hash specs to a table.

    Returns
    -------
    TableLike
        Updated table with hashed id columns.
    """
    out = table
    for spec in specs:
        out_col = spec.out_col or f"{spec.prefix}_id"
        req = required.get(out_col) if required else None
        out = apply_hash_column(out, spec=spec, required=req)
    return out


def valid_mask_expression(cols: Sequence[str]) -> ComputeExpression:
    """Return a validity mask expression for the provided columns.

    Returns
    -------
    ComputeExpression
        Boolean expression of rows with all columns valid.
    """
    mask = pc.is_valid(pc.field(cols[0]))
    for col in cols[1:]:
        mask = pc.and_(mask, pc.is_valid(pc.field(col)))
    return ensure_expression(mask)


def hash_projection(
    spec: HashSpec,
    *,
    available: Sequence[str] | None = None,
    required: Sequence[str] | None = None,
) -> tuple[ComputeExpression, str]:
    """Return a compute expression and output column for plan-lane hashing.

    Returns
    -------
    tuple[ComputeExpression, str]
        Hash expression and output column name.
    """
    expr = hash_expression(spec, available=available)
    if required:
        mask = valid_mask_expression(required)
        expr = ensure_expression(pc.if_else(mask, expr, pc.scalar(None)))
    out_col = spec.out_col or f"{spec.prefix}_id"
    return expr, out_col


def apply_hash_projection(
    plan: Plan,
    *,
    specs: Sequence[HashSpec],
    available: Sequence[str],
    required: Mapping[str, Sequence[str]] | None = None,
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Apply hash projections to a plan, appending hash ID columns.

    Returns
    -------
    Plan
        Plan with appended hash columns.
    """
    names = list(available)
    expr_map: dict[str, ComputeExpression] = {name: pc.field(name) for name in names}
    for spec in specs:
        out_col = spec.out_col or f"{spec.prefix}_id"
        req = required.get(out_col) if required else None
        expr, out_name = hash_projection(spec, available=names, required=req)
        expr_map[out_name] = expr
        if out_name not in names:
            names.append(out_name)
    expressions = [expr_map[name] for name in names]
    return plan.project(expressions, names, ctx=ctx)
