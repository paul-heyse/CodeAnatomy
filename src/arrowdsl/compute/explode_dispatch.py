"""Explode dispatch helpers across execution lanes."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast

import ibis
from ibis.expr.types import Table as IbisTable

from arrowdsl.compute.expr_core import ExplodeSpec
from arrowdsl.compute.kernels import resolve_kernel
from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import TableLike, coerce_table_like

try:
    from datafusion.dataframe import DataFrame as DataFusionDataFrame
except ImportError:  # pragma: no cover - optional dependency
    DataFusionDataFrame = None


@dataclass(frozen=True)
class ExplodeDispatchResult:
    """Normalized explode dispatch result."""

    output: TableLike | IbisTable
    lane: str


def explode_list_dispatch(
    table: object,
    *,
    spec: ExplodeSpec,
    ctx: ExecutionContext | None = None,
) -> ExplodeDispatchResult:
    """Explode list columns across available execution lanes.

    Returns
    -------
    ExplodeDispatchResult
        Exploded table and the lane identifier.

    Raises
    ------
    TypeError
        Raised when the input is not a supported table-like object.
    """
    if isinstance(table, IbisTable):
        return ExplodeDispatchResult(
            output=_explode_list_ibis(table, spec=spec),
            lane="ibis",
        )
    if DataFusionDataFrame is not None and isinstance(table, DataFusionDataFrame):
        result = _explode_list_table(
            cast("TableLike", table.to_arrow_table()),
            spec=spec,
            ctx=ctx,
        )
        return ExplodeDispatchResult(output=result, lane="datafusion")
    try:
        coerced = coerce_table_like(table)
    except TypeError as exc:
        msg = f"Unsupported explode input: {type(table)}."
        raise TypeError(msg) from exc
    resolved = _resolve_table(coerced)
    result = _explode_list_table(resolved, spec=spec, ctx=ctx)
    return ExplodeDispatchResult(output=result, lane="kernel")


def _explode_list_table(
    table: TableLike,
    *,
    spec: ExplodeSpec,
    ctx: ExecutionContext | None,
) -> TableLike:
    context = ctx or execution_context_factory("default")
    kernel = resolve_kernel("explode_list", ctx=context)
    return kernel(table, spec=spec)


def _resolve_table(value: TableLike | object) -> TableLike:
    reader = getattr(value, "read_all", None)
    if callable(reader):
        return cast("TableLike", reader())
    return cast("TableLike", value)


def _explode_list_ibis(table: IbisTable, *, spec: ExplodeSpec) -> IbisTable:
    if spec.list_col not in table.columns:
        return table
    exploded = _stable_unnest(table, spec)
    renames: dict[str, str] = {}
    if spec.list_col != spec.value_col:
        renames[spec.list_col] = spec.value_col
    if renames:
        exploded = exploded.rename(renames)
    output_cols = list(spec.parent_keys)
    output_cols.append(spec.value_col)
    if spec.idx_col is not None:
        output_cols.append(spec.idx_col)
    selected = [name for name in output_cols if name in exploded.columns]
    return exploded.select(*selected) if selected else exploded


def _stable_unnest(table: IbisTable, spec: ExplodeSpec) -> IbisTable:
    row_number_col = _unique_name("row_number", table.columns)
    indexed = table.mutate(**{row_number_col: ibis.row_number()})
    exploded = indexed.unnest(
        spec.list_col,
        offset=spec.idx_col,
        keep_empty=spec.keep_empty,
    )
    order_cols = [exploded[key] for key in spec.parent_keys if key in exploded.columns]
    order_cols.append(exploded[row_number_col])
    if spec.idx_col is not None and spec.idx_col in exploded.columns:
        order_cols.append(exploded[spec.idx_col])
    ordered = exploded.order_by(order_cols) if order_cols else exploded
    return ordered.drop(row_number_col)


def _unique_name(base: str, existing: Iterable[str]) -> str:
    existing_set = set(existing)
    if base not in existing_set:
        return base
    idx = 1
    while True:
        candidate = f"{base}_{idx}"
        if candidate not in existing_set:
            return candidate
        idx += 1


__all__ = ["ExplodeDispatchResult", "explode_list_dispatch"]
