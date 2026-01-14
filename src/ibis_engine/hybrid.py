"""Hybrid helpers bridging Ibis execution and Arrow kernel operations."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.compute.expr_core import ExprSpec
from arrowdsl.compute.filters import FilterSpec
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import set_or_append_column
from ibis_engine.query_bridge import QueryBridgeResult


def apply_derived_fields(table: TableLike, derived: Mapping[str, ExprSpec]) -> TableLike:
    """Apply derived ExprSpec columns to a table.

    Returns
    -------
    TableLike
        Table with derived columns appended or updated.
    """
    for name, expr in derived.items():
        values = expr.materialize(table)
        table = set_or_append_column(table, name, values)
    return table


def apply_filter_spec(table: TableLike, predicate: ExprSpec) -> TableLike:
    """Filter a table using an ExprSpec predicate.

    Returns
    -------
    TableLike
        Filtered table.
    """
    mask = FilterSpec(predicate).mask(table)
    return table.filter(mask)


def apply_query_bridge_fallback(table: TableLike, bridge: QueryBridgeResult) -> TableLike:
    """Apply kernel-lane fallbacks from a QueryBridgeResult.

    Returns
    -------
    TableLike
        Table after applying kernel-derived fields and predicates.
    """
    out = apply_derived_fields(table, bridge.kernel_derived)
    if bridge.kernel_pushdown is not None:
        out = apply_filter_spec(out, bridge.kernel_pushdown)
    if bridge.kernel_predicate is not None:
        out = apply_filter_spec(out, bridge.kernel_predicate)
    return out


__all__ = ["apply_derived_fields", "apply_filter_spec", "apply_query_bridge_fallback"]
