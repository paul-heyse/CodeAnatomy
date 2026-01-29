"""Utilities for working with nested DataFusion tables."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.arrow_interop import RecordBatchReaderLike, TableLike, coerce_table_like


@dataclass(frozen=True)
class ViewReference:
    """Reference a DataFusion-registered view by name."""

    name: str


class _DatafusionContext(Protocol):
    def deregister_table(self, name: str) -> None: ...


def register_nested_table(
    ctx: SessionContext | None,
    *,
    name: str,
    table: TableLike | RecordBatchReaderLike | None,
) -> None:
    """Register a nested Arrow table in a DataFusion context, if available."""
    if ctx is None or table is None:
        return
    df_ctx = cast("_DatafusionContext", ctx)
    resolved = coerce_table_like(table, requested_schema=None)
    if isinstance(resolved, RecordBatchReaderLike):
        resolved_table = resolved.read_all()
    else:
        resolved_table = resolved
    if not isinstance(resolved_table, pa.Table):
        return
    resolved_table = cast("pa.Table", resolved_table)
    with suppress(KeyError, ValueError):
        df_ctx.deregister_table(name)
    from_arrow = getattr(ctx, "from_arrow", None)
    if not callable(from_arrow):
        return
    from_arrow(resolved_table, name=name)


def materialize_view_reference(
    ctx: SessionContext | None,
    view: ViewReference,
) -> pa.Table:
    """Materialize a DataFusion view into a pyarrow table.

    Returns
    -------
    pa.Table
        Materialized table for the fragment.

    Raises
    ------
    ValueError
        Raised when a DataFusion session context is unavailable.
    """
    if ctx is None:
        msg = f"View {view.name!r} requires a DataFusion session context."
        raise ValueError(msg)
    df = ctx.table(view.name)
    batches = df.collect()
    return pa.Table.from_batches(batches)


__all__ = ["ViewReference", "materialize_view_reference", "register_nested_table"]
