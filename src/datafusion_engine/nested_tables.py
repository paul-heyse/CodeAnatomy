"""Utilities for working with nested DataFusion tables."""

from __future__ import annotations

from contextlib import suppress
from typing import Protocol, cast

import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from datafusion_engine.query_fragments import SqlFragment
from ibis_engine.registry import datafusion_context


class _DatafusionQuery(Protocol):
    def collect(self) -> list[pa.RecordBatch]:
        ...


class _DatafusionContext(Protocol):
    def deregister_table(self, name: str) -> None:
        ...

    def register_record_batches(self, name: str, batches: list[list[pa.RecordBatch]]) -> None:
        ...

    def sql(self, query: str) -> _DatafusionQuery:
        ...


def register_nested_table(
    backend: BaseBackend | None,
    *,
    name: str,
    table: TableLike | RecordBatchReaderLike | None,
) -> None:
    """Register a nested Arrow table in a DataFusion context, if available."""
    if backend is None or table is None:
        return
    ctx = datafusion_context(backend)
    if ctx is None:
        return
    df_ctx = cast("_DatafusionContext", ctx)
    resolved = coerce_table_like(table)
    if isinstance(resolved, RecordBatchReaderLike):
        resolved_table = resolved.read_all()
    else:
        resolved_table = resolved
    if not isinstance(resolved_table, pa.Table):
        return
    resolved_table = cast("pa.Table", resolved_table)
    with suppress(KeyError, ValueError):
        df_ctx.deregister_table(name)
    df_ctx.register_record_batches(name, [resolved_table.to_batches()])


def materialize_sql_fragment(
    backend: BaseBackend | None,
    fragment: SqlFragment,
) -> pa.Table:
    """Materialize a SQL fragment into a pyarrow table via DataFusion.

    Returns
    -------
    pa.Table
        Materialized table for the fragment.

    Raises
    ------
    ValueError
        Raised when an Ibis or DataFusion backend is unavailable.
    """
    if backend is None:
        msg = f"SQL fragment {fragment.name!r} requires an Ibis backend."
        raise ValueError(msg)
    ctx = datafusion_context(backend)
    if ctx is None:
        msg = f"SQL fragment {fragment.name!r} requires a DataFusion backend."
        raise ValueError(msg)
    df_ctx = cast("_DatafusionContext", ctx)
    batches = df_ctx.sql(fragment.sql).collect()
    return pa.Table.from_batches(batches)


__all__ = ["materialize_sql_fragment", "register_nested_table"]
