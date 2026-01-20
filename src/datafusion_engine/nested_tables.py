"""Utilities for working with nested DataFusion tables."""

from __future__ import annotations

from contextlib import suppress

import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from datafusion_engine.query_fragments import SqlFragment
from ibis_engine.registry import datafusion_context


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
    resolved = coerce_table_like(table)
    if isinstance(resolved, RecordBatchReaderLike):
        resolved_table = resolved.read_all()
    else:
        resolved_table = resolved
    if not isinstance(resolved_table, pa.Table):
        return
    with suppress(KeyError, ValueError):
        ctx.deregister_table(name)
    ctx.register_record_batches(name, [resolved_table.to_batches()])


def materialize_sql_fragment(
    backend: BaseBackend | None,
    fragment: SqlFragment,
) -> pa.Table:
    """Materialize a SQL fragment into a pyarrow.Table via DataFusion."""
    if backend is None:
        msg = f"SQL fragment {fragment.name!r} requires an Ibis backend."
        raise ValueError(msg)
    ctx = datafusion_context(backend)
    if ctx is None:
        msg = f"SQL fragment {fragment.name!r} requires a DataFusion backend."
        raise ValueError(msg)
    batches = ctx.sql(fragment.sql).collect()
    return pa.Table.from_batches(batches)


__all__ = ["materialize_sql_fragment", "register_nested_table"]
