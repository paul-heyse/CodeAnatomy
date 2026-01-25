"""Utilities for working with nested DataFusion tables."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
from datafusion import SessionContext, SQLOptions
from ibis.backends import BaseBackend

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.schema_registry import has_schema, schema_for
from datafusion_engine.sql_options import sql_options_for_profile
from ibis_engine.registry import datafusion_context


@dataclass(frozen=True)
class ViewReference:
    """Reference a DataFusion-registered view by name."""

    name: str


class _DatafusionQuery(Protocol):
    def collect(self) -> list[pa.RecordBatch]: ...


class _DatafusionContext(Protocol):
    def deregister_table(self, name: str) -> None: ...

    def sql_with_options(self, query: str, options: SQLOptions) -> _DatafusionQuery: ...


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
    df_ctx = cast("_DatafusionContext", ctx)
    requested_schema = schema_for(name) if has_schema(name) else None
    resolved = coerce_table_like(table, requested_schema=requested_schema)
    if isinstance(resolved, RecordBatchReaderLike):
        resolved_table = resolved.read_all()
    else:
        resolved_table = resolved
    if not isinstance(resolved_table, pa.Table):
        return
    resolved_table = cast("pa.Table", resolved_table)
    with suppress(KeyError, ValueError):
        df_ctx.deregister_table(name)
    if not isinstance(ctx, SessionContext):
        return
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(name, [resolved_table.to_batches()])


def materialize_view_reference(
    backend: BaseBackend | None,
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
        Raised when an Ibis or DataFusion backend is unavailable.
    """
    if backend is None:
        msg = f"View {view.name!r} requires an Ibis backend."
        raise ValueError(msg)
    ctx = datafusion_context(backend)
    from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    sql_options = sql_options_for_profile(None)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    plan = facade.compile(
        f"SELECT * FROM {view.name}",
        options=DataFusionCompileOptions(
            sql_options=sql_options,
            sql_policy=DataFusionSqlPolicy(),
        ),
    )
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "Nested table materialization did not return a DataFusion DataFrame."
        raise ValueError(msg)
    batches = result.dataframe.collect()
    return pa.Table.from_batches(batches)


__all__ = ["ViewReference", "materialize_view_reference", "register_nested_table"]
