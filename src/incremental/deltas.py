"""Delta utilities for incremental export changes."""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from datafusion import SessionContext, col, functions as f, lit
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import align_table
from incremental.delta_context import DeltaAccessContext, register_delta_df
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime, TempTableRegistry


def compute_changed_exports(
    *,
    runtime: IncrementalRuntime,
    prev_exports: str | None,
    curr_exports: TableLike,
    changed_files: TableLike,
) -> pa.Table:
    """Compute export deltas for changed files.

    Returns
    -------
    pa.Table
        Export delta table with added/removed rows.
    """
    schema = dataset_schema("inc_changed_exports_v1")
    curr_table = _ensure_table(curr_exports)
    changed_table = _ensure_table(changed_files)
    key_cols = _export_key_columns(curr_table.schema)
    with TempTableRegistry(runtime) as registry:
        ctx = runtime.session_context()
        curr_name = registry.register_table(curr_table, prefix="curr_exports")
        changed_name = registry.register_table(changed_table, prefix="changed_files")
        prev_name = _register_prev_exports(
            ctx,
            registry=registry,
            runtime=runtime,
            prev_exports=prev_exports,
        )
        curr_df = _prefixed_df(ctx.table(curr_name), prefix="curr")
        prev_df = _prefixed_df(ctx.table(prev_name), prefix="prev")
        changed_df = _prefixed_df(
            ctx.table(changed_name).select(col("file_id")),
            prefix="changed",
        )
        curr_changed = curr_df.join(
            changed_df,
            join_keys=(["curr_file_id"], ["changed_file_id"]),
            how="inner",
        )
        join_keys_left = [f"curr_{col_name}" for col_name in key_cols]
        join_keys_right = [f"prev_{col_name}" for col_name in key_cols]
        added_joined = curr_changed.join(
            prev_df,
            join_keys=(join_keys_left, join_keys_right),
            how="left",
        )
        added = added_joined.filter(col("prev_file_id").is_null())
        added_symbol = (
            col("curr_symbol")
            if "symbol" in key_cols
            else f.arrow_cast(lit(None), lit("Utf8"))
        )
        added_df = added.select(
            lit("added").alias("delta_kind"),
            col("curr_file_id").alias("file_id"),
            col("curr_path").alias("path"),
            col("curr_qname_id").alias("qname_id"),
            col("curr_qname").alias("qname"),
            added_symbol.alias("symbol"),
        )
        prev_changed = prev_df.join(
            changed_df,
            join_keys=(["prev_file_id"], ["changed_file_id"]),
            how="inner",
        )
        join_keys_left = [f"prev_{col_name}" for col_name in key_cols]
        join_keys_right = [f"curr_{col_name}" for col_name in key_cols]
        removed_joined = prev_changed.join(
            curr_df,
            join_keys=(join_keys_left, join_keys_right),
            how="left",
        )
        removed = removed_joined.filter(col("curr_file_id").is_null())
        removed_symbol = (
            col("prev_symbol")
            if "symbol" in key_cols
            else f.arrow_cast(lit(None), lit("Utf8"))
        )
        removed_df = removed.select(
            lit("removed").alias("delta_kind"),
            col("prev_file_id").alias("file_id"),
            col("prev_path").alias("path"),
            col("prev_qname_id").alias("qname_id"),
            col("prev_qname").alias("qname"),
            removed_symbol.alias("symbol"),
        )
        result = added_df.union(removed_df).to_arrow_table()
    return align_table(result, schema=schema, safe_cast=True)


def _register_prev_exports(
    ctx: SessionContext,
    *,
    registry: TempTableRegistry,
    runtime: IncrementalRuntime,
    prev_exports: str | None,
) -> str:
    if prev_exports is None:
        empty = table_from_arrays(dataset_schema("dim_exported_defs_v1"), columns={}, num_rows=0)
        return registry.register_table(empty, prefix="prev_exports")
    name = f"__incremental_prev_exports_{uuid4().hex}"
    register_delta_df(
        DeltaAccessContext(runtime=runtime),
        path=prev_exports,
        name=name,
    )
    registry.track(name)
    return name


def _export_key_columns(schema: pa.Schema) -> list[str]:
    key_cols = ["file_id", "qname_id"]
    if "symbol" in schema.names:
        key_cols.append("symbol")
    return key_cols


def _prefixed_df(df: DataFrame, *, prefix: str) -> DataFrame:
    names = df.schema().names
    return df.select(*(col(name).alias(f"{prefix}_{name}") for name in names))


def _ensure_table(value: TableLike) -> pa.Table:
    resolved = coerce_table_like(value, requested_schema=None)
    if isinstance(resolved, RecordBatchReaderLike):
        resolved = resolved.read_all()
    if isinstance(resolved, pa.Table):
        return resolved
    return pa.Table.from_pydict(resolved.to_pydict())


__all__ = ["compute_changed_exports"]
