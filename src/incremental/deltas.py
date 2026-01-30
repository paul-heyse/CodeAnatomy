"""Delta utilities for incremental export changes."""

from __future__ import annotations

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.arrow.build import empty_table
from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.schema.policy import align_table
from incremental.delta_context import DeltaAccessContext, register_delta_df
from incremental.plan_bundle_exec import execute_df_to_table
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from utils.uuid_factory import uuid7_hex
from utils.validation import ensure_table


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
    result = _compute_changed_exports_table(
        runtime,
        prev_exports=prev_exports,
        curr_exports=curr_exports,
        changed_files=changed_files,
    )
    return align_table(result, schema=schema, safe_cast=True)


def _register_prev_exports(
    *,
    registry: TempTableRegistry,
    runtime: IncrementalRuntime,
    prev_exports: str | None,
) -> str:
    if prev_exports is None:
        empty = empty_table(dataset_schema("dim_exported_defs_v1"))
        return registry.register_table(empty, prefix="prev_exports")
    name = f"__incremental_prev_exports_{uuid7_hex()}"
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


def _compute_changed_exports_table(
    runtime: IncrementalRuntime,
    *,
    prev_exports: str | None,
    curr_exports: TableLike,
    changed_files: TableLike,
) -> pa.Table:
    curr_table = ensure_table(curr_exports, label="curr_exports")
    key_cols = _export_key_columns(curr_table.schema)
    with TempTableRegistry(runtime) as registry:
        curr_df, prev_df, changed_df = _prefixed_sources(
            runtime,
            registry=registry,
            prev_exports=prev_exports,
            curr_table=curr_table,
            changed_table=ensure_table(changed_files, label="changed_files"),
        )
        added_df = _added_exports_df(curr_df, prev_df, changed_df, key_cols=key_cols)
        removed_df = _removed_exports_df(curr_df, prev_df, changed_df, key_cols=key_cols)
        combined = added_df.union(removed_df)
        return execute_df_to_table(runtime, combined, view_name="incremental_changed_exports")


def _prefixed_sources(
    runtime: IncrementalRuntime,
    *,
    registry: TempTableRegistry,
    prev_exports: str | None,
    curr_table: pa.Table,
    changed_table: pa.Table,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    ctx = runtime.session_runtime().ctx
    curr_name = registry.register_table(curr_table, prefix="curr_exports")
    changed_name = registry.register_table(changed_table, prefix="changed_files")
    prev_name = _register_prev_exports(
        registry=registry,
        runtime=runtime,
        prev_exports=prev_exports,
    )
    curr_df = _prefixed_df(ctx.table(curr_name), prefix="curr")
    prev_df = _prefixed_df(ctx.table(prev_name), prefix="prev")
    changed_df = _prefixed_df(ctx.table(changed_name).select(col("file_id")), prefix="changed")
    return curr_df, prev_df, changed_df


def _added_exports_df(
    curr_df: DataFrame,
    prev_df: DataFrame,
    changed_df: DataFrame,
    *,
    key_cols: list[str],
) -> DataFrame:
    curr_changed = curr_df.join(
        changed_df,
        join_keys=(["curr_file_id"], ["changed_file_id"]),
        how="inner",
    )
    added_joined = curr_changed.join(
        prev_df,
        join_keys=(_join_keys(key_cols, prefix="curr"), _join_keys(key_cols, prefix="prev")),
        how="left",
    )
    added = added_joined.filter(col("prev_file_id").is_null())
    return added.select(
        lit("added").alias("delta_kind"),
        col("curr_file_id").alias("file_id"),
        col("curr_path").alias("path"),
        col("curr_qname_id").alias("qname_id"),
        col("curr_qname").alias("qname"),
        _symbol_expr(key_cols, prefix="curr").alias("symbol"),
    )


def _removed_exports_df(
    curr_df: DataFrame,
    prev_df: DataFrame,
    changed_df: DataFrame,
    *,
    key_cols: list[str],
) -> DataFrame:
    prev_changed = prev_df.join(
        changed_df,
        join_keys=(["prev_file_id"], ["changed_file_id"]),
        how="inner",
    )
    removed_joined = prev_changed.join(
        curr_df,
        join_keys=(_join_keys(key_cols, prefix="prev"), _join_keys(key_cols, prefix="curr")),
        how="left",
    )
    removed = removed_joined.filter(col("curr_file_id").is_null())
    return removed.select(
        lit("removed").alias("delta_kind"),
        col("prev_file_id").alias("file_id"),
        col("prev_path").alias("path"),
        col("prev_qname_id").alias("qname_id"),
        col("prev_qname").alias("qname"),
        _symbol_expr(key_cols, prefix="prev").alias("symbol"),
    )


def _join_keys(cols: list[str], *, prefix: str) -> list[str]:
    return [f"{prefix}_{col_name}" for col_name in cols]


def _symbol_expr(cols: list[str], *, prefix: str) -> Expr:
    if "symbol" in cols:
        return col(f"{prefix}_symbol")
    return f.arrow_cast(lit(None), lit("Utf8"))


__all__ = ["compute_changed_exports"]
