"""Exported definition index builder for incremental impact analysis."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from arrow_utils.core.interop import TableLike, coerce_table_like
from arrow_utils.schema.build import table_from_arrays
from datafusion_engine.schema_alignment import align_table
from datafusion_ext import prefixed_hash_parts64 as prefixed_hash64
from incremental.plan_bundle_exec import execute_df_to_table
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime, TempTableRegistry


def build_exported_defs_index(
    cst_defs_norm: TableLike,
    *,
    runtime: IncrementalRuntime,
    rel_def_symbol: TableLike | None = None,
) -> pa.Table:
    """Build the exported definitions index for top-level defs.

    Returns
    -------
    pa.Table
        Exported definitions table with qname and optional symbol bindings.
    """
    schema = dataset_schema("dim_exported_defs_v1")
    cst_table = coerce_table_like(cst_defs_norm, requested_schema=None)
    schema_names = cast("Sequence[str]", cst_table.schema.names)
    if "qnames" not in schema_names:
        return _empty_exported_defs(schema)
    with TempTableRegistry(runtime) as registry:
        cst_name = registry.register_table(cst_table, prefix="cst_defs_norm")
        rel_name = None
        if rel_def_symbol is not None:
            rel_table = coerce_table_like(rel_def_symbol, requested_schema=None)
            rel_name = registry.register_table(rel_table, prefix="rel_def_symbol")
        result = _build_exported_defs_base(
            runtime,
            cst_table_name=cst_name,
            rel_table_name=rel_name,
            has_container_def_id="container_def_id" in schema_names,
        )
    return align_table(result, schema=schema, safe_cast=True)


def _build_exported_defs_base(
    runtime: IncrementalRuntime,
    *,
    cst_table_name: str,
    rel_table_name: str | None,
    has_container_def_id: bool,
) -> pa.Table:
    ctx = runtime.session_runtime().ctx
    base_df = ctx.table(cst_table_name)
    if has_container_def_id:
        base_df = base_df.filter(col("container_def_id").is_null())
    symbol_expr = f.arrow_cast(lit(None), lit("Utf8"))
    symbol_roles_expr = f.arrow_cast(lit(None), lit("Int32"))
    if rel_table_name is not None:
        rel_df = ctx.table(rel_table_name).select(
            col("def_id").alias("rel_def_id"),
            col("path").alias("rel_path"),
            col("symbol").alias("rel_symbol"),
            col("symbol_roles").alias("rel_symbol_roles"),
        )
        base_df = base_df.join(
            rel_df,
            join_keys=(["def_id", "path"], ["rel_def_id", "rel_path"]),
            how="left",
        )
        symbol_expr = col("rel_symbol")
        symbol_roles_expr = col("rel_symbol_roles")
    df = base_df.unnest_columns("qnames", preserve_nulls=False)
    qname_expr = col("qnames")
    qname_name = qname_expr["name"]
    qname_source = qname_expr["source"]
    df = df.select(
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        f.coalesce(col("def_kind_norm"), col("kind")).alias("def_kind_norm"),
        col("name").alias("name"),
        prefixed_hash64("qname", qname_name).alias("qname_id"),
        qname_name.alias("qname"),
        qname_source.alias("qname_source"),
        symbol_expr.alias("symbol"),
        symbol_roles_expr.alias("symbol_roles"),
    )
    table = execute_df_to_table(runtime, df, view_name="incremental_exported_defs")
    if table.num_rows == 0:
        return _empty_exported_defs(dataset_schema("dim_exported_defs_v1"))
    return table


def _empty_exported_defs(schema: pa.Schema) -> pa.Table:
    return table_from_arrays(schema, columns={}, num_rows=0)


__all__ = ["build_exported_defs_index"]
