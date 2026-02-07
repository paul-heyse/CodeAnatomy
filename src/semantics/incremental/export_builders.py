"""DataFusion builders for exported definitions index views."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.arrow.build import empty_table
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.udf.expr import udf_expr
from semantics.catalog.dataset_specs import dataset_spec
from semantics.types.annotated_schema import AnnotatedSchema
from semantics.types.core import SemanticType

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


def _empty_exported_defs(ctx: SessionContext) -> DataFrame:
    from schema_spec.dataset_spec_ops import dataset_spec_schema

    schema_like = dataset_spec_schema(dataset_spec("dim_exported_defs"))
    schema = cast("pa.Schema", schema_like)
    table = cast("pa.Table", empty_table(schema))
    return ctx.from_arrow(table)


def exported_defs_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame of exported definitions for incremental analysis.

    Returns:
    -------
    DataFrame
        Exported definitions DataFrame aligned to dim_exported_defs.
    """
    base_df = ctx.table("cst_defs_norm")
    schema_names = cast("tuple[str, ...]", base_df.schema().names)
    if "qnames" not in schema_names:
        return _empty_exported_defs(ctx)

    if "container_def_id" in schema_names:
        base_df = base_df.filter(col("container_def_id").is_null())

    symbol_expr = safe_cast(lit(None), "Utf8")
    symbol_roles_expr = safe_cast(lit(None), "Int32")
    if ctx.table_exist("rel_def_symbol"):
        rel_source_df = ctx.table("rel_def_symbol")
        rel_annotated = AnnotatedSchema.from_arrow_schema(rel_source_df.schema())
        rel_schema_names = set(rel_annotated.column_names)
        rel_symbol_expr = (
            col("symbol").alias("rel_symbol")
            if rel_annotated.has_semantic_type(SemanticType.SYMBOL)
            else safe_cast(lit(None), "Utf8").alias("rel_symbol")
        )
        rel_symbol_roles_expr = (
            col("symbol_roles").alias("rel_symbol_roles")
            if "symbol_roles" in rel_schema_names
            else safe_cast(lit(None), "Int32").alias("rel_symbol_roles")
        )
        rel_df = rel_source_df.select(
            col("entity_id").alias("rel_def_id"),
            col("path").alias("rel_path"),
            rel_symbol_expr,
            rel_symbol_roles_expr,
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
    def_kind_expr = col("kind")
    if "def_kind_norm" in schema_names:
        def_kind_expr = f.coalesce(col("def_kind_norm"), col("kind"))
    return df.select(
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        def_kind_expr.alias("def_kind_norm"),
        col("name").alias("name"),
        udf_expr("stable_id_parts", "qname", qname_name).alias("qname_id"),
        qname_name.alias("qname"),
        qname_source.alias("qname_source"),
        symbol_expr.alias("symbol"),
        symbol_roles_expr.alias("symbol_roles"),
    )


__all__ = ["exported_defs_df_builder"]
