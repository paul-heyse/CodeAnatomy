"""CFG block and edge analysis builders."""

from __future__ import annotations

from datafusion import SessionContext, col
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from semantics.catalog.builder_exprs import _arrow_cast, _coalesce_cols, _null_expr, _span_expr


def _join_with_code_units(
    primary_df: DataFrame,
    ctx: SessionContext,
    *,
    join_col: str = "code_unit_id",
) -> DataFrame:
    """Left-join a DataFrame with bytecode code-unit metadata.

    Returns:
        DataFrame: Input rows enriched with code-unit ``file_id``/``path`` where available.
    """
    code_units = ctx.table("py_bc_code_units")
    primary_cols = set(primary_df.schema().names)
    code_unit_cols = set(code_units.schema().names)
    if join_col not in primary_cols or "code_unit_id" not in code_unit_cols:
        return primary_df
    code_units_sel = code_units.select(
        col("code_unit_id"),
        col("file_id").alias("code_unit_file_id"),
        col("path").alias("code_unit_path"),
    )
    joined = primary_df.join(code_units_sel, "code_unit_id", how="left")
    file_id_expr = _coalesce_cols(
        joined,
        "file_id",
        "code_unit_file_id",
        default_expr=_null_expr("Utf8"),
    )
    path_expr = _coalesce_cols(
        joined,
        "path",
        "code_unit_path",
        default_expr=_null_expr("Utf8"),
    )
    return (
        joined.with_column("file_id", file_id_expr)
        .with_column("path", path_expr)
        .drop("code_unit_file_id", "code_unit_path")
    )


def cfg_blocks_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized CFG blocks.

    Returns:
        DataFrame: CFG block rows with canonical span struct columns.
    """
    joined = _join_with_code_units(ctx.table("py_bc_blocks"), ctx)

    bstart = _arrow_cast(col("start_offset"), "Int64")
    bend = f.coalesce(_arrow_cast(col("end_offset"), "Int64"), bstart)

    span = _span_expr(bstart=bstart, bend=bend)

    return joined.with_column("span", span)


def cfg_edges_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized CFG edges.

    Returns:
        DataFrame: CFG edge rows enriched with code-unit metadata.
    """
    return _join_with_code_units(ctx.table("py_bc_cfg_edges"), ctx)


__all__ = ["cfg_blocks_df_builder", "cfg_edges_df_builder"]
