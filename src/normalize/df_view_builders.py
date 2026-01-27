"""DataFusion-native view builders for normalize outputs.

This module provides DataFusion DataFrame-based view builders that replace
the Ibis-based builders in view_builders.py. These builders return DataFrame
instances directly and support DataFusionPlanBundle-based lineage extraction.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from datafusion_engine.normalize_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from datafusion_ext import stable_id

if TYPE_CHECKING:
    from datafusion.expr import Expr

# Type alias for DataFrame builder functions
DataFrameBuilder = Callable[[SessionContext], DataFrame]

# Def/Use operation detection constants
_DEF_USE_OPS: tuple[str, ...] = ("IMPORT_NAME", "IMPORT_FROM")
_DEF_USE_PREFIXES: tuple[str, ...] = ("STORE_", "DELETE_")
_USE_PREFIXES: tuple[str, ...] = ("LOAD_",)


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    """Cast an expression to an Arrow data type.

    Returns
    -------
    Expr
        Cast expression.
    """
    return f.arrow_cast(expr, lit(data_type))


def _null_expr(data_type: str) -> Expr:
    """Return a null expression with the specified Arrow data type.

    Returns
    -------
    Expr
        Null expression with the specified type.
    """
    return _arrow_cast(lit(None), data_type)


def _coalesce_cols(df: DataFrame, *col_names: str, default_expr: Expr | None = None) -> Expr:
    """Coalesce multiple columns, falling back to a default expression.

    Returns
    -------
    Expr
        Coalesced expression.
    """
    exprs = [col(name) for name in col_names if name in df.schema().names]
    if not exprs:
        return default_expr if default_expr is not None else _null_expr("Utf8")
    result = exprs[0]
    for expr in exprs[1:]:
        result = f.coalesce(result, expr)
    if default_expr is not None:
        result = f.coalesce(result, default_expr)
    return result


def _span_struct(  # noqa: PLR0913
    *,
    bstart: Expr,
    bend: Expr,
    start_line0: Expr | None = None,
    end_line0: Expr | None = None,
    start_col: Expr | None = None,
    end_col: Expr | None = None,
    col_unit: Expr | None = None,
    end_exclusive: Expr | None = None,
) -> Expr:
    """Build a span struct from component expressions.

    Returns
    -------
    Expr
        Span struct expression.
    """
    null_i32 = _null_expr("Int32")
    null_bool = _null_expr("Boolean")
    null_str = _null_expr("Utf8")

    start_line_expr = _arrow_cast(start_line0, "Int32") if start_line0 is not None else null_i32
    end_line_expr = _arrow_cast(end_line0, "Int32") if end_line0 is not None else null_i32
    start_col_expr = _arrow_cast(start_col, "Int32") if start_col is not None else null_i32
    end_col_expr = _arrow_cast(end_col, "Int32") if end_col is not None else null_i32
    col_unit_expr = _arrow_cast(col_unit, "Utf8") if col_unit is not None else null_str
    end_exclusive_expr = (
        _arrow_cast(end_exclusive, "Boolean") if end_exclusive is not None else null_bool
    )

    byte_start_expr = _arrow_cast(bstart, "Int32")
    byte_len_expr = _arrow_cast(bend - bstart, "Int32")

    return f.struct(
        start_line_expr,
        start_col_expr,
        end_line_expr,
        end_col_expr,
        col_unit_expr,
        end_exclusive_expr,
        byte_start_expr,
        byte_len_expr,
    )


def type_exprs_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized type expressions.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for normalized type expressions.
    """
    table = ctx.table("cst_type_exprs")

    # Filter out empty or whitespace-only expressions
    expr_text = _arrow_cast(col("expr_text"), "Utf8")
    trimmed = f.trim(expr_text)
    non_empty = trimmed.is_not_null() & (f.length(trimmed) > lit(0))

    df = table.filter(non_empty).with_column("type_repr", trimmed)

    # Generate type_expr_id (based on path, bstart, bend)
    type_expr_id = stable_id(
        TYPE_EXPR_ID_SPEC.prefix,
        f.concat_ws(":", col("path"), col("bstart"), col("bend")),
    )

    # Generate type_id (based on type_repr)
    type_id = stable_id(
        TYPE_ID_SPEC.prefix,
        col("type_repr"),
    )

    # Build span struct
    has_col_unit = "col_unit" in df.schema().names
    has_end_exclusive = "end_exclusive" in df.schema().names
    span = _span_struct(
        bstart=col("bstart"),
        bend=col("bend"),
        col_unit=col("col_unit") if has_col_unit else lit("byte"),
        end_exclusive=col("end_exclusive") if has_end_exclusive else _arrow_cast(lit(1), "Boolean"),
    )

    df = df.with_column("type_expr_id", type_expr_id)
    df = df.with_column("type_id", type_id)
    df = df.with_column("span", span)

    # Drop intermediate columns
    keep_cols = [
        "file_id",
        "path",
        "type_expr_id",
        "type_id",
        "type_repr",
        "expr_text",
        "span",
        "expr_kind",
        "expr_role",
        "owner_def_id",
    ]
    return df.select(*[col(c) for c in keep_cols if c in df.schema().names])


def type_nodes_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized type nodes.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for normalized type nodes.
    """
    # Get type expressions
    type_exprs = ctx.table("type_exprs_norm_v1")

    # Build rows from type expressions
    expr_type_repr = _arrow_cast(col("type_repr"), "Utf8")
    expr_trimmed = f.trim(expr_type_repr)
    expr_non_empty = expr_trimmed.is_not_null() & (f.length(expr_trimmed) > lit(0))
    expr_valid = expr_non_empty & col("type_id").is_not_null()

    expr_rows = (
        type_exprs.filter(expr_valid)
        .with_column("type_repr", expr_trimmed)
        .with_column("type_form", lit("annotation"))
        .with_column("origin", lit("annotation"))
        .select(col("type_id"), col("type_repr"), col("type_form"), col("origin"))
    )

    # Try to get SCIP symbol information if available
    try:
        scip = ctx.table("scip_symbol_information")
        if "type_repr" in scip.schema().names:
            scip_type_repr = _arrow_cast(col("type_repr"), "Utf8")
            scip_trimmed = f.trim(scip_type_repr)
            scip_non_empty = scip_trimmed.is_not_null() & (f.length(scip_trimmed) > lit(0))

            scip_rows = (
                scip.filter(scip_non_empty)
                .with_column("type_repr", scip_trimmed)
                .with_column("type_form", lit("scip"))
                .with_column("origin", lit("inferred"))
            )

            # Generate type_id for SCIP rows
            scip_type_id = stable_id(
                TYPE_ID_SPEC.prefix,
                col("type_repr"),
            )
            scip_rows = scip_rows.with_column("type_id", scip_type_id)
            scip_rows = scip_rows.select(
                col("type_id"), col("type_repr"), col("type_form"), col("origin")
            )

            # Prefer SCIP rows if they exist
            # For simplicity, union and dedupe (in production, check preview)
            return scip_rows.union(expr_rows).distinct()
    except (RuntimeError, KeyError, ValueError):
        # SCIP table not available or has issues
        pass

    return expr_rows


def cfg_blocks_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized CFG blocks.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for normalized CFG blocks.
    """
    blocks = ctx.table("py_bc_blocks")
    code_units = ctx.table("py_bc_code_units")

    # Join with code units if code_unit_id is available
    if "code_unit_id" in blocks.schema().names and "code_unit_id" in code_units.schema().names:
        code_units_sel = code_units.select(
            col("code_unit_id"),
            col("file_id").alias("code_unit_file_id"),
            col("path").alias("code_unit_path"),
        )

        joined = blocks.join(
            code_units_sel,
            "code_unit_id",
            how="left",
        )

        # Coalesce file_id and path
        file_id_expr = _coalesce_cols(
            joined, "file_id", "code_unit_file_id", default_expr=_null_expr("Utf8")
        )
        path_expr = _coalesce_cols(
            joined, "path", "code_unit_path", default_expr=_null_expr("Utf8")
        )

        joined = joined.with_column("file_id", file_id_expr)
        joined = joined.with_column("path", path_expr)
    else:
        joined = blocks

    # Build span struct
    bstart = _arrow_cast(col("start_offset"), "Int64")
    bend = f.coalesce(_arrow_cast(col("end_offset"), "Int64"), bstart)

    span = _span_struct(
        bstart=bstart,
        bend=bend,
        col_unit=lit("byte"),
        end_exclusive=_arrow_cast(lit(1), "Boolean"),
    )

    return joined.with_column("span", span)


def cfg_edges_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized CFG edges.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for normalized CFG edges.
    """
    edges = ctx.table("py_bc_cfg_edges")
    code_units = ctx.table("py_bc_code_units")

    # Join with code units if code_unit_id is available
    if "code_unit_id" in edges.schema().names and "code_unit_id" in code_units.schema().names:
        code_units_sel = code_units.select(
            col("code_unit_id"),
            col("file_id").alias("code_unit_file_id"),
            col("path").alias("code_unit_path"),
        )

        joined = edges.join(
            code_units_sel,
            "code_unit_id",
            how="left",
        )

        # Coalesce file_id and path
        file_id_expr = _coalesce_cols(
            joined, "file_id", "code_unit_file_id", default_expr=_null_expr("Utf8")
        )
        path_expr = _coalesce_cols(
            joined, "path", "code_unit_path", default_expr=_null_expr("Utf8")
        )

        joined = joined.with_column("file_id", file_id_expr)
        joined = joined.with_column("path", path_expr)
    else:
        joined = edges

    return joined


def def_use_events_df_builder(ctx: SessionContext) -> DataFrame:  # noqa: PLR0914
    """Build a DataFrame for bytecode def/use events.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for bytecode def/use events.
    """
    table = ctx.table("py_bc_instructions")

    # Coalesce symbol from argval_str or argrepr
    symbol = _coalesce_cols(table, "argval_str", "argrepr")

    # Determine def/use kind from opname
    opname_col = _arrow_cast(col("opname"), "Utf8")

    # Build kind expression using CASE statements
    is_import = (opname_col == lit(_DEF_USE_OPS[0])) | (opname_col == lit(_DEF_USE_OPS[1]))
    is_store_prefix = f.starts_with(opname_col, lit(_DEF_USE_PREFIXES[0]))
    is_delete_prefix = f.starts_with(opname_col, lit(_DEF_USE_PREFIXES[1]))
    is_load_prefix = f.starts_with(opname_col, lit(_USE_PREFIXES[0]))

    is_def = is_import | is_store_prefix | is_delete_prefix
    is_use = is_load_prefix

    true_lit = _arrow_cast(lit(1), "Boolean")
    kind = f.case(is_def).when(true_lit, lit("def")).when(is_use, lit("use")).end()

    df = table.with_column("symbol", symbol).with_column("kind", kind)

    # Generate event_id
    event_id = stable_id(
        DEF_USE_EVENT_ID_SPEC.prefix,
        f.concat_ws(":", col("code_unit_id"), col("instr_id"), col("kind"), col("symbol")),
    )

    # Build span struct
    bstart = _arrow_cast(col("offset"), "Int64")
    span = _span_struct(
        bstart=bstart,
        bend=bstart,
        col_unit=lit("byte"),
        end_exclusive=_arrow_cast(lit(1), "Boolean"),
    )

    df = df.with_column("event_id", event_id)
    df = df.with_column("span", span)

    # Filter to valid events
    valid = col("symbol").is_not_null() & col("kind").is_not_null()
    return df.filter(valid)


def reaching_defs_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for reaching-def edges.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for reaching-def edges.
    """
    table = ctx.table("py_bc_def_use_events_v1")

    # Check required columns
    required = {"kind", "code_unit_id", "symbol", "event_id"}
    schema_names = set(table.schema().names)
    if not required.issubset(schema_names):
        # Return empty table with expected schema
        return table.select(
            _null_expr("Utf8").alias("file_id"),
            _null_expr("Utf8").alias("path"),
            _null_expr("Utf8").alias("edge_id"),
            _null_expr("Utf8").alias("code_unit_id"),
            _null_expr("Utf8").alias("def_event_id"),
            _null_expr("Utf8").alias("use_event_id"),
            _null_expr("Utf8").alias("symbol"),
        ).filter(_arrow_cast(lit(0), "Boolean"))

    # Split into defs and uses
    defs = table.filter(col("kind") == lit("def")).select(
        col("code_unit_id"),
        col("symbol"),
        col("event_id").alias("def_event_id"),
    )

    uses = table.filter(col("kind") == lit("use")).select(
        col("code_unit_id"),
        col("symbol"),
        col("event_id").alias("use_event_id"),
        col("path") if "path" in schema_names else _null_expr("Utf8").alias("path"),
        col("file_id") if "file_id" in schema_names else _null_expr("Utf8").alias("file_id"),
    )

    # Join defs and uses on code_unit_id and symbol
    joined = defs.join(
        uses,
        ["code_unit_id", "symbol"],
        how="inner",
    )

    # Generate edge_id
    edge_id = stable_id(
        REACH_EDGE_ID_SPEC.prefix,
        f.concat_ws(":", col("def_event_id"), col("use_event_id")),
    )

    return joined.with_column("edge_id", edge_id)


def diagnostics_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized diagnostics.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for normalized diagnostics.

    Notes
    -----
    This is a simplified version. Full implementation would need:
    - Line index joins for CST and SCIP diagnostics
    - Severity mapping for SCIP
    - Symtable/bytecode consistency checks
    """
    # For now, return a placeholder that unions available diagnostic sources
    # Full implementation would need complex line-based span calculations

    # Try to get basic diagnostic tables
    try:
        # Simple case: just get tree-sitter errors
        ts_errors = ctx.table("ts_errors")

        bstart = _coalesce_cols(ts_errors, "bstart", "start_byte", default_expr=_null_expr("Int64"))
        bend = _coalesce_cols(ts_errors, "bend", "end_byte", default_expr=_null_expr("Int64"))

        span = _span_struct(
            bstart=bstart,
            bend=bend,
            col_unit=lit("byte"),
            end_exclusive=_arrow_cast(lit(1), "Boolean"),
        )

        df = (
            ts_errors.with_column("span", span)
            .with_column("severity", lit("ERROR"))
            .with_column("message", lit("tree-sitter error node"))
            .with_column("diag_source", lit("treesitter"))
            .with_column("code", _null_expr("Utf8"))
        )

        # Generate diag_id
        diag_id = stable_id(
            DIAG_ID_SPEC.prefix,
            f.concat_ws(":", col("path"), bstart, bend, col("diag_source"), col("message")),
        )

        return df.with_column("diag_id", diag_id)
    except (RuntimeError, KeyError, ValueError):
        # Return empty diagnostic table
        return ctx.sql("""
            SELECT
                CAST(NULL AS Utf8) AS file_id,
                CAST(NULL AS Utf8) AS path,
                CAST(NULL AS Utf8) AS diag_id,
                CAST(NULL AS Utf8) AS severity,
                CAST(NULL AS Utf8) AS message,
                CAST(NULL AS Utf8) AS diag_source,
                CAST(NULL AS Utf8) AS code
            WHERE FALSE
        """)


def span_errors_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for span error rows.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with registered base tables.

    Returns
    -------
    DataFrame
        DataFrame for span error rows.
    """
    return ctx.table("span_errors_v1")


# View builder registry mapping view names to builders
VIEW_BUILDERS: dict[str, DataFrameBuilder] = {
    "type_exprs_norm_v1": type_exprs_df_builder,
    "type_nodes_v1": type_nodes_df_builder,
    "py_bc_blocks_norm_v1": cfg_blocks_df_builder,
    "py_bc_cfg_edges_norm_v1": cfg_edges_df_builder,
    "py_bc_def_use_events_v1": def_use_events_df_builder,
    "py_bc_reaches_v1": reaching_defs_df_builder,
    "diagnostics_norm_v1": diagnostics_df_builder,
    "span_errors_v1": span_errors_df_builder,
}


__all__ = [
    "VIEW_BUILDERS",
    "DataFrameBuilder",
    "cfg_blocks_df_builder",
    "cfg_edges_df_builder",
    "def_use_events_df_builder",
    "diagnostics_df_builder",
    "reaching_defs_df_builder",
    "span_errors_df_builder",
    "type_exprs_df_builder",
    "type_nodes_df_builder",
]
