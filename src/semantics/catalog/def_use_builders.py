"""Def-use event and reaching-definition analysis builders."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import SessionContext, col, lit
from datafusion import functions as f

from datafusion_engine.hashing import DEF_USE_EVENT_ID_SPEC, REACH_EDGE_ID_SPEC
from semantics.catalog.builder_exprs import (
    _arrow_cast,
    _coalesce_cols,
    _null_expr,
    _span_expr,
    _stable_id_expr,
)

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


_DEF_USE_OPS: tuple[str, ...] = ("IMPORT_NAME", "IMPORT_FROM")
_DEF_USE_PREFIXES: tuple[str, ...] = ("STORE_", "DELETE_")
_USE_PREFIXES: tuple[str, ...] = ("LOAD_",)


def _def_use_kind_expr(opname_col: Expr) -> Expr:
    is_import = (opname_col == lit(_DEF_USE_OPS[0])) | (opname_col == lit(_DEF_USE_OPS[1]))
    is_store_prefix = f.starts_with(opname_col, lit(_DEF_USE_PREFIXES[0]))
    is_delete_prefix = f.starts_with(opname_col, lit(_DEF_USE_PREFIXES[1]))
    is_load_prefix = f.starts_with(opname_col, lit(_USE_PREFIXES[0]))
    is_def = is_import | is_store_prefix | is_delete_prefix
    return f.when(is_def, lit("def")).otherwise(
        f.when(is_load_prefix, lit("use")).otherwise(_null_expr("Utf8"))
    )


def _def_use_event_id_expr() -> Expr:
    return _stable_id_expr(
        DEF_USE_EVENT_ID_SPEC.prefix,
        (col("code_unit_id"), col("instr_id"), col("kind"), col("symbol")),
        null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
    )


def _def_use_span_expr() -> Expr:
    bstart = _arrow_cast(col("offset"), "Int64")
    return _span_expr(bstart=bstart, bend=bstart)


def def_use_events_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for bytecode def/use events.

    Returns:
        DataFrame: Filtered def/use events with stable IDs and span values.
    """
    table = ctx.table("py_bc_instructions")
    symbol = _coalesce_cols(table, "argval_str", "argrepr")
    kind = _def_use_kind_expr(_arrow_cast(col("opname"), "Utf8"))
    df = table.with_column("symbol", symbol).with_column("kind", kind)
    df = df.with_column("event_id", _def_use_event_id_expr())
    df = df.with_column("span", _def_use_span_expr())
    return df.filter(col("symbol").is_not_null() & col("kind").is_not_null())


def reaching_defs_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for reaching-def edges.

    Returns:
        DataFrame: Reaching-definition edges between def and use events.
    """
    table = ctx.table("py_bc_def_use_events_v1")

    required = {"kind", "code_unit_id", "symbol", "event_id"}
    schema_names = set(table.schema().names)
    if not required.issubset(schema_names):
        return table.select(
            _null_expr("Utf8").alias("file_id"),
            _null_expr("Utf8").alias("path"),
            _null_expr("Utf8").alias("edge_id"),
            _null_expr("Utf8").alias("code_unit_id"),
            _null_expr("Utf8").alias("def_event_id"),
            _null_expr("Utf8").alias("use_event_id"),
            _null_expr("Utf8").alias("symbol"),
        ).filter(_arrow_cast(lit(0), "Boolean"))

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

    joined = defs.join(
        uses,
        ["code_unit_id", "symbol"],
        how="inner",
    )

    edge_id = _stable_id_expr(
        REACH_EDGE_ID_SPEC.prefix,
        (col("def_event_id"), col("use_event_id")),
        null_sentinel=REACH_EDGE_ID_SPEC.null_sentinel,
    )

    return joined.with_column("edge_id", edge_id)


__all__ = ["def_use_events_df_builder", "reaching_defs_df_builder"]
