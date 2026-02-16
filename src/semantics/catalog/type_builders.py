"""Type-expression and type-node analysis builders."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from datafusion import SessionContext, col, lit

from datafusion_engine.hashing import TYPE_EXPR_ID_SPEC, TYPE_ID_SPEC
from datafusion_engine.udf.expr import udf_expr
from semantics.catalog.builder_exprs import (
    _arrow_cast,
    _normalized_text,
    _span_expr,
    _stable_id_expr,
)

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


logger = logging.getLogger(__name__)


def type_exprs_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized type expressions.

    Returns:
        DataFrame: Normalized type-expression rows with stable IDs and spans.
    """
    table = ctx.table("cst_type_exprs")

    expr_text = _arrow_cast(col("expr_text"), "Utf8")
    type_repr = udf_expr("utf8_null_if_blank", _normalized_text(expr_text))

    df = table.filter(type_repr.is_not_null()).with_column("type_repr", type_repr)

    type_expr_id = _stable_id_expr(
        TYPE_EXPR_ID_SPEC.prefix,
        (col("path"), col("bstart"), col("bend")),
        null_sentinel=TYPE_EXPR_ID_SPEC.null_sentinel,
    )

    type_id = _stable_id_expr(
        TYPE_ID_SPEC.prefix,
        (col("type_repr"),),
        null_sentinel=TYPE_ID_SPEC.null_sentinel,
    )

    has_col_unit = "col_unit" in df.schema().names
    span = _span_expr(
        bstart=col("bstart"),
        bend=col("bend"),
        col_unit=col("col_unit") if has_col_unit else None,
    )

    df = df.with_column("type_expr_id", type_expr_id)
    df = df.with_column("type_id", type_id)
    df = df.with_column("span", span)

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

    Returns:
        DataFrame: Deduplicated type-node rows merged from CST and SCIP evidence.
    """
    type_exprs = ctx.table("type_exprs_norm")

    expr_type_repr = udf_expr(
        "utf8_null_if_blank",
        _normalized_text(_arrow_cast(col("type_repr"), "Utf8")),
    )
    expr_valid = expr_type_repr.is_not_null() & col("type_id").is_not_null()

    expr_rows = (
        type_exprs.filter(expr_valid)
        .with_column("type_repr", expr_type_repr)
        .with_column("type_form", lit("annotation"))
        .with_column("origin", lit("annotation"))
        .select(col("type_id"), col("type_repr"), col("type_form"), col("origin"))
    )

    try:
        scip = ctx.table("scip_symbol_information")
        if "type_repr" in scip.schema().names:
            scip_type_repr = udf_expr(
                "utf8_null_if_blank",
                _normalized_text(_arrow_cast(col("type_repr"), "Utf8")),
            )

            scip_rows = (
                scip.filter(scip_type_repr.is_not_null())
                .with_column("type_repr", scip_type_repr)
                .with_column("type_form", lit("scip"))
                .with_column("origin", lit("inferred"))
            )

            scip_type_id = _stable_id_expr(
                TYPE_ID_SPEC.prefix,
                (col("type_repr"),),
                null_sentinel=TYPE_ID_SPEC.null_sentinel,
            )
            scip_rows = scip_rows.with_column("type_id", scip_type_id)
            scip_rows = scip_rows.select(
                col("type_id"), col("type_repr"), col("type_form"), col("origin")
            )

            return scip_rows.union(expr_rows).distinct()
    except (RuntimeError, KeyError, ValueError):
        logger.debug("SCIP symbol info table unavailable; falling back to type_expr rows.")

    return expr_rows


__all__ = ["type_exprs_df_builder", "type_nodes_df_builder"]
