"""Span unnest view builders for byte-based spans."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import col
from datafusion.dataframe import DataFrame

from semantics.span_normalize import SpanNormalizeConfig, normalize_byte_span_df

if TYPE_CHECKING:
    from datafusion import SessionContext


def ast_span_unnest_df_builder(ctx: SessionContext) -> DataFrame:
    """Build the AST span unnest view with byte-based spans.

    Returns:
    -------
    DataFrame
        AST span-unnest DataFrame.
    """
    df = ctx.table("ast_nodes")
    df = normalize_byte_span_df(
        df,
        config=SpanNormalizeConfig(span_col="span", drop_line_cols=True),
    )
    return _select_span_fields(df)


def ts_span_unnest_df_builder(ctx: SessionContext) -> DataFrame:
    """Build the tree-sitter span unnest view with byte-based spans.

    Returns:
    -------
    DataFrame
        Tree-sitter span-unnest DataFrame.
    """
    df = ctx.table("ts_nodes")
    df = normalize_byte_span_df(
        df,
        config=SpanNormalizeConfig(span_col="span", drop_line_cols=True),
    )
    return _select_span_fields(df)


def symtable_span_unnest_df_builder(ctx: SessionContext) -> DataFrame:
    """Build the symtable span unnest view with byte-based spans.

    Returns:
    -------
    DataFrame
        Symtable span-unnest DataFrame.
    """
    df = ctx.table("symtable_scopes")
    names = set(df.schema().names)
    if "span" not in names and "span_hint" in names:
        df = df.with_column("span", col("span_hint"))
    df = normalize_byte_span_df(
        df,
        config=SpanNormalizeConfig(span_col="span", drop_line_cols=True),
    )
    return _select_span_fields(df)


def py_bc_instruction_span_unnest_df_builder(ctx: SessionContext) -> DataFrame:
    """Build the bytecode instruction span unnest view with byte-based spans.

    Returns:
    -------
    DataFrame
        Bytecode instruction span-unnest DataFrame.
    """
    df = ctx.table("py_bc_instructions")
    df = normalize_byte_span_df(
        df,
        config=SpanNormalizeConfig(span_col="span", drop_line_cols=True),
    )
    return _select_span_fields(df)


def _select_span_fields(df: DataFrame) -> DataFrame:
    """Project the normalized span fields for downstream joins.

    Returns:
    -------
    DataFrame
        DataFrame selecting only span-related fields.
    """
    names = df.schema().names
    keep = [
        col(name)
        for name in ("file_id", "path", "bstart", "bend", "span", "col_unit")
        if name in names
    ]
    return df.select(*keep)


__all__ = [
    "ast_span_unnest_df_builder",
    "py_bc_instruction_span_unnest_df_builder",
    "symtable_span_unnest_df_builder",
    "ts_span_unnest_df_builder",
]
