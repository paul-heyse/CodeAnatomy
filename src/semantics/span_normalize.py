"""Helpers for normalizing byte spans in semantic views."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from datafusion_engine.udf.expr import udf_expr
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion.expr import Expr


LINE_COL_COLUMNS: tuple[str, ...] = (
    "start_line0",
    "start_col",
    "end_line0",
    "end_col",
    "line0",
    "col",
    "line_number",
    "line1",
    "start_line",
    "end_line",
    "start_char",
    "end_char",
    "line_base",
)


@dataclass(frozen=True)
class SpanNormalizeConfig:
    """Configuration for normalizing span columns into byte offsets."""

    span_col: str = "span"
    byte_start_col: str = "byte_start"
    byte_len_col: str = "byte_len"
    bstart_col: str = "bstart"
    bend_col: str = "bend"
    start_line_col: str = "start_line0"
    start_col_col: str = "start_col"
    end_line_col: str = "end_line0"
    end_col_col: str = "end_col"
    col_unit_col: str = "col_unit"
    line_index_table: str = "file_line_index_v1"
    file_id_col: str = "file_id"
    path_col: str = "path"
    drop_line_cols: bool = False
    force_col_unit: str | None = "byte"


def normalize_byte_span_df(
    df: DataFrame,
    *,
    line_index: DataFrame | None = None,
    config: SpanNormalizeConfig | None = None,
) -> DataFrame:
    """Return DataFrame with canonical byte-span columns and span struct.

    Prefers explicit bstart/bend, then byte_start/byte_len, then span.byte_span,
    and finally falls back to line/col conversion using file_line_index_v1.

    Returns:
    -------
    DataFrame
        DataFrame with bstart/bend/span columns normalized.
    """
    cfg = config or SpanNormalizeConfig()
    with stage_span(
        "semantics.normalize_byte_span",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.span_col": cfg.span_col,
            "codeanatomy.line_index_table": cfg.line_index_table,
        },
    ):
        names = set(df.schema().names)
        used_line_index = False
        bstart_expr, bend_expr = _resolve_direct_byte_spans(names, cfg)
        if bstart_expr is None or bend_expr is None:
            bstart_expr, bend_expr = _resolve_byte_len_spans(names, cfg)
        if bstart_expr is None or bend_expr is None:
            df, bstart_expr, bend_expr = _normalize_via_line_index(
                df,
                names=names,
                cfg=cfg,
                line_index=line_index,
            )
            used_line_index = True

        _validate_col_unit(df, cfg, direct_byte_spans=not used_line_index)

        df = df.with_column(cfg.bstart_col, bstart_expr)
        df = df.with_column(cfg.bend_col, bend_expr)
        df = df.with_column(
            cfg.span_col,
            udf_expr("span_make", col(cfg.bstart_col), col(cfg.bend_col)),
        )
        if cfg.force_col_unit is not None:
            df = df.with_column(cfg.col_unit_col, lit(cfg.force_col_unit))

        tmp_cols = [name for name in ("_tmp_bstart", "_tmp_bend") if name in df.schema().names]
        if tmp_cols:
            df = df.drop(*tmp_cols)

        if cfg.drop_line_cols:
            df = drop_line_columns(df)
        return df


def drop_line_columns(df: DataFrame, *, extra: Iterable[str] = ()) -> DataFrame:
    """Drop line/column metadata columns if present.

    Returns:
    -------
    DataFrame
        DataFrame without line/column metadata columns.
    """
    drop_cols = {name for name in (*LINE_COL_COLUMNS, *extra) if name in df.schema().names}
    if not drop_cols:
        return df
    return df.drop(*sorted(drop_cols))


def _nested_expr(expr: Expr, path: Iterable[str]) -> Expr:
    nested = expr
    for part in path:
        nested = nested[part]
    return nested


def _resolve_direct_byte_spans(
    names: set[str],
    cfg: SpanNormalizeConfig,
) -> tuple[Expr | None, Expr | None]:
    if cfg.bstart_col in names and cfg.bend_col in names:
        return col(cfg.bstart_col), col(cfg.bend_col)
    return None, None


def _resolve_byte_len_spans(
    names: set[str],
    cfg: SpanNormalizeConfig,
) -> tuple[Expr | None, Expr | None]:
    byte_start_expr = col(cfg.byte_start_col) if cfg.byte_start_col in names else None
    byte_len_expr = col(cfg.byte_len_col) if cfg.byte_len_col in names else None
    if cfg.span_col in names:
        span_expr = col(cfg.span_col)
        if byte_start_expr is None:
            byte_start_expr = _nested_expr(span_expr, ("byte_span", "byte_start"))
        if byte_len_expr is None:
            byte_len_expr = _nested_expr(span_expr, ("byte_span", "byte_len"))
    if byte_start_expr is None or byte_len_expr is None:
        return None, None
    return byte_start_expr, byte_start_expr + byte_len_expr


def _normalize_via_line_index(
    df: DataFrame,
    *,
    names: set[str],
    cfg: SpanNormalizeConfig,
    line_index: DataFrame | None,
) -> tuple[DataFrame, Expr, Expr]:
    if line_index is None:
        msg = "line_index DataFrame required for line/col span normalization"
        raise ValueError(msg)
    if cfg.start_line_col not in names or cfg.end_line_col not in names:
        msg = "Line-based span columns missing; cannot normalize spans."
        raise ValueError(msg)
    join_on_file_id = cfg.file_id_col in names
    join_on_path = cfg.path_col in names
    if not join_on_file_id and not join_on_path:
        msg = "Line-based span normalization requires file_id or path columns."
        raise ValueError(msg)

    start_idx = line_index.select(
        col("file_id").alias("start_file_id"),
        col("path").alias("start_path"),
        col("line_no").alias("start_line_no"),
        col("line_start_byte").alias("start_line_start_byte"),
        col("line_text").alias("start_line_text"),
    )
    end_idx = line_index.select(
        col("file_id").alias("end_file_id"),
        col("path").alias("end_path"),
        col("line_no").alias("end_line_no"),
        col("line_start_byte").alias("end_line_start_byte"),
        col("line_text").alias("end_line_text"),
    )

    if join_on_file_id:
        join_keys = ([cfg.file_id_col, cfg.start_line_col], ["start_file_id", "start_line_no"])
    else:
        join_keys = ([cfg.path_col, cfg.start_line_col], ["start_path", "start_line_no"])
    joined = df.join(start_idx, join_keys=join_keys, how="left", coalesce_duplicate_keys=True)

    if join_on_file_id:
        join_keys = ([cfg.file_id_col, cfg.end_line_col], ["end_file_id", "end_line_no"])
    else:
        join_keys = ([cfg.path_col, cfg.end_line_col], ["end_path", "end_line_no"])
    joined = joined.join(end_idx, join_keys=join_keys, how="left", coalesce_duplicate_keys=True)

    unit_expr = col(cfg.col_unit_col) if cfg.col_unit_col in names else lit("byte")

    def _byte_offset(line_start: str, line_text: str, col_name: str) -> Expr:
        base = col(line_start).cast(pa.int64())
        char_col = col(col_name).cast(pa.int64())
        offset = udf_expr("col_to_byte", col(line_text), char_col, unit_expr)
        guard = col(line_start).is_null() | col(line_text).is_null() | col(col_name).is_null()
        if cfg.col_unit_col in names:
            guard |= col(cfg.col_unit_col).is_null()
        return f.when(guard, lit(None).cast(pa.int64())).otherwise(base + offset)

    joined = joined.with_column(
        "_tmp_bstart",
        _byte_offset("start_line_start_byte", "start_line_text", cfg.start_col_col),
    )
    joined = joined.with_column(
        "_tmp_bend",
        _byte_offset("end_line_start_byte", "end_line_text", cfg.end_col_col),
    )
    joined = joined.drop(
        "start_file_id",
        "start_path",
        "start_line_no",
        "start_line_start_byte",
        "start_line_text",
        "end_file_id",
        "end_path",
        "end_line_no",
        "end_line_start_byte",
        "end_line_text",
    )
    return joined, col("_tmp_bstart"), col("_tmp_bend")


def _validate_col_unit(
    df: DataFrame,
    cfg: SpanNormalizeConfig,
    *,
    direct_byte_spans: bool,
) -> None:
    if cfg.col_unit_col not in df.schema().names:
        return
    units_df = (
        df.select(col(cfg.col_unit_col).alias("col_unit"))
        .filter(col(cfg.col_unit_col).is_not_null())
        .distinct()
        .limit(2)
    )
    table = units_df.to_arrow_table()
    if table.num_rows == 0:
        return
    rows = table.to_pylist()
    values = {str(row["col_unit"]).lower() for row in rows if row.get("col_unit") is not None}
    if not values:
        return
    if len(values) > 1:
        msg = f"Mixed col_unit values in span normalization: {sorted(values)}"
        raise ValueError(msg)
    unit = next(iter(values))
    if direct_byte_spans:
        if unit not in {"byte", "bytes"}:
            msg = f"Expected byte spans, but col_unit={unit!r}."
            raise ValueError(msg)
        return
    allowed = {"byte", "bytes", "utf8", "utf-8", "utf16", "utf-16", "utf32", "utf-32"}
    if unit not in allowed:
        msg = f"Unsupported col_unit={unit!r} for line/col normalization."
        raise ValueError(msg)


__all__ = [
    "LINE_COL_COLUMNS",
    "SpanNormalizeConfig",
    "drop_line_columns",
    "normalize_byte_span_df",
]
