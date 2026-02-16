"""Shared helpers for line-index normalization flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.udf.expr import udf_expr

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from datafusion.expr import Expr


@dataclass(frozen=True)
class LineIndexJoinOptions:
    """Configuration for joining a table against line-index metadata."""

    start_line_col: str
    end_line_col: str
    ctx: SessionContext | None = None
    line_index_df: DataFrame | None = None
    file_id_col: str = "file_id"
    path_col: str = "path"


def byte_offset_expr(
    line_start_col: str,
    line_text_col: str,
    char_col: str,
    unit_expr: Expr,
    *,
    unit_col: str | None = None,
) -> Expr:
    """Compute byte offset from line-start and character columns.

    Returns:
        Expr: DataFusion expression for byte offset with null-guarding.
    """
    base = col(line_start_col).cast(pa.int64())
    char_val = col(char_col).cast(pa.int64())
    offset = udf_expr("col_to_byte", col(line_text_col), char_val, unit_expr)
    guard = col(line_start_col).is_null() | col(line_text_col).is_null() | col(char_col).is_null()
    if unit_col is not None:
        guard |= col(unit_col).is_null()
    return f.when(guard, lit(None).cast(pa.int64())).otherwise(base + offset)


def line_index_join(
    df: DataFrame,
    line_index_table: str,
    *,
    options: LineIndexJoinOptions,
) -> DataFrame:
    """Join a DataFrame with line-index rows for start/end line resolution.

    Returns:
        DataFrame: Input rows joined with start/end line-index metadata.

    Raises:
        ValueError: If neither ``file_id`` nor ``path`` join columns are available.
    """
    line_index = options.line_index_df
    if line_index is None:
        if options.ctx is None:
            msg = "line_index_join requires either line_index_df or ctx."
            raise ValueError(msg)
        line_index = options.ctx.table(line_index_table)

    names = set(df.schema().names)
    join_on_file_id = options.file_id_col in names
    join_on_path = options.path_col in names
    if not join_on_file_id and not join_on_path:
        msg = "Line-index joins require file_id or path columns."
        raise ValueError(msg)

    start_idx = line_index.select(
        col("file_id").alias("start_file_id"),
        col("path").alias("start_path"),
        col("line_no").alias("start_line_no_idx"),
        col("line_start_byte").alias("start_line_start_byte"),
        col("line_text").alias("start_line_text"),
    )
    end_idx = line_index.select(
        col("file_id").alias("end_file_id"),
        col("path").alias("end_path"),
        col("line_no").alias("end_line_no_idx"),
        col("line_start_byte").alias("end_line_start_byte"),
        col("line_text").alias("end_line_text"),
    )

    if join_on_file_id:
        start_keys = (
            [options.file_id_col, options.start_line_col],
            ["start_file_id", "start_line_no_idx"],
        )
        end_keys = (
            [options.file_id_col, options.end_line_col],
            ["end_file_id", "end_line_no_idx"],
        )
    else:
        start_keys = (
            [options.path_col, options.start_line_col],
            ["start_path", "start_line_no_idx"],
        )
        end_keys = (
            [options.path_col, options.end_line_col],
            ["end_path", "end_line_no_idx"],
        )

    joined = df.join(
        start_idx,
        left_on=start_keys[0],
        right_on=start_keys[1],
        how="left",
        coalesce_duplicate_keys=True,
    )
    return joined.join(
        end_idx,
        left_on=end_keys[0],
        right_on=end_keys[1],
        how="left",
        coalesce_duplicate_keys=True,
    )


__all__ = ["LineIndexJoinOptions", "byte_offset_expr", "line_index_join"]
