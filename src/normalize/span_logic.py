"""Shared span logic for normalize Ibis expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import ibis
import pyarrow as pa
from ibis.expr.types import BooleanValue, NumericValue, Value

from arrowdsl.schema.semantic_types import SPAN_STORAGE
from datafusion_engine.span_utils import ENC_UTF8, ENC_UTF16, ENC_UTF32
from ibis_engine.builtin_udfs import col_to_byte
from ibis_engine.schema_utils import ibis_null_literal


@dataclass(frozen=True)
class SpanStructInputs:
    """Inputs for constructing span structs."""

    bstart: Value
    bend: Value
    start_line0: Value | None = None
    end_line0: Value | None = None
    start_col: Value | None = None
    end_col: Value | None = None
    col_unit: Value | None = None
    end_exclusive: Value | None = None


def span_struct_expr(inputs: SpanStructInputs) -> Value:
    """Return a span struct for the provided inputs.

    Returns
    -------
    Value
        Span struct expression.
    """
    span_ok = inputs.bstart.notnull() & inputs.bend.notnull()
    null_i32 = ibis_null_literal(pa.int32())
    null_bool = ibis_null_literal(pa.bool_())
    null_str = ibis_null_literal(pa.string())
    start_line_expr = (inputs.start_line0 if inputs.start_line0 is not None else null_i32).cast(
        "int32"
    )
    end_line_expr = (inputs.end_line0 if inputs.end_line0 is not None else null_i32).cast("int32")
    start_col_expr = (inputs.start_col if inputs.start_col is not None else null_i32).cast("int32")
    end_col_expr = (inputs.end_col if inputs.end_col is not None else null_i32).cast("int32")
    col_unit_expr = (inputs.col_unit if inputs.col_unit is not None else null_str).cast("string")
    end_exclusive_expr = (
        inputs.end_exclusive if inputs.end_exclusive is not None else null_bool
    ).cast("boolean")
    bstart_i64 = cast("NumericValue", inputs.bstart.cast("int64"))
    bend_i64 = cast("NumericValue", inputs.bend.cast("int64"))
    byte_start = ibis.ifelse(span_ok, bstart_i64.cast("int32"), null_i32)
    byte_len_value = bend_i64 - bstart_i64
    byte_len = ibis.ifelse(span_ok, byte_len_value.cast("int32"), null_i32)
    return ibis.struct(
        {
            "start": ibis.struct({"line0": start_line_expr, "col": start_col_expr}),
            "end": ibis.struct({"line0": end_line_expr, "col": end_col_expr}),
            "end_exclusive": end_exclusive_expr,
            "col_unit": col_unit_expr,
            "byte_span": ibis.struct({"byte_start": byte_start, "byte_len": byte_len}),
        }
    ).cast(SPAN_STORAGE)


def line_base_value(line_base: Value, *, default_base: int) -> NumericValue:
    """Normalize line-base values to int32 with defaults.

    Returns
    -------
    NumericValue
        Line-base expression normalized to int32.
    """
    return cast("NumericValue", ibis.coalesce(line_base.cast("int32"), ibis.literal(default_base)))


def zero_based_line(line_value: Value, line_base: Value) -> NumericValue:
    """Convert a line number to zero-based numbering.

    Returns
    -------
    NumericValue
        Zero-based line expression.
    """
    left = cast("NumericValue", line_value.cast("int32"))
    right = cast("NumericValue", line_base.cast("int32"))
    result = left - right
    return cast("NumericValue", result.cast("int32"))


def end_exclusive_value(end_exclusive: Value, *, default_exclusive: bool) -> BooleanValue:
    """Normalize end-exclusivity flags.

    Returns
    -------
    BooleanValue
        Normalized end-exclusivity expression.
    """
    result = ibis.coalesce(end_exclusive.cast("boolean"), ibis.literal(default_exclusive))
    return cast("BooleanValue", result)


def normalize_end_col(end_col: Value, end_exclusive: Value) -> NumericValue:
    """Normalize end columns based on end-exclusivity.

    Returns
    -------
    NumericValue
        Normalized end-column expression.
    """
    col = cast("NumericValue", end_col.cast("int64"))
    increment = cast("NumericValue", ibis.literal(1, type="int64"))
    adjusted = col + increment
    result = ibis.ifelse(end_exclusive, col, adjusted)
    return cast("NumericValue", result)


def line_offset_expr(
    line_start: Value,
    line_text: Value,
    column: Value,
    col_unit: Value,
) -> Value:
    """Return a byte offset within the file for a line/column pair.

    Returns
    -------
    Value
        Byte offset expression.
    """
    offset = column.cast("int64")
    byte_in_line = col_to_byte(line_text, offset, col_unit.cast("string"))
    left = line_start.cast("int64")
    right = byte_in_line.cast("int64")
    return left + right


def normalize_col_unit_expr(
    col_unit: Value | None,
    *,
    position_encoding: Value | None = None,
    default_unit: str = "utf32",
) -> Value:
    """Normalize col-unit values using canonical rules.

    This mirrors DataFusion UDF normalization semantics and applies
    optional position-encoding fallback when provided.

    Returns
    -------
    Value
        Normalized col-unit expression.
    """
    fallback = ibis.literal(default_unit)
    base = (
        _normalize_unit_expr(col_unit, default_unit=default_unit) if col_unit is not None else None
    )
    encoding = (
        _normalize_unit_expr(position_encoding, default_unit=default_unit)
        if position_encoding is not None
        else None
    )
    if base is None and encoding is None:
        return fallback
    if base is None:
        return ibis.coalesce(encoding, fallback)
    if encoding is None:
        return ibis.coalesce(base, fallback)
    return ibis.coalesce(base, encoding, fallback)


def _normalize_unit_expr(value: Value, *, default_unit: str) -> Value:
    text = value.cast("string").strip().lower()
    as_int = text.try_cast("int32")
    int_map = ibis.cases(
        (as_int == ibis.literal(ENC_UTF8), ibis.literal("utf8")),
        (as_int == ibis.literal(ENC_UTF16), ibis.literal("utf16")),
        (as_int == ibis.literal(ENC_UTF32), ibis.literal("utf32")),
        else_=ibis.literal(default_unit),
    )
    text_map = ibis.cases(
        (text.contains("byte"), ibis.literal("byte")),
        (text.contains("utf8"), ibis.literal("utf8")),
        (text.contains("utf16"), ibis.literal("utf16")),
        (text.contains("utf32"), ibis.literal("utf32")),
        else_=ibis.literal(default_unit),
    )
    normalized = ibis.ifelse(as_int.notnull(), int_map, text_map)
    return ibis.ifelse(value.isnull(), ibis.null(), normalized)


__all__ = [
    "SpanStructInputs",
    "end_exclusive_value",
    "line_base_value",
    "line_offset_expr",
    "normalize_col_unit_expr",
    "normalize_end_col",
    "span_struct_expr",
    "zero_based_line",
]
