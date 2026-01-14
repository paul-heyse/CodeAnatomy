"""Builtin Ibis UDFs for backend-native execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import ibis
import ibis.expr.datatypes as dt
from ibis.expr.types import Value

UdfVolatility = Literal["immutable", "stable", "volatile"]

BUILTIN_UDF_VOLATILITY: Mapping[str, UdfVolatility] = {
    "cpg_score": "stable",
}


@ibis.udf.scalar.builtin
def cpg_score(value: dt.Float64) -> dt.Float64:
    """Return a placeholder scoring value for backend-native execution.

    Volatility: stable.

    Returns
    -------
    ibis.expr.types.Value
        Placeholder scoring expression.
    """
    return value


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int64))
def stable_hash64(value: Value) -> Value:
    """Return a stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 64-bit hash expression.
    """
    return value.cast("int64")


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string))
def stable_hash128(value: Value) -> Value:
    """Return a stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 128-bit hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int32))
def position_encoding_norm(value: Value) -> Value:
    """Normalize position encoding values to enum integers.

    Returns
    -------
    ibis.expr.types.Value
        Normalized position encoding expression.
    """
    return value.cast("int32")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.int64, dt.string), dt.int64))
def col_to_byte(_line: Value, offset: Value, _col_unit: Value) -> Value:
    """Convert a line/offset pair into a UTF-8 byte offset.

    Returns
    -------
    ibis.expr.types.Value
        Byte offset expression.
    """
    return offset.cast("int64")
