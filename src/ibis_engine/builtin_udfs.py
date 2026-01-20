"""Builtin Ibis UDFs for backend-native execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Value

UdfVolatility = Literal["immutable", "stable", "volatile"]
IbisUdfLane = Literal["ibis_builtin", "ibis_pyarrow", "ibis_pandas", "ibis_python"]
IbisUdfKind = Literal["scalar", "aggregate", "window", "table"]


@dataclass(frozen=True)
class IbisUdfSpec:
    """Specification for an Ibis UDF entry."""

    func_id: str
    engine_name: str
    kind: IbisUdfKind
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: UdfVolatility = "stable"
    arg_names: tuple[str, ...] | None = None
    lanes: tuple[IbisUdfLane, ...] = ()
    rewrite_tags: tuple[str, ...] = ()
    catalog: str | None = None
    database: str | None = None


@ibis.udf.scalar.builtin(name="cpg_score")
def cpg_score(value: dt.Float64) -> dt.Float64:
    """Return a placeholder scoring value for backend-native execution.

    Volatility: stable.

    Returns
    -------
    ibis.expr.types.Value
        Placeholder scoring expression.
    """
    return value


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int64), name="stable_hash64")
def stable_hash64(value: Value) -> Value:
    """Return a stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 64-bit hash expression.
    """
    return value.cast("int64")


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string), name="stable_hash128")
def stable_hash128(value: Value) -> Value:
    """Return a stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 128-bit hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.string), dt.string), name="prefixed_hash64")
def prefixed_hash64(_prefix: Value, value: Value) -> Value:
    """Return a prefixed stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Prefixed hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.string), dt.string), name="stable_id")
def stable_id(_prefix: Value, value: Value) -> Value:
    """Return a prefixed stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Prefixed hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(
    signature=((dt.Array(value_type=dt.string),), dt.boolean), name="valid_mask"
)
def valid_mask(_values: Value) -> Value:
    """Return True when all list values are non-null.

    Returns
    -------
    ibis.expr.types.Value
        Validity mask expression.
    """
    return ibis.literal(value=True).cast("boolean")


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int32), name="position_encoding_norm")
def position_encoding_norm(value: Value) -> Value:
    """Normalize position encoding values to enum integers.

    Returns
    -------
    ibis.expr.types.Value
        Normalized position encoding expression.
    """
    return value.cast("int32")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.int64, dt.string), dt.int64), name="col_to_byte")
def col_to_byte(_line: Value, offset: Value, _col_unit: Value) -> Value:
    """Convert a line/offset pair into a UTF-8 byte offset.

    Returns
    -------
    ibis.expr.types.Value
        Byte offset expression.
    """
    return offset.cast("int64")


IBIS_UDF_SPECS: tuple[IbisUdfSpec, ...] = ()


def ibis_udf_specs() -> tuple[IbisUdfSpec, ...]:
    """Return the canonical Ibis UDF specs.

    Returns
    -------
    tuple[IbisUdfSpec, ...]
        Canonical Ibis UDF specifications.
    """
    return IBIS_UDF_SPECS


__all__ = [
    "IBIS_UDF_SPECS",
    "IbisUdfSpec",
    "col_to_byte",
    "cpg_score",
    "ibis_udf_specs",
    "position_encoding_norm",
    "prefixed_hash64",
    "stable_hash64",
    "stable_hash128",
    "stable_id",
    "valid_mask",
]
