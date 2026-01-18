"""Builtin Ibis UDFs for backend-native execution."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Value

from arrowdsl.compute.position_encoding import (
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    normalize_position_encoding,
)
from arrowdsl.core.ids import hash64_from_text, iter_array_values

UdfVolatility = Literal["immutable", "stable", "volatile"]
IbisUdfLane = Literal["ibis_builtin", "ibis_pyarrow", "ibis_pandas", "ibis_python"]
IbisUdfKind = Literal["scalar", "aggregate", "window", "table"]

BUILTIN_UDF_VOLATILITY: Mapping[str, UdfVolatility] = {
    "cpg_score": "stable",
}


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


@ibis.udf.scalar.pyarrow(
    signature=((dt.string,), dt.int64),
    name="stable_hash64",
)
def stable_hash64_pyarrow(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    """Return stable hash64 values using a PyArrow vectorized UDF.

    Returns
    -------
    pyarrow.Array | pyarrow.ChunkedArray | pyarrow.Scalar
        Stable hash values for each input element.
    """
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        return pa.scalar(hash64_from_text(value), type=pa.int64())
    out = [hash64_from_text(_text_or_none(value)) for value in iter_array_values(values)]
    return pa.array(out, type=pa.int64())


@ibis.udf.scalar.python(
    signature=((dt.string,), dt.int64),
    name="stable_hash64",
)
def stable_hash64_python(value: str | None) -> int | None:
    """Return stable hash64 values using a Python scalar fallback.

    Returns
    -------
    int | None
        Stable hash value or ``None`` for null inputs.
    """
    return hash64_from_text(value)


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string))
def stable_hash128(value: Value) -> Value:
    """Return a stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 128-bit hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.pyarrow(
    signature=((dt.string,), dt.string),
    name="stable_hash128",
)
def stable_hash128_pyarrow(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    """Return stable hash128 values using a PyArrow vectorized UDF.

    Returns
    -------
    pyarrow.Array | pyarrow.ChunkedArray | pyarrow.Scalar
        Stable hash values for each input element.
    """
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        return pa.scalar(_hash128_text(value), type=pa.string())
    out = [_hash128_text(_text_or_none(value)) for value in iter_array_values(values)]
    return pa.array(out, type=pa.string())


@ibis.udf.scalar.python(
    signature=((dt.string,), dt.string),
    name="stable_hash128",
)
def stable_hash128_python(value: str | None) -> str | None:
    """Return stable hash128 values using a Python scalar fallback.

    Returns
    -------
    str | None
        Stable hash value or ``None`` for null inputs.
    """
    return _hash128_text(value)


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int32))
def position_encoding_norm(value: Value) -> Value:
    """Normalize position encoding values to enum integers.

    Returns
    -------
    ibis.expr.types.Value
        Normalized position encoding expression.
    """
    return value.cast("int32")


@ibis.udf.scalar.pyarrow(
    signature=((dt.string,), dt.int32),
    name="position_encoding_norm",
)
def position_encoding_norm_pyarrow(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    """Normalize position encodings using a PyArrow vectorized UDF.

    Returns
    -------
    pyarrow.Array | pyarrow.ChunkedArray | pyarrow.Scalar
        Normalized position encoding values.
    """
    if isinstance(values, pa.Scalar):
        normalized = normalize_position_encoding(values.as_py())
        return pa.scalar(normalized, type=pa.int32())
    out = [normalize_position_encoding(value) for value in iter_array_values(values)]
    return pa.array(out, type=pa.int32())


@ibis.udf.scalar.python(
    signature=((dt.string,), dt.int32),
    name="position_encoding_norm",
)
def position_encoding_norm_python(value: object | None) -> int:
    """Normalize position encodings using a Python scalar fallback.

    Returns
    -------
    int
        Normalized position encoding value.
    """
    return normalize_position_encoding(value)


@ibis.udf.scalar.builtin(signature=((dt.string, dt.int64, dt.string), dt.int64))
def col_to_byte(_line: Value, offset: Value, _col_unit: Value) -> Value:
    """Convert a line/offset pair into a UTF-8 byte offset.

    Returns
    -------
    ibis.expr.types.Value
        Byte offset expression.
    """
    return offset.cast("int64")


@ibis.udf.scalar.pyarrow(
    signature=((dt.string, dt.int64, dt.string), dt.int64),
    name="col_to_byte",
)
def col_to_byte_pyarrow(
    line_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    offset_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    encoding_values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    """Convert column offsets to byte offsets using a PyArrow vectorized UDF.

    Returns
    -------
    pyarrow.Array | pyarrow.ChunkedArray | pyarrow.Scalar
        Byte offsets for each input element.
    """
    if isinstance(line_values, pa.Scalar):
        return pa.scalar(
            _col_to_byte_scalar(
                line=line_values.as_py(),
                offset=offset_values.as_py() if isinstance(offset_values, pa.Scalar) else None,
                unit=encoding_values.as_py() if isinstance(encoding_values, pa.Scalar) else None,
            ),
            type=pa.int64(),
        )
    length = len(line_values)
    line_iter = iter_array_values(line_values)
    offset_iter = _iter_array_or_scalar(offset_values, length=length)
    encoding_iter = _iter_array_or_scalar(encoding_values, length=length)
    out = [
        _col_to_byte_scalar(line=line, offset=offset, unit=unit)
        for line, offset, unit in zip(line_iter, offset_iter, encoding_iter, strict=True)
    ]
    return pa.array(out, type=pa.int64())


@ibis.udf.scalar.python(
    signature=((dt.string, dt.int64, dt.string), dt.int64),
    name="col_to_byte",
)
def col_to_byte_python(
    line: str | None,
    offset: int | None,
    unit: str | None,
) -> int | None:
    """Convert column offsets to byte offsets using a Python scalar fallback.

    Returns
    -------
    int | None
        Byte offset value or ``None`` for invalid inputs.
    """
    return _col_to_byte_scalar(line=line, offset=offset, unit=unit)


def _hash128_text(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


def _text_or_none(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _iter_array_or_scalar(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
    *,
    length: int,
) -> list[object | None]:
    if isinstance(values, pa.Scalar):
        return [values.as_py()] * length
    return list(iter_array_values(values))


def _col_to_byte_scalar(
    *,
    line: object | None,
    offset: object | None,
    unit: object | None,
) -> int | None:
    if not isinstance(line, str):
        return None
    offset_int = _coerce_int(offset)
    if offset_int is None:
        return None
    unit_name = _normalize_col_unit(unit)
    if unit_name == "byte":
        byte_len = len(line.encode("utf-8"))
        return _clamp_offset(offset_int, byte_len)
    encoding = _encoding_from_unit(unit_name)
    py_index = _code_unit_offset_to_py_index(line, offset_int, encoding)
    py_index = _clamp_offset(py_index, len(line))
    return len(line[:py_index].encode("utf-8"))


def _coerce_int(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        return int(text) if text.isdigit() else None
    return None


def _normalize_col_unit(value: object | None) -> str:
    if isinstance(value, int):
        return _col_unit_from_int(value)
    if isinstance(value, str):
        return _col_unit_from_text(value)
    return "utf32"


def _col_unit_from_int(value: int) -> str:
    encoding_map: dict[int, str] = {
        ENC_UTF8: "utf8",
        ENC_UTF16: "utf16",
        ENC_UTF32: "utf32",
    }
    return encoding_map.get(value, "utf32")


def _col_unit_from_text(value: str) -> str:
    text = value.strip().lower()
    if text.isdigit():
        return _col_unit_from_int(int(text))
    if "byte" in text:
        return "byte"
    if "utf8" in text:
        return "utf8"
    if "utf16" in text:
        return "utf16"
    if "utf32" in text:
        return "utf32"
    return "utf32"


def _encoding_from_unit(unit: str) -> int:
    if unit == "utf8":
        return ENC_UTF8
    if unit == "utf16":
        return ENC_UTF16
    return ENC_UTF32


def _code_unit_offset_to_py_index(line: str, offset: int, encoding: int) -> int:
    if encoding == ENC_UTF32:
        return offset
    if encoding == ENC_UTF8:
        encoded = line.encode("utf-8")
        byte_off = min(offset, len(encoded))
        return len(encoded[:byte_off].decode("utf-8", errors="strict"))
    if encoding == ENC_UTF16:
        encoded = line.encode("utf-16-le")
        byte_off = min(offset * 2, len(encoded))
        return len(encoded[:byte_off].decode("utf-16-le", errors="strict"))
    return min(offset, len(line))


def _clamp_offset(offset: int, limit: int) -> int:
    return max(0, min(offset, limit))


IBIS_UDF_SPECS: tuple[IbisUdfSpec, ...] = (
    IbisUdfSpec(
        func_id="cpg_score",
        engine_name="cpg_score",
        kind="scalar",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility=BUILTIN_UDF_VOLATILITY.get("cpg_score", "stable"),
        arg_names=("value",),
        lanes=("ibis_builtin",),
        rewrite_tags=("score",),
    ),
    IbisUdfSpec(
        func_id="stable_hash64",
        engine_name="stable_hash64",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.int64(),
        arg_names=("value",),
        lanes=("ibis_builtin", "ibis_pyarrow", "ibis_python"),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="stable_hash128",
        engine_name="stable_hash128",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.string(),
        arg_names=("value",),
        lanes=("ibis_builtin", "ibis_pyarrow", "ibis_python"),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="position_encoding_norm",
        engine_name="position_encoding_norm",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.int32(),
        arg_names=("value",),
        lanes=("ibis_builtin", "ibis_pyarrow", "ibis_python"),
        rewrite_tags=("position_encoding",),
    ),
    IbisUdfSpec(
        func_id="col_to_byte",
        engine_name="col_to_byte",
        kind="scalar",
        input_types=(pa.string(), pa.int64(), pa.string()),
        return_type=pa.int64(),
        arg_names=("line_text", "col", "col_unit"),
        lanes=("ibis_builtin", "ibis_pyarrow", "ibis_python"),
        rewrite_tags=("position_encoding",),
    ),
)


def ibis_udf_specs() -> tuple[IbisUdfSpec, ...]:
    """Return the canonical Ibis UDF specs.

    Returns
    -------
    tuple[IbisUdfSpec, ...]
        Canonical Ibis UDF specifications.
    """
    return IBIS_UDF_SPECS


__all__ = [
    "BUILTIN_UDF_VOLATILITY",
    "IBIS_UDF_SPECS",
    "IbisUdfSpec",
    "col_to_byte",
    "col_to_byte_pyarrow",
    "col_to_byte_python",
    "cpg_score",
    "ibis_udf_specs",
    "position_encoding_norm",
    "position_encoding_norm_pyarrow",
    "position_encoding_norm_python",
    "stable_hash64",
    "stable_hash64_pyarrow",
    "stable_hash64_python",
    "stable_hash128",
    "stable_hash128_pyarrow",
    "stable_hash128_python",
]
