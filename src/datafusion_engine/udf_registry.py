"""Shared DataFusion UDF registry helpers."""

from __future__ import annotations

import hashlib
from itertools import repeat
from weakref import WeakSet

import pyarrow as pa
from datafusion import SessionContext, udf

from arrowdsl.compute.expr_core import ENC_UTF8, ENC_UTF16, ENC_UTF32, normalize_position_encoding
from arrowdsl.core.ids import hash64_from_text, iter_array_values
from arrowdsl.core.interop import pc

_NUMERIC_REGEX = r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
_KERNEL_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()


def _normalize_span(values: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    text = pc.utf8_trim(pc.cast(values, pa.string(), safe=False))
    mask = pc.match_substring_regex(text, _NUMERIC_REGEX)
    mask = pc.fill_null(mask, fill_value=False)
    sanitized = pc.if_else(mask, text, pa.scalar(None, type=pa.string()))
    numeric = pc.cast(sanitized, pa.float64(), safe=False)
    return pc.cast(numeric, pa.int64(), safe=False)


def _stable_hash64(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        if value is None:
            return pa.scalar(None, type=pa.int64())
        hashed = hash64_from_text(str(value))
        return pa.scalar(hashed, type=pa.int64())
    out = [
        hash64_from_text(str(value)) if value is not None else None
        for value in iter_array_values(values)
    ]
    return pa.array(out, type=pa.int64())


def _hash128_text(value: str) -> str:
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


def _stable_hash128(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = values.as_py()
        if value is None:
            return pa.scalar(None, type=pa.string())
        return pa.scalar(_hash128_text(str(value)), type=pa.string())
    out = [
        _hash128_text(str(value)) if value is not None else None
        for value in iter_array_values(values)
    ]
    return pa.array(out, type=pa.string())


def _position_encoding_norm(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(values, pa.Scalar):
        value = normalize_position_encoding(values.as_py())
        return pa.scalar(value, type=pa.int32())
    out = [normalize_position_encoding(value) for value in iter_array_values(values)]
    return pa.array(out, type=pa.int32())


def _coerce_int(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        return int(raw) if raw.isdigit() else None
    return None


def _code_unit_offset_to_py_index(line: str, offset: int, position_encoding: int) -> int:
    if position_encoding == ENC_UTF32:
        return offset
    if position_encoding == ENC_UTF8:
        encoded = line.encode("utf-8")
        byte_off = min(offset, len(encoded))
        return len(encoded[:byte_off].decode("utf-8", errors="strict"))
    if position_encoding == ENC_UTF16:
        encoded = line.encode("utf-16-le")
        byte_off = min(offset * 2, len(encoded))
        return len(encoded[:byte_off].decode("utf-16-le", errors="strict"))
    return min(offset, len(line))


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


def _clamp_offset(offset: int, limit: int) -> int:
    return max(0, min(offset, limit))


def _col_to_byte(
    line_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    offset_values: pa.Array | pa.ChunkedArray | pa.Scalar,
    encoding_values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    if isinstance(line_values, pa.Scalar):
        line = line_values.as_py()
        offset_value = offset_values.as_py() if isinstance(offset_values, pa.Scalar) else None
        offset = _coerce_int(offset_value)
        unit = _normalize_col_unit(
            encoding_values.as_py() if isinstance(encoding_values, pa.Scalar) else None
        )
        if not isinstance(line, str) or offset is None:
            return pa.scalar(None, type=pa.int64())
        if unit == "byte":
            byte_len = len(line.encode("utf-8"))
            return pa.scalar(_clamp_offset(offset, byte_len), type=pa.int64())
        enc = _encoding_from_unit(unit)
        py_index = _code_unit_offset_to_py_index(line, offset, enc)
        py_index = _clamp_offset(py_index, len(line))
        return pa.scalar(len(line[:py_index].encode("utf-8")), type=pa.int64())

    length = len(line_values)
    line_iter = iter_array_values(line_values)
    offset_iter = (
        repeat(offset_values.as_py(), length)
        if isinstance(offset_values, pa.Scalar)
        else iter_array_values(offset_values)
    )
    encoding_iter = (
        repeat(encoding_values.as_py(), length)
        if isinstance(encoding_values, pa.Scalar)
        else iter_array_values(encoding_values)
    )
    out: list[int | None] = []
    for line_value, offset_value, encoding_value in zip(
        line_iter,
        offset_iter,
        encoding_iter,
        strict=True,
    ):
        if not isinstance(line_value, str):
            out.append(None)
            continue
        offset = _coerce_int(offset_value)
        unit = _normalize_col_unit(encoding_value)
        if offset is None:
            out.append(None)
            continue
        if unit == "byte":
            byte_len = len(line_value.encode("utf-8"))
            out.append(_clamp_offset(offset, byte_len))
            continue
        enc = _encoding_from_unit(unit)
        py_index = _code_unit_offset_to_py_index(line_value, offset, enc)
        py_index = _clamp_offset(py_index, len(line_value))
        out.append(len(line_value[:py_index].encode("utf-8")))
    return pa.array(out, type=pa.int64())


_NORMALIZE_SPAN_UDF = udf(
    _normalize_span,
    [pa.string()],
    pa.int64(),
    "stable",
    "normalize_span",
)
_STABLE_HASH64_UDF = udf(
    _stable_hash64,
    [pa.string()],
    pa.int64(),
    "stable",
    "stable_hash64",
)
_STABLE_HASH128_UDF = udf(
    _stable_hash128,
    [pa.string()],
    pa.string(),
    "stable",
    "stable_hash128",
)
_POSITION_ENCODING_NORM_UDF = udf(
    _position_encoding_norm,
    [pa.int32()],
    pa.int32(),
    "stable",
    "position_encoding_norm",
)
_COL_TO_BYTE_UDF = udf(
    _col_to_byte,
    [pa.string(), pa.int64(), pa.string()],
    pa.int64(),
    "stable",
    "col_to_byte",
)


def _register_kernel_udfs(ctx: SessionContext) -> None:
    if ctx in _KERNEL_UDF_CONTEXTS:
        return
    ctx.register_udf(_NORMALIZE_SPAN_UDF)
    ctx.register_udf(_STABLE_HASH64_UDF)
    ctx.register_udf(_STABLE_HASH128_UDF)
    ctx.register_udf(_POSITION_ENCODING_NORM_UDF)
    ctx.register_udf(_COL_TO_BYTE_UDF)
    _KERNEL_UDF_CONTEXTS.add(ctx)


def register_datafusion_udfs(ctx: SessionContext) -> None:
    """Register shared DataFusion UDFs in the provided session context."""
    _register_kernel_udfs(ctx)


__all__ = [
    "_NORMALIZE_SPAN_UDF",
    "_register_kernel_udfs",
    "register_datafusion_udfs",
]
