"""Fallback Python UDFs for DataFusion extension gaps.

These UDFs provide a deterministic, pure-Python implementation for a minimal
set of CodeAnatomy hashing and normalization primitives when native Rust
extensions are unavailable or incompatible.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from weakref import WeakSet

import pyarrow as pa
from datafusion import Expr, SessionContext, udf

from utils.hashing import hash64_from_text, hash128_from_text

if TYPE_CHECKING:
    from datafusion import ScalarUDF


_NULL_SEPARATOR = "\x1f"


@dataclass(frozen=True)
class FallbackUdfSpec:
    """Metadata for a fallback UDF signature."""

    name: str
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    arg_names: tuple[str, ...]
    volatility: str = "stable"


_SPAN_STRUCT_TYPE = pa.struct(
    [
        pa.field("bstart", pa.int64()),
        pa.field("bend", pa.int64()),
        pa.field("line_base", pa.int64()),
        pa.field("col_unit", pa.string()),
        pa.field("end_exclusive", pa.bool_()),
    ]
)


_FALLBACK_UDF_SPECS: dict[str, FallbackUdfSpec] = {
    "stable_hash64": FallbackUdfSpec(
        name="stable_hash64",
        input_types=(pa.string(),),
        return_type=pa.int64(),
        arg_names=("value",),
        volatility="stable",
    ),
    "stable_hash128": FallbackUdfSpec(
        name="stable_hash128",
        input_types=(pa.string(),),
        return_type=pa.string(),
        arg_names=("value",),
        volatility="stable",
    ),
    "prefixed_hash64": FallbackUdfSpec(
        name="prefixed_hash64",
        input_types=(pa.string(), pa.string()),
        return_type=pa.int64(),
        arg_names=("prefix", "value"),
        volatility="stable",
    ),
    "stable_id": FallbackUdfSpec(
        name="stable_id",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        arg_names=("prefix", "value"),
        volatility="stable",
    ),
    "col_to_byte": FallbackUdfSpec(
        name="col_to_byte",
        input_types=(pa.string(), pa.int64(), pa.string()),
        return_type=pa.int64(),
        arg_names=("line_text", "col_index", "col_unit"),
        volatility="stable",
    ),
    "stable_id_parts": FallbackUdfSpec(
        name="stable_id_parts",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        arg_names=("prefix", "part1"),
        volatility="stable",
    ),
    "prefixed_hash_parts64": FallbackUdfSpec(
        name="prefixed_hash_parts64",
        input_types=(pa.string(), pa.string()),
        return_type=pa.int64(),
        arg_names=("prefix", "part1"),
        volatility="stable",
    ),
    "span_make": FallbackUdfSpec(
        name="span_make",
        input_types=(pa.int64(), pa.int64(), pa.int64(), pa.string(), pa.bool_()),
        return_type=_SPAN_STRUCT_TYPE,
        arg_names=("bstart", "bend", "line_base", "col_unit", "end_exclusive"),
        volatility="stable",
    ),
    "span_overlaps": FallbackUdfSpec(
        name="span_overlaps",
        input_types=(_SPAN_STRUCT_TYPE, _SPAN_STRUCT_TYPE),
        return_type=pa.bool_(),
        arg_names=("span_a", "span_b"),
        volatility="stable",
    ),
    "span_contains": FallbackUdfSpec(
        name="span_contains",
        input_types=(_SPAN_STRUCT_TYPE, _SPAN_STRUCT_TYPE),
        return_type=pa.bool_(),
        arg_names=("span_a", "span_b"),
        volatility="stable",
    ),
    "utf8_normalize": FallbackUdfSpec(
        name="utf8_normalize",
        input_types=(pa.string(),),
        return_type=pa.string(),
        arg_names=("value",),
        volatility="stable",
    ),
    "utf8_null_if_blank": FallbackUdfSpec(
        name="utf8_null_if_blank",
        input_types=(pa.string(),),
        return_type=pa.string(),
        arg_names=("value",),
        volatility="stable",
    ),
    "semantic_tag": FallbackUdfSpec(
        name="semantic_tag",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        arg_names=("semantic_type", "value"),
        volatility="stable",
    ),
    "span_id": FallbackUdfSpec(
        name="span_id",
        input_types=(pa.string(), pa.string(), pa.int64(), pa.int64()),
        return_type=pa.string(),
        arg_names=("prefix", "path", "bstart", "bend"),
        volatility="stable",
    ),
}

_REGISTERABLE_UDFS: frozenset[str] = frozenset(
    {
        "stable_hash64",
        "stable_hash128",
        "prefixed_hash64",
        "stable_id",
        "col_to_byte",
    }
)

_FALLBACK_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_FALLBACK_UDF_OBJECTS: dict[str, ScalarUDF] = {}


def fallback_udf_specs() -> Mapping[str, FallbackUdfSpec]:
    """Return the fallback UDF specification registry.

    Returns:
    -------
    Mapping[str, FallbackUdfSpec]
        Snapshot of fallback UDF specs keyed by name.
    """
    return dict(_FALLBACK_UDF_SPECS)


def fallback_udf_snapshot() -> dict[str, object]:
    """Return a synthetic Rust UDF registry snapshot for fallback mode.

    Returns:
    -------
    dict[str, object]
        Snapshot payload mirroring the Rust UDF registry keys.
    """
    names = _registered_fallback_names()
    param_names: dict[str, tuple[str, ...]] = {}
    volatility: dict[str, str] = {}
    signature_inputs: dict[str, tuple[tuple[str, ...], ...]] = {}
    return_types: dict[str, tuple[str, ...]] = {}
    for name in names:
        spec = _FALLBACK_UDF_SPECS[name]
        param_names[name] = spec.arg_names
        volatility[name] = spec.volatility
        signature_inputs[name] = (tuple(str(dtype) for dtype in spec.input_types),)
        return_types[name] = (str(spec.return_type),)
    return {
        "scalar": list(names),
        "aggregate": [],
        "window": [],
        "table": [],
        "pycapsule_udfs": [],
        "aliases": {},
        "parameter_names": param_names,
        "volatility": volatility,
        "rewrite_tags": {},
        "signature_inputs": signature_inputs,
        "return_types": return_types,
        "simplify": {},
        "coerce_types": {},
        "short_circuits": {},
        "config_defaults": {},
        "custom_udfs": [],
    }


def fallback_udf_names() -> tuple[str, ...]:
    """Return the registered fallback UDF names.

    Returns:
    -------
    tuple[str, ...]
        Registered fallback UDF names.
    """
    return _registered_fallback_names()


def register_fallback_udfs(ctx: SessionContext) -> None:
    """Register fallback UDFs on the provided session context."""
    if ctx in _FALLBACK_UDF_CONTEXTS:
        return
    for name in _registered_fallback_names():
        spec = _FALLBACK_UDF_SPECS[name]
        func = _fallback_udf_function(name)
        if func is None:
            continue
        udf_obj = udf(func, list(spec.input_types), spec.return_type, spec.volatility, name)
        ctx.register_udf(udf_obj)
        _FALLBACK_UDF_OBJECTS[name] = udf_obj
    _FALLBACK_UDF_CONTEXTS.add(ctx)


def _registered_fallback_names() -> tuple[str, ...]:
    names = [
        name
        for name in _REGISTERABLE_UDFS
        if name in _FALLBACK_UDF_SPECS and _fallback_udf_function(name) is not None
    ]
    return tuple(sorted(names))


def fallback_expr(name: str, *args: object, **kwargs: object) -> Expr | None:
    """Return an Expr backed by a fallback UDF, when available.

    Returns:
    -------
    Expr | None
        Expression backed by a fallback UDF, or ``None`` when unavailable.
    """
    if kwargs:
        return None
    udf_obj = _FALLBACK_UDF_OBJECTS.get(name)
    if udf_obj is None:
        func = _fallback_udf_function(name)
        if func is None:
            return None
        spec = _FALLBACK_UDF_SPECS.get(name)
        if spec is None:
            return None
        udf_obj = udf(func, list(spec.input_types), spec.return_type, spec.volatility, name)
        _FALLBACK_UDF_OBJECTS[name] = udf_obj
    expr_args = [_as_expr(arg) for arg in args]
    return udf_obj(*expr_args)


def _as_expr(value: object) -> Expr:
    if isinstance(value, Expr):
        return value
    return Expr.literal(value)


def _as_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _values(value: object) -> list[object]:
    if isinstance(value, pa.ChunkedArray):
        chunked = cast("pa.ChunkedArray", value)
        return [item for chunk in chunked.chunks for item in chunk.to_pylist()]
    if isinstance(value, pa.Array):
        array_value = cast("pa.Array", value)
        return array_value.to_pylist()
    if isinstance(value, pa.Scalar):
        scalar_value = cast("pa.Scalar", value)
        return [scalar_value.as_py()]
    if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, bytearray)):
        return list(cast("Iterable[object]", value))
    return [value]


def _zip_values(*values: object) -> list[tuple[object, ...]]:
    lists = [_values(value) for value in values]
    if not lists:
        return []
    max_len = max(len(items) for items in lists)
    rows: list[tuple[object, ...]] = []
    for index in range(max_len):
        row: list[object] = []
        for items in lists:
            if len(items) == 1:
                row.append(items[0])
            elif index < len(items):
                row.append(items[index])
            else:
                row.append(None)
        rows.append(tuple(row))
    return rows


def _stable_hash64_udf(value: object) -> pa.Array:
    values = _values(value)
    hashed: list[int | None] = []
    for item in values:
        text = _as_text(item)
        if text is None:
            hashed.append(None)
            continue
        hashed.append(hash64_from_text(text))
    return pa.array(hashed, type=pa.int64())


def _stable_hash128_udf(value: object) -> pa.Array:
    values = _values(value)
    hashed: list[str | None] = []
    for item in values:
        text = _as_text(item)
        if text is None:
            hashed.append(None)
            continue
        hashed.append(hash128_from_text(text))
    return pa.array(hashed, type=pa.string())


def _prefixed_hash64_udf(prefix: object, value: object) -> pa.Array:
    rows = _zip_values(prefix, value)
    hashed: list[int | None] = []
    for prefix_value, data_value in rows:
        text_prefix = _as_text(prefix_value)
        if text_prefix is None:
            hashed.append(None)
            continue
        text_value = _as_text(data_value)
        if text_value is None:
            text_value = "None"
        joined = f"{text_prefix}{_NULL_SEPARATOR}{text_value}"
        hashed.append(hash64_from_text(joined))
    return pa.array(hashed, type=pa.int64())


def _stable_id_udf(prefix: object, value: object) -> pa.Array:
    rows = _zip_values(prefix, value)
    ids: list[str | None] = []
    for prefix_value, data_value in rows:
        text_prefix = _as_text(prefix_value)
        if text_prefix is None:
            ids.append(None)
            continue
        text_value = _as_text(data_value)
        if text_value is None:
            text_value = "None"
        joined = _NULL_SEPARATOR.join([text_prefix, text_value])
        ids.append(f"{text_prefix}:{hash128_from_text(joined)}")
    return pa.array(ids, type=pa.string())


def _col_to_byte_udf(
    line_text: object,
    col_index: object,
    col_unit: object,
) -> pa.Array:
    rows = _zip_values(line_text, col_index, col_unit)
    offsets: list[int | None] = []
    for line_value, col_value, unit_value in rows:
        offsets.append(_col_to_byte(line_value, col_value, unit_value))
    return pa.array(offsets, type=pa.int64())


def _col_to_byte(line_text: object, col_index: object, col_unit: object) -> int | None:
    text = _as_text(line_text)
    if text is None or col_index is None:
        return None
    try:
        if isinstance(col_index, bool):
            return None
        if isinstance(col_index, (int, float, str, bytes, bytearray)):
            index = int(col_index)
        else:
            return None
    except (TypeError, ValueError):
        return None
    if index < 0:
        return None
    unit = _as_text(col_unit)
    if unit is None:
        unit = "byte"
    normalized = unit.lower()
    if normalized in {"byte", "bytes"}:
        result = index
    elif normalized in {"utf8", "utf-8", "utf32", "utf-32"}:
        result = len(text[:index].encode("utf-8"))
    elif normalized in {"utf16", "utf-16"}:
        result = _utf16_index_to_byte(text, index)
    else:
        result = index
    return result


def _utf16_index_to_byte(text: str, utf16_index: int) -> int:
    units = 0
    byte_count = 0
    for char in text:
        char_units = len(char.encode("utf-16-le")) // 2
        if units + char_units > utf16_index:
            break
        units += char_units
        byte_count += len(char.encode("utf-8"))
        if units == utf16_index:
            break
    return byte_count


_FALLBACK_UDF_FUNCTIONS: dict[str, Callable[..., pa.Array]] = {
    "stable_hash64": _stable_hash64_udf,
    "stable_hash128": _stable_hash128_udf,
    "prefixed_hash64": _prefixed_hash64_udf,
    "stable_id": _stable_id_udf,
    "col_to_byte": _col_to_byte_udf,
}


def _fallback_udf_function(name: str) -> Callable[..., pa.Array] | None:
    return _FALLBACK_UDF_FUNCTIONS.get(name)


__all__ = [
    "fallback_expr",
    "fallback_udf_snapshot",
    "fallback_udf_specs",
    "register_fallback_udfs",
]
