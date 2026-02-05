"""Registry-based Arrow type conversion helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from schema_spec.arrow_types import (
    _PRIMITIVE_BUILDERS,
    ArrowDecimalSpec,
    ArrowDictionarySpec,
    ArrowDurationSpec,
    ArrowFieldSpec,
    ArrowFixedSizeBinarySpec,
    ArrowFixedSizeListSpec,
    ArrowLargeListSpec,
    ArrowListSpec,
    ArrowMapSpec,
    ArrowOpaqueSpec,
    ArrowPrimitiveName,
    ArrowPrimitiveSpec,
    ArrowStructSpec,
    ArrowTimestampSpec,
    ArrowTypeBase,
    ArrowTypeSpec,
)

DECIMAL_BIT_WIDTH_128 = 128
DECIMAL_BIT_WIDTH_256 = 256


def _decimal_spec_from_dtype(dtype: pa.DataType) -> ArrowDecimalSpec:
    bit_width = DECIMAL_BIT_WIDTH_128
    if dtype.bit_width == DECIMAL_BIT_WIDTH_256:
        bit_width = DECIMAL_BIT_WIDTH_256
    return ArrowDecimalSpec(precision=dtype.precision, scale=dtype.scale, bit_width=bit_width)


def _dictionary_spec_from_dtype(dtype: pa.DataType) -> ArrowDictionarySpec:
    dict_dtype = dtype
    if not patypes.is_dictionary(dict_dtype):
        msg = f"Expected dictionary dtype, got {dtype!r}."
        raise TypeError(msg)
    dict_dtype = cast("pa.DictionaryType", dict_dtype)
    dict_dtype = pa.dictionary(
        dict_dtype.index_type,
        dict_dtype.value_type,
        ordered=dict_dtype.ordered,
    )
    return ArrowDictionarySpec(
        index_type=arrow_type_from_pyarrow(dict_dtype.index_type),
        value_type=arrow_type_from_pyarrow(dict_dtype.value_type),
        ordered=dict_dtype.ordered,
    )


def _fixed_size_list_spec(dtype: pa.DataType) -> ArrowFixedSizeListSpec:
    list_dtype = dtype
    if not patypes.is_fixed_size_list(list_dtype):
        msg = f"Expected fixed-size list dtype, got {dtype!r}."
        raise TypeError(msg)
    list_dtype = cast("pa.FixedSizeListType", list_dtype)
    return ArrowFixedSizeListSpec(
        item=ArrowFieldSpec.from_pyarrow(list_dtype.value_field),
        list_size=list_dtype.list_size,
    )


def _list_spec(dtype: pa.DataType) -> ArrowListSpec:
    list_dtype = dtype
    if not patypes.is_list(list_dtype):
        msg = f"Expected list dtype, got {dtype!r}."
        raise TypeError(msg)
    list_dtype = cast("pa.ListType", list_dtype)
    return ArrowListSpec(item=ArrowFieldSpec.from_pyarrow(list_dtype.value_field))


def _large_list_spec(dtype: pa.DataType) -> ArrowLargeListSpec:
    list_dtype = dtype
    if not patypes.is_large_list(list_dtype):
        msg = f"Expected large list dtype, got {dtype!r}."
        raise TypeError(msg)
    list_dtype = cast("pa.LargeListType", list_dtype)
    return ArrowLargeListSpec(item=ArrowFieldSpec.from_pyarrow(list_dtype.value_field))


def _map_spec(dtype: pa.DataType) -> ArrowMapSpec:
    map_dtype = dtype
    if not patypes.is_map(map_dtype):
        msg = f"Expected map dtype, got {dtype!r}."
        raise TypeError(msg)
    map_dtype = cast("pa.MapType", map_dtype)
    return ArrowMapSpec(
        key=ArrowFieldSpec.from_pyarrow(map_dtype.key_field),
        item=ArrowFieldSpec.from_pyarrow(map_dtype.item_field),
        keys_sorted=map_dtype.keys_sorted,
    )


def _struct_spec(dtype: pa.DataType) -> ArrowStructSpec:
    struct_dtype = dtype
    if not patypes.is_struct(struct_dtype):
        msg = f"Expected struct dtype, got {dtype!r}."
        raise TypeError(msg)
    struct_dtype = cast("pa.StructType", struct_dtype)
    return ArrowStructSpec(
        fields=tuple(ArrowFieldSpec.from_pyarrow(field) for field in struct_dtype)
    )


def _time32_spec(dtype: pa.DataType) -> ArrowPrimitiveSpec:
    time_dtype = cast("pa.Time32Type", dtype)
    name = cast("ArrowPrimitiveName", f"time32[{time_dtype.unit}]")
    return ArrowPrimitiveSpec(name=name)


def _time64_spec(dtype: pa.DataType) -> ArrowPrimitiveSpec:
    time_dtype = cast("pa.Time64Type", dtype)
    name = cast("ArrowPrimitiveName", f"time64[{time_dtype.unit}]")
    return ArrowPrimitiveSpec(name=name)


def _timestamp_spec(dtype: pa.DataType) -> ArrowTimestampSpec:
    ts_dtype = cast("pa.TimestampType", dtype)
    return ArrowTimestampSpec(unit=ts_dtype.unit, timezone=ts_dtype.tz)


def _duration_spec(dtype: pa.DataType) -> ArrowDurationSpec:
    duration_dtype = cast("pa.DurationType", dtype)
    return ArrowDurationSpec(unit=duration_dtype.unit)


def _fixed_size_binary_spec(dtype: pa.DataType) -> ArrowFixedSizeBinarySpec:
    fixed_dtype = cast("pa.FixedSizeBinaryType", dtype)
    return ArrowFixedSizeBinarySpec(byte_width=fixed_dtype.byte_width)


_FROM_PYARROW: tuple[
    tuple[Callable[[pa.DataType], bool], Callable[[pa.DataType], ArrowTypeSpec]],
    ...,
] = (
    (patypes.is_null, lambda _: ArrowPrimitiveSpec(name="null")),
    (patypes.is_boolean, lambda _: ArrowPrimitiveSpec(name="bool")),
    (patypes.is_int8, lambda _: ArrowPrimitiveSpec(name="int8")),
    (patypes.is_int16, lambda _: ArrowPrimitiveSpec(name="int16")),
    (patypes.is_int32, lambda _: ArrowPrimitiveSpec(name="int32")),
    (patypes.is_int64, lambda _: ArrowPrimitiveSpec(name="int64")),
    (patypes.is_uint8, lambda _: ArrowPrimitiveSpec(name="uint8")),
    (patypes.is_uint16, lambda _: ArrowPrimitiveSpec(name="uint16")),
    (patypes.is_uint32, lambda _: ArrowPrimitiveSpec(name="uint32")),
    (patypes.is_uint64, lambda _: ArrowPrimitiveSpec(name="uint64")),
    (patypes.is_float16, lambda _: ArrowPrimitiveSpec(name="float16")),
    (patypes.is_float32, lambda _: ArrowPrimitiveSpec(name="float32")),
    (patypes.is_float64, lambda _: ArrowPrimitiveSpec(name="float64")),
    (patypes.is_string, lambda _: ArrowPrimitiveSpec(name="string")),
    (patypes.is_large_string, lambda _: ArrowPrimitiveSpec(name="large_string")),
    (patypes.is_binary, lambda _: ArrowPrimitiveSpec(name="binary")),
    (patypes.is_large_binary, lambda _: ArrowPrimitiveSpec(name="large_binary")),
    (patypes.is_date32, lambda _: ArrowPrimitiveSpec(name="date32")),
    (patypes.is_date64, lambda _: ArrowPrimitiveSpec(name="date64")),
    (patypes.is_time32, _time32_spec),
    (patypes.is_time64, _time64_spec),
    (patypes.is_timestamp, _timestamp_spec),
    (patypes.is_duration, _duration_spec),
    (patypes.is_decimal, _decimal_spec_from_dtype),
    (patypes.is_fixed_size_binary, _fixed_size_binary_spec),
    (patypes.is_fixed_size_list, _fixed_size_list_spec),
    (patypes.is_large_list, _large_list_spec),
    (patypes.is_list, _list_spec),
    (patypes.is_map, _map_spec),
    (patypes.is_struct, _struct_spec),
    (patypes.is_dictionary, _dictionary_spec_from_dtype),
)


def arrow_type_from_pyarrow(dtype: pa.DataType) -> ArrowTypeSpec:
    """Return a serializable ArrowTypeSpec from a pyarrow dtype.

    Returns
    -------
    ArrowTypeSpec
        Serializable Arrow type specification.
    """
    for predicate, builder in _FROM_PYARROW:
        if predicate(dtype):
            return builder(dtype)
    return ArrowOpaqueSpec(repr=str(dtype))


def _decimal_dtype_from_spec(spec: ArrowDecimalSpec) -> pa.DataType:
    if spec.bit_width == DECIMAL_BIT_WIDTH_256:
        return pa.decimal256(spec.precision, spec.scale)
    return pa.decimal128(spec.precision, spec.scale)


def _dictionary_dtype_from_spec(spec: ArrowDictionarySpec) -> pa.DataType:
    return pa.dictionary(
        arrow_type_to_pyarrow(spec.index_type),
        arrow_type_to_pyarrow(spec.value_type),
        ordered=spec.ordered,
    )


def _primitive_dtype(spec: ArrowTypeBase) -> pa.DataType:
    primitive = cast("ArrowPrimitiveSpec", spec)
    return _PRIMITIVE_BUILDERS[primitive.name]


def _timestamp_dtype(spec: ArrowTypeBase) -> pa.DataType:
    timestamp = cast("ArrowTimestampSpec", spec)
    return pa.timestamp(timestamp.unit, tz=timestamp.timezone)


def _duration_dtype(spec: ArrowTypeBase) -> pa.DataType:
    duration = cast("ArrowDurationSpec", spec)
    return pa.duration(duration.unit)


def _decimal_dtype(spec: ArrowTypeBase) -> pa.DataType:
    return _decimal_dtype_from_spec(cast("ArrowDecimalSpec", spec))


def _fixed_size_binary_dtype(spec: ArrowTypeBase) -> pa.DataType:
    fixed = cast("ArrowFixedSizeBinarySpec", spec)
    return pa.binary(fixed.byte_width)


def _fixed_size_list_dtype(spec: ArrowTypeBase) -> pa.DataType:
    fixed = cast("ArrowFixedSizeListSpec", spec)
    return pa.list_(fixed.item.to_pyarrow(), list_size=fixed.list_size)


def _large_list_dtype(spec: ArrowTypeBase) -> pa.DataType:
    large = cast("ArrowLargeListSpec", spec)
    return pa.large_list(large.item.to_pyarrow())


def _list_dtype(spec: ArrowTypeBase) -> pa.DataType:
    list_spec = cast("ArrowListSpec", spec)
    return pa.list_(list_spec.item.to_pyarrow())


def _map_dtype(spec: ArrowTypeBase) -> pa.DataType:
    map_spec = cast("ArrowMapSpec", spec)
    return pa.map_(
        map_spec.key.to_pyarrow(),
        map_spec.item.to_pyarrow(),
        keys_sorted=map_spec.keys_sorted,
    )


def _struct_dtype(spec: ArrowTypeBase) -> pa.DataType:
    struct_spec = cast("ArrowStructSpec", spec)
    return pa.struct([field.to_pyarrow() for field in struct_spec.fields])


def _dictionary_dtype(spec: ArrowTypeBase) -> pa.DataType:
    return _dictionary_dtype_from_spec(cast("ArrowDictionarySpec", spec))


def _opaque_dtype(spec: ArrowTypeBase) -> pa.DataType:
    opaque = cast("ArrowOpaqueSpec", spec)
    try:
        return pa.type_for_alias(opaque.repr)
    except (KeyError, TypeError, ValueError) as exc:
        msg = f"Unsupported opaque Arrow type: {opaque.repr}"
        raise ValueError(msg) from exc


_TO_PYARROW: dict[type[ArrowTypeBase], Callable[[ArrowTypeBase], pa.DataType]] = {
    ArrowPrimitiveSpec: _primitive_dtype,
    ArrowTimestampSpec: _timestamp_dtype,
    ArrowDurationSpec: _duration_dtype,
    ArrowDecimalSpec: _decimal_dtype,
    ArrowFixedSizeBinarySpec: _fixed_size_binary_dtype,
    ArrowFixedSizeListSpec: _fixed_size_list_dtype,
    ArrowLargeListSpec: _large_list_dtype,
    ArrowListSpec: _list_dtype,
    ArrowMapSpec: _map_dtype,
    ArrowStructSpec: _struct_dtype,
    ArrowDictionarySpec: _dictionary_dtype,
    ArrowOpaqueSpec: _opaque_dtype,
}


def arrow_type_to_pyarrow(spec: ArrowTypeSpec | ArrowTypeBase) -> pa.DataType:
    """Return pyarrow dtype for an ArrowTypeSpec.

    Returns
    -------
    pyarrow.DataType
        PyArrow data type for the spec.

    Raises
    ------
    TypeError
        Raised when the spec is unsupported.
    """
    builder = _TO_PYARROW.get(type(spec))
    if builder is not None:
        return builder(spec)
    msg = f"Unsupported Arrow type spec: {spec!r}"
    raise TypeError(msg)


__all__ = [
    "DECIMAL_BIT_WIDTH_128",
    "DECIMAL_BIT_WIDTH_256",
    "arrow_type_from_pyarrow",
    "arrow_type_to_pyarrow",
]
