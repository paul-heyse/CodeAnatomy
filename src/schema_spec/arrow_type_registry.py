"""Registry-based Arrow type conversion helpers."""

from __future__ import annotations

from collections.abc import Callable

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
    ArrowPrimitiveSpec,
    ArrowStructSpec,
    ArrowTimestampSpec,
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
    dict_dtype = pa.dictionary(dict_dtype.index_type, dict_dtype.value_type, ordered=dict_dtype.ordered)
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
    return ArrowFixedSizeListSpec(
        item=ArrowFieldSpec.from_pyarrow(list_dtype.value_field),
        list_size=list_dtype.list_size,
    )


def _list_spec(dtype: pa.DataType) -> ArrowListSpec:
    list_dtype = dtype
    if not patypes.is_list(list_dtype):
        msg = f"Expected list dtype, got {dtype!r}."
        raise TypeError(msg)
    return ArrowListSpec(item=ArrowFieldSpec.from_pyarrow(list_dtype.value_field))


def _large_list_spec(dtype: pa.DataType) -> ArrowLargeListSpec:
    list_dtype = dtype
    if not patypes.is_large_list(list_dtype):
        msg = f"Expected large list dtype, got {dtype!r}."
        raise TypeError(msg)
    return ArrowLargeListSpec(item=ArrowFieldSpec.from_pyarrow(list_dtype.value_field))


def _map_spec(dtype: pa.DataType) -> ArrowMapSpec:
    map_dtype = dtype
    if not patypes.is_map(map_dtype):
        msg = f"Expected map dtype, got {dtype!r}."
        raise TypeError(msg)
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
    return ArrowStructSpec(fields=tuple(ArrowFieldSpec.from_pyarrow(field) for field in struct_dtype))


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
    (patypes.is_time32, lambda dtype: ArrowPrimitiveSpec(name=f"time32[{dtype.unit}]")),
    (patypes.is_time64, lambda dtype: ArrowPrimitiveSpec(name=f"time64[{dtype.unit}]")),
    (patypes.is_timestamp, lambda dtype: ArrowTimestampSpec(unit=dtype.unit, timezone=dtype.tz)),
    (patypes.is_duration, lambda dtype: ArrowDurationSpec(unit=dtype.unit)),
    (patypes.is_decimal, _decimal_spec_from_dtype),
    (patypes.is_fixed_size_binary, lambda dtype: ArrowFixedSizeBinarySpec(byte_width=dtype.byte_width)),
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


_TO_PYARROW: dict[type[ArrowTypeSpec], Callable[[ArrowTypeSpec], pa.DataType]] = {
    ArrowPrimitiveSpec: lambda spec: _PRIMITIVE_BUILDERS[spec.name],
    ArrowTimestampSpec: lambda spec: pa.timestamp(spec.unit, tz=spec.timezone),
    ArrowDurationSpec: lambda spec: pa.duration(spec.unit),
    ArrowDecimalSpec: _decimal_dtype_from_spec,
    ArrowFixedSizeBinarySpec: lambda spec: pa.binary(spec.byte_width),
    ArrowFixedSizeListSpec: lambda spec: pa.list_(spec.item.to_pyarrow(), list_size=spec.list_size),
    ArrowLargeListSpec: lambda spec: pa.large_list(spec.item.to_pyarrow()),
    ArrowListSpec: lambda spec: pa.list_(spec.item.to_pyarrow()),
    ArrowMapSpec: lambda spec: pa.map_(
        spec.key.to_pyarrow(),
        spec.item.to_pyarrow(),
        keys_sorted=spec.keys_sorted,
    ),
    ArrowStructSpec: lambda spec: pa.struct([field.to_pyarrow() for field in spec.fields]),
    ArrowDictionarySpec: _dictionary_dtype_from_spec,
    ArrowOpaqueSpec: lambda spec: pa.data_type(spec.repr),
}


def arrow_type_to_pyarrow(spec: ArrowTypeSpec) -> pa.DataType:
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
