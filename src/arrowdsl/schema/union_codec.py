"""Arrow union struct codec for Delta-safe storage."""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import ScalarLike
from arrowdsl.schema.nested_builders import (
    build_struct,
    nested_array_factory,
    union_array_from_tagged_values,
)

UNION_TAG_FIELD = "__union_tag__"
UNION_VALUE_FIELD = "value"
UNION_ENCODING_META = b"arrowdsl.union_encoding"
UNION_TYPE_META = b"arrowdsl.union_type"
UNION_ENCODING_STRUCT = b"struct"


@dataclass(frozen=True)
class _UnionEncodeInputs:
    child_fields: list[pa.Field]
    encoded_child_arrays: list[pa.Array | pa.ChunkedArray]
    stored_children: list[tuple[pa.Field, pa.Field, pa.Array | pa.ChunkedArray]]
    stored_child_names: set[str]
    code_to_index: Mapping[int, int]
    type_ids: list[object]
    valid_mask: list[object]
    offsets: list[object] | None


def encode_union_table(table: pa.Table) -> pa.Table:
    """Return a table with union-typed columns encoded as structs.

    Returns
    -------
    pyarrow.Table
        Table with union columns converted into struct-encoded representations.
    """
    if not schema_has_union_types(table.schema):
        return table
    encoded_schema = encode_union_schema(table.schema)
    columns: list[pa.ChunkedArray] = []
    for field in table.schema:
        encoded = _encode_array(table.column(field.name), field=field)
        columns.append(_coerce_chunked(encoded, dtype=encoded_schema.field(field.name).type))
    return pa.Table.from_arrays(columns, schema=encoded_schema)


def decode_union_table(table: pa.Table) -> pa.Table:
    """Return a table with union-encoded struct columns decoded.

    Returns
    -------
    pyarrow.Table
        Table with union columns restored from struct-encoded representations.
    """
    if not schema_has_union_encoding(table.schema):
        return table
    decoded_schema = decode_union_schema(table.schema)
    columns: list[pa.ChunkedArray] = []
    for field in table.schema:
        decoded = _decode_array(table.column(field.name), field=field)
        columns.append(_coerce_chunked(decoded, dtype=decoded_schema.field(field.name).type))
    return pa.Table.from_arrays(columns, schema=decoded_schema)


def encode_union_schema(schema: pa.Schema) -> pa.Schema:
    """Return a schema with union types encoded as structs.

    Returns
    -------
    pyarrow.Schema
        Schema with union types replaced by struct encodings.
    """
    fields = [_encode_field(field) for field in schema]
    return pa.schema(fields, metadata=schema.metadata)


def decode_union_schema(schema: pa.Schema) -> pa.Schema:
    """Return a schema with union encodings decoded into union types.

    Returns
    -------
    pyarrow.Schema
        Schema with union-encoded struct fields decoded to unions.
    """
    fields = [_decode_field(field) for field in schema]
    return pa.schema(fields, metadata=schema.metadata)


def schema_has_union_encoding(schema: pa.Schema) -> bool:
    """Return True when a schema contains union-encoded fields.

    Returns
    -------
    bool
        True when union-encoded fields are present.
    """
    return any(_field_has_union_encoding(field) for field in schema)


def schema_has_union_types(schema: pa.Schema) -> bool:
    """Return True when a schema contains Arrow union types.

    Returns
    -------
    bool
        True when union-typed fields are present.
    """
    return any(_type_has_union(field.type) for field in schema)


def _encode_array(
    array: pa.ChunkedArray | pa.Array,
    *,
    field: pa.Field,
) -> pa.ChunkedArray | pa.Array:
    dtype = field.type
    if isinstance(array, pa.ChunkedArray):
        chunks = [_encode_array(chunk, field=field) for chunk in array.chunks]
        return pa.chunked_array(chunks, type=_encode_type(dtype))
    return _encode_array_dispatch(array, field=field)


def _decode_array(
    array: pa.ChunkedArray | pa.Array,
    *,
    field: pa.Field,
) -> pa.ChunkedArray | pa.Array:
    dtype = field.type
    if isinstance(array, pa.ChunkedArray):
        chunks = [_decode_array(chunk, field=field) for chunk in array.chunks]
        decoded_type = (
            _decode_union_type(field) if _is_union_encoded_field(field) else _decode_type(dtype)
        )
        return pa.chunked_array(chunks, type=decoded_type)
    return _decode_array_dispatch(array, field=field)


def _encode_array_dispatch(array: pa.Array, *, field: pa.Field) -> pa.Array:
    dtype = field.type
    dispatch: tuple[tuple[Callable[[pa.DataType], bool], Callable[..., pa.Array]], ...] = (
        (patypes.is_union, _encode_union_array),
        (patypes.is_struct, _encode_struct_array),
        (patypes.is_fixed_size_list, _encode_fixed_size_list_array),
        (patypes.is_list, _encode_list_array),
        (patypes.is_large_list, _encode_large_list_array),
        (patypes.is_list_view, _encode_list_view_array),
        (patypes.is_large_list_view, _encode_large_list_view_array),
        (patypes.is_map, _encode_map_array),
        (patypes.is_dictionary, _encode_dictionary_array),
    )
    for predicate, handler in dispatch:
        if predicate(dtype):
            return handler(array, field=field)
    return array


def _decode_array_dispatch(array: pa.Array, *, field: pa.Field) -> pa.Array:
    dtype = field.type
    if _is_union_encoded_field(field):
        return _decode_union_array(array, field=field)
    dispatch: tuple[tuple[Callable[[pa.DataType], bool], Callable[..., pa.Array]], ...] = (
        (patypes.is_struct, _decode_struct_array),
        (patypes.is_fixed_size_list, _decode_fixed_size_list_array),
        (patypes.is_list, _decode_list_array),
        (patypes.is_large_list, _decode_large_list_array),
        (patypes.is_list_view, _decode_list_view_array),
        (patypes.is_large_list_view, _decode_large_list_view_array),
        (patypes.is_map, _decode_map_array),
        (patypes.is_dictionary, _decode_dictionary_array),
    )
    for predicate, handler in dispatch:
        if predicate(dtype):
            return handler(array, field=field)
    return array


def _encode_union_array(array: pa.Array, *, field: pa.Field) -> pa.StructArray:
    union_type = cast("pa.UnionType", field.type)
    if not isinstance(array, pa.UnionArray):
        msg = "Expected UnionArray for union-typed field."
        raise TypeError(msg)
    child_fields = list(union_type)
    encoded_child_fields = [_encode_field(child) for child in child_fields]
    encoded_child_arrays = _encode_union_child_arrays(array, child_fields)
    stored_children = _filter_union_children(
        child_fields,
        encoded_child_fields,
        encoded_child_arrays,
    )
    encode_inputs = _build_union_encode_inputs(
        array=array,
        union_type=union_type,
        child_fields=child_fields,
        encoded_child_arrays=encoded_child_arrays,
        stored_children=stored_children,
    )
    tag_values, child_values = _collect_union_child_values(
        encode_inputs,
        length=len(array),
    )
    fields = _build_union_struct_fields(
        stored_children=stored_children,
        tag_values=tag_values,
        child_values=child_values,
    )
    mask = array.is_null()
    return build_struct(
        fields,
        mask=mask,
        struct_type=_encode_union_struct_type(union_type),
    )


def _decode_union_array(array: pa.Array, *, field: pa.Field) -> pa.Array:
    if not isinstance(array, pa.StructArray):
        msg = "Expected StructArray for union-encoded field."
        raise TypeError(msg)
    union_type = _decode_union_type(field)
    struct_type = cast("pa.StructType", field.type)
    null_child_names = {child.name for child in union_type if patypes.is_null(child.type)}
    tag_values, valid_mask = _read_union_tags(struct_type, array)
    decoded_children = _decode_union_children(struct_type, array)
    tagged_values = _build_union_tagged_values(
        tag_values=tag_values,
        valid_mask=valid_mask,
        decoded_children=decoded_children,
        null_child_names=null_child_names,
    )
    return union_array_from_tagged_values(tagged_values, union_type=union_type)


def _read_union_tags(
    struct_type: pa.StructType,
    array: pa.StructArray,
) -> tuple[list[object], list[object]]:
    tag_index = struct_type.get_field_index(UNION_TAG_FIELD)
    if tag_index < 0:
        msg = "Union-encoded struct missing tag field."
        raise ValueError(msg)
    return _array_to_list(array.field(tag_index)), _array_to_list(array.is_valid())


def _decode_union_children(
    struct_type: pa.StructType,
    array: pa.StructArray,
) -> dict[str, pa.Array | pa.ChunkedArray]:
    decoded_children: dict[str, pa.Array | pa.ChunkedArray] = {}
    for child in struct_type:
        if child.name == UNION_TAG_FIELD:
            continue
        decoded_children[child.name] = _decode_array(array.field(child.name), field=child)
    return decoded_children


def _build_union_tagged_values(
    *,
    tag_values: list[object],
    valid_mask: list[object],
    decoded_children: Mapping[str, pa.Array | pa.ChunkedArray],
    null_child_names: set[str],
) -> list[object | None]:
    tagged_values: list[object | None] = []
    for idx, tag in enumerate(tag_values):
        if not bool(valid_mask[idx]) or tag is None:
            tagged_values.append(None)
            continue
        name = str(tag)
        child_array = decoded_children.get(name)
        if child_array is None:
            if name in null_child_names:
                tagged_values.append(
                    {
                        UNION_TAG_FIELD: name,
                        UNION_VALUE_FIELD: None,
                    }
                )
                continue
            msg = f"Union-encoded tag not found in struct: {name!r}."
            raise ValueError(msg)
        tagged_values.append(
            {
                UNION_TAG_FIELD: name,
                UNION_VALUE_FIELD: _array_value(child_array, idx),
            }
        )
    return tagged_values


def _encode_union_child_arrays(
    array: pa.UnionArray,
    child_fields: list[pa.Field],
) -> list[pa.Array | pa.ChunkedArray]:
    return [_encode_array(array.field(idx), field=child) for idx, child in enumerate(child_fields)]


def _filter_union_children(
    child_fields: list[pa.Field],
    encoded_child_fields: list[pa.Field],
    encoded_child_arrays: list[pa.Array | pa.ChunkedArray],
) -> list[tuple[pa.Field, pa.Field, pa.Array | pa.ChunkedArray]]:
    return [
        (child, encoded_field, encoded_array)
        for child, encoded_field, encoded_array in zip(
            child_fields,
            encoded_child_fields,
            encoded_child_arrays,
            strict=True,
        )
        if not patypes.is_null(child.type)
    ]


def _build_union_encode_inputs(
    *,
    array: pa.UnionArray,
    union_type: pa.UnionType,
    child_fields: list[pa.Field],
    encoded_child_arrays: list[pa.Array | pa.ChunkedArray],
    stored_children: list[tuple[pa.Field, pa.Field, pa.Array | pa.ChunkedArray]],
) -> _UnionEncodeInputs:
    stored_child_names = {child.name for child, _, _ in stored_children}
    code_to_index = {code: idx for idx, code in enumerate(union_type.type_codes)}
    type_ids = _array_to_list(array.type_codes)
    valid_mask = _array_to_list(array.is_valid())
    offsets = _array_to_list(array.offsets) if union_type.mode == "dense" else None
    return _UnionEncodeInputs(
        child_fields=child_fields,
        encoded_child_arrays=encoded_child_arrays,
        stored_children=stored_children,
        stored_child_names=stored_child_names,
        code_to_index=code_to_index,
        type_ids=type_ids,
        valid_mask=valid_mask,
        offsets=offsets,
    )


def _collect_union_child_values(
    encode_inputs: _UnionEncodeInputs,
    *,
    length: int,
) -> tuple[list[str | None], dict[str, list[object | None]]]:
    tag_values: list[str | None] = [None] * length
    child_values: dict[str, list[object | None]] = {
        child.name: [None] * length for child, _, _ in encode_inputs.stored_children
    }
    for idx in range(length):
        if not bool(encode_inputs.valid_mask[idx]):
            continue
        code_value = encode_inputs.type_ids[idx]
        if not isinstance(code_value, int):
            msg = "Union type id must be an int."
            raise TypeError(msg)
        code = code_value
        child_index = encode_inputs.code_to_index.get(code)
        if child_index is None:
            msg = f"Unknown union type code: {code}."
            raise ValueError(msg)
        child_name = encode_inputs.child_fields[child_index].name
        tag_values[idx] = child_name
        child_offset = encode_inputs.offsets[idx] if encode_inputs.offsets is not None else idx
        if not isinstance(child_offset, int):
            msg = "Union child offset must be an int."
            raise TypeError(msg)
        child_array = encode_inputs.encoded_child_arrays[child_index]
        if child_name in encode_inputs.stored_child_names:
            child_values[child_name][idx] = _array_value(child_array, child_offset)
    return tag_values, child_values


def _build_union_struct_fields(
    *,
    stored_children: list[tuple[pa.Field, pa.Field, pa.Array | pa.ChunkedArray]],
    tag_values: list[str | None],
    child_values: Mapping[str, list[object | None]],
) -> dict[str, pa.Array]:
    fields: dict[str, pa.Array] = {
        UNION_TAG_FIELD: pa.array(tag_values, type=pa.string()),
    }
    for child_field, encoded_field, _ in stored_children:
        values = child_values[child_field.name]
        fields[child_field.name] = nested_array_factory(encoded_field, values)
    return fields


def _encode_struct_array(array: pa.Array, *, field: pa.Field) -> pa.StructArray:
    if not isinstance(array, pa.StructArray):
        msg = "Expected StructArray for struct-typed field."
        raise TypeError(msg)
    struct_type = cast("pa.StructType", field.type)
    encoded_fields = [_encode_field(child) for child in struct_type]
    fields: dict[str, pa.Array] = {}
    for child in struct_type:
        fields[child.name] = _coerce_array(
            _encode_array(array.field(child.name), field=child),
        )
    mask = array.is_null()
    return build_struct(fields, mask=mask, struct_type=pa.struct(encoded_fields))


def _decode_struct_array(array: pa.Array, *, field: pa.Field) -> pa.StructArray:
    if not isinstance(array, pa.StructArray):
        msg = "Expected StructArray for struct-typed field."
        raise TypeError(msg)
    struct_type = cast("pa.StructType", field.type)
    decoded_fields = [_decode_field(child) for child in struct_type]
    fields: dict[str, pa.Array] = {}
    for child in struct_type:
        fields[child.name] = _coerce_array(
            _decode_array(array.field(child.name), field=child),
        )
    mask = array.is_null()
    return build_struct(fields, mask=mask, struct_type=pa.struct(decoded_fields))


def _encode_list_array(array: pa.Array, *, field: pa.Field) -> pa.ListArray:
    if not isinstance(array, pa.ListArray):
        msg = "Expected ListArray for list-typed field."
        raise TypeError(msg)
    list_type = cast("pa.ListType", field.type)
    value_field = list_type.value_field
    encoded_value = _encode_array(array.values, field=value_field)
    encoded_field = _encode_field(value_field)
    mask = array.is_null()
    return pa.ListArray.from_arrays(
        array.offsets,
        _coerce_array(encoded_value),
        type=pa.list_(encoded_field),
        mask=mask,
    )


def _decode_list_array(array: pa.Array, *, field: pa.Field) -> pa.ListArray:
    if not isinstance(array, pa.ListArray):
        msg = "Expected ListArray for list-typed field."
        raise TypeError(msg)
    list_type = cast("pa.ListType", field.type)
    value_field = list_type.value_field
    decoded_value = _decode_array(array.values, field=value_field)
    decoded_field = _decode_field(value_field)
    mask = array.is_null()
    return pa.ListArray.from_arrays(
        array.offsets,
        _coerce_array(decoded_value),
        type=pa.list_(decoded_field),
        mask=mask,
    )


def _encode_large_list_array(array: pa.Array, *, field: pa.Field) -> pa.LargeListArray:
    if not isinstance(array, pa.LargeListArray):
        msg = "Expected LargeListArray for large list-typed field."
        raise TypeError(msg)
    list_type = cast("pa.LargeListType", field.type)
    value_field = list_type.value_field
    encoded_value = _encode_array(array.values, field=value_field)
    encoded_field = _encode_field(value_field)
    mask = array.is_null()
    return pa.LargeListArray.from_arrays(
        array.offsets,
        _coerce_array(encoded_value),
        type=pa.large_list(encoded_field),
        mask=mask,
    )


def _decode_large_list_array(array: pa.Array, *, field: pa.Field) -> pa.LargeListArray:
    if not isinstance(array, pa.LargeListArray):
        msg = "Expected LargeListArray for large list-typed field."
        raise TypeError(msg)
    list_type = cast("pa.LargeListType", field.type)
    value_field = list_type.value_field
    decoded_value = _decode_array(array.values, field=value_field)
    decoded_field = _decode_field(value_field)
    mask = array.is_null()
    return pa.LargeListArray.from_arrays(
        array.offsets,
        _coerce_array(decoded_value),
        type=pa.large_list(decoded_field),
        mask=mask,
    )


def _encode_fixed_size_list_array(array: pa.Array, *, field: pa.Field) -> pa.FixedSizeListArray:
    if not isinstance(array, pa.FixedSizeListArray):
        msg = "Expected FixedSizeListArray for fixed size list field."
        raise TypeError(msg)
    list_type = cast("pa.FixedSizeListType", field.type)
    value_field = list_type.value_field
    encoded_value = _encode_array(array.values, field=value_field)
    encoded_field = _encode_field(value_field)
    mask = array.is_null()
    return pa.FixedSizeListArray.from_arrays(
        _coerce_array(encoded_value),
        list_size=list_type.list_size,
        type=pa.list_(encoded_field, list_type.list_size),
        mask=mask,
    )


def _decode_fixed_size_list_array(array: pa.Array, *, field: pa.Field) -> pa.FixedSizeListArray:
    if not isinstance(array, pa.FixedSizeListArray):
        msg = "Expected FixedSizeListArray for fixed size list field."
        raise TypeError(msg)
    list_type = cast("pa.FixedSizeListType", field.type)
    value_field = list_type.value_field
    decoded_value = _decode_array(array.values, field=value_field)
    decoded_field = _decode_field(value_field)
    mask = array.is_null()
    return pa.FixedSizeListArray.from_arrays(
        _coerce_array(decoded_value),
        list_size=list_type.list_size,
        type=pa.list_(decoded_field, list_type.list_size),
        mask=mask,
    )


def _encode_list_view_array(array: pa.Array, *, field: pa.Field) -> pa.ListViewArray:
    if not isinstance(array, pa.ListViewArray):
        msg = "Expected ListViewArray for list view field."
        raise TypeError(msg)
    list_type = cast("pa.ListViewType", field.type)
    value_field = list_type.value_field
    encoded_value = _encode_array(array.values, field=value_field)
    encoded_field = _encode_field(value_field)
    mask = array.is_null()
    return pa.ListViewArray.from_arrays(
        array.offsets,
        array.sizes,
        _coerce_array(encoded_value),
        type=pa.list_view(encoded_field),
        mask=mask,
    )


def _decode_list_view_array(array: pa.Array, *, field: pa.Field) -> pa.ListViewArray:
    if not isinstance(array, pa.ListViewArray):
        msg = "Expected ListViewArray for list view field."
        raise TypeError(msg)
    list_type = cast("pa.ListViewType", field.type)
    value_field = list_type.value_field
    decoded_value = _decode_array(array.values, field=value_field)
    decoded_field = _decode_field(value_field)
    mask = array.is_null()
    return pa.ListViewArray.from_arrays(
        array.offsets,
        array.sizes,
        _coerce_array(decoded_value),
        type=pa.list_view(decoded_field),
        mask=mask,
    )


def _encode_large_list_view_array(
    array: pa.Array,
    *,
    field: pa.Field,
) -> pa.LargeListViewArray:
    if not isinstance(array, pa.LargeListViewArray):
        msg = "Expected LargeListViewArray for large list view field."
        raise TypeError(msg)
    list_type = cast("pa.LargeListViewType", field.type)
    value_field = list_type.value_field
    encoded_value = _encode_array(array.values, field=value_field)
    encoded_field = _encode_field(value_field)
    mask = array.is_null()
    return pa.LargeListViewArray.from_arrays(
        array.offsets,
        array.sizes,
        _coerce_array(encoded_value),
        type=pa.large_list_view(encoded_field),
        mask=mask,
    )


def _decode_large_list_view_array(
    array: pa.Array,
    *,
    field: pa.Field,
) -> pa.LargeListViewArray:
    if not isinstance(array, pa.LargeListViewArray):
        msg = "Expected LargeListViewArray for large list view field."
        raise TypeError(msg)
    list_type = cast("pa.LargeListViewType", field.type)
    value_field = list_type.value_field
    decoded_value = _decode_array(array.values, field=value_field)
    decoded_field = _decode_field(value_field)
    mask = array.is_null()
    return pa.LargeListViewArray.from_arrays(
        array.offsets,
        array.sizes,
        _coerce_array(decoded_value),
        type=pa.large_list_view(decoded_field),
        mask=mask,
    )


def _encode_map_array(array: pa.Array, *, field: pa.Field) -> pa.MapArray:
    if not isinstance(array, pa.MapArray):
        msg = "Expected MapArray for map-typed field."
        raise TypeError(msg)
    map_type = cast("pa.MapType", field.type)
    key_field = map_type.key_field
    item_field = map_type.item_field
    encoded_keys = _encode_array(array.keys, field=key_field)
    encoded_items = _encode_array(array.items, field=item_field)
    encoded_key = _encode_field(key_field)
    encoded_item = _encode_field(item_field)
    mask = array.is_null()
    return pa.MapArray.from_arrays(
        array.offsets,
        _coerce_array(encoded_keys),
        _coerce_array(encoded_items),
        type=_map_type(encoded_key, encoded_item, keys_sorted=map_type.keys_sorted),
        mask=mask,
    )


def _decode_map_array(array: pa.Array, *, field: pa.Field) -> pa.MapArray:
    if not isinstance(array, pa.MapArray):
        msg = "Expected MapArray for map-typed field."
        raise TypeError(msg)
    map_type = cast("pa.MapType", field.type)
    key_field = map_type.key_field
    item_field = map_type.item_field
    decoded_keys = _decode_array(array.keys, field=key_field)
    decoded_items = _decode_array(array.items, field=item_field)
    decoded_key = _decode_field(key_field)
    decoded_item = _decode_field(item_field)
    mask = array.is_null()
    return pa.MapArray.from_arrays(
        array.offsets,
        _coerce_array(decoded_keys),
        _coerce_array(decoded_items),
        type=_map_type(decoded_key, decoded_item, keys_sorted=map_type.keys_sorted),
        mask=mask,
    )


def _encode_dictionary_array(array: pa.Array, *, field: pa.Field) -> pa.DictionaryArray:
    if not isinstance(array, pa.DictionaryArray):
        msg = "Expected DictionaryArray for dictionary-typed field."
        raise TypeError(msg)
    dict_type = cast("pa.DictionaryType", field.type)
    encoded_dict = _encode_array(array.dictionary, field=pa.field("_", dict_type.value_type))
    return pa.DictionaryArray.from_arrays(
        array.indices,
        _coerce_array(encoded_dict),
        ordered=dict_type.ordered,
    )


def _decode_dictionary_array(array: pa.Array, *, field: pa.Field) -> pa.DictionaryArray:
    if not isinstance(array, pa.DictionaryArray):
        msg = "Expected DictionaryArray for dictionary-typed field."
        raise TypeError(msg)
    dict_type = cast("pa.DictionaryType", field.type)
    decoded_dict = _decode_array(array.dictionary, field=pa.field("_", dict_type.value_type))
    return pa.DictionaryArray.from_arrays(
        array.indices,
        _coerce_array(decoded_dict),
        ordered=dict_type.ordered,
    )


def _encode_field(field: pa.Field) -> pa.Field:
    if _is_union_encoded_field(field):
        return field
    dtype = field.type
    if patypes.is_union(dtype):
        metadata = _merge_metadata(field.metadata, _union_metadata(dtype))
        return pa.field(
            field.name,
            _encode_union_struct_type(dtype),
            nullable=field.nullable,
            metadata=metadata,
        )
    encoded = _encode_type(dtype)
    if encoded == dtype:
        return field
    return pa.field(field.name, encoded, nullable=field.nullable, metadata=field.metadata)


def _decode_field(field: pa.Field) -> pa.Field:
    if _is_union_encoded_field(field):
        metadata = _strip_union_metadata(field.metadata)
        return pa.field(
            field.name,
            _decode_union_type(field),
            nullable=field.nullable,
            metadata=metadata,
        )
    decoded = _decode_type(field.type)
    if decoded == field.type:
        return field
    return pa.field(field.name, decoded, nullable=field.nullable, metadata=field.metadata)


def _encode_type(dtype: pa.DataType) -> pa.DataType:
    if patypes.is_union(dtype):
        return _encode_union_struct_type(dtype)
    encoded: pa.DataType = dtype
    if patypes.is_struct(dtype):
        struct_type = cast("pa.StructType", dtype)
        encoded = pa.struct([_encode_field(child) for child in struct_type])
    elif patypes.is_fixed_size_list(dtype):
        list_type = cast("pa.FixedSizeListType", dtype)
        encoded = _encode_field(list_type.value_field)
        encoded = pa.list_(encoded, list_type.list_size)
    elif patypes.is_list(dtype):
        list_type = cast("pa.ListType", dtype)
        encoded = pa.list_(_encode_field(list_type.value_field))
    elif patypes.is_large_list(dtype):
        list_type = cast("pa.LargeListType", dtype)
        encoded = pa.large_list(_encode_field(list_type.value_field))
    elif patypes.is_list_view(dtype):
        list_type = cast("pa.ListViewType", dtype)
        encoded = pa.list_view(_encode_field(list_type.value_field))
    elif patypes.is_large_list_view(dtype):
        list_type = cast("pa.LargeListViewType", dtype)
        encoded = pa.large_list_view(_encode_field(list_type.value_field))
    elif patypes.is_map(dtype):
        map_type = cast("pa.MapType", dtype)
        encoded = _map_type(
            _encode_field(map_type.key_field),
            _encode_field(map_type.item_field),
            keys_sorted=map_type.keys_sorted,
        )
    elif patypes.is_dictionary(dtype):
        dict_type = cast("pa.DictionaryType", dtype)
        encoded = pa.dictionary(
            dict_type.index_type,
            _encode_type(dict_type.value_type),
            ordered=dict_type.ordered,
        )
    return encoded


def _decode_type(dtype: pa.DataType) -> pa.DataType:
    decoded: pa.DataType = dtype
    if patypes.is_struct(dtype):
        struct_type = cast("pa.StructType", dtype)
        decoded = pa.struct([_decode_field(child) for child in struct_type])
    elif patypes.is_fixed_size_list(dtype):
        list_type = cast("pa.FixedSizeListType", dtype)
        decoded = _decode_field(list_type.value_field)
        decoded = pa.list_(decoded, list_type.list_size)
    elif patypes.is_list(dtype):
        list_type = cast("pa.ListType", dtype)
        decoded = pa.list_(_decode_field(list_type.value_field))
    elif patypes.is_large_list(dtype):
        list_type = cast("pa.LargeListType", dtype)
        decoded = pa.large_list(_decode_field(list_type.value_field))
    elif patypes.is_list_view(dtype):
        list_type = cast("pa.ListViewType", dtype)
        decoded = pa.list_view(_decode_field(list_type.value_field))
    elif patypes.is_large_list_view(dtype):
        list_type = cast("pa.LargeListViewType", dtype)
        decoded = pa.large_list_view(_decode_field(list_type.value_field))
    elif patypes.is_map(dtype):
        map_type = cast("pa.MapType", dtype)
        decoded = _map_type(
            _decode_field(map_type.key_field),
            _decode_field(map_type.item_field),
            keys_sorted=map_type.keys_sorted,
        )
    elif patypes.is_dictionary(dtype):
        dict_type = cast("pa.DictionaryType", dtype)
        decoded = pa.dictionary(
            dict_type.index_type,
            _decode_type(dict_type.value_type),
            ordered=dict_type.ordered,
        )
    return decoded


def _encode_union_struct_type(dtype: pa.DataType) -> pa.StructType:
    union_type = cast("pa.UnionType", dtype)
    fields = [pa.field(UNION_TAG_FIELD, pa.string(), nullable=True)]
    for child in union_type:
        if patypes.is_null(child.type):
            continue
        encoded_child = _encode_type(child.type)
        fields.append(
            pa.field(
                child.name,
                encoded_child,
                nullable=True,
                metadata=child.metadata,
            )
        )
    return pa.struct(fields)


def _union_metadata(dtype: pa.DataType) -> Mapping[bytes, bytes]:
    union_type = cast("pa.UnionType", dtype)
    return {
        UNION_ENCODING_META: UNION_ENCODING_STRUCT,
        UNION_TYPE_META: _serialize_union_type(union_type),
    }


def _is_union_encoded_field(field: pa.Field) -> bool:
    meta = field.metadata or {}
    return meta.get(UNION_ENCODING_META) == UNION_ENCODING_STRUCT and UNION_TYPE_META in meta


def _strip_union_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[bytes, bytes] | None:
    if metadata is None:
        return None
    cleaned = {
        key: value
        for key, value in metadata.items()
        if key not in {UNION_ENCODING_META, UNION_TYPE_META}
    }
    return cleaned or None


def _decode_union_type(field: pa.Field) -> pa.UnionType:
    meta = field.metadata or {}
    payload = meta.get(UNION_TYPE_META)
    if payload is None:
        msg = "Union metadata missing union type payload."
        raise ValueError(msg)
    return cast("pa.UnionType", _deserialize_union_type(payload))


def _serialize_union_type(union_type: pa.UnionType) -> bytes:
    schema = pa.schema([pa.field("union", union_type)])
    encoded = schema.serialize().to_pybytes()
    return base64.b64encode(encoded)


def _deserialize_union_type(payload: bytes) -> pa.DataType:
    decoded = base64.b64decode(payload)
    schema = pa.ipc.read_schema(pa.BufferReader(decoded))
    return schema.field(0).type


def _merge_metadata(
    base: Mapping[bytes, bytes] | None,
    updates: Mapping[bytes, bytes],
) -> dict[bytes, bytes]:
    merged = dict(base or {})
    merged.update(updates)
    return merged


def _map_type(
    key_field: pa.Field,
    item_field: pa.Field,
    *,
    keys_sorted: bool | None,
) -> pa.MapType:
    if keys_sorted is None:
        return pa.map_(key_field, item_field)
    return pa.map_(key_field, item_field, keys_sorted=keys_sorted)


def _field_has_union_encoding(field: pa.Field) -> bool:
    if _is_union_encoded_field(field):
        return True
    return _type_has_union_encoding(field.type)


def _type_has_union(dtype: pa.DataType) -> bool:
    if patypes.is_union(dtype):
        return True
    has_union = False
    if patypes.is_struct(dtype):
        has_union = any(_type_has_union(child.type) for child in cast("pa.StructType", dtype))
    elif patypes.is_fixed_size_list(dtype) or patypes.is_list(dtype):
        list_type = cast("pa.ListType", dtype)
        has_union = _type_has_union(list_type.value_type)
    elif patypes.is_large_list(dtype):
        list_type = cast("pa.LargeListType", dtype)
        has_union = _type_has_union(list_type.value_type)
    elif patypes.is_list_view(dtype):
        list_type = cast("pa.ListViewType", dtype)
        has_union = _type_has_union(list_type.value_type)
    elif patypes.is_large_list_view(dtype):
        list_type = cast("pa.LargeListViewType", dtype)
        has_union = _type_has_union(list_type.value_type)
    elif patypes.is_map(dtype):
        map_type = cast("pa.MapType", dtype)
        has_union = _type_has_union(map_type.key_type) or _type_has_union(map_type.item_type)
    elif patypes.is_dictionary(dtype):
        dict_type = cast("pa.DictionaryType", dtype)
        has_union = _type_has_union(dict_type.value_type)
    return has_union


def _type_has_union_encoding(dtype: pa.DataType) -> bool:
    has_union = False
    if patypes.is_struct(dtype):
        has_union = any(_field_has_union_encoding(child) for child in cast("pa.StructType", dtype))
    elif patypes.is_fixed_size_list(dtype) or patypes.is_list(dtype):
        list_type = cast("pa.ListType", dtype)
        has_union = _field_has_union_encoding(list_type.value_field)
    elif patypes.is_large_list(dtype):
        list_type = cast("pa.LargeListType", dtype)
        has_union = _field_has_union_encoding(list_type.value_field)
    elif patypes.is_list_view(dtype):
        list_type = cast("pa.ListViewType", dtype)
        has_union = _field_has_union_encoding(list_type.value_field)
    elif patypes.is_large_list_view(dtype):
        list_type = cast("pa.LargeListViewType", dtype)
        has_union = _field_has_union_encoding(list_type.value_field)
    elif patypes.is_map(dtype):
        map_type = cast("pa.MapType", dtype)
        has_union = _field_has_union_encoding(map_type.key_field) or _field_has_union_encoding(
            map_type.item_field
        )
    elif patypes.is_dictionary(dtype):
        dict_type = cast("pa.DictionaryType", dtype)
        has_union = _type_has_union_encoding(dict_type.value_type)
    return has_union


def _array_value(array: pa.Array | pa.ChunkedArray, index: int) -> object | None:
    if isinstance(array, pa.ChunkedArray):
        offset = index
        for chunk in array.chunks:
            if offset < len(chunk):
                return _scalar_to_py(chunk[offset])
            offset -= len(chunk)
        msg = f"Chunked array index out of range: {index}."
        raise IndexError(msg)
    return _scalar_to_py(array[index])


def _scalar_to_py(value: object | None) -> object | None:
    if value is None:
        return None
    if isinstance(value, ScalarLike):
        return value.as_py()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


def _array_to_list(array: pa.Array) -> list[object]:
    return list(array.to_pylist())


def _coerce_array(array: pa.Array | pa.ChunkedArray) -> pa.Array:
    if isinstance(array, pa.ChunkedArray):
        return array.combine_chunks()
    return array


def _coerce_chunked(
    array: pa.Array | pa.ChunkedArray,
    *,
    dtype: pa.DataType,
) -> pa.ChunkedArray:
    if isinstance(array, pa.ChunkedArray):
        return array.cast(dtype)
    return pa.chunked_array([array], type=dtype)


__all__ = [
    "UNION_ENCODING_META",
    "UNION_TAG_FIELD",
    "UNION_TYPE_META",
    "UNION_VALUE_FIELD",
    "decode_union_schema",
    "decode_union_table",
    "encode_union_schema",
    "encode_union_table",
    "schema_has_union_encoding",
    "schema_has_union_types",
]
