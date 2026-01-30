"""Nested array builders and accumulators."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from datafusion_engine.arrow_interop import (
    ArrayLike,
    ChunkedArray,
    DataTypeLike,
    FieldLike,
    ListArrayLike,
    ScalarLike,
    StructArrayLike,
)
from datafusion_engine.arrow_schema.types import list_view_type, map_type
from utils.validation import validate_required_items

MAX_INT8_CODE = 127
MAX_INT16_CODE = 32767
_UNION_TAG_KEY = "__union_tag__"
_UNION_VALUE_KEY = "value"


class _ListType(Protocol):
    value_field: FieldLike


class _StructType(Protocol):
    def __iter__(self) -> Iterator[FieldLike]: ...


class _UnionType(Protocol):
    type_codes: Sequence[int]
    mode: str

    def __iter__(self) -> Iterator[FieldLike]: ...


def _offsets_start() -> list[int]:
    return [0]


def _sizes_start() -> list[int]:
    return []


def _safe_array(values: Sequence[object | None], *, dtype: DataTypeLike) -> ArrayLike:
    try:
        return pa.array(values, type=dtype)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        coerced: list[object | None] = []
        for value in values:
            if value is None:
                coerced.append(None)
                continue
            try:
                _ = pa.scalar(value, type=dtype)
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
                coerced.append(None)
            else:
                coerced.append(value)
        return pa.array(coerced, type=dtype)


def _normalize_union_value(value: object | None) -> object | None:
    if isinstance(value, ScalarLike):
        return value.as_py()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


def _find_child_index(
    child_types: Sequence[DataTypeLike],
    predicate: Callable[[DataTypeLike], bool],
) -> int | None:
    for idx, child_type in enumerate(child_types):
        if predicate(child_type):
            return idx
    return None


def _union_type_id_type(type_codes: Sequence[int]) -> DataTypeLike:
    max_code = max(type_codes) if type_codes else 0
    if max_code <= MAX_INT8_CODE:
        return pa.int8()
    if max_code <= MAX_INT16_CODE:
        return pa.int16()
    return pa.int32()


def _is_string_type(dtype: DataTypeLike) -> bool:
    return patypes.is_string(dtype) or patypes.is_large_string(dtype)


def _is_binary_type(dtype: DataTypeLike) -> bool:
    return patypes.is_binary(dtype) or patypes.is_large_binary(dtype)


def _union_value_predicates(
    value: object,
) -> tuple[Callable[[DataTypeLike], bool], ...]:
    if isinstance(value, bool):
        return (patypes.is_boolean,)
    if isinstance(value, int):
        return (patypes.is_integer,)
    if isinstance(value, float):
        return (patypes.is_floating,)
    if isinstance(value, str):
        return (_is_string_type,)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return (_is_binary_type,)
    return ()


def _castable_child_index(
    value: object,
    child_types: Sequence[DataTypeLike],
    null_index: int | None,
) -> int | None:
    for idx, child_type in enumerate(child_types):
        if null_index is not None and idx == null_index:
            continue
        try:
            _ = pa.scalar(value, type=child_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
            continue
        return idx
    return None


def _union_child_index(
    value: object | None,
    child_types: Sequence[DataTypeLike],
    null_index: int | None,
) -> int:
    if value is None:
        return null_index if null_index is not None else 0
    for predicate in _union_value_predicates(value):
        idx = _find_child_index(child_types, predicate)
        if idx is not None:
            return idx
    idx = _castable_child_index(value, child_types, null_index)
    if idx is not None:
        return idx
    msg = f"Union array cannot encode value: {value!r}."
    raise TypeError(msg)


def _tagged_union_mode(values: Sequence[object | None]) -> bool:
    tagged = False
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping) and _UNION_TAG_KEY in value:
            tagged = True
            continue
        if tagged:
            msg = "Tagged union values must include __union_tag__ for all non-null entries."
            raise TypeError(msg)
        return False
    return tagged


def _tagged_union_values(
    values: Sequence[object | None],
    *,
    child_types: Sequence[DataTypeLike],
    child_names: Sequence[str],
    type_codes: Sequence[int],
    null_index: int | None,
) -> tuple[list[int], list[int], list[list[object | None]]]:
    type_ids: list[int] = []
    offsets: list[int] = []
    child_values: list[list[object | None]] = [[] for _ in child_types]
    name_to_index = {name: idx for idx, name in enumerate(child_names)}
    for value in values:
        if value is None:
            child_index = null_index if null_index is not None else 0
            normalized = None
        else:
            if not isinstance(value, Mapping):
                msg = "Tagged union values must be mappings."
                raise TypeError(msg)
            tag = value.get(_UNION_TAG_KEY)
            if tag is None:
                msg = "Tagged union values require __union_tag__."
                raise TypeError(msg)
            child_index = name_to_index.get(str(tag))
            if child_index is None:
                msg = f"Tagged union value has unknown tag: {tag!r}."
                raise ValueError(msg)
            normalized = _normalize_union_value(value.get(_UNION_VALUE_KEY))
        type_ids.append(type_codes[child_index])
        child_values[child_index].append(normalized)
        offsets.append(len(child_values[child_index]) - 1)
    return type_ids, offsets, child_values


def _tagged_sparse_union_values(
    values: Sequence[object | None],
    *,
    child_types: Sequence[DataTypeLike],
    child_names: Sequence[str],
    type_codes: Sequence[int],
    null_index: int | None,
) -> tuple[list[int], list[list[object | None]]]:
    type_ids: list[int] = []
    child_values: list[list[object | None]] = [[] for _ in child_types]
    name_to_index = {name: idx for idx, name in enumerate(child_names)}
    for value in values:
        if value is None:
            child_index = null_index if null_index is not None else 0
            normalized = None
        else:
            if not isinstance(value, Mapping):
                msg = "Tagged union values must be mappings."
                raise TypeError(msg)
            tag = value.get(_UNION_TAG_KEY)
            if tag is None:
                msg = "Tagged union values require __union_tag__."
                raise TypeError(msg)
            child_index = name_to_index.get(str(tag))
            if child_index is None:
                msg = f"Tagged union value has unknown tag: {tag!r}."
                raise ValueError(msg)
            normalized = _normalize_union_value(value.get(_UNION_VALUE_KEY))
        type_ids.append(type_codes[child_index])
        for idx in range(len(child_values)):
            child_values[idx].append(normalized if idx == child_index else None)
    return type_ids, child_values


def _dense_union_values(
    values: Sequence[object | None],
    *,
    child_types: Sequence[DataTypeLike],
    type_codes: Sequence[int],
    null_index: int | None,
) -> tuple[list[int], list[int], list[list[object | None]]]:
    type_ids: list[int] = []
    offsets: list[int] = []
    child_values: list[list[object | None]] = [[] for _ in child_types]
    for value in values:
        child_index = _union_child_index(value, child_types, null_index)
        normalized = _normalize_union_value(value)
        type_ids.append(type_codes[child_index])
        child_values[child_index].append(normalized)
        offsets.append(len(child_values[child_index]) - 1)
    return type_ids, offsets, child_values


def _sparse_union_values(
    values: Sequence[object | None],
    *,
    child_types: Sequence[DataTypeLike],
    type_codes: Sequence[int],
    null_index: int | None,
) -> tuple[list[int], list[list[object | None]]]:
    type_ids: list[int] = []
    child_values: list[list[object | None]] = [[] for _ in child_types]
    for value in values:
        child_index = _union_child_index(value, child_types, null_index)
        normalized = _normalize_union_value(value)
        type_ids.append(type_codes[child_index])
        for idx in range(len(child_values)):
            child_values[idx].append(normalized if idx == child_index else None)
    return type_ids, child_values


def _union_children_arrays(
    child_types: Sequence[DataTypeLike],
    child_values: Sequence[Sequence[object | None]],
) -> list[ArrayLike]:
    children: list[ArrayLike] = []
    for child_type, values in zip(child_types, child_values, strict=True):
        children.append(_safe_array(values, dtype=child_type))
    return children


def _default_value_for_type(dtype: DataTypeLike) -> object | None:
    if patypes.is_boolean(dtype):
        return False
    if patypes.is_integer(dtype):
        return 0
    if patypes.is_floating(dtype):
        return 0.0
    if patypes.is_string(dtype):
        return ""
    if patypes.is_binary(dtype):
        return b""
    return None


def build_struct(
    fields: dict[str, ArrayLike],
    *,
    mask: ArrayLike | None = None,
    struct_type: DataTypeLike | None = None,
) -> StructArrayLike:
    """Build a struct array from named child arrays.

    Returns
    -------
    StructArrayLike
        Struct array built from child arrays.
    """
    names = list(fields.keys())
    arrays: list[ArrayLike] = []
    for name in names:
        arr = fields[name]
        if isinstance(arr, ChunkedArray):
            arr = arr.combine_chunks()
        arrays.append(arr)
    if mask is not None and isinstance(mask, ChunkedArray):
        mask = mask.combine_chunks()
    if struct_type is None:
        return pa.StructArray.from_arrays(arrays, names=names, mask=mask)
    return pa.StructArray.from_arrays(arrays, mask=mask, type=struct_type)


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    """Build a list array from offsets and flat values.

    Returns
    -------
    ListArrayLike
        List array built from offsets and values.
    """
    return pa.ListArray.from_arrays(offsets, values)


def build_list_view(
    offsets: ArrayLike,
    sizes: ArrayLike,
    values: ArrayLike,
    *,
    list_type: DataTypeLike | None = None,
    mask: ArrayLike | None = None,
) -> ListArrayLike:
    """Build a list_view array from offsets, sizes, and flat values.

    Returns
    -------
    ListArrayLike
        List view array built from offsets and sizes.
    """
    return pa.ListViewArray.from_arrays(
        offsets,
        sizes,
        values,
        type=list_type,
        mask=mask,
    )


def build_list_of_structs(
    offsets: ArrayLike,
    struct_fields: dict[str, ArrayLike],
) -> ListArrayLike:
    """Build a list<struct<...>> array from offsets and child arrays.

    Returns
    -------
    ListArrayLike
        List array with struct elements.
    """
    struct_arr = build_struct(struct_fields)
    return build_list(offsets, struct_arr)


def list_array_from_lists(
    values: Sequence[object | None],
    *,
    list_type: DataTypeLike,
    large: bool = False,
) -> ArrayLike:
    """Build a list array from Python list values.

    Returns
    -------
    ArrayLike
        List array with nested values materialized via nested builders.
    """
    offsets: list[int] = [0]
    flattened: list[object | None] = []
    null_mask: list[bool] = []
    for value in values:
        if value is None:
            null_mask.append(True)
            offsets.append(len(flattened))
        elif isinstance(value, (list, tuple)):
            null_mask.append(False)
            flattened.extend(value)
            offsets.append(len(flattened))
        else:
            null_mask.append(True)
            offsets.append(len(flattened))
    list_type = cast("_ListType", list_type)
    value_field = list_type.value_field
    child_array = nested_array_factory(value_field, flattened)
    offsets_array = pa.array(offsets, type=pa.int64() if large else pa.int32())
    mask_array = pa.array(null_mask, type=pa.bool_()) if any(null_mask) else None
    if large:
        return pa.LargeListArray.from_arrays(offsets_array, child_array, mask=mask_array)
    return pa.ListArray.from_arrays(offsets_array, child_array, mask=mask_array)


def list_view_array_from_lists(
    values: Sequence[object | None],
    *,
    value_type: DataTypeLike,
    large: bool = True,
) -> ArrayLike:
    """Build a list_view array from Python list values.

    Returns
    -------
    ArrayLike
        List view array with the requested element type.
    """
    normalized: list[list[object] | None] = []
    for value in values:
        if value is None:
            normalized.append(None)
        elif isinstance(value, (list, tuple)):
            normalized.append(list(value))
        else:
            normalized.append(None)
    list_type = list_view_type(value_type, large=large)
    return pa.array(normalized, type=list_type)


def map_array_from_pairs(
    values: Sequence[object | None],
    *,
    key_type: DataTypeLike,
    item_type: DataTypeLike,
    keys_sorted: bool | None = None,
) -> ArrayLike:
    """Build a map array from key/value pair sequences.

    Returns
    -------
    ArrayLike
        Map array with the provided key/value types.
    """
    normalized: list[list[tuple[object, object]] | None] = []
    for value in values:
        if value is None:
            normalized.append(None)
        elif isinstance(value, Mapping):
            normalized.append(list(value.items()))
        elif isinstance(value, (list, tuple)):
            normalized.append(list(value))
        else:
            normalized.append(None)
    map_dtype = map_type(key_type, item_type, keys_sorted=keys_sorted)
    return pa.array(normalized, type=map_dtype)


def struct_array_from_dicts(
    values: Sequence[object | None],
    *,
    struct_type: DataTypeLike | None = None,
) -> ArrayLike:
    """Build a struct array from mapping values.

    Returns
    -------
    ArrayLike
        Struct array with inferred or explicit type.
    """
    normalized, mask_values = _normalize_struct_values(values)
    if struct_type is None:
        return pa.array(normalized, type=None)
    struct_type = cast("_StructType", struct_type)
    required_fields = tuple(field.name for field in struct_type if not field.nullable)
    if required_fields:
        for row in normalized:
            if row is None:
                continue
            validate_required_items(
                required_fields,
                row,
                item_label="struct field(s)",
                error_type=ValueError,
            )
    fields: dict[str, ArrayLike] = {}
    for struct_field in struct_type:
        column_values = _struct_column_values(normalized, struct_field=struct_field)
        fields[struct_field.name] = nested_array_factory(struct_field, column_values)
    mask_array = pa.array(mask_values, type=pa.bool_()) if any(mask_values) else None
    return build_struct(fields, mask=mask_array, struct_type=cast("DataTypeLike", struct_type))


def _normalize_struct_values(
    values: Sequence[object | None],
) -> tuple[list[Mapping[str, object] | None], list[bool]]:
    normalized: list[Mapping[str, object] | None] = []
    mask_values: list[bool] = []
    for value in values:
        if value is None:
            normalized.append(None)
            mask_values.append(True)
        elif isinstance(value, Mapping):
            normalized.append(value)
            mask_values.append(False)
        else:
            normalized.append(None)
            mask_values.append(True)
    return normalized, mask_values


def _struct_column_values(
    rows: Sequence[Mapping[str, object] | None],
    *,
    struct_field: FieldLike,
) -> list[object | None]:
    column_values: list[object | None] = []
    for row in rows:
        if row is None:
            if struct_field.nullable:
                column_values.append(None)
            else:
                column_values.append(_default_value_for_type(struct_field.type))
            continue
        if struct_field.name in row:
            column_values.append(row.get(struct_field.name))
            continue
        if struct_field.nullable:
            column_values.append(None)
            continue
        msg = f"Missing required struct field: {struct_field.name!r}."
        raise ValueError(msg)
    return column_values


def dense_union_array(
    type_ids: ArrayLike | Sequence[int],
    offsets: ArrayLike | Sequence[int],
    children: Sequence[ArrayLike],
    *,
    field_names: Sequence[str] | None = None,
    type_codes: Sequence[int] | None = None,
) -> ArrayLike:
    """Build a dense union array from type ids, offsets, and children.

    Returns
    -------
    ArrayLike
        Dense union array.
    """
    type_codes_list = list(type_codes) if type_codes is not None else None
    type_id_type = _union_type_id_type(type_codes_list or [])
    types = pa.array(type_ids, type=type_id_type)
    value_offsets = pa.array(offsets, type=pa.int32())
    child_arrays = [
        arr.combine_chunks() if isinstance(arr, ChunkedArray) else arr for arr in children
    ]
    return pa.UnionArray.from_dense(
        types,
        value_offsets,
        child_arrays,
        field_names=list(field_names) if field_names is not None else None,
        type_codes=type_codes_list,
    )


def sparse_union_array(
    type_ids: ArrayLike | Sequence[int],
    children: Sequence[ArrayLike],
    *,
    field_names: Sequence[str] | None = None,
    type_codes: Sequence[int] | None = None,
) -> ArrayLike:
    """Build a sparse union array from type ids and children.

    Returns
    -------
    ArrayLike
        Sparse union array.
    """
    type_codes_list = list(type_codes) if type_codes is not None else None
    type_id_type = _union_type_id_type(type_codes_list or [])
    types = pa.array(type_ids, type=type_id_type)
    child_arrays = [
        arr.combine_chunks() if isinstance(arr, ChunkedArray) else arr for arr in children
    ]
    return pa.UnionArray.from_sparse(
        types,
        child_arrays,
        field_names=list(field_names) if field_names is not None else None,
        type_codes=type_codes_list,
    )


def union_array_from_values(
    values: Sequence[object | None],
    *,
    union_type: DataTypeLike,
) -> ArrayLike:
    """Build a union array from scalar values.

    Returns
    -------
    ArrayLike
        Union array built from the provided values.

    Raises
    ------
    TypeError
        Raised when the dtype is not union-typed or values are unsupported.
    ValueError
        Raised when the union mode is unsupported.
    """
    if not patypes.is_union(union_type):
        msg = "union_array_from_values requires a union dtype."
        raise TypeError(msg)
    union_type = cast("_UnionType", union_type)
    child_fields = list(union_type)
    child_names = [field.name for field in child_fields]
    child_types = [field.type for field in child_fields]
    type_codes = list(union_type.type_codes)
    null_index = _find_child_index(child_types, patypes.is_null)
    if union_type.mode == "dense":
        type_ids, offsets, child_values = _dense_union_values(
            values,
            child_types=child_types,
            type_codes=type_codes,
            null_index=null_index,
        )
        children = _union_children_arrays(child_types, child_values)
        return dense_union_array(
            type_ids,
            offsets,
            children,
            field_names=child_names,
            type_codes=type_codes,
        )
    if union_type.mode == "sparse":
        type_ids, child_values = _sparse_union_values(
            values,
            child_types=child_types,
            type_codes=type_codes,
            null_index=null_index,
        )
        children = _union_children_arrays(child_types, child_values)
        return sparse_union_array(
            type_ids,
            children,
            field_names=child_names,
            type_codes=type_codes,
        )
    msg = f"Unsupported union mode: {union_type.mode!r}."
    raise ValueError(msg)


def union_array_from_tagged_values(
    values: Sequence[object | None],
    *,
    union_type: DataTypeLike,
) -> ArrayLike:
    """Build a union array from tagged values.

    Returns
    -------
    ArrayLike
        Union array built from tagged payloads.

    Raises
    ------
    TypeError
        Raised when union_type is not a union dtype.
    ValueError
        Raised when the union mode is unsupported.
    """
    if not patypes.is_union(union_type):
        msg = "union_array_from_tagged_values requires a union dtype."
        raise TypeError(msg)
    union_type = cast("_UnionType", union_type)
    child_fields = list(union_type)
    child_names = [field.name for field in child_fields]
    child_types = [field.type for field in child_fields]
    type_codes = list(union_type.type_codes)
    null_index = _find_child_index(child_types, patypes.is_null)
    if union_type.mode == "dense":
        type_ids, offsets, child_values = _tagged_union_values(
            values,
            child_types=child_types,
            child_names=child_names,
            type_codes=type_codes,
            null_index=null_index,
        )
        children = _union_children_arrays(child_types, child_values)
        return dense_union_array(
            type_ids,
            offsets,
            children,
            field_names=child_names,
            type_codes=type_codes,
        )
    if union_type.mode == "sparse":
        type_ids, child_values = _tagged_sparse_union_values(
            values,
            child_types=child_types,
            child_names=child_names,
            type_codes=type_codes,
            null_index=null_index,
        )
        children = _union_children_arrays(child_types, child_values)
        return sparse_union_array(
            type_ids,
            children,
            field_names=child_names,
            type_codes=type_codes,
        )
    msg = f"Unsupported union mode: {union_type.mode!r}."
    raise ValueError(msg)


def dictionary_array_from_values(
    values: Sequence[object | None],
    *,
    dictionary_type: DataTypeLike,
) -> ArrayLike:
    """Build a dictionary array with fallback for malformed values.

    Returns
    -------
    ArrayLike
        Dictionary array with malformed values coerced to nulls.
    """
    return _safe_array(values, dtype=dictionary_type)


def dictionary_array_from_indices(
    indices: ArrayLike | Sequence[int | None],
    dictionary: ArrayLike | Sequence[object],
    *,
    index_type: DataTypeLike | None = None,
    dictionary_type: DataTypeLike | None = None,
    ordered: bool = False,
) -> ArrayLike:
    """Build a dictionary array from indices and dictionary values.

    Returns
    -------
    ArrayLike
        Dictionary array with explicit dictionary values.
    """
    idx_type = index_type or pa.int32()
    idx_array = pa.array(indices, type=idx_type)
    dict_array = pa.array(dictionary, type=dictionary_type)
    return pa.DictionaryArray.from_arrays(idx_array, dict_array, ordered=ordered)


def nested_array_factory(field: FieldLike, values: Sequence[object | None]) -> ArrayLike:
    """Build a nested array for the provided field.

    Returns
    -------
    ArrayLike
        Array aligned to the field type.
    """
    dtype = field.type
    if patypes.is_struct(dtype):
        array = struct_array_from_dicts(values, struct_type=cast("_StructType", dtype))
    elif patypes.is_map(dtype):
        map_type = cast("pa.MapType", dtype)
        key_type = getattr(map_type, "key_type", map_type.key_field.type)
        item_type = getattr(map_type, "item_type", map_type.item_field.type)
        keys_sorted = getattr(map_type, "keys_sorted", None)
        array = map_array_from_pairs(
            values,
            key_type=key_type,
            item_type=item_type,
            keys_sorted=keys_sorted,
        )
    elif patypes.is_list(dtype) or patypes.is_large_list(dtype):
        array = list_array_from_lists(
            values,
            list_type=dtype,
            large=patypes.is_large_list(dtype),
        )
    elif patypes.is_large_list_view(dtype) or patypes.is_list_view(dtype):
        list_type = cast("pa.ListViewType", dtype)
        array = list_view_array_from_lists(
            values,
            value_type=list_type.value_type,
            large=patypes.is_large_list_view(dtype),
        )
    elif patypes.is_dictionary(dtype):
        array = dictionary_array_from_values(values, dictionary_type=dtype)
    elif patypes.is_union(dtype):
        if _tagged_union_mode(values):
            array = union_array_from_tagged_values(values, union_type=dtype)
        else:
            array = union_array_from_values(values, union_type=dtype)
    else:
        array = pa.array(values, type=dtype)
    return array


__all__ = [
    "build_list",
    "build_list_of_structs",
    "build_list_view",
    "build_struct",
    "dense_union_array",
    "dictionary_array_from_indices",
    "dictionary_array_from_values",
    "list_array_from_lists",
    "list_view_array_from_lists",
    "map_array_from_pairs",
    "nested_array_factory",
    "sparse_union_array",
    "struct_array_from_dicts",
    "union_array_from_tagged_values",
    "union_array_from_values",
]
