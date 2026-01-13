"""Column helpers and nested array builders."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa

from arrowdsl.compute.exprs import CoalesceExpr, ColumnExpr, ConstExpr, FieldExpr
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    FieldLike,
    ListArrayLike,
    StructArrayLike,
    TableLike,
)
from arrowdsl.schema.nested_builders import (
    dictionary_array_from_values,
    list_view_array_from_lists,
    map_array_from_pairs,
    struct_array_from_dicts,
    union_array_from_values,
)


def const_array(n: int, value: object, *, dtype: DataTypeLike | None = None) -> ArrayLike:
    """Return a constant array of length ``n`` with the given value.

    Returns
    -------
    ArrayLike
        Array of repeated values.
    """
    scalar = pa.scalar(value) if dtype is None else pa.scalar(value, type=dtype)
    return pa.array([value] * n, type=scalar.type)


def set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    """Set a column by name, appending if it does not exist.

    Returns
    -------
    TableLike
        Updated table.
    """
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


@dataclass(frozen=True)
class ColumnDefaultsSpec:
    """Specification for adding default columns when missing."""

    defaults: tuple[tuple[str, ColumnExpr], ...]
    overwrite: bool = False

    def apply(self, table: TableLike) -> TableLike:
        """Apply default column values to a table.

        Returns
        -------
        TableLike
            Table with defaults applied.
        """
        out = table
        for name, expr in self.defaults:
            if not self.overwrite and name in out.column_names:
                continue
            values = expr.materialize(out)
            out = set_or_append_column(out, name, values)
        return out


def build_struct(fields: dict[str, ArrayLike], *, mask: ArrayLike | None = None) -> StructArrayLike:
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
        if isinstance(arr, ChunkedArrayLike):
            arr = arr.combine_chunks()
        elif hasattr(arr, "combine_chunks"):
            arr = cast("ChunkedArrayLike", arr).combine_chunks()
        arrays.append(arr)
    if mask is not None:
        if isinstance(mask, ChunkedArrayLike):
            mask = mask.combine_chunks()
        elif hasattr(mask, "combine_chunks"):
            mask = cast("ChunkedArrayLike", mask).combine_chunks()
    return pa.StructArray.from_arrays(arrays, names=names, mask=mask)


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    """Build a list array from offsets and flat values.

    Returns
    -------
    ListArrayLike
        List array built from offsets and values.
    """
    return pa.ListArray.from_arrays(offsets, values)


def list_view_type(value_type: DataTypeLike, *, large: bool = False) -> DataTypeLike:
    """Return a list_view type (large_list_view when requested).

    Returns
    -------
    DataTypeLike
        List view data type.
    """
    return pa.large_list_view(value_type) if large else pa.list_view(value_type)


def map_type(
    key_type: DataTypeLike,
    item_type: DataTypeLike,
    *,
    keys_sorted: bool | None = None,
) -> DataTypeLike:
    """Return a map type for the provided key/value types.

    Returns
    -------
    DataTypeLike
        Map data type.
    """
    if keys_sorted is None:
        return pa.map_(key_type, item_type)
    return pa.map_(key_type, item_type, keys_sorted=keys_sorted)


def struct_type(fields: Sequence[FieldLike] | Mapping[str, DataTypeLike]) -> DataTypeLike:
    """Return a struct type built from fields or name/type mappings.

    Returns
    -------
    DataTypeLike
        Struct data type.
    """
    if isinstance(fields, Mapping):
        return pa.struct(fields)
    return pa.struct(list(fields))


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


__all__ = [
    "CoalesceExpr",
    "ColumnDefaultsSpec",
    "ColumnExpr",
    "ConstExpr",
    "FieldExpr",
    "build_list",
    "build_list_of_structs",
    "build_list_view",
    "build_struct",
    "const_array",
    "dictionary_array_from_values",
    "list_view_array_from_lists",
    "list_view_type",
    "map_array_from_pairs",
    "map_type",
    "set_or_append_column",
    "struct_array_from_dicts",
    "struct_type",
    "union_array_from_values",
]
