"""Shared builders for nested Arrow arrays."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    ListArrayLike,
    StructArrayLike,
)


def build_struct(fields: dict[str, ArrayLike], *, mask: ArrayLike | None = None) -> StructArrayLike:
    """Build a struct array from named child arrays.

    Parameters
    ----------
    fields:
        Mapping of field name to child array.
    mask:
        Optional mask where True indicates a null struct row.

    Returns
    -------
    StructArrayLike
        Struct array with the provided fields.
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

    Parameters
    ----------
    offsets:
        Offsets array describing list boundaries.
    values:
        Flat values array.

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

    Parameters
    ----------
    offsets:
        Offsets array describing list boundaries.
    sizes:
        Sizes array describing list lengths.
    values:
        Flat values array.
    list_type:
        Optional list type override.
    mask:
        Optional mask where True indicates a null list row.

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


def build_map(
    offsets: ArrayLike,
    keys: ArrayLike,
    items: ArrayLike,
) -> ArrayLike:
    """Build a map array from offsets and flattened keys/items.

    Parameters
    ----------
    offsets:
        Offsets array describing map boundaries.
    keys:
        Flattened keys array.
    items:
        Flattened items array.

    Returns
    -------
    ArrayLike
        Map array built from offsets and flattened key/item arrays.
    """
    return pa.MapArray.from_arrays(offsets, keys, items)


def build_sparse_union(
    type_ids: ArrayLike,
    children: list[ArrayLike],
) -> ArrayLike:
    """Build a sparse union array from type ids and child arrays.

    Returns
    -------
    ArrayLike
        Sparse union array.
    """
    return pa.UnionArray.from_sparse(type_ids, children)


def build_dense_union(
    type_ids: ArrayLike,
    offsets: ArrayLike,
    children: list[ArrayLike],
) -> ArrayLike:
    """Build a dense union array from type ids, offsets, and child arrays.

    Returns
    -------
    ArrayLike
        Dense union array.
    """
    return pa.UnionArray.from_dense(type_ids, offsets, children)


def build_list_of_structs(
    offsets: ArrayLike,
    struct_fields: dict[str, ArrayLike],
) -> ListArrayLike:
    """Build a list<struct<...>> array from offsets and child arrays.

    Parameters
    ----------
    offsets:
        Offsets array describing list boundaries.
    struct_fields:
        Mapping of field name to child array for the struct.

    Returns
    -------
    ListArrayLike
        List array with struct elements.
    """
    struct_arr = build_struct(struct_fields)
    return build_list(offsets, struct_arr)
