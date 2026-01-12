"""Nested array builders for list/struct columns."""

from __future__ import annotations

from arrowdsl.nested_ops import (
    build_dense_union,
    build_list,
    build_list_view,
    build_map,
    build_sparse_union,
    build_struct,
)
from arrowdsl.nested_ops import (
    build_list_of_structs as _build_list_of_structs,
)
from arrowdsl.pyarrow_protocols import ArrayLike, DataTypeLike, ListArrayLike, StructArrayLike


def build_struct_array(
    fields: dict[str, ArrayLike],
    *,
    mask: ArrayLike | None = None,
) -> StructArrayLike:
    """Build a struct array from named child arrays.

    Parameters
    ----------
    fields
        Mapping of field name to child array.
    mask
        Optional boolean mask where True indicates a null struct row.

    Returns
    -------
    StructArrayLike
        Struct array with the provided fields.
    """
    return build_struct(fields, mask=mask)


def build_list_array(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    """Build a list array from offsets and flat values.

    Parameters
    ----------
    offsets
        Offsets array describing list boundaries.
    values
        Flat values array.

    Returns
    -------
    ListArrayLike
        List array built from offsets and values.
    """
    return build_list(offsets, values)


def build_list_view_array(
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
    offsets
        Offsets array describing list boundaries.
    sizes
        Sizes array describing list lengths.
    values
        Flat values array.
    list_type
        Optional list type override.
    mask
        Optional boolean mask where True indicates a null list row.

    Returns
    -------
    ListArrayLike
        List view array built from offsets and sizes.
    """
    return build_list_view(offsets, sizes, values, list_type=list_type, mask=mask)


def build_list_of_structs(
    offsets: ArrayLike,
    struct_fields: dict[str, ArrayLike],
) -> ListArrayLike:
    """Build a list<struct<...>> array from offsets and child arrays.

    Parameters
    ----------
    offsets
        Offsets array describing list boundaries.
    struct_fields
        Mapping of field name to child array for the struct.

    Returns
    -------
    ListArrayLike
        List array with struct elements.
    """
    return _build_list_of_structs(offsets, struct_fields)


def build_map_array(
    offsets: ArrayLike,
    keys: ArrayLike,
    items: ArrayLike,
) -> ArrayLike:
    """Build a map array from offsets and flattened key/item arrays.

    Returns
    -------
    ArrayLike
        Map array with key/item pairs.
    """
    return build_map(offsets, keys, items)


def build_sparse_union_array(type_ids: ArrayLike, children: list[ArrayLike]) -> ArrayLike:
    """Build a sparse union array from type ids and child arrays.

    Returns
    -------
    ArrayLike
        Sparse union array.
    """
    return build_sparse_union(type_ids, children)


def build_dense_union_array(
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
    return build_dense_union(type_ids, offsets, children)
