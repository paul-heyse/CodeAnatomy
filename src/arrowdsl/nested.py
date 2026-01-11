"""Nested array builders for list/struct columns."""

from __future__ import annotations

import pyarrow as pa


def build_struct_array(
    fields: dict[str, pa.Array],
    *,
    mask: pa.Array | None = None,
) -> pa.StructArray:
    """Build a struct array from named child arrays.

    Parameters
    ----------
    fields
        Mapping of field name to child array.
    mask
        Optional boolean mask where True indicates a null struct row.

    Returns
    -------
    pa.StructArray
        Struct array with the provided fields.
    """
    names = list(fields.keys())
    arrays = [fields[name] for name in names]
    return pa.StructArray.from_arrays(arrays, names=names, mask=mask)


def build_list_array(offsets: pa.Array, values: pa.Array) -> pa.ListArray:
    """Build a list array from offsets and flat values.

    Parameters
    ----------
    offsets
        Offsets array describing list boundaries.
    values
        Flat values array.

    Returns
    -------
    pa.ListArray
        List array built from offsets and values.
    """
    return pa.ListArray.from_arrays(offsets, values)


def build_list_of_structs(
    offsets: pa.Array,
    struct_fields: dict[str, pa.Array],
) -> pa.ListArray:
    """Build a list<struct<...>> array from offsets and child arrays.

    Parameters
    ----------
    offsets
        Offsets array describing list boundaries.
    struct_fields
        Mapping of field name to child array for the struct.

    Returns
    -------
    pa.ListArray
        List array with struct elements.
    """
    struct_arr = build_struct_array(struct_fields)
    return build_list_array(offsets, struct_arr)
