"""Centralized Arrow type helpers."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal, Protocol

import pyarrow as pa

from datafusion_engine.arrow.interop import DataTypeLike, FieldLike

# --- Type Protocols (formerly _type_protocols.py) ---


class ListTypeProtocol(Protocol):
    """Protocol for Arrow list types."""

    value_field: FieldLike


class StructTypeProtocol(Protocol):
    """Protocol for Arrow struct types."""

    def __iter__(self) -> Iterator[FieldLike]:
        """Iterate over struct fields."""
        ...


class MapTypeProtocol(Protocol):
    """Protocol for Arrow map types."""

    key_field: FieldLike
    item_field: FieldLike
    keys_sorted: bool


class UnionTypeProtocol(Protocol):
    """Protocol for Arrow union types."""

    type_codes: Sequence[int]
    mode: str

    def __iter__(self) -> Iterator[FieldLike]:
        """Iterate over union fields."""
        ...


def list_view_type(value_type: DataTypeLike, *, large: bool = False) -> DataTypeLike:
    """Return a list or large_list type.

    Returns:
    -------
    DataTypeLike
        Arrow list data type.
    """
    return pa.large_list(value_type) if large else pa.list_(value_type)


def map_type(
    key_type: DataTypeLike,
    item_type: DataTypeLike,
    *,
    keys_sorted: bool | None = None,
) -> DataTypeLike:
    """Return a map type with optional key ordering.

    Returns:
    -------
    DataTypeLike
        Arrow map data type.
    """
    if keys_sorted is None:
        return pa.map_(key_type, item_type)
    return pa.map_(key_type, item_type, keys_sorted=keys_sorted)


def union_type(
    children: Sequence[FieldLike],
    *,
    mode: Literal["dense", "sparse"] = "dense",
) -> DataTypeLike:
    """Return a dense or sparse union type from fields.

    Returns:
    -------
    DataTypeLike
        Arrow union data type.
    """
    if mode == "dense":
        return pa.dense_union(list(children))
    return pa.sparse_union(list(children))


DEFAULT_DICTIONARY_INDEX_TYPE = pa.int32()


def dictionary_type(
    value_type: DataTypeLike,
    *,
    index_type: DataTypeLike = DEFAULT_DICTIONARY_INDEX_TYPE,
    ordered: bool = False,
) -> DataTypeLike:
    """Return a dictionary type for categorical encoding.

    Returns:
    -------
    DataTypeLike
        Arrow dictionary data type.
    """
    return pa.dictionary(index_type, value_type, ordered=ordered)


__all__ = [
    "DEFAULT_DICTIONARY_INDEX_TYPE",
    "ListTypeProtocol",
    "MapTypeProtocol",
    "StructTypeProtocol",
    "UnionTypeProtocol",
    "dictionary_type",
    "list_view_type",
    "map_type",
    "union_type",
]
