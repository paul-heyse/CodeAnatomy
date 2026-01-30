"""Arrow schema protocol helpers."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Protocol

from datafusion_engine.arrow_interop import FieldLike


class ListTypeProtocol(Protocol):
    """Protocol for Arrow list types."""

    value_field: FieldLike


class StructTypeProtocol(Protocol):
    """Protocol for Arrow struct types."""

    def __iter__(self) -> Iterator[FieldLike]: ...


class MapTypeProtocol(Protocol):
    """Protocol for Arrow map types."""

    key_field: FieldLike
    item_field: FieldLike
    keys_sorted: bool


class UnionTypeProtocol(Protocol):
    """Protocol for Arrow union types."""

    type_codes: Sequence[int]
    mode: str

    def __iter__(self) -> Iterator[FieldLike]: ...


__all__ = [
    "ListTypeProtocol",
    "MapTypeProtocol",
    "StructTypeProtocol",
    "UnionTypeProtocol",
]
