"""Shared nested list/struct accumulator helpers for extractors."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

import arrowdsl.core.interop as pa
from arrowdsl.core.interop import ArrayLike, DataTypeLike
from arrowdsl.schema.arrays import build_list_array, build_list_of_structs


def _offsets_start() -> list[int]:
    return [0]


@dataclass
class ListAccumulator[T]:
    """Accumulate list offsets and values for list-typed columns."""

    offsets: list[int] = field(default_factory=_offsets_start)
    values: list[T | None] = field(default_factory=list)

    def append(self, items: Iterable[T | None]) -> None:
        """Append items for one logical row."""
        self.values.extend(items)
        self.offsets.append(len(self.values))

    def extend_from(self, other: ListAccumulator[T]) -> None:
        """Extend with another accumulator's offsets and values."""
        if len(other.offsets) <= 1:
            return
        base = self.offsets[-1]
        self.offsets.extend(base + offset for offset in other.offsets[1:])
        self.values.extend(other.values)

    def build(self, *, value_type: DataTypeLike) -> ArrayLike:
        """Build a list array for the accumulated values.

        Returns
        -------
        ArrayLike
            List array from offsets and values.
        """
        return build_list_array(
            pa.array(self.offsets, type=pa.int32()),
            pa.array(self.values, type=value_type),
        )


@dataclass
class StructListAccumulator:
    """Accumulate list<struct> values and offsets."""

    field_names: tuple[str, ...]
    offsets: list[int] = field(default_factory=_offsets_start)
    values: dict[str, list[object]] = field(init=False)

    def __post_init__(self) -> None:
        """Initialize the per-field value buffers."""
        self.values = {name: [] for name in self.field_names}

    @classmethod
    def with_fields(cls, field_names: Sequence[str]) -> StructListAccumulator:
        """Construct an accumulator with the provided field names.

        Returns
        -------
        StructListAccumulator
            Initialized accumulator with the field names.
        """
        return cls(field_names=tuple(field_names))

    def append_rows(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Append mapping rows for one logical list entry.

        Raises
        ------
        ValueError
            Raised when no field names are configured.
        """
        if not self.field_names:
            msg = "StructListAccumulator requires at least one field."
            raise ValueError(msg)
        for row in rows:
            for name in self.field_names:
                self.values[name].append(row.get(name))
        self.offsets.append(len(self.values[self.field_names[0]]))

    def append_tuples(self, items: Iterable[Sequence[object]]) -> None:
        """Append tuple-like rows in field order.

        Raises
        ------
        ValueError
            Raised when no field names are configured or tuple lengths mismatch.
        """
        if not self.field_names:
            msg = "StructListAccumulator requires at least one field."
            raise ValueError(msg)
        for item in items:
            values = list(item)
            if len(values) != len(self.field_names):
                msg = "StructListAccumulator tuple length mismatch."
                raise ValueError(msg)
            for name, value in zip(self.field_names, values, strict=True):
                self.values[name].append(value)
        self.offsets.append(len(self.values[self.field_names[0]]))

    def extend_from(self, other: StructListAccumulator) -> None:
        """Extend with another struct-list accumulator."""
        if len(other.offsets) <= 1:
            return
        base = self.offsets[-1]
        self.offsets.extend(base + offset for offset in other.offsets[1:])
        for name in self.field_names:
            self.values[name].extend(other.values.get(name, []))

    def build(self, *, field_types: Mapping[str, DataTypeLike]) -> ArrayLike:
        """Build a list<struct> array from accumulated values.

        Returns
        -------
        ArrayLike
            List array with struct elements.
        """
        fields = {
            name: pa.array(self.values[name], type=field_types[name]) for name in self.field_names
        }
        return build_list_of_structs(
            pa.array(self.offsets, type=pa.int32()),
            fields,
        )
