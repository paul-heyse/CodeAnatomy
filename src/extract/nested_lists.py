"""Shared nested list/struct accumulator helpers for extractors."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

import arrowdsl.core.interop as pa
from arrowdsl.core.interop import ArrayLike, DataTypeLike
from arrowdsl.schema.arrays import build_list, build_list_of_structs, build_list_view


def _offsets_start() -> list[int]:
    return [0]


def _sizes_start() -> list[int]:
    return []


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
        return build_list(
            pa.array(self.offsets, type=pa.int32()),
            pa.array(self.values, type=value_type),
        )


@dataclass
class LargeListAccumulator[T]:
    """Accumulate list offsets and values for large_list-typed columns."""

    offsets: list[int] = field(default_factory=_offsets_start)
    values: list[T | None] = field(default_factory=list)

    def append(self, items: Iterable[T | None]) -> None:
        """Append items for one logical row."""
        self.values.extend(items)
        self.offsets.append(len(self.values))

    def extend_from(self, other: LargeListAccumulator[T]) -> None:
        """Extend with another accumulator's offsets and values."""
        if len(other.offsets) <= 1:
            return
        base = self.offsets[-1]
        self.offsets.extend(base + offset for offset in other.offsets[1:])
        self.values.extend(other.values)

    def build(self, *, value_type: DataTypeLike) -> ArrayLike:
        """Build a large_list array for the accumulated values.

        Returns
        -------
        ArrayLike
            Large list array from offsets and values.
        """
        offsets = pa.array(self.offsets, type=pa.int64())
        values = pa.array(self.values, type=value_type)
        return pa.LargeListArray.from_arrays(offsets, values)


@dataclass
class ListViewAccumulator[T]:
    """Accumulate offsets/sizes for list_view columns with shared buffers."""

    offsets: list[int] = field(default_factory=_sizes_start)
    sizes: list[int] = field(default_factory=_sizes_start)
    values: list[T | None] = field(default_factory=list)

    def append(self, items: Iterable[T | None]) -> None:
        """Append items for one logical row."""
        items_list = list(items)
        self.offsets.append(len(self.values))
        self.sizes.append(len(items_list))
        self.values.extend(items_list)

    def extend_from(self, other: ListViewAccumulator[T]) -> None:
        """Extend with another list_view accumulator."""
        if not other.offsets:
            return
        base = len(self.values)
        self.offsets.extend(base + offset for offset in other.offsets)
        self.sizes.extend(other.sizes)
        self.values.extend(other.values)

    def build(self, *, value_type: DataTypeLike) -> ArrayLike:
        """Build a list_view array from the accumulated values.

        Returns
        -------
        ArrayLike
            List view array from offsets/sizes and values.
        """
        offsets = pa.array(self.offsets, type=pa.int32())
        sizes = pa.array(self.sizes, type=pa.int32())
        values = pa.array(self.values, type=value_type)
        list_type = pa.list_view(value_type)
        return build_list_view(offsets, sizes, values, list_type=list_type)


@dataclass
class LargeListViewAccumulator[T]:
    """Accumulate offsets/sizes for large_list_view columns."""

    offsets: list[int] = field(default_factory=_sizes_start)
    sizes: list[int] = field(default_factory=_sizes_start)
    values: list[T | None] = field(default_factory=list)

    def append(self, items: Iterable[T | None]) -> None:
        """Append items for one logical row."""
        items_list = list(items)
        self.offsets.append(len(self.values))
        self.sizes.append(len(items_list))
        self.values.extend(items_list)

    def extend_from(self, other: LargeListViewAccumulator[T]) -> None:
        """Extend with another list_view accumulator."""
        if not other.offsets:
            return
        base = len(self.values)
        self.offsets.extend(base + offset for offset in other.offsets)
        self.sizes.extend(other.sizes)
        self.values.extend(other.values)

    def build(self, *, value_type: DataTypeLike) -> ArrayLike:
        """Build a large_list_view array from the accumulated values.

        Returns
        -------
        ArrayLike
            Large list view array from offsets/sizes and values.
        """
        offsets = pa.array(self.offsets, type=pa.int64())
        sizes = pa.array(self.sizes, type=pa.int64())
        values = pa.array(self.values, type=value_type)
        list_type = pa.large_list_view(value_type)
        return build_list_view(offsets, sizes, values, list_type=list_type)


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


@dataclass
class StructLargeListAccumulator:
    """Accumulate list<struct> values for large_list outputs."""

    field_names: tuple[str, ...]
    offsets: list[int] = field(default_factory=_offsets_start)
    values: dict[str, list[object]] = field(init=False)

    def __post_init__(self) -> None:
        """Initialize the per-field value buffers."""
        self.values = {name: [] for name in self.field_names}

    @classmethod
    def with_fields(cls, field_names: Sequence[str]) -> StructLargeListAccumulator:
        """Construct an accumulator with the provided field names.

        Returns
        -------
        StructLargeListAccumulator
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
            msg = "StructLargeListAccumulator requires at least one field."
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
            msg = "StructLargeListAccumulator requires at least one field."
            raise ValueError(msg)
        for item in items:
            values = list(item)
            if len(values) != len(self.field_names):
                msg = "StructLargeListAccumulator tuple length mismatch."
                raise ValueError(msg)
            for name, value in zip(self.field_names, values, strict=True):
                self.values[name].append(value)
        self.offsets.append(len(self.values[self.field_names[0]]))

    def extend_from(self, other: StructLargeListAccumulator) -> None:
        """Extend with another struct-list accumulator."""
        if len(other.offsets) <= 1:
            return
        base = self.offsets[-1]
        self.offsets.extend(base + offset for offset in other.offsets[1:])
        for name in self.field_names:
            self.values[name].extend(other.values.get(name, []))

    def build(self, *, field_types: Mapping[str, DataTypeLike]) -> ArrayLike:
        """Build a large_list<struct> array from accumulated values.

        Returns
        -------
        ArrayLike
            Large list array with struct elements.
        """
        fields = {
            name: pa.array(self.values[name], type=field_types[name]) for name in self.field_names
        }
        struct_values = pa.StructArray.from_arrays(
            list(fields.values()),
            names=list(fields.keys()),
        )
        offsets = pa.array(self.offsets, type=pa.int64())
        return pa.LargeListArray.from_arrays(offsets, struct_values)


@dataclass
class StructLargeListViewAccumulator:
    """Accumulate list_view<struct> values for large_list_view outputs."""

    field_names: tuple[str, ...]
    offsets: list[int] = field(default_factory=_sizes_start)
    sizes: list[int] = field(default_factory=_sizes_start)
    values: dict[str, list[object]] = field(init=False)

    def __post_init__(self) -> None:
        """Initialize the per-field value buffers."""
        self.values = {name: [] for name in self.field_names}

    @classmethod
    def with_fields(cls, field_names: Sequence[str]) -> StructLargeListViewAccumulator:
        """Construct an accumulator with the provided field names.

        Returns
        -------
        StructLargeListViewAccumulator
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
            msg = "StructLargeListViewAccumulator requires at least one field."
            raise ValueError(msg)
        items = list(rows)
        self.offsets.append(len(self.values[self.field_names[0]]))
        self.sizes.append(len(items))
        for row in items:
            for name in self.field_names:
                self.values[name].append(row.get(name))

    def append_tuples(self, items: Iterable[Sequence[object]]) -> None:
        """Append tuple-like rows in field order.

        Raises
        ------
        ValueError
            Raised when no field names are configured or tuple lengths mismatch.
        """
        if not self.field_names:
            msg = "StructLargeListViewAccumulator requires at least one field."
            raise ValueError(msg)
        items_list = [list(item) for item in items]
        self.offsets.append(len(self.values[self.field_names[0]]))
        self.sizes.append(len(items_list))
        for values in items_list:
            if len(values) != len(self.field_names):
                msg = "StructLargeListViewAccumulator tuple length mismatch."
                raise ValueError(msg)
            for name, value in zip(self.field_names, values, strict=True):
                self.values[name].append(value)

    def extend_from(self, other: StructLargeListViewAccumulator) -> None:
        """Extend with another struct list_view accumulator."""
        if not other.offsets:
            return
        base = len(self.values[self.field_names[0]])
        self.offsets.extend(base + offset for offset in other.offsets)
        self.sizes.extend(other.sizes)
        for name in self.field_names:
            self.values[name].extend(other.values.get(name, []))

    def build(self, *, field_types: Mapping[str, DataTypeLike]) -> ArrayLike:
        """Build a large_list_view<struct> array from accumulated values.

        Returns
        -------
        ArrayLike
            Large list view array with struct elements.
        """
        fields = {
            name: pa.array(self.values[name], type=field_types[name]) for name in self.field_names
        }
        struct_values = pa.StructArray.from_arrays(
            list(fields.values()),
            names=list(fields.keys()),
        )
        offsets = pa.array(self.offsets, type=pa.int64())
        sizes = pa.array(self.sizes, type=pa.int64())
        list_type = pa.large_list_view(struct_values.type)
        return build_list_view(offsets, sizes, struct_values, list_type=list_type)


__all__ = [
    "LargeListAccumulator",
    "LargeListViewAccumulator",
    "ListAccumulator",
    "ListViewAccumulator",
    "StructLargeListAccumulator",
    "StructLargeListViewAccumulator",
    "StructListAccumulator",
]
