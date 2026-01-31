"""Fluent nested type builders for Arrow schemas.

Provide a builder pattern for constructing complex nested Arrow types (structs,
lists, maps) in a consistent, type-safe manner. Common nested type templates
are pre-defined for reuse across the codebase.

Examples
--------
Build a custom struct type:

    >>> builder = NestedTypeBuilder()
    >>> builder.add_field("name", pa.string()).add_field("age", pa.int32())
    >>> struct_type = builder.build_struct()

Build a nested struct with embedded span:

    >>> builder = NestedTypeBuilder()
    >>> builder.add_struct("location", span_struct_builder())
    >>> builder.add_field("value", pa.float64())
    >>> schema = builder.build_schema()

Use pre-defined templates:

    >>> span_type = span_struct_type()  # Full span with line/col and byte positions
    >>> byte_span = byte_span_struct_type()  # Just byte start/len
    >>> attrs = attrs_map_type()  # Map(Utf8, Utf8) for attributes
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.interop import DataTypeLike, FieldLike, SchemaLike

if TYPE_CHECKING:
    from typing import Self


# ---------------------------------------------------------------------------
# Fluent Builder
# ---------------------------------------------------------------------------


@dataclass
class NestedTypeBuilder:
    """Fluent builder for constructing nested Arrow types.

    Accumulate field definitions and build struct types, schemas, or individual
    nested types. Supports chaining via fluent interface.

    Attributes
    ----------
    fields : list[pa.Field]
        Accumulated field definitions.
    """

    fields: list[pa.Field] = field(default_factory=list)

    def add_field(
        self,
        name: str,
        dtype: DataTypeLike,
        *,
        nullable: bool = True,
        metadata: dict[bytes, bytes] | None = None,
    ) -> Self:
        """Add a scalar field to the builder.

        Parameters
        ----------
        name
            Field name.
        dtype
            Arrow data type for the field.
        nullable
            Whether the field accepts null values. Default True.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self.fields.append(pa.field(name, dtype, nullable=nullable, metadata=metadata))
        return self

    def add_struct(
        self,
        name: str,
        builder: NestedTypeBuilder,
        *,
        nullable: bool = True,
        metadata: dict[bytes, bytes] | None = None,
    ) -> Self:
        """Add a nested struct field from another builder.

        Parameters
        ----------
        name
            Field name for the struct.
        builder
            Nested type builder defining the struct fields.
        nullable
            Whether the struct field accepts null values. Default True.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        struct_type = builder.build_struct()
        self.fields.append(pa.field(name, struct_type, nullable=nullable, metadata=metadata))
        return self

    def add_struct_type(
        self,
        name: str,
        struct_type: DataTypeLike,
        *,
        nullable: bool = True,
        metadata: dict[bytes, bytes] | None = None,
    ) -> Self:
        """Add a nested struct field from an existing struct type.

        Parameters
        ----------
        name
            Field name for the struct.
        struct_type
            Pre-built struct type.
        nullable
            Whether the struct field accepts null values. Default True.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        self.fields.append(pa.field(name, struct_type, nullable=nullable, metadata=metadata))
        return self

    def add_list(
        self,
        name: str,
        item_type: DataTypeLike,
        *,
        nullable: bool = True,
        large: bool = False,
        metadata: dict[bytes, bytes] | None = None,
    ) -> Self:
        """Add a list field with the given item type.

        Parameters
        ----------
        name
            Field name for the list.
        item_type
            Arrow data type for list elements.
        nullable
            Whether the list field accepts null values. Default True.
        large
            Use large_list type for >2GB arrays. Default False.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        list_type = pa.large_list(item_type) if large else pa.list_(item_type)
        self.fields.append(pa.field(name, list_type, nullable=nullable, metadata=metadata))
        return self

    def add_map(  # noqa: PLR0913
        self,
        name: str,
        key_type: DataTypeLike,
        value_type: DataTypeLike,
        *,
        nullable: bool = True,
        keys_sorted: bool = False,
        metadata: dict[bytes, bytes] | None = None,
    ) -> Self:
        """Add a map field with key and value types.

        Parameters
        ----------
        name
            Field name for the map.
        key_type
            Arrow data type for map keys.
        value_type
            Arrow data type for map values.
        nullable
            Whether the map field accepts null values. Default True.
        keys_sorted
            Whether map keys are sorted. Default False.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        map_type = pa.map_(key_type, value_type, keys_sorted=keys_sorted)
        self.fields.append(pa.field(name, map_type, nullable=nullable, metadata=metadata))
        return self

    def add_list_of_structs(
        self,
        name: str,
        builder: NestedTypeBuilder,
        *,
        nullable: bool = True,
        large: bool = False,
        metadata: dict[bytes, bytes] | None = None,
    ) -> Self:
        """Add a list field containing struct elements.

        Parameters
        ----------
        name
            Field name for the list.
        builder
            Nested type builder defining the struct element fields.
        nullable
            Whether the list field accepts null values. Default True.
        large
            Use large_list type for >2GB arrays. Default False.
        metadata
            Optional field metadata.

        Returns
        -------
        Self
            Builder instance for chaining.
        """
        struct_type = builder.build_struct()
        list_type = pa.large_list(struct_type) if large else pa.list_(struct_type)
        self.fields.append(pa.field(name, list_type, nullable=nullable, metadata=metadata))
        return self

    def build_struct(self) -> pa.StructType:
        """Build a struct type from accumulated fields.

        Returns
        -------
        pa.StructType
            Struct type containing all added fields.
        """
        return pa.struct(self.fields)

    def build_schema(self, *, metadata: dict[bytes, bytes] | None = None) -> SchemaLike:
        """Build an Arrow schema from accumulated fields.

        Parameters
        ----------
        metadata
            Optional schema-level metadata.

        Returns
        -------
        pa.Schema
            Arrow schema containing all added fields.
        """
        return pa.schema(self.fields, metadata=metadata)

    def build_field(
        self,
        name: str,
        *,
        nullable: bool = True,
        metadata: dict[bytes, bytes] | None = None,
    ) -> FieldLike:
        """Build a single field with struct type from accumulated fields.

        Parameters
        ----------
        name
            Name for the resulting field.
        nullable
            Whether the field accepts null values. Default True.
        metadata
            Optional field metadata.

        Returns
        -------
        pa.Field
            Field with struct type containing all added fields.
        """
        return pa.field(name, self.build_struct(), nullable=nullable, metadata=metadata)


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def nested_field(
    name: str,
    nested_builder: NestedTypeBuilder,
    *,
    nullable: bool = True,
    metadata: dict[bytes, bytes] | None = None,
) -> FieldLike:
    """Create a field with nested struct type from a builder.

    Parameters
    ----------
    name
        Field name.
    nested_builder
        Builder defining the struct fields.
    nullable
        Whether the field accepts null values. Default True.
    metadata
        Optional field metadata.

    Returns
    -------
    pa.Field
        Field with struct data type.
    """
    return pa.field(
        name,
        nested_builder.build_struct(),
        nullable=nullable,
        metadata=metadata,
    )


def list_of(
    item_type: DataTypeLike,
    *,
    large: bool = False,
) -> DataTypeLike:
    """Create a list type with the given item type.

    Parameters
    ----------
    item_type
        Arrow data type for list elements.
    large
        Use large_list for >2GB arrays. Default False.

    Returns
    -------
    pa.ListType | pa.LargeListType
        Arrow list data type.
    """
    return pa.large_list(item_type) if large else pa.list_(item_type)


def list_of_structs(
    builder: NestedTypeBuilder,
    *,
    large: bool = False,
) -> DataTypeLike:
    """Create a list type containing struct elements.

    Parameters
    ----------
    builder
        Builder defining the struct element fields.
    large
        Use large_list for >2GB arrays. Default False.

    Returns
    -------
    pa.ListType | pa.LargeListType
        Arrow list data type with struct elements.
    """
    struct_type = builder.build_struct()
    return pa.large_list(struct_type) if large else pa.list_(struct_type)


def map_of(
    key_type: DataTypeLike,
    value_type: DataTypeLike,
    *,
    keys_sorted: bool = False,
) -> DataTypeLike:
    """Create a map type with key and value types.

    Parameters
    ----------
    key_type
        Arrow data type for map keys.
    value_type
        Arrow data type for map values.
    keys_sorted
        Whether keys are sorted. Default False.

    Returns
    -------
    pa.MapType
        Arrow map data type.
    """
    return pa.map_(key_type, value_type, keys_sorted=keys_sorted)


# ---------------------------------------------------------------------------
# Common Nested Type Templates
# ---------------------------------------------------------------------------


def line_col_struct_builder() -> NestedTypeBuilder:
    """Return a builder for line/column position struct.

    The struct contains:
    - line0: int32 (0-indexed line number)
    - col: int32 (column position)

    Returns
    -------
    NestedTypeBuilder
        Builder configured for line/column struct.
    """
    return (
        NestedTypeBuilder()
        .add_field("line0", pa.int32(), nullable=True)
        .add_field("col", pa.int32(), nullable=True)
    )


def line_col_struct_type() -> pa.StructType:
    """Return the line/column position struct type.

    Returns
    -------
    pa.StructType
        Struct type for line/column positions.
    """
    return line_col_struct_builder().build_struct()


def byte_span_struct_builder() -> NestedTypeBuilder:
    """Return a builder for byte span struct.

    The struct contains:
    - byte_start: int32 (byte offset from file start)
    - byte_len: int32 (length in bytes)

    Returns
    -------
    NestedTypeBuilder
        Builder configured for byte span struct.
    """
    return (
        NestedTypeBuilder()
        .add_field("byte_start", pa.int32(), nullable=True)
        .add_field("byte_len", pa.int32(), nullable=True)
    )


def byte_span_struct_type() -> pa.StructType:
    """Return the byte span struct type.

    This matches the canonical BYTE_SPAN_STORAGE type used throughout
    the codebase for byte-level span representation.

    Returns
    -------
    pa.StructType
        Struct type for byte spans.
    """
    return byte_span_struct_builder().build_struct()


def span_struct_builder() -> NestedTypeBuilder:
    """Return a builder for the full span struct.

    The struct contains:
    - start: struct(line0: int32, col: int32)
    - end: struct(line0: int32, col: int32)
    - end_exclusive: bool
    - col_unit: string (e.g., "byte", "char")
    - byte_span: struct(byte_start: int32, byte_len: int32)

    Returns
    -------
    NestedTypeBuilder
        Builder configured for full span struct.
    """
    return (
        NestedTypeBuilder()
        .add_struct("start", line_col_struct_builder(), nullable=True)
        .add_struct("end", line_col_struct_builder(), nullable=True)
        .add_field("end_exclusive", pa.bool_(), nullable=True)
        .add_field("col_unit", pa.string(), nullable=True)
        .add_struct("byte_span", byte_span_struct_builder(), nullable=True)
    )


def span_struct_type() -> pa.StructType:
    """Return the full span struct type.

    This matches the canonical SPAN_STORAGE type used throughout the
    codebase for source location representation.

    Returns
    -------
    pa.StructType
        Struct type for full spans with line/col and byte positions.
    """
    return span_struct_builder().build_struct()


def attrs_map_type() -> DataTypeLike:
    """Return the standard attributes map type.

    This is Map(Utf8, Utf8), matching ATTRS_T used throughout the
    schema registry for key-value attribute storage.

    Returns
    -------
    pa.MapType
        Map type for string key-value attributes.
    """
    return pa.map_(pa.string(), pa.string())


def string_list_type(*, large: bool = False) -> DataTypeLike:
    """Return a list of strings type.

    Parameters
    ----------
    large
        Use large_list for >2GB arrays. Default False.

    Returns
    -------
    pa.ListType | pa.LargeListType
        List type containing string elements.
    """
    return pa.large_list(pa.string()) if large else pa.list_(pa.string())


def int32_list_type(*, large: bool = False) -> DataTypeLike:
    """Return a list of int32 type.

    Parameters
    ----------
    large
        Use large_list for >2GB arrays. Default False.

    Returns
    -------
    pa.ListType | pa.LargeListType
        List type containing int32 elements.
    """
    return pa.large_list(pa.int32()) if large else pa.list_(pa.int32())


def int64_list_type(*, large: bool = False) -> DataTypeLike:
    """Return a list of int64 type.

    Parameters
    ----------
    large
        Use large_list for >2GB arrays. Default False.

    Returns
    -------
    pa.ListType | pa.LargeListType
        List type containing int64 elements.
    """
    return pa.large_list(pa.int64()) if large else pa.list_(pa.int64())


__all__ = [
    "NestedTypeBuilder",
    "attrs_map_type",
    "byte_span_struct_builder",
    "byte_span_struct_type",
    "int32_list_type",
    "int64_list_type",
    "line_col_struct_builder",
    "line_col_struct_type",
    "list_of",
    "list_of_structs",
    "map_of",
    "nested_field",
    "span_struct_builder",
    "span_struct_type",
    "string_list_type",
]
