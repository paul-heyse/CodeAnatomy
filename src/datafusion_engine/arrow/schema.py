"""Shared Arrow schema builders for common patterns."""

from __future__ import annotations

import pyarrow as pa


def id_field(name: str = "id", *, nullable: bool = False) -> pa.Field:
    """Return standard identifier field (string).

    Returns:
    -------
    pa.Field
        Identifier field definition.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def version_field(name: str = "version", *, nullable: bool = False) -> pa.Field:
    """Return version field (int32).

    Returns:
    -------
    pa.Field
        Version field definition.
    """
    return pa.field(name, pa.int32(), nullable=nullable)


def timestamp_field(name: str = "timestamp", *, nullable: bool = True) -> pa.Field:
    """Return timestamp field (timestamp[us]).

    Returns:
    -------
    pa.Field
        Timestamp field definition.
    """
    return pa.field(name, pa.timestamp("us"), nullable=nullable)


def task_name_field(name: str = "task_name", *, nullable: bool = False) -> pa.Field:
    """Return task name field (string).

    Returns:
    -------
    pa.Field
        Task name field definition.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def fingerprint_field(name: str = "fingerprint", *, nullable: bool = False) -> pa.Field:
    """Return fingerprint field (string).

    Returns:
    -------
    pa.Field
        Fingerprint field definition.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def plan_fingerprint_field(
    name: str = "plan_fingerprint",
    *,
    nullable: bool = False,
) -> pa.Field:
    """Return plan fingerprint field (string).

    Returns:
    -------
    pa.Field
        Plan fingerprint field definition.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def dataset_name_field(
    name: str = "dataset_name",
    *,
    nullable: bool = False,
) -> pa.Field:
    """Return dataset name field (string).

    Returns:
    -------
    pa.Field
        Dataset name field definition.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def path_field(name: str = "path", *, nullable: bool = False) -> pa.Field:
    """Return file path field (string).

    Returns:
    -------
    pa.Field
        File path field definition.
    """
    return pa.field(name, pa.string(), nullable=nullable)


def line_field(name: str = "line", *, nullable: bool = False) -> pa.Field:
    """Return line number field (int32).

    Returns:
    -------
    pa.Field
        Line number field definition.
    """
    return pa.field(name, pa.int32(), nullable=nullable)


def column_field(name: str = "column", *, nullable: bool = True) -> pa.Field:
    """Return column number field (int32).

    Returns:
    -------
    pa.Field
        Column number field definition.
    """
    return pa.field(name, pa.int32(), nullable=nullable)


def byte_offset_field(name: str, *, nullable: bool = False) -> pa.Field:
    """Return byte offset field (int64).

    Returns:
    -------
    pa.Field
        Byte offset field definition.
    """
    return pa.field(name, pa.int64(), nullable=nullable)


_DEFAULT_VERSION_TYPE = pa.int32()


def map_entry_type(*, with_kind: bool = True) -> pa.StructType:
    """Return a map entry struct type.

    Parameters
    ----------
    with_kind
        When True, include a value_kind field for type-tagging.

    Returns:
    -------
    pa.StructType
        Struct type for map entries.
    """
    fields: list[pa.Field] = [pa.field("key", pa.string(), nullable=False)]
    if with_kind:
        fields.append(pa.field("value_kind", pa.string(), nullable=False))
    fields.append(pa.field("value", pa.string(), nullable=True))
    return pa.struct(fields)


def versioned_entries_schema(
    entry_type: pa.DataType,
    *,
    version_type: pa.DataType | None = None,
    entries_nullable: bool = True,
) -> pa.Schema:
    """Return a versioned entries schema.

    Returns:
    -------
    pa.Schema
        Schema with version and entries fields.
    """
    resolved_version = version_type or _DEFAULT_VERSION_TYPE
    return pa.schema(
        [
            pa.field("version", resolved_version, nullable=False),
            pa.field("entries", pa.list_(entry_type), nullable=entries_nullable),
        ]
    )


def string_list_schema(*, version_type: pa.DataType | None = None) -> pa.Schema:
    """Return a versioned string list schema.

    Returns:
    -------
    pa.Schema
        Schema for versioned string lists.
    """
    return versioned_entries_schema(pa.string(), version_type=version_type)


def versioned_metadata_schema(
    name: str,
    extra_fields: list[pa.Field],
    *,
    schema_version: int = 1,
) -> pa.Schema:
    """Return a metadata schema with version and name fields.

    Parameters
    ----------
    name
        Base name for the identifier field.
    extra_fields
        Additional fields to include.
    schema_version
        Schema version for metadata.

    Returns:
    -------
    pa.Schema
        Metadata schema with version and identifier fields.
    """
    fields = [version_field(), pa.field(name, pa.string(), nullable=False), *extra_fields]
    metadata = {"schema_version": str(schema_version)}
    return pa.schema(fields, metadata=metadata)


def plan_fingerprint_entry_type() -> pa.StructType:
    """Return a struct type for plan fingerprint entries.

    Returns:
    -------
    pa.StructType
        Struct type for plan fingerprint entries.
    """
    return pa.struct(
        [
            pa.field("plan_name", pa.string(), nullable=False),
            pa.field("plan_fingerprint", pa.string(), nullable=False),
            pa.field("plan_task_signature", pa.string(), nullable=False),
        ]
    )


__all__ = [
    "byte_offset_field",
    "column_field",
    "dataset_name_field",
    "fingerprint_field",
    "id_field",
    "line_field",
    "map_entry_type",
    "path_field",
    "plan_fingerprint_entry_type",
    "plan_fingerprint_field",
    "string_list_schema",
    "task_name_field",
    "timestamp_field",
    "version_field",
    "versioned_entries_schema",
    "versioned_metadata_schema",
]
