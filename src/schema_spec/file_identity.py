"""Canonical file identity field definitions.

Provide centralized constants and helpers for file identity fields used throughout
the codebase. File identity fields uniquely locate source artifacts within a
repository structure.

Field Semantics
---------------
file_id : str
    Stable, content-addressable identifier derived from file path and/or hash.
    Non-nullable in most contexts.
path : str
    Relative path from repository root. Non-nullable in most contexts.
file_sha256 : str | None
    SHA-256 hash of file content. Nullable when content hash is unavailable.
repo : str | None
    Repository identifier or root path. Nullable for single-repo contexts.

Usage
-----
For DataFusion/Arrow schemas, use the raw field tuples::

    from schema_spec.file_identity import FILE_IDENTITY_FIELDS

    schema = pa.schema([*FILE_IDENTITY_FIELDS, ...])

For schema_spec bundles, use the bundle helper::

    from schema_spec.file_identity import file_identity_bundle

    bundle = file_identity_bundle(include_repo=True)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from schema_spec.field_spec import FieldSpec

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import DataTypeLike, SchemaLike

# ---------------------------------------------------------------------------
# Canonical field definitions as pyarrow.Field tuples
# ---------------------------------------------------------------------------

FILE_ID_FIELD: pa.Field = pa.field("file_id", pa.string(), nullable=False)
"""Stable file identifier. Non-nullable."""

PATH_FIELD: pa.Field = pa.field("path", pa.string(), nullable=False)
"""Relative file path from repository root. Non-nullable."""

FILE_SHA256_FIELD: pa.Field = pa.field("file_sha256", pa.string(), nullable=True)
"""SHA-256 hash of file content. Nullable when unavailable."""

REPO_FIELD: pa.Field = pa.field("repo", pa.string(), nullable=True)
"""Repository identifier. Nullable for single-repo contexts."""

# Core file identity fields (file_id, path) - always required
FILE_IDENTITY_CORE_FIELDS: tuple[pa.Field, ...] = (
    FILE_ID_FIELD,
    PATH_FIELD,
)

# Full file identity fields (repo, path, file_id, file_sha256)
FILE_IDENTITY_FIELDS: tuple[pa.Field, ...] = (
    REPO_FIELD,
    PATH_FIELD,
    FILE_ID_FIELD,
    FILE_SHA256_FIELD,
)

# File identity fields for nesting in struct types (nullable for outer joins)
FILE_IDENTITY_FIELDS_FOR_NESTING: tuple[pa.Field, ...] = (
    pa.field("file_id", pa.string(), nullable=True),
    pa.field("path", pa.string(), nullable=True),
)


# ---------------------------------------------------------------------------
# Field accessor functions
# ---------------------------------------------------------------------------


def file_identity_fields(
    *,
    include_repo: bool = True,
    include_sha256: bool = True,
) -> tuple[pa.Field, ...]:
    """Return file identity fields with optional components.

    Parameters
    ----------
    include_repo
        Include the repo field. Default True.
    include_sha256
        Include the file_sha256 field. Default True.

    Returns
    -------
    tuple[pa.Field, ...]
        Tuple of pyarrow.Field objects for file identity.
    """
    fields: list[pa.Field] = []
    if include_repo:
        fields.append(REPO_FIELD)
    fields.append(PATH_FIELD)
    fields.append(FILE_ID_FIELD)
    if include_sha256:
        fields.append(FILE_SHA256_FIELD)
    return tuple(fields)


def file_identity_struct(
    *,
    include_repo: bool = False,
    include_sha256: bool = False,
) -> DataTypeLike:
    """Return a struct type for file identity.

    Parameters
    ----------
    include_repo
        Include the repo field in the struct. Default False.
    include_sha256
        Include the file_sha256 field in the struct. Default False.

    Returns
    -------
    pa.StructType
        Struct type containing file identity fields.
    """
    return pa.struct(file_identity_fields(include_repo=include_repo, include_sha256=include_sha256))


def file_identity_fields_for_nesting() -> tuple[pa.Field, ...]:
    """Return nullable file identity fields suitable for nested structs.

    Use for file identity fields embedded in outer-join results or optional
    nested structures where nullability is required.

    Returns
    -------
    tuple[pa.Field, ...]
        Tuple of nullable file_id and path fields.
    """
    return FILE_IDENTITY_FIELDS_FOR_NESTING


def schema_with_file_identity(
    *additional_fields: pa.Field,
    include_repo: bool = True,
    include_sha256: bool = True,
) -> SchemaLike:
    """Build a schema with file identity fields prepended.

    Parameters
    ----------
    *additional_fields
        Additional fields to include after file identity fields.
    include_repo
        Include the repo field. Default True.
    include_sha256
        Include the file_sha256 field. Default True.

    Returns
    -------
    pa.Schema
        Schema with file identity fields followed by additional fields.
    """
    identity = file_identity_fields(include_repo=include_repo, include_sha256=include_sha256)
    return pa.schema([*identity, *additional_fields])


# ---------------------------------------------------------------------------
# FieldSpec-based helpers for schema_spec bundles
# ---------------------------------------------------------------------------


def file_identity_field_specs(
    *,
    include_repo: bool = False,
    include_sha256: bool = True,
) -> tuple[FieldSpec, ...]:
    """Return FieldSpec objects for file identity columns.

    Parameters
    ----------
    include_repo
        Include the repo field spec. Default False (matches legacy behavior).
    include_sha256
        Include the file_sha256 field spec. Default True.

    Returns
    -------
    tuple[FieldSpec, ...]
        Tuple of FieldSpec objects for schema_spec bundles.
    """
    from datafusion_engine.arrow import interop
    from schema_spec.arrow_types import arrow_type_from_pyarrow

    specs: list[FieldSpec] = []
    if include_repo:
        specs.append(
            FieldSpec(name="repo", dtype=arrow_type_from_pyarrow(interop.string()), nullable=True)
        )
    specs.append(FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())))
    specs.append(FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())))
    if include_sha256:
        specs.append(
            FieldSpec(name="file_sha256", dtype=arrow_type_from_pyarrow(interop.string()))
        )
    return tuple(specs)


__all__ = [
    "FILE_IDENTITY_CORE_FIELDS",
    "FILE_IDENTITY_FIELDS",
    "FILE_IDENTITY_FIELDS_FOR_NESTING",
    "FILE_ID_FIELD",
    "FILE_SHA256_FIELD",
    "PATH_FIELD",
    "REPO_FIELD",
    "file_identity_field_specs",
    "file_identity_fields",
    "file_identity_fields_for_nesting",
    "file_identity_struct",
    "schema_with_file_identity",
]
