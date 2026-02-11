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

import pyarrow as pa

from schema_spec.field_spec import FieldSpec

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

    Returns:
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
        specs.append(FieldSpec(name="file_sha256", dtype=arrow_type_from_pyarrow(interop.string())))
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
]
