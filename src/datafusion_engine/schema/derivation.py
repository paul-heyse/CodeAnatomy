"""Derive PyArrow schemas from extract metadata descriptors.

This module provides the programmatic path from ``ExtractMetadata``
field descriptors to ``pa.Schema`` objects, enabling schema derivation
that can be validated against the static registry for parity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.schema.field_types import resolve_field_type

if TYPE_CHECKING:
    from datafusion_engine.extract.metadata import ExtractMetadata

# ---------------------------------------------------------------------------
# Identity column sets
# ---------------------------------------------------------------------------

_STANDARD_IDENTITY_COLUMNS: tuple[str, ...] = ("repo", "path", "file_id", "file_sha256")
_SCIP_IDENTITY_COLUMNS: tuple[str, ...] = ("index_id", "index_path")
_REPO_FILES_IDENTITY_COLUMNS: tuple[str, ...] = ("file_id", "path", "file_sha256")

# Datasets that use non-standard identity column layouts.
_SCIP_DATASET_NAMES: frozenset[str] = frozenset({"scip_index_v1"})
_REPO_FILES_DATASET_NAMES: frozenset[str] = frozenset({"repo_files_v1"})


def _identity_columns_for(metadata: ExtractMetadata) -> tuple[str, ...]:
    """Return the identity column names for the given dataset metadata."""
    if metadata.name in _SCIP_DATASET_NAMES:
        return _SCIP_IDENTITY_COLUMNS
    if metadata.name in _REPO_FILES_DATASET_NAMES:
        return _REPO_FILES_IDENTITY_COLUMNS
    return _STANDARD_IDENTITY_COLUMNS


def derive_extract_schema(metadata: ExtractMetadata) -> pa.Schema:
    """Derive a ``pa.Schema`` from extract metadata descriptors.

    The derived schema includes identity columns followed by evidence
    columns in ``metadata.fields`` order, with types resolved from
    ``metadata.field_types``.

    Args:
        metadata: Extract metadata with populated ``fields`` and ``field_types``.

    Returns:
        Derived schema matching the structure of the static registry
        schema for this dataset.

    Raises:
        ValueError: If a field has no type information in
            ``metadata.field_types`` and cannot be resolved.
    """
    identity_cols = _identity_columns_for(metadata)
    fields: list[pa.Field] = [pa.field(col_name, pa.string()) for col_name in identity_cols]

    # Evidence columns from metadata.fields, skipping any that are
    # already covered by the identity columns (e.g. SCIP's index_id,
    # index_path appear in both identity and fields).
    identity_set = frozenset(identity_cols)
    for field_name in metadata.fields:
        if field_name in identity_set:
            continue
        type_hint = metadata.field_types.get(field_name, "")
        if not type_hint:
            msg = (
                f"No field_type for {field_name!r} in dataset {metadata.name!r}. "
                f"Available field_types: {sorted(metadata.field_types)}"
            )
            raise ValueError(msg)
        pa_type = resolve_field_type(field_name, type_hint)
        nested_child_names = metadata.nested_shapes.get(field_name)
        if nested_child_names:
            nested_struct = pa.struct(
                [pa.field(child_name, pa.string()) for child_name in nested_child_names]
            )
            if pa.types.is_large_list(pa_type):
                pa_type = pa.large_list(nested_struct)
            elif pa.types.is_list(pa_type):
                pa_type = pa.list_(nested_struct)
            elif pa.types.is_struct(pa_type):
                pa_type = nested_struct
        fields.append(pa.field(field_name, pa_type))

    return pa.schema(fields)


def derive_nested_dataset_schema(
    metadata: ExtractMetadata,
    nested_path: str,
) -> pa.Schema | None:
    """Derive a schema for a nested dataset from metadata shapes.

    Args:
        metadata: Parent dataset metadata with ``nested_shapes`` populated.
        nested_path: Dot-separated column path within the parent
            (e.g. ``"nodes"``).

    Returns:
        Schema with child field names if the shape is available,
        otherwise ``None``.
    """
    # Only handle single-level paths for now.
    if "." in nested_path:
        return None

    child_fields = metadata.nested_shapes.get(nested_path)
    if child_fields is None:
        return None

    # For nested datasets we produce a flat schema of string-typed columns,
    # since the exact child types require deeper resolution from the static
    # schema.  The field *names* are the deliverable for parity testing.
    return pa.schema([pa.field(name, pa.string()) for name in child_fields])


__all__ = [
    "derive_extract_schema",
    "derive_nested_dataset_schema",
]
