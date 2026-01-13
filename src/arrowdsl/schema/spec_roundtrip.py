"""Helpers for encoding schema specs into Arrow metadata."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.schema.schema import SchemaMetadataSpec
from schema_spec.specs import (
    KEY_FIELDS_META,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
    TableSchemaSpec,
    schema_metadata_for_spec,
)


def _split_names(raw: bytes | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    text = raw.decode("utf-8", errors="replace")
    return tuple(name for name in text.split(",") if name)


def apply_spec_metadata(spec: TableSchemaSpec) -> SchemaMetadataSpec:
    """Return a metadata spec for the provided schema spec.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for the schema.
    """
    return SchemaMetadataSpec(schema_metadata=schema_metadata_for_spec(spec))


def schema_identity_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> tuple[str | None, int | None]:
    """Return schema name/version derived from metadata.

    Returns
    -------
    tuple[str | None, int | None]
        Schema name and version, when available.
    """
    if not metadata:
        return None, None
    name = metadata.get(SCHEMA_META_NAME)
    version = metadata.get(SCHEMA_META_VERSION)
    decoded_name = name.decode("utf-8", errors="replace") if name is not None else None
    decoded_version = None
    if version is not None:
        try:
            decoded_version = int(version.decode("utf-8", errors="replace"))
        except ValueError:
            decoded_version = None
    return decoded_name, decoded_version


def schema_constraints_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return required non-null and key fields parsed from metadata.

    Returns
    -------
    tuple[tuple[str, ...], tuple[str, ...]]
        Required non-null and key field names.
    """
    if not metadata:
        return (), ()
    required = _split_names(metadata.get(REQUIRED_NON_NULL_META))
    key_fields = _split_names(metadata.get(KEY_FIELDS_META))
    return required, key_fields


__all__ = [
    "KEY_FIELDS_META",
    "REQUIRED_NON_NULL_META",
    "apply_spec_metadata",
    "schema_constraints_from_metadata",
    "schema_identity_from_metadata",
    "schema_metadata_for_spec",
]
