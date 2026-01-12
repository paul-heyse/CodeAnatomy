"""Helpers for encoding schema specs into Arrow metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.schema.schema import SchemaMetadataSpec
from schema_spec.specs import (
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
    TableSchemaSpec,
    schema_metadata,
)

REQUIRED_NON_NULL_META = b"required_non_null"
KEY_FIELDS_META = b"key_fields"


def _join_names(names: Sequence[str]) -> bytes:
    return ",".join(names).encode("utf-8")


def _split_names(raw: bytes | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    text = raw.decode("utf-8", errors="replace")
    return tuple(name for name in text.split(",") if name)


def schema_metadata_for_spec(spec: TableSchemaSpec) -> dict[bytes, bytes]:
    """Return schema metadata encoding name/version and constraints."""
    meta = schema_metadata(spec.name, spec.version)
    if spec.required_non_null:
        meta[REQUIRED_NON_NULL_META] = _join_names(spec.required_non_null)
    if spec.key_fields:
        meta[KEY_FIELDS_META] = _join_names(spec.key_fields)
    return meta


def apply_spec_metadata(spec: TableSchemaSpec) -> SchemaMetadataSpec:
    """Return a metadata spec for the provided schema spec."""
    return SchemaMetadataSpec(schema_metadata=schema_metadata_for_spec(spec))


def schema_identity_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> tuple[str | None, int | None]:
    """Return schema name/version derived from metadata."""
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
    """Return required non-null and key fields parsed from metadata."""
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
