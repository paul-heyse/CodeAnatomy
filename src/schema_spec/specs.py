"""Schema specification models and shared field bundles."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from typing import Literal, cast

import arrowdsl.core.interop as pa
from arrowdsl.compute.expr import ExprSpec
from arrowdsl.core.interop import DataTypeLike, FieldLike, SchemaLike
from arrowdsl.schema.arrays import list_view_type
from arrowdsl.schema.encoding import (
    ENCODING_DICTIONARY,
    ENCODING_META,
    dict_field_metadata,
)
from arrowdsl.schema.schema import CastErrorPolicy, SchemaMetadataSpec, SchemaTransform

SCHEMA_META_NAME = b"schema_name"
SCHEMA_META_VERSION = b"schema_version"
REQUIRED_NON_NULL_META = b"required_non_null"
KEY_FIELDS_META = b"key_fields"
DICT_STRING = pa.dictionary(pa.int32(), pa.string())

PROVENANCE_COLS: tuple[str, ...] = (
    "prov_filename",
    "prov_fragment_index",
    "prov_batch_index",
    "prov_last_in_fragment",
)

PROVENANCE_SOURCE_FIELDS: dict[str, str] = {
    "prov_filename": "__filename",
    "prov_fragment_index": "__fragment_index",
    "prov_batch_index": "__batch_index",
    "prov_last_in_fragment": "__last_in_fragment",
}


def schema_metadata(name: str, version: int | None) -> dict[bytes, bytes]:
    """Return schema metadata for name/version tagging.

    Returns
    -------
    dict[bytes, bytes]
        Encoded schema metadata mapping.
    """
    meta = {SCHEMA_META_NAME: name.encode("utf-8")}
    if version is not None:
        meta[SCHEMA_META_VERSION] = str(version).encode("utf-8")
    return meta


def schema_metadata_for_spec(spec: TableSchemaSpec) -> dict[bytes, bytes]:
    """Return schema metadata encoding name/version and constraints.

    Returns
    -------
    dict[bytes, bytes]
        Encoded schema metadata mapping.
    """
    meta = schema_metadata(spec.name, spec.version)
    if spec.required_non_null:
        meta[REQUIRED_NON_NULL_META] = ",".join(spec.required_non_null).encode("utf-8")
    if spec.key_fields:
        meta[KEY_FIELDS_META] = ",".join(spec.key_fields).encode("utf-8")
    return meta


def _field_metadata(metadata: dict[str, str]) -> dict[bytes, bytes]:
    """Encode metadata keys/values for Arrow.

    Returns
    -------
    dict[bytes, bytes]
        Encoded metadata mapping.
    """
    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in metadata.items()}


@dataclass(frozen=True)
class ArrowFieldSpec:
    """Specification for a single Arrow field."""

    name: str
    dtype: DataTypeLike
    nullable: bool = True
    metadata: dict[str, str] = field(default_factory=dict)
    encoding: Literal["dictionary"] | None = None

    def to_arrow_field(self) -> FieldLike:
        """Build a pyarrow.Field from the spec.

        Returns
        -------
        pyarrow.Field
            Arrow field instance.
        """
        metadata = dict(self.metadata)
        if self.encoding is not None:
            metadata[ENCODING_META] = self.encoding
        metadata = _field_metadata(metadata)
        return pa.field(self.name, self.dtype, nullable=self.nullable, metadata=metadata)


def dict_field(
    name: str,
    *,
    index_type: DataTypeLike | None = None,
    ordered: bool = False,
    nullable: bool = True,
    metadata: dict[str, str] | None = None,
) -> ArrowFieldSpec:
    """Return an ArrowFieldSpec configured for dictionary encoding.

    Returns
    -------
    ArrowFieldSpec
        Field spec configured with dictionary encoding metadata.
    """
    idx_type = index_type or pa.int32()
    meta = dict_field_metadata(index_type=idx_type, ordered=ordered, metadata=metadata)
    dict_factory = cast("Callable[..., object]", pa.dictionary)
    dtype = cast("DataTypeLike", dict_factory(idx_type, pa.string(), ordered=ordered))
    return ArrowFieldSpec(
        name=name,
        dtype=dtype,
        nullable=nullable,
        metadata=meta,
    )


@dataclass(frozen=True)
class DerivedFieldSpec:
    """Specification for a derived column."""

    name: str
    expr: ExprSpec


@dataclass(frozen=True)
class TableSchemaSpec:
    """Specification for a table schema and associated constraints."""

    name: str
    fields: list[ArrowFieldSpec]
    version: int | None = None
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate the table schema specification.

        Raises
        ------
        ValueError
            Raised when field definitions are duplicated or constraints reference
            unknown fields.
        """
        seen: set[str] = set()
        dupes: list[str] = []
        for field_spec in self.fields:
            if field_spec.name in seen:
                dupes.append(field_spec.name)
            else:
                seen.add(field_spec.name)
        if dupes:
            msg = f"duplicate field names: {sorted(set(dupes))}"
            raise ValueError(msg)
        missing_required = [name for name in self.required_non_null if name not in seen]
        if missing_required:
            msg = f"unknown fields: {missing_required}"
            raise ValueError(msg)
        missing_keys = [name for name in self.key_fields if name not in seen]
        if missing_keys:
            msg = f"unknown fields: {missing_keys}"
            raise ValueError(msg)

    def with_constraints(
        self,
        *,
        required_non_null: Iterable[str],
        key_fields: Iterable[str],
    ) -> TableSchemaSpec:
        """Return a new TableSchemaSpec with updated constraints.

        Returns
        -------
        TableSchemaSpec
            Updated table schema spec.
        """
        return replace(
            self,
            required_non_null=tuple(required_non_null),
            key_fields=tuple(key_fields),
        )

    def to_arrow_schema(self) -> SchemaLike:
        """Build a pyarrow.Schema from the spec.

        Returns
        -------
        pyarrow.Schema
            Arrow schema instance.
        """
        schema = pa.schema([field.to_arrow_field() for field in self.fields])
        meta = schema_metadata_for_spec(self)
        return SchemaMetadataSpec(schema_metadata=meta).apply(schema)

    def to_transform(
        self,
        *,
        safe_cast: bool = True,
        keep_extra_columns: bool = False,
        on_error: CastErrorPolicy = "unsafe",
    ) -> SchemaTransform:
        """Create a schema transform for aligning tables to this spec.

        Returns
        -------
        SchemaTransform
            Transform configured for this schema spec.
        """
        return SchemaTransform(
            schema=self.to_arrow_schema(),
            safe_cast=safe_cast,
            keep_extra_columns=keep_extra_columns,
            on_error=on_error,
        )


@dataclass(frozen=True)
class FieldBundle:
    """Bundle of fields plus required/key constraints."""

    name: str
    fields: tuple[ArrowFieldSpec, ...]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class NestedFieldSpec:
    """Nested field specification with a builder hook."""

    name: str
    dtype: DataTypeLike
    builder: Callable[..., pa.ArrayLike]


def file_identity_bundle(*, include_sha256: bool = True) -> FieldBundle:
    """Return a bundle for file identity columns.

    Returns
    -------
    FieldBundle
        Bundle containing file identity fields.
    """
    fields = [
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
    ]
    if include_sha256:
        fields.append(ArrowFieldSpec(name="file_sha256", dtype=pa.string()))
    return FieldBundle(name="file_identity", fields=tuple(fields))


def span_bundle() -> FieldBundle:
    """Return a bundle for byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing byte-span fields.
    """
    return FieldBundle(
        name="span",
        fields=(
            ArrowFieldSpec(name="bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="bend", dtype=pa.int64()),
        ),
    )


def call_span_bundle() -> FieldBundle:
    """Return a bundle for callsite byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing callsite byte-span fields.
    """
    return FieldBundle(
        name="call_span",
        fields=(
            ArrowFieldSpec(name="call_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="call_bend", dtype=pa.int64()),
        ),
    )


def scip_range_bundle(*, prefix: str = "", include_len: bool = False) -> FieldBundle:
    """Return a bundle for SCIP line/character range columns.

    Returns
    -------
    FieldBundle
        Bundle containing SCIP range fields.
    """
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    fields = [
        ArrowFieldSpec(name=f"{normalized}start_line", dtype=pa.int32()),
        ArrowFieldSpec(name=f"{normalized}start_char", dtype=pa.int32()),
        ArrowFieldSpec(name=f"{normalized}end_line", dtype=pa.int32()),
        ArrowFieldSpec(name=f"{normalized}end_char", dtype=pa.int32()),
    ]
    if include_len:
        fields.append(ArrowFieldSpec(name=f"{normalized}range_len", dtype=pa.int32()))
    name = f"{normalized}scip_range" if normalized else "scip_range"
    return FieldBundle(name=name, fields=tuple(fields))


def provenance_bundle() -> FieldBundle:
    """Return a bundle for dataset scan provenance columns.

    Returns
    -------
    FieldBundle
        Bundle containing provenance fields.
    """
    return FieldBundle(
        name="provenance",
        fields=(
            ArrowFieldSpec(name=PROVENANCE_COLS[0], dtype=pa.string()),
            ArrowFieldSpec(name=PROVENANCE_COLS[1], dtype=pa.int32()),
            ArrowFieldSpec(name=PROVENANCE_COLS[2], dtype=pa.int32()),
            ArrowFieldSpec(name=PROVENANCE_COLS[3], dtype=pa.bool_()),
        ),
    )


__all__ = [
    "DICT_STRING",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "KEY_FIELDS_META",
    "PROVENANCE_COLS",
    "PROVENANCE_SOURCE_FIELDS",
    "REQUIRED_NON_NULL_META",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
    "ArrowFieldSpec",
    "DerivedFieldSpec",
    "FieldBundle",
    "NestedFieldSpec",
    "TableSchemaSpec",
    "call_span_bundle",
    "dict_field",
    "file_identity_bundle",
    "list_view_type",
    "provenance_bundle",
    "schema_metadata",
    "schema_metadata_for_spec",
    "scip_range_bundle",
    "span_bundle",
]
