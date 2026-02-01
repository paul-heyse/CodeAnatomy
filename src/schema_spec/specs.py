"""Schema specification models and shared field bundles."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa

from arrow_utils.core.schema_constants import (
    KEY_FIELDS_META,
    PROVENANCE_COLS,
    PROVENANCE_SOURCE_FIELDS,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from datafusion_engine.arrow import interop
from datafusion_engine.arrow.build import list_view_type
from datafusion_engine.arrow.interop import DataTypeLike, SchemaLike
from datafusion_engine.arrow.metadata import (
    ENCODING_DICTIONARY,
    ENCODING_META,
    SchemaMetadataSpec,
    dict_field_metadata,
)
from datafusion_engine.arrow.metadata_codec import encode_metadata_list
from datafusion_engine.schema.policy import CastErrorPolicy, SchemaTransform
from schema_spec.field_spec import FieldSpec

DICT_STRING = interop.dictionary(interop.int32(), interop.string())

if TYPE_CHECKING:
    from datafusion_engine.expr.spec import ExprSpec


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
        meta[REQUIRED_NON_NULL_META] = encode_metadata_list(spec.required_non_null)
    if spec.key_fields:
        meta[KEY_FIELDS_META] = encode_metadata_list(spec.key_fields)
    return meta


def _decode_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Decode Arrow metadata into a string-keyed mapping.

    Returns
    -------
    dict[str, str]
        Decoded metadata mapping with string keys and values.
    """
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def _encoding_hint_from_field(
    field_meta: Mapping[str, str],
    *,
    dtype: DataTypeLike,
) -> Literal["dictionary"] | None:
    hint = field_meta.get(ENCODING_META)
    if hint == ENCODING_DICTIONARY:
        return ENCODING_DICTIONARY
    if pa.types.is_dictionary(_ensure_arrow_dtype(dtype)):
        return ENCODING_DICTIONARY
    return None


def _ensure_arrow_dtype(dtype: DataTypeLike) -> pa.DataType:
    if isinstance(dtype, pa.DataType):
        return dtype
    msg = f"Expected pyarrow.DataType, got {type(dtype)!r}."
    raise TypeError(msg)


def dict_field(
    name: str,
    *,
    index_type: DataTypeLike | None = None,
    ordered: bool = False,
    nullable: bool = True,
    metadata: dict[str, str] | None = None,
) -> FieldSpec:
    """Return a FieldSpec configured for dictionary encoding.

    Returns
    -------
    FieldSpec
        Field spec configured with dictionary encoding metadata.
    """
    idx_type = index_type or interop.int32()
    meta = dict_field_metadata(index_type=idx_type, ordered=ordered, metadata=metadata)
    dict_factory = cast("Callable[..., object]", interop.dictionary)
    dtype = cast("DataTypeLike", dict_factory(idx_type, interop.string(), ordered=ordered))
    return FieldSpec(
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
    fields: list[FieldSpec]
    version: int | None = None
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()

    @classmethod
    def from_schema(
        cls,
        name: str,
        schema: SchemaLike,
        *,
        version: int | None = None,
    ) -> TableSchemaSpec:
        """Create a table schema spec from an Arrow schema.

        Parameters
        ----------
        name
            Dataset name to use when schema metadata omits one.
        schema
            Arrow schema to convert.
        version
            Optional version override for schema metadata.

        Returns
        -------
        TableSchemaSpec
            Table schema specification derived from the Arrow schema.
        """
        from datafusion_engine.arrow.metadata import (
            schema_constraints_from_metadata,
            schema_identity_from_metadata,
        )

        fields: list[FieldSpec] = []
        for schema_field in schema:
            meta = _decode_metadata(schema_field.metadata)
            encoding = _encoding_hint_from_field(meta, dtype=schema_field.type)
            fields.append(
                FieldSpec(
                    name=schema_field.name,
                    dtype=schema_field.type,
                    nullable=schema_field.nullable,
                    metadata=meta,
                    encoding=encoding,
                    default_value=meta.get("default_value"),
                )
            )
        identity = schema_identity_from_metadata(schema.metadata)
        meta_name = identity.get("name")
        meta_version = identity.get("version")
        parsed_version: int | None = None
        if isinstance(meta_version, str):
            try:
                parsed_version = int(meta_version)
            except ValueError:
                parsed_version = None
        required_non_null, key_fields = schema_constraints_from_metadata(schema.metadata)
        resolved_name = meta_name or name
        resolved_version = version if version is not None else parsed_version
        return cls(
            name=resolved_name,
            version=resolved_version,
            fields=fields,
            required_non_null=required_non_null,
            key_fields=key_fields,
        )

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
        schema = interop.schema([field.to_arrow_field() for field in self.fields])
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
    fields: tuple[FieldSpec, ...]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class NestedFieldSpec:
    """Nested field specification with a builder hook."""

    name: str
    dtype: DataTypeLike
    builder: Callable[..., interop.ArrayLike]


def file_identity_bundle(*, include_sha256: bool = True) -> FieldBundle:
    """Return a bundle for file identity columns.

    Parameters
    ----------
    include_sha256
        Include the file_sha256 field. Default True.

    Returns
    -------
    FieldBundle
        Bundle containing file identity fields.

    See Also
    --------
    schema_spec.file_identity : Canonical file identity constants and helpers.
    """
    from schema_spec.file_identity import file_identity_field_specs

    return FieldBundle(
        name="file_identity",
        fields=file_identity_field_specs(include_sha256=include_sha256),
    )


def span_bundle() -> FieldBundle:
    """Return a bundle for byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing byte-span fields (bstart, bend).

    See Also
    --------
    prefixed_span_bundle : Create a span bundle with a custom prefix.
    schema_spec.span_fields : Low-level span field generators.
    """
    from schema_spec.span_fields import make_span_field_specs as _span_fields

    return FieldBundle(name="span", fields=_span_fields(""))


def call_span_bundle() -> FieldBundle:
    """Return a bundle for callsite byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing callsite byte-span fields (call_bstart, call_bend).

    See Also
    --------
    prefixed_span_bundle : Create a span bundle with a custom prefix.
    """
    return prefixed_span_bundle("call_")


def name_span_bundle() -> FieldBundle:
    """Return a bundle for name byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing name span fields (name_bstart, name_bend).
    """
    return prefixed_span_bundle("name_")


def def_span_bundle() -> FieldBundle:
    """Return a bundle for definition byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing definition span fields (def_bstart, def_bend).
    """
    return prefixed_span_bundle("def_")


def stmt_span_bundle() -> FieldBundle:
    """Return a bundle for statement byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing statement span fields (stmt_bstart, stmt_bend).
    """
    return prefixed_span_bundle("stmt_")


def alias_span_bundle() -> FieldBundle:
    """Return a bundle for alias byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing alias span fields (alias_bstart, alias_bend).
    """
    return prefixed_span_bundle("alias_")


def prefixed_span_bundle(prefix: str) -> FieldBundle:
    """Return a bundle for byte-span columns with a given prefix.

    Parameters
    ----------
    prefix
        Span prefix (e.g., "call_", "def_", "name_").

    Returns
    -------
    FieldBundle
        Bundle containing prefixed byte-span fields.

    Examples
    --------
    >>> bundle = prefixed_span_bundle("call_")
    >>> [f.name for f in bundle.fields]
    ['call_bstart', 'call_bend']
    """
    from schema_spec.span_fields import SPAN_PREFIXES, STANDARD_SPAN_TYPES, SpanPrefix
    from schema_spec.span_fields import make_span_field_specs as _span_fields

    if prefix in SPAN_PREFIXES:
        span_prefix = cast("SpanPrefix", prefix)
        fields = _span_fields(span_prefix)
        bundle_name = STANDARD_SPAN_TYPES[span_prefix]
        return FieldBundle(name=bundle_name, fields=fields)

    normalized = prefix
    if prefix and not prefix.endswith("_"):
        normalized = f"{prefix}_"
    fields = (
        FieldSpec(name=f"{normalized}bstart", dtype=interop.int64()),
        FieldSpec(name=f"{normalized}bend", dtype=interop.int64()),
    )
    return FieldBundle(name=f"{prefix}span", fields=fields)


def scip_range_bundle(*, prefix: str = "", include_len: bool = False) -> FieldBundle:
    """Return a bundle for SCIP line/character range columns.

    Returns
    -------
    FieldBundle
        Bundle containing SCIP range fields.
    """
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    fields = [
        FieldSpec(name=f"{normalized}start_line", dtype=interop.int32()),
        FieldSpec(name=f"{normalized}start_char", dtype=interop.int32()),
        FieldSpec(name=f"{normalized}end_line", dtype=interop.int32()),
        FieldSpec(name=f"{normalized}end_char", dtype=interop.int32()),
    ]
    if include_len:
        fields.append(FieldSpec(name=f"{normalized}range_len", dtype=interop.int32()))
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
            FieldSpec(name=PROVENANCE_COLS[0], dtype=interop.string()),
            FieldSpec(name=PROVENANCE_COLS[1], dtype=interop.int32()),
            FieldSpec(name=PROVENANCE_COLS[2], dtype=interop.int32()),
            FieldSpec(name=PROVENANCE_COLS[3], dtype=interop.bool_()),
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
    "DerivedFieldSpec",
    "FieldBundle",
    "FieldSpec",
    "NestedFieldSpec",
    "TableSchemaSpec",
    "alias_span_bundle",
    "call_span_bundle",
    "def_span_bundle",
    "dict_field",
    "file_identity_bundle",
    "list_view_type",
    "name_span_bundle",
    "prefixed_span_bundle",
    "provenance_bundle",
    "schema_metadata",
    "schema_metadata_for_spec",
    "scip_range_bundle",
    "span_bundle",
    "stmt_span_bundle",
]
