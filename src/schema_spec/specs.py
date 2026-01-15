"""Schema specification models and shared field bundles."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Literal, cast

import ibis
import pyarrow as pa
from sqlglot import exp

from arrowdsl.compute.expr_core import ExprSpec
from arrowdsl.core import interop
from arrowdsl.core.interop import DataTypeLike, FieldLike, SchemaLike
from arrowdsl.core.schema_constants import (
    KEY_FIELDS_META,
    PROVENANCE_COLS,
    PROVENANCE_SOURCE_FIELDS,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from arrowdsl.schema.build import list_view_type
from arrowdsl.schema.metadata import ENCODING_DICTIONARY, ENCODING_META, dict_field_metadata
from arrowdsl.schema.schema import CastErrorPolicy, SchemaMetadataSpec, SchemaTransform

DICT_STRING = interop.dictionary(interop.int32(), interop.string())


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


def _ensure_arrow_dtype(dtype: DataTypeLike) -> pa.DataType:
    if isinstance(dtype, pa.DataType):
        return dtype
    msg = f"Expected pyarrow.DataType, got {type(dtype)!r}."
    raise TypeError(msg)


_SIMPLE_IBIS_TYPES: tuple[tuple[Callable[[pa.DataType], bool], str], ...] = (
    (pa.types.is_boolean, "boolean"),
    (pa.types.is_int8, "int8"),
    (pa.types.is_int16, "int16"),
    (pa.types.is_int32, "int32"),
    (pa.types.is_int64, "int64"),
    (pa.types.is_uint8, "uint8"),
    (pa.types.is_uint16, "uint16"),
    (pa.types.is_uint32, "uint32"),
    (pa.types.is_uint64, "uint64"),
    (pa.types.is_float16, "float16"),
    (pa.types.is_float32, "float32"),
    (pa.types.is_float64, "float64"),
    (lambda dt: pa.types.is_string(dt) or pa.types.is_large_string(dt), "string"),
    (lambda dt: pa.types.is_binary(dt) or pa.types.is_large_binary(dt), "binary"),
    (lambda dt: pa.types.is_date32(dt) or pa.types.is_date64(dt), "date"),
)


def _arrow_dtype_to_ibis(dtype: DataTypeLike) -> str:
    arrow_dtype = _ensure_arrow_dtype(dtype)
    for check, name in _SIMPLE_IBIS_TYPES:
        if check(arrow_dtype):
            return name
    if pa.types.is_timestamp(arrow_dtype):
        return _timestamp_to_ibis(cast("pa.TimestampType", arrow_dtype))
    if pa.types.is_list(arrow_dtype) or pa.types.is_large_list(arrow_dtype):
        list_dtype = cast("pa.ListType | pa.LargeListType", arrow_dtype)
        return _list_to_ibis(list_dtype)
    if pa.types.is_struct(arrow_dtype):
        return _struct_to_ibis(cast("pa.StructType", arrow_dtype))
    if pa.types.is_dictionary(arrow_dtype):
        dict_dtype = cast("pa.DictionaryType", arrow_dtype)
        return _arrow_dtype_to_ibis(dict_dtype.value_type)
    msg = f"Unsupported Arrow dtype for Ibis schema: {arrow_dtype}"
    raise ValueError(msg)


def _timestamp_to_ibis(dtype: pa.TimestampType) -> str:
    unit = dtype.unit
    tz = f", tz='{dtype.tz}'" if dtype.tz else ""
    return f"timestamp('{unit}'{tz})"


def _list_to_ibis(dtype: pa.ListType | pa.LargeListType) -> str:
    return f"array<{_arrow_dtype_to_ibis(dtype.value_type)}>"


def _struct_to_ibis(dtype: pa.StructType) -> str:
    fields = ", ".join(f"{field.name}: {_arrow_dtype_to_ibis(field.type)}" for field in dtype)
    return f"struct<{fields}>"


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _external_table_options_clause(options: Mapping[str, object] | None) -> str | None:
    if not options:
        return None
    formatted: list[str] = []
    for key, value in options.items():
        if key in {"schema", "table_name"} or value is None:
            continue
        rendered = _option_literal(value)
        if rendered is None:
            continue
        formatted.append(f"{_sql_literal(str(key))} {rendered}")
    if not formatted:
        return None
    return f"OPTIONS ({', '.join(formatted)})"


def _option_literal(value: object) -> str | None:
    if isinstance(value, bool):
        return _sql_literal("true" if value else "false")
    if isinstance(value, (int, float)):
        return _sql_literal(str(value))
    if isinstance(value, str):
        return _sql_literal(value)
    return None


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
        return interop.field(self.name, self.dtype, nullable=self.nullable, metadata=metadata)


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
    idx_type = index_type or interop.int32()
    meta = dict_field_metadata(index_type=idx_type, ordered=ordered, metadata=metadata)
    dict_factory = cast("Callable[..., object]", interop.dictionary)
    dtype = cast("DataTypeLike", dict_factory(idx_type, interop.string(), ordered=ordered))
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
class ExternalTableConfig:
    """Configuration for CREATE EXTERNAL TABLE statements."""

    location: str
    file_format: str
    table_name: str | None = None
    dialect: str | None = None
    options: Mapping[str, object] | None = None
    partitioned_by: Sequence[str] | None = None
    compression: str | None = None


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
        schema = interop.schema([field.to_arrow_field() for field in self.fields])
        meta = schema_metadata_for_spec(self)
        return SchemaMetadataSpec(schema_metadata=meta).apply(schema)

    def to_ibis_schema(self) -> ibis.Schema:
        """Build an Ibis schema for SQLGlot/DDL tooling.

        Returns
        -------
        ibis.Schema
            Ibis schema derived from the Arrow field specs.
        """
        return ibis.schema({field.name: _arrow_dtype_to_ibis(field.dtype) for field in self.fields})

    def to_sqlglot_column_defs(self, *, dialect: str | None = None) -> list[exp.ColumnDef]:
        """Return SQLGlot ColumnDef nodes for the schema.

        Returns
        -------
        list[sqlglot.expressions.ColumnDef]
            SQLGlot ColumnDef nodes derived from the Ibis schema.
        """
        dialect_name = dialect or "ansi"
        return self.to_ibis_schema().to_sqlglot_column_defs(dialect=dialect_name)

    def to_create_table_sql(
        self,
        *,
        dialect: str | None = None,
        table_name: str | None = None,
    ) -> str:
        """Return a CREATE TABLE statement for the schema.

        Returns
        -------
        str
            CREATE TABLE statement using SQLGlot-rendered column definitions.
        """
        name = table_name or self.name
        dialect_name = dialect or "ansi"
        column_defs = self.to_sqlglot_column_defs(dialect=dialect_name)
        columns_sql = ", ".join(col.sql(dialect=dialect_name) for col in column_defs)
        return f"CREATE TABLE {name} ({columns_sql})"

    def to_create_external_table_sql(self, config: ExternalTableConfig) -> str:
        """Return a CREATE EXTERNAL TABLE statement for the schema.

        Returns
        -------
        str
            CREATE EXTERNAL TABLE statement using SQLGlot-rendered column definitions.
        """
        name = config.table_name or self.name
        dialect_name = config.dialect or "datafusion"
        column_defs = self.to_sqlglot_column_defs(dialect=dialect_name)
        columns_sql = ", ".join(col.sql(dialect=dialect_name) for col in column_defs)
        parts = [f"CREATE EXTERNAL TABLE {name} ({columns_sql})"]
        parts.append(f"STORED AS {config.file_format.upper()}")
        if config.compression:
            parts.append(f"COMPRESSION TYPE {config.compression}")
        parts.append(f"LOCATION {_sql_literal(config.location)}")
        if config.partitioned_by:
            parts.append(f"PARTITIONED BY ({', '.join(config.partitioned_by)})")
        options_clause = _external_table_options_clause(config.options)
        if options_clause:
            parts.append(options_clause)
        return "\n".join(parts)

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
    builder: Callable[..., interop.ArrayLike]


def file_identity_bundle(*, include_sha256: bool = True) -> FieldBundle:
    """Return a bundle for file identity columns.

    Returns
    -------
    FieldBundle
        Bundle containing file identity fields.
    """
    fields = [
        ArrowFieldSpec(name="file_id", dtype=interop.string()),
        ArrowFieldSpec(name="path", dtype=interop.string()),
    ]
    if include_sha256:
        fields.append(ArrowFieldSpec(name="file_sha256", dtype=interop.string()))
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
            ArrowFieldSpec(name="bstart", dtype=interop.int64()),
            ArrowFieldSpec(name="bend", dtype=interop.int64()),
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
            ArrowFieldSpec(name="call_bstart", dtype=interop.int64()),
            ArrowFieldSpec(name="call_bend", dtype=interop.int64()),
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
        ArrowFieldSpec(name=f"{normalized}start_line", dtype=interop.int32()),
        ArrowFieldSpec(name=f"{normalized}start_char", dtype=interop.int32()),
        ArrowFieldSpec(name=f"{normalized}end_line", dtype=interop.int32()),
        ArrowFieldSpec(name=f"{normalized}end_char", dtype=interop.int32()),
    ]
    if include_len:
        fields.append(ArrowFieldSpec(name=f"{normalized}range_len", dtype=interop.int32()))
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
            ArrowFieldSpec(name=PROVENANCE_COLS[0], dtype=interop.string()),
            ArrowFieldSpec(name=PROVENANCE_COLS[1], dtype=interop.int32()),
            ArrowFieldSpec(name=PROVENANCE_COLS[2], dtype=interop.int32()),
            ArrowFieldSpec(name=PROVENANCE_COLS[3], dtype=interop.bool_()),
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
