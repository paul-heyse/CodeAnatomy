"""Schema specification models and shared field bundles."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast

import ibis
import pyarrow as pa

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
from arrowdsl.io.ipc import payload_hash
from arrowdsl.schema.build import list_view_type
from arrowdsl.schema.encoding_metadata import (
    ENCODING_DICTIONARY,
    ENCODING_META,
    dict_field_metadata,
)
from arrowdsl.schema.metadata import metadata_list_bytes
from arrowdsl.schema.schema import CastErrorPolicy, SchemaMetadataSpec, SchemaTransform
from sqlglot_tools.compat import exp
from sqlglot_tools.optimizer import (
    ExternalTableCompressionProperty,
    ExternalTableOptionsProperty,
    ExternalTableOrderProperty,
    SqlGlotPolicy,
    normalize_ddl_sql,
    register_datafusion_dialect,
    resolve_sqlglot_policy,
    sqlglot_sql,
)

DICT_STRING = interop.dictionary(interop.int32(), interop.string())

DDL_FINGERPRINT_VERSION: int = 1
_DDL_FINGERPRINT_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("dialect", pa.string()),
        pa.field("columns", pa.list_(pa.string())),
    ]
)

if TYPE_CHECKING:
    from arrowdsl.spec.expr_ir import ExprIR


def _ddl_policy(dialect: str) -> SqlGlotPolicy:
    """Return a SQLGlot policy with the given dialect pinned.

    Notes
    -----
    Uses ``resolve_sqlglot_policy`` to honor centralized policy defaults.

    Returns
    -------
    SqlGlotPolicy
        SQLGlot policy configured for the dialect.
    """
    policy_name = "datafusion_ddl" if dialect in {"datafusion", "datafusion_ext"} else None
    policy = resolve_sqlglot_policy(name=policy_name)
    return replace(policy, read_dialect=dialect, write_dialect=dialect)


def _table_schema_expr(
    name: str,
    *,
    expressions: Sequence[exp.Expression],
) -> exp.Schema:
    """Return a SQLGlot schema expression for a table definition.

    Returns
    -------
    sqlglot.exp.Schema
        SQLGlot schema expression for a table.
    """
    return exp.Schema(
        this=exp.Table(this=exp.Identifier(this=name)),
        expressions=list(expressions),
    )


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
        meta[REQUIRED_NON_NULL_META] = metadata_list_bytes(spec.required_non_null)
    if spec.key_fields:
        meta[KEY_FIELDS_META] = metadata_list_bytes(spec.key_fields)
    return meta


def _field_metadata(metadata: dict[str, str]) -> dict[bytes, bytes]:
    """Encode metadata keys/values for Arrow.

    Returns
    -------
    dict[bytes, bytes]
        Encoded metadata mapping.
    """
    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in metadata.items()}


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


def _external_table_options_property(
    options: Mapping[str, object] | None,
) -> ExternalTableOptionsProperty | None:
    """Return the external table options property when options are provided.

    Returns
    -------
    ExternalTableOptionsProperty | None
        Options property when entries are available.
    """
    if not options:
        return None
    entries: list[exp.Expression] = []
    for key, value in options.items():
        if key in {"schema", "table_name"} or value is None:
            continue
        rendered = _option_literal_value(value)
        if rendered is None:
            continue
        entries.append(
            exp.Tuple(
                expressions=[
                    exp.Literal.string(str(key)),
                    exp.Literal.string(rendered),
                ]
            )
        )
    if not entries:
        return None
    return ExternalTableOptionsProperty(expressions=entries)


def _option_literal_value(value: object) -> str | None:
    """Return a string literal payload for external table options.

    Returns
    -------
    str | None
        Rendered value payload for external options.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return None


@dataclass(frozen=True)
class ArrowFieldSpec:
    """Specification for a single Arrow field."""

    name: str
    dtype: DataTypeLike
    nullable: bool = True
    metadata: dict[str, str] = field(default_factory=dict)
    default_value: str | None = None
    encoding: Literal["dictionary"] | None = None

    def to_arrow_field(self) -> FieldLike:
        """Build a pyarrow.Field from the spec.

        Returns
        -------
        pyarrow.Field
            Arrow field instance.
        """
        metadata = dict(self.metadata)
        if self.default_value is not None:
            metadata.setdefault("default_value", self.default_value)
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
    expr: ExprIR


@dataclass(frozen=True)
class ExternalTableConfig:
    """Configuration for CREATE EXTERNAL TABLE statements."""

    location: str
    file_format: str
    table_name: str | None = None
    dialect: str | None = None
    options: Mapping[str, object] | None = None
    partitioned_by: Sequence[str] | None = None
    file_sort_order: Sequence[str] | None = None
    compression: str | None = None
    unbounded: bool = False


@dataclass(frozen=True)
class ExternalTableConfigOverrides:
    """Optional overrides for ExternalTableConfig generation."""

    table_name: str | None = None
    dialect: str | None = None
    options: Mapping[str, object] | None = None
    partitioned_by: Sequence[str] | None = None
    file_sort_order: Sequence[str] | None = None
    compression: str | None = None
    unbounded: bool | None = None


@dataclass(frozen=True)
class TableSchemaSpec:
    """Specification for a table schema and associated constraints."""

    name: str
    fields: list[ArrowFieldSpec]
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
        from arrowdsl.schema.metadata import (
            schema_constraints_from_metadata,
            schema_identity_from_metadata,
        )

        fields: list[ArrowFieldSpec] = []
        for schema_field in schema:
            meta = _decode_metadata(schema_field.metadata)
            encoding = _encoding_hint_from_field(meta, dtype=schema_field.type)
            fields.append(
                ArrowFieldSpec(
                    name=schema_field.name,
                    dtype=schema_field.type,
                    nullable=schema_field.nullable,
                    metadata=meta,
                    encoding=encoding,
                    default_value=meta.get("default_value"),
                )
            )
        meta_name, meta_version = schema_identity_from_metadata(schema.metadata)
        required_non_null, key_fields = schema_constraints_from_metadata(schema.metadata)
        resolved_name = meta_name or name
        resolved_version = version if version is not None else meta_version
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
        dialect_name = dialect or "datafusion"
        if dialect_name in {"datafusion", "datafusion_ext"}:
            register_datafusion_dialect()
        column_defs = list(self.to_ibis_schema().to_sqlglot_column_defs(dialect=dialect_name))
        required = set(self.required_non_null) | set(self.key_fields)
        updated: list[exp.ColumnDef] = []
        for field_spec, column_def in zip(self.fields, column_defs, strict=True):
            constraints = list(column_def.args.get("constraints") or [])
            if field_spec.default_value is not None:
                default_expr = exp.Literal.string(field_spec.default_value)
                constraints.append(
                    exp.ColumnConstraint(kind=exp.DefaultColumnConstraint(this=default_expr))
                )
            if field_spec.name in required or not field_spec.nullable:
                constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
            if constraints:
                updated_column = column_def.copy()
                updated_column.set("constraints", constraints)
            else:
                updated_column = column_def
            updated.append(updated_column)
        return updated

    def ddl_fingerprint(self, *, dialect: str | None = None) -> str:
        """Return a DDL fingerprint derived from SQLGlot column definitions.

        Returns
        -------
        str
            Stable fingerprint for the schema DDL representation.
        """
        dialect_name = dialect or "datafusion"
        column_defs = self.to_sqlglot_column_defs(dialect=dialect_name)
        payload = {
            "version": DDL_FINGERPRINT_VERSION,
            "dialect": dialect_name,
            "columns": [column.sql(dialect=dialect_name) for column in column_defs],
        }
        return payload_hash(payload, _DDL_FINGERPRINT_SCHEMA)

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
        dialect_name = dialect or "datafusion"
        if dialect_name in {"datafusion", "datafusion_ext"}:
            register_datafusion_dialect()
        column_defs = self.to_sqlglot_column_defs(dialect=dialect_name)
        expressions: list[exp.Expression] = list(column_defs)
        if self.key_fields:
            expressions.append(
                exp.PrimaryKey(expressions=[exp.to_identifier(field) for field in self.key_fields])
            )
        schema_expr = _table_schema_expr(name, expressions=expressions)
        create_expr = exp.Create(this=schema_expr, kind="TABLE")
        ddl = sqlglot_sql(create_expr, policy=_ddl_policy(dialect_name))
        return normalize_ddl_sql(ddl)

    def to_create_external_table_sql(self, config: ExternalTableConfig) -> str:
        """Return a CREATE EXTERNAL TABLE statement for the schema.

        Returns
        -------
        str
            CREATE EXTERNAL TABLE statement using SQLGlot-rendered column definitions.
        """
        name = config.table_name or self.name
        dialect_name = config.dialect or "datafusion"
        if dialect_name in {"datafusion", "datafusion_ext"}:
            register_datafusion_dialect()
        column_defs = self.to_sqlglot_column_defs(dialect=dialect_name)
        expressions: list[exp.Expression] = list(column_defs)
        if self.key_fields:
            expressions.append(
                exp.PrimaryKey(expressions=[exp.to_identifier(field) for field in self.key_fields])
            )
        file_format = config.file_format
        stored_as = "DELTATABLE" if file_format.lower() == "delta" else file_format.upper()
        properties: list[exp.Expression] = [
            exp.FileFormatProperty(this=exp.Var(this=stored_as)),
            exp.LocationProperty(this=exp.Literal.string(config.location)),
        ]
        if config.compression:
            properties.append(
                ExternalTableCompressionProperty(this=exp.Var(this=config.compression))
            )
        if config.partitioned_by:
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Tuple(
                        expressions=[exp.Identifier(this=name) for name in config.partitioned_by]
                    )
                )
            )
        if config.file_sort_order:
            properties.append(
                ExternalTableOrderProperty(
                    expressions=[exp.Identifier(this=name) for name in config.file_sort_order]
                )
            )
        options_property = _external_table_options_property(config.options)
        if options_property is not None:
            properties.append(options_property)
        kind = "UNBOUNDED EXTERNAL" if config.unbounded else "EXTERNAL"
        schema_expr = _table_schema_expr(name, expressions=expressions)
        create_expr = exp.Create(
            this=schema_expr,
            kind=kind,
            properties=exp.Properties(expressions=properties),
        )
        ddl = sqlglot_sql(create_expr, policy=_ddl_policy(dialect_name))
        return normalize_ddl_sql(ddl)

    @staticmethod
    def external_table_config(
        *,
        location: str,
        file_format: str,
        overrides: ExternalTableConfigOverrides | None = None,
    ) -> ExternalTableConfig:
        """Return an ExternalTableConfig derived from this schema.

        Parameters
        ----------
        location:
            Dataset location for the external table.
        file_format:
            Storage format for the external table (e.g., parquet, csv).
        overrides:
            Optional overrides for table options and formatting.

        Returns
        -------
        ExternalTableConfig
            External table configuration for this schema.
        """
        resolved = overrides or ExternalTableConfigOverrides()
        return ExternalTableConfig(
            location=location,
            file_format=file_format,
            table_name=resolved.table_name,
            dialect=resolved.dialect,
            options=resolved.options,
            partitioned_by=resolved.partitioned_by,
            file_sort_order=resolved.file_sort_order,
            compression=resolved.compression,
            unbounded=bool(resolved.unbounded) if resolved.unbounded is not None else False,
        )

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
