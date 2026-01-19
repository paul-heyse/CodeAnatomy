"""Schema alignment, encoding, and empty-table helpers."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, TypedDict, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.expr_core import cast_expr, or_exprs
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    FieldLike,
    ScalarLike,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.chunking import ChunkPolicy
from arrowdsl.schema.encoding_policy import EncodingPolicy, EncodingSpec, apply_encoding
from arrowdsl.schema.normalize import NormalizePolicy

type CastErrorPolicy = Literal["unsafe", "keep", "raise"]


class _StructType(Protocol):
    def __iter__(self) -> Iterator[FieldLike]: ...


class _ListType(Protocol):
    value_field: FieldLike


class _MapType(Protocol):
    key_field: FieldLike
    item_field: FieldLike
    keys_sorted: bool


class _SchemaFieldSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def nullable(self) -> bool: ...


class _TableSchemaSpec(Protocol):
    @property
    def required_non_null(self) -> Sequence[str]: ...

    @property
    def fields(self) -> Sequence[_SchemaFieldSpec]: ...


class AlignmentInfo(TypedDict):
    """Alignment metadata for schema casting and column selection."""

    input_cols: list[str]
    input_rows: int
    missing_cols: list[str]
    dropped_cols: list[str]
    casted_cols: list[str]
    output_rows: int


def _is_nested_type(dtype: DataTypeLike) -> bool:
    return (
        patypes.is_struct(dtype)
        or patypes.is_list(dtype)
        or patypes.is_large_list(dtype)
        or patypes.is_map(dtype)
    )


def _prefer_base_nested(base: SchemaLike, unified: SchemaLike) -> SchemaLike:
    base_fields = {schema_field.name: schema_field for schema_field in base}
    fields: list[FieldLike] = []
    for schema_field in unified:
        base_field = base_fields.get(schema_field.name)
        if base_field is None:
            fields.append(schema_field)
            continue
        if _is_nested_type(base_field.type) and _is_nested_type(schema_field.type):
            updated = pa.field(
                base_field.name,
                base_field.type,
                nullable=schema_field.nullable,
                metadata=base_field.metadata,
            )
            fields.append(updated)
            continue
        fields.append(schema_field)
    return pa.schema(fields, metadata=unified.metadata)


def _cast_column(
    col: ArrayLike,
    field: FieldLike,
    *,
    safe_cast: bool,
    on_error: CastErrorPolicy,
) -> tuple[ArrayLike, bool]:
    """Cast a column to a field type, returning the casted flag.

    Returns
    -------
    tuple[ArrayLike, bool]
        Casted column and whether a cast occurred.

    Raises
    ------
    ArrowInvalid
        Raised when casting fails and ``on_error`` is ``"raise"``.
    ArrowTypeError
        Raised when casting fails and ``on_error`` is ``"raise"``.
    """
    if col.type == field.type:
        return col, False
    try:
        return pc.cast(col, field.type, safe=safe_cast), True
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        if on_error == "unsafe":
            return pc.cast(col, field.type, safe=False), True
        if on_error == "keep":
            return col, False
        raise


def align_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool,
    on_error: CastErrorPolicy = "unsafe",
    keep_extra_columns: bool = False,
) -> tuple[TableLike, AlignmentInfo]:
    """Align and cast a table to a target schema.

    Returns
    -------
    tuple[TableLike, AlignmentInfo]
        Aligned table and alignment metadata.
    """
    info: AlignmentInfo = {
        "input_cols": list(table.column_names),
        "input_rows": int(table.num_rows),
        "missing_cols": [],
        "dropped_cols": [],
        "casted_cols": [],
        "output_rows": 0,
    }

    target_names = [field.name for field in schema]
    missing = [name for name in target_names if name not in table.column_names]
    extra = [name for name in table.column_names if name not in target_names]

    arrays: list[ArrayLike] = []
    for schema_field in schema:
        if schema_field.name in table.column_names:
            col, casted = _cast_column(
                table[schema_field.name],
                schema_field,
                safe_cast=safe_cast,
                on_error=on_error,
            )
            if casted:
                info["casted_cols"].append(schema_field.name)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(table.num_rows, type=schema_field.type))

    aligned = pa.Table.from_arrays(arrays, schema=schema)
    info["missing_cols"] = missing
    info["dropped_cols"] = extra
    info["output_rows"] = int(aligned.num_rows)

    if keep_extra_columns:
        for name in table.column_names:
            if name not in aligned.column_names:
                aligned = aligned.append_column(name, table[name])

    return aligned, info


def align_table(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool = True,
    keep_extra_columns: bool = False,
    on_error: CastErrorPolicy = "unsafe",
) -> TableLike:
    """Return a table aligned to the target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    aligned, _ = align_to_schema(
        table,
        schema=schema,
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra_columns,
        on_error=on_error,
    )
    return aligned


@dataclass(frozen=True)
class SchemaTransform:
    """Schema alignment transform using shared alignment utilities."""

    schema: SchemaLike
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: CastErrorPolicy = "unsafe"

    def apply(self, table: TableLike) -> TableLike:
        """Align a table to the stored schema.

        Returns
        -------
        TableLike
            Aligned table.
        """
        aligned, _ = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned

    def apply_with_info(self, table: TableLike) -> tuple[TableLike, AlignmentInfo]:
        """Align a table to the stored schema and return alignment info.

        Returns
        -------
        tuple[TableLike, AlignmentInfo]
            Aligned table and alignment metadata.
        """
        aligned, info = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned, info


@dataclass(frozen=True)
class SchemaMetadataSpec:
    """Schema metadata mutation policy."""

    schema_metadata: dict[bytes, bytes] = field(default_factory=dict)
    field_metadata: dict[str, dict[bytes, bytes]] = field(default_factory=dict)

    @staticmethod
    def _split_metadata(
        metadata: dict[str, dict[bytes, bytes]],
    ) -> tuple[dict[str, dict[bytes, bytes]], dict[str, dict[bytes, bytes]]]:
        top_level: dict[str, dict[bytes, bytes]] = {}
        nested: dict[str, dict[bytes, bytes]] = {}
        for name, meta in metadata.items():
            if "." in name:
                nested[name] = meta
            else:
                top_level[name] = meta
        return top_level, nested

    @staticmethod
    def _apply_struct_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
    ) -> FieldLike:
        children: list[FieldLike] = []
        struct_type = cast("_StructType", field.type)
        for child in struct_type:
            child_prefix = f"{prefix}.{child.name}"
            updated_child = SchemaMetadataSpec._apply_nested_metadata(
                child,
                nested,
                child_prefix,
            )
            child_meta = nested.get(child_prefix)
            if child_meta:
                merged = dict(updated_child.metadata or {})
                merged.update(child_meta)
                updated_child = updated_child.with_metadata(merged)
            children.append(updated_child)
        return pa.field(
            field.name,
            pa.struct(children),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    @staticmethod
    def _apply_list_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
        list_factory: Callable[[FieldLike], DataTypeLike],
    ) -> FieldLike:
        list_type = cast("_ListType", field.type)
        value_field = list_type.value_field
        child_prefix = f"{prefix}.{value_field.name}"
        updated_child = SchemaMetadataSpec._apply_nested_metadata(
            value_field,
            nested,
            child_prefix,
        )
        child_meta = nested.get(child_prefix)
        if child_meta:
            merged = dict(updated_child.metadata or {})
            merged.update(child_meta)
            updated_child = updated_child.with_metadata(merged)
        new_type = list_factory(updated_child)
        return pa.field(field.name, new_type, nullable=field.nullable, metadata=field.metadata)

    @staticmethod
    def _apply_map_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
    ) -> FieldLike:
        map_type = cast("_MapType", field.type)
        key_field = map_type.key_field
        item_field = map_type.item_field

        key_prefix = f"{prefix}.{key_field.name}"
        updated_key = SchemaMetadataSpec._apply_nested_metadata(
            key_field,
            nested,
            key_prefix,
        )
        key_meta = nested.get(key_prefix)
        if key_meta:
            merged = dict(updated_key.metadata or {})
            merged.update(key_meta)
            updated_key = updated_key.with_metadata(merged)
        if updated_key.nullable:
            updated_key = pa.field(
                updated_key.name,
                updated_key.type,
                nullable=False,
                metadata=updated_key.metadata,
            )

        item_prefix = f"{prefix}.{item_field.name}"
        updated_item = SchemaMetadataSpec._apply_nested_metadata(
            item_field,
            nested,
            item_prefix,
        )
        item_meta = nested.get(item_prefix)
        if item_meta:
            merged = dict(updated_item.metadata or {})
            merged.update(item_meta)
            updated_item = updated_item.with_metadata(merged)

        new_type = pa.map_(updated_key, updated_item, keys_sorted=map_type.keys_sorted)
        return pa.field(field.name, new_type, nullable=field.nullable, metadata=field.metadata)

    @staticmethod
    def _apply_nested_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
    ) -> FieldLike:
        if not nested:
            return field

        if patypes.is_struct(field.type):
            return SchemaMetadataSpec._apply_struct_metadata(field, nested, prefix)

        list_factories: list[
            tuple[Callable[[DataTypeLike], bool], Callable[[FieldLike], DataTypeLike]]
        ] = [
            (patypes.is_list, pa.list_),
            (patypes.is_large_list, pa.large_list),
            (patypes.is_list_view, pa.list_view),
            (patypes.is_large_list_view, pa.large_list_view),
        ]
        for predicate, factory in list_factories:
            if predicate(field.type):
                return SchemaMetadataSpec._apply_list_metadata(field, nested, prefix, factory)

        if patypes.is_map(field.type):
            return SchemaMetadataSpec._apply_map_metadata(field, nested, prefix)

        return field

    def apply(self, schema: SchemaLike) -> SchemaLike:
        """Return a schema with metadata updates applied.

        Returns
        -------
        SchemaLike
            Updated schema with metadata applied.
        """
        top_level, nested = self._split_metadata(self.field_metadata)
        fields: list[FieldLike] = []
        for schema_field in schema:
            meta = top_level.get(schema_field.name)
            if meta is None:
                updated = schema_field
            else:
                merged = dict(schema_field.metadata or {})
                merged.update(meta)
                updated = schema_field.with_metadata(merged)
            updated = self._apply_nested_metadata(updated, nested, updated.name)
            fields.append(updated)

        updated = pa.schema(fields)
        if self.schema_metadata:
            meta = dict(schema.metadata or {})
            meta.update(self.schema_metadata)
            return updated.with_metadata(meta)
        if schema.metadata is None:
            return updated
        return updated.with_metadata(schema.metadata)


@dataclass(frozen=True)
class SchemaEvolutionSpec:
    """Unify/cast/concat policy for evolving schemas."""

    promote_options: str = "permissive"

    def unify_schema(self, tables: Sequence[TableLike]) -> SchemaLike:
        """Return a unified schema for the provided tables.

        Returns
        -------
        SchemaLike
            Unified schema for the tables.
        """
        schemas = [table.schema for table in tables]
        return unify_schemas_core(schemas, promote_options=self.promote_options)

    def unify_schema_from_schemas(self, schemas: Sequence[SchemaLike]) -> SchemaLike:
        """Return a unified schema for a sequence of schemas.

        Returns
        -------
        SchemaLike
            Unified schema for the schemas.
        """
        return unify_schemas_core(schemas, promote_options=self.promote_options)

    def unify_and_cast(
        self,
        tables: Sequence[TableLike],
        *,
        safe_cast: bool = True,
        on_error: CastErrorPolicy = "unsafe",
        keep_extra_columns: bool = False,
    ) -> TableLike:
        """Unify schemas, align tables, and concatenate.

        Returns
        -------
        TableLike
            Concatenated table with unified schema.
        """
        if not tables:
            return pa.Table.from_arrays([], schema=pa.schema([]))
        schema = self.unify_schema(tables)
        aligned: list[TableLike] = []
        for table in tables:
            aligned_table, _ = align_to_schema(
                table,
                schema=schema,
                safe_cast=safe_cast,
                on_error=on_error,
                keep_extra_columns=keep_extra_columns,
            )
            aligned.append(aligned_table)
        return pa.concat_tables(aligned, promote_options=self.promote_options)


@dataclass(frozen=True)
class EncodingPolicyWithChunks:
    """Encoding policy plus chunk normalization for schema alignment."""

    policy: EncodingPolicy
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)

    def apply(self, table: TableLike) -> TableLike:
        """Apply encoding policy and chunk policy.

        Returns
        -------
        TableLike
            Table with encoding and chunk policy applied.
        """
        return NormalizePolicy(encoding=self.policy, chunk=self.chunk_policy).apply(table)


def encode_expression(column: str) -> ComputeExpression:
    """Return a compute expression for dictionary encoding a column.

    Returns
    -------
    ComputeExpression
        Expression applying dictionary encoding.
    """
    expr = pc.field(column)
    encoded = pc.dictionary_encode(cast("ArrayLike", expr))
    return ensure_expression(encoded)


def projection_for_schema(
    schema: SchemaLike,
    *,
    available: Sequence[str] | None = None,
    safe_cast: bool = True,
) -> tuple[list[ComputeExpression], list[str]]:
    """Return projection expressions to align with a schema.

    Returns
    -------
    tuple[list[ComputeExpression], list[str]]
        Expressions and names aligned to the schema.
    """
    available_set = set(available or schema.names)
    expressions: list[ComputeExpression] = []
    names: list[str] = []
    for schema_field in schema:
        if patypes.is_list_view(schema_field.type) or patypes.is_large_list_view(schema_field.type):
            if schema_field.name in available_set:
                expr = ensure_expression(pc.field(schema_field.name))
            else:
                expr = ensure_expression(pc.scalar(pa.scalar(None, type=schema_field.type)))
        elif schema_field.name in available_set:
            expr = cast_expr(pc.field(schema_field.name), schema_field.type, safe=safe_cast)
        else:
            expr = cast_expr(pc.scalar(None), schema_field.type, safe=safe_cast)
        expressions.append(ensure_expression(expr))
        names.append(schema_field.name)
    return expressions, names


def encoding_projection(
    columns: Sequence[str],
    *,
    available: Sequence[str],
) -> tuple[list[ComputeExpression], list[str]]:
    """Return projection expressions to apply dictionary encoding.

    Returns
    -------
    tuple[list[ComputeExpression], list[str]]
        Expressions and column names for encoding projection.
    """
    encode_set = set(columns)
    expressions: list[ComputeExpression] = []
    names: list[str] = []
    for name in available:
        expr = encode_expression(name) if name in encode_set else pc.field(name)
        expressions.append(expr)
        names.append(name)
    return expressions, names


def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    """Return columns marked for dictionary encoding via field metadata.

    Returns
    -------
    list[str]
        Column names marked for dictionary encoding.
    """
    encoding_columns: list[str] = []
    for schema_field in schema:
        meta = schema_field.metadata or {}
        if meta.get(b"encoding") == b"dictionary":
            encoding_columns.append(schema_field.name)
    return encoding_columns


def encode_table(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified columns on a table.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    policy = EncodingPolicy(dictionary_cols=frozenset(columns))
    return apply_encoding(table, policy=policy)


def best_fit_type(array: ArrayLike, candidates: Sequence[DataTypeLike]) -> DataTypeLike:
    """Return the most specific candidate type that preserves validity.

    Returns
    -------
    pyarrow.DataType
        Best-fit type for the array.
    """
    total_rows = len(array)
    for dtype in candidates:
        casted = pc.cast(array, dtype, safe=False)
        valid = pc.is_valid(casted)
        total = pc.call_function("sum", [pc.cast(valid, pa.int64())])
        value = cast("int | float | bool | None", cast("ScalarLike", total).as_py())
        if value is None:
            continue
        count = int(value)
        if count == total_rows:
            return dtype
    return array.type


def unify_schemas(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
    prefer_nested: bool = True,
) -> SchemaLike:
    """Unify schemas while preserving metadata from the first schema.

    Returns
    -------
    SchemaLike
        Unified schema with metadata preserved.
    """
    if not schemas:
        return pa.schema([])
    unified = unify_schemas_core(schemas, promote_options=promote_options)
    if prefer_nested:
        unified = _prefer_base_nested(schemas[0], unified)
    metadata_module = importlib.import_module("arrowdsl.schema.metadata")
    return metadata_module.metadata_spec_from_schema(schemas[0]).apply(unified)


def unify_schema_with_metadata(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    """Unify schemas while preserving metadata from the first schema.

    Returns
    -------
    SchemaLike
        Unified schema with metadata preserved.
    """
    return unify_schemas(schemas, promote_options=promote_options)


def unify_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
) -> TableLike:
    """Unify and concatenate tables with metadata-aware schema alignment.

    Returns
    -------
    TableLike
        Concatenated table aligned to the unified schema.
    """
    if not tables:
        return pa.Table.from_arrays([], names=[])
    schema = unify_schemas([table.schema for table in tables], promote_options=promote_options)
    aligned: list[TableLike] = []
    for table in tables:
        aligned_table, _ = align_to_schema(table, schema=schema, safe_cast=True)
        aligned.append(aligned_table)
    combined = pa.concat_tables(aligned, promote_options="default")
    return ChunkPolicy().apply(combined)


def infer_schema_from_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
    prefer_nested: bool = True,
) -> SchemaLike:
    """Infer a unified schema from tables using Arrow evolution rules.

    Returns
    -------
    SchemaLike
        Unified schema for the input tables.
    """
    schemas = [table.schema for table in tables if table is not None]
    return unify_schemas(
        schemas,
        promote_options=promote_options,
        prefer_nested=prefer_nested,
    )


def required_field_names(spec: _TableSchemaSpec) -> tuple[str, ...]:
    """Return required field names (explicit or non-nullable).

    Returns
    -------
    tuple[str, ...]
        Required field names.
    """
    required = set(spec.required_non_null)
    return tuple(
        field.name for field in spec.fields if field.name in required or not field.nullable
    )


def required_non_null_mask(
    required: Sequence[str],
    *,
    available: set[str],
) -> ComputeExpression:
    """Return a plan-lane mask for required non-null violations.

    Returns
    -------
    ComputeExpression
        Boolean expression for invalid rows.
    """
    exprs = [
        ensure_expression(pc.invert(pc.is_valid(pc.field(name))))
        for name in required
        if name in available
    ]
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(value=False)))
    return or_exprs(exprs)


def missing_key_fields(keys: Sequence[str], *, missing_cols: Sequence[str]) -> tuple[str, ...]:
    """Return key fields missing from the available columns.

    Returns
    -------
    tuple[str, ...]
        Missing key field names.
    """
    missing = set(missing_cols)
    return tuple(key for key in keys if key in missing)


def unify_schemas_core(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    """Return a unified schema using Arrow evolution rules.

    Returns
    -------
    SchemaLike
        Unified schema derived from the input schemas.
    """
    if not schemas:
        return pa.schema([])
    try:
        return pa.unify_schemas(list(schemas), promote_options=promote_options)
    except TypeError:
        return pa.unify_schemas(list(schemas))


def register_extension_types(types: Sequence[pa.ExtensionType]) -> None:
    """Register extension types with pyarrow.

    Parameters
    ----------
    types
        Sequence of extension types to register.
    """
    for extension_type in types:
        try:
            pa.register_extension_type(extension_type)
        except (ValueError, pa.ArrowKeyError):
            continue


def extension_types_from_schema(schema: SchemaLike) -> tuple[pa.ExtensionType, ...]:
    """Collect extension types from a schema.

    Returns
    -------
    tuple[pa.ExtensionType, ...]
        Extension types referenced by the schema.
    """
    collected: list[pa.ExtensionType] = []

    def _collect(data_type: pa.DataType) -> None:
        if isinstance(data_type, pa.ExtensionType):
            collected.append(cast("pa.ExtensionType", data_type))
            return
        if pa.types.is_struct(data_type):
            for field in data_type:
                _collect(field.type)
            return
        if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            _collect(data_type.value_type)
            return
        if pa.types.is_map(data_type):
            _collect(data_type.key_type)
            _collect(data_type.item_type)

    for schema_field in schema:
        _collect(schema_field.type)
    return tuple(collected)


def register_schema_extensions(schema: SchemaLike) -> None:
    """Register extension types referenced by a schema."""
    extensions = extension_types_from_schema(schema)
    if extensions:
        register_extension_types(extensions)

def schema_fingerprint(schema: SchemaLike) -> str:
    """Return a stable fingerprint for the provided schema.

    Returns
    -------
    str
        Schema fingerprint hash.
    """
    module = importlib.import_module("arrowdsl.schema.serialization")
    return cast("str", module.schema_fingerprint(schema))


def schema_to_dict(schema: SchemaLike) -> dict[str, object]:
    """Return a JSON-ready schema payload.

    Returns
    -------
    dict[str, object]
        Schema payload dictionary.
    """
    module = importlib.import_module("arrowdsl.schema.serialization")
    return cast("dict[str, object]", module.schema_to_dict(schema))


__all__ = [
    "AlignmentInfo",
    "CastErrorPolicy",
    "EncodingPolicy",
    "EncodingPolicyWithChunks",
    "EncodingSpec",
    "SchemaEvolutionSpec",
    "SchemaMetadataSpec",
    "SchemaTransform",
    "align_table",
    "align_to_schema",
    "best_fit_type",
    "empty_table",
    "encode_expression",
    "encode_table",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "extension_types_from_schema",
    "infer_schema_from_tables",
    "missing_key_fields",
    "projection_for_schema",
    "register_extension_types",
    "register_schema_extensions",
    "required_field_names",
    "required_non_null_mask",
    "schema_fingerprint",
    "schema_to_dict",
    "unify_schema_with_metadata",
    "unify_schemas",
    "unify_schemas_core",
    "unify_tables",
]
