"""Schema alignment, encoding, and empty-table helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, TypedDict, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.compute.macros import FieldExpr
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    FieldLike,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.schema.arrays import set_or_append_column
from arrowdsl.schema.builders import empty_table

type CastErrorPolicy = Literal["unsafe", "keep", "raise"]


class _StructType(Protocol):
    def __iter__(self) -> Iterator[FieldLike]: ...


class _ListType(Protocol):
    value_field: FieldLike


class _MapType(Protocol):
    key_field: FieldLike
    item_field: FieldLike
    keys_sorted: bool


class AlignmentInfo(TypedDict):
    """Alignment metadata for schema casting and column selection."""

    input_cols: list[str]
    input_rows: int
    missing_cols: list[str]
    dropped_cols: list[str]
    casted_cols: list[str]
    output_rows: int


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
        if not schemas:
            return pa.schema([])
        try:
            return pa.unify_schemas(list(schemas), promote_options=self.promote_options)
        except TypeError:
            return pa.unify_schemas(list(schemas))

    def unify_schema_from_schemas(self, schemas: Sequence[SchemaLike]) -> SchemaLike:
        """Return a unified schema for a sequence of schemas.

        Returns
        -------
        SchemaLike
            Unified schema for the schemas.
        """
        if not schemas:
            return pa.schema([])
        try:
            return pa.unify_schemas(list(schemas), promote_options=self.promote_options)
        except TypeError:
            return pa.unify_schemas(list(schemas))

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
        return pa.concat_tables(aligned, promote=True)


@dataclass(frozen=True)
class EncodingSpec:
    """Specification for dictionary encoding a column."""

    column: str
    dtype: DataTypeLike | None = None


@dataclass(frozen=True)
class EncodingPolicy:
    """Encoding policy bundling dictionary encodes and chunk normalization."""

    specs: tuple[EncodingSpec, ...] = ()
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)

    def apply(self, table: TableLike) -> TableLike:
        """Apply encoding specs and chunk policy.

        Returns
        -------
        TableLike
            Table with encoding and chunk policy applied.
        """
        out = encode_columns(table, specs=self.specs)
        return self.chunk_policy.apply(out)


def encode_dictionary(table: TableLike, spec: EncodingSpec) -> TableLike:
    """Dictionary-encode a column.

    Returns
    -------
    TableLike
        Table with the column dictionary-encoded when applicable.
    """
    if spec.column not in table.column_names:
        return table
    column = table[spec.column]
    if patypes.is_dictionary(column.type) and (spec.dtype is None or column.type == spec.dtype):
        return table
    if spec.dtype is not None:
        encoded = pc.cast(column, spec.dtype, safe=False)
        return set_or_append_column(table, spec.column, encoded)
    encoded = pc.dictionary_encode(column)
    return set_or_append_column(table, spec.column, encoded)


def encode_columns(table: TableLike, specs: tuple[EncodingSpec, ...]) -> TableLike:
    """Apply dictionary encoding for multiple columns.

    Returns
    -------
    TableLike
        Table with dictionary encoding applied.
    """
    out = table
    for spec in specs:
        out = encode_dictionary(out, spec)
    return out


def encode_expression(column: str) -> ComputeExpression:
    """Return a compute expression for dictionary encoding a column.

    Returns
    -------
    ComputeExpression
        Expression applying dictionary encoding.
    """
    expr = FieldExpr(column).to_expression()
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
        if schema_field.name in available_set:
            expr = pc.cast(pc.field(schema_field.name), schema_field.type, safe=safe_cast)
        else:
            expr = pc.cast(pc.scalar(None), schema_field.type, safe=safe_cast)
        expressions.append(ensure_expression(expr))
        names.append(schema_field.name)
    return expressions, names


def schema_to_dict(schema: SchemaLike) -> dict[str, object]:
    """Serialize an Arrow schema to a plain dictionary.

    Returns
    -------
    dict[str, object]
        JSON-serializable schema representation.
    """
    return {
        "fields": [
            {"name": field.name, "type": str(field.type), "nullable": bool(field.nullable)}
            for field in schema
        ]
    }


def schema_fingerprint(schema: SchemaLike) -> str:
    """Compute a stable schema fingerprint hash.

    Returns
    -------
    str
        SHA-256 fingerprint of the schema.
    """
    payload = json.dumps(schema_to_dict(schema), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "AlignmentInfo",
    "CastErrorPolicy",
    "EncodingPolicy",
    "EncodingSpec",
    "SchemaEvolutionSpec",
    "SchemaMetadataSpec",
    "SchemaTransform",
    "align_to_schema",
    "empty_table",
    "encode_columns",
    "encode_dictionary",
    "encode_expression",
    "projection_for_schema",
    "schema_fingerprint",
    "schema_to_dict",
]
