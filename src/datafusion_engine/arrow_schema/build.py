"""Table and column builders for Arrow utilities."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.types as patypes

from arrow_utils.core.array_iter import iter_arrays
from datafusion_engine.arrow_interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    FieldLike,
    SchemaLike,
    TableLike,
    ensure_expression,
)
from datafusion_engine.arrow_schema.nested_builders import (
    build_list,
    build_list_of_structs,
    build_list_view,
    build_struct,
    dense_union_array,
    dictionary_array_from_indices,
    dictionary_array_from_values,
    list_array_from_lists,
    list_view_array_from_lists,
    map_array_from_pairs,
    nested_array_factory,
    sparse_union_array,
    struct_array_from_dicts,
    union_array_from_tagged_values,
    union_array_from_values,
)
from datafusion_engine.arrow_schema.types import list_view_type, map_type


def _resolve_schema(schema: SchemaLike) -> pa.Schema:
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Schema must be a pyarrow.Schema derived from DataFusion."
    raise TypeError(msg)


class ColumnExpr(Protocol):
    """Protocol for column expressions used by defaults."""

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return a concrete array for the provided table."""
        ...


@dataclass(frozen=True)
class ConstExpr:
    """Column expression representing a constant literal."""

    value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the constant value.

        Returns
        -------
        ComputeExpression
            Expression representing the constant value.
        """
        scalar_value = self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return ensure_expression(ds.scalar(scalar_value))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the constant as a full-length array.

        Returns
        -------
        ArrayLike
            Array filled with the constant value.
        """
        values = [self.value] * table.num_rows
        scalar_type = pa.scalar(self.value, type=self.dtype).type
        return pa.array(values, type=scalar_type)


@dataclass(frozen=True)
class FieldExpr:
    """Column expression referencing an existing column."""

    name: str

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the column reference.

        Returns
        -------
        ComputeExpression
            Expression referencing the column.
        """
        return ds.field(self.name)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the column values from the table.

        Returns
        -------
        ArrayLike
            Column values from the table.
        """
        return table[self.name]


@dataclass(frozen=True)
class ColumnOrNullExpr:
    """Column expression that falls back to typed nulls when missing."""

    name: str
    dtype: DataTypeLike

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the column values or typed nulls.

        Returns
        -------
        ArrayLike
            Column values or typed nulls when missing.

        Raises
        ------
        TypeError
            Raised when the column does not support cast().
        """
        if self.name not in table.column_names:
            return pa.nulls(table.num_rows, type=self.dtype)
        column = table[self.name]
        cast_fn = getattr(column, "cast", None)
        if not callable(cast_fn):
            msg = "Column does not support cast()."
            raise TypeError(msg)
        return cast("ArrayLike", cast_fn(self.dtype, safe=False))


@dataclass(frozen=True)
class CoalesceExpr:
    """Column expression that coalesces multiple expressions."""

    exprs: tuple[ColumnExpr, ...]

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table.

        Returns
        -------
        ArrayLike
            Coalesced array result.
        """
        if not self.exprs:
            return pa.nulls(table.num_rows, type=pa.null())
        arrays = [expr.materialize(table) for expr in self.exprs]
        rows: list[object | None] = []
        for values in iter_arrays(arrays):
            first = next((value for value in values if value is not None), None)
            rows.append(first)
        return pa.array(rows, type=arrays[0].type)


def const_array(n: int, value: object, *, dtype: DataTypeLike | None = None) -> ArrayLike:
    """Return a constant array of length ``n`` with the given value.

    Returns
    -------
    ArrayLike
        Array of repeated values.
    """
    scalar = pa.scalar(value) if dtype is None else pa.scalar(value, type=dtype)
    return pa.array([value] * n, type=scalar.type)


def set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    """Set a column by name, appending if it does not exist.

    Returns
    -------
    TableLike
        Updated table.
    """
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


@dataclass(frozen=True)
class ColumnDefaultsSpec:
    """Specification for adding default columns when missing."""

    defaults: tuple[tuple[str, ColumnExpr], ...]
    overwrite: bool = False

    def apply(self, table: TableLike) -> TableLike:
        """Apply default column values to a table.

        Returns
        -------
        TableLike
            Table with defaults applied.
        """
        out = table
        for name, expr in self.defaults:
            if not self.overwrite and name in out.column_names:
                continue
            values = expr.materialize(out)
            out = set_or_append_column(out, name, values)
        return out


def struct_type(fields: Sequence[FieldLike] | Mapping[str, DataTypeLike]) -> DataTypeLike:
    """Return a struct type built from fields or name/type mappings.

    Returns
    -------
    DataTypeLike
        Struct data type.
    """
    if isinstance(fields, Mapping):
        return pa.struct(fields)
    return pa.struct(list(fields))


def column_or_null(
    table: TableLike,
    col: str,
    dtype: DataTypeLike,
) -> ArrayLike:
    """Return a column array or a typed null array when missing.

    Returns
    -------
    ArrayLike
        Column array or a typed null array.
    """
    return ColumnOrNullExpr(name=col, dtype=dtype).materialize(table)


def maybe_dictionary(
    values: ArrayLike | ChunkedArrayLike,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    """Return dictionary-encoded values when requested by dtype.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Dictionary-encoded values when needed.

    Raises
    ------
    TypeError
        Raised when dictionary encoding is unsupported.
    """
    if patypes.is_dictionary(values.type):
        return values
    if patypes.is_dictionary(dtype):
        encode_fn = getattr(values, "dictionary_encode", None)
        if not callable(encode_fn):
            msg = "Values do not support dictionary_encode()."
            raise TypeError(msg)
        return cast("ArrayLike | ChunkedArrayLike", encode_fn())
    return values


def pick_first(
    table: TableLike,
    cols: Sequence[str],
    *,
    ignore_missing: bool = True,
    default: object | None = None,
) -> object | None:
    """Return the first row value from the first column found.

    Returns
    -------
    object | None
        First row value or default.

    Raises
    ------
    KeyError
        Raised when a required column is missing.
    """
    if table.num_rows == 0:
        return default
    for col in cols:
        if col not in table.column_names:
            if ignore_missing:
                continue
            msg = f"Missing column {col!r}."
            raise KeyError(msg)
        column = table[col]
        if isinstance(column, pa.ChunkedArray):
            chunked = cast("pa.ChunkedArray", column)
            if chunked.num_chunks == 0:
                return default
            first_chunk = chunked.chunk(0)
            value = first_chunk[0]
        else:
            value = cast("pa.Array", column)[0]
        return value.as_py() if hasattr(value, "as_py") else value
    return default


def table_from_arrays(
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
    *,
    schema: SchemaLike | None = None,
    names: Sequence[str] | None = None,
) -> TableLike:
    """Return a table from arrays with optional schema metadata.

    Returns
    -------
    TableLike
        Table built from arrays.

    Raises
    ------
    ValueError
        Raised when both schema and names are missing.
    """
    if schema is None and names is None:
        msg = "table_from_arrays requires schema or names."
        raise ValueError(msg)
    resolved_schema = _resolve_schema(schema) if schema is not None else None
    return pa.table(arrays, schema=resolved_schema, names=names)


def table_from_columns(
    schema: SchemaLike,
    columns: Mapping[str, ArrayLike | ChunkedArrayLike],
) -> TableLike:
    """Return a table from a schema and column mapping.

    Returns
    -------
    TableLike
        Table built from ordered schema fields.

    Raises
    ------
    ValueError
        Raised when any schema field is missing from the column mapping.
    """
    resolved_schema = _resolve_schema(schema)
    arrays: list[ArrayLike | ChunkedArrayLike] = []
    missing: list[str] = []
    for field in resolved_schema:
        if field.name not in columns:
            missing.append(field.name)
            continue
        arrays.append(columns[field.name])
    if missing:
        msg = f"Missing columns for schema fields: {sorted(missing)}."
        raise ValueError(msg)
    return pa.table(arrays, schema=resolved_schema)


def empty_table(schema: SchemaLike) -> TableLike:
    """Return an empty table for a schema.

    Returns
    -------
    TableLike
        Empty Arrow table.
    """
    resolved = _resolve_schema(schema)
    columns = [pa.array([], type=field.type) for field in resolved]
    return pa.table(columns, schema=resolved)


def table_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike | None = None,
) -> TableLike:
    """Return a table from a sequence of mapping rows.

    Returns
    -------
    TableLike
        Table built from mapping rows.
    """
    resolved_schema = _resolve_schema(schema) if schema is not None else None
    return pa.Table.from_pylist(list(rows), schema=resolved_schema)


def table_from_row_dicts(rows: Iterable[Mapping[str, object]]) -> TableLike:
    """Return a table from a sequence of mapping rows.

    Returns
    -------
    TableLike
        Table built from mapping rows.
    """
    return table_from_rows(rows)


def schema_columns(
    schema: SchemaLike,
    *,
    include_metadata: bool = True,
) -> list[str]:
    """Return schema column names with optional metadata flattening.

    Returns
    -------
    list[str]
        Column name list.
    """
    resolved = _resolve_schema(schema)
    cols = list(resolved.names)
    if include_metadata:
        meta = resolved.metadata or {}
        cols.extend(sorted(key.decode("utf-8") for key in meta))
    return cols


def schema_fields(schema: SchemaLike) -> list[FieldLike]:
    """Return schema fields in order.

    Returns
    -------
    list[FieldLike]
        Schema fields.
    """
    resolved = _resolve_schema(schema)
    return list(resolved)


def table_from_schema(schema: SchemaLike) -> TableLike:
    """Return an empty table for a schema.

    Returns
    -------
    TableLike
        Empty Arrow table.
    """
    return empty_table(schema)


def rows_to_table(rows: Iterable[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Return a table from rows and an explicit schema.

    Returns
    -------
    TableLike
        Table built from mapping rows with the provided schema.
    """
    return table_from_rows(rows, schema=schema)


def record_batch_reader_from_rows(
    schema: SchemaLike,
    rows: Iterable[Mapping[str, object]],
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader built from mapping rows.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader yielding record batches aligned to the schema.
    """
    resolved = _resolve_schema(schema)
    table = pa.Table.from_pylist(list(rows), schema=resolved)
    return pa.RecordBatchReader.from_batches(resolved, table.to_batches())


def record_batch_reader_from_row_batches(
    schema: SchemaLike,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader built from row batches.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader yielding record batches aligned to the schema.
    """
    resolved = _resolve_schema(schema)
    batches = [pa.RecordBatch.from_pylist(list(batch), schema=resolved) for batch in row_batches]
    return pa.RecordBatchReader.from_batches(resolved, batches)


def list_table_from_rows(
    rows: Iterable[Sequence[object]],
    *,
    fields: Sequence[FieldLike] | Mapping[str, DataTypeLike],
) -> TableLike:
    """Return a table from row sequences and explicit field definitions.

    Returns
    -------
    TableLike
        Table built from row sequences.
    """
    if isinstance(fields, Mapping):
        schema = pa.schema([(name, dtype) for name, dtype in fields.items()])
    else:
        schema = pa.schema(list(fields))
    columns: list[list[object]] = [[] for _ in schema]
    for row in rows:
        for idx, value in enumerate(row):
            columns[idx].append(value)
    arrays = [
        pa.array(values, type=field.type) for values, field in zip(columns, schema, strict=True)
    ]
    return pa.table(arrays, schema=schema)


def iter_columns(table: TableLike) -> Iterator[tuple[str, ArrayLike | ChunkedArrayLike]]:
    """Iterate column name and array pairs.

    Yields
    ------
    tuple[str, ArrayLike | ChunkedArrayLike]
        Column name and values.
    """
    for name in table.column_names:
        yield name, table[name]


def iter_rows(table: TableLike) -> Iterator[dict[str, object | None]]:
    """Iterate rows of a table as dictionaries.

    Yields
    ------
    dict[str, object | None]
        Mapping of column name to value.
    """
    names = list(table.column_names)
    columns = [table[name] for name in names]
    for values in zip(*columns, strict=True):
        row: dict[str, object | None] = {}
        for name, value in zip(names, values, strict=True):
            row[name] = value.as_py() if hasattr(value, "as_py") else value
        yield row


def rows_from_table(table: TableLike) -> list[dict[str, object | None]]:
    """Return all rows from a table as dictionaries.

    Returns
    -------
    list[dict[str, object | None]]
        List of row mappings.
    """
    return list(iter_rows(table))


def table_from_arrays_with_schema(
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
    schema: SchemaLike,
) -> TableLike:
    """Return a table from arrays with a given schema.

    Returns
    -------
    TableLike
        Table built from arrays with a schema.
    """
    return pa.table(arrays, schema=_resolve_schema(schema))


def cast_table(
    table: TableLike,
    schema: SchemaLike,
    *,
    safe: bool = True,
) -> TableLike:
    """Cast a table to the provided schema.

    Returns
    -------
    TableLike
        Casted table.
    """
    resolved = _resolve_schema(schema)
    return table.cast(resolved, safe=safe)


def list_view_array_from_text(
    values: Sequence[str],
    *,
    large: bool = False,
) -> ArrayLike:
    """Return a list view array from string values.

    Returns
    -------
    ArrayLike
        List view array from string values.
    """
    dtype = list_view_type(pa.string(), large=large)
    return pa.array(values, type=dtype)


def value_field(
    field: FieldLike,
    *,
    large: bool = False,
) -> FieldLike:
    """Return a value field for list or map types.

    Returns
    -------
    FieldLike
        Value field for nested list/map types.
    """
    dtype = field.type
    if patypes.is_list(dtype) or patypes.is_large_list(dtype):
        list_type = cast("pa.ListType | pa.LargeListType", dtype)
        value_type = list_type.value_type
        return pa.field(field.name, list_view_type(value_type, large=large))
    if patypes.is_map(dtype):
        map_type_value = cast("pa.MapType", dtype)
        item_type = map_type_value.item_type
        keys_sorted = cast("bool", getattr(map_type_value, "keys_sorted", False))
        return pa.field(
            field.name, map_type(map_type_value.key_type, item_type, keys_sorted=keys_sorted)
        )
    return field


def build_const_table(
    n: int,
    values: Mapping[str, object],
) -> TableLike:
    """Return a constant table of length ``n``.

    Returns
    -------
    TableLike
        Constant table with repeated values.
    """
    arrays = [pa.array([value] * n, type=pa.scalar(value).type) for value in values.values()]
    return pa.table(arrays, names=list(values.keys()))


def concatenate_tables(
    tables: Iterable[TableLike],
    *,
    promote: bool = True,
) -> TableLike:
    """Concatenate multiple tables with schema unification.

    Returns
    -------
    TableLike
        Concatenated table.

    Raises
    ------
    ValueError
        Raised when no tables are provided.
    """
    tables_list = list(tables)
    if not tables_list:
        msg = "concatenate_tables requires at least one table."
        raise ValueError(msg)
    if len(tables_list) == 1:
        return tables_list[0]
    return pa.concat_tables(tables_list, promote=promote)


def table_from_schema_payload(payload: Mapping[str, object]) -> TableLike:
    """Return a table from a schema payload mapping.

    Returns
    -------
    TableLike
        Table constructed from the payload mapping.

    Raises
    ------
    TypeError
        Raised when the payload schema or rows are invalid.
    """
    schema = payload.get("schema")
    if not isinstance(schema, pa.Schema):
        msg = "Payload must include a pyarrow.Schema under 'schema'."
        raise TypeError(msg)
    schema = cast("pa.Schema", schema)
    rows = payload.get("rows")
    if rows is None:
        return empty_table(schema)
    if isinstance(rows, Sequence):
        return pa.Table.from_pylist(list(rows), schema=schema)
    msg = "Payload 'rows' must be a sequence of mapping rows."
    raise TypeError(msg)


def register_schema_extensions(schema: SchemaLike) -> None:
    """Register schema extension types when present."""
    resolved = _resolve_schema(schema)
    for field in resolved:
        is_extension = getattr(patypes, "is_extension", None)
        if callable(is_extension) and is_extension(field.type):
            extension_type = cast("pa.ExtensionType", field.type)
            pa.register_extension_type(extension_type)
            continue
        if isinstance(field.type, pa.ExtensionType):
            extension_type = cast("pa.ExtensionType", field.type)
            pa.register_extension_type(extension_type)


__all__ = [
    "CoalesceExpr",
    "ColumnDefaultsSpec",
    "ColumnExpr",
    "ColumnOrNullExpr",
    "ConstExpr",
    "FieldExpr",
    "array_from_lists",
    "build_const_table",
    "build_list",
    "build_list_of_structs",
    "build_list_view",
    "build_struct",
    "cast_table",
    "column_or_null",
    "concatenate_tables",
    "const_array",
    "dense_union_array",
    "dictionary_array_from_indices",
    "dictionary_array_from_values",
    "empty_table",
    "iter_columns",
    "iter_rows",
    "list_array_from_lists",
    "list_table_from_rows",
    "list_view_array_from_lists",
    "list_view_array_from_text",
    "list_view_type",
    "map_array_from_pairs",
    "map_type",
    "maybe_dictionary",
    "nested_array_factory",
    "pick_first",
    "record_batch_reader_from_row_batches",
    "record_batch_reader_from_rows",
    "register_schema_extensions",
    "rows_from_table",
    "rows_to_table",
    "schema_columns",
    "schema_fields",
    "set_or_append_column",
    "sparse_union_array",
    "struct_array_from_dicts",
    "struct_type",
    "table_from_arrays",
    "table_from_arrays_with_schema",
    "table_from_columns",
    "table_from_row_dicts",
    "table_from_rows",
    "table_from_schema",
    "table_from_schema_payload",
    "union_array_from_tagged_values",
    "union_array_from_values",
    "value_field",
]


def array_from_lists(values: Iterable[Sequence[object]]) -> ArrayLike:
    """Return a list array for a sequence of lists.

    Returns
    -------
    ArrayLike
        List array representing the nested lists.
    """
    normalized = [list(item) for item in values]
    flattened = [item for row in normalized for item in row]
    value_type = pa.infer_type(flattened) if flattened else pa.null()
    list_type = pa.list_(value_type)
    return list_array_from_lists(normalized, list_type=list_type)
