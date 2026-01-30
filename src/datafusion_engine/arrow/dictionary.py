"""Dictionary normalization helpers for Arrow tables."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from datafusion_engine.arrow.interop import ChunkedArrayLike, TableLike


def _schema_with_metadata(
    names: list[str],
    columns: list[ChunkedArrayLike],
    *,
    source_schema: pa.Schema,
) -> pa.Schema:
    fields: list[pa.Field] = []
    for name, column in zip(names, columns, strict=True):
        if name in source_schema.names:
            source_field = source_schema.field(name)
            fields.append(
                pa.field(
                    name,
                    column.type,
                    nullable=source_field.nullable,
                    metadata=source_field.metadata,
                )
            )
        else:
            fields.append(pa.field(name, column.type))
    return pa.schema(fields, metadata=source_schema.metadata)


def _cast_column(
    column: ChunkedArrayLike,
    dtype: pa.DataType,
    *,
    message: str,
) -> ChunkedArrayLike:
    cast_fn = getattr(column, "cast", None)
    if not callable(cast_fn):
        raise TypeError(message)
    return cast("ChunkedArrayLike", cast_fn(dtype, safe=False))


def _dictionary_encode(column: ChunkedArrayLike) -> ChunkedArrayLike:
    encode_fn = getattr(column, "dictionary_encode", None)
    if not callable(encode_fn):
        msg = "Column does not support dictionary_encode()."
        raise TypeError(msg)
    return cast("ChunkedArrayLike", encode_fn())


def _decode_dictionary_columns(
    table: TableLike,
) -> tuple[list[str], list[ChunkedArrayLike], dict[str, pa.DictionaryType], pa.Schema]:
    source_schema = table.schema
    dict_cols: dict[str, pa.DictionaryType] = {}
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for field in source_schema:
        column = table[field.name]
        if patypes.is_dictionary(field.type):
            dict_type = cast("pa.DictionaryType", field.type)
            dict_cols[field.name] = dict_type
            column = _cast_column(
                column,
                dict_type.value_type,
                message="Dictionary column does not support cast().",
            )
        columns.append(column)
        names.append(field.name)
    decoded_schema = _schema_with_metadata(names, columns, source_schema=source_schema)
    return names, columns, dict_cols, decoded_schema


def _encode_dictionary_columns(
    decoded: pa.Table,
    *,
    names: list[str],
    dict_cols: dict[str, pa.DictionaryType],
    source_schema: pa.Schema,
) -> pa.Table:
    columns: list[ChunkedArrayLike] = []
    for field in decoded.schema:
        column = decoded[field.name]
        dict_type = dict_cols.get(field.name)
        if dict_type is not None:
            encoded = _dictionary_encode(column)
            column = _cast_column(
                encoded,
                dict_type,
                message="Encoded dictionary column does not support cast().",
            )
        columns.append(column)
    encoded_schema = _schema_with_metadata(names, columns, source_schema=source_schema)
    return pa.table(columns, schema=encoded_schema)


def normalize_dictionaries(
    table: TableLike,
    *,
    combine_chunks: bool = True,
) -> TableLike:
    """Return a table with unified dictionaries and normalized chunks.

    Returns
    -------
    TableLike
        Table with unified dictionary columns.

    Raises
    ------
    TypeError
        Raised when dictionary casting or encoding is unsupported.
    """
    try:
        names, columns, dict_cols, decoded_schema = _decode_dictionary_columns(table)
        decoded = pa.table(columns, schema=decoded_schema)
        if combine_chunks:
            decoded = decoded.combine_chunks()
        if not dict_cols:
            return decoded
        return _encode_dictionary_columns(
            decoded,
            names=names,
            dict_cols=dict_cols,
            source_schema=table.schema,
        )
    except TypeError as exc:
        msg = "Dictionary normalization failed."
        raise TypeError(msg) from exc


__all__ = ["normalize_dictionaries"]
