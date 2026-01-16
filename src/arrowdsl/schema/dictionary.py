"""Dictionary normalization helpers for Arrow tables."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import ChunkedArrayLike, TableLike, pc


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
    """
    def schema_with_metadata(
        names: list[str],
        columns: list[ChunkedArrayLike],
        *,
        source_schema: pa.Schema,
    ) -> pa.Schema:
        fields: list[pa.Field] = []
        for name, col in zip(names, columns, strict=True):
            if name in source_schema.names:
                source_field = source_schema.field(name)
                fields.append(
                    pa.field(
                        name,
                        col.type,
                        nullable=source_field.nullable,
                        metadata=source_field.metadata,
                    )
                )
            else:
                fields.append(pa.field(name, col.type))
        return pa.schema(fields, metadata=source_schema.metadata)

    source_schema = table.schema
    dict_cols: dict[str, pa.DictionaryType] = {}
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for field in source_schema:
        col = table[field.name]
        if patypes.is_dictionary(field.type):
            dict_type = cast("pa.DictionaryType", field.type)
            dict_cols[field.name] = dict_type
            col = cast("ChunkedArrayLike", pc.cast(col, dict_type.value_type))
        columns.append(col)
        names.append(field.name)
    decoded_schema = schema_with_metadata(names, columns, source_schema=source_schema)
    decoded = pa.table(columns, schema=decoded_schema)
    if combine_chunks:
        decoded = decoded.combine_chunks()
    if not dict_cols:
        return decoded
    columns = []
    for field in decoded.schema:
        col = decoded[field.name]
        dict_type = dict_cols.get(field.name)
        if dict_type is not None:
            encoded = pc.dictionary_encode(col)
            col = cast("ChunkedArrayLike", pc.cast(encoded, dict_type))
        columns.append(col)
    encoded_schema = schema_with_metadata(names, columns, source_schema=source_schema)
    return pa.table(columns, schema=encoded_schema)


__all__ = ["normalize_dictionaries"]
