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
    dict_cols: dict[str, pa.DictionaryType] = {}
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for field in table.schema:
        col = cast("ChunkedArrayLike", table[field.name])
        if patypes.is_dictionary(field.type):
            dict_type = cast("pa.DictionaryType", field.type)
            dict_cols[field.name] = dict_type
            col = cast("ChunkedArrayLike", pc.cast(col, dict_type.value_type))
        columns.append(col)
        names.append(field.name)
    decoded = pa.table(columns, names=names)
    if combine_chunks:
        decoded = decoded.combine_chunks()
    if not dict_cols:
        return decoded
    columns = []
    for field in decoded.schema:
        col = cast("ChunkedArrayLike", decoded[field.name])
        dict_type = dict_cols.get(field.name)
        if dict_type is not None:
            encoded = pc.dictionary_encode(col)
            col = cast("ChunkedArrayLike", pc.cast(encoded, dict_type))
        columns.append(col)
    return pa.table(columns, names=names)


__all__ = ["normalize_dictionaries"]
