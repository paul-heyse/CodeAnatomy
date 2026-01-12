"""Post-processing helpers for extractor tables."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import EncodingSpec, encode_columns


def apply_encoding(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified string columns.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    specs = tuple(EncodingSpec(column=col) for col in columns)
    return encode_columns(table, specs=specs)
