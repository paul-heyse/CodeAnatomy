"""Shared encoding metadata helpers."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

ENCODING_META = "encoding"
ENCODING_DICTIONARY = "dictionary"
DICT_INDEX_META = "dictionary_index_type"
DICT_ORDERED_META = "dictionary_ordered"


def dict_field_metadata(
    *,
    index_type: pa.DataType | None = None,
    ordered: bool = False,
    metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return metadata for dictionary-encoded field specs.

    Returns:
    -------
    dict[str, str]
        Metadata mapping for dictionary encoding.
    """
    idx_type = index_type or pa.int32()
    meta = {
        ENCODING_META: ENCODING_DICTIONARY,
        DICT_INDEX_META: str(idx_type),
        DICT_ORDERED_META: "1" if ordered else "0",
    }
    if metadata is not None:
        meta.update(metadata)
    return meta


__all__ = [
    "DICT_INDEX_META",
    "DICT_ORDERED_META",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "dict_field_metadata",
]
