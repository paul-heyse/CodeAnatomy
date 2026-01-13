"""Encoding policy helpers for schema specifications."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import ArrayLike, DataTypeLike, FieldLike, SchemaLike, TableLike
from arrowdsl.schema.nested_builders import (
    dictionary_array_from_indices as _dictionary_from_indices,
)
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec

ENCODING_META = "encoding"
ENCODING_DICTIONARY = "dictionary"
DICT_INDEX_META = "dictionary_index_type"
DICT_ORDERED_META = "dictionary_ordered"

_ORDERED_TRUE = {"1", "true", "yes", "y", "t"}
_INDEX_TYPES: Mapping[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
}


class _ArrowFieldSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def dtype(self) -> DataTypeLike: ...

    @property
    def metadata(self) -> Mapping[str, str]: ...

    @property
    def encoding(self) -> str | None: ...


class _TableSchemaSpec(Protocol):
    @property
    def fields(self) -> Sequence[_ArrowFieldSpec]: ...


def _meta_value(meta: Mapping[bytes, bytes] | None, key: str) -> str | None:
    if not meta:
        return None
    raw = meta.get(key.encode("utf-8"))
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _index_type_from_meta(meta: Mapping[bytes, bytes] | None) -> pa.DataType:
    raw = _meta_value(meta, DICT_INDEX_META)
    if raw is None:
        return pa.int32()
    return _INDEX_TYPES.get(raw.strip().lower(), pa.int32())


def _ordered_from_meta(meta: Mapping[bytes, bytes] | None) -> bool:
    raw = _meta_value(meta, DICT_ORDERED_META)
    if raw is None:
        return False
    return raw.strip().lower() in _ORDERED_TRUE


def dict_field_metadata(
    *,
    index_type: pa.DataType | None = None,
    ordered: bool = False,
    metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return metadata for dictionary-encoded field specs.

    Returns
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


def _encoding_hint(field: _ArrowFieldSpec) -> str | None:
    if field.encoding is not None:
        return field.encoding
    return field.metadata.get(ENCODING_META)


def _encoding_spec_from_field(field: _ArrowFieldSpec) -> EncodingSpec | None:
    hint = _encoding_hint(field)
    if hint != ENCODING_DICTIONARY:
        if patypes.is_dictionary(field.dtype):
            return EncodingSpec(column=field.name, dtype=field.dtype)
        return None
    if patypes.is_dictionary(field.dtype):
        return EncodingSpec(column=field.name, dtype=field.dtype)
    return EncodingSpec(column=field.name)


def _encoding_spec_from_metadata(field: FieldLike) -> EncodingSpec | None:
    meta = field.metadata or {}
    if meta.get(ENCODING_META.encode("utf-8")) != ENCODING_DICTIONARY.encode("utf-8"):
        return None
    if patypes.is_dictionary(field.type):
        return EncodingSpec(column=field.name, dtype=field.type)
    idx_type = _index_type_from_meta(meta)
    ordered = _ordered_from_meta(meta)
    dtype = pa.dictionary(idx_type, field.type, ordered=ordered)
    return EncodingSpec(column=field.name, dtype=dtype)


def encoding_policy_from_spec(table_spec: _TableSchemaSpec) -> EncodingPolicy:
    """Return an encoding policy derived from a TableSchemaSpec.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from the table spec.
    """
    specs = tuple(
        spec
        for field in table_spec.fields
        if (spec := _encoding_spec_from_field(field)) is not None
    )
    return EncodingPolicy(specs=specs)


def encoding_policy_from_fields(fields: Sequence[_ArrowFieldSpec]) -> EncodingPolicy:
    """Return an encoding policy derived from ArrowFieldSpec values.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from field specs.
    """
    specs = tuple(
        spec for field in fields if (spec := _encoding_spec_from_field(field)) is not None
    )
    return EncodingPolicy(specs=specs)


def encoding_policy_from_schema(schema: SchemaLike) -> EncodingPolicy:
    """Return an encoding policy derived from schema field metadata.

    Returns
    -------
    EncodingPolicy
        Encoding policy for dictionary-encoded columns.
    """
    specs = tuple(
        spec for field in schema if (spec := _encoding_spec_from_metadata(field)) is not None
    )
    return EncodingPolicy(specs=specs)


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
    out = table.combine_chunks() if combine_chunks else table
    return out.unify_dictionaries()


def dictionary_array_from_indices(
    indices: ArrayLike | Sequence[int | None],
    dictionary: ArrayLike | Sequence[object],
    *,
    index_type: DataTypeLike | None = None,
    dictionary_type: DataTypeLike | None = None,
    ordered: bool = False,
) -> ArrayLike:
    """Build a dictionary array from indices and dictionary values.

    Returns
    -------
    ArrayLike
        Dictionary array with explicit dictionary values.
    """
    return _dictionary_from_indices(
        indices,
        dictionary,
        index_type=index_type,
        dictionary_type=dictionary_type,
        ordered=ordered,
    )


__all__ = [
    "DICT_INDEX_META",
    "DICT_ORDERED_META",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "dict_field_metadata",
    "dictionary_array_from_indices",
    "encoding_policy_from_fields",
    "encoding_policy_from_schema",
    "encoding_policy_from_spec",
    "normalize_dictionaries",
]
