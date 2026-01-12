"""Encoding policies and helpers for normalize outputs."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.core.interop import FieldLike, SchemaLike
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec
from schema_spec.specs import ArrowFieldSpec

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


def dict_field(
    name: str,
    *,
    index_type: pa.DataType | None = None,
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
    val_type = pa.string()
    idx_type = index_type or pa.int32()
    meta = {
        ENCODING_META: ENCODING_DICTIONARY,
        DICT_INDEX_META: str(idx_type),
        DICT_ORDERED_META: "1" if ordered else "0",
    }
    if metadata is not None:
        meta.update(metadata)
    return ArrowFieldSpec(
        name=name,
        dtype=pa.dictionary(idx_type, val_type, ordered=ordered),
        nullable=nullable,
        metadata=meta,
    )


def _encoding_spec_from_field(field: FieldLike) -> EncodingSpec | None:
    meta = field.metadata or {}
    if meta.get(ENCODING_META.encode("utf-8")) != ENCODING_DICTIONARY.encode("utf-8"):
        return None
    if patypes.is_dictionary(field.type):
        return EncodingSpec(column=field.name, dtype=field.type)
    idx_type = _index_type_from_meta(meta)
    ordered = _ordered_from_meta(meta)
    dtype = pa.dictionary(idx_type, field.type, ordered=ordered)
    return EncodingSpec(column=field.name, dtype=dtype)


def encoding_policy_from_schema(schema: SchemaLike) -> EncodingPolicy:
    """Return an encoding policy derived from schema field metadata.

    Returns
    -------
    EncodingPolicy
        Encoding policy for dictionary-encoded columns.
    """
    specs = tuple(
        spec for field in schema if (spec := _encoding_spec_from_field(field)) is not None
    )
    return EncodingPolicy(specs=specs, chunk_policy=ChunkPolicy())


__all__ = [
    "DICT_INDEX_META",
    "DICT_ORDERED_META",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "dict_field",
    "encoding_policy_from_schema",
]
