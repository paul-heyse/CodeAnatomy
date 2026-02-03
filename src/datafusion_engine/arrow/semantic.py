"""Arrow extension and metadata helpers for semantic types."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

SEMANTIC_TYPE_META = b"semantic_type"


@dataclass(frozen=True)
class SemanticTypeInfo:
    """Describe a semantic type and its metadata encoding."""

    name: str
    extension_name: str
    storage_type: pa.DataType


BYTE_SPAN_STORAGE = pa.struct(
    [
        ("byte_start", pa.int32()),
        ("byte_len", pa.int32()),
    ]
)
SPAN_STORAGE = pa.struct(
    [
        ("start", pa.struct([("line0", pa.int32()), ("col", pa.int32())])),
        ("end", pa.struct([("line0", pa.int32()), ("col", pa.int32())])),
        ("end_exclusive", pa.bool_()),
        ("col_unit", pa.string()),
        ("byte_span", BYTE_SPAN_STORAGE),
    ]
)

SPAN_TYPE_INFO = SemanticTypeInfo(
    name="Span",
    extension_name="codeintel.span",
    storage_type=SPAN_STORAGE,
)
BYTE_SPAN_TYPE_INFO = SemanticTypeInfo(
    name="ByteSpan",
    extension_name="codeintel.byte_span",
    storage_type=BYTE_SPAN_STORAGE,
)
NODE_ID_TYPE_INFO = SemanticTypeInfo(
    name="NodeId",
    extension_name="codeintel.node_id",
    storage_type=pa.string(),
)
EDGE_ID_TYPE_INFO = SemanticTypeInfo(
    name="EdgeId",
    extension_name="codeintel.edge_id",
    storage_type=pa.string(),
)
SPAN_ID_TYPE_INFO = SemanticTypeInfo(
    name="SpanId",
    extension_name="codeintel.span_id",
    storage_type=pa.string(),
)


def _arrow_ext_serialize(self: _SemanticExtensionType) -> bytes:
    return self._info.name.encode("utf-8")


@classmethod
def _arrow_ext_deserialize(
    cls,
    _storage_type: pa.DataType,
    serialized: object,
    *_args: object,
) -> pa.ExtensionType:
    payload = _coerce_extension_payload(serialized, _args)
    name = payload.decode("utf-8")
    if name == SPAN_TYPE_INFO.name:
        return _SemanticExtensionType(SPAN_TYPE_INFO)
    if name == BYTE_SPAN_TYPE_INFO.name:
        return _SemanticExtensionType(BYTE_SPAN_TYPE_INFO)
    if name == NODE_ID_TYPE_INFO.name:
        return _SemanticExtensionType(NODE_ID_TYPE_INFO)
    if name == EDGE_ID_TYPE_INFO.name:
        return _SemanticExtensionType(EDGE_ID_TYPE_INFO)
    if name == SPAN_ID_TYPE_INFO.name:
        return _SemanticExtensionType(SPAN_ID_TYPE_INFO)
    msg = f"Unsupported semantic extension type: {name!r}."
    raise ValueError(msg)


class _SemanticExtensionType(pa.ExtensionType):
    __arrow_ext_serialize__ = _arrow_ext_serialize
    __arrow_ext_deserialize__ = _arrow_ext_deserialize

    def __init__(self, info: SemanticTypeInfo) -> None:
        self._info = info
        super().__init__(info.storage_type, info.extension_name)


def _coerce_extension_payload(
    serialized: object,
    extra_args: tuple[object, ...],
) -> bytes:
    if isinstance(serialized, (bytes, bytearray, memoryview)):
        return bytes(serialized)
    for value in extra_args:
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
    msg = "Semantic extension payload was not provided as bytes."
    raise TypeError(msg)


def register_semantic_extension_types() -> None:
    """Register semantic extension types with pyarrow."""
    for info in (
        SPAN_TYPE_INFO,
        BYTE_SPAN_TYPE_INFO,
        NODE_ID_TYPE_INFO,
        EDGE_ID_TYPE_INFO,
        SPAN_ID_TYPE_INFO,
    ):
        with contextlib.suppress(ValueError, pa.ArrowKeyError):
            pa.register_extension_type(_SemanticExtensionType(info))


def _extension_type(info: SemanticTypeInfo) -> pa.DataType:
    register_semantic_extension_types()
    return _SemanticExtensionType(info)


def semantic_type_metadata(info: SemanticTypeInfo) -> dict[bytes, bytes]:
    """Return canonical metadata for a semantic type.

    Returns
    -------
    dict[bytes, bytes]
        Encoded metadata mapping for the semantic type.
    """
    return {SEMANTIC_TYPE_META: info.name.encode("utf-8")}


def span_metadata(
    *,
    line_base: int = 0,
    col_unit: str = "byte",
    end_exclusive: bool = True,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return metadata for span-typed fields.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping for span semantic types.
    """
    metadata = semantic_type_metadata(SPAN_TYPE_INFO)
    metadata.update(
        {
            b"line_base": str(line_base).encode("utf-8"),
            b"col_unit": str(col_unit).encode("utf-8"),
            b"end_exclusive": b"true" if end_exclusive else b"false",
        }
    )
    if extra:
        metadata.update(extra)
    return metadata


def node_id_metadata(
    *,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return metadata for node-id semantic types.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping for node id semantic types.
    """
    metadata = semantic_type_metadata(NODE_ID_TYPE_INFO)
    if extra:
        metadata.update(extra)
    return metadata


def edge_id_metadata(
    *,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return metadata for edge-id semantic types.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping for edge id semantic types.
    """
    metadata = semantic_type_metadata(EDGE_ID_TYPE_INFO)
    if extra:
        metadata.update(extra)
    return metadata


def span_id_metadata(
    *,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return metadata for span-id semantic types.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping for span id semantic types.
    """
    metadata = semantic_type_metadata(SPAN_ID_TYPE_INFO)
    if extra:
        metadata.update(extra)
    return metadata


def span_type() -> pa.DataType:
    """Return the registered Span extension type.

    Returns
    -------
    pyarrow.DataType
        Extension data type for spans.
    """
    return _extension_type(SPAN_TYPE_INFO)


def byte_span_type() -> pa.DataType:
    """Return the registered ByteSpan extension type.

    Returns
    -------
    pyarrow.DataType
        Extension data type for byte spans.
    """
    return _extension_type(BYTE_SPAN_TYPE_INFO)


def node_id_type() -> pa.DataType:
    """Return the registered NodeId extension type.

    Returns
    -------
    pyarrow.DataType
        Extension data type for node ids.
    """
    return _extension_type(NODE_ID_TYPE_INFO)


def edge_id_type() -> pa.DataType:
    """Return the registered EdgeId extension type.

    Returns
    -------
    pyarrow.DataType
        Extension data type for edge ids.
    """
    return _extension_type(EDGE_ID_TYPE_INFO)


def span_id_type() -> pa.DataType:
    """Return the registered SpanId extension type.

    Returns
    -------
    pyarrow.DataType
        Extension data type for span ids.
    """
    return _extension_type(SPAN_ID_TYPE_INFO)


def semantic_type_for_field_name(name: str) -> SemanticTypeInfo | None:
    """Return the semantic type info for a field name when matched.

    Returns
    -------
    SemanticTypeInfo | None
        Semantic type info when a field name is recognized.
    """
    normalized = name.lower()
    if normalized == "edge_id" or normalized.endswith("_edge_id"):
        return EDGE_ID_TYPE_INFO
    if normalized == "span_id" or normalized.endswith("_span_id"):
        return SPAN_ID_TYPE_INFO
    if normalized in {"error_id", "missing_id", "capture_id"}:
        return SPAN_ID_TYPE_INFO
    if normalized == "node_id" or normalized.endswith("_node_id"):
        return NODE_ID_TYPE_INFO
    if normalized in {"parent_id", "child_id"}:
        return NODE_ID_TYPE_INFO
    return None


def apply_semantic_types(schema: pa.Schema) -> pa.Schema:
    """Return a schema with semantic extension types and metadata applied.

    Returns
    -------
    pyarrow.Schema
        Schema with semantic types applied to matching fields.
    """
    fields = [_apply_semantic_field(field) for field in schema]
    return pa.schema(fields, metadata=schema.metadata)


def _apply_semantic_field(field: pa.Field) -> pa.Field:
    data_type = _apply_semantic_type(field.type)
    metadata = dict(field.metadata or {})
    semantic = semantic_type_for_field_name(field.name)
    if semantic is NODE_ID_TYPE_INFO:
        metadata.update(node_id_metadata())
        data_type = node_id_type()
    elif semantic is EDGE_ID_TYPE_INFO:
        metadata.update(edge_id_metadata())
        data_type = edge_id_type()
    elif semantic is SPAN_ID_TYPE_INFO:
        metadata.update(span_id_metadata())
        data_type = span_id_type()
    return pa.field(
        field.name,
        data_type,
        nullable=field.nullable,
        metadata=metadata or None,
    )


def _apply_semantic_type(data_type: pa.DataType) -> pa.DataType:
    if isinstance(data_type, pa.ExtensionType):
        return data_type
    if pa.types.is_struct(data_type):
        return pa.struct([_apply_semantic_field(field) for field in data_type])
    if pa.types.is_list(data_type):
        value_field = _apply_semantic_field(data_type.value_field)
        return pa.list_(value_field)
    if pa.types.is_large_list(data_type):
        value_field = _apply_semantic_field(data_type.value_field)
        return pa.large_list(value_field)
    return data_type


__all__ = [
    "BYTE_SPAN_STORAGE",
    "BYTE_SPAN_TYPE_INFO",
    "EDGE_ID_TYPE_INFO",
    "NODE_ID_TYPE_INFO",
    "SEMANTIC_TYPE_META",
    "SPAN_ID_TYPE_INFO",
    "SPAN_STORAGE",
    "SPAN_TYPE_INFO",
    "apply_semantic_types",
    "byte_span_type",
    "edge_id_metadata",
    "edge_id_type",
    "node_id_metadata",
    "node_id_type",
    "register_semantic_extension_types",
    "semantic_type_for_field_name",
    "semantic_type_metadata",
    "span_id_metadata",
    "span_id_type",
    "span_metadata",
    "span_type",
]
