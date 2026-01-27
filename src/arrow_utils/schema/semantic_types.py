"""Arrow extension and metadata helpers for semantic types."""

from __future__ import annotations

import contextlib
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


class _SemanticExtensionType(pa.ExtensionType):
    def __init__(self, info: SemanticTypeInfo) -> None:
        self._info = info
        super().__init__(info.storage_type, info.extension_name)

    def arrow_ext_serialize(self) -> bytes:
        return self._info.name.encode("utf-8")

    @classmethod
    def arrow_ext_deserialize(
        cls,
        _storage_type: pa.DataType,
        serialized: bytes,
    ) -> pa.ExtensionType:
        name = serialized.decode("utf-8")
        if name == SPAN_TYPE_INFO.name:
            return _SemanticExtensionType(SPAN_TYPE_INFO)
        if name == BYTE_SPAN_TYPE_INFO.name:
            return _SemanticExtensionType(BYTE_SPAN_TYPE_INFO)
        msg = f"Unsupported semantic extension type: {name!r}."
        raise ValueError(msg)


_SemanticExtensionType.__arrow_ext_serialize__ = _SemanticExtensionType.arrow_ext_serialize
_SemanticExtensionType.__arrow_ext_deserialize__ = classmethod(
    _SemanticExtensionType.arrow_ext_deserialize
)


def register_semantic_extension_types() -> None:
    """Register semantic extension types with pyarrow."""
    for info in (SPAN_TYPE_INFO, BYTE_SPAN_TYPE_INFO):
        with contextlib.suppress(ValueError, pa.ArrowKeyError):
            pa.register_extension_type(_SemanticExtensionType(info))


def _extension_type(info: SemanticTypeInfo) -> pa.DataType:
    register_semantic_extension_types()
    return _SemanticExtensionType(info)


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


__all__ = [
    "BYTE_SPAN_STORAGE",
    "BYTE_SPAN_TYPE_INFO",
    "SEMANTIC_TYPE_META",
    "SPAN_STORAGE",
    "SPAN_TYPE_INFO",
    "byte_span_type",
    "register_semantic_extension_types",
    "span_type",
]
