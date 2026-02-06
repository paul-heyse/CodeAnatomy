"""Serializable Arrow type specifications."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec
import pyarrow as pa

from serde_msgspec import StructBaseStrict


class ArrowTypeBase(StructBaseStrict, frozen=True, tag=True, tag_field="type"):
    """Base tagged Arrow type specification."""


ArrowPrimitiveName = Literal[
    "null",
    "bool",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16",
    "float32",
    "float64",
    "string",
    "large_string",
    "binary",
    "large_binary",
    "date32",
    "date64",
    "time32[ms]",
    "time32[s]",
    "time64[us]",
    "time64[ns]",
]


class ArrowPrimitiveSpec(ArrowTypeBase, tag="primitive", frozen=True):
    """Primitive Arrow type specification."""

    name: ArrowPrimitiveName


class ArrowTimestampSpec(ArrowTypeBase, tag="timestamp", frozen=True):
    """Timestamp Arrow type specification."""

    unit: Literal["s", "ms", "us", "ns"]
    timezone: str | None = None


class ArrowDurationSpec(ArrowTypeBase, tag="duration", frozen=True):
    """Duration Arrow type specification."""

    unit: Literal["s", "ms", "us", "ns"]


class ArrowDecimalSpec(ArrowTypeBase, tag="decimal", frozen=True):
    """Decimal Arrow type specification."""

    precision: int
    scale: int = 0
    bit_width: Literal[128, 256] = 128


class ArrowFixedSizeBinarySpec(ArrowTypeBase, tag="fixed_size_binary", frozen=True):
    """Fixed-size binary Arrow type specification."""

    byte_width: int


class ArrowFieldSpec(StructBaseStrict, frozen=True):
    """Arrow field specification for nested types."""

    name: str
    dtype: ArrowTypeSpec
    nullable: bool = True
    metadata: dict[str, str] = msgspec.field(default_factory=dict)

    @classmethod
    def from_pyarrow(cls, field: pa.Field) -> ArrowFieldSpec:
        """Return an ArrowFieldSpec from a pyarrow.Field.

        Returns:
        -------
        ArrowFieldSpec
            Serializable field specification.
        """
        return cls(
            name=field.name,
            dtype=arrow_type_from_pyarrow(field.type),
            nullable=field.nullable,
            metadata=_decode_metadata(field.metadata),
        )

    def to_pyarrow(self) -> pa.Field:
        """Return a pyarrow.Field for the spec.

        Returns:
        -------
        pyarrow.Field
            PyArrow field definition.
        """
        return pa.field(
            self.name,
            arrow_type_to_pyarrow(self.dtype),
            nullable=self.nullable,
            metadata=_encode_metadata(self.metadata),
        )


class ArrowListSpec(ArrowTypeBase, tag="list", frozen=True):
    """List Arrow type specification."""

    item: ArrowFieldSpec


class ArrowLargeListSpec(ArrowTypeBase, tag="large_list", frozen=True):
    """Large list Arrow type specification."""

    item: ArrowFieldSpec


class ArrowFixedSizeListSpec(ArrowTypeBase, tag="fixed_size_list", frozen=True):
    """Fixed-size list Arrow type specification."""

    item: ArrowFieldSpec
    list_size: int


class ArrowMapSpec(ArrowTypeBase, tag="map", frozen=True):
    """Map Arrow type specification."""

    key: ArrowFieldSpec
    item: ArrowFieldSpec
    keys_sorted: bool = False


class ArrowStructSpec(ArrowTypeBase, tag="struct", frozen=True):
    """Struct Arrow type specification."""

    fields: tuple[ArrowFieldSpec, ...] = ()


class ArrowDictionarySpec(ArrowTypeBase, tag="dictionary", frozen=True):
    """Dictionary-encoded Arrow type specification."""

    index_type: ArrowTypeSpec
    value_type: ArrowTypeSpec
    ordered: bool = False


class ArrowOpaqueSpec(ArrowTypeBase, tag="opaque", frozen=True):
    """Opaque Arrow type fallback for unsupported types."""

    repr: str


type ArrowTypeSpec = (
    ArrowPrimitiveSpec
    | ArrowTimestampSpec
    | ArrowDurationSpec
    | ArrowDecimalSpec
    | ArrowFixedSizeBinarySpec
    | ArrowListSpec
    | ArrowLargeListSpec
    | ArrowFixedSizeListSpec
    | ArrowMapSpec
    | ArrowStructSpec
    | ArrowDictionarySpec
    | ArrowOpaqueSpec
)


_PRIMITIVE_BUILDERS: dict[ArrowPrimitiveName, pa.DataType] = {
    "null": pa.null(),
    "bool": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "time32[s]": pa.time32("s"),
    "time32[ms]": pa.time32("ms"),
    "time64[us]": pa.time64("us"),
    "time64[ns]": pa.time64("ns"),
}


def _decode_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def _encode_metadata(metadata: Mapping[str, str]) -> dict[bytes, bytes]:
    return {str(key).encode("utf-8"): str(value).encode("utf-8") for key, value in metadata.items()}


def arrow_type_from_pyarrow(dtype: pa.DataType) -> ArrowTypeSpec:
    """Return a serializable ArrowTypeSpec from a pyarrow dtype.

    Returns:
    -------
    ArrowTypeSpec
        Serializable Arrow type specification.
    """
    from schema_spec.arrow_type_registry import arrow_type_from_pyarrow as _arrow_type_from_pyarrow

    return _arrow_type_from_pyarrow(dtype)


def arrow_type_to_pyarrow(spec: ArrowTypeSpec | ArrowTypeBase) -> pa.DataType:
    """Return pyarrow dtype for an ArrowTypeSpec.

    Returns:
    -------
    pyarrow.DataType
        PyArrow data type for the spec.
    """
    from schema_spec.arrow_type_registry import arrow_type_to_pyarrow as _arrow_type_to_pyarrow

    return _arrow_type_to_pyarrow(spec)


__all__ = [
    "ArrowDecimalSpec",
    "ArrowDictionarySpec",
    "ArrowDurationSpec",
    "ArrowFieldSpec",
    "ArrowFixedSizeBinarySpec",
    "ArrowFixedSizeListSpec",
    "ArrowLargeListSpec",
    "ArrowListSpec",
    "ArrowMapSpec",
    "ArrowOpaqueSpec",
    "ArrowPrimitiveName",
    "ArrowPrimitiveSpec",
    "ArrowStructSpec",
    "ArrowTimestampSpec",
    "ArrowTypeBase",
    "ArrowTypeSpec",
    "arrow_type_from_pyarrow",
    "arrow_type_to_pyarrow",
]
