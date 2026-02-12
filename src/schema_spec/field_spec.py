"""Canonical field specification for Arrow schemas."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from datafusion_engine.arrow import interop
from datafusion_engine.arrow.interop import FieldLike
from datafusion_engine.arrow.metadata import ENCODING_META
from schema_spec.arrow_types import ArrowTypeBase, ArrowTypeSpec, arrow_type_to_pyarrow
from serde_msgspec import StructBaseStrict


def _encode_metadata(metadata: Mapping[str, str]) -> dict[bytes, bytes]:
    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in metadata.items()}


class FieldSpec(StructBaseStrict, frozen=True):
    """Specification for a single Arrow field."""

    name: str
    dtype: ArrowTypeSpec
    nullable: bool = True
    metadata: dict[str, str] = msgspec.field(default_factory=dict)
    default_value: str | None = None
    encoding: Literal["dictionary"] | None = None

    def __post_init__(self) -> None:
        """Validate dtype is a serializable ArrowTypeSpec.

        Raises:
            TypeError: If the operation cannot be completed.
        """
        if not isinstance(self.dtype, ArrowTypeBase):
            msg = f"FieldSpec.dtype must be ArrowTypeSpec, got {type(self.dtype).__name__}"
            raise TypeError(msg)

    def to_arrow_field(self) -> FieldLike:
        """Build a pyarrow.Field from the spec.

        Returns:
        -------
        pyarrow.Field
            Arrow field instance.
        """
        metadata = dict(self.metadata)
        if self.default_value is not None:
            metadata.setdefault("default_value", self.default_value)
        if self.encoding is not None:
            metadata[ENCODING_META] = self.encoding
        encoded = _encode_metadata(metadata)
        dtype = (
            arrow_type_to_pyarrow(self.dtype)
            if isinstance(self.dtype, ArrowTypeBase)
            else self.dtype
        )
        return interop.field(self.name, dtype, nullable=self.nullable, metadata=encoded)


__all__ = ["FieldSpec"]
