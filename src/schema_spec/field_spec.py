"""Canonical field specification for Arrow schemas."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

import datafusion_engine.arrow_interop as interop
from datafusion_engine.arrow_interop import DataTypeLike, FieldLike
from datafusion_engine.arrow_schema.encoding_metadata import ENCODING_META


def _encode_metadata(metadata: Mapping[str, str]) -> dict[bytes, bytes]:
    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in metadata.items()}


@dataclass(frozen=True)
class FieldSpec:
    """Specification for a single Arrow field."""

    name: str
    dtype: DataTypeLike
    nullable: bool = True
    metadata: dict[str, str] = field(default_factory=dict)
    default_value: str | None = None
    encoding: Literal["dictionary"] | None = None

    def to_arrow_field(self) -> FieldLike:
        """Build a pyarrow.Field from the spec.

        Returns
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
        return interop.field(self.name, self.dtype, nullable=self.nullable, metadata=encoded)


__all__ = ["FieldSpec"]
