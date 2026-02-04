"""Protocol definitions for schema specification helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import DataTypeLike, SchemaLike
    from schema_spec.arrow_types import ArrowTypeSpec


class ArrowFieldSpec(Protocol):
    """Protocol for Arrow field specifications."""

    @property
    def name(self) -> str:
        """Field name."""
        ...

    @property
    def dtype(self) -> DataTypeLike | ArrowTypeSpec:
        """Arrow-compatible data type."""
        ...

    @property
    def nullable(self) -> bool:
        """Whether the field allows null values."""
        ...

    @property
    def metadata(self) -> Mapping[str, str]:
        """Field-level metadata mapping."""
        ...

    @property
    def encoding(self) -> str | None:
        """Optional encoding hint for the field."""
        ...


class TableSchemaSpec(Protocol):
    """Protocol for table schema specifications."""

    @property
    def name(self) -> str:
        """Table name."""
        ...

    @property
    def fields(self) -> Sequence[ArrowFieldSpec]:
        """Schema fields in declared order."""
        ...

    @property
    def key_fields(self) -> Sequence[str]:
        """Primary key field names."""
        ...

    @property
    def required_non_null(self) -> Sequence[str]:
        """Field names that must be non-null."""
        ...

    def to_arrow_schema(self) -> SchemaLike:
        """Return the schema as a PyArrow schema."""
        ...


__all__ = ["ArrowFieldSpec", "TableSchemaSpec"]
