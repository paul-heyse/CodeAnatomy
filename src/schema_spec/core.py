"""Core schema specification models."""

from __future__ import annotations

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


def _field_metadata(metadata: dict[str, str]) -> dict[bytes, bytes]:
    """Encode metadata keys/values for Arrow.

    Returns
    -------
    dict[bytes, bytes]
        Encoded metadata mapping.
    """
    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in metadata.items()}


class ArrowFieldSpec(BaseModel):
    """Specification for a single Arrow field."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    dtype: pa.DataType
    nullable: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)

    def to_arrow_field(self) -> pa.Field:
        """Build a pyarrow.Field from the spec.

        Returns
        -------
        pyarrow.Field
            Arrow field instance.
        """
        metadata = _field_metadata(self.metadata)
        return pa.field(self.name, self.dtype, nullable=self.nullable, metadata=metadata)


class TableSchemaSpec(BaseModel):
    """Specification for a table schema and associated constraints."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    fields: list[ArrowFieldSpec]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()

    @field_validator("fields")
    @classmethod
    def _unique_field_names(cls, fields: list[ArrowFieldSpec]) -> list[ArrowFieldSpec]:
        seen: set[str] = set()
        dupes: list[str] = []
        for field in fields:
            if field.name in seen:
                dupes.append(field.name)
            else:
                seen.add(field.name)
        if dupes:
            msg = f"duplicate field names: {sorted(set(dupes))}"
            raise ValueError(msg)
        return fields

    @field_validator("required_non_null", "key_fields")
    @classmethod
    def _validate_field_refs(
        cls,
        values: tuple[str, ...],
        info: ValidationInfo,
    ) -> tuple[str, ...]:
        field_names = {field.name for field in info.data["fields"]}
        missing = [name for name in values if name not in field_names]
        if missing:
            msg = f"unknown fields: {missing}"
            raise ValueError(msg)
        return values

    def to_arrow_schema(self) -> pa.Schema:
        """Build a pyarrow.Schema from the spec.

        Returns
        -------
        pyarrow.Schema
            Arrow schema instance.
        """
        return pa.schema([field.to_arrow_field() for field in self.fields])
