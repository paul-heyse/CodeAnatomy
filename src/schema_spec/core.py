"""Core schema specification models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import DataTypeLike, FieldLike, SchemaLike
from arrowdsl.schema import CastErrorPolicy
from arrowdsl.schema_ops import SchemaTransform
from schema_spec.metadata import schema_metadata


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
    dtype: DataTypeLike
    nullable: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)

    def to_arrow_field(self) -> FieldLike:
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
    version: int | None = None
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

    def to_arrow_schema(self) -> SchemaLike:
        """Build a pyarrow.Schema from the spec.

        Returns
        -------
        pyarrow.Schema
            Arrow schema instance.
        """
        schema = pa.schema([field.to_arrow_field() for field in self.fields])
        if self.version is None:
            return schema
        meta = dict(schema.metadata or {})
        meta.update(schema_metadata(self.name, self.version))
        return schema.with_metadata(meta)

    def to_transform(
        self,
        *,
        safe_cast: bool = True,
        keep_extra_columns: bool = False,
        on_error: CastErrorPolicy = "unsafe",
    ) -> SchemaTransform:
        """Create a schema transform for aligning tables to this spec.

        Parameters
        ----------
        safe_cast:
            When ``True``, allow safe casts only.
        keep_extra_columns:
            When ``True``, retain extra columns after alignment.
        on_error:
            Behavior when casting fails ("unsafe", "keep", "raise").

        Returns
        -------
        SchemaTransform
            Transform configured for this schema spec.
        """
        return SchemaTransform(
            schema=self.to_arrow_schema(),
            safe_cast=safe_cast,
            keep_extra_columns=keep_extra_columns,
            on_error=on_error,
        )
