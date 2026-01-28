"""Schema policy helpers for alignment, encoding, and validation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from datafusion_engine.arrow_interop import DataTypeLike, SchemaLike, TableLike
from datafusion_engine.arrow_schema.encoding import EncodingPolicy
from datafusion_engine.arrow_schema.metadata import SchemaMetadataSpec, encoding_policy_from_spec
from datafusion_engine.encoding import apply_encoding
from datafusion_engine.schema_alignment import AlignmentInfo, CastErrorPolicy, SchemaTransform
from datafusion_engine.schema_validation import ArrowValidationOptions


class _ArrowFieldSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def dtype(self) -> DataTypeLike: ...

    @property
    def nullable(self) -> bool: ...

    @property
    def metadata(self) -> Mapping[str, str]: ...

    @property
    def encoding(self) -> str | None: ...


class _TableSchemaSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def fields(self) -> Sequence[_ArrowFieldSpec]: ...

    @property
    def key_fields(self) -> Sequence[str]: ...

    @property
    def required_non_null(self) -> Sequence[str]: ...

    def to_arrow_schema(self) -> SchemaLike: ...


@dataclass(frozen=True)
class SchemaPolicy:
    """Unified schema policy for alignment, encoding, and validation."""

    schema: SchemaLike
    encoding: EncodingPolicy | None = None
    metadata: SchemaMetadataSpec | None = None
    validation: ArrowValidationOptions | None = None
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: CastErrorPolicy = "unsafe"

    def resolved_schema(self) -> SchemaLike:
        """Return the schema with metadata applied.

        Returns
        -------
        SchemaLike
            Schema with metadata applied.
        """
        schema = self.schema
        if self.metadata is not None:
            schema = self.metadata.apply(schema)
        return schema

    def transform(self) -> SchemaTransform:
        """Return a SchemaTransform for the policy.

        Returns
        -------
        SchemaTransform
            Schema transform for alignment/casting.
        """
        return SchemaTransform(
            schema=self.resolved_schema(),
            safe_cast=self.safe_cast,
            keep_extra_columns=self.keep_extra_columns,
            on_error=self.on_error,
        )

    def apply(self, table: TableLike) -> TableLike:
        """Align and optionally encode a table.

        Returns
        -------
        TableLike
            Aligned (and encoded) table.
        """
        aligned = self.transform().apply(table)
        if self.encoding is None:
            return aligned
        return apply_encoding(aligned, policy=self.encoding)

    def apply_with_info(self, table: TableLike) -> tuple[TableLike, AlignmentInfo]:
        """Align and optionally encode a table, returning alignment metadata.

        Returns
        -------
        tuple[TableLike, AlignmentInfo]
            Aligned (and encoded) table and alignment info.
        """
        aligned, info = self.transform().apply_with_info(table)
        if self.encoding is None:
            return aligned, info
        return apply_encoding(aligned, policy=self.encoding), info


@dataclass(frozen=True)
class SchemaPolicyOptions:
    """Optional overrides for schema policy factory construction."""

    schema: SchemaLike | None = None
    encoding: EncodingPolicy | None = None
    metadata: SchemaMetadataSpec | None = None
    validation: ArrowValidationOptions | None = None
    safe_cast: bool | None = None
    keep_extra_columns: bool | None = None
    on_error: CastErrorPolicy | None = None


def schema_policy_factory(
    spec: _TableSchemaSpec,
    *,
    options: SchemaPolicyOptions | None = None,
) -> SchemaPolicy:
    """Return a schema policy derived from a table spec.

    Returns
    -------
    SchemaPolicy
        Schema policy with defaults applied.
    """
    options = options or SchemaPolicyOptions()
    resolved_schema = options.schema or spec.to_arrow_schema()
    resolved_encoding = options.encoding or encoding_policy_from_spec(spec)
    if resolved_encoding is not None and not resolved_encoding.dictionary_cols:
        resolved_encoding = None
    resolved_safe_cast = True if options.safe_cast is None else options.safe_cast
    resolved_keep_extra = (
        False if options.keep_extra_columns is None else options.keep_extra_columns
    )
    resolved_on_error = (
        options.on_error
        if options.on_error is not None
        else ("unsafe" if resolved_safe_cast else "raise")
    )
    return SchemaPolicy(
        schema=resolved_schema,
        encoding=resolved_encoding,
        metadata=options.metadata,
        validation=options.validation,
        safe_cast=resolved_safe_cast,
        keep_extra_columns=resolved_keep_extra,
        on_error=resolved_on_error,
    )


__all__ = ["SchemaPolicy", "SchemaPolicyOptions", "schema_policy_factory"]
