"""Schema policy helpers for alignment, encoding, metadata, and validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.encoding import encoding_policy_from_spec
from arrowdsl.schema.schema import (
    AlignmentInfo,
    CastErrorPolicy,
    EncodingPolicy,
    SchemaMetadataSpec,
    SchemaTransform,
)
from schema_spec.specs import TableSchemaSpec

if TYPE_CHECKING:
    from arrowdsl.schema.validation import ArrowValidationOptions


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
        return self.encoding.apply(aligned)

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
        return self.encoding.apply(aligned), info


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
    spec: TableSchemaSpec,
    *,
    ctx: ExecutionContext,
    options: SchemaPolicyOptions | None = None,
) -> SchemaPolicy:
    """Return a schema policy derived from a table spec and execution context.

    Returns
    -------
    SchemaPolicy
        Schema policy with defaults applied.
    """
    options = options or SchemaPolicyOptions()
    resolved_schema = options.schema or spec.to_arrow_schema()
    resolved_encoding = options.encoding or encoding_policy_from_spec(spec)
    if resolved_encoding is not None and not resolved_encoding.specs:
        resolved_encoding = None
    resolved_safe_cast = ctx.safe_cast if options.safe_cast is None else options.safe_cast
    resolved_keep_extra = (
        ctx.provenance if options.keep_extra_columns is None else options.keep_extra_columns
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
