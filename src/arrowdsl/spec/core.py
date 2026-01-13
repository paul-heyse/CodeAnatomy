"""Arrow-native spec table helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaMetadataSpec, SchemaTransform
from arrowdsl.schema.validation import ArrowValidationOptions, ValidationReport, validate_table
from arrowdsl.spec.tables.base import SpecTableCodec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import table_spec_from_schema


@dataclass(frozen=True)
class SpecTableSpec:
    """Schema definition and constraints for a spec table."""

    name: str
    schema: SchemaLike
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()
    metadata_spec: SchemaMetadataSpec = field(default_factory=SchemaMetadataSpec)

    def schema_with_metadata(self) -> SchemaLike:
        """Return the schema with metadata applied.

        Returns
        -------
        SchemaLike
            Schema with metadata merged in.
        """
        return self.metadata_spec.apply(self.schema)

    def table_spec(self) -> TableSchemaSpec:
        """Return a TableSchemaSpec with constraints applied.

        Returns
        -------
        TableSchemaSpec
            Table schema spec for validation.
        """
        spec = table_spec_from_schema(self.name, self.schema)
        return spec.with_constraints(
            required_non_null=self.required_non_null,
            key_fields=self.key_fields,
        )

    def align(self, table: TableLike) -> TableLike:
        """Align a table to the spec schema.

        Returns
        -------
        TableLike
            Table aligned to the schema.
        """
        transform = SchemaTransform(
            schema=self.schema_with_metadata(),
            safe_cast=True,
            keep_extra_columns=False,
            on_error="unsafe",
        )
        return transform.apply(table)

    def codec[SpecT](
        self,
        *,
        encode_row: Callable[[SpecT], dict[str, object]],
        decode_row: Callable[[Mapping[str, object]], SpecT],
        sort_keys: tuple[str, ...] = (),
    ) -> SpecTableCodec[SpecT]:
        """Return a SpecTableCodec for the schema.

        Returns
        -------
        SpecTableCodec[SpecT]
            Codec for encoding/decoding spec tables.
        """
        return SpecTableCodec(
            schema=self.schema_with_metadata(),
            spec=self,
            encode_row=encode_row,
            decode_row=decode_row,
            sort_keys=sort_keys,
        )

    def validate(
        self,
        table: TableLike,
        *,
        ctx: ExecutionContext,
        options: ArrowValidationOptions | None = None,
    ) -> ValidationReport:
        """Validate a table with Arrow-native constraints.

        Returns
        -------
        ValidationReport
            Validation report with errors and stats.
        """
        options = options or ArrowValidationOptions.from_policy(ctx.schema_validation)
        return validate_table(table, spec=self.table_spec(), options=options, ctx=ctx)


__all__ = ["SpecTableSpec"]
