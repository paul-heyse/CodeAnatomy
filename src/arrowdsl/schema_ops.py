"""Schema transformation helpers built on Arrow alignment."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import FieldLike, SchemaLike, TableLike
from arrowdsl.schema import AlignmentInfo, CastErrorPolicy, align_to_schema


@dataclass(frozen=True)
class SchemaTransform:
    """Schema alignment transform using shared alignment utilities.

    Parameters
    ----------
    schema:
        Target schema.
    safe_cast:
        When ``True``, allow safe casts only.
    keep_extra_columns:
        When ``True``, retain extra columns after alignment.
    """

    schema: SchemaLike
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: CastErrorPolicy = "unsafe"

    def apply(self, table: TableLike) -> TableLike:
        """Align a table to the stored schema.

        Parameters
        ----------
        table:
            Input table.

        Returns
        -------
        TableLike
            Aligned table.
        """
        aligned, _ = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned

    def apply_with_info(self, table: TableLike) -> tuple[TableLike, AlignmentInfo]:
        """Align a table to the stored schema and return alignment info.

        Parameters
        ----------
        table:
            Input table.

        Returns
        -------
        tuple[TableLike, AlignmentInfo]
            Aligned table and alignment metadata.
        """
        aligned, info = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned, info


@dataclass(frozen=True)
class SchemaMetadataSpec:
    """Schema metadata mutation policy."""

    schema_metadata: dict[bytes, bytes] = field(default_factory=dict)
    field_metadata: dict[str, dict[bytes, bytes]] = field(default_factory=dict)

    def apply(self, schema: SchemaLike) -> SchemaLike:
        """Return a schema with metadata updates applied.

        Returns
        -------
        SchemaLike
            Schema with merged schema/field metadata.
        """
        fields: list[FieldLike] = []
        for schema_field in schema:
            meta = self.field_metadata.get(schema_field.name)
            if meta is None:
                fields.append(schema_field)
                continue
            merged = dict(schema_field.metadata or {})
            merged.update(meta)
            fields.append(schema_field.with_metadata(merged))

        updated = pa.schema(fields)
        if self.schema_metadata:
            meta = dict(schema.metadata or {})
            meta.update(self.schema_metadata)
            return updated.with_metadata(meta)
        if schema.metadata is None:
            return updated
        return updated.with_metadata(schema.metadata)


@dataclass(frozen=True)
class SchemaEvolutionSpec:
    """Unify/cast/concat policy for evolving schemas."""

    promote_options: str = "permissive"

    def unify_schema(self, tables: Sequence[TableLike]) -> SchemaLike:
        """Return a unified schema for the provided tables.

        Returns
        -------
        SchemaLike
            Unified schema for the table set.
        """
        schemas = [table.schema for table in tables]
        if not schemas:
            return pa.schema([])
        try:
            return pa.unify_schemas(list(schemas), promote_options=self.promote_options)
        except TypeError:
            return pa.unify_schemas(list(schemas))

    def unify_and_cast(
        self,
        tables: Sequence[TableLike],
        *,
        safe_cast: bool = True,
        on_error: CastErrorPolicy = "unsafe",
        keep_extra_columns: bool = False,
    ) -> TableLike:
        """Unify schemas, align tables, and concatenate.

        Returns
        -------
        TableLike
            Concatenated table aligned to the unified schema.
        """
        if not tables:
            return pa.Table.from_arrays([], schema=pa.schema([]))
        schema = self.unify_schema(tables)
        aligned: list[TableLike] = []
        for table in tables:
            aligned_table, _ = align_to_schema(
                table,
                schema=schema,
                safe_cast=safe_cast,
                on_error=on_error,
                keep_extra_columns=keep_extra_columns,
            )
            aligned.append(aligned_table)
        return pa.concat_tables(aligned, promote=True)
