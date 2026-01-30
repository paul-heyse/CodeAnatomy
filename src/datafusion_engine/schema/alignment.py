"""DataFusion-backed schema alignment and evolution helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, TypedDict

import pyarrow as pa

from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from utils.validation import find_missing

type CastErrorPolicy = Literal["unsafe", "keep", "raise"]


class AlignmentInfo(TypedDict):
    """Alignment metadata for schema casting and column selection."""

    input_cols: list[str]
    input_rows: int
    missing_cols: list[str]
    dropped_cols: list[str]
    casted_cols: list[str]
    output_rows: int


def align_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool,
    on_error: CastErrorPolicy = "unsafe",
    keep_extra_columns: bool = False,
) -> tuple[TableLike, AlignmentInfo]:
    """Align and cast a table to a target schema.

    Raises
    ------
    ValueError
        Raised when unsupported alignment options are requested.

    Returns
    -------
    tuple[TableLike, AlignmentInfo]
        Aligned table and alignment metadata.
    """
    if on_error == "keep":
        msg = "on_error='keep' is not supported by DataFusion alignment."
        raise ValueError(msg)
    _ = safe_cast
    resolved_schema = pa.schema(schema)
    resolved_table = to_arrow_table(table)
    input_cols = list(resolved_table.column_names)
    target_names = [field.name for field in resolved_schema]
    missing = find_missing(target_names, set(input_cols))
    extra = [name for name in input_cols if name not in target_names]
    casted = [
        name
        for name in target_names
        if name in input_cols
        and resolved_table.schema.field(name).type != resolved_schema.field(name).type
    ]
    from datafusion_engine.session.runtime import align_table_to_schema

    aligned = align_table_to_schema(
        resolved_table,
        schema=resolved_schema,
        keep_extra_columns=keep_extra_columns,
    )
    info: AlignmentInfo = {
        "input_cols": input_cols,
        "input_rows": int(resolved_table.num_rows),
        "missing_cols": missing,
        "dropped_cols": extra,
        "casted_cols": casted,
        "output_rows": int(aligned.num_rows),
    }
    return aligned, info


def align_table(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool = True,
    keep_extra_columns: bool = False,
    on_error: CastErrorPolicy = "unsafe",
) -> TableLike:
    """Return a table aligned to the target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    aligned, _ = align_to_schema(
        table,
        schema=schema,
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra_columns,
        on_error=on_error,
    )
    return aligned


def align_table_to_contract(
    table: TableLike,
    *,
    contract: object,
    safe_cast: bool = True,
    on_error: CastErrorPolicy = "unsafe",
) -> TableLike:
    """Align a table to a SchemaContract.

    Returns
    -------
    TableLike
        Table aligned to the contract schema.

    Raises
    ------
    TypeError
        Raised when the contract is not a SchemaContract.
    """
    from datafusion_engine.schema.contracts import EvolutionPolicy, SchemaContract

    if not isinstance(contract, SchemaContract):
        msg = "align_table_to_contract requires a SchemaContract."
        raise TypeError(msg)
    keep_extra = contract.evolution_policy != EvolutionPolicy.STRICT
    return align_table(
        table,
        schema=contract.to_arrow_schema(),
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra,
        on_error=on_error,
    )


@dataclass(frozen=True)
class SchemaTransform:
    """Schema alignment transform using DataFusion alignment."""

    schema: SchemaLike
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: CastErrorPolicy = "unsafe"

    def apply(self, table: TableLike) -> TableLike:
        """Align a table to the stored schema.

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
class SchemaEvolutionSpec:
    """Unify/cast/concat policy for evolving schemas."""

    promote_options: str = "permissive"
    rename_map: Mapping[str, str] = field(default_factory=dict)
    allow_missing: bool = False
    allow_extra: bool = True
    allow_casts: bool = True

    def unify_schema(self, tables: Sequence[TableLike]) -> SchemaLike:
        """Return a unified schema for the provided tables.

        Returns
        -------
        SchemaLike
            Unified schema for the tables.
        """
        schemas = [table.schema for table in tables]
        return unify_schemas_core(schemas, promote_options=self.promote_options)

    def unify_schema_from_schemas(self, schemas: Sequence[SchemaLike]) -> SchemaLike:
        """Return a unified schema for a sequence of schemas.

        Returns
        -------
        SchemaLike
            Unified schema for the schemas.
        """
        return unify_schemas_core(schemas, promote_options=self.promote_options)

    def resolve_name(self, name: str) -> str:
        """Return the logical name for a physical column.

        Returns
        -------
        str
            Logical column name after applying rename mapping.
        """
        return self.rename_map.get(name, name)

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
            Concatenated table with unified schema.
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
        return pa.concat_tables(aligned, promote_options=self.promote_options)


def unify_schemas_core(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    """Return a unified schema using Arrow evolution rules.

    Returns
    -------
    SchemaLike
        Unified schema derived from the input schemas.
    """
    if not schemas:
        return pa.schema([])
    try:
        return pa.unify_schemas(list(schemas), promote_options=promote_options)
    except TypeError:
        return pa.unify_schemas(list(schemas))


__all__ = [
    "AlignmentInfo",
    "CastErrorPolicy",
    "SchemaEvolutionSpec",
    "SchemaTransform",
    "align_table",
    "align_table_to_contract",
    "align_to_schema",
    "unify_schemas_core",
]
