"""Canonical dataset-contract types shared across schema-spec modules."""

from __future__ import annotations

from typing import Literal

import pyarrow as pa

from datafusion_engine.kernels import DedupeSpec, SortKey
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.arrow_types import (
    ArrowTypeBase,
    ArrowTypeSpec,
    arrow_type_from_pyarrow,
    arrow_type_to_pyarrow,
)
from serde_msgspec import StructBaseStrict


class SortKeySpec(StructBaseStrict, frozen=True):
    """Sort key specification."""

    column: str
    order: Literal["ascending", "descending"] = "ascending"

    def to_sort_key(self) -> SortKey:
        """Convert to ``SortKey``.

        Returns:
        -------
        SortKey
            Normalized sort-key value.
        """
        return SortKey(column=self.column, order=self.order)


class DedupeSpecSpec(StructBaseStrict, frozen=True):
    """Dedupe specification."""

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKeySpec, ...] = ()
    strategy: Literal[
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    ] = "KEEP_FIRST_AFTER_SORT"

    def to_dedupe_spec(self) -> DedupeSpec:
        """Convert to ``DedupeSpec``.

        Returns:
        -------
        DedupeSpec
            Normalized dedupe specification.
        """
        return DedupeSpec(
            keys=self.keys,
            tie_breakers=tuple(tb.to_sort_key() for tb in self.tie_breakers),
            strategy=self.strategy,
        )


class ContractRow(StructBaseStrict, frozen=True):
    """Lightweight contract configuration for dataset rows."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None
    constraints: tuple[str, ...] = ()
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None


class TableSchemaContract(StructBaseStrict, frozen=True):
    """Combine file and partition schema into a table schema contract."""

    file_schema: pa.Schema
    partition_cols: tuple[tuple[str, ArrowTypeSpec], ...] = ()

    def __post_init__(self) -> None:
        """Normalize partition column types into ``ArrowTypeSpec``."""
        if not self.partition_cols:
            return
        normalized = tuple(
            (
                name,
                arrow_type_from_pyarrow(dtype) if isinstance(dtype, pa.DataType) else dtype,
            )
            for name, dtype in self.partition_cols
        )
        if normalized != self.partition_cols:
            object.__setattr__(self, "partition_cols", normalized)

    def partition_schema(self) -> pa.Schema | None:
        """Return the partition schema when partition columns are present."""
        if not self.partition_cols:
            return None
        fields = [
            pa.field(
                name,
                arrow_type_to_pyarrow(dtype) if isinstance(dtype, ArrowTypeBase) else dtype,
                nullable=False,
            )
            for name, dtype in self.partition_cols
        ]
        return pa.schema(fields)

    def partition_cols_pyarrow(self) -> tuple[tuple[str, pa.DataType], ...]:
        """Return partition columns as pyarrow data types."""
        return tuple(
            (
                name,
                arrow_type_to_pyarrow(dtype) if isinstance(dtype, ArrowTypeBase) else dtype,
            )
            for name, dtype in self.partition_cols
        )


__all__ = [
    "ContractRow",
    "DedupeSpecSpec",
    "SortKeySpec",
    "TableSchemaContract",
]
