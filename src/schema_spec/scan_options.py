"""Canonical scan option types for schema specifications."""

from __future__ import annotations

import pyarrow as pa

from schema_spec.arrow_types import (
    ArrowTypeBase,
    ArrowTypeSpec,
    arrow_type_from_pyarrow,
    arrow_type_to_pyarrow,
)
from schema_spec.dataset_contracts import TableSchemaContract
from serde_msgspec import StructBaseStrict


class ParquetColumnOptions(StructBaseStrict, frozen=True):
    """Per-column Parquet scan options."""

    statistics_enabled: tuple[str, ...] = ()
    bloom_filter_enabled: tuple[str, ...] = ()
    dictionary_enabled: tuple[str, ...] = ()

    def external_table_options(self) -> dict[str, str]:
        """Return DataFusion external table options for column settings."""
        options: dict[str, str] = {}
        if self.statistics_enabled:
            options["statistics_enabled"] = ",".join(self.statistics_enabled)
        if self.bloom_filter_enabled:
            options["bloom_filter_enabled"] = ",".join(self.bloom_filter_enabled)
        if self.dictionary_enabled:
            options["dictionary_enabled"] = ",".join(self.dictionary_enabled)
        return options


class DataFusionScanOptions(StructBaseStrict, frozen=True):
    """DataFusion-specific scan configuration."""

    partition_cols: tuple[tuple[str, ArrowTypeSpec], ...] = ()
    file_sort_order: tuple[tuple[str, str], ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = True
    skip_arrow_metadata: bool | None = None
    binary_as_string: bool | None = None
    schema_force_view_types: bool | None = None
    listing_table_factory_infer_partitions: bool | None = None
    listing_table_ignore_subdirectory: bool | None = None
    file_extension: str | None = None
    cache: bool = False
    collect_statistics: bool | None = None
    meta_fetch_concurrency: int | None = None
    list_files_cache_ttl: str | None = None
    list_files_cache_limit: str | None = None
    projection_exprs: tuple[str, ...] = ()
    parquet_column_options: ParquetColumnOptions | None = None
    listing_mutable: bool = False
    unbounded: bool = False
    table_schema_contract: TableSchemaContract | None = None
    expr_adapter_factory: object | None = None

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

    def partition_cols_pyarrow(self) -> tuple[tuple[str, pa.DataType], ...]:
        """Return partition columns as pyarrow data types."""
        return tuple(
            (
                name,
                arrow_type_to_pyarrow(dtype) if isinstance(dtype, ArrowTypeBase) else dtype,
            )
            for name, dtype in self.partition_cols
        )


class DeltaScanOptions(StructBaseStrict, frozen=True):
    """Delta-specific scan configuration."""

    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool = False
    schema: pa.Schema | None = None


__all__ = [
    "DataFusionScanOptions",
    "DeltaScanOptions",
    "ParquetColumnOptions",
]
