"""Scan default and policy helpers for dataset registration."""

from __future__ import annotations

from dataclasses import dataclass

import msgspec
import pyarrow as pa

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
    resolve_dataset_schema,
)
from schema_spec.dataset_spec import (
    DataFusionScanOptions,
    ParquetColumnOptions,
)

DEFAULT_CACHE_MAX_COLUMNS = 64

CST_EXTERNAL_TABLE_NAME = "libcst_files_v1"
CST_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
CST_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)
CST_PROJECTION_EXPRS: tuple[str, ...] = (
    "repo",
    "path",
    "file_id",
    "nodes",
    "edges",
    "parse_manifest",
    "parse_errors",
    "refs",
    "imports",
    "callsites",
    "defs",
    "type_exprs",
    "docstrings",
    "decorators",
    "call_args",
    "attrs",
)
CST_PARQUET_COLUMN_OPTIONS = ParquetColumnOptions(statistics_enabled=("file_id", "path", "repo"))
AST_EXTERNAL_TABLE_NAME = "ast_files_v1"
AST_PARTITION_FIELDS: tuple[str, ...] = ("repo", "path")
AST_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("repo", "ascending"),
    ("path", "ascending"),
)
SYMTABLE_EXTERNAL_TABLE_NAME = "symtable_files_v1"
SYMTABLE_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
SYMTABLE_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)
BYTECODE_EXTERNAL_TABLE_NAME = "bytecode_files_v1"
BYTECODE_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
BYTECODE_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)
TREE_SITTER_EXTERNAL_TABLE_NAME = "tree_sitter_files_v1"
TREE_SITTER_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
TREE_SITTER_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)


@dataclass(frozen=True)
class ScanDefaults:
    partition_fields: tuple[str, ...]
    file_sort_order: tuple[tuple[str, str], ...]
    infer_partitions: bool
    cache_ttl: str
    listing_mutable: bool
    projection_exprs: tuple[str, ...]
    parquet_column_options: ParquetColumnOptions | None
    collect_statistics: bool = False


DEFAULT_SCAN_CONFIGS: dict[str, ScanDefaults] = {
    CST_EXTERNAL_TABLE_NAME: ScanDefaults(
        partition_fields=CST_PARTITION_FIELDS,
        file_sort_order=CST_FILE_SORT_ORDER,
        infer_partitions=True,
        cache_ttl="2m",
        listing_mutable=True,
        projection_exprs=CST_PROJECTION_EXPRS,
        parquet_column_options=CST_PARQUET_COLUMN_OPTIONS,
    ),
    AST_EXTERNAL_TABLE_NAME: ScanDefaults(
        partition_fields=AST_PARTITION_FIELDS,
        file_sort_order=AST_FILE_SORT_ORDER,
        infer_partitions=True,
        cache_ttl="2m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
    ),
    BYTECODE_EXTERNAL_TABLE_NAME: ScanDefaults(
        partition_fields=BYTECODE_PARTITION_FIELDS,
        file_sort_order=BYTECODE_FILE_SORT_ORDER,
        infer_partitions=True,
        cache_ttl="5m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
    ),
    TREE_SITTER_EXTERNAL_TABLE_NAME: ScanDefaults(
        partition_fields=TREE_SITTER_PARTITION_FIELDS,
        file_sort_order=TREE_SITTER_FILE_SORT_ORDER,
        infer_partitions=False,
        cache_ttl="1m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
        collect_statistics=True,
    ),
    SYMTABLE_EXTERNAL_TABLE_NAME: ScanDefaults(
        partition_fields=SYMTABLE_PARTITION_FIELDS,
        file_sort_order=SYMTABLE_FILE_SORT_ORDER,
        infer_partitions=False,
        cache_ttl="1m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
    ),
}


def _schema_field_type(schema: SchemaLike, field: str) -> pa.DataType | None:
    """Return the Arrow type for a schema field, when available."""
    if field not in schema.names:
        return None
    return schema.field(field).type


def default_scan_options_for_dataset(
    name: str,
    *,
    schema: SchemaLike | None,
) -> DataFusionScanOptions | None:
    """Return default DataFusion scan options for supported datasets."""
    defaults = DEFAULT_SCAN_CONFIGS.get(name)
    if defaults is None or schema is None:
        return None
    partition_cols: list[tuple[str, pa.DataType]] = []
    for field_name in defaults.partition_fields:
        dtype = _schema_field_type(schema, field_name)
        if dtype is not None:
            partition_cols.append((field_name, dtype))
    file_sort_order = tuple(
        (field, order) for field, order in defaults.file_sort_order if field in schema.names
    )
    return DataFusionScanOptions(
        partition_cols=tuple(partition_cols),
        file_sort_order=file_sort_order,
        file_extension=None,
        parquet_pruning=True,
        skip_metadata=True,
        collect_statistics=defaults.collect_statistics,
        listing_table_factory_infer_partitions=defaults.infer_partitions,
        list_files_cache_limit=str(64 * 1024 * 1024),
        list_files_cache_ttl=defaults.cache_ttl,
        projection_exprs=defaults.projection_exprs,
        parquet_column_options=defaults.parquet_column_options,
        listing_mutable=defaults.listing_mutable,
    )


def apply_scan_defaults(name: str, location: DatasetLocation) -> DatasetLocation:
    """Attach default scan options to a dataset location.

    Returns:
    -------
    DatasetLocation
        Dataset location with inferred scan options applied when available.
    """
    updated = location
    if location.resolved.datafusion_scan is not None:
        return updated
    schema: SchemaLike | None = None
    try:
        schema = resolve_dataset_schema(location)
    except (RuntimeError, TypeError, ValueError):
        schema = None
    defaults = default_scan_options_for_dataset(name, schema=schema)
    if defaults is None:
        return updated
    overrides = updated.overrides
    if overrides is None:
        overrides = DatasetLocationOverrides(datafusion_scan=defaults)
    else:
        overrides = msgspec.structs.replace(overrides, datafusion_scan=defaults)
    return msgspec.structs.replace(updated, overrides=overrides)


__all__ = [
    "BYTECODE_EXTERNAL_TABLE_NAME",
    "DEFAULT_CACHE_MAX_COLUMNS",
    "apply_scan_defaults",
    "default_scan_options_for_dataset",
]
