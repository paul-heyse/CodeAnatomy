"""Schema system registry, factories, and Arrow validation integration.

This module provides dataset specifications, contracts, and schema discovery
utilities. Schema discovery and validation are sourced from DataFusion's
catalog and information_schema views, making DataFusion the source of truth
for all schema metadata.

IO contracts are specified through DataFusion registration (register_listing_table,
register_object_store). Schema validation and introspection query DataFusion's
information_schema.columns, information_schema.tables, information_schema.table_constraints,
and information_schema.key_column_usage views.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

import msgspec
import pyarrow as pa

from core.config_base import config_fingerprint
from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.extensions.schema_runtime import load_schema_runtime
from datafusion_engine.kernels import DedupeSpec, SortKey
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.arrow_types import (
    ArrowTypeBase,
    ArrowTypeSpec,
    arrow_type_from_pyarrow,
    arrow_type_to_pyarrow,
)
from schema_spec.specs import TableSchemaSpec
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Validate an Arrow table with Arrow-native validation.

    Args:
        table: Arrow table-like object to validate.
        spec: Table schema spec.
        options: Optional Arrow validation options.
        runtime_profile: Optional runtime profile for diagnostics.

    Returns:
        TableLike: Result.

    """
    from schema_spec.validation import validate_arrow_table as _validate_arrow_table

    return _validate_arrow_table(
        table,
        spec=spec,
        options=options,
        runtime_profile=runtime_profile,
    )


class SortKeySpec(StructBaseStrict, frozen=True):
    """Sort key specification."""

    column: str
    order: Literal["ascending", "descending"] = "ascending"

    def to_sort_key(self) -> SortKey:
        """Convert to a SortKey instance.

        Returns:
        -------
        SortKey
            Sort key instance.
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
        """Convert to a DedupeSpec instance.

        Returns:
        -------
        DedupeSpec
            Dedupe spec instance.
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
    """Combine file and partition schema into a TableSchema contract.

    This contract specifies the schema expectations for DataFusion table
    registration. The file_schema represents the data file schema, while
    partition_cols specify partition column metadata used in DataFusion's
    register_listing_table() table_partition_cols parameter.

    Attributes:
    ----------
    file_schema : pa.Schema
        Arrow schema for data files (without partition columns).
    partition_cols : tuple[tuple[str, pa.DataType], ...]
        Partition column specifications (name, dtype pairs).
    """

    file_schema: pa.Schema
    partition_cols: tuple[tuple[str, ArrowTypeSpec], ...] = ()

    def __post_init__(self) -> None:
        """Normalize partition column types into ArrowTypeSpec."""
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
        """Return the partition schema when partition columns are present.

        Returns:
        -------
        pyarrow.Schema | None
            Partition schema when partition columns are defined.
        """
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
        """Return partition columns as pyarrow data types.

        Returns:
        -------
        tuple[tuple[str, pyarrow.DataType], ...]
            Partition columns with PyArrow data types.
        """
        return tuple(
            (
                name,
                arrow_type_to_pyarrow(dtype) if isinstance(dtype, ArrowTypeBase) else dtype,
            )
            for name, dtype in self.partition_cols
        )


class ParquetColumnOptions(StructBaseStrict, frozen=True):
    """Per-column Parquet scan options."""

    statistics_enabled: tuple[str, ...] = ()
    bloom_filter_enabled: tuple[str, ...] = ()
    dictionary_enabled: tuple[str, ...] = ()

    def external_table_options(self) -> dict[str, str]:
        """Return DataFusion external table options for column settings.

        Returns:
        -------
        dict[str, str]
            External table options keyed by option name.
        """
        options: dict[str, str] = {}
        if self.statistics_enabled:
            options["statistics_enabled"] = ",".join(self.statistics_enabled)
        if self.bloom_filter_enabled:
            options["bloom_filter_enabled"] = ",".join(self.bloom_filter_enabled)
        if self.dictionary_enabled:
            options["dictionary_enabled"] = ",".join(self.dictionary_enabled)
        return options


class DataFusionScanOptions(StructBaseStrict, frozen=True):
    """DataFusion-specific scan configuration.

    IO contracts are specified through DataFusion registration and DDL surfaces.
    These options map directly to DataFusion's register_listing_table() parameters
    and CREATE EXTERNAL TABLE options.

    Attributes:
    ----------
    partition_cols : tuple[tuple[str, pa.DataType], ...]
        Partition column specifications for DataFusion registration.
    file_sort_order : tuple[tuple[str, str], ...]
        File-level sort order for DataFusion DDL.
    parquet_pruning : bool
        Enable Parquet predicate pushdown.
    skip_metadata : bool
        Skip Parquet metadata reads for performance.
    skip_arrow_metadata : bool | None
        Skip Arrow schema metadata in Parquet files.
    binary_as_string : bool | None
        Treat binary columns as string in scans.
    schema_force_view_types : bool | None
        Force schema types to match view definitions.
    listing_table_factory_infer_partitions : bool | None
        Infer partition columns from directory structure.
    listing_table_ignore_subdirectory : bool | None
        Ignore subdirectories in listing table scans.
    file_extension : str | None
        File extension filter for listing table scans.
    cache : bool
        Enable DataFusion's table scan caching.
    collect_statistics : bool | None
        Collect table statistics during registration.
    meta_fetch_concurrency : int | None
        Concurrency level for metadata fetching.
    list_files_cache_ttl : str | None
        TTL for listing table file cache.
    list_files_cache_limit : str | None
        Size limit for listing table file cache.
    projection_exprs : tuple[str, ...]
        Projection expressions for scan optimization.
    parquet_column_options : ParquetColumnOptions | None
        Per-column Parquet scan settings.
    listing_mutable : bool
        Mark listing table as mutable for dynamic file discovery.
    unbounded : bool
        Mark table as unbounded/streaming source.
    table_schema_contract : TableSchemaContract | None
        Schema contract for file and partition schemas.
    expr_adapter_factory : object | None
        Custom expression adapter factory for specialized scans.
    """

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
        """Normalize partition column types into ArrowTypeSpec."""
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
        """Return partition columns as pyarrow data types.

        Returns:
        -------
        tuple[tuple[str, pyarrow.DataType], ...]
            Partition columns with PyArrow data types.
        """
        return tuple(
            (
                name,
                arrow_type_to_pyarrow(dtype) if isinstance(dtype, ArrowTypeBase) else dtype,
            )
            for name, dtype in self.partition_cols
        )


class ScanPolicyDefaults(StructBaseStrict, frozen=True):
    """Policy defaults for DataFusion listing scans."""

    collect_statistics: bool | None = None
    list_files_cache_ttl: str | None = None
    list_files_cache_limit: str | None = None
    meta_fetch_concurrency: int | None = None

    def has_any(self) -> bool:
        """Return True when any default is configured.

        Returns:
        -------
        bool
            ``True`` when any default is set.
        """
        return any(
            value is not None
            for value in (
                self.collect_statistics,
                self.list_files_cache_ttl,
                self.list_files_cache_limit,
                self.meta_fetch_concurrency,
            )
        )

    def apply(self, options: DataFusionScanOptions) -> DataFusionScanOptions:
        """Apply defaults to DataFusion scan options.

        Returns:
        -------
        DataFusionScanOptions
            Scan options with policy defaults applied.
        """
        return msgspec.structs.replace(
            options,
            collect_statistics=(
                options.collect_statistics
                if options.collect_statistics is not None
                else self.collect_statistics
            ),
            list_files_cache_ttl=(
                options.list_files_cache_ttl
                if options.list_files_cache_ttl is not None
                else self.list_files_cache_ttl
            ),
            list_files_cache_limit=(
                options.list_files_cache_limit
                if options.list_files_cache_limit is not None
                else self.list_files_cache_limit
            ),
            meta_fetch_concurrency=(
                options.meta_fetch_concurrency
                if options.meta_fetch_concurrency is not None
                else self.meta_fetch_concurrency
            ),
        )

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a fingerprint payload for policy defaults.

        Returns:
        -------
        Mapping[str, object]
            Serializable fingerprint payload.
        """
        return {
            "collect_statistics": self.collect_statistics,
            "list_files_cache_ttl": self.list_files_cache_ttl,
            "list_files_cache_limit": self.list_files_cache_limit,
            "meta_fetch_concurrency": self.meta_fetch_concurrency,
        }


class DeltaScanPolicyDefaults(StructBaseStrict, frozen=True):
    """Policy defaults for Delta scan options."""

    enable_parquet_pushdown: bool | None = None
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool | None = None
    file_column_name: str | None = None

    def has_any(self) -> bool:
        """Return True when any default is configured.

        Returns:
        -------
        bool
            ``True`` when any default is set.
        """
        return any(
            value is not None
            for value in (
                self.enable_parquet_pushdown,
                self.schema_force_view_types,
                self.wrap_partition_values,
                self.file_column_name,
            )
        )

    def apply(self, options: DeltaScanOptions) -> DeltaScanOptions:
        """Apply defaults to Delta scan options.

        Returns:
        -------
        DeltaScanOptions
            Delta scan options with policy defaults applied.
        """
        return msgspec.structs.replace(
            options,
            enable_parquet_pushdown=(
                self.enable_parquet_pushdown
                if self.enable_parquet_pushdown is not None
                else options.enable_parquet_pushdown
            ),
            schema_force_view_types=(
                self.schema_force_view_types
                if self.schema_force_view_types is not None
                else options.schema_force_view_types
            ),
            wrap_partition_values=(
                self.wrap_partition_values
                if self.wrap_partition_values is not None
                else options.wrap_partition_values
            ),
            file_column_name=(
                self.file_column_name
                if self.file_column_name is not None
                else options.file_column_name
            ),
        )

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a fingerprint payload for policy defaults.

        Returns:
        -------
        Mapping[str, object]
            Serializable fingerprint payload.
        """
        return {
            "enable_parquet_pushdown": self.enable_parquet_pushdown,
            "schema_force_view_types": self.schema_force_view_types,
            "wrap_partition_values": self.wrap_partition_values,
            "file_column_name": self.file_column_name,
        }


class ScanPolicyConfig(StructBaseStrict, frozen=True):
    """Policy defaults for scan options by dataset format."""

    listing: ScanPolicyDefaults = msgspec.field(default_factory=ScanPolicyDefaults)
    delta_listing: ScanPolicyDefaults = msgspec.field(default_factory=ScanPolicyDefaults)
    delta_scan: DeltaScanPolicyDefaults = msgspec.field(default_factory=DeltaScanPolicyDefaults)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a fingerprint payload for scan policies.

        Returns:
        -------
        Mapping[str, object]
            Serializable fingerprint payload.
        """
        return {
            "listing": self.listing.fingerprint_payload(),
            "delta_listing": self.delta_listing.fingerprint_payload(),
            "delta_scan": self.delta_scan.fingerprint_payload(),
        }

    def fingerprint(self) -> str:
        """Return the fingerprint hash for the scan policy config.

        Returns:
        -------
        str
            Deterministic fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())


def apply_scan_policy(
    options: DataFusionScanOptions | None,
    *,
    policy: ScanPolicyConfig | None,
    dataset_format: str | None,
) -> DataFusionScanOptions | None:
    """Apply policy defaults to DataFusion scan options.

    Returns:
    -------
    DataFusionScanOptions | None
        Scan options with defaults applied, or ``None`` when unchanged.

    Raises:
        RuntimeError: If the Rust ``SchemaRuntime`` bridge is unavailable,
            fails during execution, or returns an invalid payload.
    """
    if policy is None:
        return options
    defaults = policy.delta_listing if dataset_format == "delta" else policy.listing
    if options is None and not defaults.has_any():
        return None
    base = options or DataFusionScanOptions()
    runtime = load_schema_runtime()
    defaults_payload = defaults.apply(DataFusionScanOptions())
    merged_json = runtime.apply_scan_policy_json(
        msgspec.json.encode(base).decode("utf-8"),
        msgspec.json.encode(defaults_payload).decode("utf-8"),
    )
    try:
        return msgspec.json.decode(merged_json, type=DataFusionScanOptions)
    except msgspec.DecodeError as exc:
        msg = "SchemaRuntime.apply_scan_policy_json returned invalid DataFusionScanOptions JSON."
        raise RuntimeError(msg) from exc


def apply_delta_scan_policy(
    options: DeltaScanOptions | None,
    *,
    policy: ScanPolicyConfig | None,
) -> DeltaScanOptions | None:
    """Apply policy defaults to Delta scan options.

    Returns:
    -------
    DeltaScanOptions | None
        Delta scan options with defaults applied, or ``None`` when unchanged.

    Raises:
        RuntimeError: If the Rust ``SchemaRuntime`` bridge is unavailable,
            fails during execution, or returns an invalid payload.
    """
    if policy is None:
        return options
    defaults = policy.delta_scan
    if options is None and not defaults.has_any():
        return None
    base = options or DeltaScanOptions()
    runtime = load_schema_runtime()
    defaults_payload = defaults.apply(DeltaScanOptions())
    merged_json = runtime.apply_delta_scan_policy_json(
        msgspec.json.encode(base).decode("utf-8"),
        msgspec.json.encode(defaults_payload).decode("utf-8"),
    )
    try:
        return msgspec.json.decode(merged_json, type=DeltaScanOptions)
    except msgspec.DecodeError as exc:
        msg = "SchemaRuntime.apply_delta_scan_policy_json returned invalid DeltaScanOptions JSON."
        raise RuntimeError(msg) from exc


from schema_spec import dataset_spec_runtime as _dataset_spec_runtime
from storage.deltalake import DeltaSchemaRequest

# Keep dataset_spec as the canonical import surface by explicitly binding
# runtime-extracted symbols (instead of dynamic globals() updates).
ContractCatalogSpec = _dataset_spec_runtime.ContractCatalogSpec
ContractKwargs = _dataset_spec_runtime.ContractKwargs
ContractSpec = _dataset_spec_runtime.ContractSpec
ContractSpecKwargs = _dataset_spec_runtime.ContractSpecKwargs
DatasetKind = _dataset_spec_runtime.DatasetKind
DatasetOpenSpec = _dataset_spec_runtime.DatasetOpenSpec
DatasetPolicies = _dataset_spec_runtime.DatasetPolicies
DatasetSpec = _dataset_spec_runtime.DatasetSpec
DatasetSpecKwargs = _dataset_spec_runtime.DatasetSpecKwargs
DeltaCdfPolicy = _dataset_spec_runtime.DeltaCdfPolicy
DeltaMaintenancePolicy = _dataset_spec_runtime.DeltaMaintenancePolicy
DeltaPolicyBundle = _dataset_spec_runtime.DeltaPolicyBundle
DeltaScanOptions = _dataset_spec_runtime.DeltaScanOptions
DeltaSchemaPolicy = _dataset_spec_runtime.DeltaSchemaPolicy
DeltaWritePolicy = _dataset_spec_runtime.DeltaWritePolicy
TableSpecConstraints = _dataset_spec_runtime.TableSpecConstraints
ValidationPolicySpec = _dataset_spec_runtime.ValidationPolicySpec
VirtualFieldSpec = _dataset_spec_runtime.VirtualFieldSpec
dataset_spec_contract = _dataset_spec_runtime.dataset_spec_contract
dataset_spec_contract_spec_or_default = _dataset_spec_runtime.dataset_spec_contract_spec_or_default
dataset_spec_datafusion_scan = _dataset_spec_runtime.dataset_spec_datafusion_scan
dataset_spec_delta_cdf_policy = _dataset_spec_runtime.dataset_spec_delta_cdf_policy
dataset_spec_delta_constraints = _dataset_spec_runtime.dataset_spec_delta_constraints
dataset_spec_delta_feature_gate = _dataset_spec_runtime.dataset_spec_delta_feature_gate
dataset_spec_delta_maintenance_policy = _dataset_spec_runtime.dataset_spec_delta_maintenance_policy
dataset_spec_delta_scan = _dataset_spec_runtime.dataset_spec_delta_scan
dataset_spec_delta_schema_policy = _dataset_spec_runtime.dataset_spec_delta_schema_policy
dataset_spec_delta_write_policy = _dataset_spec_runtime.dataset_spec_delta_write_policy
dataset_spec_encoding_policy = _dataset_spec_runtime.dataset_spec_encoding_policy
dataset_spec_from_contract = _dataset_spec_runtime.dataset_spec_from_contract
dataset_spec_from_dataset = _dataset_spec_runtime.dataset_spec_from_dataset
dataset_spec_from_path = _dataset_spec_runtime.dataset_spec_from_path
dataset_spec_from_schema = _dataset_spec_runtime.dataset_spec_from_schema
dataset_spec_name = _dataset_spec_runtime.dataset_spec_name
dataset_spec_ordering = _dataset_spec_runtime.dataset_spec_ordering
dataset_spec_query = _dataset_spec_runtime.dataset_spec_query
dataset_spec_resolved_view_specs = _dataset_spec_runtime.dataset_spec_resolved_view_specs
dataset_spec_schema = _dataset_spec_runtime.dataset_spec_schema
dataset_spec_strict_schema_validation = _dataset_spec_runtime.dataset_spec_strict_schema_validation
dataset_spec_with_delta_maintenance = _dataset_spec_runtime.dataset_spec_with_delta_maintenance
dataset_table_column_defaults = _dataset_spec_runtime.dataset_table_column_defaults
dataset_table_constraints = _dataset_spec_runtime.dataset_table_constraints
dataset_table_ddl_fingerprint = _dataset_spec_runtime.dataset_table_ddl_fingerprint
dataset_table_definition = _dataset_spec_runtime.dataset_table_definition
dataset_table_logical_plan = _dataset_spec_runtime.dataset_table_logical_plan
ddl_fingerprint_from_definition = _dataset_spec_runtime.ddl_fingerprint_from_definition
make_contract_spec = _dataset_spec_runtime.make_contract_spec
make_dataset_spec = _dataset_spec_runtime.make_dataset_spec
make_table_spec = _dataset_spec_runtime.make_table_spec
resolve_schema_evolution_spec = _dataset_spec_runtime.resolve_schema_evolution_spec
schema_from_datafusion_catalog = _dataset_spec_runtime.schema_from_datafusion_catalog
SCHEMA_EVOLUTION_PRESETS = _dataset_spec_runtime.SCHEMA_EVOLUTION_PRESETS
table_spec_from_schema = _dataset_spec_runtime.table_spec_from_schema
validation_policy_payload = _dataset_spec_runtime.validation_policy_payload
validation_policy_to_arrow_options = _dataset_spec_runtime.validation_policy_to_arrow_options

__all__ = [
    "SCHEMA_EVOLUTION_PRESETS",
    "ArrowValidationOptions",
    "ContractCatalogSpec",
    "ContractKwargs",
    "ContractRow",
    "ContractSpec",
    "ContractSpecKwargs",
    "DataFusionScanOptions",
    "DatasetKind",
    "DatasetOpenSpec",
    "DatasetPolicies",
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "DeltaCdfPolicy",
    "DeltaMaintenancePolicy",
    "DeltaPolicyBundle",
    "DeltaScanOptions",
    "DeltaScanPolicyDefaults",
    "DeltaSchemaPolicy",
    "DeltaSchemaRequest",
    "DeltaWritePolicy",
    "ParquetColumnOptions",
    "ScanPolicyConfig",
    "ScanPolicyDefaults",
    "SortKeySpec",
    "TableSchemaContract",
    "TableSpecConstraints",
    "ValidationPolicySpec",
    "VirtualFieldSpec",
    "apply_delta_scan_policy",
    "apply_scan_policy",
    "dataset_spec_contract",
    "dataset_spec_contract_spec_or_default",
    "dataset_spec_datafusion_scan",
    "dataset_spec_delta_cdf_policy",
    "dataset_spec_delta_constraints",
    "dataset_spec_delta_feature_gate",
    "dataset_spec_delta_maintenance_policy",
    "dataset_spec_delta_scan",
    "dataset_spec_delta_schema_policy",
    "dataset_spec_delta_write_policy",
    "dataset_spec_encoding_policy",
    "dataset_spec_from_contract",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "dataset_spec_name",
    "dataset_spec_ordering",
    "dataset_spec_query",
    "dataset_spec_resolved_view_specs",
    "dataset_spec_schema",
    "dataset_spec_strict_schema_validation",
    "dataset_spec_with_delta_maintenance",
    "dataset_table_column_defaults",
    "dataset_table_constraints",
    "dataset_table_ddl_fingerprint",
    "dataset_table_definition",
    "dataset_table_logical_plan",
    "ddl_fingerprint_from_definition",
    "make_contract_spec",
    "make_dataset_spec",
    "make_table_spec",
    "resolve_schema_evolution_spec",
    "schema_from_datafusion_catalog",
    "table_spec_from_schema",
    "validate_arrow_table",
    "validation_policy_payload",
    "validation_policy_to_arrow_options",
]
