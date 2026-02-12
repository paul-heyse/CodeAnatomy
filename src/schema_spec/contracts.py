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

import importlib
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack, cast

import msgspec
import pyarrow as pa
import pyarrow.dataset as ds

from arrow_utils.core.ordering import Ordering, OrderingLevel
from core.config_base import config_fingerprint
from core_types import IdentifierStr, NonNegativeInt
from datafusion_engine.arrow.build import register_schema_extensions
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    encoding_policy_from_spec,
    merge_metadata_specs,
    metadata_spec_from_schema,
    ordering_metadata_spec,
)
from datafusion_engine.delta.protocol import DeltaFeatureGate
from datafusion_engine.expr.query_spec import ProjectionSpec, QuerySpec
from datafusion_engine.expr.spec import ExprSpec
from datafusion_engine.extensions.schema_runtime import load_schema_runtime
from datafusion_engine.kernels import DedupeSpec, SortKey
from datafusion_engine.schema.alignment import SchemaEvolutionSpec
from datafusion_engine.schema.finalize import Contract
from datafusion_engine.schema.introspection import SchemaIntrospector
from datafusion_engine.schema.registry import extract_nested_dataset_names
from datafusion_engine.schema.validation import ArrowValidationOptions, validate_table
from schema_spec.arrow_types import (
    ArrowTypeBase,
    ArrowTypeSpec,
    arrow_type_from_pyarrow,
    arrow_type_to_pyarrow,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import DerivedFieldSpec, FieldBundle, TableSchemaSpec
from schema_spec.view_specs import ViewSpec
from serde_msgspec import StructBaseStrict
from storage.dataset_sources import (
    DatasetDiscoveryOptions,
    DatasetSourceOptions,
    PathLike,
    normalize_dataset_source,
)
from storage.deltalake import DeltaSchemaRequest
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from utils.hashing import hash_sha256_hex
from utils.validation import validate_required_items

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

    Raises:
        ValueError: If schema or policy validation fails.
    """
    options = options or ArrowValidationOptions()
    report = validate_table(
        table,
        spec=spec,
        options=options,
        runtime_profile=runtime_profile,
    )
    if options.strict is True and not report.valid:
        msg = f"Arrow validation failed for {spec.name!r}."
        raise ValueError(msg)
    return report.validated


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


class DeltaScanOptions(StructBaseStrict, frozen=True):
    """Delta-specific scan configuration.

    Delta table registration uses DataFusion's native Delta TableProvider.
    All IO contracts for Delta tables are specified through DataFusion
    registration and catalog metadata.

    Notes:
    -----
    Delta CDF configuration is handled via DatasetLocation.delta_cdf_options
    to keep change-data-feed registration separate from standard scans.

    Attributes:
    ----------
    file_column_name : str | None
        Column name for source file metadata in Delta scans.
    enable_parquet_pushdown : bool
        Enable predicate pushdown to underlying Parquet files.
    schema_force_view_types : bool | None
        Force schema types to match view definitions.
    wrap_partition_values : bool
        Wrap partition values in structs for nested access.
    schema : pa.Schema | None
        Optional schema override for Delta table registration.
    """

    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool = False
    schema: pa.Schema | None = None


class DeltaCdfPolicy(StructBaseStrict, frozen=True):
    """Policy describing Delta CDF requirements."""

    required: bool = False
    allow_out_of_range: bool = False

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the Delta CDF policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing Delta CDF policy settings.
        """
        return {
            "required": self.required,
            "allow_out_of_range": self.allow_out_of_range,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the Delta CDF policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


class DeltaMaintenancePolicy(StructBaseStrict, frozen=True):
    """Policy describing Delta maintenance expectations."""

    optimize_on_write: bool = False
    optimize_target_size: int | None = None
    z_order_cols: tuple[str, ...] = ()
    z_order_when: Literal["never", "periodic", "after_partition_complete"] = "never"
    vacuum_on_write: bool = False
    vacuum_retention_hours: int | None = None
    vacuum_dry_run: bool = False
    enforce_retention_duration: bool = True
    checkpoint_on_write: bool = False
    enable_deletion_vectors: bool = False
    enable_v2_checkpoints: bool = False
    enable_log_compaction: bool = False

    # Outcome-based maintenance thresholds (Wave 4A)
    optimize_file_threshold: int | None = None
    total_file_threshold: int | None = None
    vacuum_version_threshold: int | None = None
    checkpoint_version_interval: int | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the Delta maintenance policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing Delta maintenance policy settings.
        """
        return {
            "optimize_on_write": self.optimize_on_write,
            "optimize_target_size": self.optimize_target_size,
            "z_order_cols": self.z_order_cols,
            "z_order_when": self.z_order_when,
            "vacuum_on_write": self.vacuum_on_write,
            "vacuum_retention_hours": self.vacuum_retention_hours,
            "vacuum_dry_run": self.vacuum_dry_run,
            "enforce_retention_duration": self.enforce_retention_duration,
            "checkpoint_on_write": self.checkpoint_on_write,
            "enable_deletion_vectors": self.enable_deletion_vectors,
            "enable_v2_checkpoints": self.enable_v2_checkpoints,
            "enable_log_compaction": self.enable_log_compaction,
            "optimize_file_threshold": self.optimize_file_threshold,
            "total_file_threshold": self.total_file_threshold,
            "vacuum_version_threshold": self.vacuum_version_threshold,
            "checkpoint_version_interval": self.checkpoint_version_interval,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the Delta maintenance policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


type DatasetKind = Literal["primary", "delta_cdf"]


def _ordering_metadata_spec(
    contract_spec: ContractSpec | None,
    table_spec: TableSchemaSpec,
) -> SchemaMetadataSpec | None:
    if contract_spec is not None and contract_spec.canonical_sort:
        keys = tuple((key.column, key.order) for key in contract_spec.canonical_sort)
        return ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    if table_spec.key_fields:
        keys = tuple((name, "ascending") for name in table_spec.key_fields)
        return ordering_metadata_spec(OrderingLevel.IMPLICIT, keys=keys)
    return None


def _validate_view_specs(view_specs: Sequence[ViewSpec], *, label: str) -> None:
    """Validate view spec invariants.

    Args:
        view_specs: Description.
        label: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    names = [view.name for view in view_specs]
    if not names:
        return
    if "" in names:
        msg = f"{label} view_specs contain empty view names."
        raise ValueError(msg)
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        msg = f"{label} view_specs contain duplicate names: {sorted(duplicates)}"
        raise ValueError(msg)


class ContractSpec(StructBaseStrict, frozen=True):
    """Output contract specification."""

    name: IdentifierStr
    table_schema: TableSchemaSpec

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None
    view_specs: tuple[ViewSpec, ...] = ()

    def __post_init__(self) -> None:
        """Validate contract spec invariants."""
        if self.virtual_field_docs is None:
            return
        validate_required_items(
            self.virtual_field_docs,
            self.virtual_fields,
            item_label="virtual_field_docs keys in virtual_fields",
            error_type=ValueError,
        )
        _validate_view_specs(self.view_specs, label="contract")

    def to_contract(self) -> Contract:
        """Convert to a runtime Contract.

        Returns:
        -------
        Contract
            Runtime contract instance.
        """
        version = self.version if self.version is not None else self.table_schema.version
        contract_kwargs: ContractKwargs = {
            "name": self.name,
            "schema": self.table_schema.to_arrow_schema(),
            "schema_spec": self.table_schema,
            "key_fields": self.table_schema.key_fields,
            "required_non_null": self.table_schema.required_non_null,
            "dedupe": self.dedupe.to_dedupe_spec() if self.dedupe is not None else None,
            "canonical_sort": tuple(sk.to_sort_key() for sk in self.canonical_sort),
            "constraints": self.constraints,
            "version": version,
            "virtual_fields": self.virtual_fields,
            "virtual_field_docs": self.virtual_field_docs,
            "validation": self.validation,
        }
        return Contract(**contract_kwargs)


class DeltaPolicyBundle(StructBaseStrict, frozen=True):
    """Delta-specific policy bundle for dataset specs."""

    scan: DeltaScanOptions | None = None
    cdf_policy: DeltaCdfPolicy | None = None
    maintenance_policy: DeltaMaintenancePolicy | None = None
    write_policy: DeltaWritePolicy | None = None
    schema_policy: DeltaSchemaPolicy | None = None
    feature_gate: DeltaFeatureGate | None = None
    constraints: tuple[str, ...] = ()


class ValidationPolicySpec(StructBaseStrict, frozen=True):
    """Runtime DataFrame validation policy."""

    enabled: bool = True
    lazy: bool = True
    sample: NonNegativeInt | None = None
    head: NonNegativeInt | None = None
    tail: NonNegativeInt | None = None
    strict: bool | Literal["filter"] | None = None
    coerce: bool | None = None


def validation_policy_to_arrow_options(
    policy: ValidationPolicySpec | None,
) -> ArrowValidationOptions | None:
    """Map dataframe validation policy to Arrow validation options.

    Returns:
    -------
    ArrowValidationOptions | None
        Arrow validation options derived from the dataframe policy, or
        ``None`` when validation is disabled.
    """
    if policy is None or not policy.enabled:
        return None
    strict: bool | Literal["filter"] = "filter"
    if policy.strict is not None:
        strict = policy.strict
    coerce = bool(policy.coerce) if policy.coerce is not None else False
    return ArrowValidationOptions(
        strict=strict,
        coerce=coerce,
    )


def validation_policy_payload(policy: ValidationPolicySpec | None) -> Mapping[str, object] | None:
    """Return a JSON-serializable payload for the dataframe validation policy.

    Returns:
    -------
    Mapping[str, object] | None
        Serializable policy payload when defined.
    """
    if policy is None:
        return None
    return {
        "enabled": policy.enabled,
        "lazy": policy.lazy,
        "sample": policy.sample,
        "head": policy.head,
        "tail": policy.tail,
        "strict": policy.strict,
        "coerce": policy.coerce,
    }


class DatasetPolicies(StructBaseStrict, frozen=True):
    """Policy bundle for dataset specs."""

    datafusion_scan: DataFusionScanOptions | None = None
    delta: DeltaPolicyBundle | None = None
    validation: ArrowValidationOptions | None = None
    dataframe_validation: ValidationPolicySpec | None = None
    strict_schema_validation: bool | None = None


class DatasetSpec(StructBaseStrict, frozen=True):
    """Unified dataset spec with schema, contract, and query behavior."""

    table_spec: TableSchemaSpec
    dataset_kind: DatasetKind = "primary"
    contract_spec: ContractSpec | None = None
    query_spec: QuerySpec | None = None
    view_specs: tuple[ViewSpec, ...] = ()
    policies: DatasetPolicies = msgspec.field(default_factory=DatasetPolicies)
    derived_fields: tuple[DerivedFieldSpec, ...] = ()
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
    evolution_spec: SchemaEvolutionSpec = msgspec.field(default_factory=SchemaEvolutionSpec)
    metadata_spec: SchemaMetadataSpec = msgspec.field(default_factory=SchemaMetadataSpec)

    def __post_init__(self) -> None:
        """Validate dataset spec invariants."""
        _validate_view_specs(self.view_specs, label="dataset")


class DatasetOpenSpec(StructBaseStrict, frozen=True):
    """Dataset open parameters for schema discovery."""

    dataset_format: str = "delta"
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    partitioning: str | None = "hive"
    schema: SchemaLike | None = None
    read_options: Mapping[str, object] = msgspec.field(default_factory=dict)
    storage_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    delta_log_storage_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    delta_version: int | None = None
    delta_timestamp: str | None = None
    discovery: DatasetDiscoveryOptions | None = msgspec.field(
        default_factory=DatasetDiscoveryOptions
    )

    def open(self, path: PathLike) -> ds.Dataset:
        """Open a dataset using the stored options.

        Args:
            path: Description.

        Returns:
            ds.Dataset: Result.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.dataset_format == "delta":
            msg = "Delta dataset discovery requires DataFusion catalog introspection."
            raise ValueError(msg)
        if self.dataset_format == "parquet":
            msg = "Parquet dataset discovery is disabled for delta-only IO."
            raise ValueError(msg)
        if self.schema is not None:
            register_schema_extensions(self.schema)
        return normalize_dataset_source(
            path,
            options=DatasetSourceOptions(
                dataset_format=self.dataset_format,
                filesystem=self.filesystem,
                files=self.files,
                partitioning=self.partitioning,
                schema=self.schema,
                storage_options=self.storage_options,
                delta_version=self.delta_version,
                delta_timestamp=self.delta_timestamp,
                discovery=self.discovery,
            ),
        )


class TableSpecConstraints(StructBaseStrict, frozen=True):
    """Required and key fields for a table schema."""

    required_non_null: Iterable[str] = ()
    key_fields: Iterable[str] = ()


class VirtualFieldSpec(StructBaseStrict, frozen=True):
    """Virtual field names and documentation for a contract."""

    fields: tuple[str, ...] = ()
    docs: Mapping[str, str] | None = None


class ContractSpecKwargs(TypedDict, total=False):
    """Keyword arguments supported by make_contract_spec."""

    dedupe: DedupeSpecSpec | None
    canonical_sort: Iterable[SortKeySpec]
    constraints: Iterable[str]
    virtual: VirtualFieldSpec | None
    version: int | None
    validation: ArrowValidationOptions | None
    view_specs: Sequence[ViewSpec]


class ContractKwargs(TypedDict):
    """Keyword arguments for building runtime contracts."""

    name: str
    schema: SchemaLike
    schema_spec: TableSchemaSpec | None
    key_fields: tuple[str, ...]
    required_non_null: tuple[str, ...]
    dedupe: DedupeSpec | None
    canonical_sort: tuple[SortKey, ...]
    constraints: tuple[str, ...]
    version: int | None
    virtual_fields: tuple[str, ...]
    virtual_field_docs: dict[str, str] | None
    validation: ArrowValidationOptions | None


class DatasetSpecKwargs(TypedDict, total=False):
    """Keyword arguments supported by make_dataset_spec."""

    dataset_kind: DatasetKind
    contract_spec: ContractSpec | None
    query_spec: QuerySpec | None
    view_specs: Sequence[ViewSpec]
    policies: DatasetPolicies | None
    datafusion_scan: DataFusionScanOptions | None
    delta: DeltaPolicyBundle | None
    delta_scan: DeltaScanOptions | None
    delta_cdf_policy: DeltaCdfPolicy | None
    delta_maintenance_policy: DeltaMaintenancePolicy | None
    delta_write_policy: DeltaWritePolicy | None
    delta_schema_policy: DeltaSchemaPolicy | None
    delta_feature_gate: DeltaFeatureGate | None
    delta_constraints: Sequence[str]
    derived_fields: Sequence[DerivedFieldSpec]
    predicate: ExprSpec | None
    pushdown_predicate: ExprSpec | None
    evolution_spec: SchemaEvolutionSpec | None
    metadata_spec: SchemaMetadataSpec | None
    validation: ArrowValidationOptions | None
    dataframe_validation: ValidationPolicySpec | None
    strict_schema_validation: bool | None


def _merge_names(*parts: Iterable[str]) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for name in part:
            if name in seen:
                continue
            merged.append(name)
            seen.add(name)
    return tuple(merged)


def _merge_constraints(*parts: Iterable[str]) -> tuple[str, ...]:
    """Return unique constraint expressions in stable order.

    Returns:
    -------
    tuple[str, ...]
        Normalized constraint expressions.
    """
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for constraint in part:
            normalized = constraint.strip()
            if not normalized or normalized in seen:
                continue
            merged.append(normalized)
            seen.add(normalized)
    return tuple(merged)


def _delta_constraints_from_table_spec(table_spec: TableSchemaSpec) -> tuple[str, ...]:
    """Return default Delta constraints derived from schema constraints.

    Returns:
    -------
    tuple[str, ...]
        Constraint expressions derived from required and key fields.
    """
    required = _merge_names(table_spec.required_non_null, table_spec.key_fields)
    if not required:
        return ()
    required_set = set(required)
    ordered = [field.name for field in table_spec.fields if field.name in required_set]
    return tuple(f"{name} IS NOT NULL" for name in ordered)


def _merge_fields(
    bundles: Iterable[FieldBundle],
    fields: Iterable[FieldSpec],
) -> list[FieldSpec]:
    merged: list[FieldSpec] = []
    for bundle in bundles:
        merged.extend(bundle.fields)
    merged.extend(fields)
    return merged


def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[FieldSpec],
    constraints: TableSpecConstraints | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from field bundles and explicit fields.

    Returns:
    -------
    TableSchemaSpec
        Table schema specification.
    """
    if constraints is None:
        constraints = TableSpecConstraints()
    bundle_required = (bundle.required_non_null for bundle in bundles)
    bundle_keys = (bundle.key_fields for bundle in bundles)
    return TableSchemaSpec(
        name=name,
        version=version,
        fields=_merge_fields(bundles, fields),
        required_non_null=_merge_names(*bundle_required, constraints.required_non_null),
        key_fields=_merge_names(*bundle_keys, constraints.key_fields),
    )


def table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from an Arrow schema.

    Returns:
    -------
    TableSchemaSpec
        Table schema specification derived from the Arrow schema.
    """
    return TableSchemaSpec.from_schema(name, schema, version=version)


def make_contract_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[ContractSpecKwargs],
) -> ContractSpec:
    """Create a ContractSpec from a TableSchemaSpec.

    Returns:
    -------
    ContractSpec
        Contract specification.
    """
    dedupe = kwargs.get("dedupe")
    canonical_sort = kwargs.get("canonical_sort", ())
    constraints = kwargs.get("constraints", ())
    virtual = kwargs.get("virtual")
    version = kwargs.get("version")
    validation = kwargs.get("validation")
    view_specs = tuple(cast("Sequence[ViewSpec]", kwargs.get("view_specs", ())))
    virtual_fields = virtual.fields if virtual is not None else ()
    virtual_docs = dict(virtual.docs) if virtual is not None and virtual.docs is not None else None
    return ContractSpec(
        name=table_spec.name,
        table_schema=table_spec,
        dedupe=dedupe,
        canonical_sort=tuple(canonical_sort),
        constraints=tuple(constraints),
        version=version if version is not None else table_spec.version,
        virtual_fields=virtual_fields,
        virtual_field_docs=virtual_docs,
        validation=validation,
        view_specs=view_specs,
    )


def make_dataset_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[DatasetSpecKwargs],
) -> DatasetSpec:
    """Create a DatasetSpec from schema, contract, and query settings.

    Returns:
    -------
    DatasetSpec
        Dataset specification bundling schema, contract, and query behavior.
    """
    policies = kwargs.get("policies")
    if policies is None:
        delta_bundle = kwargs.get("delta")
        if delta_bundle is None:
            delta_bundle = DeltaPolicyBundle(
                scan=kwargs.get("delta_scan"),
                cdf_policy=kwargs.get("delta_cdf_policy"),
                maintenance_policy=kwargs.get("delta_maintenance_policy"),
                write_policy=kwargs.get("delta_write_policy"),
                schema_policy=kwargs.get("delta_schema_policy"),
                feature_gate=kwargs.get("delta_feature_gate"),
                constraints=tuple(kwargs.get("delta_constraints", ())),
            )
        policies = DatasetPolicies(
            datafusion_scan=kwargs.get("datafusion_scan"),
            delta=delta_bundle,
            validation=kwargs.get("validation"),
            dataframe_validation=kwargs.get("dataframe_validation"),
            strict_schema_validation=kwargs.get("strict_schema_validation"),
        )
    return DatasetSpec(
        table_spec=table_spec,
        dataset_kind=kwargs.get("dataset_kind", "primary"),
        contract_spec=kwargs.get("contract_spec"),
        query_spec=kwargs.get("query_spec"),
        view_specs=tuple(kwargs.get("view_specs", ())),
        policies=policies,
        derived_fields=tuple(kwargs.get("derived_fields", ())),
        predicate=kwargs.get("predicate"),
        pushdown_predicate=kwargs.get("pushdown_predicate"),
        evolution_spec=kwargs.get("evolution_spec") or SchemaEvolutionSpec(),
        metadata_spec=kwargs.get("metadata_spec") or SchemaMetadataSpec(),
    )


def _sort_key_specs(keys: Iterable[SortKey]) -> tuple[SortKeySpec, ...]:
    """Convert SortKey objects into SortKeySpec values.

    Returns:
    -------
    tuple[SortKeySpec, ...]
        SortKeySpec values for the keys.
    """
    return tuple(SortKeySpec(column=key.column, order=key.order) for key in keys)


def _dedupe_spec_spec(dedupe: DedupeSpec | None) -> DedupeSpecSpec | None:
    """Convert a DedupeSpec into a DedupeSpecSpec.

    Returns:
    -------
    DedupeSpecSpec | None
        Dedupe spec model or ``None``.
    """
    if dedupe is None:
        return None
    return DedupeSpecSpec(
        keys=dedupe.keys,
        tie_breakers=_sort_key_specs(dedupe.tie_breakers),
        strategy=dedupe.strategy,
    )


def _table_spec_from_contract(contract: Contract) -> TableSchemaSpec:
    """Build a TableSchemaSpec from a runtime contract.

    Returns:
    -------
    TableSchemaSpec
        Table schema specification derived from the contract.
    """
    if contract.schema_spec is not None:
        return contract.schema_spec
    table_spec = TableSchemaSpec.from_schema(
        contract.name,
        contract.schema,
        version=contract.version,
    )
    if not contract.required_non_null and not contract.key_fields:
        return table_spec
    return msgspec.structs.replace(
        table_spec,
        required_non_null=contract.required_non_null,
        key_fields=contract.key_fields,
    )


def dataset_spec_from_contract(contract: Contract) -> DatasetSpec:
    """Create a DatasetSpec aligned to a runtime Contract.

    Returns:
    -------
    DatasetSpec
        Dataset spec derived from the contract.
    """
    table_spec = _table_spec_from_contract(contract)
    constraints = contract.constraints if hasattr(contract, "constraints") else ()
    delta_constraints = _merge_constraints(
        constraints,
        _delta_constraints_from_table_spec(table_spec),
    )
    contract_spec = ContractSpec(
        name=contract.name,
        table_schema=table_spec,
        dedupe=_dedupe_spec_spec(contract.dedupe),
        canonical_sort=_sort_key_specs(contract.canonical_sort),
        constraints=constraints,
        version=contract.version,
        virtual_fields=contract.virtual_fields,
        virtual_field_docs=contract.virtual_field_docs,
        validation=contract.validation,
    )
    return make_dataset_spec(
        table_spec=table_spec,
        contract_spec=contract_spec,
        delta_constraints=delta_constraints,
    )


def dataset_spec_name(spec: DatasetSpec) -> str:
    """Return the dataset name."""
    return spec.table_spec.name


def dataset_spec_datafusion_scan(spec: DatasetSpec) -> DataFusionScanOptions | None:
    """Return DataFusion scan options from the policy bundle."""
    return spec.policies.datafusion_scan


def dataset_spec_delta_scan(spec: DatasetSpec) -> DeltaScanOptions | None:
    """Return Delta scan options from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.scan


def dataset_spec_delta_cdf_policy(spec: DatasetSpec) -> DeltaCdfPolicy | None:
    """Return Delta CDF policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.cdf_policy


def dataset_spec_delta_maintenance_policy(spec: DatasetSpec) -> DeltaMaintenancePolicy | None:
    """Return Delta maintenance policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.maintenance_policy


def dataset_spec_with_delta_maintenance(
    spec: DatasetSpec,
    maintenance_policy: DeltaMaintenancePolicy | None,
) -> DatasetSpec:
    """Return a dataset spec with updated Delta maintenance policy."""
    if maintenance_policy is None:
        return spec
    delta = spec.policies.delta
    if delta is None:
        delta = DeltaPolicyBundle(maintenance_policy=maintenance_policy)
    else:
        delta = msgspec.structs.replace(delta, maintenance_policy=maintenance_policy)
    policies = msgspec.structs.replace(spec.policies, delta=delta)
    return msgspec.structs.replace(spec, policies=policies)


def dataset_spec_delta_write_policy(spec: DatasetSpec) -> DeltaWritePolicy | None:
    """Return Delta write policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.write_policy


def dataset_spec_delta_schema_policy(spec: DatasetSpec) -> DeltaSchemaPolicy | None:
    """Return Delta schema policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.schema_policy


def dataset_spec_delta_feature_gate(spec: DatasetSpec) -> DeltaFeatureGate | None:
    """Return Delta feature gate from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.feature_gate


def dataset_spec_delta_constraints(spec: DatasetSpec) -> tuple[str, ...]:
    """Return declared Delta constraints from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return ()
    return delta.constraints


def dataset_spec_strict_schema_validation(spec: DatasetSpec) -> bool | None:
    """Return strict schema validation preference from the policy bundle."""
    return spec.policies.strict_schema_validation


def dataset_spec_schema(spec: DatasetSpec) -> SchemaLike:
    """Return the Arrow schema with dataset metadata and ordering applied."""
    ordering = _ordering_metadata_spec(spec.contract_spec, spec.table_spec)
    delta_policy = spec.policies.delta
    if (
        ordering is None
        and delta_policy is not None
        and delta_policy.write_policy is not None
        and delta_policy.write_policy.zorder_by
    ):
        keys = tuple((name, "ascending") for name in delta_policy.write_policy.zorder_by)
        ordering = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    merged = merge_metadata_specs(spec.metadata_spec, ordering)
    return merged.apply(spec.table_spec.to_arrow_schema())


def dataset_spec_ordering(spec: DatasetSpec) -> Ordering:
    """Return ordering metadata derived from contract and key fields."""
    if spec.contract_spec is not None and spec.contract_spec.canonical_sort:
        keys = tuple((key.column, key.order) for key in spec.contract_spec.canonical_sort)
        return Ordering.explicit(keys)
    if spec.table_spec.key_fields:
        return Ordering.implicit()
    return Ordering.unordered()


def dataset_spec_query(spec: DatasetSpec) -> QuerySpec:
    """Return query spec, deriving projection from schema fields when absent."""
    if spec.query_spec is not None:
        return spec.query_spec
    cols = tuple(field.name for field in spec.table_spec.fields)
    derived = {field_spec.name: field_spec.expr for field_spec in spec.derived_fields}
    return QuerySpec(
        projection=ProjectionSpec(base=cols, derived=derived),
        predicate=spec.predicate,
        pushdown_predicate=spec.pushdown_predicate,
    )


def dataset_spec_contract_spec_or_default(spec: DatasetSpec) -> ContractSpec:
    """Return contract spec with policy validation defaulted when missing."""
    if spec.contract_spec is not None:
        validation = spec.policies.validation
        if validation is None or spec.contract_spec.validation is not None:
            return spec.contract_spec
        return msgspec.structs.replace(spec.contract_spec, validation=validation)
    return ContractSpec(
        name=spec.table_spec.name,
        table_schema=spec.table_spec,
        version=spec.table_spec.version,
        validation=spec.policies.validation,
    )


def dataset_spec_contract(spec: DatasetSpec) -> Contract:
    """Return runtime Contract derived from DatasetSpec."""
    return dataset_spec_contract_spec_or_default(spec).to_contract()


def dataset_spec_resolved_view_specs(spec: DatasetSpec) -> tuple[ViewSpec, ...]:
    """Return merged view specs with contract-level views taking precedence."""
    specs: list[ViewSpec] = []
    seen: set[str] = set()
    if spec.contract_spec is not None:
        for view in spec.contract_spec.view_specs:
            if view.name in seen:
                continue
            specs.append(view)
            seen.add(view.name)
    for view in spec.view_specs:
        if view.name in seen:
            continue
        specs.append(view)
        seen.add(view.name)
    return tuple(specs)


def dataset_spec_encoding_policy(spec: DatasetSpec) -> EncodingPolicy | None:
    """Return encoding policy derived from the dataset schema spec."""
    policy = encoding_policy_from_spec(spec.table_spec)
    if not policy.specs:
        return None
    return policy


def ddl_fingerprint_from_definition(ddl: str) -> str:
    """Return a stable fingerprint for a CREATE TABLE statement.

    Parameters
    ----------
    ddl
        CREATE TABLE statement text.

    Returns:
    -------
    str
        Stable fingerprint of the DDL string.
    """
    return hash_sha256_hex(ddl.encode("utf-8"))


def dataset_table_ddl_fingerprint(name: str) -> str | None:
    """Return the DDL fingerprint for a DataFusion-registered table.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns:
    -------
    str | None
        Stable DDL fingerprint when available.
    """
    ddl = dataset_table_definition(name)
    if ddl is None:
        return None
    return ddl_fingerprint_from_definition(ddl)


def dataset_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from an Arrow schema.

    Returns:
    -------
    DatasetSpec
        Dataset spec derived from the schema.
    """
    table_spec = TableSchemaSpec.from_schema(name, schema, version=version)
    metadata_spec = metadata_spec_from_schema(schema)
    evolution_spec = resolve_schema_evolution_spec(name)
    return make_dataset_spec(
        table_spec=table_spec,
        metadata_spec=metadata_spec,
        evolution_spec=evolution_spec,
        delta_constraints=_delta_constraints_from_table_spec(table_spec),
    )


def dataset_table_definition(name: str) -> str | None:
    """Return the DataFusion CREATE TABLE definition for a dataset.

    This is the canonical source for DDL provenance. All table metadata
    should be sourced from DataFusion's catalog and information_schema.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns:
    -------
    str | None
        CREATE TABLE statement when available from DataFusion catalog.
    """
    module = importlib.import_module("datafusion_engine.runtime")
    runtime = module.DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=module.sql_options_for_profile(runtime))
    return introspector.table_definition(name)


def dataset_table_constraints(name: str) -> tuple[str, ...]:
    """Return DataFusion constraint metadata for a dataset.

    Constraints are sourced from DataFusion's information_schema.table_constraints
    and information_schema.key_column_usage views.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns:
    -------
    tuple[str, ...]
        Constraint expressions or identifiers from DataFusion catalog.
    """
    module = importlib.import_module("datafusion_engine.runtime")
    runtime = module.DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=module.sql_options_for_profile(runtime))
    return introspector.table_constraints(name)


def dataset_table_column_defaults(name: str) -> dict[str, object]:
    """Return DataFusion column default metadata for a dataset.

    Column defaults are sourced from DataFusion's information_schema.columns view
    (column_default column).

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns:
    -------
    dict[str, object]
        Mapping of column names to default expressions from DataFusion catalog.
    """
    module = importlib.import_module("datafusion_engine.runtime")
    runtime = module.DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=module.sql_options_for_profile(runtime))
    return introspector.table_column_defaults(name)


def dataset_table_logical_plan(name: str) -> str | None:
    """Return DataFusion logical plan text for a dataset.

    The logical plan is retrieved from DataFusion's DataFrame.logical_plan() method
    and represents the optimized query plan for the table.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns:
    -------
    str | None
        Logical plan text from DataFusion when available.
    """
    module = importlib.import_module("datafusion_engine.runtime")
    runtime = module.DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=module.sql_options_for_profile(runtime))
    return introspector.table_logical_plan(name)


def schema_from_datafusion_catalog(name: str) -> pa.Schema:
    """Return Arrow schema from DataFusion catalog for a registered table.

    This is the canonical schema discovery method. All schema queries should
    use DataFusion's catalog as the source of truth via ctx.table(name).schema().

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns:
    -------
    pyarrow.Schema
        Arrow schema resolved from DataFusion catalog.
    """
    module = importlib.import_module("datafusion_engine.runtime")
    runtime = module.DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=module.sql_options_for_profile(runtime))
    return introspector.table_schema(name)


def dataset_spec_from_dataset(
    name: str,
    dataset: ds.Dataset,
    *,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from a dataset schema.

    Returns:
    -------
    DatasetSpec
        Dataset spec derived from the dataset schema.
    """
    return dataset_spec_from_schema(name, dataset.schema, version=version)


def dataset_spec_from_path(
    name: str,
    path: PathLike,
    *,
    options: DatasetOpenSpec | None = None,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from a dataset path.

    Args:
        name: Dataset name.
        path: Dataset path or URI.
        options: Optional dataset-open options.
        version: Optional schema version override.

    Returns:
        DatasetSpec: Result.

    Raises:
        ValueError: If dataset schema cannot be resolved.
    """
    options = options or DatasetOpenSpec()
    if options.dataset_format == "delta":
        from datafusion_engine.delta.service import delta_service_for_profile

        schema = delta_service_for_profile(None).table_schema(
            DeltaSchemaRequest(
                path=str(path),
                storage_options=options.storage_options or None,
                log_storage_options=options.delta_log_storage_options or None,
                version=options.delta_version,
                timestamp=options.delta_timestamp,
            )
        )
        if schema is None:
            msg = f"Delta schema unavailable for dataset at {path!r}."
            raise ValueError(msg)
        return dataset_spec_from_schema(name, schema, version=version)
    dataset = options.open(path)
    return dataset_spec_from_dataset(name, dataset, version=version)


SCHEMA_EVOLUTION_PRESETS: Mapping[str, SchemaEvolutionSpec] = {
    name: SchemaEvolutionSpec(
        allow_missing=True,
        allow_extra=True,
        allow_casts=True,
    )
    for name in (*extract_nested_dataset_names(), "symtable_files_v1")
}


def resolve_schema_evolution_spec(name: str) -> SchemaEvolutionSpec:
    """Return a canonical schema evolution spec for a dataset name.

    Returns:
    -------
    SchemaEvolutionSpec
        Evolution rules for the dataset name.
    """
    return SCHEMA_EVOLUTION_PRESETS.get(name, SchemaEvolutionSpec())


class ContractCatalogSpec(StructBaseStrict, frozen=True):
    """Collection of contract specifications keyed by name."""

    contracts: dict[str, ContractSpec]

    def __post_init__(self) -> None:
        """Validate that contract names match mapping keys.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        mismatched = [name for name, spec in self.contracts.items() if name != spec.name]
        if mismatched:
            msg = f"contract key mismatch: {mismatched}"
            raise ValueError(msg)

    def to_contracts(self) -> dict[str, ContractSpec]:
        """Return a name->spec mapping.

        Returns:
        -------
        dict[str, ContractSpec]
            Mapping of contract names to specs.
        """
        return dict(self.contracts)


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
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "DeltaCdfPolicy",
    "DeltaMaintenancePolicy",
    "DeltaScanOptions",
    "DeltaScanPolicyDefaults",
    "DeltaSchemaPolicy",
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
