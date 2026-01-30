"""DataFusion-native dataset location and registry helpers."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast

from arrow_utils.core.ordering import OrderingLevel
from core_types import PathLike
from datafusion_engine.arrow_interop import SchemaLike
from datafusion_engine.arrow_schema.abi import schema_to_dict
from datafusion_engine.delta_protocol import DeltaFeatureGate
from datafusion_engine.delta_scan_config import delta_scan_config_snapshot_from_options
from datafusion_engine.delta_store_policy import apply_delta_store_policy
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.runtime import normalize_dataset_locations_for_profile
from schema_spec.specs import TableSchemaSpec
from serde_msgspec import to_builtins
from storage.deltalake import DeltaCdfOptions, DeltaSchemaRequest, delta_table_schema
from utils.registry_protocol import MutableRegistry

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from schema_spec.system import (
        DataFusionScanOptions,
        DatasetSpec,
        DeltaCdfPolicy,
        DeltaMaintenancePolicy,
        DeltaScanOptions,
        DeltaSchemaPolicy,
        DeltaWritePolicy,
    )

type DatasetFormat = str
type DataFusionProvider = Literal["listing", "delta_cdf"]


@dataclass(frozen=True)
class DatasetLocation:
    """Location metadata for a dataset."""

    path: PathLike
    format: DatasetFormat = "delta"
    partitioning: str | None = "hive"
    read_options: Mapping[str, object] = field(default_factory=dict)
    storage_options: Mapping[str, str] = field(default_factory=dict)
    delta_log_storage_options: Mapping[str, str] = field(default_factory=dict)
    delta_scan: DeltaScanOptions | None = None
    delta_cdf_options: DeltaCdfOptions | None = None
    delta_cdf_policy: DeltaCdfPolicy | None = None
    delta_maintenance_policy: DeltaMaintenancePolicy | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_feature_gate: DeltaFeatureGate | None = None
    delta_constraints: tuple[str, ...] = ()
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    table_spec: TableSchemaSpec | None = None
    dataset_spec: DatasetSpec | None = None
    datafusion_scan: DataFusionScanOptions | None = None
    datafusion_provider: DataFusionProvider | None = None
    delta_version: int | None = None
    delta_timestamp: str | None = None


@dataclass
class DatasetCatalog(MutableRegistry[str, DatasetLocation]):
    """Map dataset names to locations for DataFusion registration."""

    def register(self, key: str, value: DatasetLocation, *, overwrite: bool = False) -> None:
        """Register a dataset location.

        Parameters
        ----------
        key:
            Dataset name.
        value:
            Location metadata.
        overwrite:
            Whether to overwrite existing entries.

        Raises
        ------
        ValueError
            Raised when the dataset name is empty.
        """
        if not key:
            msg = "DatasetCatalog.register: name must be non-empty."
            raise ValueError(msg)
        super().register(key, value, overwrite=overwrite)

    def get(self, key: str) -> DatasetLocation:
        """Return a registered dataset location.

        Parameters
        ----------
        key:
            Dataset name.

        Returns
        -------
        DatasetLocation
            Location metadata for the dataset.

        Raises
        ------
        KeyError
            Raised when the dataset name is not registered.
        """
        value = super().get(key)
        if value is None:
            msg = f"DatasetCatalog: unknown dataset {key!r}."
            raise KeyError(msg)
        return value

    def has(self, name: str) -> bool:
        """Return whether a dataset name is registered.

        Parameters
        ----------
        name:
            Dataset name.

        Returns
        -------
        bool
            ``True`` when the dataset is registered.
        """
        return name in self

    def names(self) -> list[str]:
        """Return registered dataset names in sorted order.

        Returns
        -------
        list[str]
            Sorted dataset names.
        """
        return sorted(self._entries)


def registry_snapshot(catalog: DatasetCatalog) -> list[dict[str, object]]:
    """Return a JSON-ready snapshot of registry locations.

    Non-Delta locations are omitted because catalog autoload handles
    non-Delta registration.

    Returns
    -------
    list[dict[str, object]]
        Registry snapshot payloads.
    """
    snapshot: list[dict[str, object]] = []
    for name in catalog.names():
        loc = catalog.get(name)
        if loc.format != "delta":
            continue
        schema = resolve_dataset_schema(loc)
        scan = None
        if loc.datafusion_scan is not None:
            scan = {
                "partition_cols": [
                    (col, str(dtype)) for col, dtype in loc.datafusion_scan.partition_cols
                ],
                "file_sort_order": [list(key) for key in loc.datafusion_scan.file_sort_order],
                "parquet_pruning": loc.datafusion_scan.parquet_pruning,
                "skip_metadata": loc.datafusion_scan.skip_metadata,
                "skip_arrow_metadata": loc.datafusion_scan.skip_arrow_metadata,
                "binary_as_string": loc.datafusion_scan.binary_as_string,
                "schema_force_view_types": loc.datafusion_scan.schema_force_view_types,
                "listing_table_factory_infer_partitions": (
                    loc.datafusion_scan.listing_table_factory_infer_partitions
                ),
                "listing_table_ignore_subdirectory": (
                    loc.datafusion_scan.listing_table_ignore_subdirectory
                ),
                "file_extension": loc.datafusion_scan.file_extension,
                "cache": loc.datafusion_scan.cache,
                "collect_statistics": loc.datafusion_scan.collect_statistics,
                "meta_fetch_concurrency": loc.datafusion_scan.meta_fetch_concurrency,
                "list_files_cache_ttl": loc.datafusion_scan.list_files_cache_ttl,
                "list_files_cache_limit": loc.datafusion_scan.list_files_cache_limit,
                "projection_exprs": list(loc.datafusion_scan.projection_exprs),
                "unbounded": loc.datafusion_scan.unbounded,
            }
        delta_scan_snapshot = delta_scan_config_snapshot_from_options(loc.delta_scan)
        delta_scan = (
            cast("dict[str, object]", to_builtins(delta_scan_snapshot, str_keys=True))
            if delta_scan_snapshot is not None
            else None
        )
        delta_write_policy = None
        if loc.delta_write_policy is not None:
            parquet_writer_policy = None
            if loc.delta_write_policy.parquet_writer_policy is not None:
                parquet_writer_policy = {
                    "statistics_enabled": list(
                        loc.delta_write_policy.parquet_writer_policy.statistics_enabled
                    ),
                    "statistics_level": loc.delta_write_policy.parquet_writer_policy.statistics_level,
                    "bloom_filter_enabled": list(
                        loc.delta_write_policy.parquet_writer_policy.bloom_filter_enabled
                    ),
                    "bloom_filter_fpp": loc.delta_write_policy.parquet_writer_policy.bloom_filter_fpp,
                    "bloom_filter_ndv": loc.delta_write_policy.parquet_writer_policy.bloom_filter_ndv,
                    "dictionary_enabled": list(
                        loc.delta_write_policy.parquet_writer_policy.dictionary_enabled
                    ),
                }
            delta_write_policy = {
                "target_file_size": loc.delta_write_policy.target_file_size,
                "partition_by": list(loc.delta_write_policy.partition_by),
                "zorder_by": list(loc.delta_write_policy.zorder_by),
                "stats_policy": loc.delta_write_policy.stats_policy,
                "stats_columns": (
                    list(loc.delta_write_policy.stats_columns)
                    if loc.delta_write_policy.stats_columns is not None
                    else None
                ),
                "stats_max_columns": loc.delta_write_policy.stats_max_columns,
                "parquet_writer_policy": parquet_writer_policy,
                "enable_features": list(loc.delta_write_policy.enable_features),
            }
        delta_schema_policy = None
        if loc.delta_schema_policy is not None:
            delta_schema_policy = {
                "schema_mode": loc.delta_schema_policy.schema_mode,
                "column_mapping_mode": loc.delta_schema_policy.column_mapping_mode,
            }
        provider = resolve_datafusion_provider(loc)
        snapshot.append(
            {
                "name": name,
                "path": str(loc.path),
                "format": loc.format,
                "partitioning": loc.partitioning,
                "datafusion_provider": provider,
                "storage_options": dict(loc.storage_options) if loc.storage_options else None,
                "delta_log_storage_options": (
                    dict(loc.delta_log_storage_options) if loc.delta_log_storage_options else None
                ),
                "delta_scan": delta_scan,
                "delta_write_policy": delta_write_policy,
                "delta_schema_policy": delta_schema_policy,
                "delta_constraints": list(loc.delta_constraints) if loc.delta_constraints else None,
                "delta_version": loc.delta_version,
                "delta_timestamp": loc.delta_timestamp,
                "scan": scan,
                "ddl_fingerprint": None,
                "schema_identity_hash": schema_identity_hash(schema)
                if schema is not None
                else None,
                "schema": schema_to_dict(schema) if schema is not None else None,
            }
        )
    return snapshot


def dataset_catalog_from_profile(
    profile: DataFusionRuntimeProfile | None,
) -> DatasetCatalog:
    """Build a DatasetCatalog from a runtime profile.

    Returns
    -------
    DatasetCatalog
        Catalog with dataset locations derived from the profile.
    """
    catalog = DatasetCatalog()
    if profile is None:
        return catalog
    profile_ref = profile

    def _register(name: str, location: DatasetLocation) -> None:
        if catalog.has(name):
            return
        resolved = apply_delta_store_policy(location, policy=profile_ref.delta_store_policy)
        catalog.register(name, resolved)

    def _register_locations(locations: Mapping[str, DatasetLocation]) -> None:
        for name, location in locations.items():
            _register(name, location)

    def _register_registry_catalog(registry: DatasetCatalog) -> None:
        for name in registry.names():
            if catalog.has(name):
                continue
            with suppress(KeyError):
                _register(name, registry.get(name))

    _register_locations(profile.extract_dataset_locations)
    _register_locations(profile.scip_dataset_locations)
    _register_locations(normalize_dataset_locations_for_profile(profile))
    for registry in profile.registry_catalogs.values():
        _register_registry_catalog(registry)
    return catalog


def resolve_datafusion_scan_options(location: DatasetLocation) -> DataFusionScanOptions | None:
    """Return DataFusion scan options for a dataset location.

    Precedence:
      1) Explicit ``DatasetLocation.datafusion_scan`` overrides everything.
      2) ``DatasetSpec.datafusion_scan`` provides defaults when location overrides are absent.

    Returns
    -------
    DataFusionScanOptions | None
        Scan options derived from the dataset location, when present.
    """
    scan = location.datafusion_scan
    if scan is None and location.dataset_spec is not None:
        scan = location.dataset_spec.datafusion_scan
    if scan is None:
        return None
    if location.dataset_spec is None:
        return scan
    if scan.file_sort_order:
        return scan
    ordering = location.dataset_spec.ordering()
    file_sort_order: tuple[tuple[str, str], ...] = ()
    if ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        file_sort_order = tuple(ordering.keys)
    elif location.dataset_spec.table_spec.key_fields:
        file_sort_order = tuple(
            (name, "ascending") for name in location.dataset_spec.table_spec.key_fields
        )
    if not file_sort_order:
        return scan
    return replace(scan, file_sort_order=file_sort_order)


def resolve_datafusion_provider(location: DatasetLocation) -> DataFusionProvider | None:
    """Return the effective DataFusion provider for a dataset location.

    Returns
    -------
    DataFusionProvider | None
        Resolved provider for the dataset location, when available.
    """
    if location.datafusion_provider is not None:
        return location.datafusion_provider
    cdf_policy = resolve_delta_cdf_policy(location)
    if cdf_policy is not None and cdf_policy.required:
        return "delta_cdf"
    if location.dataset_spec is not None and location.dataset_spec.dataset_kind == "delta_cdf":
        return "delta_cdf"
    return None


def resolve_delta_cdf_policy(location: DatasetLocation) -> DeltaCdfPolicy | None:
    """Return Delta CDF policy for a dataset location.

    Returns
    -------
    DeltaCdfPolicy | None
        Resolved Delta CDF policy when configured.
    """
    if location.delta_cdf_policy is not None:
        return location.delta_cdf_policy
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_cdf_policy
    return None


def resolve_delta_log_storage_options(location: DatasetLocation) -> Mapping[str, str] | None:
    """Return Delta log-store options for a dataset location.

    Returns
    -------
    Mapping[str, str] | None
        Log-store options for Delta table access.
    """
    if location.delta_log_storage_options:
        return location.delta_log_storage_options
    if location.storage_options:
        return location.storage_options
    return None


def resolve_delta_write_policy(location: DatasetLocation) -> DeltaWritePolicy | None:
    """Return Delta write policy for a dataset location.

    Returns
    -------
    DeltaWritePolicy | None
        Delta write policy derived from the dataset location, when present.
    """
    if location.delta_write_policy is not None:
        return location.delta_write_policy
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_write_policy
    return None


def resolve_delta_schema_policy(location: DatasetLocation) -> DeltaSchemaPolicy | None:
    """Return Delta schema policy for a dataset location.

    Returns
    -------
    DeltaSchemaPolicy | None
        Delta schema policy derived from the dataset location, when present.
    """
    if location.delta_schema_policy is not None:
        return location.delta_schema_policy
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_schema_policy
    return None


def resolve_delta_maintenance_policy(location: DatasetLocation) -> DeltaMaintenancePolicy | None:
    """Return Delta maintenance policy for a dataset location.

    Returns
    -------
    DeltaMaintenancePolicy | None
        Delta maintenance policy derived from the dataset location, when present.
    """
    if location.delta_maintenance_policy is not None:
        return location.delta_maintenance_policy
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_maintenance_policy
    return None


def resolve_delta_feature_gate(location: DatasetLocation) -> DeltaFeatureGate | None:
    """Return Delta protocol/feature gate requirements for a dataset location.

    Returns
    -------
    DeltaFeatureGate | None
        Feature gate requirements for the dataset when configured.
    """
    if location.delta_feature_gate is not None:
        return location.delta_feature_gate
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_feature_gate
    return None


def resolve_dataset_schema(location: DatasetLocation) -> SchemaLike | None:
    """Return the resolved schema for a dataset location.

    Returns
    -------
    SchemaLike | None
        Resolved schema when available, otherwise ``None``.

    Raises
    ------
    ValueError
        Raised when Delta schema resolution fails for a Delta dataset.
    """
    scan = resolve_datafusion_scan_options(location)
    if scan is not None and scan.table_schema_contract is not None:
        return scan.table_schema_contract.file_schema
    if location.table_spec is not None:
        return location.table_spec.to_arrow_schema()
    if location.dataset_spec is not None:
        return location.dataset_spec.schema()
    if location.format == "delta":
        schema = delta_table_schema(
            DeltaSchemaRequest(
                path=str(location.path),
                storage_options=location.storage_options or None,
                log_storage_options=location.delta_log_storage_options or None,
                version=location.delta_version,
                timestamp=location.delta_timestamp,
                gate=resolve_delta_feature_gate(location),
            )
        )
        if schema is None:
            msg = f"Delta schema unavailable for dataset at {location.path!r}."
            raise ValueError(msg)
        return schema
    return None


__all__ = [
    "DataFusionProvider",
    "DataFusionScanOptions",
    "DatasetCatalog",
    "DatasetFormat",
    "DatasetLocation",
    "DatasetSpec",
    "DeltaScanOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "PathLike",
    "dataset_catalog_from_profile",
    "registry_snapshot",
    "resolve_datafusion_provider",
    "resolve_datafusion_scan_options",
    "resolve_dataset_schema",
    "resolve_delta_cdf_policy",
    "resolve_delta_feature_gate",
    "resolve_delta_log_storage_options",
    "resolve_delta_maintenance_policy",
    "resolve_delta_schema_policy",
    "resolve_delta_write_policy",
]


def __getattr__(name: str) -> object:
    if name in {
        "DataFusionScanOptions",
        "DatasetSpec",
        "DeltaCdfPolicy",
        "DeltaMaintenancePolicy",
        "DeltaScanOptions",
        "DeltaSchemaPolicy",
        "DeltaWritePolicy",
    }:
        from schema_spec import system as schema_system

        return getattr(schema_system, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
