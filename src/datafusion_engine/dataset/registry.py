"""DataFusion-native dataset location and registry helpers."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from functools import cached_property
from typing import TYPE_CHECKING, Literal, cast

from arrow_utils.core.ordering import OrderingLevel
from core_types import PathLike
from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.identity import schema_identity_hash
from schema_spec.specs import TableSchemaSpec
from serde_msgspec import to_builtins
from storage.deltalake import DeltaCdfOptions, DeltaSchemaRequest, delta_table_schema
from utils.registry_protocol import MutableRegistry

if TYPE_CHECKING:
    from datafusion_engine.delta.protocol import DeltaFeatureGate
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
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
class DatasetLocationOverrides:
    """Override-only fields for dataset locations."""

    delta_scan: DeltaScanOptions | None = None
    delta_cdf_policy: DeltaCdfPolicy | None = None
    delta_maintenance_policy: DeltaMaintenancePolicy | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_feature_gate: DeltaFeatureGate | None = None
    delta_constraints: tuple[str, ...] | None = None
    datafusion_scan: DataFusionScanOptions | None = None
    table_spec: TableSchemaSpec | None = None


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
    overrides: DatasetLocationOverrides | None = None

    @cached_property
    def resolved(self) -> ResolvedDatasetLocation:
        """Return a resolved view of this dataset location."""
        return resolve_dataset_location(self)


@dataclass(frozen=True)
class ResolvedDatasetLocation:
    """Resolved view of dataset location overrides and defaults."""

    location: DatasetLocation
    dataset_spec: DatasetSpec | None
    datafusion_scan: DataFusionScanOptions | None
    datafusion_provider: DataFusionProvider | None
    delta_scan: DeltaScanOptions | None
    delta_cdf_policy: DeltaCdfPolicy | None
    delta_log_storage_options: Mapping[str, str] | None
    delta_write_policy: DeltaWritePolicy | None
    delta_schema_policy: DeltaSchemaPolicy | None
    delta_maintenance_policy: DeltaMaintenancePolicy | None
    delta_feature_gate: DeltaFeatureGate | None
    delta_constraints: tuple[str, ...]
    table_spec: TableSchemaSpec | None
    schema: SchemaLike | None


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
    from datafusion_engine.delta.scan_config import delta_scan_config_snapshot_from_options

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

    # Deferred import to avoid circular import with session.runtime
    from datafusion_engine.dataset.semantic_catalog import build_semantic_dataset_catalog
    from datafusion_engine.session.runtime import normalize_dataset_locations_for_profile

    _register_locations(
        catalog,
        profile=profile_ref,
        locations=profile.data_sources.extract_dataset_locations,
    )
    _register_registry_catalog_name(
        catalog,
        profile=profile_ref,
        name=profile.data_sources.extract_output_catalog_name,
    )
    _register_locations(
        catalog,
        profile=profile_ref,
        locations=profile.data_sources.semantic_output_locations,
    )
    _register_registry_catalog_name(
        catalog,
        profile=profile_ref,
        name=profile.data_sources.semantic_output_catalog_name,
    )

    unified_catalog = build_semantic_dataset_catalog(
        semantic_output_root=profile.data_sources.semantic_output_root,
        extract_output_root=profile.data_sources.extract_output_root,
    )
    _register_registry_catalog(
        catalog,
        profile=profile_ref,
        registry=unified_catalog,
    )
    _register_locations(
        catalog,
        profile=profile_ref,
        locations=profile.data_sources.scip_dataset_locations,
    )
    _register_locations(
        catalog,
        profile=profile_ref,
        locations=normalize_dataset_locations_for_profile(profile),
    )
    for registry in profile.catalog.registry_catalogs.values():
        _register_registry_catalog(
            catalog,
            profile=profile_ref,
            registry=registry,
        )
    return catalog


def _register_location(
    catalog: DatasetCatalog,
    *,
    profile: DataFusionRuntimeProfile,
    name: str,
    location: DatasetLocation,
) -> None:
    if catalog.has(name):
        return
    from datafusion_engine.delta.store_policy import apply_delta_store_policy

    resolved = apply_delta_store_policy(location, policy=profile.policies.delta_store_policy)
    catalog.register(name, resolved)


def _register_locations(
    catalog: DatasetCatalog,
    *,
    profile: DataFusionRuntimeProfile,
    locations: Mapping[str, DatasetLocation],
) -> None:
    for name, location in locations.items():
        _register_location(
            catalog,
            profile=profile,
            name=name,
            location=location,
        )


def _register_registry_catalog(
    catalog: DatasetCatalog,
    *,
    profile: DataFusionRuntimeProfile,
    registry: DatasetCatalog,
) -> None:
    for name in registry.names():
        if catalog.has(name):
            continue
        with suppress(KeyError):
            _register_location(
                catalog,
                profile=profile,
                name=name,
                location=registry.get(name),
            )


def _register_registry_catalog_name(
    catalog: DatasetCatalog,
    *,
    profile: DataFusionRuntimeProfile,
    name: str | None,
) -> None:
    if name is None:
        return
    registry = profile.catalog.registry_catalogs.get(name)
    if registry is None:
        return
    _register_registry_catalog(
        catalog,
        profile=profile,
        registry=registry,
    )


def _resolve_override(
    location: DatasetLocation,
    overrides: DatasetLocationOverrides | None,
    field: str,
    *,
    fallback_spec: bool = True,
) -> object | None:
    if overrides is not None:
        value = getattr(overrides, field)
        if value is not None:
            return value
    value = getattr(location, field)
    if value is not None:
        return value
    if fallback_spec and location.dataset_spec is not None:
        return getattr(location.dataset_spec, field, None)
    return None


def _resolve_datafusion_scan(
    location: DatasetLocation,
    overrides: DatasetLocationOverrides | None,
) -> DataFusionScanOptions | None:
    scan = None
    if overrides is not None and overrides.datafusion_scan is not None:
        scan = overrides.datafusion_scan
    elif location.datafusion_scan is not None:
        scan = location.datafusion_scan
    elif location.dataset_spec is not None:
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


def _resolve_delta_scan(
    location: DatasetLocation,
    overrides: DatasetLocationOverrides | None,
) -> DeltaScanOptions | None:
    from storage.deltalake.scan_profile import build_delta_scan_config

    if overrides is not None and overrides.delta_scan is not None:
        location = replace(location, delta_scan=overrides.delta_scan)
    return build_delta_scan_config(location)


def _resolve_dataset_schema_internal(
    location: DatasetLocation,
    *,
    datafusion_scan: DataFusionScanOptions | None,
    table_spec: TableSchemaSpec | None,
    delta_feature_gate: DeltaFeatureGate | None,
    delta_log_storage_options: Mapping[str, str] | None,
) -> SchemaLike | None:
    if datafusion_scan is not None and datafusion_scan.table_schema_contract is not None:
        return datafusion_scan.table_schema_contract.file_schema
    if table_spec is not None:
        return table_spec.to_arrow_schema()
    if location.dataset_spec is not None:
        return location.dataset_spec.schema()
    if location.format == "delta":
        schema = delta_table_schema(
            DeltaSchemaRequest(
                path=str(location.path),
                storage_options=location.storage_options or None,
                log_storage_options=delta_log_storage_options,
                version=location.delta_version,
                timestamp=location.delta_timestamp,
                gate=delta_feature_gate,
            )
        )
        if schema is None:
            msg = f"Delta schema unavailable for dataset at {location.path!r}."
            raise ValueError(msg)
        return schema
    return None


def resolve_dataset_location(location: DatasetLocation) -> ResolvedDatasetLocation:
    """Return a resolved view of a dataset location.

    Returns
    -------
    ResolvedDatasetLocation
        Resolved dataset location with derived configuration.
    """
    overrides = location.overrides
    dataset_spec = location.dataset_spec
    datafusion_scan = _resolve_datafusion_scan(location, overrides)
    delta_scan = _resolve_delta_scan(location, overrides)
    delta_cdf_policy = cast(
        "DeltaCdfPolicy | None",
        _resolve_override(location, overrides, "delta_cdf_policy"),
    )
    delta_write_policy = cast(
        "DeltaWritePolicy | None",
        _resolve_override(location, overrides, "delta_write_policy"),
    )
    delta_schema_policy = cast(
        "DeltaSchemaPolicy | None",
        _resolve_override(location, overrides, "delta_schema_policy"),
    )
    delta_maintenance_policy = cast(
        "DeltaMaintenancePolicy | None",
        _resolve_override(location, overrides, "delta_maintenance_policy"),
    )
    delta_feature_gate = cast(
        "DeltaFeatureGate | None",
        _resolve_override(location, overrides, "delta_feature_gate"),
    )
    delta_constraints = cast(
        "tuple[str, ...]",
        _resolve_override(location, overrides, "delta_constraints") or (),
    )
    table_spec = cast(
        "TableSchemaSpec | None",
        _resolve_override(location, overrides, "table_spec", fallback_spec=False),
    )
    datafusion_provider = location.datafusion_provider
    if datafusion_provider is None:
        if (delta_cdf_policy is not None and delta_cdf_policy.required) or (
            dataset_spec is not None and dataset_spec.dataset_kind == "delta_cdf"
        ):
            datafusion_provider = "delta_cdf"
    delta_log_storage_options = (
        location.delta_log_storage_options or location.storage_options or None
    )
    schema = _resolve_dataset_schema_internal(
        location,
        datafusion_scan=datafusion_scan,
        table_spec=table_spec,
        delta_feature_gate=delta_feature_gate,
        delta_log_storage_options=delta_log_storage_options,
    )
    return ResolvedDatasetLocation(
        location=location,
        dataset_spec=dataset_spec,
        datafusion_scan=datafusion_scan,
        datafusion_provider=datafusion_provider,
        delta_scan=delta_scan,
        delta_cdf_policy=delta_cdf_policy,
        delta_log_storage_options=delta_log_storage_options,
        delta_write_policy=delta_write_policy,
        delta_schema_policy=delta_schema_policy,
        delta_maintenance_policy=delta_maintenance_policy,
        delta_feature_gate=delta_feature_gate,
        delta_constraints=delta_constraints,
        table_spec=table_spec,
        schema=schema,
    )


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
    resolved = resolve_dataset_location(location)
    return resolved.datafusion_scan


def resolve_datafusion_provider(location: DatasetLocation) -> DataFusionProvider | None:
    """Return the effective DataFusion provider for a dataset location.

    Returns
    -------
    DataFusionProvider | None
        Resolved provider for the dataset location, when available.
    """
    return resolve_dataset_location(location).datafusion_provider


def resolve_delta_cdf_policy(location: DatasetLocation) -> DeltaCdfPolicy | None:
    """Return Delta CDF policy for a dataset location.

    Returns
    -------
    DeltaCdfPolicy | None
        Resolved Delta CDF policy when configured.
    """
    return resolve_dataset_location(location).delta_cdf_policy


def resolve_delta_log_storage_options(location: DatasetLocation) -> Mapping[str, str] | None:
    """Return Delta log-store options for a dataset location.

    Returns
    -------
    Mapping[str, str] | None
        Log-store options for Delta table access.
    """
    return resolve_dataset_location(location).delta_log_storage_options


def resolve_delta_write_policy(location: DatasetLocation) -> DeltaWritePolicy | None:
    """Return Delta write policy for a dataset location.

    Returns
    -------
    DeltaWritePolicy | None
        Delta write policy derived from the dataset location, when present.
    """
    return resolve_dataset_location(location).delta_write_policy


def resolve_delta_schema_policy(location: DatasetLocation) -> DeltaSchemaPolicy | None:
    """Return Delta schema policy for a dataset location.

    Returns
    -------
    DeltaSchemaPolicy | None
        Delta schema policy derived from the dataset location, when present.
    """
    return resolve_dataset_location(location).delta_schema_policy


def resolve_delta_maintenance_policy(location: DatasetLocation) -> DeltaMaintenancePolicy | None:
    """Return Delta maintenance policy for a dataset location.

    Returns
    -------
    DeltaMaintenancePolicy | None
        Delta maintenance policy derived from the dataset location, when present.
    """
    return resolve_dataset_location(location).delta_maintenance_policy


def resolve_delta_feature_gate(location: DatasetLocation) -> DeltaFeatureGate | None:
    """Return Delta protocol/feature gate requirements for a dataset location.

    Returns
    -------
    DeltaFeatureGate | None
        Feature gate requirements for the dataset when configured.
    """
    return resolve_dataset_location(location).delta_feature_gate


def resolve_dataset_schema(location: DatasetLocation) -> SchemaLike | None:
    """Return the resolved schema for a dataset location.

    Returns
    -------
    SchemaLike | None
        Resolved schema when available, otherwise ``None``.
    """
    return resolve_dataset_location(location).schema


__all__ = [
    "DataFusionProvider",
    "DataFusionScanOptions",
    "DatasetCatalog",
    "DatasetFormat",
    "DatasetLocation",
    "DatasetLocationOverrides",
    "DatasetSpec",
    "DeltaScanOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "PathLike",
    "ResolvedDatasetLocation",
    "dataset_catalog_from_profile",
    "registry_snapshot",
    "resolve_datafusion_provider",
    "resolve_datafusion_scan_options",
    "resolve_dataset_location",
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
