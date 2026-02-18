"""Dataset registration surfaces for DataFusion SessionContext.

This module provides dataset registration utilities that bind datasets to
DataFusion's catalog via native registration APIs and DDL surfaces. All IO
contracts are specified through DataFusion registration parameters and options.

Registration surfaces:
- register_object_store(): Object store routing (S3, GCS, Azure, etc.)
- CREATE EXTERNAL TABLE: DDL-based registration with schema and options
- register_table(): TableProvider registration with custom capsules

Schema discovery and validation query DataFusion's catalog and information_schema
views after registration, ensuring DataFusion is the source of truth for all
table metadata.

Schema drift and evolution:
Schema adapters are attached at registration time via the runtime profile's
physical expression adapter factory. When `enable_schema_evolution_adapter=True`
in the runtime profile, scan-time adapters handle schema drift resolution at
the TableProvider boundary, eliminating the need for downstream cast/projection
transforms. This ensures schema normalization happens during physical plan
execution rather than in post-processing.

Write routing:
All write operations should use `WritePipeline` from `datafusion_engine.write_pipeline`,
which provides DataFusion-native write surfaces (write_parquet, write_csv, write_json,
write_table, and streaming Delta writes via provider inserts).
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse
from weakref import WeakKeyDictionary

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import ensure_path
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.catalog.introspection import (
    invalidate_introspection_cache,
)
from datafusion_engine.dataset.ddl_types import ddl_type_alias
from datafusion_engine.dataset.registration_delta_helpers import (
    _cache_prefix_for_registration,
    _delta_cdf_artifact_payload,
    _delta_provider_artifact_payload,
    _delta_pruning_predicate,
    _DeltaProviderArtifactContext,
    _DeltaProviderRegistration,
    _DeltaRegistrationResult,
    _DeltaRegistrationState,
    _enforce_delta_native_provider_policy,
    _PartitionSchemaContext,
    _populate_schema_adapter_factories,
    _provider_for_registration,
    _record_delta_cdf_artifact,
    _record_delta_log_health,
    _record_delta_snapshot_if_applicable,
    _scheme_prefix,
)
from datafusion_engine.dataset.registration_input import (
    dataset_input_plugin,
    input_plugin_prefixes,
)
from datafusion_engine.dataset.registration_projection import (
    _apply_projection_exprs,
    _projection_exprs_for_schema,
    _sql_literal_for_field,
)
from datafusion_engine.dataset.registration_provider import (
    TableProviderArtifact as _TableProviderArtifact,
)
from datafusion_engine.dataset.registration_provider import (
    record_table_provider_artifact as _record_table_provider_artifact,
)
from datafusion_engine.dataset.registration_provider import (
    table_provider_capsule as _table_provider_capsule,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_capabilities as _update_table_provider_capabilities,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_fingerprints as _update_table_provider_fingerprints,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_scan_config as _update_table_provider_scan_config,
)
from datafusion_engine.dataset.registration_scan import (
    apply_scan_defaults as _apply_scan_defaults,
)
from datafusion_engine.dataset.registration_validation import (
    _expected_column_defaults,
)
from datafusion_engine.dataset.registry import (
    DatasetLocation,
)
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.schema.contracts import (
    table_constraint_definitions,
    table_constraints_from_location,
)
from datafusion_engine.schema.validation import _datafusion_type_name
from datafusion_engine.session.introspection import schema_introspector_for_profile
from datafusion_engine.tables.metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
)
from schema_spec.scan_options import DataFusionScanOptions
from storage.deltalake import DeltaCdfOptions

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

_INPUT_PLUGIN_PREFIXES = ("artifact://", "dataset://", "repo://")
_DDL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

logger = logging.getLogger(__name__)


def _sql_type_name(dtype: pa.DataType) -> str:
    dtype_name = _datafusion_type_name(dtype)
    alias = ddl_type_alias(dtype_name)
    return alias or dtype_name


@dataclass(frozen=True)
class DataFusionRegistryOptions:
    """Resolved DataFusion registration options for a dataset."""

    scan: DataFusionScanOptions | None
    schema: SchemaLike | None
    read_options: Mapping[str, object]
    cache: bool
    provider: Literal["listing", "delta_cdf"] | None


@dataclass(frozen=True)
class DeltaCdfArtifact:
    """Diagnostics payload for Delta CDF registration."""

    name: str
    path: str
    provider: str
    options: DeltaCdfOptions | None
    log_storage_options: Mapping[str, str] | None
    snapshot: Mapping[str, object] | None = None


@dataclass(frozen=True)
class DataFusionCachePolicy(FingerprintableConfig):
    """Cache policy overrides for DataFusion dataset registration."""

    enabled: bool | None = None
    max_columns: int | None = None
    storage: Literal["memory", "delta_staging"] = "memory"

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for cache policy overrides.

        Returns:
        -------
        Mapping[str, object]
            Payload describing cache policy overrides.
        """
        return {
            "enabled": self.enabled,
            "max_columns": self.max_columns,
            "storage": self.storage,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for cache policy overrides.

        Returns:
        -------
        str
            Deterministic fingerprint for the cache policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class _DataFusionCacheSettings:
    """Resolved cache settings for DataFusion registration."""

    enabled: bool
    max_columns: int | None
    storage: Literal["memory", "delta_staging"]


@dataclass(frozen=True)
class DataFusionRegistrationContext:
    """Inputs needed to register a dataset with DataFusion."""

    ctx: SessionContext
    name: str
    location: DatasetLocation
    options: DataFusionRegistryOptions
    cache: _DataFusionCacheSettings
    caches: DatasetCaches
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass
class DatasetCaches:
    """Injectable mutable cache container for dataset registration state."""

    cached_datasets: WeakKeyDictionary[SessionContext, set[str]] = field(
        default_factory=WeakKeyDictionary
    )
    registered_catalogs: WeakKeyDictionary[SessionContext, set[str]] = field(
        default_factory=WeakKeyDictionary
    )
    registered_schemas: WeakKeyDictionary[SessionContext, set[tuple[str, str]]] = field(
        default_factory=WeakKeyDictionary
    )


def _resolve_dataset_caches(caches: DatasetCaches | None) -> DatasetCaches:
    return caches or DatasetCaches()


def _resolve_registry_options_for_location(
    location: DatasetLocation,
) -> DataFusionRegistryOptions:
    """Resolve DataFusion registration hints for a dataset location.

    Args:
        location: Description.

    Returns:
        DataFusionRegistryOptions: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    resolved = location.resolved
    scan = resolved.datafusion_scan
    schema = resolved.schema
    provider = resolved.datafusion_provider
    format_name = location.format or "delta"
    cdf_policy = resolved.delta_cdf_policy
    if provider == "dataset":
        msg = "DataFusion dataset providers are not supported; use listing or native formats."
        raise ValueError(msg)
    if cdf_policy is not None and cdf_policy.required:
        if format_name != "delta":
            msg = "Delta CDF policy requires delta-format datasets."
            raise ValueError(msg)
        provider = "delta_cdf"
    if provider == "delta_cdf" and format_name != "delta":
        msg = "Delta CDF provider requires delta-format datasets."
        raise ValueError(msg)
    if provider is None and format_name == "delta" and location.delta_cdf_options is not None:
        provider = "delta_cdf"
    if provider is None and format_name != "delta":
        provider = "listing"
    if provider is None and _prefers_listing_table(location, scan=scan):
        provider = "listing"
    return DataFusionRegistryOptions(
        scan=scan,
        schema=schema,
        read_options=dict(location.read_options),
        cache=bool(scan.cache) if scan is not None else False,
        provider=provider,
    )


def _apply_runtime_scan_hardening(
    options: DataFusionRegistryOptions,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRegistryOptions:
    if runtime_profile is None:
        return options
    hardened_scan = _scan_hardening_defaults(options.scan, runtime_profile=runtime_profile)
    updated = options
    if hardened_scan is not options.scan:
        updated = replace(updated, scan=hardened_scan)
    return updated


def _scan_hardening_defaults(
    scan: DataFusionScanOptions | None,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionScanOptions | None:
    if scan is None or scan.schema_force_view_types is not None:
        return scan
    enable_view_types = _schema_hardening_view_types(runtime_profile)
    if not enable_view_types:
        return scan
    return replace(scan, schema_force_view_types=True)


def _schema_hardening_view_types(runtime_profile: DataFusionRuntimeProfile) -> bool:
    hardening = runtime_profile.policies.schema_hardening
    if hardening is not None:
        return hardening.enable_view_types
    return runtime_profile.policies.schema_hardening_name == "arrow_performance"


def _prefers_listing_table(
    location: DatasetLocation,
    *,
    scan: DataFusionScanOptions | None,
) -> bool:
    if location.format is None or location.format == "delta":
        return False
    if location.files:
        return False
    if scan is not None and (scan.partition_cols or scan.file_sort_order):
        return True
    return _path_is_directory(location.path)


def _path_is_directory(path: str | Path) -> bool:
    if isinstance(path, str) and "://" in path:
        parsed = urlparse(path)
        if parsed.path.endswith("/"):
            return True
        return not Path(parsed.path).suffix
    resolved = ensure_path(path)
    if resolved.exists():
        return resolved.is_dir()
    return not resolved.suffix


@dataclass(frozen=True)
class _ExternalTableDdlRequest:
    name: str
    location: DatasetLocation
    format_name: str
    column_defs: tuple[str, ...]
    partition_cols: tuple[str, ...]
    ordering: tuple[tuple[str, str], ...]
    options: Mapping[str, str]
    unbounded: bool


def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    overwrite: bool = True,
) -> DataFrame:
    """Register a dataset location with DataFusion and return a DataFrame.

    Returns:
    -------
    DataFrame
        Registered DataFrame bound to ``name``.
    """
    from datafusion_engine.session.helpers import deregister_table
    from datafusion_engine.tables.registration import (
        TableRegistrationRequest,
        register_table,
    )

    if runtime_profile is not None:
        from datafusion_engine.registry_facade import registry_facade_for_context
        from semantics.program_manifest import ManifestDatasetBindings

        facade = registry_facade_for_context(
            ctx,
            runtime_profile=runtime_profile,
            dataset_resolver=ManifestDatasetBindings(locations={}),
        )
        return facade.register_dataset_df(
            name=name,
            location=location,
            cache_policy=cache_policy,
            overwrite=overwrite,
        )
    existing = False
    if overwrite:
        from datafusion_engine.schema.introspection_core import table_names_snapshot

        existing = name in table_names_snapshot(ctx)
        deregister_table(ctx, name)
    df = register_table(
        ctx,
        TableRegistrationRequest(
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=runtime_profile,
        ),
    )
    scan = location.resolved.datafusion_scan
    if existing and scan is not None and scan.listing_mutable and runtime_profile is not None:
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DATAFUSION_LISTING_REFRESH_SPEC

        record_artifact(
            runtime_profile,
            DATAFUSION_LISTING_REFRESH_SPEC,
            {
                "name": name,
                "path": str(location.path),
                "format": location.format,
                "event_time_unix_ms": int(time.time() * 1000),
            },
        )
    return df


def _build_registration_context(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
    caches: DatasetCaches | None = None,
) -> DataFusionRegistrationContext:
    resolved_caches = _resolve_dataset_caches(caches)
    location = _apply_scan_defaults(name, location)
    runtime_profile = _prepare_runtime_profile(runtime_profile, ctx=ctx, caches=resolved_caches)
    options = _resolve_registry_options(name, location, runtime_profile=runtime_profile)
    if options.provider == "listing":
        _register_object_store_for_location(ctx, location, runtime_profile=runtime_profile)
    cache = _resolve_cache_policy(
        options,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )
    context = DataFusionRegistrationContext(
        ctx=ctx,
        name=name,
        location=location,
        options=options,
        cache=cache,
        caches=resolved_caches,
        runtime_profile=runtime_profile,
    )
    provider_metadata: dict[str, str] = {}
    if location.delta_version is not None:
        provider_metadata["delta_version"] = str(location.delta_version)
    if location.delta_timestamp is not None:
        provider_metadata["delta_timestamp"] = str(location.delta_timestamp)
    if location.delta_cdf_options is not None:
        provider_metadata["delta_cdf_enabled"] = "true"
    if location.datafusion_provider is not None:
        provider_metadata["datafusion_provider"] = str(location.datafusion_provider)
    default_values: dict[str, object] = {}
    resolved_table_spec = location.resolved.table_spec
    if resolved_table_spec is not None:
        default_values = dict(_expected_column_defaults(resolved_table_spec.to_arrow_schema()))
    schema_adapter_enabled = (
        runtime_profile is not None and runtime_profile.features.enable_schema_evolution_adapter
    )
    metadata = TableProviderMetadata(
        table_name=name,
        ddl=None,
        constraints=table_constraint_definitions(table_constraints_from_location(location)),
        default_values=default_values,
        storage_location=str(location.path),
        file_format=location.format or "unknown",
        partition_columns=tuple(col for col, _ in options.scan.partition_cols)
        if options.scan and options.scan.partition_cols
        else (),
        unbounded=options.scan.unbounded if options.scan else False,
        ddl_fingerprint=None,
        metadata=provider_metadata,
        schema_adapter_enabled=schema_adapter_enabled,
    )
    record_table_provider_metadata(ctx, metadata=metadata)
    return context


def _register_object_store_for_location(
    ctx: SessionContext,
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    if location.filesystem is None:
        return
    scheme = _scheme_prefix(location.path)
    if scheme is None:
        return
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    adapter.register_object_store(
        scheme=scheme,
        store=location.filesystem,
        host=None,
    )


def _prepare_runtime_profile(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    ctx: SessionContext,
    caches: DatasetCaches | None = None,
) -> DataFusionRuntimeProfile | None:
    if runtime_profile is None:
        return None
    runtime_profile = _populate_schema_adapter_factories(runtime_profile)
    resolved_caches = _resolve_dataset_caches(caches)
    _ensure_catalog_schema(
        ctx,
        catalog=runtime_profile.catalog.default_catalog,
        schema=runtime_profile.catalog.default_schema,
        caches=resolved_caches,
    )
    return runtime_profile


def _resolve_registry_options(
    _name: str,
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRegistryOptions:
    options = _resolve_registry_options_for_location(location)
    return _apply_runtime_scan_hardening(options, runtime_profile=runtime_profile)


def _invalidate_information_schema_cache(
    runtime_profile: DataFusionRuntimeProfile | None,
    ctx: SessionContext,
    *,
    cache_prefix: str | None = None,
) -> None:
    if runtime_profile is None or not runtime_profile.catalog.enable_information_schema:
        return
    invalidate_introspection_cache(ctx)
    introspector = schema_introspector_for_profile(
        runtime_profile,
        ctx,
        cache_prefix=cache_prefix,
    )
    introspector.invalidate_cache()


def _resolve_cache_policy(
    options: DataFusionRegistryOptions,
    *,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> _DataFusionCacheSettings:
    from datafusion_engine.dataset.registration_cache import _resolve_cache_policy as _resolve_cache

    return _resolve_cache(
        options,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )


def _ensure_catalog_schema(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
    caches: DatasetCaches | None = None,
) -> None:
    from datafusion_engine.dataset.registration_listing import (
        _ensure_catalog_schema as _ensure_schema,
    )

    _ensure_schema(
        ctx,
        catalog=catalog,
        schema=schema,
        caches=caches,
    )


def _register_dataset_with_context(
    context: DataFusionRegistrationContext,
) -> DataFrame:
    from datafusion_engine.dataset.registration_listing import (
        _register_dataset_with_context as _register_dataset,
    )

    return _register_dataset(context)


def _maybe_cache(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    from datafusion_engine.dataset.registration_cache import _maybe_cache as _cache_df

    return _cache_df(context, df)


def cached_dataset_names(
    ctx: SessionContext,
    *,
    caches: DatasetCaches | None = None,
) -> tuple[str, ...]:
    """Return cached dataset names for a session context."""
    from datafusion_engine.dataset.registration_cache import (
        cached_dataset_names as _cached_dataset_names,
    )

    return _cached_dataset_names(ctx, caches=caches)


# Public aliases used by extracted registration modules to avoid private imports.
DataFusionCacheSettings = _DataFusionCacheSettings
DDL_IDENTIFIER_RE = _DDL_IDENTIFIER_RE
ExternalTableDdlRequest = _ExternalTableDdlRequest
PartitionSchemaContext = _PartitionSchemaContext
DeltaProviderArtifactContext = _DeltaProviderArtifactContext
DeltaProviderRegistration = _DeltaProviderRegistration
DeltaRegistrationState = _DeltaRegistrationState
DeltaRegistrationResult = _DeltaRegistrationResult
TableProviderArtifact = _TableProviderArtifact
apply_projection_exprs = _apply_projection_exprs
cache_prefix_for_registration = _cache_prefix_for_registration
delta_cdf_artifact_payload = _delta_cdf_artifact_payload
delta_provider_artifact_payload = _delta_provider_artifact_payload
delta_pruning_predicate = _delta_pruning_predicate
enforce_delta_native_provider_policy = _enforce_delta_native_provider_policy
expected_column_defaults = _expected_column_defaults
invalidate_information_schema_cache = _invalidate_information_schema_cache
maybe_cache = _maybe_cache
projection_exprs_for_schema = _projection_exprs_for_schema
provider_for_registration = _provider_for_registration
record_delta_cdf_artifact = _record_delta_cdf_artifact
record_delta_log_health = _record_delta_log_health
record_delta_snapshot_if_applicable = _record_delta_snapshot_if_applicable
record_table_provider_artifact = _record_table_provider_artifact
resolve_dataset_caches = _resolve_dataset_caches
sql_literal_for_field = _sql_literal_for_field
sql_type_name = _sql_type_name
table_provider_capsule = _table_provider_capsule
update_table_provider_capabilities = _update_table_provider_capabilities
update_table_provider_fingerprints = _update_table_provider_fingerprints
update_table_provider_scan_config = _update_table_provider_scan_config


__all__ = [
    "DDL_IDENTIFIER_RE",
    "DataFusionCachePolicy",
    "DataFusionCacheSettings",
    "DataFusionRegistryOptions",
    "DeltaProviderArtifactContext",
    "DeltaProviderRegistration",
    "DeltaRegistrationResult",
    "DeltaRegistrationState",
    "ExternalTableDdlRequest",
    "PartitionSchemaContext",
    "TableProviderArtifact",
    "apply_projection_exprs",
    "cache_prefix_for_registration",
    "cached_dataset_names",
    "dataset_input_plugin",
    "delta_cdf_artifact_payload",
    "delta_provider_artifact_payload",
    "delta_pruning_predicate",
    "enforce_delta_native_provider_policy",
    "expected_column_defaults",
    "input_plugin_prefixes",
    "invalidate_information_schema_cache",
    "maybe_cache",
    "projection_exprs_for_schema",
    "provider_for_registration",
    "record_delta_cdf_artifact",
    "record_delta_log_health",
    "record_delta_snapshot_if_applicable",
    "record_table_provider_artifact",
    "register_dataset_df",
    "resolve_dataset_caches",
    "sql_literal_for_field",
    "sql_type_name",
    "table_provider_capsule",
    "update_table_provider_capabilities",
    "update_table_provider_fingerprints",
    "update_table_provider_scan_config",
]
