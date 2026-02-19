"""Delta/provider/schema-evolution helpers for dataset registration."""

from __future__ import annotations

import importlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow.fs as pafs
from datafusion import SessionContext, SQLOptions
from datafusion.dataframe import DataFrame

from arrow_utils.core.ordering import OrderingLevel
from core_types import ensure_path
from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.metadata import ordering_from_schema
from datafusion_engine.dataset.registration_scan import BYTECODE_EXTERNAL_TABLE_NAME
from datafusion_engine.dataset.registration_schema import (
    _partition_schema_validation,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.dataset.resolution import DatasetResolution
from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.delta.protocol import combined_table_features, delta_protocol_compatibility
from datafusion_engine.delta.provider_artifacts import (
    RegistrationProviderArtifactInput,
    build_delta_provider_build_result,
    provider_build_request_from_registration_context,
)
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.session.introspection import schema_introspector_for_profile
from datafusion_engine.session.runtime_dataset_io import cache_prefix_for_delta_snapshot
from datafusion_engine.tables.metadata import TableProviderCapsule
from obs.otel import get_run_id
from schema_spec.dataset_contracts import TableSchemaContract
from schema_spec.dataset_spec_runtime import (
    DatasetSpec,
    dataset_spec_from_schema,
    make_dataset_spec,
)
from schema_spec.scan_options import DataFusionScanOptions, DeltaScanOptions
from storage.deltalake import DeltaCdfOptions
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion_engine.dataset.registration_core import (
        DataFusionRegistrationContext,
        DeltaCdfArtifact,
    )
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

_BYTECODE_EXTERNAL_TABLE_NAME = BYTECODE_EXTERNAL_TABLE_NAME
logger = logging.getLogger(__name__)


def _resolve_dataset_spec(name: str, location: DatasetLocation) -> DatasetSpec | None:
    resolved = location.resolved
    if resolved.dataset_spec is not None:
        return resolved.dataset_spec
    if resolved.table_spec is not None:
        return make_dataset_spec(table_spec=resolved.table_spec)
    schema = resolved.schema
    if schema is None:
        return None
    return dataset_spec_from_schema(name, schema)


@dataclass(frozen=True)
class _DeltaProviderArtifactContext:
    dataset_name: str | None
    provider_mode: str
    ffi_table_provider: bool
    strict_native_provider_enabled: bool | None
    strict_native_provider_violation: bool | None
    delta_scan: DeltaScanOptions | None
    delta_scan_effective: Mapping[str, object] | None
    delta_scan_snapshot: object | None
    delta_scan_identity_hash: str | None
    snapshot: Mapping[str, object] | None
    registration_path: str
    predicate: str | None
    predicate_error: str | None
    add_actions: Sequence[Mapping[str, object]] | None


@dataclass(frozen=True)
class _DeltaProviderRegistration:
    resolution: DatasetResolution
    predicate_sql: str | None
    predicate_error: str | None


@dataclass(frozen=True)
class _DeltaRegistrationState:
    registration: _DeltaProviderRegistration
    resolution: DatasetResolution
    adapter: DataFusionIOAdapter
    provider: object
    provider_to_register: object
    provider_is_native: bool
    format_name: str
    cache_prefix: str | None


@dataclass(frozen=True)
class _DeltaRegistrationResult:
    df: DataFrame
    cache_prefix: str | None


def _delta_pruning_predicate(
    context: DataFusionRegistrationContext,
) -> tuple[str | None, str | None]:
    dataset_spec = _resolve_dataset_spec(context.name, context.location)
    if dataset_spec is None:
        return None, None
    from schema_spec.dataset_spec import dataset_spec_query

    query_spec = dataset_spec_query(dataset_spec)
    predicate = query_spec.pushdown_predicate or query_spec.predicate
    if predicate is None:
        return None, None
    try:
        return predicate.to_sql(), None
    except (TypeError, ValueError) as exc:
        return None, str(exc)


def _scan_files_from_add_actions(
    add_actions: Sequence[Mapping[str, object]] | None,
) -> tuple[str, ...]:
    if not add_actions:
        return ()
    return tuple(
        str(entry["path"])
        for entry in add_actions
        if isinstance(entry, Mapping) and entry.get("path") is not None
    )


def _provider_for_registration(provider: object) -> object:
    if hasattr(provider, "__datafusion_table_provider__"):
        return provider
    try:
        import pyarrow.dataset as ds
        from datafusion.catalog import Table
        from datafusion.dataframe import DataFrame
    except ImportError:
        return TableProviderCapsule(provider)
    if isinstance(provider, (ds.Dataset, DataFrame, Table)):
        return provider
    return TableProviderCapsule(provider)


def _cache_prefix_for_registration(
    context: DataFusionRegistrationContext,
    *,
    resolution: DatasetResolution,
) -> str | None:
    if context.runtime_profile is None:
        return None
    if context.runtime_profile.policies.snapshot_pinned_mode != "delta_version":
        return None
    return cache_prefix_for_delta_snapshot(
        context.runtime_profile,
        dataset_name=context.name,
        snapshot=resolution.delta_snapshot,
    )


def _enforce_delta_native_provider_policy(
    state: _DeltaRegistrationState,
    context: DataFusionRegistrationContext,
) -> None:
    if state.format_name != "delta" or state.provider_is_native:
        return
    msg = (
        "Delta provider registration fell back to a PyArrow dataset; "
        "FFI TableProvider is required for pushdown and correctness."
    )
    if (
        context.runtime_profile is not None
        and context.runtime_profile.features.enforce_delta_ffi_provider
    ):
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    logger.warning(msg)


def _record_delta_snapshot_if_applicable(
    context: DataFusionRegistrationContext,
    *,
    resolution: DatasetResolution,
    schema_identity_hash_value: str | None,
    ddl_fingerprint: str | None,
) -> None:
    if resolution.delta_snapshot is None:
        return
    from datafusion_engine.delta.observability import (
        DELTA_MAINTENANCE_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_SNAPSHOT_TABLE_NAME,
        DeltaSnapshotArtifact,
        record_delta_snapshot,
    )

    observability_tables = {
        DELTA_SNAPSHOT_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_MAINTENANCE_TABLE_NAME,
    }
    if context.name in observability_tables:
        return
    record_delta_snapshot(
        context.runtime_profile,
        artifact=DeltaSnapshotArtifact(
            table_uri=str(context.location.path),
            snapshot=resolution.delta_snapshot,
            dataset_name=context.name,
            schema_identity_hash=schema_identity_hash_value,
            ddl_fingerprint=ddl_fingerprint,
        ),
        ctx=context.ctx,
    )


def _delta_provider_artifact_payload(
    ctx: SessionContext,
    location: DatasetLocation,
    *,
    context: _DeltaProviderArtifactContext,
) -> dict[str, object]:
    compatibility = is_delta_extension_compatible(
        ctx,
        entrypoint="delta_table_provider_from_session",
        require_non_fallback=bool(context.strict_native_provider_enabled),
    )
    request = provider_build_request_from_registration_context(
        RegistrationProviderArtifactInput(
            table_uri=str(location.path),
            dataset_format=location.format,
            provider_kind="delta",
            compatibility=compatibility,
            context=context,
        )
    )
    request = replace(
        request,
        delta_version=location.delta_version,
        delta_timestamp=location.delta_timestamp,
        delta_log_storage_options=(
            dict(location.delta_log_storage_options) if location.delta_log_storage_options else None
        ),
        delta_storage_options=(
            dict(location.storage_options) if location.storage_options else None
        ),
    )
    return build_delta_provider_build_result(request).as_payload()


def _record_delta_log_health(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    name: str,
    location: DatasetLocation,
    resolution: DatasetResolution,
) -> None:
    if runtime_profile is None:
        return
    if location.format != "delta":
        return
    payload = _delta_log_health_payload(
        name=name,
        location=location,
        resolution=resolution,
        runtime_profile=runtime_profile,
    )
    from serde_artifact_specs import DELTA_LOG_HEALTH_SPEC

    record_artifact(runtime_profile, DELTA_LOG_HEALTH_SPEC, payload)


def _delta_log_health_payload(
    *,
    name: str,
    location: DatasetLocation,
    resolution: DatasetResolution,
    runtime_profile: DataFusionRuntimeProfile,
) -> dict[str, object]:
    delta_log_present, checkpoint_version, log_reason, log_count = _delta_log_status(location)
    snapshot = resolution.delta_snapshot
    min_reader = _snapshot_int(snapshot, "min_reader_version")
    min_writer = _snapshot_int(snapshot, "min_writer_version")
    reader_features = _snapshot_features(snapshot, "reader_features")
    writer_features = _snapshot_features(snapshot, "writer_features")
    compatibility = delta_protocol_compatibility(
        snapshot,
        runtime_profile.policies.delta_protocol_support,
    )
    table_features = list(combined_table_features(compatibility))
    protocol_compatible = compatibility.compatible
    severity = _delta_health_severity(
        delta_log_present=delta_log_present,
        protocol_compatible=protocol_compatible,
    )
    return {
        "dataset": name,
        "path": str(location.path),
        "delta_log_present": delta_log_present,
        "delta_log_reason": log_reason,
        "delta_log_file_count": log_count,
        "last_checkpoint_version": checkpoint_version,
        "min_reader_version": min_reader,
        "min_writer_version": min_writer,
        "reader_features": reader_features,
        "writer_features": writer_features,
        "table_features": table_features,
        "protocol_compatible": protocol_compatible,
        "protocol_reason": compatibility.reason,
        "missing_reader_features": list(compatibility.missing_reader_features),
        "missing_writer_features": list(compatibility.missing_writer_features),
        "run_id": get_run_id(),
        "diagnostic.severity": severity,
        "diagnostic.category": "delta_protocol",
    }


def _delta_log_status(
    location: DatasetLocation,
) -> tuple[bool | None, int | None, str | None, int | None]:
    path_text = str(location.path)
    parsed = urlparse(path_text)
    if parsed.scheme and parsed.scheme != "file":
        return None, None, "remote_path", None
    path = Path(parsed.path if parsed.scheme == "file" and parsed.path else path_text)
    delta_log = path / "_delta_log"
    if not delta_log.exists():
        return False, None, "delta_log_missing", None
    try:
        json_files = list(delta_log.glob("*.json"))
    except OSError:
        return False, None, "delta_log_unreadable", None
    if not json_files:
        return False, None, "delta_log_empty", 0
    checkpoint_version = _read_last_checkpoint_version(delta_log)
    return True, checkpoint_version, None, len(json_files)


def _read_last_checkpoint_version(delta_log: Path) -> int | None:
    checkpoint = delta_log / "_last_checkpoint"
    if not checkpoint.exists():
        return None
    try:
        with checkpoint.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if isinstance(payload, Mapping):
        return coerce_int(payload.get("version"))
    return None


def _delta_health_severity(
    *,
    delta_log_present: bool | None,
    protocol_compatible: bool | None,
) -> str:
    if protocol_compatible is False:
        return "error"
    if delta_log_present is False:
        return "warn"
    return "info"


def _snapshot_int(snapshot: Mapping[str, object] | None, key: str) -> int | None:
    if snapshot is None:
        return None
    return coerce_int(snapshot.get(key))


def _snapshot_features(snapshot: Mapping[str, object] | None, key: str) -> list[str]:
    if snapshot is None:
        return []
    values = snapshot.get(key)
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        return [str(item) for item in values if str(item)]
    return []


def _delta_cdf_artifact_payload(
    location: DatasetLocation,
    *,
    resolution: DatasetResolution,
) -> dict[str, object]:
    return {
        "path": str(location.path),
        "format": location.format,
        "delta_log_storage_options": (
            dict(location.delta_log_storage_options) if location.delta_log_storage_options else None
        ),
        "delta_storage_options": (
            dict(location.storage_options) if location.storage_options else None
        ),
        "delta_snapshot": resolution.delta_snapshot,
        "cdf_options": _cdf_options_payload(location.delta_cdf_options),
    }


def _validate_ordering_contract(
    context: DataFusionRegistrationContext,
    *,
    scan: DataFusionScanOptions | None,
) -> None:
    if context.name != _BYTECODE_EXTERNAL_TABLE_NAME:
        return
    schema = context.options.schema
    if schema is None:
        return
    ordering = ordering_from_schema(schema)
    if ordering.level != OrderingLevel.EXPLICIT or not ordering.keys:
        return
    expected = [list(key) for key in ordering.keys]
    if scan is None or not scan.file_sort_order:
        msg = f"{context.name} ordering requires file_sort_order {expected}."
        raise ValueError(msg)
    if [list(key) for key in scan.file_sort_order] != expected:
        msg = (
            f"{context.name} file_sort_order {[list(key) for key in scan.file_sort_order]} does not match "
            f"schema ordering {expected}."
        )
        raise ValueError(msg)


def _populate_schema_adapter_factories(
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionRuntimeProfile:
    if not runtime_profile.features.enable_schema_evolution_adapter:
        return runtime_profile
    if (
        runtime_profile.policies.schema_adapter_factories
        or not runtime_profile.catalog.registry_catalogs
    ):
        return runtime_profile
    factories: dict[str, object] = {}
    adapter_factory: object | None = None
    for catalog in runtime_profile.catalog.registry_catalogs.values():
        for name in catalog.names():
            location = catalog.get(name)
            spec = _resolve_dataset_spec(name, location)
            if spec is None:
                continue
            if not _requires_schema_evolution_adapter(spec.evolution_spec):
                continue
            if adapter_factory is None:
                adapter_factory = _schema_evolution_adapter_factory()
            factories.setdefault(name, adapter_factory)
    if not factories:
        return runtime_profile
    merged = dict(runtime_profile.policies.schema_adapter_factories)
    for name, factory in factories.items():
        merged.setdefault(name, factory)
    return replace(
        runtime_profile,
        policies=replace(
            runtime_profile.policies,
            schema_adapter_factories=merged,
        ),
    )


def _resolve_expr_adapter_factory(
    scan: DataFusionScanOptions | None,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    dataset_name: str,
    location: DatasetLocation,
) -> object | None:
    if scan is not None and scan.expr_adapter_factory is not None:
        return scan.expr_adapter_factory
    if runtime_profile is not None:
        factory = runtime_profile.policies.schema_adapter_factories.get(dataset_name)
        if factory is not None:
            return factory
    spec = _resolve_dataset_spec(dataset_name, location)
    if spec is not None and _requires_schema_evolution_adapter(spec.evolution_spec):
        return _schema_evolution_adapter_factory()
    if (
        runtime_profile is not None
        and runtime_profile.policies.physical_expr_adapter_factory is not None
    ):
        return runtime_profile.policies.physical_expr_adapter_factory
    return None


def _ensure_expr_adapter_factory(
    ctx: SessionContext,
    *,
    factory: object | None,
    evolution_required: bool,
) -> None:
    if factory is None:
        if evolution_required:
            msg = "Schema evolution adapter is required but not configured."
            raise ValueError(msg)
        return
    register = getattr(ctx, "register_physical_expr_adapter_factory", None)
    if callable(register):
        try:
            register(factory)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Failed to register physical expr adapter factory: {exc}"
            raise ValueError(msg) from exc
        return
    if evolution_required:
        msg = (
            "Schema evolution adapter is required but SessionContext does not support "
            "physical expr adapter registration."
        )
        raise TypeError(msg)
    msg = "SessionContext does not support physical expr adapter registration."
    raise TypeError(msg)


def _requires_schema_evolution_adapter(evolution: object) -> bool:
    rename_map = getattr(evolution, "rename_map", None)
    allow_missing = bool(getattr(evolution, "allow_missing", False))
    allow_extra = bool(getattr(evolution, "allow_extra", False))
    allow_casts = bool(getattr(evolution, "allow_casts", False))
    return bool(rename_map) or allow_missing or allow_extra or allow_casts


def _schema_evolution_adapter_factory() -> object:
    module = None
    for module_name in ("datafusion_engine.extensions.datafusion_ext",):
        try:
            candidate = importlib.import_module(module_name)
        except ImportError:
            continue
        if hasattr(candidate, "schema_evolution_adapter_factory"):
            module = candidate
            break
    if module is None:
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg)
    factory = getattr(module, "schema_evolution_adapter_factory", None)
    if not callable(factory):
        msg = "schema_evolution_adapter_factory is unavailable in the extension module."
        raise TypeError(msg)
    return factory()


def _fingerprints_match(left: str | None, right: str | None) -> bool | None:
    if left is None or right is None:
        return None
    return left == right


def _table_schema_contract_payload(
    contract: TableSchemaContract | None,
) -> dict[str, object] | None:
    if contract is None:
        return None
    partition_schema = contract.partition_schema()
    return {
        "file_schema": schema_to_dict(contract.file_schema),
        "partition_cols": [
            {"name": name, "dtype": str(dtype)} for name, dtype in contract.partition_cols_pyarrow()
        ],
        "partition_schema": schema_to_dict(partition_schema)
        if partition_schema is not None
        else None,
    }


def _optional_bool(value: object | None) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _schema_evolution_payload(evolution: object | None) -> dict[str, object] | None:
    if evolution is None:
        return None
    rename_map = getattr(evolution, "rename_map", None)
    rename_payload = (
        {str(key): str(value) for key, value in rename_map.items()}
        if isinstance(rename_map, Mapping)
        else None
    )
    return {
        "promote_options": getattr(evolution, "promote_options", None),
        "rename_map": rename_payload,
        "allow_missing": _optional_bool(getattr(evolution, "allow_missing", None)),
        "allow_extra": _optional_bool(getattr(evolution, "allow_extra", None)),
        "allow_casts": _optional_bool(getattr(evolution, "allow_casts", None)),
    }


def _schema_evolution_details(
    context: DataFusionRegistrationContext,
) -> tuple[dict[str, object] | None, bool | None]:
    spec = _resolve_dataset_spec(context.name, context.location)
    if spec is None:
        return None, None
    evolution_spec = spec.evolution_spec
    return (
        _schema_evolution_payload(evolution_spec),
        _requires_schema_evolution_adapter(evolution_spec),
    )


def _adapter_factory_payload(factory: object | None) -> str | None:
    if factory is None:
        return None
    class_name = getattr(factory, "__class__", None)
    if class_name is not None:
        return class_name.__name__
    return repr(factory)


@dataclass(frozen=True)
class _TableProvenanceRequest:
    ctx: SessionContext
    name: str
    enable_information_schema: bool
    sql_options: SQLOptions
    runtime_profile: DataFusionRuntimeProfile | None = None
    cache_prefix: str | None = None


def _table_provenance_snapshot(
    request: _TableProvenanceRequest,
) -> dict[str, object]:
    if request.runtime_profile is not None:
        introspector = schema_introspector_for_profile(
            request.runtime_profile,
            request.ctx,
            cache_prefix=request.cache_prefix,
        )
    else:
        introspector = SchemaIntrospector(request.ctx, sql_options=request.sql_options)
    table_definition = introspector.table_definition(request.name)
    constraints: tuple[str, ...] = ()
    column_defaults: dict[str, object] | None = None
    logical_plan: str | None = None
    if request.enable_information_schema:
        constraints = introspector.table_constraints(request.name)
        column_defaults = introspector.table_column_defaults(request.name) or None
        logical_plan = introspector.table_logical_plan(request.name)
    return {
        "table_definition": table_definition,
        "table_constraints": list(constraints) if constraints else None,
        "column_defaults": column_defaults,
        "logical_plan": logical_plan,
    }


@dataclass(frozen=True)
class _PartitionSchemaContext:
    ctx: SessionContext
    table_name: str
    enable_information_schema: bool
    runtime_profile: DataFusionRuntimeProfile | None = None


def _require_partition_schema_validation(
    context: _PartitionSchemaContext,
    *,
    expected_partition_cols: Sequence[tuple[str, str]] | None,
) -> None:
    validation = _partition_schema_validation(
        context,
        expected_partition_cols=expected_partition_cols,
    )
    if validation is None:
        return
    missing_value = validation.get("missing_partition_cols")
    missing: list[str] = []
    if isinstance(missing_value, Sequence) and not isinstance(
        missing_value,
        (str, bytes, bytearray),
    ):
        missing = [str(item) for item in missing_value]
    order_matches = validation.get("partition_order_matches")
    type_value = validation.get("partition_type_mismatches")
    type_mismatches: list[dict[str, str]] = []
    if isinstance(type_value, Sequence) and not isinstance(
        type_value,
        (str, bytes, bytearray),
    ):
        type_mismatches = [
            {str(key): str(val) for key, val in item.items()}
            for item in type_value
            if isinstance(item, Mapping)
        ]
    if missing or order_matches is False or type_mismatches:
        msg = f"Partition schema validation failed for {context.table_name}: {validation}."
        raise ValueError(msg)


def _normalize_filesystem(filesystem: object) -> pafs.FileSystem | None:
    if isinstance(filesystem, pafs.FileSystem):
        return filesystem
    handler = getattr(pafs, "FSSpecHandler", None)
    if handler is not None:
        try:
            return pafs.PyFileSystem(handler(filesystem))
        except (TypeError, ValueError):
            return None
    if isinstance(filesystem, pafs.FileSystemHandler):
        return pafs.PyFileSystem(filesystem)
    return None


def _record_delta_cdf_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaCdfArtifact,
) -> None:
    payload: dict[str, object] = {
        "name": artifact.name,
        "path": artifact.path,
        "provider": artifact.provider,
        "options": _cdf_options_payload(artifact.options),
        "delta_log_storage_options": (
            dict(artifact.log_storage_options) if artifact.log_storage_options else None
        ),
        "delta_snapshot": artifact.snapshot,
    }
    from serde_artifact_specs import DATAFUSION_DELTA_CDF_SPEC

    record_artifact(runtime_profile, DATAFUSION_DELTA_CDF_SPEC, payload)


def _cdf_options_payload(options: DeltaCdfOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "columns": list(options.columns) if options.columns is not None else None,
        "predicate": options.predicate,
        "allow_out_of_range": options.allow_out_of_range,
    }


def _scheme_prefix(path: str | Path) -> str | None:
    if not isinstance(path, str):
        return None
    parsed = urlparse(path)
    if not parsed.scheme:
        return None
    return f"{parsed.scheme}://"


def _merge_kwargs(base: Mapping[str, object], extra: Mapping[str, object]) -> dict[str, object]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _effective_skip_metadata(location: DatasetLocation, scan: DataFusionScanOptions) -> bool:
    return scan.skip_metadata and not _has_metadata_sidecars(location.path)


def _has_metadata_sidecars(path: str | Path) -> bool:
    if isinstance(path, str) and "://" in path:
        return False
    base = ensure_path(path)
    if not base.exists():
        return False
    if base.is_dir():
        return (base / "_common_metadata").exists() or (base / "_metadata").exists()
    return False
