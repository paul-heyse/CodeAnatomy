"""Delta-aware scan planning for deterministic scheduling."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from datafusion import SessionContext

from datafusion_engine.arrow_schema.abi import schema_to_dict
from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_datafusion_provider,
    resolve_delta_feature_gate,
    resolve_delta_scan_options,
)
from datafusion_engine.delta_control_plane import DeltaSnapshotRequest, delta_add_actions
from datafusion_engine.delta_protocol import (
    DeltaFeatureGate,
    DeltaProtocolCompatibility,
    DeltaProtocolSnapshot,
    delta_protocol_compatibility,
)
from datafusion_engine.lineage_datafusion import ScanLineage
from serde_artifacts import DeltaScanConfigSnapshot
from serde_msgspec import dumps_msgpack, to_builtins
from storage.deltalake import build_delta_file_index_from_add_actions
from storage.deltalake.file_pruning import (
    FilePruningPolicy,
    PartitionFilter,
    StatsFilter,
    evaluate_and_select_files,
)

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

_EQUALS_FILTER_PATTERN = re.compile(
    r"([A-Za-z_][A-Za-z0-9_.]*)\s*(?:=|==)\s*['\"]?([^'\"\s]+)['\"]?"
)
_COMPARISON_FILTER_PATTERN = re.compile(
    r"([A-Za-z_][A-Za-z0-9_.]*)\s*(==|!=|>=|<=|=|>|<)\s*['\"]?([^'\"\s]+)['\"]?"
)
_COMPARISON_OPS: tuple[Literal["!=", "<", "<=", "=", ">", ">="], ...] = (
    "!=",
    "<",
    "<=",
    "=",
    ">",
    ">=",
)
_MIN_QUALIFIED_PARTS = 2


@dataclass(frozen=True)
class _ScanUnitKeyRequest:
    """Inputs required to build a scan-unit key."""

    dataset_name: str
    delta_version: int | None
    delta_timestamp: str | None
    delta_feature_gate: DeltaFeatureGate | None
    delta_protocol: DeltaProtocolSnapshot | None
    storage_options_hash: str | None
    delta_scan_config_hash: str | None
    datafusion_provider: str | None
    projected_columns: tuple[str, ...]
    pushed_filters: tuple[str, ...]


def _scan_unit_key(request: _ScanUnitKeyRequest) -> str:
    payload = {
        "dataset_name": request.dataset_name,
        "delta_version": request.delta_version,
        "delta_timestamp": request.delta_timestamp,
        "delta_feature_gate": _gate_payload(request.delta_feature_gate),
        "delta_protocol": request.delta_protocol,
        "storage_options_hash": request.storage_options_hash,
        "delta_scan_config_hash": request.delta_scan_config_hash,
        "datafusion_provider": request.datafusion_provider,
        "projected_columns": request.projected_columns,
        "pushed_filters": request.pushed_filters,
    }
    digest = hashlib.sha256(dumps_msgpack(payload)).hexdigest()[:16]
    if request.delta_version is not None:
        version_token = str(request.delta_version)
    elif request.delta_timestamp:
        version_token = request.delta_timestamp
    else:
        version_token = "latest"
    return f"scan::{request.dataset_name}::{version_token}::{digest}"


@dataclass(frozen=True)
class ScanUnit:
    """Deterministic, pruned scan description for scheduling."""

    key: str
    dataset_name: str
    delta_version: int | None
    delta_timestamp: str | None
    snapshot_timestamp: int | None
    delta_feature_gate: DeltaFeatureGate | None
    delta_protocol: DeltaProtocolSnapshot | None
    storage_options_hash: str | None
    delta_scan_config: DeltaScanConfigSnapshot | None
    delta_scan_config_hash: str | None
    datafusion_provider: str | None
    protocol_compatible: bool | None
    protocol_compatibility: DeltaProtocolCompatibility | None
    total_files: int
    candidate_file_count: int
    pruned_file_count: int
    candidate_files: tuple[Path, ...]
    pushed_filters: tuple[str, ...]
    projected_columns: tuple[str, ...]


@dataclass(frozen=True)
class _ScanPlanArtifactRequest:
    """Inputs required to record a scan plan artifact."""

    runtime_profile: DataFusionRuntimeProfile | None
    dataset_name: str
    location: DatasetLocation
    payload: _DeltaAddActionsPayload
    pruning: object
    lineage: ScanLineage


@dataclass(frozen=True)
class _DeltaScanResolution:
    """Resolved Delta scan metadata and compatibility state."""

    candidate_files: tuple[Path, ...]
    delta_version: int | None
    delta_timestamp: str | None
    snapshot_timestamp: int | None
    delta_protocol: DeltaProtocolSnapshot | None
    total_files: int
    candidate_file_count: int
    pruned_file_count: int
    protocol_compatibility: DeltaProtocolCompatibility | None
    protocol_compatible: bool | None


def plan_scan_unit(
    ctx: SessionContext,
    *,
    dataset_name: str,
    location: DatasetLocation | None,
    lineage: ScanLineage,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> ScanUnit:
    """Plan a deterministic scan unit for a dataset lineage entry.

    Returns
    -------
    ScanUnit
        Deterministic scan unit for the dataset and lineage inputs.
    """
    delta_feature_gate = resolve_delta_feature_gate(location) if location is not None else None
    storage_options_hash = _storage_options_hash(location)
    delta_scan_config = _delta_scan_config_snapshot(location)
    delta_scan_config_hash = _delta_scan_config_hash(delta_scan_config)
    datafusion_provider = _provider_marker(location, runtime_profile=runtime_profile)
    delta_resolution = _resolve_delta_scan_resolution(
        ctx,
        location=location,
        lineage=lineage,
        dataset_name=dataset_name,
        runtime_profile=runtime_profile,
    )
    key = _scan_unit_key(
        _ScanUnitKeyRequest(
            dataset_name=dataset_name,
            delta_version=delta_resolution.delta_version,
            delta_timestamp=delta_resolution.delta_timestamp,
            delta_feature_gate=delta_feature_gate,
            delta_protocol=delta_resolution.delta_protocol,
            storage_options_hash=storage_options_hash,
            delta_scan_config_hash=delta_scan_config_hash,
            datafusion_provider=datafusion_provider,
            projected_columns=lineage.projected_columns,
            pushed_filters=lineage.pushed_filters,
        )
    )
    return ScanUnit(
        key=key,
        dataset_name=dataset_name,
        delta_version=delta_resolution.delta_version,
        delta_timestamp=delta_resolution.delta_timestamp,
        snapshot_timestamp=delta_resolution.snapshot_timestamp,
        delta_feature_gate=delta_feature_gate,
        delta_protocol=delta_resolution.delta_protocol,
        storage_options_hash=storage_options_hash,
        delta_scan_config=delta_scan_config,
        delta_scan_config_hash=delta_scan_config_hash,
        datafusion_provider=datafusion_provider,
        protocol_compatible=delta_resolution.protocol_compatible,
        protocol_compatibility=delta_resolution.protocol_compatibility,
        total_files=delta_resolution.total_files,
        candidate_file_count=delta_resolution.candidate_file_count,
        pruned_file_count=delta_resolution.pruned_file_count,
        candidate_files=delta_resolution.candidate_files,
        pushed_filters=lineage.pushed_filters,
        projected_columns=lineage.projected_columns,
    )


def _resolve_delta_scan_resolution(
    ctx: SessionContext,
    *,
    location: DatasetLocation | None,
    lineage: ScanLineage,
    dataset_name: str,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> _DeltaScanResolution:
    """Resolve Delta scan metadata and compatibility state.

    Returns
    -------
    _DeltaScanResolution
        Resolved Delta scan metadata and compatibility signals.
    """
    delta_version = location.delta_version if location is not None else None
    delta_timestamp = None
    if location is not None and delta_version is None:
        delta_timestamp = location.delta_timestamp
    resolution = _DeltaScanResolution(
        candidate_files=(),
        delta_version=delta_version,
        delta_timestamp=delta_timestamp,
        snapshot_timestamp=None,
        delta_protocol=None,
        total_files=0,
        candidate_file_count=0,
        pruned_file_count=0,
        protocol_compatibility=None,
        protocol_compatible=None,
    )
    if location is None or location.format != "delta":
        return resolution
    (
        candidate_files,
        delta_version,
        snapshot_timestamp,
        delta_protocol,
        total_files,
        candidate_file_count,
        pruned_file_count,
    ) = _delta_scan_candidates(
        ctx,
        location=location,
        lineage=lineage,
        dataset_name=dataset_name,
        runtime_profile=runtime_profile,
    )
    protocol_compatibility = _delta_protocol_compatibility(
        delta_protocol,
        runtime_profile,
    )
    protocol_compatible = _compatibility_flag(protocol_compatibility)
    _enforce_protocol_compatibility(
        dataset_name,
        runtime_profile=runtime_profile,
        protocol_compatible=protocol_compatible,
        protocol_compatibility=protocol_compatibility,
    )
    return _DeltaScanResolution(
        candidate_files=candidate_files,
        delta_version=delta_version,
        delta_timestamp=delta_timestamp,
        snapshot_timestamp=snapshot_timestamp,
        delta_protocol=delta_protocol,
        total_files=total_files,
        candidate_file_count=candidate_file_count,
        pruned_file_count=pruned_file_count,
        protocol_compatibility=protocol_compatibility,
        protocol_compatible=protocol_compatible,
    )


def plan_scan_units(
    ctx: SessionContext,
    *,
    dataset_locations: Mapping[str, DatasetLocation],
    scans_by_task: Mapping[str, Sequence[ScanLineage]],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> tuple[tuple[ScanUnit, ...], dict[str, tuple[str, ...]]]:
    """Plan scan units for each task based on structured lineage scans.

    Returns
    -------
    tuple[tuple[ScanUnit, ...], dict[str, tuple[str, ...]]]
        Sorted scan units and per-task scan unit keys.
    """
    scan_units: dict[str, ScanUnit] = {}
    scan_keys_by_task: dict[str, tuple[str, ...]] = {}
    for task_name in sorted(scans_by_task):
        scan_keys: list[str] = []
        for scan in scans_by_task[task_name]:
            location = _resolve_dataset_location(scan.dataset_name, dataset_locations)
            unit = plan_scan_unit(
                ctx,
                dataset_name=scan.dataset_name,
                location=location,
                lineage=scan,
                runtime_profile=runtime_profile,
            )
            scan_units.setdefault(unit.key, unit)
            scan_keys.append(unit.key)
        scan_keys_by_task[task_name] = tuple(dict.fromkeys(scan_keys))
    units_sorted = tuple(sorted(scan_units.values(), key=lambda unit: unit.key))
    return units_sorted, scan_keys_by_task


def _delta_scan_candidates(
    ctx: SessionContext,
    *,
    location: DatasetLocation,
    lineage: ScanLineage,
    dataset_name: str,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> tuple[
    tuple[Path, ...],
    int | None,
    int | None,
    DeltaProtocolSnapshot | None,
    int,
    int,
    int,
]:
    request = _delta_snapshot_request(location)
    payload = _delta_add_actions_payload(request=request)
    empty_candidates: tuple[Path, ...] = ()
    if not payload.add_actions:
        return (
            empty_candidates,
            payload.delta_version,
            payload.snapshot_timestamp,
            payload.delta_protocol,
            0,
            0,
            0,
        )
    index = build_delta_file_index_from_add_actions(payload.add_actions)
    policy = _policy_from_lineage(location=location, lineage=lineage)
    pruning = evaluate_and_select_files(index, policy, ctx=ctx)
    candidate_files = tuple(
        Path(str(location.path)) / Path(path) for path in pruning.candidate_paths
    )
    _record_scan_plan_artifact(
        _ScanPlanArtifactRequest(
            runtime_profile=runtime_profile,
            dataset_name=dataset_name,
            location=location,
            payload=payload,
            pruning=pruning,
            lineage=lineage,
        )
    )
    return (
        candidate_files,
        payload.delta_version,
        payload.snapshot_timestamp,
        payload.delta_protocol,
        pruning.total_files,
        pruning.candidate_count,
        pruning.pruned_count,
    )


@dataclass(frozen=True)
class _DeltaAddActionsPayload:
    """Normalized Delta add-action payload for scan planning."""

    delta_version: int | None
    snapshot_timestamp: int | None
    delta_protocol: DeltaProtocolSnapshot | None
    add_actions: tuple[Mapping[str, object], ...]


def _delta_snapshot_request(location: DatasetLocation) -> DeltaSnapshotRequest:
    """Build a control-plane snapshot request for a dataset location.

    Returns
    -------
    DeltaSnapshotRequest
        Snapshot request pinned to version or timestamp when provided.
    """
    pinned_version = location.delta_version
    pinned_timestamp = location.delta_timestamp if pinned_version is None else None
    return DeltaSnapshotRequest(
        table_uri=str(location.path),
        storage_options=_delta_storage_options(location),
        version=pinned_version,
        timestamp=pinned_timestamp,
        gate=resolve_delta_feature_gate(location),
    )


def _gate_payload(
    gate: DeltaFeatureGate | None,
) -> tuple[int | None, int | None, tuple[str, ...], tuple[str, ...]] | None:
    if gate is None:
        return None
    return (
        gate.min_reader_version,
        gate.min_writer_version,
        tuple(gate.required_reader_features),
        tuple(gate.required_writer_features),
    )


def _delta_add_actions_payload(
    *,
    request: DeltaSnapshotRequest,
) -> _DeltaAddActionsPayload:
    """Resolve add actions and Delta version from the control plane.

    Returns
    -------
    _DeltaAddActionsPayload
        Delta version plus normalized add-action payloads.

    Raises
    ------
    TypeError
        Raised when the control-plane response is missing snapshot metadata.
    """
    response = delta_add_actions(request)
    snapshot = response.get("snapshot")
    if not isinstance(snapshot, Mapping):
        msg = "Delta add-actions response missing snapshot mapping."
        raise TypeError(msg)
    delta_version = _resolve_delta_version(snapshot, pinned_version=request.version)
    snapshot_timestamp = _resolve_snapshot_timestamp(snapshot)
    delta_protocol = _delta_protocol_payload(snapshot)
    add_actions = _coerce_add_actions(response.get("add_actions"))
    return _DeltaAddActionsPayload(
        delta_version=delta_version,
        snapshot_timestamp=snapshot_timestamp,
        delta_protocol=delta_protocol,
        add_actions=add_actions,
    )


def _resolve_delta_version(
    snapshot: Mapping[str, object],
    *,
    pinned_version: int | None,
) -> int | None:
    """Resolve a Delta version from snapshot metadata.

    Returns
    -------
    int | None
        Resolved Delta version, or the pinned version when unavailable.
    """
    version_value = snapshot.get("version")
    if isinstance(version_value, int):
        return version_value
    if isinstance(version_value, str) and version_value.strip():
        try:
            return int(version_value)
        except ValueError:
            return pinned_version
    return pinned_version


def _resolve_snapshot_timestamp(snapshot: Mapping[str, object]) -> int | None:
    """Resolve a snapshot timestamp from snapshot metadata.

    Returns
    -------
    int | None
        Snapshot timestamp as an integer when available.
    """
    if not snapshot:
        return None
    timestamp = snapshot.get("snapshot_timestamp")
    if isinstance(timestamp, int):
        return timestamp
    if isinstance(timestamp, float):
        return int(timestamp)
    if isinstance(timestamp, str) and timestamp.strip():
        try:
            return int(timestamp)
        except ValueError:
            return None
    return None


def _delta_protocol_payload(snapshot: Mapping[str, object]) -> DeltaProtocolSnapshot | None:
    min_reader_version = _coerce_int(snapshot.get("min_reader_version"))
    min_writer_version = _coerce_int(snapshot.get("min_writer_version"))
    reader_features = tuple(_coerce_str_list(snapshot.get("reader_features")))
    writer_features = tuple(_coerce_str_list(snapshot.get("writer_features")))
    if (
        min_reader_version is None
        and min_writer_version is None
        and not reader_features
        and not writer_features
    ):
        return None
    return DeltaProtocolSnapshot(
        min_reader_version=min_reader_version,
        min_writer_version=min_writer_version,
        reader_features=reader_features,
        writer_features=writer_features,
    )


def _delta_scan_config_snapshot(
    location: DatasetLocation | None,
) -> DeltaScanConfigSnapshot | None:
    if location is None:
        return None
    options = resolve_delta_scan_options(location)
    if options is None:
        return None
    schema_payload = schema_to_dict(options.schema) if options.schema is not None else None
    return DeltaScanConfigSnapshot(
        file_column_name=options.file_column_name,
        enable_parquet_pushdown=options.enable_parquet_pushdown,
        schema_force_view_types=options.schema_force_view_types,
        wrap_partition_values=options.wrap_partition_values,
        schema=dict(schema_payload) if schema_payload is not None else None,
    )


def _delta_scan_config_hash(snapshot: DeltaScanConfigSnapshot | None) -> str | None:
    if snapshot is None:
        return None
    payload = dumps_msgpack(snapshot)
    return hashlib.sha256(payload).hexdigest()


def _provider_marker(
    location: DatasetLocation | None,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> str | None:
    if location is None:
        return None
    provider = resolve_datafusion_provider(location)
    if provider is not None:
        return provider
    format_name = str(location.format or "")
    if format_name == "delta":
        if runtime_profile is not None and runtime_profile.enable_delta_ddl_registration:
            return "delta_ddl"
        return "delta_table_provider"
    return format_name or None


def _delta_protocol_compatibility(
    protocol: DeltaProtocolSnapshot | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaProtocolCompatibility | None:
    if runtime_profile is None:
        return None
    support = runtime_profile.delta_protocol_support
    return delta_protocol_compatibility(protocol, support)


def _compatibility_flag(payload: DeltaProtocolCompatibility | None) -> bool | None:
    if payload is None:
        return None
    return payload.compatible


def _enforce_protocol_compatibility(
    dataset_name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    protocol_compatible: bool | None,
    protocol_compatibility: DeltaProtocolCompatibility | None,
) -> None:
    if runtime_profile is None or protocol_compatible is not False:
        return
    mode = runtime_profile.delta_protocol_mode
    if mode == "ignore":
        return
    compatibility_payload: dict[str, object]
    if protocol_compatibility is None:
        compatibility_payload = {}
    else:
        compatibility_payload = cast("dict[str, object]", to_builtins(protocol_compatibility))
    payload = {
        "dataset_name": dataset_name,
        "compatibility": compatibility_payload,
        "mode": mode,
    }
    if mode == "warn":
        runtime_profile.record_artifact("delta_protocol_incompatible_v1", payload)
        return
    msg = f"Delta protocol compatibility failed for dataset {dataset_name!r}."
    raise ValueError(msg)


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_str_list(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value if str(item)]
    return []


def _coerce_add_actions(add_actions_raw: object) -> tuple[Mapping[str, object], ...]:
    """Coerce add actions into a tuple of mapping payloads.

    Returns
    -------
    tuple[Mapping[str, object], ...]
        Normalized add-action mappings.
    """
    if isinstance(add_actions_raw, Sequence) and not isinstance(
        add_actions_raw, (str, bytes, bytearray)
    ):
        return tuple(action for action in add_actions_raw if isinstance(action, Mapping))
    return ()


def _delta_storage_options(location: DatasetLocation) -> Mapping[str, str] | None:
    """Normalize Delta storage and log-store options.

    Returns
    -------
    Mapping[str, str] | None
        Combined storage options, or ``None`` when no options are provided.
    """
    storage_options = {key: str(value) for key, value in dict(location.storage_options).items()}
    log_storage_options = {
        key: str(value) for key, value in dict(location.delta_log_storage_options).items()
    }
    if log_storage_options:
        storage_options.update(log_storage_options)
    if not storage_options:
        return None
    return storage_options


def _storage_options_hash(location: DatasetLocation | None) -> str | None:
    if location is None:
        return None
    storage_options = {
        str(key): str(value) for key, value in dict(location.storage_options).items()
    }
    log_storage = {
        str(key): str(value) for key, value in dict(location.delta_log_storage_options).items()
    }
    if log_storage:
        storage_options.update(log_storage)
    if not storage_options:
        return None
    payload = tuple(sorted(storage_options.items()))
    return hashlib.sha256(dumps_msgpack(payload)).hexdigest()


def _record_scan_plan_artifact(request: _ScanPlanArtifactRequest) -> None:
    if request.runtime_profile is None:
        return
    from datafusion_engine.delta_observability import (
        DeltaScanPlanArtifact,
        record_delta_scan_plan,
    )

    total_files = getattr(request.pruning, "total_files", 0)
    candidate_files = getattr(request.pruning, "candidate_count", 0)
    pruned_files = getattr(request.pruning, "pruned_count", 0)
    record_delta_scan_plan(
        request.runtime_profile,
        artifact=DeltaScanPlanArtifact(
            dataset_name=request.dataset_name,
            table_uri=str(request.location.path),
            delta_version=request.payload.delta_version,
            snapshot_timestamp=request.payload.snapshot_timestamp,
            total_files=int(total_files),
            candidate_files=int(candidate_files),
            pruned_files=int(pruned_files),
            pushed_filters=request.lineage.pushed_filters,
            projected_columns=request.lineage.projected_columns,
            delta_protocol=request.payload.delta_protocol,
            delta_feature_gate=resolve_delta_feature_gate(request.location),
            storage_options_hash=_storage_options_hash(request.location),
        ),
    )


def _policy_from_lineage(
    *,
    location: DatasetLocation,
    lineage: ScanLineage,
) -> FilePruningPolicy:
    partition_filters: list[PartitionFilter] = []
    stats_filters: list[StatsFilter] = []
    partition_columns = _partition_column_names(location)
    if partition_columns and lineage.pushed_filters:
        partition_filters.extend(
            _partition_filters_from_strings(lineage.pushed_filters, partition_columns)
        )
    if lineage.pushed_filters:
        stats_filters.extend(_stats_filters_from_strings(lineage.pushed_filters, partition_columns))
    return FilePruningPolicy(partition_filters=partition_filters, stats_filters=stats_filters)


def _partition_column_names(location: DatasetLocation) -> set[str]:
    scan = location.datafusion_scan
    if scan is None:
        return set()
    return {name for name, _dtype in scan.partition_cols}


def _partition_filters_from_strings(
    filters: Sequence[str],
    partition_columns: set[str],
) -> list[PartitionFilter]:
    resolved: list[PartitionFilter] = []
    for filter_text in filters:
        match = _EQUALS_FILTER_PATTERN.search(filter_text)
        if match is None:
            continue
        column = match.group(1).split(".")[-1]
        if column not in partition_columns:
            continue
        value = match.group(2)
        resolved.append(PartitionFilter(column=column, op="=", value=value))
    return resolved


def _stats_filters_from_strings(
    filters: Sequence[str],
    partition_columns: set[str],
) -> list[StatsFilter]:
    resolved: list[StatsFilter] = []
    for filter_text in filters:
        match = _COMPARISON_FILTER_PATTERN.search(filter_text)
        if match is None:
            continue
        column = match.group(1).split(".")[-1]
        if column in partition_columns:
            continue
        op = _normalize_stats_op(match.group(2))
        if op is None:
            continue
        value_text = match.group(3)
        value, cast_type = _parse_filter_value(value_text)
        resolved.append(
            StatsFilter(
                column=column,
                op=op,
                value=value,
                cast_type=cast_type,
            )
        )
    return resolved


def _normalize_stats_op(value: str) -> Literal["!=", "<", "<=", "=", ">", ">="] | None:
    normalized = value
    if normalized == "==":
        normalized = "="
    if normalized in _COMPARISON_OPS:
        return cast("Literal['!=', '<', '<=', '=', '>', '>=']", normalized)
    return None


def _parse_filter_value(value: str) -> tuple[object, str | None]:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true", "Boolean"
    try:
        return int(value), "Int64"
    except ValueError:
        pass
    try:
        return float(value), "Float64"
    except ValueError:
        return value, None


def _resolve_dataset_location(
    dataset_name: str,
    locations: Mapping[str, DatasetLocation],
) -> DatasetLocation | None:
    if dataset_name in locations:
        return locations[dataset_name]
    parts = dataset_name.split(".")
    candidates = [dataset_name]
    if parts:
        candidates.append(parts[-1])
    if len(parts) >= _MIN_QUALIFIED_PARTS:
        candidates.append(".".join(parts[-2:]))
    for candidate in candidates:
        if candidate in locations:
            return locations[candidate]
    return None


__all__ = ["ScanUnit", "plan_scan_unit", "plan_scan_units"]
