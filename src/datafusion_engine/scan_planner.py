"""Delta-aware scan planning for deterministic scheduling."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from datafusion import SessionContext

from datafusion_engine.dataset_registry import DatasetLocation, resolve_delta_feature_gate
from datafusion_engine.delta_control_plane import DeltaSnapshotRequest, delta_add_actions
from datafusion_engine.delta_protocol import DeltaFeatureGate
from datafusion_engine.lineage_datafusion import ScanLineage
from serde_msgspec import dumps_msgpack
from storage.deltalake import build_delta_file_index_from_add_actions
from storage.deltalake.file_pruning import (
    FilePruningPolicy,
    PartitionFilter,
    evaluate_and_select_files,
)

_EQUALS_FILTER_PATTERN = re.compile(
    r"([A-Za-z_][A-Za-z0-9_.]*)\s*(?:=|==)\s*['\"]?([^'\"\s]+)['\"]?"
)
_MIN_QUALIFIED_PARTS = 2


def _scan_unit_key(
    dataset_name: str,
    *,
    delta_version: int | None,
    delta_timestamp: str | None,
    delta_feature_gate: DeltaFeatureGate | None,
    projected_columns: tuple[str, ...],
    pushed_filters: tuple[str, ...],
) -> str:
    payload = {
        "dataset_name": dataset_name,
        "delta_version": delta_version,
        "delta_timestamp": delta_timestamp,
        "delta_feature_gate": _gate_payload(delta_feature_gate),
        "projected_columns": projected_columns,
        "pushed_filters": pushed_filters,
    }
    digest = hashlib.sha256(dumps_msgpack(payload)).hexdigest()[:16]
    if delta_version is not None:
        version_token = str(delta_version)
    elif delta_timestamp:
        version_token = delta_timestamp
    else:
        version_token = "latest"
    return f"scan::{dataset_name}::{version_token}::{digest}"


@dataclass(frozen=True)
class ScanUnit:
    """Deterministic, pruned scan description for scheduling."""

    key: str
    dataset_name: str
    delta_version: int | None
    delta_timestamp: str | None
    snapshot_timestamp: int | None
    delta_feature_gate: DeltaFeatureGate | None
    candidate_files: tuple[Path, ...]
    pushed_filters: tuple[str, ...]
    projected_columns: tuple[str, ...]


def plan_scan_unit(
    ctx: SessionContext,
    *,
    dataset_name: str,
    location: DatasetLocation | None,
    lineage: ScanLineage,
) -> ScanUnit:
    """Plan a deterministic scan unit for a dataset lineage entry.

    Returns
    -------
    ScanUnit
        Deterministic scan unit for the dataset and lineage inputs.
    """
    delta_version = location.delta_version if location is not None else None
    delta_timestamp = None
    if location is not None and delta_version is None:
        delta_timestamp = location.delta_timestamp
    delta_feature_gate = resolve_delta_feature_gate(location) if location is not None else None
    candidate_files: tuple[Path, ...] = ()
    snapshot_timestamp: int | None = None
    if location is not None and location.format == "delta":
        candidate_files, delta_version, snapshot_timestamp = _delta_scan_candidates(
            ctx,
            location=location,
            lineage=lineage,
        )
    key = _scan_unit_key(
        dataset_name,
        delta_version=delta_version,
        delta_timestamp=delta_timestamp,
        delta_feature_gate=delta_feature_gate,
        projected_columns=lineage.projected_columns,
        pushed_filters=lineage.pushed_filters,
    )
    return ScanUnit(
        key=key,
        dataset_name=dataset_name,
        delta_version=delta_version,
        delta_timestamp=delta_timestamp,
        snapshot_timestamp=snapshot_timestamp,
        delta_feature_gate=delta_feature_gate,
        candidate_files=candidate_files,
        pushed_filters=lineage.pushed_filters,
        projected_columns=lineage.projected_columns,
    )


def plan_scan_units(
    ctx: SessionContext,
    *,
    dataset_locations: Mapping[str, DatasetLocation],
    scans_by_task: Mapping[str, Sequence[ScanLineage]],
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
) -> tuple[tuple[Path, ...], int | None, int | None]:
    request = _delta_snapshot_request(location)
    payload = _delta_add_actions_payload(request=request)
    if not payload.add_actions:
        return (), payload.delta_version, payload.snapshot_timestamp
    index = build_delta_file_index_from_add_actions(payload.add_actions)
    policy = _policy_from_lineage(location=location, lineage=lineage)
    pruning = evaluate_and_select_files(index, policy, ctx=ctx)
    candidate_files = tuple(
        Path(str(location.path)) / Path(path) for path in pruning.candidate_paths
    )
    return candidate_files, payload.delta_version, payload.snapshot_timestamp


@dataclass(frozen=True)
class _DeltaAddActionsPayload:
    """Normalized Delta add-action payload for scan planning."""

    delta_version: int | None
    snapshot_timestamp: int | None
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
    add_actions = _coerce_add_actions(response.get("add_actions"))
    return _DeltaAddActionsPayload(
        delta_version=delta_version,
        snapshot_timestamp=snapshot_timestamp,
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
    """Resolve a snapshot timestamp from snapshot metadata."""
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


def _policy_from_lineage(
    *,
    location: DatasetLocation,
    lineage: ScanLineage,
) -> FilePruningPolicy:
    partition_filters: list[PartitionFilter] = []
    partition_columns = _partition_column_names(location)
    if partition_columns and lineage.pushed_filters:
        partition_filters.extend(
            _partition_filters_from_strings(lineage.pushed_filters, partition_columns)
        )
    return FilePruningPolicy(partition_filters=partition_filters, stats_filters=[])


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
