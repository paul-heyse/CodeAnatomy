"""Delta-aware scan planning for deterministic scheduling."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from datafusion import SessionContext

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.lineage_datafusion import ScanLineage
from serde_msgspec import dumps_msgpack
from storage.deltalake.delta import delta_table_version, open_delta_table
from storage.deltalake.file_index import build_delta_file_index
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
    projected_columns: tuple[str, ...],
    pushed_filters: tuple[str, ...],
) -> str:
    payload = {
        "dataset_name": dataset_name,
        "delta_version": delta_version,
        "projected_columns": projected_columns,
        "pushed_filters": pushed_filters,
    }
    digest = hashlib.sha256(dumps_msgpack(payload)).hexdigest()[:16]
    version_token = "latest" if delta_version is None else str(delta_version)
    return f"scan::{dataset_name}::{version_token}::{digest}"


@dataclass(frozen=True)
class ScanUnit:
    """Deterministic, pruned scan description for scheduling."""

    key: str
    dataset_name: str
    delta_version: int | None
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
    candidate_files: tuple[Path, ...] = ()
    if location is not None and location.format == "delta":
        candidate_files, delta_version = _delta_scan_candidates(
            ctx,
            location=location,
            lineage=lineage,
        )
    key = _scan_unit_key(
        dataset_name,
        delta_version=delta_version,
        projected_columns=lineage.projected_columns,
        pushed_filters=lineage.pushed_filters,
    )
    return ScanUnit(
        key=key,
        dataset_name=dataset_name,
        delta_version=delta_version,
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
) -> tuple[tuple[Path, ...], int | None]:
    table_path = str(location.path)
    storage_options = dict(location.storage_options)
    log_storage_options = dict(location.delta_log_storage_options)
    pinned_version = location.delta_version
    pinned_timestamp = location.delta_timestamp if pinned_version is None else None
    if pinned_version is None and pinned_timestamp is None:
        pinned_version = delta_table_version(
            table_path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
    table = open_delta_table(
        table_path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        version=pinned_version,
        timestamp=pinned_timestamp,
    )
    index = build_delta_file_index(table)
    policy = _policy_from_lineage(location=location, lineage=lineage)
    pruning = evaluate_and_select_files(index, policy, ctx=ctx)
    candidate_files = tuple(Path(table_path) / Path(path) for path in pruning.candidate_paths)
    return candidate_files, table.version()


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
