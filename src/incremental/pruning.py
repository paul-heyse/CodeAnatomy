"""Delta file pruning helpers for incremental reads."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds

from datafusion_engine.runtime import DiagnosticsSink
from storage.deltalake import StorageOptions, build_delta_file_index, open_delta_table
from storage.deltalake.file_pruning import FilePruningResult


@dataclass(frozen=True)
class FileScopePolicy:
    """File-id scope for Delta file pruning."""

    file_id_column: str
    file_ids: tuple[str, ...]

    def is_empty(self) -> bool:
        """Return whether this scope is empty.

        Returns
        -------
        bool
            True when no file ids are present.
        """
        return not self.file_ids


def prune_delta_files(
    path: Path,
    policy: FileScopePolicy,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> FilePruningResult:
    """Return candidate files for a file-id scope based on Delta metadata.

    Returns
    -------
    FilePruningResult
        Pruning result with candidate file paths.
    """
    table = open_delta_table(
        str(path),
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    index = build_delta_file_index(table)
    total_files = index.num_rows
    if total_files == 0:
        return FilePruningResult(
            candidate_count=0,
            total_files=0,
            pruned_count=0,
            candidate_paths=[],
        )
    path_values = index.column("path").to_pylist()
    partition_values = index.column("partition_values").to_pylist()
    candidate_paths = _filter_paths_by_partition(
        path_values,
        partition_values,
        policy,
    )
    return FilePruningResult(
        candidate_count=len(candidate_paths),
        total_files=total_files,
        pruned_count=total_files - len(candidate_paths),
        candidate_paths=[str(Path(path) / path_part) for path_part in candidate_paths],
    )


def read_pruned_delta_dataset(
    path: Path,
    policy: FileScopePolicy,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> ds.Dataset | None:
    """Build a PyArrow dataset from the pruned Delta file list.

    Returns
    -------
    pyarrow.dataset.Dataset | None
        Dataset for candidate files, or None when no files match.
    """
    result = prune_delta_files(
        path,
        policy,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    if not result.candidate_paths:
        return None
    return ds.dataset(result.candidate_paths, format="parquet")


def record_pruning_metrics(
    *,
    sink: DiagnosticsSink | None,
    dataset_name: str,
    result: FilePruningResult,
) -> None:
    """Record pruning diagnostics when a sink is configured."""
    if sink is None:
        return
    payload = {
        "dataset_name": dataset_name,
        "candidate_count": result.candidate_count,
        "total_files": result.total_files,
        "pruned_count": result.pruned_count,
        "pruned_percentage": result.pruned_percentage,
        "retention_percentage": result.retention_percentage,
    }
    sink.record_artifact("incremental_file_pruning_v1", payload)


def _filter_paths_by_partition(
    paths: list[Any],
    partition_values: list[Any],
    policy: FileScopePolicy,
) -> list[str]:
    if policy.is_empty():
        return [str(value) for value in paths]
    file_ids = set(policy.file_ids)
    candidates: list[str] = []
    for path_value, partition in zip(paths, partition_values, strict=False):
        if not isinstance(partition, dict):
            return [str(value) for value in paths]
        file_id = partition.get(policy.file_id_column)
        if file_id is None:
            return [str(value) for value in paths]
        if file_id in file_ids:
            candidates.append(str(path_value))
    return candidates


__all__ = [
    "FileScopePolicy",
    "prune_delta_files",
    "read_pruned_delta_dataset",
    "record_pruning_metrics",
]
