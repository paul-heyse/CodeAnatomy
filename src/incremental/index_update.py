"""Incremental module and import index updates."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.io.parquet import DatasetWriteConfig, upsert_dataset_partitions_parquet
from arrowdsl.schema.metadata import encoding_policy_from_schema
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges

_MODULE_INDEX_DATASET = "py_module_index_v1"
_IMPORTS_RESOLVED_DATASET = "py_imports_resolved_v1"


def upsert_module_index(
    module_index: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert module index partitions by file_id.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    if "file_id" not in module_index.column_names:
        return {}
    delete_partitions = _partition_specs("file_id", changes.deleted_file_ids)
    schema = dataset_schema(_MODULE_INDEX_DATASET)
    path = upsert_dataset_partitions_parquet(
        module_index,
        base_dir=state_store.dataset_dir(_MODULE_INDEX_DATASET),
        partition_cols=("file_id",),
        delete_partitions=delete_partitions,
        config=DatasetWriteConfig(
            schema=schema,
            encoding_policy=encoding_policy_from_schema(schema),
        ),
    )
    return {_MODULE_INDEX_DATASET: path}


def upsert_imports_resolved(
    imports_resolved: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert resolved import partitions by importer_file_id.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    if "importer_file_id" not in imports_resolved.column_names:
        return {}
    delete_partitions = _partition_specs("importer_file_id", changes.deleted_file_ids)
    schema = dataset_schema(_IMPORTS_RESOLVED_DATASET)
    path = upsert_dataset_partitions_parquet(
        imports_resolved,
        base_dir=state_store.dataset_dir(_IMPORTS_RESOLVED_DATASET),
        partition_cols=("importer_file_id",),
        delete_partitions=delete_partitions,
        config=DatasetWriteConfig(
            schema=schema,
            encoding_policy=encoding_policy_from_schema(schema),
        ),
    )
    return {_IMPORTS_RESOLVED_DATASET: path}


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["upsert_imports_resolved", "upsert_module_index"]
