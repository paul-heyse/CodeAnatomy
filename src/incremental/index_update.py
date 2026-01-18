"""Incremental module and import index updates."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.io.delta import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
)
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
    data = coerce_delta_table(
        module_index,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = upsert_dataset_partitions_delta(
        data,
        options=DeltaUpsertOptions(
            base_dir=str(state_store.dataset_dir(_MODULE_INDEX_DATASET)),
            partition_cols=("file_id",),
            delete_partitions=delete_partitions,
            options=DeltaWriteOptions(schema_mode="merge"),
        ),
    )
    return {_MODULE_INDEX_DATASET: result.path}


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
    data = coerce_delta_table(
        imports_resolved,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = upsert_dataset_partitions_delta(
        data,
        options=DeltaUpsertOptions(
            base_dir=str(state_store.dataset_dir(_IMPORTS_RESOLVED_DATASET)),
            partition_cols=("importer_file_id",),
            delete_partitions=delete_partitions,
            options=DeltaWriteOptions(schema_mode="merge"),
        ),
    )
    return {_IMPORTS_RESOLVED_DATASET: result.path}


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["upsert_imports_resolved", "upsert_module_index"]
