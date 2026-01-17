"""Incremental exported definitions updates."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.io.parquet import DatasetWriteConfig, upsert_dataset_partitions_parquet
from arrowdsl.schema.metadata import encoding_policy_from_schema
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges

_EXPORTED_DEFS_DATASET = "dim_exported_defs_v1"


def upsert_exported_defs(
    exported_defs: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert exported definition partitions by file_id.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    if "file_id" not in exported_defs.column_names:
        return {}
    delete_partitions = _partition_specs("file_id", changes.deleted_file_ids)
    schema = dataset_schema(_EXPORTED_DEFS_DATASET)
    path = upsert_dataset_partitions_parquet(
        exported_defs,
        base_dir=state_store.dataset_dir(_EXPORTED_DEFS_DATASET),
        partition_cols=("file_id",),
        delete_partitions=delete_partitions,
        config=DatasetWriteConfig(
            schema=schema,
            encoding_policy=encoding_policy_from_schema(schema),
        ),
    )
    return {_EXPORTED_DEFS_DATASET: path}


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["upsert_exported_defs"]
