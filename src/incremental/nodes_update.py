"""Incremental CPG node updates."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.io.parquet import DatasetWriteConfig, upsert_dataset_partitions_parquet
from arrowdsl.schema.metadata import encoding_policy_from_schema
from cpg.schemas import CPG_NODES_SCHEMA
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges

_CPG_NODES_DATASET = "cpg_nodes_v1"


def upsert_cpg_nodes(
    nodes: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert CPG nodes into the incremental state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    if "file_id" not in nodes.column_names:
        return {}
    delete_partitions = _partition_specs("file_id", changes.deleted_file_ids)
    path = upsert_dataset_partitions_parquet(
        nodes,
        base_dir=state_store.dataset_dir(_CPG_NODES_DATASET),
        partition_cols=("file_id",),
        delete_partitions=delete_partitions,
        config=DatasetWriteConfig(
            schema=CPG_NODES_SCHEMA,
            encoding_policy=encoding_policy_from_schema(CPG_NODES_SCHEMA),
        ),
    )
    return {_CPG_NODES_DATASET: path}


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["upsert_cpg_nodes"]
