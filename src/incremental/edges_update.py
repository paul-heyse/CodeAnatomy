"""Incremental CPG edge updates."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import encoding_policy_from_schema
from cpg.schemas import CPG_EDGES_SCHEMA
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges
from storage.deltalake import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
)

_CPG_EDGES_DATASET = "cpg_edges_v1"


def upsert_cpg_edges(
    edges: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert CPG edges into the incremental state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    if "edge_owner_file_id" not in edges.column_names:
        return {}
    delete_partitions = _partition_specs("edge_owner_file_id", changes.deleted_file_ids)
    data = coerce_delta_table(
        edges,
        schema=CPG_EDGES_SCHEMA,
        encoding_policy=encoding_policy_from_schema(CPG_EDGES_SCHEMA),
    )
    result = upsert_dataset_partitions_delta(
        data,
        options=DeltaUpsertOptions(
            base_dir=str(state_store.dataset_dir(_CPG_EDGES_DATASET)),
            partition_cols=("edge_owner_file_id",),
            delete_partitions=delete_partitions,
            options=DeltaWriteOptions(schema_mode="merge"),
        ),
    )
    return {_CPG_EDGES_DATASET: result.path}


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["upsert_cpg_edges"]
