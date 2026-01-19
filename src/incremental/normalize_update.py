"""Incremental normalize dataset updates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import encoding_policy_from_schema
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges
from normalize.registry_specs import dataset_name_from_alias
from storage.deltalake import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
)


def upsert_normalize_outputs(
    outputs: Mapping[str, TableLike],
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert normalize outputs into the incremental state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    delete_partitions = _partition_specs("file_id", changes.deleted_file_ids)
    updated: dict[str, str] = {}
    for alias, table_like in outputs.items():
        try:
            dataset_name = dataset_name_from_alias(alias)
        except KeyError:
            continue
        table = table_like
        if "file_id" not in table.column_names:
            continue
        data = coerce_delta_table(
            table,
            schema=table.schema,
            encoding_policy=encoding_policy_from_schema(table.schema),
        )
        result = upsert_dataset_partitions_delta(
            data,
            options=DeltaUpsertOptions(
                base_dir=str(state_store.dataset_dir(dataset_name)),
                partition_cols=("file_id",),
                delete_partitions=delete_partitions,
                options=DeltaWriteOptions(schema_mode="merge"),
            ),
        )
        updated[dataset_name] = result.path
    return updated


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["upsert_normalize_outputs"]
