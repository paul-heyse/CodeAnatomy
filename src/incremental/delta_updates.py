"""Unified Delta dataset update helpers for incremental pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import encoding_policy_from_schema
from cpg.schemas import CPG_EDGES_SCHEMA, CPG_NODES_SCHEMA
from datafusion_engine.extract_bundles import dataset_name_for_output
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges
from normalize.registry_runtime import dataset_name_from_alias
from storage.deltalake import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
    write_table_delta,
)


@dataclass(frozen=True)
class PartitionedDatasetSpec:
    """Partitioned dataset descriptor for incremental upserts."""

    name: str
    partition_column: str
    schema: pa.Schema | None = None


def upsert_partitioned_dataset(
    table: TableLike,
    *,
    spec: PartitionedDatasetSpec,
    base_dir: str,
    changes: IncrementalFileChanges,
) -> str | None:
    """Upsert a partitioned dataset using the incremental delete set.

    Returns
    -------
    str | None
        Dataset path when updated, otherwise ``None``.
    """
    if spec.partition_column not in table.column_names:
        return None
    schema = spec.schema or table.schema
    delete_partitions = _partition_specs(spec.partition_column, changes.deleted_file_ids)
    data = coerce_delta_table(
        table,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = upsert_dataset_partitions_delta(
        data,
        options=DeltaUpsertOptions(
            base_dir=base_dir,
            partition_cols=(spec.partition_column,),
            delete_partitions=delete_partitions,
            options=DeltaWriteOptions(schema_mode="merge"),
        ),
    )
    return result.path


def write_overwrite_dataset(
    table: TableLike,
    *,
    dataset_name: str,
    schema: pa.Schema,
    state_store: StateStore,
    commit_metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Overwrite a dataset with schema enforcement.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    data = coerce_delta_table(
        table,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = write_table_delta(
        data,
        str(state_store.dataset_dir(dataset_name)),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=dict(commit_metadata) if commit_metadata else None,
        ),
    )
    return {dataset_name: result.path}


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
    spec = PartitionedDatasetSpec(
        name="cpg_nodes_v1",
        partition_column="file_id",
        schema=CPG_NODES_SCHEMA,
    )
    path = upsert_partitioned_dataset(
        nodes,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
    )
    return {} if path is None else {spec.name: path}


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
    spec = PartitionedDatasetSpec(
        name="cpg_edges_v1",
        partition_column="edge_owner_file_id",
        schema=CPG_EDGES_SCHEMA,
    )
    path = upsert_partitioned_dataset(
        edges,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
    )
    return {} if path is None else {spec.name: path}


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
    spec = PartitionedDatasetSpec(
        name="dim_exported_defs_v1",
        partition_column="file_id",
        schema=dataset_schema("dim_exported_defs_v1"),
    )
    path = upsert_partitioned_dataset(
        exported_defs,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
    )
    return {} if path is None else {spec.name: path}


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
    spec = PartitionedDatasetSpec(
        name="py_module_index_v1",
        partition_column="file_id",
        schema=dataset_schema("py_module_index_v1"),
    )
    path = upsert_partitioned_dataset(
        module_index,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
    )
    return {} if path is None else {spec.name: path}


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
    spec = PartitionedDatasetSpec(
        name="py_imports_resolved_v1",
        partition_column="importer_file_id",
        schema=dataset_schema("py_imports_resolved_v1"),
    )
    path = upsert_partitioned_dataset(
        imports_resolved,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
    )
    return {} if path is None else {spec.name: path}


def upsert_extract_outputs(
    outputs: Mapping[str, TableLike],
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert extract outputs into the incremental state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to dataset paths.
    """
    updated: dict[str, str] = {}
    for output, table in outputs.items():
        dataset_name = dataset_name_for_output(output)
        if dataset_name is None:
            continue
        spec = PartitionedDatasetSpec(name=dataset_name, partition_column="file_id")
        path = upsert_partitioned_dataset(
            table,
            spec=spec,
            base_dir=str(state_store.dataset_dir(dataset_name)),
            changes=changes,
        )
        if path is not None:
            updated[dataset_name] = path
    return updated


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
        Mapping of dataset names to dataset paths.
    """
    updated: dict[str, str] = {}
    for alias, table in outputs.items():
        try:
            dataset_name = dataset_name_from_alias(alias)
        except KeyError:
            continue
        spec = PartitionedDatasetSpec(name=dataset_name, partition_column="file_id")
        path = upsert_partitioned_dataset(
            table,
            spec=spec,
            base_dir=str(state_store.dataset_dir(dataset_name)),
            changes=changes,
        )
        if path is not None:
            updated[dataset_name] = path
    return updated


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = [
    "PartitionedDatasetSpec",
    "upsert_cpg_edges",
    "upsert_cpg_nodes",
    "upsert_exported_defs",
    "upsert_extract_outputs",
    "upsert_imports_resolved",
    "upsert_module_index",
    "upsert_normalize_outputs",
    "upsert_partitioned_dataset",
    "write_overwrite_dataset",
]
