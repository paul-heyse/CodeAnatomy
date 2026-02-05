"""Unified Delta dataset update helpers for incremental pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.arrow.metadata import encoding_policy_from_schema
from datafusion_engine.delta import DeltaMutationRequest
from datafusion_engine.extract.bundles import dataset_name_for_output
from datafusion_engine.io.write import WriteMode
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.schema.contracts import delta_constraints_for_location
from semantics.catalog.dataset_specs import dataset_name_from_alias
from semantics.incremental.delta_context import (
    DeltaAccessContext,
    run_delta_maintenance_if_configured,
)
from semantics.incremental.registry_specs import dataset_schema
from semantics.incremental.state_store import StateStore
from semantics.incremental.types import IncrementalFileChanges
from semantics.incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)
from storage.deltalake import (
    DeltaDeleteWhereRequest,
    coerce_delta_input,
    idempotent_commit_properties,
)

if TYPE_CHECKING:
    import pyarrow as pa

_STREAMING_ROW_THRESHOLD: int = 100_000


@dataclass(frozen=True)
class PartitionedDatasetSpec:
    """Partitioned dataset descriptor for incremental upserts."""

    name: str
    partition_column: str
    schema: pa.Schema | None = None


@dataclass(frozen=True)
class OverwriteDatasetSpec:
    """Overwrite dataset descriptor for full-table writes."""

    name: str
    schema: pa.Schema
    commit_metadata: Mapping[str, str] | None = None


def upsert_partitioned_dataset(
    table: TableLike,
    *,
    spec: PartitionedDatasetSpec,
    base_dir: str,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
) -> str | None:
    """Upsert a partitioned dataset using the incremental delete set.

    Returns
    -------
    str | None
        Dataset path when updated, otherwise ``None``.

    Raises
    ------
    ValueError
        Raised when required partition columns are missing.
    RuntimeError
        Raised when the Delta write result is unavailable.
    """
    if spec.partition_column not in table.column_names:
        msg = f"Partition column {spec.partition_column!r} is required for dataset {spec.name!r}."
        raise ValueError(msg)
    if spec.schema is not None and spec.partition_column not in spec.schema.names:
        msg = (
            "Partition column "
            f"{spec.partition_column!r} missing from schema for dataset {spec.name!r}."
        )
        raise ValueError(msg)
    schema = spec.schema or table.schema
    delete_partitions = _partition_specs(spec.partition_column, changes.deleted_file_ids)
    data = coerce_delta_input(
        table,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
        prefer_reader=True,
    )
    _delete_delta_partitions(
        base_dir,
        delete_partitions=delete_partitions,
        context=context,
        dataset_name=spec.name,
    )
    dataset_location = context.runtime.profile.catalog_ops.dataset_location(spec.name)
    extra_constraints = delta_constraints_for_location(dataset_location)
    resolved_storage = context.resolve_storage(table_uri=base_dir)
    write_result = write_delta_table_via_pipeline(
        runtime=context.runtime,
        table=data.data,
        request=IncrementalDeltaWriteRequest(
            destination=base_dir,
            mode=WriteMode.APPEND,
            schema_mode="merge",
            partition_by=(spec.partition_column,),
            storage_options=resolved_storage.storage_options,
            log_storage_options=resolved_storage.log_storage_options,
            constraints=extra_constraints,
            commit_metadata={"operation": "append", "dataset": spec.name},
            operation_id=f"incremental_partitioned::{spec.name}",
        ),
    )
    if write_result.delta_result is None:
        msg = f"Partitioned delta write returned no result for {spec.name!r}."
        raise RuntimeError(msg)
    return write_result.delta_result.path


def write_overwrite_dataset(
    table: TableLike,
    *,
    spec: OverwriteDatasetSpec,
    state_store: StateStore,
    context: DeltaAccessContext,
) -> dict[str, str]:
    """Overwrite a dataset with schema enforcement.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.

    Raises
    ------
    RuntimeError
        Raised when the Delta write result is unavailable.
    """
    metadata = spec.commit_metadata
    data = coerce_delta_input(
        table,
        schema=spec.schema,
        encoding_policy=encoding_policy_from_schema(spec.schema),
        prefer_reader=True,
    )
    target = str(state_store.dataset_dir(spec.name))
    dataset_location = context.runtime.profile.catalog_ops.dataset_location(spec.name)
    extra_constraints = delta_constraints_for_location(dataset_location)
    resolved_storage = context.resolve_storage(table_uri=target)
    write_result = write_delta_table_via_pipeline(
        runtime=context.runtime,
        table=data.data,
        request=IncrementalDeltaWriteRequest(
            destination=target,
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata=dict(metadata) if metadata else None,
            storage_options=resolved_storage.storage_options,
            log_storage_options=resolved_storage.log_storage_options,
            constraints=extra_constraints,
            operation_id=f"incremental_overwrite::{spec.name}",
        ),
    )
    if write_result.delta_result is None:
        msg = f"Overwrite delta write returned no result for {spec.name!r}."
        raise RuntimeError(msg)
    if data.row_count is not None and data.row_count > _STREAMING_ROW_THRESHOLD:
        record_artifact(
            context.runtime.profile,
            "incremental_streaming_writes_v1",
            {"dataset_name": spec.name, "row_count": data.row_count},
        )
    return {spec.name: write_result.delta_result.path}


def upsert_cpg_nodes(
    nodes: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
        schema=None,
    )
    path = upsert_partitioned_dataset(
        nodes,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
        context=context,
    )
    return {} if path is None else {spec.name: path}


def upsert_cpg_edges(
    edges: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
        schema=None,
    )
    path = upsert_partitioned_dataset(
        edges,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
        context=context,
    )
    return {} if path is None else {spec.name: path}


def upsert_exported_defs(
    exported_defs: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
        context=context,
    )
    return {} if path is None else {spec.name: path}


def upsert_module_index(
    module_index: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
        context=context,
    )
    return {} if path is None else {spec.name: path}


def upsert_imports_resolved(
    imports_resolved: TableLike,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
        context=context,
    )
    return {} if path is None else {spec.name: path}


def upsert_extract_outputs(
    outputs: Mapping[str, TableLike],
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
            context=context,
        )
        if path is not None:
            updated[dataset_name] = path
    return updated


def upsert_normalize_outputs(
    outputs: Mapping[str, TableLike],
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    context: DeltaAccessContext,
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
            context=context,
        )
        if path is not None:
            updated[dataset_name] = path
    return updated


def _delete_delta_partitions(
    base_dir: str,
    *,
    delete_partitions: Sequence[Mapping[str, str]],
    context: DeltaAccessContext,
    dataset_name: str | None,
) -> None:
    if not delete_partitions:
        return
    predicate = _partition_predicate(delete_partitions)
    if not predicate:
        return
    commit_metadata = {"dataset": base_dir, "operation": "delete"}
    commit_options, commit_run = context.runtime.profile.delta_ops.reserve_delta_commit(
        key=base_dir,
        metadata=commit_metadata,
        commit_metadata=commit_metadata,
    )
    commit_properties = idempotent_commit_properties(
        operation="delta_delete",
        mode="delete",
        idempotent=commit_options,
        extra_metadata=commit_metadata,
    )
    ctx = context.runtime.session_runtime().ctx
    dataset_location = (
        context.runtime.profile.catalog_ops.dataset_location(dataset_name) if dataset_name else None
    )
    extra_constraints = delta_constraints_for_location(dataset_location)
    resolved_storage = context.resolve_storage(table_uri=base_dir)
    delta_service = context.runtime.profile.delta_ops.delta_service()
    delta_service.mutate(
        DeltaMutationRequest(
            delete=DeltaDeleteWhereRequest(
                path=base_dir,
                predicate=predicate,
                storage_options=resolved_storage.storage_options,
                log_storage_options=resolved_storage.log_storage_options,
                commit_properties=commit_properties,
                commit_metadata=commit_metadata,
                extra_constraints=extra_constraints,
                runtime_profile=context.runtime.profile,
                dataset_name=dataset_name,
            )
        ),
        ctx=ctx,
    )
    context.runtime.profile.delta_ops.finalize_delta_commit(
        key=base_dir,
        run=commit_run,
        metadata={"operation": "delete", "partition_count": len(delete_partitions)},
    )
    run_delta_maintenance_if_configured(
        context,
        table_uri=base_dir,
        dataset_name=dataset_name,
        storage_options=resolved_storage.storage_options,
        log_storage_options=resolved_storage.log_storage_options,
    )


def _partition_predicate(
    partitions: Sequence[Mapping[str, str]],
) -> str:
    clauses: list[str] = []
    for partition in partitions:
        if not partition:
            continue
        parts = [f"{key} = '{value}'" for key, value in partition.items()]
        clauses.append(f"({' AND '.join(parts)})")
    return " OR ".join(clauses)


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = [
    "OverwriteDatasetSpec",
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
