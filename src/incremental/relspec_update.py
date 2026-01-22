"""Incremental relationship rule helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import ibis
import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext
from deltalake import DeltaTable
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import encoding_policy_from_schema
from arrowdsl.schema.schema import align_table, empty_table
from datafusion_engine.extract_bundles import dataset_name_for_output
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import dataset_spec_from_context
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.execution_factory import ibis_backend_from_ctx, ibis_execution_from_ctx
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from ibis_engine.param_tables import ParamTablePolicy, param_table_name
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import (
    FILE_ID_PARAM_THRESHOLD,
    FileIdQueryOptions,
    apply_query_spec,
    dataset_query_for_file_ids,
)
from ibis_engine.registry import DatasetLocation, ReadDatasetParams, read_dataset
from ibis_engine.sources import plan_from_source
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import CdfReadResult, read_cdf_changes
from incremental.changes import file_changes_from_cdf
from incremental.invalidations import validate_schema_identity
from incremental.pruning import FileScopePolicy, prune_delta_files, record_pruning_metrics
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges, IncrementalImpact
from normalize.registry_runtime import dataset_name_from_alias
from relspec.engine import PlanResolver
from relspec.incremental import incremental_spec
from schema_spec.system import dataset_spec_from_schema
from storage.deltalake import (
    build_commit_properties,
    coerce_delta_table,
    delta_cdf_enabled,
    delta_table_version,
)

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from arrowdsl.core.scan_telemetry import ScanTelemetry
    from relspec.model import DatasetRef


def scoped_site_datasets() -> frozenset[str]:
    """Return the datasets eligible for file-id scoping.

    Returns
    -------
    frozenset[str]
        Dataset names eligible for file-id scoping.
    """
    return incremental_spec().scoped_datasets


def impacted_file_ids(
    impact: IncrementalImpact,
    *,
    extra_file_ids: Sequence[str] = (),
) -> tuple[str, ...]:
    """Return the impacted file ids for incremental relationship runs.

    Returns
    -------
    tuple[str, ...]
        Sorted impacted file ids.
    """
    combined = set(impact.impacted_file_ids)
    combined.update(impact.changed_file_ids)
    combined.update(extra_file_ids)
    return tuple(sorted(combined))


@dataclass(frozen=True)
class DeltaSnapshot:
    """Delta time-travel selector for incremental reads."""

    version: int | None = None
    timestamp: str | None = None

    def read_options(self) -> Mapping[str, object] | None:
        """Return read options for Delta time travel.

        Returns
        -------
        Mapping[str, object] | None
            Read options compatible with Delta time travel.

        Raises
        ------
        ValueError
            Raised when both version and timestamp are specified.
        """
        if self.version is not None and self.timestamp is not None:
            msg = "DeltaSnapshot cannot include both version and timestamp."
            raise ValueError(msg)
        if self.version is not None:
            return {"version": self.version}
        if self.timestamp is not None:
            return {"timestamp": self.timestamp}
        return None


@dataclass(frozen=True)
class CdfDatasetReadOptions:
    """Options for reading Delta CDF changes for a dataset."""

    dataset_name: str
    runtime: IncrementalRuntime
    cursor_store: CdfCursorStore
    filter_policy: CdfFilterPolicy | None
    file_id_column: str


@dataclass(frozen=True)
class RelspecStateReadOptions:
    """Options controlling state-store reads for relspec inputs."""

    runtime: IncrementalRuntime | None = None
    cdf_policy: CdfFilterPolicy | None = None
    use_delta_cdf: bool = True
    use_delta_time_travel: bool = True


def _merge_file_ids(
    base: Sequence[str],
    changes: IncrementalFileChanges | None,
) -> tuple[str, ...]:
    combined = set(base)
    if changes is not None:
        combined.update(changes.changed_file_ids)
        combined.update(changes.deleted_file_ids)
    return tuple(sorted(combined))


def _cdf_has_columns(result: CdfReadResult, *, file_id_column: str) -> bool:
    column_names = set(result.table.column_names)
    return "_change_type" in column_names and file_id_column in column_names


def _cdf_changes_for_dataset(
    dataset_dir: Path,
    *,
    options: CdfDatasetReadOptions,
) -> tuple[CdfReadResult | None, IncrementalFileChanges | None]:
    if not delta_cdf_enabled(str(dataset_dir)):
        return None, None
    cdf_result = read_cdf_changes(
        options.runtime,
        dataset_path=str(dataset_dir),
        dataset_name=options.dataset_name,
        cursor_store=options.cursor_store,
        filter_policy=options.filter_policy,
    )
    if cdf_result is None:
        return None, None
    if not _cdf_has_columns(cdf_result, file_id_column=options.file_id_column):
        return cdf_result, None
    changes = file_changes_from_cdf(
        cdf_result,
        runtime=options.runtime,
        file_id_column=options.file_id_column,
    )
    if not changes.changed_file_ids and not changes.deleted_file_ids:
        return cdf_result, None
    return cdf_result, changes


def _delta_snapshot_version(dataset_dir: Path) -> int | None:
    return delta_table_version(str(dataset_dir))


def _table_registered(ctx: SessionContext, *, table_name: str) -> bool:
    try:
        ctx.table(table_name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return False
    return True


def _register_delta_table_for_insert(
    ctx: SessionContext,
    *,
    dataset_name: str,
    dataset_dir: Path,
    schema: pa.Schema,
    runtime: IncrementalRuntime,
) -> None:
    if not DeltaTable.is_deltatable(str(dataset_dir)):
        return
    if _table_registered(ctx, table_name=dataset_name):
        return
    location = DatasetLocation(
        path=str(dataset_dir),
        format="delta",
        partitioning="hive",
        dataset_spec=dataset_spec_from_schema(dataset_name, schema),
    )
    with contextlib.suppress(RuntimeError, TypeError, ValueError):
        register_dataset_df(
            ctx,
            name=dataset_name,
            location=location,
            runtime_profile=runtime.profile,
        )


def relspec_inputs_from_state(
    fallback: Mapping[str, TableLike],
    *,
    state_root: Path,
    ctx: ExecutionContext,
    file_ids: Sequence[str],
    options: RelspecStateReadOptions | None = None,
) -> dict[str, TableLike]:
    """Load relationship input tables from the state store with file-id filtering.

    Returns
    -------
    dict[str, TableLike]
        Relationship input tables with scoped file ids when available.
    """
    spec = incremental_spec()
    resolved_options = options or RelspecStateReadOptions()
    cursor_store = None
    if resolved_options.runtime is not None and resolved_options.use_delta_cdf:
        cursor_store = CdfCursorStore(
            cursors_path=StateStore(state_root).cdf_cursors_path(),
        )
    out: dict[str, TableLike] = {}
    for name, table in fallback.items():
        dataset_name = _relspec_state_dataset_name(name)
        if dataset_name is None:
            out[name] = table
            continue
        dataset_dir = state_root / "datasets" / dataset_name
        if not dataset_dir.exists():
            out[name] = table
            continue
        snapshot = None
        file_id_column = spec.file_id_column_for(dataset_name)
        effective_file_ids = tuple(file_ids)
        if (
            resolved_options.runtime is not None
            and cursor_store is not None
            and resolved_options.use_delta_cdf
            and file_id_column is not None
        ):
            cdf_result, cdf_changes = _cdf_changes_for_dataset(
                dataset_dir,
                options=CdfDatasetReadOptions(
                    dataset_name=dataset_name,
                    runtime=resolved_options.runtime,
                    cursor_store=cursor_store,
                    filter_policy=resolved_options.cdf_policy,
                    file_id_column=file_id_column,
                ),
            )
            if cdf_result is not None and resolved_options.use_delta_time_travel:
                snapshot = DeltaSnapshot(version=cdf_result.updated_version)
            if cdf_changes is not None:
                effective_file_ids = _merge_file_ids(file_ids, cdf_changes)
        if snapshot is None and resolved_options.use_delta_time_travel:
            version = _delta_snapshot_version(dataset_dir)
            if version is not None:
                snapshot = DeltaSnapshot(version=version)
        out[name] = _read_state_dataset(
            dataset_dir,
            dataset_name=dataset_name,
            ctx=ctx,
            file_ids=effective_file_ids,
            snapshot=snapshot,
        )
    return out


def upsert_relationship_outputs(
    outputs: Mapping[str, TableLike],
    *,
    state_root: Path,
    changes: IncrementalFileChanges,
    runtime: IncrementalRuntime,
) -> dict[str, str]:
    """Upsert relationship outputs by edge_owner_file_id into the state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    updated: dict[str, str] = {}
    spec = incremental_spec()
    for name, table_like in outputs.items():
        dataset_name = spec.relation_contract_for_output(name)
        if dataset_name is None:
            continue
        table = table_like
        file_id_column = spec.file_id_column_for(dataset_name, columns=table.column_names)
        if file_id_column is None or file_id_column not in table.column_names:
            continue
        dataset_dir = state_root / "datasets" / dataset_name
        delete_partitions = _partition_specs(file_id_column, changes.deleted_file_ids)
        data = coerce_delta_table(
            table,
            schema=table.schema,
            encoding_policy=encoding_policy_from_schema(table.schema),
        )
        base_dir = str(dataset_dir)
        _delete_delta_partitions(
            base_dir,
            delete_partitions=delete_partitions,
            runtime=runtime,
        )
        _register_delta_table_for_insert(
            runtime.profile.session_context(),
            dataset_name=dataset_name,
            dataset_dir=dataset_dir,
            schema=data.schema,
            runtime=runtime,
        )
        result = write_ibis_dataset_delta(
            data,
            base_dir,
            options=IbisDatasetWriteOptions(
                execution=runtime.ibis_execution(),
                writer_strategy="datafusion",
                delta_options=IbisDeltaWriteOptions(
                    mode="append",
                    schema_mode="merge",
                    partition_by=(file_id_column,),
                ),
            ),
        )
        updated[dataset_name] = result.path
    return updated


def scoped_relspec_resolver(
    base: PlanResolver[IbisPlan],
    *,
    file_ids: Sequence[str],
    scoped_datasets: frozenset[str] | None = None,
) -> PlanResolver[IbisPlan]:
    """Wrap a resolver to apply file-id predicates to scoped datasets.

    Returns
    -------
    PlanResolver[IbisPlan]
        Resolver scoped to file ids for selected datasets.
    """
    scoped = scoped_site_datasets() if scoped_datasets is None else scoped_datasets
    file_ids_tuple = tuple(file_ids)
    return _ScopedResolver(base=base, scoped=scoped, file_ids=file_ids_tuple)


@dataclass(frozen=True)
class _ScopedResolver:
    base: PlanResolver[IbisPlan]
    scoped: frozenset[str]
    file_ids: tuple[str, ...]

    @property
    def backend(self) -> BaseBackend | None:
        return self.base.backend

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        plan = self.base.resolve(ref, ctx=ctx)
        if ref.name not in self.scoped:
            return plan
        expr = plan.expr
        file_id_column = incremental_spec().file_id_column_for(ref.name, columns=expr.columns)
        if file_id_column is None or file_id_column not in expr.columns:
            return plan
        values = [ibis.literal(value) for value in self.file_ids]
        expr = expr.filter(expr[file_id_column].isin(values))
        return type(plan)(expr=expr, ordering=plan.ordering)

    def telemetry(self, ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        return self.base.telemetry(ref, ctx=ctx)


@dataclass(frozen=True)
class _PrunedDatasetInputs:
    dataset_dir: Path
    dataset_name: str
    file_id_column: str
    file_ids: Sequence[str]
    schema: pa.Schema
    ctx: ExecutionContext


@dataclass(frozen=True)
class _FileIdFilterInputs:
    table: IbisTable | TableLike
    dataset_schema: pa.Schema
    dataset_name: str
    file_ids: Sequence[str]
    ctx: ExecutionContext
    backend: BaseBackend
    dataset_dir: Path


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


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


def _delete_delta_partitions(
    base_dir: str,
    *,
    delete_partitions: Sequence[Mapping[str, str]],
    runtime: IncrementalRuntime,
) -> None:
    if not delete_partitions:
        return
    predicate = _partition_predicate(delete_partitions)
    if not predicate:
        return
    commit_options, commit_run = runtime.profile.reserve_delta_commit(
        key=base_dir,
        metadata={"dataset": base_dir, "operation": "delete"},
    )
    commit_properties = build_commit_properties(
        app_id=commit_options.app_id,
        version=commit_options.version,
    )
    DeltaTable(base_dir).delete(predicate, commit_properties=commit_properties)
    runtime.profile.finalize_delta_commit(
        key=base_dir,
        run=commit_run,
        metadata={"operation": "delete", "partition_count": len(delete_partitions)},
    )


def _relspec_state_dataset_name(name: str) -> str | None:
    alias = incremental_spec().normalize_alias(name)
    if alias is not None:
        try:
            return dataset_name_from_alias(alias)
        except KeyError:
            return None
    try:
        dataset_name = dataset_name_for_output(name)
    except KeyError:
        dataset_name = None
    if dataset_name is not None:
        return dataset_name
    try:
        return dataset_name_from_alias(name)
    except KeyError:
        return None


def _read_state_dataset(
    dataset_dir: Path,
    *,
    dataset_name: str,
    ctx: ExecutionContext,
    file_ids: Sequence[str],
    snapshot: DeltaSnapshot | None = None,
) -> TableLike:
    try:
        dataset_spec = dataset_spec_from_context(dataset_name)
    except KeyError:
        dataset_spec = None
    schema = dataset_spec.schema() if dataset_spec is not None else None
    if ctx.runtime.datafusion is None:
        msg = "DataFusion runtime profile is required for delta state datasets."
        raise TypeError(msg)
    backend = ibis_backend_from_ctx(ctx)
    read_options = snapshot.read_options() if snapshot is not None else None
    table = read_dataset(
        backend,
        params=ReadDatasetParams(
            path=dataset_dir,
            dataset_format="delta",
            partitioning="hive",
            read_options=read_options,
        ),
    )
    dataset_schema = schema or table.schema().to_pyarrow()
    if schema is not None:
        validate_schema_identity(
            expected=schema,
            actual=dataset_schema,
            dataset_name=dataset_name,
        )
    filtered = _apply_file_id_filter(
        _FileIdFilterInputs(
            table=table,
            dataset_schema=dataset_schema,
            dataset_name=dataset_name,
            file_ids=file_ids,
            ctx=ctx,
            backend=backend,
            dataset_dir=dataset_dir,
        )
    )
    if filtered is not None:
        return filtered
    if schema is None:
        return table.to_pyarrow()
    return empty_table(schema)


def _apply_file_id_filter(inputs: _FileIdFilterInputs) -> TableLike | None:
    spec = incremental_spec()
    file_id_column = spec.file_id_column_for(
        inputs.dataset_name,
        columns=inputs.dataset_schema.names,
    )
    if file_id_column is None:
        return None
    if inputs.file_ids and len(inputs.file_ids) >= FILE_ID_PARAM_THRESHOLD:
        pruned = _read_state_dataset_pruned(
            _PrunedDatasetInputs(
                dataset_dir=inputs.dataset_dir,
                dataset_name=inputs.dataset_name,
                file_id_column=file_id_column,
                file_ids=inputs.file_ids,
                schema=inputs.dataset_schema,
                ctx=inputs.ctx,
            )
        )
        if pruned is not None:
            return pruned
    param_spec = spec.file_id_param_spec()
    policy = ParamTablePolicy()
    table_name = param_table_name(policy, param_spec.logical_name)
    query = dataset_query_for_file_ids(
        inputs.file_ids,
        schema=inputs.dataset_schema,
        options=FileIdQueryOptions(
            file_id_column=file_id_column,
            param_table_name=table_name,
            param_key_column=param_spec.key_col,
            param_table_threshold=FILE_ID_PARAM_THRESHOLD,
        ),
    )
    plan = plan_from_source(
        inputs.table,
        ctx=inputs.ctx,
        backend=inputs.backend,
        name=inputs.dataset_name,
    )
    plan = IbisPlan(expr=apply_query_spec(plan.expr, spec=query), ordering=plan.ordering)
    execution = ibis_execution_from_ctx(inputs.ctx, backend=inputs.backend)
    return materialize_ibis_plan(plan, execution=execution)


def _read_state_dataset_pruned(inputs: _PrunedDatasetInputs) -> pa.Table | None:
    policy = FileScopePolicy(
        file_id_column=inputs.file_id_column,
        file_ids=tuple(inputs.file_ids),
    )
    result = prune_delta_files(inputs.dataset_dir, policy)
    diagnostics_sink = (
        inputs.ctx.runtime.datafusion.diagnostics_sink if inputs.ctx.runtime.datafusion else None
    )
    record_pruning_metrics(
        sink=diagnostics_sink,
        dataset_name=inputs.dataset_name,
        result=result,
    )
    if not result.candidate_paths:
        return empty_table(inputs.schema)
    dataset = ds.dataset(result.candidate_paths, format="parquet")
    filter_expr = ds.field(inputs.file_id_column).isin(list(inputs.file_ids))
    table = dataset.to_table(filter=filter_expr)
    return align_table(table, schema=inputs.schema, safe_cast=True)


__all__ = [
    "impacted_file_ids",
    "relspec_inputs_from_state",
    "scoped_relspec_resolver",
    "upsert_relationship_outputs",
]
