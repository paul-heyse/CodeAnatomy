"""Incremental relationship rule helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import ibis
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import encoding_policy_from_schema
from arrowdsl.schema.schema import empty_table
from datafusion_engine.extract_bundles import dataset_name_for_output
from datafusion_engine.runtime import dataset_spec_from_context
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.param_tables import ParamTablePolicy, param_table_name
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import (
    FILE_ID_PARAM_THRESHOLD,
    FileIdQueryOptions,
    apply_query_spec,
    dataset_query_for_file_ids,
)
from ibis_engine.registry import ReadDatasetParams, read_dataset
from ibis_engine.sources import plan_from_source
from incremental.invalidations import validate_schema_identity
from incremental.types import IncrementalFileChanges, IncrementalImpact
from normalize.registry_runtime import dataset_name_from_alias
from relspec.engine import PlanResolver
from relspec.incremental import incremental_spec
from storage.deltalake import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
)

if TYPE_CHECKING:
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


def relspec_inputs_from_state(
    fallback: Mapping[str, TableLike],
    *,
    state_root: Path,
    ctx: ExecutionContext,
    file_ids: Sequence[str],
) -> dict[str, TableLike]:
    """Load relationship input tables from the state store with file-id filtering.

    Returns
    -------
    dict[str, TableLike]
        Relationship input tables with scoped file ids when available.
    """
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
        out[name] = _read_state_dataset(
            dataset_dir,
            dataset_name=dataset_name,
            ctx=ctx,
            file_ids=file_ids,
        )
    return out


def upsert_relationship_outputs(
    outputs: Mapping[str, TableLike],
    *,
    state_root: Path,
    changes: IncrementalFileChanges,
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
        delete_partitions = _partition_specs(file_id_column, changes.deleted_file_ids)
        data = coerce_delta_table(
            table,
            schema=table.schema,
            encoding_policy=encoding_policy_from_schema(table.schema),
        )
        result = upsert_dataset_partitions_delta(
            data,
            options=DeltaUpsertOptions(
                base_dir=str(state_root / "datasets" / dataset_name),
                partition_cols=(file_id_column,),
                delete_partitions=delete_partitions,
                options=DeltaWriteOptions(schema_mode="merge"),
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


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


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
) -> TableLike:
    try:
        dataset_spec = dataset_spec_from_context(dataset_name)
    except KeyError:
        dataset_spec = None
    schema = dataset_spec.schema() if dataset_spec is not None else None
    if ctx.runtime.datafusion is None:
        msg = "DataFusion runtime profile is required for delta state datasets."
        raise TypeError(msg)
    backend = build_backend(IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion))
    table = read_dataset(
        backend,
        params=ReadDatasetParams(
            path=dataset_dir,
            dataset_format="delta",
            partitioning="hive",
        ),
    )
    dataset_schema = schema or table.schema().to_pyarrow()
    if schema is not None:
        validate_schema_identity(
            expected=schema,
            actual=dataset_schema,
            dataset_name=dataset_name,
        )
    spec = incremental_spec()
    file_id_column = spec.file_id_column_for(dataset_name, columns=dataset_schema.names)
    if file_id_column is not None:
        param_spec = spec.file_id_param_spec()
        policy = ParamTablePolicy()
        table_name = param_table_name(policy, param_spec.logical_name)
        query = dataset_query_for_file_ids(
            file_ids,
            schema=dataset_schema,
            options=FileIdQueryOptions(
                file_id_column=file_id_column,
                param_table_name=table_name,
                param_key_column=param_spec.key_col,
                param_table_threshold=FILE_ID_PARAM_THRESHOLD,
            ),
        )
        plan = plan_from_source(table, ctx=ctx, backend=backend, name=dataset_name)
        plan = IbisPlan(expr=apply_query_spec(plan.expr, spec=query), ordering=plan.ordering)
        execution = IbisExecutionContext(ctx=ctx, ibis_backend=backend)
        return materialize_ibis_plan(plan, execution=execution)
    if schema is None:
        return table.to_pyarrow()
    return empty_table(schema)


__all__ = [
    "impacted_file_ids",
    "relspec_inputs_from_state",
    "scoped_relspec_resolver",
    "upsert_relationship_outputs",
]
