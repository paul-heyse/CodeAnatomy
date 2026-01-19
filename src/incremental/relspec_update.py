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
from extract.registry_bundles import dataset_name_for_output
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_query_spec, dataset_query_for_file_ids
from ibis_engine.scan_io import plan_from_source
from incremental.invalidations import validate_schema_identity
from incremental.types import IncrementalFileChanges, IncrementalImpact
from normalize.registry_specs import dataset_name_from_alias
from relspec.engine import PlanResolver
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY
from storage.dataset_sources import (
    DatasetDiscoveryOptions,
    DatasetSourceOptions,
    normalize_dataset_source,
    unwrap_dataset,
)
from storage.deltalake import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
)

if TYPE_CHECKING:
    from arrowdsl.core.scan_telemetry import ScanTelemetry
    from relspec.model import DatasetRef

SCOPED_SITE_DATASETS: frozenset[str] = frozenset(
    {
        "cst_name_refs",
        "cst_imports",
        "cst_callsites",
        "cst_defs",
        "scip_occurrences",
        "scip_symbol_relationships",
        "callsite_qname_candidates",
        "type_exprs_norm",
        "diagnostics_norm",
        "rt_signatures",
        "rt_signature_params",
        "rt_members",
    }
)

_NORMALIZE_ALIAS_OVERRIDES: Mapping[str, str] = {
    "cst_defs": "cst_defs_norm",
    "cst_imports": "cst_imports_norm",
    "scip_occurrences": "scip_occurrences_norm",
}
_RELATION_OUTPUT_DATASETS: Mapping[str, str] = {
    "rel_name_symbol": "rel_name_symbol_v1",
    "rel_import_symbol": "rel_import_symbol_v1",
    "rel_def_symbol": "rel_def_symbol_v1",
    "rel_callsite_symbol": "rel_callsite_symbol_v1",
    "rel_callsite_qname": "rel_callsite_qname_v1",
}


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
    delete_partitions = _partition_specs("edge_owner_file_id", changes.deleted_file_ids)
    updated: dict[str, str] = {}
    for name, table_like in outputs.items():
        dataset_name = _RELATION_OUTPUT_DATASETS.get(name)
        if dataset_name is None:
            continue
        table = table_like
        if "edge_owner_file_id" not in table.column_names:
            continue
        data = coerce_delta_table(
            table,
            schema=table.schema,
            encoding_policy=encoding_policy_from_schema(table.schema),
        )
        result = upsert_dataset_partitions_delta(
            data,
            options=DeltaUpsertOptions(
                base_dir=str(state_root / "datasets" / dataset_name),
                partition_cols=("edge_owner_file_id",),
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
    scoped = SCOPED_SITE_DATASETS if scoped_datasets is None else scoped_datasets
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
        if "file_id" not in expr.columns:
            return plan
        values = [ibis.literal(value) for value in self.file_ids]
        expr = expr.filter(expr["file_id"].isin(values))
        return type(plan)(expr=expr, ordering=plan.ordering)

    def telemetry(self, ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        return self.base.telemetry(ref, ctx=ctx)


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


def _relspec_state_dataset_name(name: str) -> str | None:
    alias = _NORMALIZE_ALIAS_OVERRIDES.get(name)
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
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(dataset_name)
    schema = dataset_spec.schema() if dataset_spec is not None else None
    dataset = unwrap_dataset(
        normalize_dataset_source(
            dataset_dir,
            options=DatasetSourceOptions(
                schema=schema,
                partitioning="hive",
                discovery=DatasetDiscoveryOptions(),
            ),
        )
    )
    if schema is not None:
        validate_schema_identity(
            expected=schema,
            actual=dataset.schema,
            dataset_name=dataset_name,
        )
    if schema is not None and "file_id" in schema.names:
        query = dataset_query_for_file_ids(file_ids, schema=schema)
        backend = build_backend(IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion))
        plan = plan_from_source(
            dataset.to_table(),
            ctx=ctx,
            backend=backend,
            name=dataset_name,
        )
        plan = IbisPlan(expr=apply_query_spec(plan.expr, spec=query), ordering=plan.ordering)
        execution = IbisExecutionContext(ctx=ctx, ibis_backend=backend)
        return materialize_ibis_plan(plan, execution=execution)
    if schema is None:
        return dataset.to_table()
    return empty_table(schema)


__all__ = [
    "SCOPED_SITE_DATASETS",
    "impacted_file_ids",
    "relspec_inputs_from_state",
    "scoped_relspec_resolver",
    "upsert_relationship_outputs",
]
