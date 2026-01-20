"""Incremental relationship rule helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import ibis
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
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
from normalize.op_specs import normalize_op_specs
from normalize.registry_specs import dataset_name_from_alias
from relspec.contracts import RELATION_OUTPUT_NAME
from relspec.engine import PlanResolver
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.definitions import RelationshipPayload
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

_FILE_ID_COLUMNS: tuple[str, ...] = ("edge_owner_file_id", "file_id")


def _file_id_column_from_columns(columns: Sequence[str]) -> str | None:
    names = set(columns)
    for candidate in _FILE_ID_COLUMNS:
        if candidate in names:
            return candidate
    return None


def _file_id_column_from_schema(schema: SchemaLike | None) -> str | None:
    if schema is None:
        return None
    return _file_id_column_from_columns(schema.names)


def _dataset_schema_for_name(name: str) -> SchemaLike | None:
    spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(name)
    if spec is not None:
        return spec.schema()
    resolved = _relspec_state_dataset_name(name)
    if resolved is None or resolved == name:
        return None
    spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(resolved)
    if spec is None:
        return None
    return spec.schema()


def _file_id_column_for_dataset(
    name: str,
    *,
    columns: Sequence[str] | None = None,
) -> str | None:
    column = _file_id_column_from_schema(_dataset_schema_for_name(name))
    if column is not None:
        return column
    if columns is None:
        return None
    return _file_id_column_from_columns(columns)


def _resolve_normalize_alias(name: str) -> str | None:
    try:
        dataset_name_from_alias(name)
    except KeyError:
        return None
    return name


def _canonical_normalize_output(spec_name: str, outputs: Sequence[str]) -> str | None:
    resolved = [name for name in outputs if _resolve_normalize_alias(name) is not None]
    if not resolved:
        return None
    if spec_name in resolved:
        return spec_name
    norm_aliases = sorted(name for name in resolved if name.endswith("_norm"))
    if norm_aliases:
        return norm_aliases[0]
    return sorted(resolved)[0]


@cache
def _normalize_alias_overrides() -> Mapping[str, str]:
    overrides: dict[str, str] = {}
    specs = normalize_op_specs(rule_definitions_cached("normalize"))
    for spec in specs:
        canonical = _canonical_normalize_output(spec.name, spec.outputs)
        if canonical is None:
            continue
        for output in spec.outputs:
            if output == canonical:
                continue
            overrides.setdefault(output, canonical)
    return overrides


@cache
def _relation_output_datasets() -> Mapping[str, str]:
    mapping: dict[str, str] = {}
    for rule in rule_definitions_cached("cpg"):
        payload = rule.payload
        if not isinstance(payload, RelationshipPayload):
            continue
        output_name = payload.output_dataset or rule.output
        contract_name = payload.contract_name
        if not output_name or not contract_name:
            continue
        if contract_name == RELATION_OUTPUT_NAME:
            continue
        existing = mapping.get(output_name)
        if existing is not None and existing != contract_name:
            msg = (
                "Conflicting relationship output contracts for "
                f"{output_name!r}: {existing!r} vs {contract_name!r}."
            )
            raise ValueError(msg)
        mapping[output_name] = contract_name
    return mapping


@cache
def scoped_site_datasets() -> frozenset[str]:
    inputs = {name for rule in rule_definitions_cached("cpg") for name in rule.inputs}
    scoped = {name for name in inputs if _file_id_column_for_dataset(name) is not None}
    return frozenset(scoped or inputs)




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
    for name, table_like in outputs.items():
        dataset_name = _relation_output_datasets().get(name)
        if dataset_name is None:
            continue
        table = table_like
        file_id_column = _file_id_column_for_dataset(
            dataset_name,
            columns=table.column_names,
        )
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
        file_id_column = _file_id_column_for_dataset(ref.name, columns=expr.columns)
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
    alias = _normalize_alias_overrides().get(name)
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
    dataset_schema = schema or dataset.schema
    if schema is not None:
        validate_schema_identity(
            expected=schema,
            actual=dataset.schema,
            dataset_name=dataset_name,
        )
    file_id_column = _file_id_column_from_schema(dataset_schema)
    if file_id_column is not None:
        query = dataset_query_for_file_ids(
            file_ids,
            schema=dataset_schema,
            file_id_column=file_id_column,
        )
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


SCOPED_SITE_DATASETS: frozenset[str] = scoped_site_datasets()


__all__ = [
    "SCOPED_SITE_DATASETS",
    "impacted_file_ids",
    "relspec_inputs_from_state",
    "scoped_relspec_resolver",
    "upsert_relationship_outputs",
]
