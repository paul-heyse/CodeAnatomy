"""Plan scan and row ingestion helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow.dataset as ds

from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import ComputeExpression, SchemaLike, TableLike, pc
from arrowdsl.core.schema_constants import PROVENANCE_SOURCE_FIELDS
from arrowdsl.plan.builder import PlanBuilder
from arrowdsl.plan.dataset_wrappers import DatasetLike, is_one_shot_dataset
from arrowdsl.plan.ops import scan_ordering_effect
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.build import rows_to_table as rows_to_table_factory
from schema_spec.system import DatasetSpec


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    return rows_to_table_factory(rows, schema)


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + DatasetSpec pairing for plan compilation."""

    dataset: DatasetLike
    spec: DatasetSpec


PlanSource = TableLike | DatasetLike | ds.Scanner | DatasetSource | Plan


def plan_from_dataset(
    dataset: DatasetLike,
    *,
    spec: DatasetSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Compile a dataset-backed scan plan with ordering metadata.

    Returns
    -------
    Plan
        Plan representing the dataset scan and projection.
    """
    query = spec.query()
    return query.to_plan(
        dataset=dataset,
        ctx=ctx,
        label=spec.name,
        scan_provenance=tuple(ctx.runtime.scan.scan_provenance_columns),
    )


def _plan_from_scan_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None,
    label: str,
) -> Plan | None:
    if isinstance(source, DatasetSource):
        return plan_from_dataset(source.dataset, spec=source.spec, ctx=ctx)
    return _plan_from_dataset_or_scanner(
        source,
        ctx=ctx,
        columns=columns,
        label=label,
    )


def _plan_from_dataset_or_scanner(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None,
    label: str,
) -> Plan | None:
    if isinstance(source, ds.Dataset) or is_one_shot_dataset(source):
        return _plan_from_dataset_obj(source, ctx=ctx, columns=columns, label=label)
    if isinstance(source, ds.Scanner):
        scanner = cast("ds.Scanner", source)
        return Plan.from_reader(scanner.to_reader(), label=label)
    return None


def _plan_from_dataset_obj(
    dataset: DatasetLike,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None,
    label: str,
) -> Plan:
    available = set(dataset.schema.names)
    scan_cols = list(columns) if columns is not None else list(dataset.schema.names)
    scan_cols = [name for name in scan_cols if name in available]
    builder = PlanBuilder()
    scan_provenance = tuple(ctx.runtime.scan.scan_provenance_columns)
    if not scan_provenance:
        builder.scan(
            dataset=dataset,
            columns=scan_cols,
            predicate=None,
            ordering_effect=scan_ordering_effect(ctx),
        )
        plan_ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=plan_ir,
            label=label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )
    cols_map: dict[str, ComputeExpression] = {name: pc.field(name) for name in scan_cols}
    for name in scan_provenance:
        if name in cols_map:
            continue
        source_field = PROVENANCE_SOURCE_FIELDS.get(name)
        if source_field is not None:
            cols_map[name] = pc.field(source_field)
            continue
        if name in available:
            cols_map[name] = pc.field(name)
    builder.scan(
        dataset=dataset,
        columns=cols_map,
        predicate=None,
        ordering_effect=scan_ordering_effect(ctx),
    )
    plan_ir, ordering, pipeline_breakers = builder.build()
    return Plan(
        ir=plan_ir,
        label=label,
        ordering=ordering,
        pipeline_breakers=pipeline_breakers,
    )


def plan_from_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    """Return a plan for tables, readers, or dataset-backed sources.

    Returns
    -------
    Plan
        Acero-backed plan for dataset/table sources.
    """
    if isinstance(source, Plan):
        return source
    plan = _plan_from_scan_source(source, ctx=ctx, columns=columns, label=label)
    if plan is not None:
        return plan
    return Plan.table_source(cast("TableLike", source), label=label)


def plan_from_source_profile(
    source: PlanSource,
    *,
    profile: str,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    """Return a plan for a source using a named execution profile.

    Returns
    -------
    Plan
        Plan for the source with the named profile context.
    """
    ctx = execution_context_factory(profile)
    return plan_from_source(source, ctx=ctx, columns=columns, label=label)


__all__ = [
    "DatasetSource",
    "PlanSource",
    "plan_from_dataset",
    "plan_from_source",
    "plan_from_source_profile",
    "rows_to_table",
]
