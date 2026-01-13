"""Plan source helpers for tables, readers, and datasets."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow.dataset as ds

from arrowdsl.core.context import (
    ExecutionContext,
    Ordering,
    execution_context_factory,
)
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.plan.plan import Plan, PlanFactory
from arrowdsl.plan.query import scan_context_factory
from arrowdsl.plan.rows import plan_from_rows
from arrowdsl.plan.scan_specs import DatasetFactorySpec, ScanSpec
from schema_spec.system import DatasetSpec


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + DatasetSpec pairing for plan compilation."""

    dataset: ds.Dataset
    spec: DatasetSpec


@dataclass(frozen=True)
class InMemoryDatasetSource:
    """RecordBatchReader source with one-shot semantics."""

    reader: RecordBatchReaderLike
    one_shot: bool = True


@dataclass(frozen=True)
class RowSource:
    """Row iterator source with an explicit schema."""

    rows: Iterable[Mapping[str, object]]
    schema: SchemaLike
    batch_size: int = 4096


PlanSource = (
    TableLike
    | RecordBatchReaderLike
    | ds.Dataset
    | ds.Scanner
    | DatasetSource
    | DatasetFactorySpec
    | ScanSpec
    | InMemoryDatasetSource
    | RowSource
    | Plan
)


def plan_from_dataset(
    dataset: ds.Dataset,
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
    scan_ctx = spec.scan_context(dataset, ctx)
    ordering = (
        Ordering.implicit()
        if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
        else Ordering.unordered()
    )
    return Plan(
        decl=scan_ctx.acero_decl(),
        label=spec.name,
        ordering=ordering,
        pipeline_breakers=(),
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
    if isinstance(source, ScanSpec):
        scan_ctx = scan_context_factory(source, ctx=ctx)
        return scan_ctx.to_plan(label=label)
    if isinstance(source, DatasetFactorySpec):
        return plan_from_source(
            source.build(),
            ctx=ctx,
            columns=columns,
            label=label,
        )
    if isinstance(source, ds.Dataset):
        dataset = cast("ds.Dataset", source)
        available = set(dataset.schema.names)
        scan_cols = list(columns) if columns is not None else list(dataset.schema.names)
        scan_cols = [name for name in scan_cols if name in available]
        factory = PlanFactory(ctx=ctx)
        return factory.scan(dataset, columns=scan_cols, label=label)
    if isinstance(source, ds.Scanner):
        scanner = cast("ds.Scanner", source)
        return Plan.from_reader(scanner.to_reader(), label=label)
    return None


def _plan_from_reader_source(
    source: PlanSource,
    *,
    label: str,
) -> Plan | None:
    if isinstance(source, InMemoryDatasetSource):
        if source.one_shot:
            return Plan.table_source(source.reader.read_all(), label=label)
        return Plan.from_reader(source.reader, label=label)
    if isinstance(source, RowSource):
        return plan_from_rows(
            source.rows,
            schema=source.schema,
            batch_size=source.batch_size,
            label=label,
        )
    if isinstance(source, RecordBatchReaderLike):
        return Plan.from_reader(source, label=label)
    return None


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
    plan = _plan_from_reader_source(source, label=label)
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


def plan_from_scan_columns(
    dataset: ds.Dataset,
    *,
    columns: Sequence[str] | Mapping[str, ComputeExpression],
    ctx: ExecutionContext,
    label: str = "",
) -> Plan:
    """Build a scan plan from explicit columns.

    Returns
    -------
    Plan
        Plan built from a scan declaration.
    """
    factory = PlanFactory(ctx=ctx)
    return factory.scan(dataset, columns=columns, label=label)


__all__ = [
    "DatasetSource",
    "InMemoryDatasetSource",
    "PlanSource",
    "RowSource",
    "plan_from_dataset",
    "plan_from_scan_columns",
    "plan_from_source",
    "plan_from_source_profile",
]
