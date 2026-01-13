"""Plan source helpers for tables, readers, and datasets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow.dataset as ds

from arrowdsl.core.context import ExecutionContext, Ordering, OrderingEffect
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    TableLike,
)
from arrowdsl.plan.ops import ScanOp
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import scan_context_from_spec
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


PlanSource = (
    TableLike
    | RecordBatchReaderLike
    | ds.Dataset
    | ds.Scanner
    | DatasetSource
    | DatasetFactorySpec
    | ScanSpec
    | InMemoryDatasetSource
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
    plan: Plan
    if isinstance(source, Plan):
        plan = source
    elif isinstance(source, DatasetSource):
        plan = plan_from_dataset(source.dataset, spec=source.spec, ctx=ctx)
    elif isinstance(source, ScanSpec):
        scan_ctx = scan_context_from_spec(source, ctx=ctx)
        ordering = (
            Ordering.implicit()
            if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
            else Ordering.unordered()
        )
        plan = Plan(
            decl=scan_ctx.acero_decl(),
            label=label,
            ordering=ordering,
            pipeline_breakers=(),
        )
    elif isinstance(source, DatasetFactorySpec):
        plan = plan_from_source(
            source.build(),
            ctx=ctx,
            columns=columns,
            label=label,
        )
    elif isinstance(source, ds.Dataset):
        dataset = cast("ds.Dataset", source)
        available = set(dataset.schema.names)
        scan_cols = list(columns) if columns is not None else list(dataset.schema.names)
        scan_cols = [name for name in scan_cols if name in available]
        scan_ordering = (
            OrderingEffect.IMPLICIT
            if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
            else OrderingEffect.UNORDERED
        )
        scan_op = ScanOp(
            dataset=dataset,
            columns=scan_cols,
            predicate=None,
            ordering_effect=scan_ordering,
        )
        decl = scan_op.to_declaration([], ctx=ctx)
        ordering = scan_op.apply_ordering(Ordering.unordered())
        plan = Plan(decl=decl, label=label, ordering=ordering, pipeline_breakers=())
    elif isinstance(source, ds.Scanner):
        scanner = cast("ds.Scanner", source)
        plan = Plan.table_source(scanner.to_table(), label=label)
    elif isinstance(source, InMemoryDatasetSource):
        if source.one_shot:
            plan = Plan.table_source(source.reader.read_all(), label=label)
        else:
            plan = Plan.from_reader(source.reader, label=label)
    elif isinstance(source, RecordBatchReaderLike):
        plan = Plan.table_source(source.read_all(), label=label)
    else:
        plan = Plan.table_source(source, label=label)
    return plan


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
    scan_op = ScanOp(dataset=dataset, columns=columns, predicate=None)
    decl = scan_op.to_declaration([], ctx=ctx)
    ordering = scan_op.apply_ordering(Ordering.unordered())
    return Plan(decl=decl, label=label, ordering=ordering, pipeline_breakers=())


__all__ = [
    "DatasetSource",
    "InMemoryDatasetSource",
    "PlanSource",
    "plan_from_dataset",
    "plan_from_scan_columns",
    "plan_from_source",
]
