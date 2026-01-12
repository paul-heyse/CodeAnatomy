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
from schema_spec.system import DatasetSpec


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + DatasetSpec pairing for plan compilation."""

    dataset: ds.Dataset
    spec: DatasetSpec


PlanSource = TableLike | RecordBatchReaderLike | ds.Dataset | ds.Scanner | DatasetSource | Plan


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
    if isinstance(source, Plan):
        return source
    if isinstance(source, DatasetSource):
        return plan_from_dataset(source.dataset, spec=source.spec, ctx=ctx)
    if isinstance(source, ds.Dataset):
        available = set(source.schema.names)
        scan_cols = list(columns) if columns is not None else list(source.schema.names)
        scan_cols = [name for name in scan_cols if name in available]
        scan_ordering = (
            OrderingEffect.IMPLICIT
            if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
            else OrderingEffect.UNORDERED
        )
        scan_op = ScanOp(
            dataset=source,
            columns=scan_cols,
            predicate=None,
            ordering_effect=scan_ordering,
        )
        decl = scan_op.to_declaration([], ctx=ctx)
        ordering = scan_op.apply_ordering(Ordering.unordered())
        return Plan(decl=decl, label=label, ordering=ordering, pipeline_breakers=())
    if isinstance(source, ds.Scanner):
        scanner = cast("ds.Scanner", source)
        return Plan.table_source(scanner.to_table(), label=label)
    if isinstance(source, RecordBatchReaderLike):
        return Plan.table_source(source.read_all(), label=label)
    return Plan.table_source(source, label=label)


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
    "PlanSource",
    "plan_from_dataset",
    "plan_from_scan_columns",
    "plan_from_source",
]
