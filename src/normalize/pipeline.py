"""Minimal plan execution helpers for normalization pipelines."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import pyarrow as pa
from pyarrow import acero

from arrowdsl.compute.kernels import canonical_sort as _canonical_sort
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.ops import SortKey
from arrowdsl.plan.plan import Plan, PlanSpec


def canonical_sort(
    table: pa.Table,
    sort_keys: Sequence[tuple[str, Literal["ascending", "descending"]]],
) -> pa.Table:
    """Return a canonically sorted table using stable sort indices.

    Returns
    -------
    pa.Table
        Sorted table.
    """
    keys = [SortKey(column=col, order=order) for col, order in sort_keys]
    return _canonical_sort(table, sort_keys=keys)


def run_plan(decl: acero.Declaration, *, use_threads: bool) -> pa.Table:
    """Execute an Acero declaration as a table.

    Returns
    -------
    pa.Table
        Materialized table from the declaration.
    """
    return decl.to_table(use_threads=use_threads)


def run_plan_streamable(
    plan: Plan,
    *,
    ctx: ExecutionContext,
) -> RecordBatchReaderLike | TableLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    RecordBatchReaderLike | TableLike
        Streaming reader when possible, otherwise a table.
    """
    spec = PlanSpec.from_plan(plan)
    if spec.pipeline_breakers:
        return spec.to_table(ctx=ctx)
    return spec.to_reader(ctx=ctx)


__all__ = ["canonical_sort", "run_plan", "run_plan_streamable"]
