"""Minimal plan execution helpers for normalization pipelines."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, cast

import pyarrow as pa

from arrowdsl.compute.kernels import canonical_sort as _canonical_sort
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.ops import SortKey
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan as run_plan_core
from arrowdsl.plan.runner import run_plan_streamable as run_plan_streamable_core


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


def run_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    """Execute a plan as a table with canonical metadata handling.

    Returns
    -------
    TableLike
        Materialized table from the plan.

    Raises
    ------
    TypeError
        Raised when plan execution yields a reader instead of a table.
    """
    result = run_plan_core(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    if isinstance(result.value, pa.RecordBatchReader):
        msg = "Expected table result from run_plan."
        raise TypeError(msg)
    return cast("TableLike", result.value)


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
    return run_plan_streamable_core(plan, ctx=ctx, attach_ordering_metadata=True)


__all__ = ["canonical_sort", "run_plan", "run_plan_streamable"]
