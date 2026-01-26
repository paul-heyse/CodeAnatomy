"""Ibis execution helpers for incremental plans."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.core.ordering import Ordering
from ibis_engine.execution import execute_ibis_plan
from ibis_engine.plan import IbisPlan
from incremental.runtime import IncrementalRuntime
from incremental.sqlglot_artifacts import record_view_artifact

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable


def ibis_expr_to_table(
    expr: IbisTable,
    *,
    runtime: IncrementalRuntime,
    name: str,
) -> pa.Table:
    """Execute an Ibis expression and return a PyArrow table.

    Returns
    -------
    pyarrow.Table
        Materialized Arrow table for the expression.
    """
    plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
    table = execute_ibis_plan(
        plan,
        execution=runtime.ibis_execution(),
        streaming=False,
    ).require_table()
    arrow_table = cast("pa.Table", table)
    record_view_artifact(runtime, name=name, expr=expr, schema=arrow_table.schema)
    return arrow_table


__all__ = ["ibis_expr_to_table"]
