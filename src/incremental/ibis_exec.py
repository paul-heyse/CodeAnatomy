"""Ibis execution helpers for incremental plans."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.core.ordering import Ordering
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.plan import IbisPlan
from incremental.runtime import IncrementalRuntime
from incremental.sqlglot_artifacts import record_sqlglot_plan_artifact

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
    record_sqlglot_plan_artifact(runtime, name=name, expr=expr)
    plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
    table = materialize_ibis_plan(plan, execution=runtime.ibis_execution())
    return cast("pa.Table", table)


__all__ = ["ibis_expr_to_table"]
