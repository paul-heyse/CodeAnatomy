"""Ibis helpers for incremental pipelines."""

from __future__ import annotations

import ibis
from ibis.backends import BaseBackend

from arrowdsl.core.interop import TableLike
from arrowdsl.core.ordering import Ordering
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table


def ibis_table_from_arrow(
    backend: BaseBackend,
    table: TableLike,
    *,
    name: str | None = None,
) -> ibis.Table:
    """Register a TableLike as an Ibis table expression.

    Returns
    -------
    ibis.Table
        Registered Ibis table expression.
    """
    plan = register_ibis_table(
        table,
        options=SourceToIbisOptions(
            backend=backend,
            name=name,
            ordering=Ordering.unordered(),
        ),
    )
    return plan.expr


__all__ = ["ibis_table_from_arrow"]
