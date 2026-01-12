"""Schema unification helpers for CPG tables."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from schema_spec.system import DatasetSpec


def unify_tables(
    *,
    spec: DatasetSpec,
    tables: Sequence[TableLike],
    ctx: ExecutionContext | None,
) -> TableLike:
    """Unify tables using the dataset's evolution spec.

    Returns
    -------
    TableLike
        Unified table with aligned schema.

    Raises
    ------
    ValueError
        Raised when no tables are provided.
    """
    if not tables:
        msg = "unify_tables requires at least one table."
        raise ValueError(msg)
    return spec.unify_tables(tables, ctx=ctx)


__all__ = ["unify_tables"]
