"""Schema unification helpers for CPG tables."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from cpg.plan_specs import align_table_to_schema, unify_schema_with_metadata
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
    unified = spec.unify_tables(tables, ctx=ctx)
    promote_options = spec.evolution_spec.promote_options
    schema = unify_schema_with_metadata(
        [table.schema for table in tables],
        promote_options=promote_options,
    )
    return align_table_to_schema(unified, schema=schema)


__all__ = ["unify_tables"]
