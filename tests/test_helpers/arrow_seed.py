"""Helpers for seeding Arrow tables in tests."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.io.ingest import datafusion_from_arrow

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


def register_arrow_table(
    ctx: SessionContext,
    *,
    name: str,
    value: pa.Table,
    batch_size: int | None = None,
    ingest_hook: Callable[[Mapping[str, object]], None] | None = None,
) -> DataFrame:
    """Register an Arrow table and return the DataFusion DataFrame.

    Parameters
    ----------
    ctx
        DataFusion session context for registration.
    name
        Table name to register.
    value
        PyArrow table to register.
    batch_size
        Optional batch size for ingestion.
    ingest_hook
        Optional ingest hook for diagnostics.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for the registered table.
    """
    if ctx.table_exist(name):
        ctx.deregister_table(name)
    return datafusion_from_arrow(
        ctx,
        name=name,
        value=value,
        batch_size=batch_size,
        ingest_hook=ingest_hook,
    )
