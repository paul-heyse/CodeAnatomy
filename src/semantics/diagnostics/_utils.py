"""Shared helpers for semantic diagnostics DataFrame builders."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.arrow.interop import empty_table_for_schema


def empty_diagnostic_frame(ctx: SessionContext, schema: pa.Schema) -> DataFrame:
    """Return an empty diagnostics DataFrame for a target schema.

    Raises:
        TypeError: If ``ctx`` has no supported Arrow registration method.
    """
    empty = empty_table_for_schema(schema)
    from_arrow = getattr(ctx, "from_arrow", None)
    if callable(from_arrow):
        return cast("DataFrame", from_arrow(empty))
    from_arrow_table = getattr(ctx, "from_arrow_table", None)
    if callable(from_arrow_table):
        return cast("DataFrame", from_arrow_table(empty))
    msg = "SessionContext does not expose a supported Arrow registration method."
    raise TypeError(msg)


__all__ = ["empty_diagnostic_frame"]
