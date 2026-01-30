"""DataFusion session helper utilities."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.catalog.introspection import invalidate_introspection_cache
from datafusion_engine.io.ingest import datafusion_from_arrow
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion import SessionContext


def register_temp_table(
    ctx: SessionContext,
    table: object,
    *,
    prefix: str = "__temp_",
) -> str:
    """Register a PyArrow table as a temporary table.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table
        Arrow-like input to register.
    prefix
        Prefix for the generated table name.

    Returns
    -------
    str
        The generated table name.
    """
    name = f"{prefix}{uuid7_hex()}"
    resolved = to_arrow_table(table)
    datafusion_from_arrow(ctx, name=name, value=resolved)
    return name


def deregister_table(ctx: SessionContext, name: str) -> None:
    """Deregister a table from the session context.

    Parameters
    ----------
    ctx
        DataFusion session context.
    name
        Name of the table to deregister.
    """
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)
            invalidate_introspection_cache(ctx)


@contextmanager
def temp_table(
    ctx: SessionContext,
    table: object,
    *,
    prefix: str = "__temp_",
) -> Iterator[str]:
    """Context manager for temporary table registration.

    Automatically deregisters the table on exit.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table
        Arrow-like input to register.
    prefix
        Prefix for the generated table name.

    Yields
    ------
    str
        The generated table name.
    """
    name = register_temp_table(ctx, table, prefix=prefix)
    try:
        yield name
    finally:
        deregister_table(ctx, name)


__all__ = [
    "deregister_table",
    "register_temp_table",
    "temp_table",
]
