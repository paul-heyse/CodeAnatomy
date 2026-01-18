"""Helpers for building Ibis plans from table-like sources."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.context import Ordering
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ibis_schema_from_arrow, normalize_table_for_ibis

DatabaseHint = tuple[str, str] | str | None


class ViewBackend(Protocol):
    """Protocol for backends supporting view registration."""

    def create_view(
        self,
        name: str,
        expr: IbisTable,
        *,
        database: DatabaseHint = None,
        overwrite: bool = False,
    ) -> None:
        """Register an Ibis view on the backend."""
        ...


@dataclass(frozen=True)
class SourceToIbisOptions:
    """Options for bridging sources into Ibis plans."""

    backend: BaseBackend
    name: str | None = None
    ordering: Ordering | None = None
    overwrite: bool = True


def table_to_ibis(
    table: TableLike,
    *,
    backend: BaseBackend,
    name: str | None = None,
    ordering: Ordering | None = None,
    overwrite: bool = True,
) -> IbisPlan:
    """Register a table-like value as an Ibis view and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered view when a name is provided.
    """
    if isinstance(table, pa.Table):
        normalized = normalize_table_for_ibis(table)
        expr = ibis.memtable(normalized, schema=ibis_schema_from_arrow(normalized.schema))
    else:
        expr = ibis.memtable(table)
    return register_ibis_view(
        expr,
        backend=backend,
        name=name,
        ordering=ordering,
        overwrite=overwrite,
    )


def register_ibis_view(
    expr: IbisTable,
    *,
    backend: BaseBackend,
    name: str | None,
    ordering: Ordering | None = None,
    overwrite: bool = True,
) -> IbisPlan:
    """Register an Ibis expression as a backend view and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered view.
    """
    view_name = _resolve_name(name)
    if view_name is None:
        return IbisPlan(expr=expr, ordering=ordering or Ordering.unordered())
    backend_view = cast("ViewBackend", backend)
    database = _default_database_hint(backend)
    if database is None:
        backend_view.create_view(view_name, expr, overwrite=overwrite)
        registered = backend.table(view_name)
    else:
        backend_view.create_view(view_name, expr, database=database, overwrite=overwrite)
        registered = backend.table(view_name, database=database)
    return IbisPlan(expr=registered, ordering=ordering or Ordering.unordered())


def source_to_ibis(
    source: IbisPlan | IbisTable | TableLike | RecordBatchReaderLike,
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Bridge a source into an Ibis plan.

    Returns
    -------
    IbisPlan
        Ibis plan registered on the backend when a name is provided.
    """
    if isinstance(source, IbisPlan):
        return source
    if isinstance(source, IbisTable):
        if options.name:
            return register_ibis_view(
                source,
                backend=options.backend,
                name=options.name,
                ordering=options.ordering,
                overwrite=options.overwrite,
            )
        return IbisPlan(expr=source, ordering=options.ordering or Ordering.unordered())
    table = _ensure_table(source)
    return table_to_ibis(
        table,
        backend=options.backend,
        name=options.name,
        ordering=options.ordering,
        overwrite=options.overwrite,
    )


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _resolve_name(name: str | None) -> str | None:
    if not name:
        return None
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    return sanitized or "view"


def _default_database_hint(backend: BaseBackend) -> tuple[str, str] | None:
    catalog = getattr(backend, "current_catalog", None)
    if callable(catalog):
        catalog = catalog()
    database = getattr(backend, "current_database", None)
    if callable(database):
        database = database()
    if isinstance(catalog, str) and isinstance(database, str):
        return (catalog, database)
    return None


__all__ = [
    "SourceToIbisOptions",
    "register_ibis_view",
    "source_to_ibis",
    "table_to_ibis",
]
