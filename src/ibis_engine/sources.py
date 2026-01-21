"""Helpers for building Ibis plans from table-like sources."""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.core.ordering import Ordering
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ibis_schema_from_arrow
from obs.diagnostics import DiagnosticsCollector

DatabaseHint = tuple[str, str] | str | None
_QUALIFIED_PARTS_SINGLE = 1
_QUALIFIED_PARTS_DOUBLE = 2
_QUALIFIED_PARTS_TRIPLE = 3


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


class TableBackend(Protocol):
    """Protocol for backends supporting table registration."""

    def create_table(
        self,
        name: str,
        obj: object | None = None,
        *,
        schema: object | None = None,
        **kwargs: object,
    ) -> IbisTable:
        """Register a table on the backend.

        Keyword arguments may include database, temp, or overwrite options.
        """
        ...

    def table(
        self,
        name: str,
        *,
        database: DatabaseHint = None,
    ) -> IbisTable:
        """Return a table expression from the backend."""
        ...


@dataclass(frozen=True)
class SourceToIbisOptions:
    """Options for bridging sources into Ibis plans."""

    backend: BaseBackend
    name: str | None = None
    ordering: Ordering | None = None
    overwrite: bool = True
    namespace_recorder: Callable[[Mapping[str, object]], None] | None = None


def table_to_ibis(
    table: TableLike,
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Register a table-like value as an Ibis table and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered table.
    """
    return register_ibis_table(table, options=options)


def register_ibis_table(
    table: TableLike,
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Register a table-like value as a backend table and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered table.
    """
    backend = cast("TableBackend", options.backend)
    database_hint, name = _parse_database_hint(options.name)
    table_name = name or _temporary_table_name()
    table_value = _as_pyarrow_table(table)
    schema = ibis_schema_from_arrow(table_value.schema)
    temp = name is None
    backend.create_table(
        table_name,
        obj=table_value,
        schema=schema,
        database=database_hint or _default_database_hint(options.backend),
        temp=temp,
        overwrite=options.overwrite,
    )
    _record_namespace_action(
        options.namespace_recorder,
        action="create_table",
        name=table_name,
        database=database_hint or _default_database_hint(options.backend),
        overwrite=options.overwrite,
    )
    if database_hint is None:
        registered = backend.table(table_name)
    else:
        registered = backend.table(table_name, database=database_hint)
    return IbisPlan(expr=registered, ordering=options.ordering or Ordering.unordered())


def register_ibis_view(
    expr: IbisTable,
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Register an Ibis expression as a backend view and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered view.
    """
    database_hint, view_name = _parse_database_hint(options.name)
    if view_name is None:
        return IbisPlan(expr=expr, ordering=options.ordering or Ordering.unordered())
    backend_view = cast("ViewBackend", options.backend)
    database = database_hint or _default_database_hint(options.backend)
    if database is None:
        backend_view.create_view(view_name, expr, overwrite=options.overwrite)
        _record_namespace_action(
            options.namespace_recorder,
            action="create_view",
            name=view_name,
            database=None,
            overwrite=options.overwrite,
        )
        registered = options.backend.table(view_name)
    else:
        backend_view.create_view(
            view_name,
            expr,
            database=database,
            overwrite=options.overwrite,
        )
        _record_namespace_action(
            options.namespace_recorder,
            action="create_view",
            name=view_name,
            database=database,
            overwrite=options.overwrite,
        )
        registered = options.backend.table(view_name, database=database)
    return IbisPlan(expr=registered, ordering=options.ordering or Ordering.unordered())


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
                options=options,
            )
        return IbisPlan(expr=source, ordering=options.ordering or Ordering.unordered())
    table = _ensure_table(source)
    return table_to_ibis(
        table,
        options=options,
    )


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _as_pyarrow_table(value: TableLike) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    return pa.table(value)


def _temporary_table_name() -> str:
    return f"tmp_{uuid.uuid4().hex}"


def _resolve_name(name: str | None) -> str | None:
    if not name:
        return None
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    return sanitized or "view"


def _parse_database_hint(name: str | None) -> tuple[DatabaseHint, str | None]:
    if not name:
        return None, None
    parts = [part for part in name.split(".") if part]
    if not parts:
        return None, None
    if len(parts) == _QUALIFIED_PARTS_SINGLE:
        return None, _resolve_name(parts[0])
    if len(parts) == _QUALIFIED_PARTS_DOUBLE:
        return _resolve_name(parts[0]), _resolve_name(parts[1])
    if len(parts) == _QUALIFIED_PARTS_TRIPLE:
        catalog = _resolve_name(parts[0])
        schema = _resolve_name(parts[1])
        table = _resolve_name(parts[2])
        return (cast("str", catalog), cast("str", schema)), table
    msg = f"Unsupported qualified table name: {name!r}."
    raise ValueError(msg)


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


def _database_payload(database: DatabaseHint) -> dict[str, object]:
    if database is None:
        return {"catalog": None, "schema": None, "database": None}
    if isinstance(database, tuple):
        return {"catalog": database[0], "schema": database[1], "database": None}
    return {"catalog": None, "schema": database, "database": database}


def _record_namespace_action(
    recorder: Callable[[Mapping[str, object]], None] | None,
    *,
    action: str,
    name: str,
    database: DatabaseHint,
    overwrite: bool,
) -> None:
    if recorder is None:
        return
    payload: dict[str, object] = {
        "action": action,
        "name": name,
        "overwrite": overwrite,
        **_database_payload(database),
    }
    recorder(payload)


def namespace_recorder_from_ctx(
    ctx: object | None,
) -> Callable[[Mapping[str, object]], None] | None:
    """Return a namespace recorder derived from an execution context.

    Returns
    -------
    Callable[[Mapping[str, object]], None] | None
        Recorder callback when diagnostics are enabled.
    """
    if ctx is None:
        return None
    runtime = getattr(ctx, "runtime", None)
    datafusion = getattr(runtime, "datafusion", None)
    diagnostics = getattr(datafusion, "diagnostics_sink", None)
    if diagnostics is None:
        return None
    diagnostics_sink = cast("DiagnosticsCollector", diagnostics)

    def _record(payload: Mapping[str, object]) -> None:
        diagnostics_sink.record_artifact("ibis_namespace_actions_v1", payload)

    return _record


def record_namespace_action(
    recorder: Callable[[Mapping[str, object]], None] | None,
    *,
    action: str,
    name: str,
    database: DatabaseHint,
    overwrite: bool,
) -> None:
    """Record a namespace action for diagnostics.

    Parameters
    ----------
    recorder:
        Recorder callback to emit diagnostics.
    action:
        Action name (create_view, create_table, insert, etc.).
    name:
        Target object name.
    database:
        Optional database/catalog hint.
    overwrite:
        Whether the action overwrites existing namespace entries.
    """
    _record_namespace_action(
        recorder,
        action=action,
        name=name,
        database=database,
        overwrite=overwrite,
    )


def resolve_database_hint(name: str | None) -> tuple[DatabaseHint, str | None]:
    """Resolve a database hint and sanitized name from a qualified string.

    Parameters
    ----------
    name:
        Qualified name to parse.

    Returns
    -------
    tuple[DatabaseHint, str | None]
        Database hint and sanitized name.
    """
    return _parse_database_hint(name)


__all__ = [
    "DatabaseHint",
    "SourceToIbisOptions",
    "namespace_recorder_from_ctx",
    "record_namespace_action",
    "register_ibis_table",
    "register_ibis_view",
    "resolve_database_hint",
    "source_to_ibis",
    "table_to_ibis",
]
