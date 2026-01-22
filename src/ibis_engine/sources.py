"""Helpers for building Ibis plans from table-like sources."""

from __future__ import annotations

import contextlib
import inspect
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal, Protocol, cast

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction, WriterProperties
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.core.ordering import Ordering
from datafusion_engine.table_provider_metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.registry import datafusion_context
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
    table_metadata: Mapping[str, str] | None = None


class DatasetSpecLike(Protocol):
    """Protocol for dataset specs used in Ibis plan compilation."""

    def query(self) -> IbisQuerySpec:
        """Return the query spec for the dataset."""
        ...

    def ordering(self) -> Ordering:
        """Return the ordering metadata for the dataset."""
        ...


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + dataset spec pairing for Ibis plan compilation."""

    dataset: TableLike | RecordBatchReaderLike | IbisTable
    spec: DatasetSpecLike


type PlanSource = IbisPlan | IbisTable | TableLike | RecordBatchReaderLike | DatasetSource
type DeltaWriteMode = Literal["error", "append", "overwrite", "ignore"]
type DeltaSchemaMode = Literal["merge", "overwrite"]


@dataclass(frozen=True)
class IbisDeltaReadOptions:
    """Options for Delta reads via Ibis backends."""

    table_name: str | None = None
    storage_options: Mapping[str, str] | None = None
    version: int | None = None
    timestamp: str | None = None
    options: Mapping[str, object] | None = None


@dataclass(frozen=True)
class IbisDeltaWriteOptions:
    """Options for Delta writes via Ibis backends."""

    mode: DeltaWriteMode = "append"
    schema_mode: DeltaSchemaMode | None = None
    predicate: str | None = None
    partition_by: Sequence[str] | None = None
    configuration: Mapping[str, str | None] | None = None
    commit_metadata: Mapping[str, str] | None = None
    target_file_size: int | None = None
    writer_properties: WriterProperties | None = None
    app_id: str | None = None
    version: int | None = None
    storage_options: Mapping[str, str] | None = None


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


def record_batches_to_ibis(
    batches: Sequence[pa.RecordBatch],
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Register record batches as an Ibis table and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered table.
    """
    return register_ibis_record_batches(batches, options=options)


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
    _record_table_metadata(options, table_name)
    return IbisPlan(expr=registered, ordering=options.ordering or Ordering.unordered())


def register_ibis_record_batches(
    batches: Sequence[pa.RecordBatch],
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Register record batches as a backend table and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered table.

    Raises
    ------
    TypeError
        Raised when record batch registration is unsupported.
    ValueError
        Raised when the input batch list is empty.
    """
    if not batches:
        msg = "Record batch registration requires at least one batch."
        raise ValueError(msg)
    backend = cast("TableBackend", options.backend)
    database_hint, name = _parse_database_hint(options.name)
    table_name = name or _temporary_table_name()
    try:
        ctx = datafusion_context(options.backend)
    except ValueError:
        table = pa.Table.from_batches(batches)
        return register_ibis_table(table, options=options)
    register_batches = getattr(ctx, "register_record_batches", None)
    if not callable(register_batches):
        msg = "Ibis backend does not support record batch registration."
        raise TypeError(msg)
    if options.overwrite:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(table_name)
    register_batches(table_name, [list(batches)])
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
    _record_table_metadata(options, table_name)
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
    _record_table_metadata(options, view_name)
    return IbisPlan(expr=registered, ordering=options.ordering or Ordering.unordered())


def _record_table_metadata(options: SourceToIbisOptions, table_name: str) -> None:
    if not options.table_metadata:
        return
    with contextlib.suppress(ValueError, TypeError):
        ctx = datafusion_context(options.backend)
        existing = table_provider_metadata(id(ctx), table_name=table_name)
        base = existing or TableProviderMetadata(table_name=table_name)
        merged = dict(base.metadata)
        for key, value in options.table_metadata.items():
            merged[str(key)] = str(value)
        metadata = replace(base, metadata=merged)
        record_table_provider_metadata(id(ctx), metadata=metadata)


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


def plan_from_dataset(
    dataset: TableLike | RecordBatchReaderLike | IbisTable,
    *,
    spec: DatasetSpecLike,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None = None,
) -> IbisPlan:
    """Return an Ibis plan for a dataset spec.

    Returns
    -------
    IbisPlan
        Ibis plan with the query spec applied.
    """
    plan = _plan_from_source(dataset, ctx=ctx, backend=backend, name=name)
    expr = apply_query_spec(plan.expr, spec=spec.query())
    return IbisPlan(expr=expr, ordering=spec.ordering())


def plan_from_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None = None,
) -> IbisPlan:
    """Return an Ibis plan for a source value.

    Returns
    -------
    IbisPlan
        Ibis plan for the source.
    """
    if isinstance(source, DatasetSource):
        return plan_from_dataset(
            source.dataset,
            spec=source.spec,
            ctx=ctx,
            backend=backend,
            name=name,
        )
    return _plan_from_source(source, ctx=ctx, backend=backend, name=name)


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _plan_from_source(
    source: IbisPlan | IbisTable | TableLike | RecordBatchReaderLike,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None,
) -> IbisPlan:
    ordering = _ordering_for_ctx(ctx)
    options = SourceToIbisOptions(
        backend=backend,
        name=name,
        ordering=ordering,
        namespace_recorder=namespace_recorder_from_ctx(ctx),
    )
    return source_to_ibis(source, options=options)


def _ordering_for_ctx(ctx: ExecutionContext) -> Ordering:
    if ctx.runtime.scan.implicit_ordering:
        return Ordering.implicit()
    return Ordering.unordered()


def _as_pyarrow_table(value: TableLike) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    return pa.table(value)


def read_delta_ibis(
    backend: BaseBackend,
    path: str,
    *,
    options: IbisDeltaReadOptions | None = None,
) -> IbisTable:
    """Read a Delta table via the backend read_delta method.

    Parameters
    ----------
    backend : BaseBackend
        Ibis backend to execute the read.
    path : str
        Delta table path.
    options : IbisDeltaReadOptions | None
        Read options controlling table name and storage configuration.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table expression for the Delta source.

    Raises
    ------
    TypeError
        Raised when the backend does not support read_delta.
    """
    read_delta = getattr(backend, "read_delta", None)
    if not callable(read_delta):
        msg = f"Backend {type(backend).__name__} does not support read_delta."
        raise TypeError(msg)
    resolved = options or IbisDeltaReadOptions()
    kwargs: dict[str, object] = dict(resolved.options or {})
    if resolved.table_name is not None:
        kwargs.setdefault("table_name", resolved.table_name)
    if resolved.storage_options:
        kwargs.setdefault("storage_options", dict(resolved.storage_options))
    if resolved.version is not None:
        kwargs["version"] = resolved.version
    if resolved.timestamp is not None:
        kwargs["timestamp"] = resolved.timestamp
    result = read_delta(path, **_filter_kwargs(read_delta, kwargs))
    return cast("IbisTable", result)


def write_delta_ibis(
    backend: BaseBackend,
    expr: IbisTable,
    path: str,
    *,
    options: IbisDeltaWriteOptions | None = None,
    storage_options: Mapping[str, str] | None = None,
) -> int | None:
    """Write a Delta table via the backend to_delta method.

    Parameters
    ----------
    backend : BaseBackend
        Ibis backend to execute the write.
    expr : IbisTable
        Ibis table expression to write.
    path : str
        Delta table path.
    options : IbisDeltaWriteOptions | None
        Delta write options.
    storage_options : Mapping[str, str] | None
        Storage options to merge with the write options.

    Returns
    -------
    int | None
        Delta table version when available.

    Raises
    ------
    TypeError
        Raised when the backend does not support to_delta.
    ValueError
        Raised when predicate filters are used without overwrite mode.
    """
    to_delta = getattr(backend, "to_delta", None)
    if not callable(to_delta):
        msg = f"Backend {type(backend).__name__} does not support to_delta."
        raise TypeError(msg)
    resolved = options or IbisDeltaWriteOptions()
    if resolved.predicate is not None and resolved.mode != "overwrite":
        msg = "Delta predicate filters require overwrite mode."
        raise ValueError(msg)
    merged_storage = _merge_storage_options(resolved.storage_options, storage_options)
    kwargs: dict[str, object] = {
        "mode": resolved.mode,
        "predicate": resolved.predicate,
        "partition_by": list(resolved.partition_by) if resolved.partition_by else None,
        "configuration": dict(resolved.configuration) if resolved.configuration else None,
        "target_file_size": resolved.target_file_size,
        "writer_properties": resolved.writer_properties,
        "storage_options": dict(merged_storage) if merged_storage else None,
    }
    if resolved.schema_mode is not None:
        kwargs["schema_mode"] = resolved.schema_mode
        if resolved.schema_mode == "overwrite":
            kwargs["overwrite_schema"] = True
    commit_properties = _commit_properties(resolved)
    if commit_properties is not None:
        kwargs["commit_properties"] = commit_properties
    to_delta(expr, path, **_filter_kwargs(to_delta, kwargs))
    return _delta_table_version(path, storage_options=merged_storage)


def _commit_properties(options: IbisDeltaWriteOptions) -> CommitProperties | None:
    custom_metadata = (
        dict(options.commit_metadata) if options.commit_metadata is not None else None
    )
    app_transactions = None
    if options.app_id is not None and options.version is not None:
        app_transactions = [Transaction(app_id=options.app_id, version=options.version)]
    if custom_metadata is None and app_transactions is None:
        return None
    return CommitProperties(
        app_transactions=app_transactions,
        custom_metadata=custom_metadata,
    )


def _merge_storage_options(
    base: Mapping[str, str] | None,
    overrides: Mapping[str, str] | None,
) -> Mapping[str, str] | None:
    merged: dict[str, str] = {}
    if base:
        merged.update({str(key): str(value) for key, value in base.items()})
    if overrides:
        merged.update({str(key): str(value) for key, value in overrides.items()})
    return merged or None


def _delta_table_version(
    path: str,
    *,
    storage_options: Mapping[str, str] | None,
) -> int | None:
    storage = dict(storage_options) if storage_options else None
    try:
        return DeltaTable(path, storage_options=storage).version()
    except (RuntimeError, TypeError, ValueError):
        return None


def _filter_kwargs(
    fn: Callable[..., object],
    kwargs: Mapping[str, object],
) -> dict[str, object]:
    params = inspect.signature(fn).parameters
    return {key: value for key, value in kwargs.items() if key in params}


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
    "DatasetSource",
    "DatasetSpecLike",
    "IbisDeltaReadOptions",
    "IbisDeltaWriteOptions",
    "PlanSource",
    "SourceToIbisOptions",
    "namespace_recorder_from_ctx",
    "plan_from_dataset",
    "plan_from_source",
    "read_delta_ibis",
    "record_batches_to_ibis",
    "record_namespace_action",
    "register_ibis_record_batches",
    "register_ibis_table",
    "register_ibis_view",
    "resolve_database_hint",
    "source_to_ibis",
    "table_to_ibis",
    "write_delta_ibis",
]
