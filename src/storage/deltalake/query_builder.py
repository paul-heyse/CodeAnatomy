"""Query Delta tables using delta-rs' embedded DataFusion QueryBuilder."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from pyarrow import RecordBatchReader


class _QueryBuilderProtocol(Protocol):
    def __init__(self) -> None: ...

    def register(
        self,
        table_name: str,
        delta_table: DeltaTable,
    ) -> _QueryBuilderProtocol: ...

    def execute(self, sql: str) -> RecordBatchReader: ...


def _require_query_builder() -> type[_QueryBuilderProtocol]:
    """Return the delta-rs QueryBuilder class.

    Returns
    -------
    object
        QueryBuilder class from delta-rs.

    Raises
    ------
    RuntimeError
        Raised when deltalake.QueryBuilder is unavailable.
    """
    try:
        from deltalake import QueryBuilder
    except ImportError as exc:  # pragma: no cover - optional dependency
        msg = "deltalake.QueryBuilder is unavailable."
        raise RuntimeError(msg) from exc
    return cast("type[_QueryBuilderProtocol]", QueryBuilder)


def open_delta_table(path: str, storage_options: Mapping[str, str] | None = None) -> DeltaTable:
    """Open a DeltaTable with optional storage options.

    Returns
    -------
    DeltaTable
        Opened DeltaTable instance.

    Raises
    ------
    RuntimeError
        Raised when deltalake.DeltaTable is unavailable.
    """
    try:
        from deltalake import DeltaTable
    except ImportError as exc:  # pragma: no cover - optional dependency
        msg = "deltalake.DeltaTable is unavailable."
        raise RuntimeError(msg) from exc
    options = dict(storage_options) if storage_options else None
    return DeltaTable(path, storage_options=options)


def execute_query(
    sql: str,
    tables: Mapping[str, DeltaTable],
) -> RecordBatchReader:
    """Execute SQL using the embedded QueryBuilder engine.

    Returns
    -------
    RecordBatchReader
        Record batch reader with query results.
    """
    query_builder_cls = _require_query_builder()
    builder = query_builder_cls()
    for name, table in tables.items():
        builder = builder.register(name, table)
    return builder.execute(sql)
