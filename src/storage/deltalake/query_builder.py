"""Delta QueryBuilder execution helpers."""

from __future__ import annotations

import importlib
from typing import cast

from deltalake import DeltaTable

from arrowdsl.core.interop import RecordBatchReaderLike
from storage.deltalake.delta import StorageOptions


def query_delta_via_querybuilder(
    path: str,
    sql: str,
    *,
    table_name: str = "t",
    storage_options: StorageOptions | None = None,
) -> RecordBatchReaderLike:
    """Execute SQL against a Delta table using deltalake QueryBuilder.

    Returns
    -------
    RecordBatchReaderLike
        Streaming record batch reader for the query.

    Raises
    ------
    ImportError
        Raised when deltalake.QueryBuilder is unavailable.
    """
    query_builder = _query_builder_class()
    if query_builder is None:
        msg = "deltalake.QueryBuilder is unavailable."
        raise ImportError(msg)
    storage = dict(storage_options) if storage_options else None
    table = DeltaTable(path, storage_options=storage)
    builder = query_builder().register(table_name, table)
    return cast("RecordBatchReaderLike", builder.execute(sql))


def query_builder_available() -> bool:
    """Return True when deltalake QueryBuilder is available.

    Returns
    -------
    bool
        ``True`` when QueryBuilder is available.
    """
    return _query_builder_class() is not None


def _query_builder_class() -> type | None:
    try:
        module = importlib.import_module("deltalake.query")
    except ImportError:
        module = importlib.import_module("deltalake")
    query_builder = getattr(module, "QueryBuilder", None)
    return query_builder if isinstance(query_builder, type) else None


__all__ = ["query_builder_available", "query_delta_via_querybuilder"]
