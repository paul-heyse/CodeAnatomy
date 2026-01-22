"""Normalize output writers for Delta-backed datasets."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import TableLike
from datafusion_engine.table_provider_metadata import table_provider_metadata
from ibis_engine.execution import IbisExecutionContext
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from normalize.runtime import NormalizeRuntime

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class NormalizeDeltaWriteResult:
    """Result payload for normalize Delta writes."""

    table_name: str
    mode: str
    rows_affected: int | None
    commit_key: str
    app_id: str | None
    version: int | None
    path: str


@dataclass(frozen=True)
class NormalizeDeltaWriteRequest:
    """Inputs for normalize Delta writes."""

    table_name: str
    dataframe: DataFrame | TableLike
    mode: Literal["append", "overwrite"] = "append"
    commit_key: str | None = None
    commit_metadata: Mapping[str, object] | None = None


def write_normalize_output_delta(
    runtime: NormalizeRuntime,
    request: NormalizeDeltaWriteRequest,
) -> NormalizeDeltaWriteResult:
    """Write a normalize output table into Delta via Ibis to_delta.

    Parameters
    ----------
    runtime:
        Normalize runtime with DataFusion session + Ibis backend.
    request:
        Write request containing output table and commit metadata.

    Returns
    -------
    NormalizeDeltaWriteResult
        Write metadata including reserved commit identifiers.

    Raises
    ------
    ValueError
        Raised when the table is not registered or not Delta-backed.
    """
    table_name = request.table_name
    metadata = table_provider_metadata(id(runtime.ctx), table_name=table_name)
    if metadata is None or metadata.storage_location is None:
        msg = f"Delta storage location missing for table {table_name!r}."
        raise ValueError(msg)
    if metadata.file_format is not None and metadata.file_format != "delta":
        msg = f"Table {table_name!r} is not registered as a Delta table."
        raise ValueError(msg)
    path = metadata.storage_location
    key = request.commit_key or path
    commit_metadata = _stringify_commit_metadata(request.commit_metadata)
    commit_options, commit_run = runtime.runtime_profile.reserve_delta_commit(
        key=key,
        metadata=request.commit_metadata,
        commit_metadata=commit_metadata,
    )
    table, row_count = _normalize_write_input(request.dataframe)
    delta_options = IbisDeltaWriteOptions(
        mode=request.mode,
        commit_metadata=commit_metadata,
        app_id=commit_options.app_id,
        version=commit_options.version,
    )
    result = write_ibis_dataset_delta(
        table,
        path,
        options=IbisDatasetWriteOptions(
            execution=_normalize_execution(runtime),
            writer_strategy="datafusion",
            delta_options=delta_options,
        ),
    )
    runtime.runtime_profile.finalize_delta_commit(
        key=key,
        run=commit_run,
        metadata={"rows_affected": row_count},
    )
    write_result = NormalizeDeltaWriteResult(
        table_name=table_name,
        mode=request.mode,
        rows_affected=row_count,
        commit_key=key,
        app_id=commit_options.app_id,
        version=commit_options.version,
        path=result.path,
    )
    _record_delta_write(runtime, result=write_result)
    return write_result


def _normalize_write_input(source: DataFrame | TableLike) -> tuple[TableLike, int]:
    if isinstance(source, DataFrame):
        table = source.to_arrow_table()
        return table, table.num_rows
    return source, source.num_rows


def _normalize_execution(runtime: NormalizeRuntime) -> IbisExecutionContext:
    return IbisExecutionContext(
        ctx=runtime.execution_ctx,
        ibis_backend=runtime.ibis_backend,
    )


def _stringify_commit_metadata(
    metadata: Mapping[str, object] | None,
) -> Mapping[str, str] | None:
    if metadata is None:
        return None
    resolved = {str(key): str(value) for key, value in metadata.items()}
    return resolved or None


def _record_delta_write(runtime: NormalizeRuntime, *, result: NormalizeDeltaWriteResult) -> None:
    if runtime.diagnostics is None:
        return
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "table_name": result.table_name,
        "path": result.path,
        "mode": result.mode,
        "rows_affected": result.rows_affected,
        "commit_key": result.commit_key,
        "commit_options": {"app_id": result.app_id, "version": result.version},
    }
    runtime.diagnostics.record_artifact("normalize_delta_writes_v1", payload)


__all__ = [
    "NormalizeDeltaWriteRequest",
    "NormalizeDeltaWriteResult",
    "write_normalize_output_delta",
]
