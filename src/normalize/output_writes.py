"""Normalize output write helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.interop import TableLike
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from normalize.runtime import NormalizeRuntime


@dataclass(frozen=True)
class NormalizeDeltaWriteRequest:
    """Inputs for writing a normalize output Delta table."""

    table_name: str
    dataframe: TableLike
    path: str
    mode: Literal["append", "overwrite"] = "append"
    commit_metadata: Mapping[str, str] | None = None


@dataclass(frozen=True)
class NormalizeDeltaWriteResult:
    """Result metadata for normalize Delta writes."""

    path: str
    rows_affected: int | None
    mode: str
    commit_key: str | None
    app_id: str | None
    version: int | None


def write_normalize_output_delta(
    runtime: NormalizeRuntime,
    request: NormalizeDeltaWriteRequest,
) -> NormalizeDeltaWriteResult:
    """Write a normalize output table as Delta.

    Returns
    -------
    NormalizeDeltaWriteResult
        Metadata about the Delta write.
    """
    execution = ibis_execution_from_ctx(runtime.execution_ctx, backend=runtime.ibis_backend)
    schema_mode = "overwrite" if request.mode == "overwrite" else "merge"
    result = write_ibis_dataset_delta(
        request.dataframe,
        request.path,
        options=IbisDatasetWriteOptions(
            execution=execution,
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode=request.mode,
                schema_mode=schema_mode,
                commit_metadata=request.commit_metadata,
            ),
        ),
        table_name=request.table_name,
    )
    return NormalizeDeltaWriteResult(
        path=result.path,
        rows_affected=None,
        mode=request.mode,
        commit_key=request.path,
        app_id=None,
        version=result.version,
    )


__all__ = [
    "NormalizeDeltaWriteRequest",
    "NormalizeDeltaWriteResult",
    "write_normalize_output_delta",
]
