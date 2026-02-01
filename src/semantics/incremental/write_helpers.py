"""Write helpers for incremental Delta persistence via WritePipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
    WriteResult,
)
from datafusion_engine.lineage.diagnostics import recorder_for_profile
from storage.deltalake import delta_schema_configuration, delta_write_configuration
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from semantics.incremental.runtime import IncrementalRuntime
    from storage.deltalake import StorageOptions


@dataclass(frozen=True)
class IncrementalDeltaWriteRequest:
    """Delta write request payload for incremental pipelines."""

    destination: str
    mode: WriteMode
    schema_mode: Literal["merge", "overwrite"] | None = None
    partition_by: tuple[str, ...] = ()
    commit_metadata: Mapping[str, object] | None = None
    write_policy: DeltaWritePolicy | None = None
    schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    constraints: tuple[str, ...] = ()
    operation_id: str | None = None


def write_delta_table_via_pipeline(
    *,
    runtime: IncrementalRuntime,
    table: TableLike | pa.Table,
    request: IncrementalDeltaWriteRequest,
) -> WriteResult:
    """Write an Arrow table to Delta via WritePipeline.

    Parameters
    ----------
    runtime:
        Incremental runtime providing SessionRuntime and diagnostics.
    table:
        Arrow table or table-like input to write.
    request:
        Write request settings for the Delta destination.

    Returns
    -------
    WriteResult
        Result payload from the WritePipeline write.
    """
    session_runtime = runtime.session_runtime()
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__incremental_write_{uuid7_hex()}",
        value=table,
    )
    configuration: dict[str, str] = {}
    configuration.update(delta_write_configuration(request.write_policy) or {})
    configuration.update(delta_schema_configuration(request.schema_policy) or {})
    format_options: dict[str, object] = {}
    if request.commit_metadata:
        format_options["commit_metadata"] = dict(request.commit_metadata)
    if request.schema_mode is not None:
        format_options["schema_mode"] = request.schema_mode
    if configuration:
        format_options["table_properties"] = configuration
    if request.storage_options is not None:
        format_options["storage_options"] = dict(request.storage_options)
    if request.log_storage_options is not None:
        format_options["log_storage_options"] = dict(request.log_storage_options)
    if request.write_policy is not None and request.write_policy.target_file_size is not None:
        format_options["target_file_size"] = request.write_policy.target_file_size
    pipeline = WritePipeline(
        session_runtime.ctx,
        sql_options=runtime.profile.sql_options(),
        recorder=recorder_for_profile(
            runtime.profile,
            operation_id=request.operation_id or request.destination,
        ),
        runtime_profile=runtime.profile,
    )
    return pipeline.write(
        WriteRequest(
            source=df,
            destination=request.destination,
            format=WriteFormat.DELTA,
            mode=request.mode,
            partition_by=request.partition_by,
            format_options=format_options or None,
            constraints=request.constraints,
        )
    )


__all__ = ["IncrementalDeltaWriteRequest", "write_delta_table_via_pipeline"]
