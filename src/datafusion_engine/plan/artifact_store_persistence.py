"""Persistence helpers extracted from ``artifact_store_core``."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.plan.artifact_serialization import (
    msgpack_payload_raw as _msgpack_payload_raw,
)
from serde_artifacts import DeltaStatsDecision, WriteArtifactRow

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

_WRITE_EVENT_KIND = "write"


def _profile_name(profile: DataFusionRuntimeProfile) -> str | None:
    return profile.policies.config_policy_name


@dataclass(frozen=True)
class WriteArtifactRequest:
    """Inputs for persisting write artifacts."""

    destination: str
    write_format: str
    mode: str
    method: str
    table_uri: str
    delta_version: int | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    partition_by: Sequence[str] = ()
    table_properties: Mapping[str, str] | None = None
    commit_metadata: Mapping[str, str] | None = None
    stats_decision: DeltaStatsDecision | None = None
    duration_ms: float | None = None
    row_count: int | None = None
    status: str | None = None
    error: str | None = None


def persist_write_artifact(
    profile: DataFusionRuntimeProfile,
    *,
    request: WriteArtifactRequest,
) -> WriteArtifactRow | None:
    """Persist a write artifact row for a Delta write operation.

    Returns:
    -------
    WriteArtifactRow | None
        Persisted row payload.
    """
    row = WriteArtifactRow(
        event_time_unix_ms=int(time.time() * 1000),
        profile_name=_profile_name(profile),
        event_kind=_WRITE_EVENT_KIND,
        destination=request.destination,
        format=request.write_format,
        mode=request.mode,
        method=request.method,
        table_uri=request.table_uri,
        delta_version=request.delta_version,
        commit_app_id=request.commit_app_id,
        commit_version=request.commit_version,
        commit_run_id=request.commit_run_id,
        delta_write_policy_msgpack=_msgpack_payload_raw(
            request.delta_write_policy if request.delta_write_policy is not None else {}
        ),
        delta_schema_policy_msgpack=_msgpack_payload_raw(
            request.delta_schema_policy if request.delta_schema_policy is not None else {}
        ),
        partition_by=tuple(request.partition_by),
        table_properties=dict(request.table_properties) if request.table_properties else {},
        commit_metadata=dict(request.commit_metadata) if request.commit_metadata else {},
        delta_stats_decision_msgpack=_msgpack_payload_raw(
            request.stats_decision if request.stats_decision is not None else {}
        ),
        duration_ms=request.duration_ms,
        row_count=request.row_count,
        status=request.status,
        error=request.error,
    )
    from serde_artifact_specs import WRITE_ARTIFACT_SPEC

    record_artifact(profile, WRITE_ARTIFACT_SPEC, row.to_row())
    return row


@dataclass(frozen=True)
class _ArtifactTableWriteRequest:
    """Request payload for artifact table writes."""

    table_path: Path
    arrow_table: pa.Table
    commit_metadata: Mapping[str, str]
    mode: str
    schema_mode: str | None
    operation_id: str


def _write_artifact_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: _ArtifactTableWriteRequest,
) -> int | None:
    from datafusion_engine.io.write_core import WriteFormat, WriteMode, WriteRequest
    from datafusion_engine.io.write_pipeline import WritePipeline
    from datafusion_engine.lineage.diagnostics import recorder_for_profile

    if request.mode == "append":
        write_mode = WriteMode.APPEND
    elif request.mode == "overwrite":
        write_mode = WriteMode.OVERWRITE
    else:
        msg = f"Unsupported write mode for artifacts: {request.mode!r}."
        raise ValueError(msg)
    format_options: dict[str, object] = {"commit_metadata": dict(request.commit_metadata)}
    if request.schema_mode is not None:
        format_options["schema_mode"] = request.schema_mode
    df = ctx.from_arrow(request.arrow_table)
    recorder = recorder_for_profile(profile, operation_id=request.operation_id)
    pipeline = WritePipeline(
        ctx,
        sql_options=profile.sql_options(),
        recorder=recorder,
        runtime_profile=profile,
    )
    result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(request.table_path),
            format=WriteFormat.DELTA,
            mode=write_mode,
            format_options=format_options,
        )
    )
    if result.delta_result is not None and result.delta_result.version is not None:
        return result.delta_result.version
    return profile.delta_ops.delta_service().table_version(path=str(request.table_path))


__all__ = [
    "WriteArtifactRequest",
    "persist_write_artifact",
]
