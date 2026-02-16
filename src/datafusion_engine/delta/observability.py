"""Delta observability tables and recording helpers."""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.dataset as ds
from opentelemetry import trace

from datafusion_engine.arrow.field_builders import (
    binary_field,
    bool_field,
    int32_field,
    int64_field,
    list_field,
    string_field,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.payload import (
    msgpack_or_none,
    msgpack_payload,
    string_list,
    string_map,
)
from datafusion_engine.errors import DataFusionEngineError
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)
from datafusion_engine.session.facade import DataFusionExecutionFacade
from obs.otel.run_context import get_run_id
from serde_msgspec import StructBaseStrict
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


DELTA_SNAPSHOT_TABLE_NAME = "datafusion_delta_snapshots_v2"
DELTA_MUTATION_TABLE_NAME = "datafusion_delta_mutations_v2"
DELTA_SCAN_PLAN_TABLE_NAME = "datafusion_delta_scan_plans_v2"
DELTA_MAINTENANCE_TABLE_NAME = "datafusion_delta_maintenance_v2"

try:
    _DEFAULT_OBSERVABILITY_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_OBSERVABILITY_ROOT = Path.cwd() / ".artifacts"

_DELTA_TRACING_LOCK = threading.Lock()
_DELTA_TRACING_READY = threading.Event()
_LOGGER = logging.getLogger(__name__)
_OBSERVABILITY_APPEND_ERRORS = (
    DataFusionEngineError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
    KeyError,
    ImportError,
    AttributeError,
)


def ensure_delta_tracing() -> tuple[bool, str | None]:
    """Initialize delta-rs tracing once per process.

    Returns:
    -------
    tuple[bool, str | None]
        (installed, error) result.
    """
    if _DELTA_TRACING_READY.is_set():
        return True, None
    with _DELTA_TRACING_LOCK:
        if _DELTA_TRACING_READY.is_set():
            return True, None
        try:
            from deltalake import init_tracing
        except ImportError as exc:  # pragma: no cover - optional dependency
            return False, str(exc)
        try:
            init_tracing()
        except (RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover - env-dependent
            _LOGGER.debug("Delta tracing init failed: %s", exc)
            return False, str(exc)
        _DELTA_TRACING_READY.set()
        return True, None


class DeltaSnapshotArtifact(StructBaseStrict, frozen=True):
    """Snapshot artifact payload for Delta tables."""

    table_uri: str
    snapshot: Mapping[str, object]
    dataset_name: str | None = None
    schema_identity_hash: str | None = None
    ddl_fingerprint: str | None = None


class DeltaMutationArtifact(StructBaseStrict, frozen=True):
    """Mutation artifact payload for Delta tables."""

    table_uri: str
    operation: str
    report: Mapping[str, object]
    dataset_name: str | None = None
    mode: str | None = None
    commit_metadata: Mapping[str, str] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None
    constraint_status: str | None = None
    constraint_violations: Sequence[str] = ()


class DeltaScanPlanArtifact(StructBaseStrict, frozen=True):
    """Scan planning artifact payload for Delta tables."""

    dataset_name: str
    table_uri: str
    delta_version: int | None
    snapshot_timestamp: int | None
    total_files: int
    candidate_files: int
    pruned_files: int
    pushed_filters: Sequence[str]
    projected_columns: Sequence[str]
    delta_protocol: DeltaProtocolSnapshot | None


class DeltaMaintenanceArtifact(StructBaseStrict, frozen=True):
    """Maintenance artifact payload for Delta tables (optimize/vacuum/checkpoint)."""

    table_uri: str
    operation: str
    report: Mapping[str, object]
    dataset_name: str | None = None
    retention_hours: int | None = None
    dry_run: bool | None = None
    commit_metadata: Mapping[str, str] | None = None


class DeltaFeatureStateArtifact(StructBaseStrict, frozen=True):
    """Feature state artifact payload for Delta tables."""

    table_uri: str
    enabled_features: Mapping[str, str]
    dataset_name: str | None = None
    delta_version: int | None = None
    commit_metadata: Mapping[str, str] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None


class _AppendObservabilityRequest(StructBaseStrict, frozen=True):
    """Inputs required to append a Delta observability row."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None
    location: DatasetLocation
    schema: pa.Schema
    payload: Mapping[str, object]
    operation: str
    commit_metadata: Mapping[str, str] | None


def _trace_span_ids() -> tuple[str | None, str | None]:
    span = trace.get_current_span()
    if span is None:
        return None, None
    context = span.get_span_context()
    if context is None or not context.is_valid:
        return None, None
    return f"{context.trace_id:032x}", f"{context.span_id:016x}"


def record_delta_snapshot(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaSnapshotArtifact,
    ctx: SessionContext | None = None,
) -> int | None:
    """Persist a Delta snapshot artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    active_ctx = ctx or profile.session_context()
    location = _ensure_observability_table(
        active_ctx,
        profile,
        name=DELTA_SNAPSHOT_TABLE_NAME,
        schema=_delta_snapshot_schema(),
    )
    if location is None:
        return None
    snapshot = artifact.snapshot
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "delta_version": coerce_int(snapshot.get("version")),
        "snapshot_timestamp": coerce_int(snapshot.get("snapshot_timestamp")),
        "min_reader_version": coerce_int(snapshot.get("min_reader_version")),
        "min_writer_version": coerce_int(snapshot.get("min_writer_version")),
        "reader_features": string_list(snapshot.get("reader_features") or ()),
        "writer_features": string_list(snapshot.get("writer_features") or ()),
        "table_properties": string_map(snapshot.get("table_properties") or {}),
        "schema_msgpack": msgpack_payload(_schema_payload(snapshot.get("schema_json"))),
        "schema_identity_hash": artifact.schema_identity_hash,
        "ddl_fingerprint": artifact.ddl_fingerprint,
        "partition_columns": string_list(snapshot.get("partition_columns") or ()),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=active_ctx,
            runtime_profile=profile,
            location=location,
            schema=_delta_snapshot_schema(),
            payload=payload,
            operation="delta_snapshot_artifact",
            commit_metadata={"table_uri": artifact.table_uri},
        )
    )


def record_delta_mutation(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaMutationArtifact,
    ctx: SessionContext | None = None,
) -> int | None:
    """Persist a Delta mutation artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    if _is_observability_target(
        dataset_name=artifact.dataset_name,
        table_uri=artifact.table_uri,
    ):
        return None
    active_ctx = ctx or profile.session_context()
    location = _ensure_observability_table(
        active_ctx,
        profile,
        name=DELTA_MUTATION_TABLE_NAME,
        schema=_delta_mutation_schema(),
    )
    if location is None:
        return None
    report = artifact.report
    snapshot_payload = _snapshot_payload(report)
    table_properties = _snapshot_table_properties(snapshot_payload)
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "operation": artifact.operation,
        "mode": artifact.mode,
        "delta_version": coerce_int(report.get("version")),
        "min_reader_version": coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features": string_list(snapshot_payload.get("reader_features") or ()),
        "writer_features": string_list(snapshot_payload.get("writer_features") or ()),
        "table_properties": table_properties,
        "constraint_status": artifact.constraint_status,
        "constraint_violations": string_list(artifact.constraint_violations),
        "commit_app_id": artifact.commit_app_id,
        "commit_version": artifact.commit_version,
        "commit_run_id": artifact.commit_run_id,
        "commit_metadata": string_map(dict(artifact.commit_metadata or {})),
        "metrics_msgpack": msgpack_payload(report.get("metrics") or {}),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=active_ctx,
            runtime_profile=profile,
            location=location,
            schema=_delta_mutation_schema(),
            payload=payload,
            operation=f"delta_mutation_{artifact.operation}",
            commit_metadata=artifact.commit_metadata,
        )
    )


def record_delta_feature_state(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaFeatureStateArtifact,
) -> None:
    """Record Delta feature state adoption in diagnostics."""
    if profile is None:
        return
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "delta_version": artifact.delta_version,
        "enabled_features": string_map(dict(artifact.enabled_features)),
        "commit_metadata": string_map(dict(artifact.commit_metadata or {})),
        "commit_app_id": artifact.commit_app_id,
        "commit_version": artifact.commit_version,
        "commit_run_id": artifact.commit_run_id,
    }
    from serde_artifact_specs import DATAFUSION_DELTA_FEATURES_SPEC

    profile.record_artifact(DATAFUSION_DELTA_FEATURES_SPEC, payload)


def record_delta_scan_plan(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaScanPlanArtifact,
) -> int | None:
    """Persist a Delta scan-plan artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = _ensure_observability_table(
        ctx,
        profile,
        name=DELTA_SCAN_PLAN_TABLE_NAME,
        schema=_delta_scan_plan_schema(),
    )
    if location is None:
        return None
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "delta_version": artifact.delta_version,
        "snapshot_timestamp": artifact.snapshot_timestamp,
        "total_files": artifact.total_files,
        "candidate_files": artifact.candidate_files,
        "pruned_files": artifact.pruned_files,
        "pushed_filters": string_list(artifact.pushed_filters),
        "projected_columns": string_list(artifact.projected_columns),
        "delta_protocol_msgpack": msgpack_or_none(artifact.delta_protocol),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
            runtime_profile=profile,
            location=location,
            schema=_delta_scan_plan_schema(),
            payload=payload,
            operation="delta_scan_plan",
            commit_metadata={"dataset_name": artifact.dataset_name},
        )
    )


def record_delta_maintenance(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaMaintenanceArtifact,
) -> int | None:
    """Persist a Delta maintenance artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = _ensure_observability_table(
        ctx,
        profile,
        name=DELTA_MAINTENANCE_TABLE_NAME,
        schema=_delta_maintenance_schema(),
    )
    if location is None:
        return None
    report = artifact.report
    snapshot_payload = _snapshot_payload(report)
    table_properties = _snapshot_table_properties(snapshot_payload)
    log_retention = table_properties.get("delta.logRetentionDuration")
    checkpoint_interval = table_properties.get("delta.checkpointInterval")
    checkpoint_retention = table_properties.get("delta.checkpointRetentionDuration")
    checkpoint_protection = table_properties.get("delta.checkpointProtection")
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "operation": artifact.operation,
        "delta_version": coerce_int(report.get("version")),
        "min_reader_version": coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features": string_list(snapshot_payload.get("reader_features") or ()),
        "writer_features": string_list(snapshot_payload.get("writer_features") or ()),
        "table_properties": table_properties,
        "log_retention_duration": log_retention,
        "checkpoint_interval": checkpoint_interval,
        "checkpoint_retention_duration": checkpoint_retention,
        "checkpoint_protection": checkpoint_protection,
        "retention_hours": artifact.retention_hours,
        "dry_run": artifact.dry_run,
        "metrics_msgpack": msgpack_payload(report.get("metrics") or {}),
        "commit_metadata": string_map(dict(artifact.commit_metadata or {})),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
            runtime_profile=profile,
            location=location,
            schema=_delta_maintenance_schema(),
            payload=payload,
            operation=f"delta_maintenance_{artifact.operation}",
            commit_metadata=artifact.commit_metadata,
        )
    )


def _ensure_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    schema: pa.Schema,
) -> DatasetLocation | None:
    table_path = _observability_root(profile) / name
    delta_log_path = table_path / "_delta_log"
    has_delta_log = delta_log_path.exists() and any(delta_log_path.glob("*.json"))
    from serde_artifact_specs import (
        DELTA_OBSERVABILITY_BOOTSTRAP_COMPLETED_SPEC,
        DELTA_OBSERVABILITY_BOOTSTRAP_FAILED_SPEC,
        DELTA_OBSERVABILITY_BOOTSTRAP_STARTED_SPEC,
        DELTA_OBSERVABILITY_REGISTER_FAILED_SPEC,
    )

    if not has_delta_log:
        profile.record_artifact(
            DELTA_OBSERVABILITY_BOOTSTRAP_STARTED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "operation": "delta_observability_bootstrap",
            },
        )
        try:
            _bootstrap_observability_table(
                ctx,
                profile,
                table_path=table_path,
                schema=schema,
                operation="delta_observability_bootstrap",
            )
        except (
            RuntimeError,
            TypeError,
            ValueError,
            OSError,
            ImportError,
        ) as exc:
            profile.record_artifact(
                DELTA_OBSERVABILITY_BOOTSTRAP_FAILED_SPEC,
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "table": name,
                    "path": str(table_path),
                    "operation": "delta_observability_bootstrap",
                    "error": str(exc),
                },
            )
            return None
        profile.record_artifact(
            DELTA_OBSERVABILITY_BOOTSTRAP_COMPLETED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "operation": "delta_observability_bootstrap",
            },
        )
    if not _ensure_observability_schema(
        ctx,
        profile,
        table_path=table_path,
        schema=schema,
        operation="delta_observability_schema_reset",
    ):
        return None
    location = DatasetLocation(path=str(table_path), format="delta")
    try:
        DataFusionExecutionFacade(
            ctx=ctx,
            runtime_profile=profile,
        ).register_dataset(
            name=name,
            location=location,
            overwrite=True,
        )
    except (
        DataFusionEngineError,
        RuntimeError,
        ValueError,
        TypeError,
        OSError,
        KeyError,
    ) as exc:
        if _register_observability_fallback(
            ctx,
            profile,
            name=name,
            table_path=table_path,
            exc=exc,
        ):
            return location
        profile.record_artifact(
            DELTA_OBSERVABILITY_REGISTER_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(exc),
            },
        )
        return None
    return location


def ensure_delta_observability_tables(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> dict[str, DatasetLocation | None]:
    """Ensure all Delta observability tables exist and are registered.

    Returns:
    -------
    dict[str, DatasetLocation | None]
        Mapping of observability table names to resolved locations.
    """
    specs = (
        (DELTA_SNAPSHOT_TABLE_NAME, _delta_snapshot_schema()),
        (DELTA_MUTATION_TABLE_NAME, _delta_mutation_schema()),
        (DELTA_SCAN_PLAN_TABLE_NAME, _delta_scan_plan_schema()),
        (DELTA_MAINTENANCE_TABLE_NAME, _delta_maintenance_schema()),
    )
    return {
        name: _ensure_observability_table(
            ctx,
            profile,
            name=name,
            schema=schema,
        )
        for name, schema in specs
    }


def _register_observability_fallback(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    table_path: Path,
    exc: Exception,
) -> bool:
    from serde_artifact_specs import (
        DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC,
        DELTA_OBSERVABILITY_REGISTER_FALLBACK_USED_SPEC,
    )

    if not _is_control_plane_registration_error(exc):
        return False
    try:
        dataset = ds.dataset(str(table_path), format="parquet")
    except (RuntimeError, TypeError, ValueError, OSError) as dataset_exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(dataset_exc),
                "cause": str(exc),
                "stage": "dataset",
            },
        )
        return False
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(name):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(name)
    try:
        adapter.register_dataset(name, dataset)
    except (RuntimeError, TypeError, ValueError, OSError, KeyError) as register_exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(register_exc),
                "cause": str(exc),
                "stage": "register",
            },
        )
        return False
    profile.record_artifact(
        DELTA_OBSERVABILITY_REGISTER_FALLBACK_USED_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": name,
            "path": str(table_path),
            "cause": str(exc),
        },
    )
    return True


def _is_control_plane_registration_error(exc: Exception) -> bool:
    if not isinstance(exc, DataFusionEngineError):
        return False
    message = str(exc).lower()
    return "control-plane failed" in message or "degraded python fallback paths" in message


def _ensure_observability_schema(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> bool:
    from serde_artifact_specs import (
        DELTA_OBSERVABILITY_SCHEMA_CHECK_FAILED_SPEC,
        DELTA_OBSERVABILITY_SCHEMA_DRIFT_SPEC,
        DELTA_OBSERVABILITY_SCHEMA_RESET_FAILED_SPEC,
    )

    table_name = table_path.name
    try:
        from arro3.core import Schema as Arro3Schema
        from deltalake import DeltaTable

        current_schema = DeltaTable(str(table_path)).schema().to_arrow()
        expected_schema = Arro3Schema.from_arrow(schema)
    except (
        RuntimeError,
        TypeError,
        ValueError,
        OSError,
        ImportError,
        AttributeError,
    ) as exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_SCHEMA_CHECK_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": table_name,
                "path": str(table_path),
                "operation": operation,
                "error": str(exc),
            },
        )
        return False
    if current_schema.equals(expected_schema):
        return True
    profile.record_artifact(
        DELTA_OBSERVABILITY_SCHEMA_DRIFT_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": table_name,
            "path": str(table_path),
            "operation": operation,
            "expected_fields": list(expected_schema.names),
            "observed_fields": list(current_schema.names),
        },
    )
    try:
        _bootstrap_observability_table(
            ctx,
            profile,
            table_path=table_path,
            schema=schema,
            operation=operation,
        )
    except (
        RuntimeError,
        TypeError,
        ValueError,
        OSError,
        ImportError,
    ) as exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_SCHEMA_RESET_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": table_name,
                "path": str(table_path),
                "operation": operation,
                "error": str(exc),
            },
        )
        return False
    return True


def _bootstrap_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> None:
    _ = (ctx, profile)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_row = _bootstrap_observability_row(schema)
    empty = pa.Table.from_pylist([bootstrap_row], schema=schema)
    from deltalake import CommitProperties
    from deltalake.exceptions import (
        CommitFailedError,
        DeltaError,
        DeltaProtocolError,
        SchemaMismatchError,
        TableNotFoundError,
    )
    from deltalake.writer import write_deltalake

    commit_properties = CommitProperties(
        custom_metadata=_observability_commit_metadata(
            operation,
            {"table": table_path.name},
        ),
    )
    try:
        write_deltalake(
            str(table_path),
            empty,
            mode="overwrite",
            schema_mode="overwrite",
            commit_properties=commit_properties,
        )
    except (
        CommitFailedError,
        DeltaError,
        DeltaProtocolError,
        OSError,
        RuntimeError,
        SchemaMismatchError,
        TableNotFoundError,
        TypeError,
        ValueError,
    ) as exc:
        if not _is_corrupt_delta_log_error(exc):
            raise
        with contextlib.suppress(OSError):
            shutil.rmtree(table_path)
        table_path.parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(
            str(table_path),
            empty,
            mode="overwrite",
            schema_mode="overwrite",
            commit_properties=commit_properties,
        )


def _bootstrap_observability_row(schema: pa.Schema) -> dict[str, object]:
    row: dict[str, object] = {}
    for field in schema:
        if field.nullable:
            row[field.name] = None
            continue
        dtype = field.type
        if pa.types.is_string(dtype):
            row[field.name] = "__bootstrap__"
        elif pa.types.is_integer(dtype):
            row[field.name] = 0
        elif pa.types.is_binary(dtype):
            row[field.name] = b""
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            row[field.name] = []
        elif pa.types.is_map(dtype):
            row[field.name] = {}
        else:
            row[field.name] = None
    return row


def _append_observability_row(request: _AppendObservabilityRequest) -> int | None:
    table = pa.Table.from_pylist([dict(request.payload)], schema=request.schema)
    pipeline = WritePipeline(ctx=request.ctx, runtime_profile=request.runtime_profile)
    commit_metadata = _observability_commit_metadata(
        request.operation,
        request.commit_metadata,
    )
    try:
        append_name = f"{request.location.path}_append_{time.time_ns()}"
        df = datafusion_from_arrow(request.ctx, name=append_name, value=table)
        result = pipeline.write(
            WriteRequest(
                source=df,
                destination=str(request.location.path),
                format=WriteFormat.DELTA,
                mode=WriteMode.APPEND,
                format_options={"commit_metadata": commit_metadata},
            )
        )
    except _OBSERVABILITY_APPEND_ERRORS as exc:  # pragma: no cover - defensive fail-open path
        if (
            request.runtime_profile is not None
            and _is_schema_mismatch_error(exc)
            and _ensure_observability_schema(
                request.ctx,
                request.runtime_profile,
                table_path=Path(str(request.location.path)),
                schema=request.schema,
                operation=f"{request.operation}_schema_repair",
            )
        ):
            retry_name = f"{request.location.path}_append_retry_{time.time_ns()}"
            retry_df = datafusion_from_arrow(request.ctx, name=retry_name, value=table)
            try:
                result = pipeline.write(
                    WriteRequest(
                        source=retry_df,
                        destination=str(request.location.path),
                        format=WriteFormat.DELTA,
                        mode=WriteMode.APPEND,
                        format_options={"commit_metadata": commit_metadata},
                    )
                )
            except (
                _OBSERVABILITY_APPEND_ERRORS
            ) as retry_exc:  # pragma: no cover - defensive fail-open path
                _record_append_failure(request, retry_exc)
                return None
        else:
            _record_append_failure(request, exc)
            return None
    if result.delta_result is None:
        return None
    return result.delta_result.version


def _is_schema_mismatch_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "schemamismatcherror",
            "schema mismatch",
            "cannot cast schema",
            "number of fields does not match",
        )
    )


def _is_corrupt_delta_log_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "invalid json in log record" in message or "duplicate field `operation`" in message


def _observability_commit_metadata(
    operation: str,
    metadata: Mapping[str, str] | None,
) -> dict[str, str]:
    sanitized: dict[str, str] = {
        str(key): str(value)
        for key, value in (metadata or {}).items()
        if str(key).lower() != "operation"
    }
    sanitized["observability_operation"] = operation
    return sanitized


def _is_observability_target(
    *,
    dataset_name: str | None,
    table_uri: str | None,
) -> bool:
    names = {
        DELTA_SNAPSHOT_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_MAINTENANCE_TABLE_NAME,
    }
    if dataset_name in names:
        return True
    if table_uri is None:
        return False
    return Path(table_uri).name in names


def _record_append_failure(request: _AppendObservabilityRequest, exc: Exception) -> None:
    if request.runtime_profile is None:
        return
    from serde_artifact_specs import DELTA_OBSERVABILITY_APPEND_FAILED_SPEC

    request.runtime_profile.record_artifact(
        DELTA_OBSERVABILITY_APPEND_FAILED_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": request.location.path,
            "operation": request.operation,
            "error": str(exc),
        },
    )


def _observability_root(profile: DataFusionRuntimeProfile) -> Path:
    root_value = profile.policies.plan_artifacts_root
    root = Path(root_value) if root_value else _DEFAULT_OBSERVABILITY_ROOT
    return root / "delta_observability"


def _delta_snapshot_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            int64_field("delta_version"),
            int64_field("snapshot_timestamp"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            binary_field("schema_msgpack", nullable=False),
            string_field("schema_identity_hash"),
            string_field("ddl_fingerprint"),
            list_field("partition_columns", pa.string()),
        ]
    )


def _delta_mutation_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            string_field("operation", nullable=False),
            string_field("mode"),
            int64_field("delta_version"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            string_field("constraint_status"),
            list_field("constraint_violations", pa.string()),
            string_field("commit_app_id"),
            int64_field("commit_version"),
            string_field("commit_run_id"),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
            binary_field("metrics_msgpack", nullable=False),
        ]
    )


def _delta_scan_plan_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name", nullable=False),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            int64_field("delta_version"),
            int64_field("snapshot_timestamp"),
            int64_field("total_files", nullable=False),
            int64_field("candidate_files", nullable=False),
            int64_field("pruned_files", nullable=False),
            list_field("pushed_filters", pa.string()),
            list_field("projected_columns", pa.string()),
            binary_field("delta_protocol_msgpack"),
        ]
    )


def _delta_maintenance_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            string_field("operation", nullable=False),
            int64_field("delta_version"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            string_field("log_retention_duration"),
            string_field("checkpoint_interval"),
            string_field("checkpoint_retention_duration"),
            string_field("checkpoint_protection"),
            int64_field("retention_hours"),
            bool_field("dry_run"),
            binary_field("metrics_msgpack", nullable=False),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
        ]
    )


def _snapshot_payload(report: Mapping[str, object]) -> dict[str, object]:
    snapshot = report.get("snapshot") if isinstance(report, Mapping) else None
    if isinstance(snapshot, Mapping):
        return {str(key): value for key, value in dict(snapshot).items()}
    metrics = report.get("metrics") if isinstance(report, Mapping) else None
    if isinstance(metrics, Mapping):
        nested = metrics.get("snapshot")
        if isinstance(nested, Mapping):
            return {str(key): value for key, value in dict(nested).items()}
    return {}


def _snapshot_table_properties(snapshot_payload: Mapping[str, object]) -> dict[str, str]:
    properties = snapshot_payload.get("table_properties")
    if not isinstance(properties, Mapping):
        return {}
    return {str(key): str(value) for key, value in dict(properties).items()}


def _schema_payload(value: object) -> object:
    empty: dict[str, object] = {}
    if value is None:
        return empty
    if isinstance(value, str):
        if not value:
            return empty
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
    return value


__all__ = [
    "DELTA_MAINTENANCE_TABLE_NAME",
    "DELTA_MUTATION_TABLE_NAME",
    "DELTA_SCAN_PLAN_TABLE_NAME",
    "DELTA_SNAPSHOT_TABLE_NAME",
    "DeltaFeatureStateArtifact",
    "DeltaMaintenanceArtifact",
    "DeltaMutationArtifact",
    "DeltaScanPlanArtifact",
    "DeltaSnapshotArtifact",
    "ensure_delta_observability_tables",
    "record_delta_feature_state",
    "record_delta_maintenance",
    "record_delta_mutation",
    "record_delta_scan_plan",
    "record_delta_snapshot",
]
