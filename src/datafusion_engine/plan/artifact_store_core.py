"""Delta-backed plan artifact persistence for deterministic observability."""
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa

from datafusion_engine.dataset.registry import (
    DatasetLocation,
)
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.plan.artifact_serialization import (
    delta_inputs_payload as _delta_inputs_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    event_payload_msgpack as _event_payload_msgpack,
)
from datafusion_engine.plan.artifact_serialization import (
    event_time_unix_ms as _event_time_unix_ms,
)
from datafusion_engine.plan.artifact_serialization import (
    lineage_payload as _lineage_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    msgpack_or_none as _msgpack_or_none,
)
from datafusion_engine.plan.artifact_serialization import (
    msgpack_payload as _msgpack_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    msgpack_payload_raw as _msgpack_payload_raw,
)
from datafusion_engine.plan.artifact_serialization import (
    normalized_event_payload as _normalized_event_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    plan_details_payload as _plan_details_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    plan_identity_payload as _plan_identity_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    proto_msgpack as _proto_msgpack,
)
from datafusion_engine.plan.artifact_serialization import (
    scan_units_payload as _scan_units_payload,
)
from datafusion_engine.plan.artifact_serialization import (
    substrait_msgpack as _substrait_msgpack,
)
from datafusion_engine.plan.artifact_serialization import (
    udf_compatibility as _udf_compatibility,
)
from datafusion_engine.plan.perf_policy import PlanBundleComparisonPolicy
from datafusion_engine.plan.signals import extract_plan_signals, plan_signals_payload
from serde_artifacts import DeltaStatsDecision, PlanArtifactRow, WriteArtifactRow
from serde_msgspec import (
    StructBaseCompat,
    decode_json_lines,
)
from utils.hashing import hash_json_default, hash_sha256_hex

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.lineage.reporting import LineageReport
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v10"
WRITE_ARTIFACTS_TABLE_NAME = "datafusion_write_artifacts_v2"
PIPELINE_EVENTS_TABLE_NAME = "datafusion_pipeline_events_v2"
_ARTIFACTS_DIRNAME = PLAN_ARTIFACTS_TABLE_NAME
_WRITE_ARTIFACTS_DIRNAME = WRITE_ARTIFACTS_TABLE_NAME
_PIPELINE_EVENTS_DIRNAME = PIPELINE_EVENTS_TABLE_NAME
_LOCAL_ARTIFACTS_DIRNAME = "artifacts"
_PLAN_EVENT_KIND = "plan"
_EXECUTION_EVENT_KIND = "execution"
_WRITE_EVENT_KIND = "write"
try:
    _DEFAULT_ARTIFACTS_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_ARTIFACTS_ROOT = Path.cwd() / ".artifacts"


@dataclass(frozen=True)
class DeterminismValidationResult:
    """Result of plan determinism validation against artifact store."""

    is_deterministic: bool
    plan_fingerprint: str
    view_name: str | None
    matching_artifact_count: int
    conflicting_fingerprints: tuple[str, ...]
    plan_identity_hashes: tuple[str, ...] = ()
    conflicting_plan_identity_hashes: tuple[str, ...] = ()
    validation_error: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for determinism validation.

        Returns:
        -------
        dict[str, object]
            Serializable diagnostics payload.
        """
        return {
            "is_deterministic": self.is_deterministic,
            "plan_fingerprint": self.plan_fingerprint,
            "view_name": self.view_name,
            "matching_artifact_count": self.matching_artifact_count,
            "conflicting_fingerprints": list(self.conflicting_fingerprints),
            "plan_identity_hashes": list(self.plan_identity_hashes),
            "conflicting_plan_identity_hashes": list(self.conflicting_plan_identity_hashes),
            "validation_error": self.validation_error,
        }


class _DeterminismRow(StructBaseCompat, frozen=True):
    """Typed determinism query row for plan artifacts."""

    plan_fingerprint: str | None = None
    plan_identity_hash: str | None = None


@dataclass(frozen=True)
class PipelineEventRow:
    """Serializable pipeline event row persisted to the Delta store."""

    event_time_unix_ms: int
    profile_name: str | None
    run_id: str
    event_name: str
    plan_signature: str
    reduced_plan_signature: str
    event_payload_msgpack: bytes
    event_payload_hash: str

    def to_row(self) -> dict[str, object]:
        """Return a row mapping ready for Arrow/Delta ingestion.

        Returns:
        -------
        dict[str, object]
            Row payload for ingestion.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms,
            "profile_name": self.profile_name,
            "run_id": self.run_id,
            "event_name": self.event_name,
            "plan_signature": self.plan_signature,
            "reduced_plan_signature": self.reduced_plan_signature,
            "event_payload_msgpack": self.event_payload_msgpack,
            "event_payload_hash": self.event_payload_hash,
        }


class PipelineEventLine(StructBaseCompat, frozen=True):
    """Typed line entry for NDJSON pipeline event ingestion."""

    event_name: str
    payload: object


def ensure_plan_artifacts_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure the plan artifacts table exists and is registered.

    Returns:
    -------
    DatasetLocation | None
        Location for the plan artifacts table when enabled.
    """
    from datafusion_engine.plan.artifact_store_cache import (
        ensure_plan_artifacts_table as _ensure_plan_artifacts_table,
    )

    return _ensure_plan_artifacts_table(ctx, profile)


def ensure_pipeline_events_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure the pipeline events table exists and is registered.

    Returns:
    -------
    DatasetLocation | None
        Location for the pipeline events table when enabled.
    """
    from datafusion_engine.plan.artifact_store_cache import (
        ensure_pipeline_events_table as _ensure_pipeline_events_table,
    )

    return _ensure_pipeline_events_table(ctx, profile)


@dataclass(frozen=True)
class PlanArtifactsForViewsRequest:
    """Inputs for persisting plan artifacts for view nodes."""

    view_nodes: Sequence[ViewNode]
    scan_units: Sequence[ScanUnit] = ()
    scan_keys_by_view: Mapping[str, Sequence[str]] | None = None
    lineage_by_view: Mapping[str, LineageReport] | None = None


@dataclass(frozen=True)
class PipelineEventsRequest:
    """Inputs for persisting pipeline lifecycle events."""

    run_id: str
    plan_signature: str
    reduced_plan_signature: str
    events_snapshot: Mapping[str, Sequence[object]] = field(default_factory=dict)
    event_names: Sequence[str] | None = None
    events_ndjson: bytes | None = None


def _events_snapshot_as_lists(
    snapshot: Mapping[str, Sequence[object]],
) -> dict[str, list[object]]:
    """Convert event snapshot mappings into list-backed payloads.

    Returns:
    -------
    dict[str, list[object]]
        Normalized snapshot entries with mutable payload lists.
    """
    normalized: dict[str, list[object]] = {}
    for event_name, payloads in snapshot.items():
        normalized[event_name] = list(payloads)
    return normalized


def _comparison_policy_for_profile(
    profile: DataFusionRuntimeProfile,
) -> PlanBundleComparisonPolicy:
    performance_policy = getattr(profile.policies, "performance_policy", None)
    if performance_policy is None:
        return PlanBundleComparisonPolicy()
    return performance_policy.comparison


def _apply_plan_artifact_retention(
    row: PlanArtifactRow,
    *,
    comparison_policy: PlanBundleComparisonPolicy,
) -> PlanArtifactRow:
    retained = row
    if not comparison_policy.retain_p2_artifacts:
        retained = msgspec.structs.replace(
            retained,
            substrait_msgpack=b"",
            logical_plan_proto_msgpack=None,
            optimized_plan_proto_msgpack=None,
            execution_plan_proto_msgpack=None,
            substrait_validation_msgpack=None,
        )
    if not comparison_policy.retain_p1_artifacts:
        retained = msgspec.structs.replace(
            retained,
            explain_tree_rows_msgpack=None,
            explain_verbose_rows_msgpack=None,
            explain_analyze_duration_ms=None,
            explain_analyze_output_rows=None,
            lineage_msgpack=_msgpack_payload({}),
            scan_units_msgpack=_msgpack_payload(()),
            plan_details_msgpack=_msgpack_payload({"retained": False}),
            plan_signals_msgpack=_msgpack_payload({"retained": False}),
            udf_snapshot_msgpack=_msgpack_payload({}),
            udf_planner_snapshot_msgpack=None,
            udf_compatibility_detail_msgpack=_msgpack_payload({"retained": False}),
        )
    return retained


from datafusion_engine.plan.artifact_store_query import (
    persist_plan_artifacts_for_views,
    validate_plan_determinism,
)


@dataclass(frozen=True)
class PlanArtifactBuildRequest:
    """Inputs for building plan artifact rows."""

    view_name: str
    bundle: DataFusionPlanArtifact
    lineage: LineageReport | None = None
    scan_units: Sequence[ScanUnit] = ()
    scan_keys: Sequence[str] = ()
    event_kind: str = _PLAN_EVENT_KIND
    execution_duration_ms: float | None = None
    execution_status: str | None = None
    execution_error: str | None = None


def persist_execution_artifact(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactBuildRequest,
) -> PlanArtifactRow | None:
    """Persist an execution artifact row for a plan bundle.

    Returns:
    -------
    PlanArtifactRow | None
        Persisted plan artifact row when storage is enabled.
    """
    location = ensure_plan_artifacts_table(ctx, profile)
    if location is None:
        return None
    comparison_policy = _comparison_policy_for_profile(profile)
    if not comparison_policy.retain_p0_artifacts:
        return None
    row = build_plan_artifact_row(
        ctx,
        profile,
        request=replace(request, event_kind=_EXECUTION_EVENT_KIND),
    )
    row = _apply_plan_artifact_retention(
        row,
        comparison_policy=comparison_policy,
    )
    persisted = persist_plan_artifact_rows(ctx, profile, rows=(row,), location=location)
    return persisted[0] if persisted else None


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

    Parameters
    ----------
    profile
        Runtime profile for artifact storage configuration.
    request
        Write artifact request payload.

    Returns:
    -------
    WriteArtifactRow | None
        Persisted write artifact row, or None if persistence is disabled.
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
    """Write an artifact table via the unified write pipeline.

    Args:
        ctx: DataFusion session context.
        profile: Active runtime profile.
        request: Artifact table write request payload.

    Returns:
        int | None: Result.

    Raises:
        ValueError: If the artifact write mode is unsupported.
    """
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


def persist_plan_artifact_rows(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PlanArtifactRow],
    location: DatasetLocation | None = None,
) -> tuple[PlanArtifactRow, ...]:
    """Persist plan artifact rows to the Delta-backed artifact store.

    Args:
        ctx: DataFusion session context.
        profile: Active runtime profile.
        rows: Plan artifact rows to persist.
        location: Optional pre-resolved artifact location.

    Returns:
        tuple[PlanArtifactRow, ...]: Result.

    Raises:
        RuntimeError: If the resulting Delta version cannot be resolved.
    """
    if not rows:
        return ()
    resolved_location = location or ensure_plan_artifacts_table(ctx, profile)
    if resolved_location is None:
        return ()
    table_path = Path(resolved_location.path)
    arrow_table = pa.Table.from_pylist(
        [row.to_row() for row in rows],
        schema=_plan_artifacts_schema(),
    )
    commit_metadata = _commit_metadata_for_rows(rows)
    final_version = _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=arrow_table,
            commit_metadata=commit_metadata,
            mode="append",
            schema_mode="merge",
            operation_id="plan_artifacts_store",
        ),
    )
    if final_version is None:
        msg = f"Failed to resolve Delta version for plan artifacts: {table_path}."
        raise RuntimeError(msg)
    _refresh_plan_artifacts_registration(ctx, profile, resolved_location)
    _record_plan_artifact_summary(profile, rows=rows, path=str(table_path), version=final_version)
    return tuple(rows)


def persist_pipeline_events(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PipelineEventsRequest,
) -> tuple[PipelineEventRow, ...]:
    """Persist pipeline lifecycle events to the Delta-backed artifact store.

    Returns:
    -------
    tuple[PipelineEventRow, ...]
        Persisted pipeline event rows.
    """
    location = ensure_pipeline_events_table(ctx, profile)
    if location is None:
        return ()
    events_snapshot = _events_snapshot_as_lists(request.events_snapshot)
    if request.events_ndjson is not None:
        decoded = decode_json_lines(request.events_ndjson, target_type=PipelineEventLine)
        for line in decoded:
            events_snapshot.setdefault(line.event_name, []).append(line.payload)
    names = (
        tuple(request.event_names)
        if request.event_names is not None
        else tuple(sorted(events_snapshot))
    )
    rows: list[PipelineEventRow] = []
    for event_name in names:
        rows_for_event = events_snapshot.get(event_name, ())
        if not rows_for_event:
            continue
        for payload in rows_for_event:
            normalized = _normalized_event_payload(payload)
            event_time_unix_ms = _event_time_unix_ms(normalized)
            payload_msgpack = _event_payload_msgpack(normalized)
            payload_hash = hash_sha256_hex(payload_msgpack)
            rows.append(
                PipelineEventRow(
                    event_time_unix_ms=event_time_unix_ms,
                    profile_name=_profile_name(profile),
                    run_id=request.run_id,
                    event_name=event_name,
                    plan_signature=request.plan_signature,
                    reduced_plan_signature=request.reduced_plan_signature,
                    event_payload_msgpack=payload_msgpack,
                    event_payload_hash=payload_hash,
                )
            )
    if not rows:
        return ()
    return persist_pipeline_event_rows(ctx, profile, rows=rows, location=location)


def persist_pipeline_event_rows(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PipelineEventRow],
    location: DatasetLocation | None = None,
) -> tuple[PipelineEventRow, ...]:
    """Persist pipeline event rows to the Delta-backed artifact store.

    Args:
        ctx: DataFusion session context.
        profile: Active runtime profile.
        rows: Pipeline event rows to persist.
        location: Optional pre-resolved artifact location.

    Returns:
        tuple[PipelineEventRow, ...]: Result.

    Raises:
        RuntimeError: If the resulting Delta version cannot be resolved.
    """
    if not rows:
        return ()
    resolved_location = location or ensure_pipeline_events_table(ctx, profile)
    if resolved_location is None:
        return ()
    table_path = Path(resolved_location.path)
    arrow_table = pa.Table.from_pylist(
        [row.to_row() for row in rows],
        schema=_pipeline_events_schema(),
    )
    commit_metadata = _commit_metadata_for_pipeline_events(rows)
    final_version = _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=arrow_table,
            commit_metadata=commit_metadata,
            mode="append",
            schema_mode="merge",
            operation_id="pipeline_events_store",
        ),
    )
    if final_version is None:
        msg = f"Failed to resolve Delta version for pipeline events: {table_path}."
        raise RuntimeError(msg)
    _refresh_pipeline_events_registration(ctx, profile, resolved_location)
    _record_pipeline_events_summary(profile, rows=rows, path=str(table_path), version=final_version)
    return tuple(rows)


def build_plan_artifact_row(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactBuildRequest,
) -> PlanArtifactRow:
    """Build a plan artifact row from a plan bundle and optional lineage.

    Returns:
        PlanArtifactRow: Serialized plan artifact row.

    Raises:
        ValueError: If lineage is unavailable from both the request and extracted
        plan signals.
    """
    signals = extract_plan_signals(request.bundle, scan_units=request.scan_units)
    resolved_lineage = request.lineage or signals.lineage
    if resolved_lineage is None:
        msg = (
            "Lineage is required to persist plan artifacts but was unavailable for "
            f"view {request.view_name!r}."
        )
        raise ValueError(msg)
    scan_payload = _scan_units_payload(request.scan_units, scan_keys=request.scan_keys)
    scan_keys_payload = tuple(sorted(set(request.scan_keys)))
    delta_inputs_payload = _delta_inputs_payload(request.bundle)
    plan_identity_payload = _plan_identity_payload(
        bundle=request.bundle,
        profile=profile,
        delta_inputs_payload=delta_inputs_payload,
        scan_payload=scan_payload,
        scan_keys_payload=scan_keys_payload,
    )
    plan_identity_hash = hash_json_default(plan_identity_payload)
    udf_ok, udf_detail = _udf_compatibility(ctx, request.bundle)
    plan_details_payload = _plan_details_payload(
        request.bundle,
        delta_inputs_payload=delta_inputs_payload,
        scan_payload=scan_payload,
        plan_identity_hash=plan_identity_hash,
    )
    signals_payload = plan_signals_payload(signals)
    return PlanArtifactRow(
        event_time_unix_ms=int(time.time() * 1000),
        profile_name=_profile_name(profile),
        event_kind=request.event_kind,
        view_name=request.view_name,
        plan_fingerprint=request.bundle.plan_fingerprint,
        plan_identity_hash=plan_identity_hash,
        udf_snapshot_hash=request.bundle.artifacts.udf_snapshot_hash,
        function_registry_hash=request.bundle.artifacts.function_registry_hash,
        required_udfs=tuple(request.bundle.required_udfs),
        required_rewrite_tags=tuple(request.bundle.required_rewrite_tags),
        domain_planner_names=tuple(request.bundle.artifacts.domain_planner_names),
        delta_inputs_msgpack=_msgpack_payload(delta_inputs_payload),
        df_settings=dict(request.bundle.artifacts.df_settings),
        planning_env_msgpack=_msgpack_payload(request.bundle.artifacts.planning_env_snapshot),
        planning_env_hash=request.bundle.artifacts.planning_env_hash,
        rulepack_msgpack=_msgpack_or_none(request.bundle.artifacts.rulepack_snapshot),
        rulepack_hash=request.bundle.artifacts.rulepack_hash,
        information_schema_msgpack=_msgpack_payload(
            request.bundle.artifacts.information_schema_snapshot
        ),
        information_schema_hash=request.bundle.artifacts.information_schema_hash,
        substrait_msgpack=_substrait_msgpack(request.bundle.substrait_bytes),
        logical_plan_proto_msgpack=_proto_msgpack(request.bundle.artifacts.logical_plan_proto),
        optimized_plan_proto_msgpack=_proto_msgpack(request.bundle.artifacts.optimized_plan_proto),
        execution_plan_proto_msgpack=_proto_msgpack(request.bundle.artifacts.execution_plan_proto),
        explain_tree_rows_msgpack=_msgpack_or_none(request.bundle.artifacts.explain_tree_rows),
        explain_verbose_rows_msgpack=_msgpack_or_none(
            request.bundle.artifacts.explain_verbose_rows
        ),
        explain_analyze_duration_ms=request.bundle.artifacts.explain_analyze_duration_ms,
        explain_analyze_output_rows=request.bundle.artifacts.explain_analyze_output_rows,
        substrait_validation_msgpack=_msgpack_or_none(
            request.bundle.artifacts.substrait_validation
        ),
        lineage_msgpack=_msgpack_payload(_lineage_payload(resolved_lineage)),
        scan_units_msgpack=_msgpack_payload(scan_payload),
        scan_keys=scan_keys_payload,
        plan_details_msgpack=_msgpack_payload(plan_details_payload),
        plan_signals_msgpack=_msgpack_payload(signals_payload),
        udf_snapshot_msgpack=_msgpack_payload(request.bundle.artifacts.udf_snapshot),
        udf_planner_snapshot_msgpack=_msgpack_or_none(
            request.bundle.artifacts.udf_planner_snapshot
        ),
        udf_compatibility_ok=udf_ok,
        udf_compatibility_detail_msgpack=_msgpack_payload(udf_detail),
        execution_duration_ms=request.execution_duration_ms,
        execution_status=request.execution_status,
        execution_error=request.execution_error,
    )


from datafusion_engine.plan.artifact_store_tables import (
    _bootstrap_pipeline_events_table,
    _bootstrap_plan_artifacts_table,
    _commit_metadata_for_pipeline_events,
    _commit_metadata_for_rows,
    _delta_schema_available,
    _pipeline_events_location,
    _pipeline_events_schema,
    _plan_artifacts_location,
    _plan_artifacts_schema,
    _profile_name,
    _record_pipeline_events_summary,
    _record_plan_artifact_summary,
    _refresh_pipeline_events_registration,
    _refresh_plan_artifacts_registration,
    _reset_artifacts_table_path,
)

__all__ = [
    "PIPELINE_EVENTS_TABLE_NAME",
    "PLAN_ARTIFACTS_TABLE_NAME",
    "WRITE_ARTIFACTS_TABLE_NAME",
    "DeterminismValidationResult",
    "PipelineEventRow",
    "PipelineEventsRequest",
    "PlanArtifactRow",
    "PlanArtifactsForViewsRequest",
    "WriteArtifactRow",
    "_bootstrap_pipeline_events_table",
    "_bootstrap_plan_artifacts_table",
    "_delta_schema_available",
    "_pipeline_events_location",
    "_plan_artifacts_location",
    "_reset_artifacts_table_path",
    "build_plan_artifact_row",
    "ensure_pipeline_events_table",
    "ensure_plan_artifacts_table",
    "persist_execution_artifact",
    "persist_pipeline_event_rows",
    "persist_pipeline_events",
    "persist_plan_artifact_rows",
    "persist_plan_artifacts_for_views",
    "persist_write_artifact",
    "validate_plan_determinism",
]
