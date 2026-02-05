"""Delta-backed plan artifact persistence for deterministic observability."""

from __future__ import annotations

import contextlib
import shutil
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa

from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
)
from datafusion_engine.delta.scan_config import resolve_delta_scan_options
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.sql.options import planning_sql_options
from datafusion_engine.views.graph import extract_lineage_from_bundle
from schema_spec.system import dataset_spec_from_schema
from serde_artifacts import DeltaStatsDecision, PlanArtifactRow, WriteArtifactRow
from serde_msgspec import (
    StructBaseCompat,
    convert,
    decode_json_lines,
    dumps_msgpack,
    ensure_raw,
    to_builtins,
    validation_error_payload,
)
from serde_msgspec_ext import SubstraitBytes
from storage.deltalake import DeltaSchemaRequest
from utils.hashing import hash_json_default, hash_sha256_hex

if TYPE_CHECKING:
    from datafusion import SessionContext, SQLOptions

    from datafusion_engine.lineage.datafusion import LineageReport
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v9"
WRITE_ARTIFACTS_TABLE_NAME = "datafusion_write_artifacts_v2"
HAMILTON_EVENTS_TABLE_NAME = "datafusion_hamilton_events_v2"
_ARTIFACTS_DIRNAME = PLAN_ARTIFACTS_TABLE_NAME
_WRITE_ARTIFACTS_DIRNAME = WRITE_ARTIFACTS_TABLE_NAME
_HAMILTON_EVENTS_DIRNAME = HAMILTON_EVENTS_TABLE_NAME
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

        Returns
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
class HamiltonEventRow:
    """Serializable Hamilton event row persisted to the Delta store."""

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

        Returns
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


class HamiltonEventLine(StructBaseCompat, frozen=True):
    """Typed line entry for NDJSON Hamilton event ingestion."""

    event_name: str
    payload: object


def ensure_plan_artifacts_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure the plan artifacts table exists and is registered.

    Returns
    -------
    DatasetLocation | None
        Location for the plan artifacts table when enabled.
    """
    location = _plan_artifacts_location(profile)
    if location is None:
        return None
    table_path = Path(location.path)
    existing_version = profile.delta_service().table_version(path=str(table_path))
    if existing_version is None:
        if table_path.exists():
            _reset_artifacts_table_path(
                profile,
                table_path,
                table_name=PLAN_ARTIFACTS_TABLE_NAME,
                reason="delta_table_version_unavailable",
            )
        _bootstrap_plan_artifacts_table(ctx, profile, table_path)
    elif not _delta_schema_available(location, profile=profile):
        _reset_artifacts_table_path(
            profile,
            table_path,
            table_name=PLAN_ARTIFACTS_TABLE_NAME,
            reason="delta_schema_unavailable",
        )
        _bootstrap_plan_artifacts_table(ctx, profile, table_path)
    _refresh_plan_artifacts_registration(ctx, profile, location)
    return location


def ensure_hamilton_events_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure the Hamilton events table exists and is registered.

    Returns
    -------
    DatasetLocation | None
        Location for the Hamilton events table when enabled.
    """
    location = _hamilton_events_location(profile)
    if location is None:
        return None
    table_path = Path(location.path)
    existing_version = profile.delta_service().table_version(path=str(table_path))
    if existing_version is None:
        if table_path.exists():
            _reset_artifacts_table_path(
                profile,
                table_path,
                table_name=HAMILTON_EVENTS_TABLE_NAME,
                reason="delta_table_version_unavailable",
            )
        _bootstrap_hamilton_events_table(ctx, profile, table_path)
    elif not _delta_schema_available(location, profile=profile):
        _reset_artifacts_table_path(
            profile,
            table_path,
            table_name=HAMILTON_EVENTS_TABLE_NAME,
            reason="delta_schema_unavailable",
        )
        _bootstrap_hamilton_events_table(ctx, profile, table_path)
    _refresh_hamilton_events_registration(ctx, profile, location)
    return location


@dataclass(frozen=True)
class PlanArtifactsForViewsRequest:
    """Inputs for persisting plan artifacts for view nodes."""

    view_nodes: Sequence[ViewNode]
    scan_units: Sequence[ScanUnit] = ()
    scan_keys_by_view: Mapping[str, Sequence[str]] | None = None
    lineage_by_view: Mapping[str, LineageReport] | None = None


@dataclass(frozen=True)
class HamiltonEventsRequest:
    """Inputs for persisting Hamilton lifecycle events."""

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

    Returns
    -------
    dict[str, list[object]]
        Normalized snapshot entries with mutable payload lists.
    """
    normalized: dict[str, list[object]] = {}
    for event_name, payloads in snapshot.items():
        normalized[event_name] = list(payloads)
    return normalized


def persist_plan_artifacts_for_views(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactsForViewsRequest,
) -> tuple[PlanArtifactRow, ...]:
    """Persist plan artifacts for view nodes with plan bundles.

    Parameters
    ----------
    ctx
        DataFusion session context used for artifact persistence.
    profile
        Runtime profile for artifact storage configuration.
    request
        Plan artifact persistence request payload.

    Returns
    -------
    tuple[PlanArtifactRow, ...]
        Persisted plan artifact rows.
    """
    location = ensure_plan_artifacts_table(ctx, profile)
    if location is None:
        return ()
    rows: list[PlanArtifactRow] = []
    for node in request.view_nodes:
        bundle = node.plan_bundle
        if bundle is None:
            continue
        scan_keys = (
            tuple(request.scan_keys_by_view.get(node.name, ())) if request.scan_keys_by_view else ()
        )
        lineage = request.lineage_by_view.get(node.name) if request.lineage_by_view else None
        rows.append(
            build_plan_artifact_row(
                ctx,
                profile,
                request=PlanArtifactBuildRequest(
                    view_name=node.name,
                    bundle=bundle,
                    lineage=lineage,
                    scan_units=request.scan_units,
                    scan_keys=scan_keys,
                ),
            )
        )
    if not rows:
        return ()
    return persist_plan_artifact_rows(ctx, profile, rows=rows, location=location)


def validate_plan_determinism(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    plan_fingerprint: str,
    view_name: str | None = None,
) -> DeterminismValidationResult:
    """Validate that a plan fingerprint is deterministic via artifact queries.

    Queries the plan artifact table to check if the same logical plan
    consistently produces the same fingerprint across executions.

    Parameters
    ----------
    ctx
        DataFusion SessionContext for artifact queries.
    profile
        Runtime profile for artifact table configuration.
    plan_fingerprint
        Plan fingerprint to validate.
    view_name
        Optional view name to scope the validation query.

    Returns
    -------
    DeterminismValidationResult
        Result indicating whether the plan is deterministic.
    """
    location = _plan_artifacts_location(profile)
    if location is None:
        return DeterminismValidationResult(
            is_deterministic=True,
            plan_fingerprint=plan_fingerprint,
            view_name=view_name,
            matching_artifact_count=0,
            conflicting_fingerprints=(),
            validation_error="artifact_store_disabled",
        )
    table_path = str(location.path)
    results, identity_error = _collect_determinism_results(
        ctx,
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        sql_options=planning_sql_options(profile),
    )
    if results is None:
        return DeterminismValidationResult(
            is_deterministic=True,
            plan_fingerprint=plan_fingerprint,
            view_name=view_name,
            matching_artifact_count=0,
            conflicting_fingerprints=(),
            validation_error=identity_error,
        )
    row_count, fingerprints, identities = _determinism_sets(results)
    is_deterministic, plan_identity_hashes, conflicting_identities = _determinism_outcome(
        plan_fingerprint=plan_fingerprint,
        fingerprints=fingerprints,
        identities=identities,
    )
    conflicting = tuple(sorted(fp for fp in fingerprints if fp != plan_fingerprint))
    return DeterminismValidationResult(
        is_deterministic=is_deterministic,
        plan_fingerprint=plan_fingerprint,
        view_name=view_name,
        matching_artifact_count=row_count,
        conflicting_fingerprints=conflicting,
        plan_identity_hashes=plan_identity_hashes,
        conflicting_plan_identity_hashes=conflicting_identities,
        validation_error=identity_error,
    )


def _collect_determinism_results(
    ctx: SessionContext,
    table_path: str,
    *,
    view_name: str | None,
    plan_fingerprint: str,
    sql_options: SQLOptions,
) -> tuple[list[pa.RecordBatch] | None, str | None]:
    identity_error: str | None = None
    identity_query = _determinism_validation_query(
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        include_identity=True,
    )
    try:
        return ctx.sql_with_options(identity_query, sql_options).collect(), None
    except (RuntimeError, ValueError, TypeError) as exc:
        identity_error = str(exc)
    fallback_query = _determinism_validation_query(
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        include_identity=False,
    )
    try:
        return ctx.sql_with_options(fallback_query, sql_options).collect(), identity_error
    except (RuntimeError, ValueError, TypeError) as fallback_exc:
        error = identity_error or str(fallback_exc)
        return None, error


def _determinism_sets(
    results: Sequence[pa.RecordBatch],
) -> tuple[int, set[str], set[str]]:
    fingerprints: set[str] = set()
    identities: set[str] = set()
    row_count = 0
    for batch in results:
        for row in batch.to_pylist():
            row_count += 1
            try:
                payload = convert(row, target_type=_DeterminismRow, strict=True)
            except msgspec.ValidationError as exc:
                details = validation_error_payload(exc)
                msg = f"Determinism row validation failed: {details}"
                raise ValueError(msg) from exc
            if payload.plan_fingerprint is not None:
                fingerprints.add(str(payload.plan_fingerprint))
            if payload.plan_identity_hash is not None:
                identities.add(str(payload.plan_identity_hash))
    return row_count, fingerprints, identities


def _determinism_outcome(
    *,
    plan_fingerprint: str,
    fingerprints: set[str],
    identities: set[str],
) -> tuple[bool, tuple[str, ...], tuple[str, ...]]:
    plan_identity_hashes = tuple(sorted(identities))
    if plan_identity_hashes:
        baseline_identity = plan_identity_hashes[0]
        conflicting_identities = tuple(
            value for value in plan_identity_hashes if value != baseline_identity
        )
        return (not conflicting_identities), plan_identity_hashes, conflicting_identities
    is_deterministic = len(fingerprints) <= 1 or plan_fingerprint in fingerprints
    return is_deterministic, plan_identity_hashes, ()


def _determinism_validation_query(
    table_path: str,
    *,
    view_name: str | None,
    plan_fingerprint: str,
    include_identity: bool,
) -> str:
    """Build SQL query for determinism validation.

    Returns
    -------
    str
        SQL query for determinism checks.
    """
    plan_literal = plan_fingerprint.replace("'", "''")
    select_cols = "plan_fingerprint"
    if include_identity:
        select_cols = "plan_fingerprint, plan_identity_hash"
    base = (
        f"SELECT DISTINCT {select_cols} FROM delta_scan('{table_path}') "
        f"WHERE plan_fingerprint = '{plan_literal}'"
    )
    if view_name is None:
        return base
    view_literal = view_name.replace("'", "''")
    return f"{base} AND view_name = '{view_literal}'"


@dataclass(frozen=True)
class PlanArtifactBuildRequest:
    """Inputs for building plan artifact rows."""

    view_name: str
    bundle: DataFusionPlanBundle
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

    Returns
    -------
    PlanArtifactRow | None
        Persisted plan artifact row when storage is enabled.
    """
    location = ensure_plan_artifacts_table(ctx, profile)
    if location is None:
        return None
    row = build_plan_artifact_row(
        ctx,
        profile,
        request=replace(request, event_kind=_EXECUTION_EVENT_KIND),
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

    Returns
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
    record_artifact(profile, "write_artifact_v2", row.to_row())
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

    Returns
    -------
    int | None
        Resolved Delta table version when available.

    Raises
    ------
    ValueError
        Raised when the requested write mode is unsupported.
    """
    from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
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
    return profile.delta_service().table_version(path=str(request.table_path))


def persist_plan_artifact_rows(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PlanArtifactRow],
    location: DatasetLocation | None = None,
) -> tuple[PlanArtifactRow, ...]:
    """Persist plan artifact rows to the Delta-backed artifact store.

    Returns
    -------
    tuple[PlanArtifactRow, ...]
        Persisted plan artifact rows.

    Raises
    ------
    RuntimeError
        Raised when the Delta version cannot be resolved after write.
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


def persist_hamilton_events(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: HamiltonEventsRequest,
) -> tuple[HamiltonEventRow, ...]:
    """Persist Hamilton lifecycle events to the Delta-backed artifact store.

    Returns
    -------
    tuple[HamiltonEventRow, ...]
        Persisted Hamilton event rows.
    """
    location = ensure_hamilton_events_table(ctx, profile)
    if location is None:
        return ()
    events_snapshot = _events_snapshot_as_lists(request.events_snapshot)
    if request.events_ndjson is not None:
        decoded = decode_json_lines(request.events_ndjson, target_type=HamiltonEventLine)
        for line in decoded:
            events_snapshot.setdefault(line.event_name, []).append(line.payload)
    names = (
        tuple(request.event_names)
        if request.event_names is not None
        else tuple(sorted(events_snapshot))
    )
    rows: list[HamiltonEventRow] = []
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
                HamiltonEventRow(
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
    return persist_hamilton_event_rows(ctx, profile, rows=rows, location=location)


def persist_hamilton_event_rows(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[HamiltonEventRow],
    location: DatasetLocation | None = None,
) -> tuple[HamiltonEventRow, ...]:
    """Persist Hamilton event rows to the Delta-backed artifact store.

    Returns
    -------
    tuple[HamiltonEventRow, ...]
        Persisted Hamilton event rows.

    Raises
    ------
    RuntimeError
        Raised when the Delta version cannot be resolved after write.
    """
    if not rows:
        return ()
    resolved_location = location or ensure_hamilton_events_table(ctx, profile)
    if resolved_location is None:
        return ()
    table_path = Path(resolved_location.path)
    arrow_table = pa.Table.from_pylist(
        [row.to_row() for row in rows],
        schema=_hamilton_events_schema(),
    )
    commit_metadata = _commit_metadata_for_hamilton_events(rows)
    final_version = _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=arrow_table,
            commit_metadata=commit_metadata,
            mode="append",
            schema_mode="merge",
            operation_id="hamilton_events_store",
        ),
    )
    if final_version is None:
        msg = f"Failed to resolve Delta version for Hamilton events: {table_path}."
        raise RuntimeError(msg)
    _refresh_hamilton_events_registration(ctx, profile, resolved_location)
    _record_hamilton_events_summary(profile, rows=rows, path=str(table_path), version=final_version)
    return tuple(rows)


def build_plan_artifact_row(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactBuildRequest,
) -> PlanArtifactRow:
    """Build a plan artifact row from a plan bundle and optional lineage.

    Returns
    -------
    PlanArtifactRow
        Serialized plan artifact row.
    """
    resolved_lineage = request.lineage or extract_lineage_from_bundle(request.bundle)
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


def _plan_artifacts_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    root = _plan_artifacts_root(profile)
    if root is None:
        return None
    dataset_spec = dataset_spec_from_schema(
        PLAN_ARTIFACTS_TABLE_NAME,
        _plan_artifacts_schema(),
    )
    location = DatasetLocation(
        path=str(root / _ARTIFACTS_DIRNAME),
        format="delta",
        storage_options={},
        delta_log_storage_options={},
        dataset_spec=dataset_spec,
    )
    return _with_delta_settings(location)


def _hamilton_events_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    root = _plan_artifacts_root(profile)
    if root is None:
        return None
    dataset_spec = dataset_spec_from_schema(
        HAMILTON_EVENTS_TABLE_NAME,
        _hamilton_events_schema(),
    )
    location = DatasetLocation(
        path=str(root / _HAMILTON_EVENTS_DIRNAME),
        format="delta",
        storage_options={},
        delta_log_storage_options={},
        dataset_spec=dataset_spec,
    )
    return _with_delta_settings(location)


def _with_delta_settings(location: DatasetLocation) -> DatasetLocation:
    resolved_scan = resolve_delta_scan_options(location)
    resolved_log = location.resolved.delta_log_storage_options
    overrides = location.overrides
    if resolved_scan is not None:
        from schema_spec.system import DeltaPolicyBundle

        delta_bundle = DeltaPolicyBundle(scan=resolved_scan)
        if overrides is None:
            overrides = DatasetLocationOverrides(delta=delta_bundle)
        else:
            overrides = msgspec.structs.replace(overrides, delta=delta_bundle)
    return msgspec.structs.replace(
        location,
        overrides=overrides,
        delta_log_storage_options=dict(resolved_log or {}),
    )


def _delta_schema_available(
    location: DatasetLocation,
    *,
    profile: DataFusionRuntimeProfile,
) -> bool:
    schema = profile.delta_service().table_schema(
        DeltaSchemaRequest(
            path=str(location.path),
            storage_options=location.storage_options or None,
            log_storage_options=location.delta_log_storage_options or None,
            version=location.delta_version,
            timestamp=location.delta_timestamp,
            gate=location.resolved.delta_feature_gate,
        )
    )
    return schema is not None


def _reset_artifacts_table_path(
    profile: DataFusionRuntimeProfile,
    table_path: Path,
    *,
    table_name: str,
    reason: str,
) -> None:
    if table_path.exists():
        shutil.rmtree(table_path)
    table_path.mkdir(parents=True, exist_ok=True)
    record_artifact(
        profile,
        "artifact_store_reset_v1",
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": table_name,
            "path": str(table_path),
            "reason": reason,
        },
    )


def _plan_artifacts_root(profile: DataFusionRuntimeProfile) -> Path | None:
    root_value = profile.policies.plan_artifacts_root
    if root_value is None:
        if profile.policies.local_filesystem_root is not None:
            root_value = str(
                Path(profile.policies.local_filesystem_root) / _LOCAL_ARTIFACTS_DIRNAME
            )
        else:
            root_value = str(_DEFAULT_ARTIFACTS_ROOT)
    if "://" in root_value:
        record_artifact(
            profile,
            "plan_artifacts_store_unavailable_v1",
            {
                "reason": "non_local_root",
                "root": root_value,
            },
        )
        return None
    root_path = Path(root_value)
    root_path.mkdir(parents=True, exist_ok=True)
    return root_path


def _profile_name(profile: DataFusionRuntimeProfile) -> str | None:
    return profile.policies.config_policy_name


def _plan_artifacts_schema() -> pa.Schema:
    from datafusion_engine.schema.registry import DATAFUSION_PLAN_ARTIFACTS_SCHEMA

    schema = DATAFUSION_PLAN_ARTIFACTS_SCHEMA
    if isinstance(schema, pa.Schema):
        return schema
    return pa.schema(schema)


def _bootstrap_plan_artifacts_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    table_path: Path,
    *,
    schema: pa.Schema | None = None,
    table_name: str = PLAN_ARTIFACTS_TABLE_NAME,
) -> None:
    resolved_schema = schema or _plan_artifacts_schema()
    empty_table = pa.Table.from_pylist([], schema=resolved_schema)
    commit_metadata = {
        "codeanatomy_operation": "plan_artifacts_bootstrap",
        "codeanatomy_mode": "overwrite",
        "codeanatomy_table": table_name,
    }
    _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=empty_table,
            commit_metadata=commit_metadata,
            mode="overwrite",
            schema_mode="overwrite",
            operation_id="plan_artifacts_bootstrap",
        ),
    )


def _refresh_plan_artifacts_registration(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    location: DatasetLocation,
    *,
    table_name: str = PLAN_ARTIFACTS_TABLE_NAME,
) -> None:
    from datafusion_engine.dataset.registration import DataFusionCachePolicy
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(table_name):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(table_name)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(
        name=table_name,
        location=location,
        cache_policy=DataFusionCachePolicy(enabled=False, max_columns=None),
    )


def _record_plan_artifact_summary(
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PlanArtifactRow],
    path: str,
    version: int,
    table_name: str = PLAN_ARTIFACTS_TABLE_NAME,
) -> None:
    kinds = sorted({row.event_kind for row in rows})
    payload = {
        "table": table_name,
        "path": path,
        "row_count": len(rows),
        "event_kinds": kinds,
        "view_names": sorted({row.view_name for row in rows}),
        "delta_version": version,
    }
    record_artifact(profile, "plan_artifacts_store_v2", payload)
    for row in rows:
        record_artifact(profile, table_name, row.to_row())


def _hamilton_events_schema() -> pa.Schema:
    from datafusion_engine.schema.registry import DATAFUSION_HAMILTON_EVENTS_V2_SCHEMA

    schema = DATAFUSION_HAMILTON_EVENTS_V2_SCHEMA
    if isinstance(schema, pa.Schema):
        return schema
    return pa.schema(schema)


def _bootstrap_hamilton_events_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    table_path: Path,
) -> None:
    schema = _hamilton_events_schema()
    empty_table = pa.Table.from_pylist([], schema=schema)
    commit_metadata = {
        "codeanatomy_operation": "hamilton_events_bootstrap",
        "codeanatomy_mode": "overwrite",
        "codeanatomy_table": HAMILTON_EVENTS_TABLE_NAME,
    }
    _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=empty_table,
            commit_metadata=commit_metadata,
            mode="overwrite",
            schema_mode="overwrite",
            operation_id="hamilton_events_bootstrap",
        ),
    )


def _refresh_hamilton_events_registration(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    location: DatasetLocation,
) -> None:
    from datafusion_engine.dataset.registration import DataFusionCachePolicy
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(HAMILTON_EVENTS_TABLE_NAME):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(HAMILTON_EVENTS_TABLE_NAME)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(
        name=HAMILTON_EVENTS_TABLE_NAME,
        location=location,
        cache_policy=DataFusionCachePolicy(enabled=False, max_columns=None),
    )


def _record_hamilton_events_summary(
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[HamiltonEventRow],
    path: str,
    version: int,
) -> None:
    payload = {
        "table": HAMILTON_EVENTS_TABLE_NAME,
        "path": path,
        "row_count": len(rows),
        "event_names": sorted({row.event_name for row in rows}),
        "run_ids": sorted({row.run_id for row in rows}),
        "delta_version": version,
    }
    record_artifact(profile, "hamilton_events_store_v2", payload)
    for row in rows:
        record_artifact(profile, HAMILTON_EVENTS_TABLE_NAME, row.to_row())


def _commit_metadata_for_rows(rows: Sequence[PlanArtifactRow]) -> dict[str, str]:
    event_kinds = sorted({row.event_kind for row in rows})
    view_names = sorted({row.view_name for row in rows})
    metadata: dict[str, str] = {
        "codeanatomy_operation": "plan_artifacts_store",
        "codeanatomy_mode": "append",
        "codeanatomy_row_count": str(len(rows)),
        "codeanatomy_event_kinds": ",".join(event_kinds),
    }
    if view_names:
        metadata["codeanatomy_first_view_name"] = view_names[0]
        metadata["codeanatomy_view_count"] = str(len(view_names))
    return metadata


def _commit_metadata_for_hamilton_events(rows: Sequence[HamiltonEventRow]) -> dict[str, str]:
    event_names = sorted({row.event_name for row in rows})
    run_ids = sorted({row.run_id for row in rows})
    metadata: dict[str, str] = {
        "codeanatomy_operation": "hamilton_events_store",
        "codeanatomy_mode": "append",
        "codeanatomy_row_count": str(len(rows)),
        "codeanatomy_event_name_count": str(len(event_names)),
    }
    if event_names:
        metadata["codeanatomy_first_event_name"] = event_names[0]
    if run_ids:
        metadata["codeanatomy_first_run_id"] = run_ids[0]
        metadata["codeanatomy_run_id_count"] = str(len(run_ids))
    return metadata


def _normalized_event_payload(payload: object) -> object:
    if isinstance(payload, Mapping):
        try:
            return msgspec.convert(payload, type=dict[str, object], strict=False, str_keys=True)
        except msgspec.ValidationError:
            return {str(key): value for key, value in payload.items()}
    return payload


def _event_time_unix_ms(payload: object) -> int:
    if isinstance(payload, Mapping):
        value = payload.get("event_time_unix_ms")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return int(time.time() * 1000)


def _event_payload_msgpack(payload: object) -> bytes:
    if isinstance(payload, msgspec.Struct):
        return dumps_msgpack(payload)
    if isinstance(payload, Mapping):
        return dumps_msgpack(to_builtins(payload, str_keys=True))
    return dumps_msgpack(to_builtins(payload, str_keys=True))


def _lineage_payload(report: LineageReport) -> dict[str, object]:
    scans = [
        {
            "dataset_name": scan.dataset_name,
            "projected_columns": list(scan.projected_columns),
            "pushed_filters": list(scan.pushed_filters),
        }
        for scan in report.scans
    ]
    joins = [
        {
            "join_type": join.join_type,
            "left_keys": list(join.left_keys),
            "right_keys": list(join.right_keys),
        }
        for join in report.joins
    ]
    exprs = [
        {
            "kind": expr.kind,
            "referenced_columns": [list(pair) for pair in expr.referenced_columns],
            "referenced_udfs": list(expr.referenced_udfs),
            "text": expr.text,
        }
        for expr in report.exprs
    ]
    required_columns = {
        dataset: list(columns) for dataset, columns in report.required_columns_by_dataset.items()
    }
    return {
        "scans": scans,
        "joins": joins,
        "exprs": exprs,
        "required_udfs": list(report.required_udfs),
        "required_rewrite_tags": list(report.required_rewrite_tags),
        "required_columns_by_dataset": required_columns,
        "filters": list(report.filters),
        "aggregations": list(report.aggregations),
        "window_functions": list(report.window_functions),
        "subqueries": list(report.subqueries),
        "referenced_udfs": list(report.referenced_udfs),
        "referenced_tables": list(report.referenced_tables),
        "all_required_columns": [list(pair) for pair in report.all_required_columns],
    }


def _scan_units_payload(
    scan_units: Sequence[ScanUnit],
    *,
    scan_keys: Sequence[str],
) -> tuple[dict[str, object], ...]:
    scan_key_set = set(scan_keys)
    payloads: list[dict[str, object]] = []
    for unit in scan_units:
        if scan_key_set and unit.key not in scan_key_set:
            continue
        payloads.append(
            {
                "key": unit.key,
                "dataset_name": unit.dataset_name,
                "delta_version": unit.delta_version,
                "delta_timestamp": unit.delta_timestamp,
                "snapshot_timestamp": unit.snapshot_timestamp,
                "delta_protocol": (
                    to_builtins(unit.delta_protocol) if unit.delta_protocol is not None else None
                ),
                "delta_scan_config": (
                    to_builtins(unit.delta_scan_config)
                    if unit.delta_scan_config is not None
                    else None
                ),
                "delta_scan_config_hash": unit.delta_scan_config_hash,
                "datafusion_provider": unit.datafusion_provider,
                "protocol_compatible": unit.protocol_compatible,
                "protocol_compatibility": (
                    to_builtins(unit.protocol_compatibility)
                    if unit.protocol_compatibility is not None
                    else None
                ),
                "total_files": unit.total_files,
                "candidate_file_count": unit.candidate_file_count,
                "pruned_file_count": unit.pruned_file_count,
                "candidate_files": [str(path) for path in unit.candidate_files],
                "pushed_filters": list(unit.pushed_filters),
                "projected_columns": list(unit.projected_columns),
            }
        )
    payloads.sort(key=lambda item: str(item["key"]))
    return tuple(payloads)


def _delta_inputs_payload(bundle: DataFusionPlanBundle) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = [
        {
            "dataset_name": pin.dataset_name,
            "version": pin.version,
            "timestamp": pin.timestamp,
            "protocol": (
                to_builtins(pin.protocol, str_keys=True) if pin.protocol is not None else None
            ),
            "delta_scan_config": (
                to_builtins(pin.delta_scan_config) if pin.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": pin.delta_scan_config_hash,
            "datafusion_provider": pin.datafusion_provider,
            "protocol_compatible": pin.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(pin.protocol_compatibility)
                if pin.protocol_compatibility is not None
                else None
            ),
        }
        for pin in bundle.delta_inputs
    ]
    payloads.sort(key=lambda item: str(item["dataset_name"]))
    return tuple(payloads)


def _delta_protocol_payload(protocol: object | None) -> dict[str, object] | None:
    if protocol is None:
        return None
    if isinstance(protocol, Mapping):
        payload = {str(key): value for key, value in protocol.items()}
        return payload or None
    if isinstance(protocol, msgspec.Struct):
        resolved = to_builtins(protocol, str_keys=True)
        if isinstance(resolved, Mapping):
            return {str(key): value for key, value in resolved.items()} or None
    return None


def _plan_identity_payload(
    *,
    bundle: DataFusionPlanBundle,
    profile: DataFusionRuntimeProfile,
    delta_inputs_payload: Sequence[Mapping[str, object]],
    scan_payload: Sequence[Mapping[str, object]],
    scan_keys_payload: Sequence[str],
) -> Mapping[str, object]:
    df_settings_entries = tuple(
        sorted((str(key), str(value)) for key, value in bundle.artifacts.df_settings.items())
    )
    return {
        "version": 4,
        "plan_fingerprint": bundle.plan_fingerprint,
        "udf_snapshot_hash": bundle.artifacts.udf_snapshot_hash,
        "function_registry_hash": bundle.artifacts.function_registry_hash,
        "required_udfs": tuple(sorted(bundle.required_udfs)),
        "required_rewrite_tags": tuple(sorted(bundle.required_rewrite_tags)),
        "domain_planner_names": tuple(sorted(bundle.artifacts.domain_planner_names)),
        "df_settings_entries": df_settings_entries,
        "planning_env_hash": bundle.artifacts.planning_env_hash,
        "rulepack_hash": bundle.artifacts.rulepack_hash,
        "information_schema_hash": bundle.artifacts.information_schema_hash,
        "delta_inputs": tuple(delta_inputs_payload),
        "scan_units": tuple(scan_payload),
        "scan_keys": tuple(sorted(set(scan_keys_payload))),
        "profile_settings_hash": profile.settings_hash(),
        "profile_context_key": profile.context_cache_key(),
    }


def _plan_details_payload(
    bundle: DataFusionPlanBundle,
    *,
    delta_inputs_payload: Sequence[Mapping[str, object]],
    scan_payload: Sequence[Mapping[str, object]],
    plan_identity_hash: str,
) -> Mapping[str, object]:
    base_details = dict(bundle.plan_details)
    base_details["delta_inputs_hash"] = hash_json_default(delta_inputs_payload)
    base_details["scan_units_hash"] = hash_json_default(scan_payload)
    base_details["df_settings_hash"] = hash_json_default(bundle.artifacts.df_settings)
    base_details["plan_identity_hash"] = plan_identity_hash
    return base_details


def _udf_compatibility(
    ctx: SessionContext,
    bundle: DataFusionPlanBundle,
) -> tuple[bool, Mapping[str, object]]:
    from datafusion_engine.udf.runtime import (
        rust_udf_snapshot,
        rust_udf_snapshot_hash,
        udf_names_from_snapshot,
    )

    snapshot = rust_udf_snapshot(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    planned_hash = bundle.artifacts.udf_snapshot_hash
    snapshot_match = snapshot_hash == planned_hash
    snapshot_udfs = set(udf_names_from_snapshot(snapshot))
    missing_udfs = sorted(set(bundle.required_udfs) - snapshot_udfs)
    compatibility_ok = snapshot_match and not missing_udfs
    detail = {
        "planned_snapshot_hash": planned_hash,
        "execution_snapshot_hash": snapshot_hash,
        "snapshot_match": snapshot_match,
        "required_udf_count": len(bundle.required_udfs),
        "snapshot_udf_count": len(snapshot_udfs),
        "missing_udfs": missing_udfs,
    }
    return compatibility_ok, detail


def _substrait_msgpack(payload: bytes) -> bytes:
    return dumps_msgpack(SubstraitBytes(payload))


def _proto_msgpack(payload: object | None) -> bytes | None:
    if payload is None:
        return None
    return dumps_msgpack(payload)


def _msgpack_payload(payload: object) -> bytes:
    return dumps_msgpack(to_builtins(payload, str_keys=True))


def _msgpack_payload_raw(payload: object) -> msgspec.Raw:
    return ensure_raw(dumps_msgpack(to_builtins(payload, str_keys=True)))


def _msgpack_or_none(payload: object | None) -> bytes | None:
    if payload is None:
        return None
    return dumps_msgpack(to_builtins(payload, str_keys=True))


__all__ = [
    "HAMILTON_EVENTS_TABLE_NAME",
    "PLAN_ARTIFACTS_TABLE_NAME",
    "WRITE_ARTIFACTS_TABLE_NAME",
    "DeterminismValidationResult",
    "HamiltonEventRow",
    "HamiltonEventsRequest",
    "PlanArtifactRow",
    "PlanArtifactsForViewsRequest",
    "WriteArtifactRow",
    "build_plan_artifact_row",
    "ensure_hamilton_events_table",
    "ensure_plan_artifacts_table",
    "persist_execution_artifact",
    "persist_hamilton_event_rows",
    "persist_hamilton_events",
    "persist_plan_artifact_rows",
    "persist_plan_artifacts_for_views",
    "persist_write_artifact",
    "validate_plan_determinism",
]
