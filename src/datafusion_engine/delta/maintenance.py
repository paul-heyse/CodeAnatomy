"""Delta maintenance planning and execution helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.control_plane import (
    DeltaCheckpointRequest,
    DeltaCommitOptions,
    DeltaOptimizeRequest,
    DeltaVacuumRequest,
    delta_create_checkpoint,
    delta_optimize_compact,
    delta_vacuum,
)
from datafusion_engine.delta.observability import (
    DeltaMaintenanceArtifact,
    record_delta_maintenance,
)
from datafusion_engine.delta.service import (
    DeltaFeatureMutationRequest,
    delta_service_for_profile,
)
from storage.deltalake.delta import DeltaFeatureMutationOptions

if TYPE_CHECKING:
    from datafusion_engine.delta.protocol import DeltaFeatureGate
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DeltaMaintenancePolicy

_MIN_RETENTION_HOURS = 168


@dataclass(frozen=True)
class DeltaMaintenancePlan:
    """Resolved maintenance plan for a Delta table."""

    table_uri: str
    dataset_name: str | None
    storage_options: Mapping[str, str] | None
    log_storage_options: Mapping[str, str] | None
    delta_version: int | None
    delta_timestamp: str | None
    feature_gate: DeltaFeatureGate | None
    policy: DeltaMaintenancePolicy


@dataclass(frozen=True)
class DeltaMaintenancePlanInput:
    """Inputs for resolving a Delta maintenance plan."""

    dataset_location: DatasetLocation | None
    table_uri: str
    dataset_name: str | None
    storage_options: Mapping[str, str] | None
    log_storage_options: Mapping[str, str] | None
    delta_version: int | None
    delta_timestamp: str | None
    feature_gate: DeltaFeatureGate | None
    policy: DeltaMaintenancePolicy | None


def resolve_delta_maintenance_plan(
    request: DeltaMaintenancePlanInput,
) -> DeltaMaintenancePlan | None:
    """Resolve a maintenance plan from location and policy inputs.

    Returns:
    -------
    DeltaMaintenancePlan | None
        Maintenance plan when maintenance is configured, otherwise ``None``.
    """
    resolved = request.policy
    if resolved is None and request.dataset_location is not None:
        resolved = request.dataset_location.resolved.delta_maintenance_policy
    if resolved is None:
        return None
    if not _has_maintenance(resolved):
        return None
    resolved_storage = dict(request.storage_options) if request.storage_options else None
    resolved_log_storage = (
        dict(request.log_storage_options) if request.log_storage_options else None
    )
    return DeltaMaintenancePlan(
        table_uri=request.table_uri,
        dataset_name=request.dataset_name,
        storage_options=resolved_storage,
        log_storage_options=resolved_log_storage,
        delta_version=request.delta_version,
        delta_timestamp=request.delta_timestamp,
        feature_gate=request.feature_gate,
        policy=resolved,
    )


def _threshold_exceeded(value: int | None, threshold: int | None) -> bool:
    """Return True when a metric value exceeds the configured threshold."""
    if value is None or threshold is None:
        return False
    return value > threshold


@dataclass(frozen=True)
class WriteOutcomeMetrics:
    """Captured write outcome metrics for maintenance decisions.

    Parameters
    ----------
    files_created
        Number of new data files created by the write.
    total_file_count
        Total file count in the table after writing.
    version_delta
        Number of versions advanced during this write.
    final_version
        Table version after write completion.
    """

    files_created: int | None = None
    total_file_count: int | None = None
    version_delta: int | None = None
    final_version: int | None = None


def _safe_int(val: object) -> int | None:
    """Coerce a value to int if possible.

    Parameters
    ----------
    val
        Value to coerce.

    Returns:
    -------
    int | None
        Coerced integer when conversion succeeds; otherwise ``None``.
    """
    if val is None:
        return None
    if isinstance(val, (int, float, str)):
        try:
            return int(val)
        except (ValueError, OverflowError):
            return None
    return None


def build_write_outcome_metrics(
    write_result: Mapping[str, object],
    *,
    initial_version: int | None = None,
) -> WriteOutcomeMetrics:
    """Build write outcome metrics from a Delta write result mapping.

    Parameters
    ----------
    write_result
        Raw write result mapping from Delta write operations.
    initial_version
        Table version before the write, used to compute version_delta.

    Returns:
    -------
    WriteOutcomeMetrics
        Captured metrics for maintenance threshold evaluation.
    """
    # files_created: prefer explicit key, fall back to len(data_files)
    files_created = _safe_int(write_result.get("files_created"))
    if files_created is None:
        data_files = write_result.get("data_files")
        if isinstance(data_files, (list, tuple)):
            files_created = len(data_files)

    total_file_count = _safe_int(write_result.get("total_file_count"))

    # final_version and version_delta
    final_version = _safe_int(write_result.get("final_version"))
    version_delta: int | None = None
    if final_version is not None and initial_version is not None:
        version_delta = final_version - initial_version

    return WriteOutcomeMetrics(
        files_created=files_created,
        total_file_count=total_file_count,
        version_delta=version_delta,
        final_version=final_version,
    )


def maintenance_decision_artifact_payload(
    metrics: WriteOutcomeMetrics,
    *,
    plan: DeltaMaintenancePlan | None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    """Build artifact payload for a maintenance decision.

    Parameters
    ----------
    metrics
        Write outcome metrics that triggered (or didn't trigger) maintenance.
    plan
        Resolved maintenance plan, or ``None`` if no maintenance needed.
    dataset_name
        Logical name of the dataset.

    Returns:
    -------
    dict[str, object]
        Payload suitable for ``profile.record_artifact()``.
    """
    triggered_operations: list[str] = []
    if plan is not None:
        policy = plan.policy
        if policy.optimize_on_write:
            triggered_operations.append("optimize")
        if policy.vacuum_on_write:
            triggered_operations.append("vacuum")
        if policy.checkpoint_on_write:
            triggered_operations.append("checkpoint")
        if policy.enable_deletion_vectors:
            triggered_operations.append("enable_deletion_vectors")
        if policy.enable_v2_checkpoints:
            triggered_operations.append("enable_v2_checkpoints")
        if policy.enable_log_compaction:
            triggered_operations.append("enable_log_compaction")

    return {
        "dataset_name": dataset_name,
        "files_created": metrics.files_created,
        "total_file_count": metrics.total_file_count,
        "version_delta": metrics.version_delta,
        "final_version": metrics.final_version,
        "maintenance_triggered": plan is not None,
        "triggered_operations": triggered_operations,
    }


def resolve_maintenance_from_execution(
    metrics: WriteOutcomeMetrics,
    *,
    plan_input: DeltaMaintenancePlanInput,
) -> DeltaMaintenancePlan | None:
    """Resolve a maintenance plan from write outcome metrics and thresholds.

    Fall back to the standard policy-presence-based resolver when outcome
    thresholds are not configured on the policy.

    Parameters
    ----------
    metrics
        Write outcome metrics captured after Delta write.
    plan_input
        Standard maintenance plan input with policy and location.

    Returns:
    -------
    DeltaMaintenancePlan | None
        Maintenance plan when thresholds are exceeded, otherwise ``None``.
    """
    policy = plan_input.policy
    if policy is None and plan_input.dataset_location is not None:
        policy = plan_input.dataset_location.resolved.delta_maintenance_policy
    if policy is None:
        return None

    # Check if any outcome thresholds are configured
    has_thresholds = any(
        v is not None
        for v in (
            policy.optimize_file_threshold,
            policy.total_file_threshold,
            policy.vacuum_version_threshold,
            policy.checkpoint_version_interval,
        )
    )

    if not has_thresholds:
        # No outcome thresholds configured; use standard policy-presence resolver
        return resolve_delta_maintenance_plan(plan_input)

    # Evaluate outcome-based thresholds
    needs_optimize = _threshold_exceeded(
        metrics.files_created, policy.optimize_file_threshold
    ) or _threshold_exceeded(metrics.total_file_count, policy.total_file_threshold)
    needs_vacuum = _threshold_exceeded(metrics.version_delta, policy.vacuum_version_threshold)
    needs_checkpoint = _threshold_exceeded(
        metrics.version_delta, policy.checkpoint_version_interval
    )

    if not (needs_optimize or needs_vacuum or needs_checkpoint):
        return None

    # Build a policy with only the triggered operations enabled
    import msgspec

    triggered_policy = msgspec.structs.replace(
        policy,
        optimize_on_write=needs_optimize or policy.optimize_on_write,
        vacuum_on_write=needs_vacuum or policy.vacuum_on_write,
        checkpoint_on_write=needs_checkpoint or policy.checkpoint_on_write,
    )
    resolved_storage = dict(plan_input.storage_options) if plan_input.storage_options else None
    resolved_log_storage = (
        dict(plan_input.log_storage_options) if plan_input.log_storage_options else None
    )
    return DeltaMaintenancePlan(
        table_uri=plan_input.table_uri,
        dataset_name=plan_input.dataset_name,
        storage_options=resolved_storage,
        log_storage_options=resolved_log_storage,
        delta_version=plan_input.delta_version,
        delta_timestamp=plan_input.delta_timestamp,
        feature_gate=plan_input.feature_gate,
        policy=triggered_policy,
    )


def run_delta_maintenance(
    ctx: SessionContext,
    *,
    plan: DeltaMaintenancePlan,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> tuple[Mapping[str, object], ...]:
    """Execute the maintenance plan and record observability artifacts.

    Returns:
    -------
    tuple[Mapping[str, object], ...]
        Sequence of maintenance reports emitted by the control plane.
    """
    policy = plan.policy
    service = delta_service_for_profile(runtime_profile)
    reports: list[Mapping[str, object]] = []
    if plan.policy.enable_deletion_vectors:
        report = service.features.enable_deletion_vectors(
            _feature_mutation_options(
                plan,
                runtime_profile=runtime_profile,
                commit_metadata={"operation": "enable_deletion_vectors"},
            )
        )
        reports.append(report)
        _record_maintenance(
            runtime_profile,
            request=_MaintenanceRecordRequest(
                plan=plan,
                operation="enable_deletion_vectors",
                report=report,
                retention_hours=None,
                dry_run=None,
                commit_metadata={"operation": "enable_deletion_vectors"},
            ),
        )
    if plan.policy.enable_v2_checkpoints:
        report = service.features.enable_v2_checkpoints(
            _feature_mutation_options(
                plan,
                runtime_profile=runtime_profile,
                commit_metadata={"operation": "enable_v2_checkpoints"},
            )
        )
        reports.append(report)
        _record_maintenance(
            runtime_profile,
            request=_MaintenanceRecordRequest(
                plan=plan,
                operation="enable_v2_checkpoints",
                report=report,
                retention_hours=None,
                dry_run=None,
                commit_metadata={"operation": "enable_v2_checkpoints"},
            ),
        )
    if policy.optimize_on_write:
        report = _run_optimize(ctx, plan=plan)
        reports.append(report)
        _record_maintenance(
            runtime_profile,
            request=_MaintenanceRecordRequest(
                plan=plan,
                operation="optimize",
                report=report,
                retention_hours=None,
                dry_run=None,
                commit_metadata={"operation": "optimize"},
            ),
        )
    if policy.vacuum_on_write:
        report, retention_hours = _run_vacuum(ctx, plan=plan)
        reports.append(report)
        _record_maintenance(
            runtime_profile,
            request=_MaintenanceRecordRequest(
                plan=plan,
                operation="vacuum",
                report=report,
                retention_hours=retention_hours,
                dry_run=policy.vacuum_dry_run,
                commit_metadata={"operation": "vacuum"},
            ),
        )
    if getattr(policy, "checkpoint_on_write", False):
        report = _run_checkpoint(ctx, plan=plan)
        reports.append(report)
        _record_maintenance(
            runtime_profile,
            request=_MaintenanceRecordRequest(
                plan=plan,
                operation="checkpoint",
                report=report,
                retention_hours=None,
                dry_run=None,
                commit_metadata={"operation": "checkpoint"},
            ),
        )
    if policy.enable_log_compaction:
        report = service.cleanup_log(
            path=plan.table_uri,
            storage_options=plan.storage_options,
            log_storage_options=plan.log_storage_options,
            dataset_name=plan.dataset_name,
        )
        reports.append(report)
    return tuple(reports)


def _feature_mutation_options(
    plan: DeltaMaintenancePlan,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    commit_metadata: Mapping[str, str] | None,
) -> DeltaFeatureMutationOptions:
    service = delta_service_for_profile(runtime_profile)
    request = DeltaFeatureMutationRequest(
        path=plan.table_uri,
        storage_options=plan.storage_options,
        log_storage_options=plan.log_storage_options,
        dataset_name=plan.dataset_name,
        commit_metadata=commit_metadata,
        gate=plan.feature_gate,
    )
    return service.features.feature_mutation_options(request)


def _has_maintenance(policy: DeltaMaintenancePolicy) -> bool:
    core_flags = (
        policy.optimize_on_write,
        policy.vacuum_on_write,
        policy.enable_deletion_vectors,
        policy.enable_v2_checkpoints,
        policy.enable_log_compaction,
        bool(getattr(policy, "checkpoint_on_write", False)),
    )
    if any(core_flags):
        return True
    threshold_values = (
        getattr(policy, "optimize_file_threshold", None),
        getattr(policy, "total_file_threshold", None),
        getattr(policy, "vacuum_version_threshold", None),
        getattr(policy, "checkpoint_version_interval", None),
    )
    return any(value is not None for value in threshold_values)


def _optimize_z_order_cols(policy: DeltaMaintenancePolicy) -> tuple[str, ...] | None:
    if policy.z_order_cols and policy.z_order_when != "never":
        return policy.z_order_cols
    return None


def _run_optimize(
    ctx: SessionContext,
    *,
    plan: DeltaMaintenancePlan,
) -> Mapping[str, object]:
    return delta_optimize_compact(
        ctx,
        request=DeltaOptimizeRequest(
            table_uri=plan.table_uri,
            storage_options=plan.storage_options,
            version=plan.delta_version,
            timestamp=plan.delta_timestamp,
            target_size=plan.policy.optimize_target_size,
            z_order_cols=_optimize_z_order_cols(plan.policy),
            gate=plan.feature_gate,
            commit_options=DeltaCommitOptions(metadata={"operation": "optimize"}),
        ),
    )


def _run_vacuum(
    ctx: SessionContext,
    *,
    plan: DeltaMaintenancePlan,
) -> tuple[Mapping[str, object], int | None]:
    retention_hours = plan.policy.vacuum_retention_hours
    if plan.policy.enforce_retention_duration and (
        retention_hours is None or retention_hours < _MIN_RETENTION_HOURS
    ):
        msg = (
            "Delta vacuum retention_hours must be at least "
            f"{_MIN_RETENTION_HOURS} when enforcement is enabled."
        )
        raise ValueError(msg)
    report = delta_vacuum(
        ctx,
        request=DeltaVacuumRequest(
            table_uri=plan.table_uri,
            storage_options=plan.storage_options,
            version=plan.delta_version,
            timestamp=plan.delta_timestamp,
            retention_hours=retention_hours,
            dry_run=plan.policy.vacuum_dry_run,
            enforce_retention_duration=plan.policy.enforce_retention_duration,
            gate=plan.feature_gate,
            commit_options=DeltaCommitOptions(metadata={"operation": "vacuum"}),
        ),
    )
    return report, retention_hours


def _run_checkpoint(
    ctx: SessionContext,
    *,
    plan: DeltaMaintenancePlan,
) -> Mapping[str, object]:
    return delta_create_checkpoint(
        ctx,
        request=DeltaCheckpointRequest(
            table_uri=plan.table_uri,
            storage_options=plan.storage_options,
            version=plan.delta_version,
            timestamp=plan.delta_timestamp,
            gate=plan.feature_gate,
        ),
    )


@dataclass(frozen=True)
class _MaintenanceRecordRequest:
    plan: DeltaMaintenancePlan
    operation: str
    report: Mapping[str, object]
    retention_hours: int | None
    dry_run: bool | None
    commit_metadata: Mapping[str, str] | None


def _record_maintenance(
    profile: DataFusionRuntimeProfile | None,
    *,
    request: _MaintenanceRecordRequest,
) -> None:
    record_delta_maintenance(
        profile,
        artifact=DeltaMaintenanceArtifact(
            table_uri=request.plan.table_uri,
            operation=request.operation,
            report=request.report,
            dataset_name=request.plan.dataset_name,
            retention_hours=request.retention_hours,
            dry_run=request.dry_run,
            commit_metadata=request.commit_metadata,
        ),
    )


def maintenance_z_order_cols(
    policy: DeltaMaintenancePolicy | None,
) -> Sequence[str]:
    """Return z-order columns for the policy when configured.

    Returns:
    -------
    Sequence[str]
        Z-order columns, or empty sequence when not configured.
    """
    if policy is None:
        return ()
    cols = _optimize_z_order_cols(policy)
    return cols or ()


__all__ = [
    "DeltaMaintenancePlan",
    "DeltaMaintenancePlanInput",
    "WriteOutcomeMetrics",
    "build_write_outcome_metrics",
    "maintenance_decision_artifact_payload",
    "maintenance_z_order_cols",
    "resolve_delta_maintenance_plan",
    "resolve_maintenance_from_execution",
    "run_delta_maintenance",
]
