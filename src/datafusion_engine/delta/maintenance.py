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

    Returns
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


def run_delta_maintenance(
    ctx: SessionContext,
    *,
    plan: DeltaMaintenancePlan,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> tuple[Mapping[str, object], ...]:
    """Execute the maintenance plan and record observability artifacts.

    Returns
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
    if policy.optimize_on_write:
        return True
    if policy.vacuum_on_write:
        return True
    if policy.enable_deletion_vectors:
        return True
    if policy.enable_v2_checkpoints:
        return True
    if policy.enable_log_compaction:
        return True
    return bool(getattr(policy, "checkpoint_on_write", False))


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

    Returns
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
    "maintenance_z_order_cols",
    "resolve_delta_maintenance_plan",
    "run_delta_maintenance",
]
