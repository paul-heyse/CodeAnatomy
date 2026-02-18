"""Delta maintenance planning and execution helpers."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import msgspec
from datafusion import SessionContext

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.control_plane_core import (
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
from datafusion_engine.delta.service import DeltaFeatureMutationRequest, DeltaService
from obs.datafusion_engine_runtime_metrics import (
    maintenance_tracer,
    record_delta_maintenance_run,
)
from storage.deltalake.delta_write import DeltaFeatureMutationOptions

if TYPE_CHECKING:
    from datafusion_engine.delta.protocol import DeltaFeatureGate
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.dataset_spec import DeltaMaintenancePolicy

_MIN_RETENTION_HOURS = 168
logger = logging.getLogger(__name__)


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


def _resolve_base_maintenance_plan(
    request: DeltaMaintenancePlanInput,
) -> DeltaMaintenancePlan | None:
    """Resolve base maintenance plan from request inputs.

    Returns:
        DeltaMaintenancePlan | None: The resolved plan when maintenance is configured.
    """
    resolved = request.policy
    if resolved is None and request.dataset_location is not None:
        resolved = request.dataset_location.delta_maintenance_policy
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


@dataclass(frozen=True)
class DeltaMaintenanceDecision:
    """Outcome-driven Delta maintenance decision.

    Parameters
    ----------
    plan
        Resolved maintenance plan when maintenance should run.
    metrics
        Write outcome metrics used for threshold evaluation.
    reasons
        Deterministic reason labels describing the decision.
    used_fallback
        ``True`` when decision fell back to compatibility behavior because
        write metrics were unavailable.
    """

    plan: DeltaMaintenancePlan | None
    metrics: WriteOutcomeMetrics
    reasons: tuple[str, ...]
    used_fallback: bool = False


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


def _checkpoint_interval_reached(final_version: int | None, interval: int | None) -> bool:
    """Return True when final version is aligned to checkpoint interval."""
    if final_version is None or interval is None or interval <= 0:
        return False
    return final_version % interval == 0


def _has_executable_operations(policy: DeltaMaintenancePolicy) -> bool:
    """Return True when policy enables at least one executable operation."""
    return any(
        (
            policy.optimize_on_write,
            policy.vacuum_on_write,
            policy.enable_deletion_vectors,
            policy.enable_v2_checkpoints,
            policy.enable_log_compaction,
            bool(getattr(policy, "checkpoint_on_write", False)),
        )
    )


def _set_policy_flag(
    policy: DeltaMaintenancePolicy,
    *,
    flag_name: str,
) -> DeltaMaintenancePolicy:
    """Return policy with ``flag_name`` enabled when currently disabled."""
    if bool(getattr(policy, flag_name, False)):
        return policy
    return msgspec.structs.replace(policy, **{flag_name: True})


def _apply_execution_thresholds(
    policy: DeltaMaintenancePolicy,
    *,
    metrics: WriteOutcomeMetrics,
) -> tuple[DeltaMaintenancePolicy, tuple[str, ...]]:
    """Apply outcome thresholds and return effective policy plus reasons.

    Returns:
    -------
    tuple[DeltaMaintenancePolicy, tuple[str, ...]]
        Effective policy with threshold-driven flags and ordered reason codes.
    """
    effective = policy
    reasons: list[str] = []
    threshold_checks = (
        (
            "optimize_on_write",
            _threshold_exceeded(metrics.files_created, policy.optimize_file_threshold),
            "optimize_file_threshold_exceeded",
        ),
        (
            "optimize_on_write",
            _threshold_exceeded(metrics.total_file_count, policy.total_file_threshold),
            "total_file_threshold_exceeded",
        ),
        (
            "vacuum_on_write",
            _threshold_exceeded(metrics.version_delta, policy.vacuum_version_threshold),
            "vacuum_version_threshold_exceeded",
        ),
        (
            "checkpoint_on_write",
            _checkpoint_interval_reached(metrics.final_version, policy.checkpoint_version_interval),
            "checkpoint_interval_reached",
        ),
    )
    for flag_name, is_triggered, reason in threshold_checks:
        if not is_triggered:
            continue
        reasons.append(reason)
        effective = _set_policy_flag(effective, flag_name=flag_name)

    for flag_name, reason in (
        ("optimize_on_write", "optimize_on_write"),
        ("vacuum_on_write", "vacuum_on_write"),
        ("checkpoint_on_write", "checkpoint_on_write"),
    ):
        if bool(getattr(effective, flag_name, False)) and reason not in reasons:
            reasons.append(reason)

    for flag_name, reason in (
        ("enable_deletion_vectors", "enable_deletion_vectors"),
        ("enable_v2_checkpoints", "enable_v2_checkpoints"),
        ("enable_log_compaction", "enable_log_compaction"),
    ):
        if bool(getattr(effective, flag_name, False)):
            reasons.append(reason)
    return effective, tuple(dict.fromkeys(reasons))


def resolve_maintenance_from_execution(
    request: DeltaMaintenancePlanInput,
    *,
    metrics: WriteOutcomeMetrics | None,
) -> DeltaMaintenanceDecision:
    """Resolve maintenance plan from write outcomes with compatibility fallback.

    Returns:
    -------
    DeltaMaintenanceDecision
        Decision bundle containing optional plan, metrics, and reasons.
    """
    base_plan = _resolve_base_maintenance_plan(request)
    resolved_metrics = metrics or WriteOutcomeMetrics()
    if base_plan is None:
        return DeltaMaintenanceDecision(
            plan=None,
            metrics=resolved_metrics,
            reasons=("maintenance_not_configured",),
            used_fallback=metrics is None,
        )
    if metrics is None:
        return DeltaMaintenanceDecision(
            plan=base_plan,
            metrics=resolved_metrics,
            reasons=("metrics_unavailable_compatibility_fallback",),
            used_fallback=True,
        )
    effective_policy, reasons = _apply_execution_thresholds(base_plan.policy, metrics=metrics)
    if not _has_executable_operations(effective_policy):
        return DeltaMaintenanceDecision(
            plan=None,
            metrics=metrics,
            reasons=reasons or ("thresholds_not_met",),
            used_fallback=False,
        )
    effective_plan = DeltaMaintenancePlan(
        table_uri=base_plan.table_uri,
        dataset_name=base_plan.dataset_name,
        storage_options=base_plan.storage_options,
        log_storage_options=base_plan.log_storage_options,
        delta_version=base_plan.delta_version,
        delta_timestamp=base_plan.delta_timestamp,
        feature_gate=base_plan.feature_gate,
        policy=effective_policy,
    )
    return DeltaMaintenanceDecision(
        plan=effective_plan,
        metrics=metrics,
        reasons=reasons,
        used_fallback=False,
    )


def maintenance_decision_artifact_payload(
    decision: DeltaMaintenanceDecision,
    *,
    dataset_name: str | None = None,
) -> dict[str, object]:
    """Build artifact payload for a maintenance decision.

    Parameters
    ----------
    decision
        Outcome decision bundle containing metrics, reasons, and optional plan.
    dataset_name
        Logical name of the dataset.

    Returns:
    -------
    dict[str, object]
        Payload suitable for ``profile.record_artifact()``.
    """
    metrics = decision.metrics
    plan = decision.plan
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
        "reasons": list(decision.reasons),
        "used_fallback": decision.used_fallback,
    }


def run_delta_maintenance(
    ctx: SessionContext,
    *,
    plan: DeltaMaintenancePlan,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[Mapping[str, object], ...]:
    """Execute the maintenance plan and record observability artifacts.

    Returns:
    -------
    tuple[Mapping[str, object], ...]
        Sequence of maintenance reports emitted by the control plane.
    """
    policy = plan.policy
    service = cast("DeltaService", runtime_profile.delta_ops.delta_service())
    reports: list[Mapping[str, object]] = []
    logger.info(
        "Running delta maintenance table_uri=%s dataset=%s",
        plan.table_uri,
        plan.dataset_name,
    )
    if plan.policy.enable_deletion_vectors:
        report = _run_maintenance_operation(
            operation="enable_deletion_vectors",
            table_uri=plan.table_uri,
            execute=lambda: service.features.enable_deletion_vectors(
                _feature_mutation_options(
                    plan,
                    delta_service=service,
                    commit_metadata={"operation": "enable_deletion_vectors"},
                )
            ),
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
        report = _run_maintenance_operation(
            operation="enable_v2_checkpoints",
            table_uri=plan.table_uri,
            execute=lambda: service.features.enable_v2_checkpoints(
                _feature_mutation_options(
                    plan,
                    delta_service=service,
                    commit_metadata={"operation": "enable_v2_checkpoints"},
                )
            ),
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
        report = _run_maintenance_operation(
            operation="optimize",
            table_uri=plan.table_uri,
            execute=lambda: _run_optimize(ctx, plan=plan),
        )
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
        report, retention_hours = _run_maintenance_operation(
            operation="vacuum",
            table_uri=plan.table_uri,
            execute=lambda: _run_vacuum(ctx, plan=plan),
        )
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
        report = _run_maintenance_operation(
            operation="checkpoint",
            table_uri=plan.table_uri,
            execute=lambda: _run_checkpoint(ctx, plan=plan),
        )
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
        report = _run_maintenance_operation(
            operation="log_compaction",
            table_uri=plan.table_uri,
            execute=lambda: service.cleanup_log(
                path=plan.table_uri,
                storage_options=plan.storage_options,
                log_storage_options=plan.log_storage_options,
                dataset_name=plan.dataset_name,
            ),
        )
        reports.append(report)
    logger.info(
        "Delta maintenance completed table_uri=%s operations=%s",
        plan.table_uri,
        len(reports),
    )
    return tuple(reports)


def _run_maintenance_operation[T](
    *,
    operation: str,
    table_uri: str,
    execute: Callable[[], T],
) -> T:
    start = time.perf_counter()
    tracer = maintenance_tracer()
    with tracer.start_as_current_span(f"delta.maintenance.{operation}") as span:
        span.set_attribute("delta.table_uri", table_uri)
        span.set_attribute("delta.operation", operation)
        try:
            result = execute()
        except (
            RuntimeError,
            TypeError,
            ValueError,
            OSError,
        ) as exc:
            duration_s = time.perf_counter() - start
            record_delta_maintenance_run(
                operation=operation,
                status="error",
                duration_s=duration_s,
            )
            span.set_attribute("status", "error")
            span.record_exception(exc)
            logger.exception(
                "Delta maintenance operation failed table_uri=%s operation=%s",
                table_uri,
                operation,
            )
            raise
        duration_s = time.perf_counter() - start
        record_delta_maintenance_run(operation=operation, status="ok", duration_s=duration_s)
        span.set_attribute("status", "ok")
        span.set_attribute("duration_s", duration_s)
        return result


def _feature_mutation_options(
    plan: DeltaMaintenancePlan,
    *,
    delta_service: DeltaService,
    commit_metadata: Mapping[str, str] | None,
) -> DeltaFeatureMutationOptions:
    request = DeltaFeatureMutationRequest(
        path=plan.table_uri,
        storage_options=plan.storage_options,
        log_storage_options=plan.log_storage_options,
        dataset_name=plan.dataset_name,
        commit_metadata=commit_metadata,
        gate=plan.feature_gate,
    )
    return delta_service.features.feature_mutation_options(request)


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
    "DeltaMaintenanceDecision",
    "DeltaMaintenancePlan",
    "DeltaMaintenancePlanInput",
    "WriteOutcomeMetrics",
    "build_write_outcome_metrics",
    "maintenance_decision_artifact_payload",
    "maintenance_z_order_cols",
    "resolve_maintenance_from_execution",
    "run_delta_maintenance",
]
