"""Unit tests for outcome-driven Delta maintenance decisions."""

from __future__ import annotations

from datafusion_engine.delta.maintenance import (
    DeltaMaintenancePlanInput,
    WriteOutcomeMetrics,
    maintenance_decision_artifact_payload,
    resolve_maintenance_from_execution,
)
from schema_spec.system import DeltaMaintenancePolicy


def _plan_input(policy: DeltaMaintenancePolicy | None) -> DeltaMaintenancePlanInput:
    return DeltaMaintenancePlanInput(
        dataset_location=None,
        table_uri="/tmp/table",
        dataset_name="events",
        storage_options=None,
        log_storage_options=None,
        delta_version=10,
        delta_timestamp=None,
        feature_gate=None,
        policy=policy,
    )


def test_metrics_unavailable_uses_internal_compatibility_fallback() -> None:
    decision = resolve_maintenance_from_execution(
        _plan_input(DeltaMaintenancePolicy(optimize_on_write=True)),
        metrics=None,
    )

    assert decision.plan is not None
    assert decision.reasons == ("metrics_unavailable_compatibility_fallback",)
    assert decision.used_fallback is True


def test_optimize_threshold_exceeded_enables_optimize() -> None:
    decision = resolve_maintenance_from_execution(
        _plan_input(DeltaMaintenancePolicy(optimize_file_threshold=5)),
        metrics=WriteOutcomeMetrics(files_created=6, final_version=11),
    )

    assert decision.plan is not None
    assert decision.plan.policy.optimize_on_write is True
    assert decision.reasons == (
        "optimize_file_threshold_exceeded",
        "optimize_on_write",
    )
    assert decision.used_fallback is False


def test_vacuum_threshold_exceeded_enables_vacuum() -> None:
    decision = resolve_maintenance_from_execution(
        _plan_input(DeltaMaintenancePolicy(vacuum_version_threshold=2)),
        metrics=WriteOutcomeMetrics(version_delta=3, final_version=11),
    )

    assert decision.plan is not None
    assert decision.plan.policy.vacuum_on_write is True
    assert decision.reasons == (
        "vacuum_version_threshold_exceeded",
        "vacuum_on_write",
    )


def test_checkpoint_interval_enables_checkpoint() -> None:
    decision = resolve_maintenance_from_execution(
        _plan_input(DeltaMaintenancePolicy(checkpoint_version_interval=5)),
        metrics=WriteOutcomeMetrics(final_version=20),
    )

    assert decision.plan is not None
    assert decision.plan.policy.checkpoint_on_write is True
    assert decision.reasons == (
        "checkpoint_interval_reached",
        "checkpoint_on_write",
    )


def test_thresholds_not_met_returns_no_plan() -> None:
    decision = resolve_maintenance_from_execution(
        _plan_input(
            DeltaMaintenancePolicy(
                optimize_file_threshold=10,
                total_file_threshold=10,
                vacuum_version_threshold=3,
                checkpoint_version_interval=5,
            )
        ),
        metrics=WriteOutcomeMetrics(
            files_created=1, total_file_count=2, version_delta=1, final_version=21
        ),
    )

    assert decision.plan is None
    assert decision.reasons == ("thresholds_not_met",)
    assert decision.used_fallback is False


def test_artifact_payload_is_deterministic_and_ordered() -> None:
    decision = resolve_maintenance_from_execution(
        _plan_input(
            DeltaMaintenancePolicy(
                optimize_on_write=True,
                vacuum_on_write=True,
                checkpoint_on_write=True,
                enable_deletion_vectors=True,
                enable_v2_checkpoints=True,
                enable_log_compaction=True,
            )
        ),
        metrics=WriteOutcomeMetrics(final_version=25),
    )

    first = maintenance_decision_artifact_payload(decision, dataset_name="events")
    second = maintenance_decision_artifact_payload(decision, dataset_name="events")

    assert first == second
    assert first["triggered_operations"] == [
        "optimize",
        "vacuum",
        "checkpoint",
        "enable_deletion_vectors",
        "enable_v2_checkpoints",
        "enable_log_compaction",
    ]
    assert first["reasons"] == [
        "optimize_on_write",
        "vacuum_on_write",
        "checkpoint_on_write",
        "enable_deletion_vectors",
        "enable_v2_checkpoints",
        "enable_log_compaction",
    ]
    assert first["used_fallback"] is False
