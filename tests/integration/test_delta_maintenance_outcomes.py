"""Integration-level coverage for outcome-driven Delta maintenance decisions."""

from __future__ import annotations

import pytest

from datafusion_engine.delta.maintenance import (
    DeltaMaintenancePlanInput,
    WriteOutcomeMetrics,
    resolve_maintenance_from_execution,
)
from schema_spec.contracts import DeltaMaintenancePolicy


@pytest.mark.integration
def test_metrics_unavailable_keeps_base_policy_plan() -> None:
    decision = resolve_maintenance_from_execution(
        DeltaMaintenancePlanInput(
            dataset_location=None,
            table_uri="/tmp/table",
            dataset_name="events",
            storage_options=None,
            log_storage_options=None,
            delta_version=1,
            delta_timestamp=None,
            feature_gate=None,
            policy=DeltaMaintenancePolicy(
                optimize_on_write=True,
                vacuum_on_write=True,
            ),
        ),
        metrics=None,
    )

    assert decision.plan is not None
    assert decision.plan.policy.optimize_on_write is True
    assert decision.plan.policy.vacuum_on_write is True
    assert decision.used_fallback is True
    assert decision.reasons == ("metrics_unavailable_compatibility_fallback",)


@pytest.mark.integration
def test_metrics_threshold_path_disables_untriggered_operations() -> None:
    decision = resolve_maintenance_from_execution(
        DeltaMaintenancePlanInput(
            dataset_location=None,
            table_uri="/tmp/table",
            dataset_name="events",
            storage_options=None,
            log_storage_options=None,
            delta_version=1,
            delta_timestamp=None,
            feature_gate=None,
            policy=DeltaMaintenancePolicy(optimize_file_threshold=100, vacuum_version_threshold=10),
        ),
        metrics=WriteOutcomeMetrics(files_created=1, version_delta=1, final_version=2),
    )

    assert decision.plan is None
    assert decision.used_fallback is False
    assert decision.reasons == ("thresholds_not_met",)
