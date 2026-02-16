"""Performance-policy comparison controls in plan artifact retention/gating."""

from __future__ import annotations

from datafusion_engine.plan.artifact_store_core import (
    _apply_plan_artifact_retention,
    _plan_diff_gate_violations,
)
from datafusion_engine.plan.perf_policy import PlanBundleComparisonPolicy
from serde_artifacts import PlanArtifactRow


def _sample_plan_artifact_row() -> PlanArtifactRow:
    return PlanArtifactRow(
        event_time_unix_ms=1,
        profile_name="test",
        event_kind="plan",
        view_name="view_a",
        plan_fingerprint="fp_new",
        plan_identity_hash="id_new",
        udf_snapshot_hash="udf",
        function_registry_hash="registry",
        required_udfs=(),
        required_rewrite_tags=(),
        domain_planner_names=(),
        delta_inputs_msgpack=b"{}",
        df_settings={},
        planning_env_msgpack=b"{}",
        planning_env_hash="env",
        rulepack_msgpack=None,
        rulepack_hash=None,
        information_schema_msgpack=b"{}",
        information_schema_hash="schema",
        substrait_msgpack=b"substrait",
        logical_plan_proto_msgpack=b"logical",
        optimized_plan_proto_msgpack=b"optimized",
        execution_plan_proto_msgpack=b"execution",
        explain_tree_rows_msgpack=b"tree",
        explain_verbose_rows_msgpack=b"verbose",
        explain_analyze_duration_ms=1.0,
        explain_analyze_output_rows=1,
        substrait_validation_msgpack=b"validation",
        lineage_msgpack=b"lineage",
        scan_units_msgpack=b"scan",
        scan_keys=(),
        plan_details_msgpack=b"details",
        udf_snapshot_msgpack=b"snapshot",
        udf_planner_snapshot_msgpack=b"planner",
        udf_compatibility_ok=True,
        udf_compatibility_detail_msgpack=b"compat",
        plan_signals_msgpack=b"signals",
    )


def test_comparison_policy_retention_redacts_p1_p2_payloads() -> None:
    """Test comparison policy retention redacts p1 p2 payloads."""
    row = _sample_plan_artifact_row()
    redacted = _apply_plan_artifact_retention(
        row,
        comparison_policy=PlanBundleComparisonPolicy(
            retain_p0_artifacts=True,
            retain_p1_artifacts=False,
            retain_p2_artifacts=False,
            enable_diff_gates=False,
        ),
    )
    assert redacted.substrait_msgpack == b""
    assert redacted.logical_plan_proto_msgpack is None
    assert redacted.execution_plan_proto_msgpack is None
    assert redacted.explain_tree_rows_msgpack is None
    assert redacted.plan_details_msgpack != row.plan_details_msgpack
    assert redacted.plan_signals_msgpack != row.plan_signals_msgpack


def test_diff_gate_detects_plan_identity_change() -> None:
    """Test diff gate detects plan identity change."""
    row = _sample_plan_artifact_row()
    violations = _plan_diff_gate_violations(
        (row,),
        previous_by_view={"view_a": ("fp_old", "id_old")},
    )
    assert len(violations) == 1
    assert violations[0]["view_name"] == "view_a"
    assert violations[0]["previous_plan_identity_hash"] == "id_old"
    assert violations[0]["current_plan_identity_hash"] == "id_new"
