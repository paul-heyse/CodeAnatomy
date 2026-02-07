"""Unit tests for policy validation checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from datafusion_engine.plan.signals import PlanSignals, ScanUnitCompatSummary
from relspec.policy_validation import validate_policy_bundle

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from relspec.execution_plan import ExecutionPlan
    from semantics.program_manifest import SemanticProgramManifest


@dataclass(frozen=True)
class _FeatureGates:
    enable_udfs: bool


@dataclass(frozen=True)
class _RuntimeProfile:
    features: _FeatureGates


@dataclass(frozen=True)
class _ViewNode:
    name: str
    required_udfs: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ProtocolCompatibility:
    compatible: bool | None
    reason: str | None = None


@dataclass(frozen=True)
class _ScanUnit:
    dataset_name: str
    protocol_compatibility: _ProtocolCompatibility | None = None


@dataclass(frozen=True)
class _TaskPlanMetric:
    stats_row_count: int | None = None


@dataclass(frozen=True)
class _ExecutionPlan:
    view_nodes: tuple[_ViewNode, ...]
    scan_units: tuple[_ScanUnit, ...]
    task_plan_metrics: dict[str, _TaskPlanMetric]
    plan_signals_by_task: dict[str, PlanSignals] = field(default_factory=dict)


@dataclass(frozen=True)
class _DatasetBindings:
    locations: dict[str, object]

    def names(self) -> tuple[str, ...]:
        return tuple(self.locations)


@dataclass(frozen=True)
class _SemanticManifest:
    dataset_bindings: _DatasetBindings


def _runtime_profile(*, enable_udfs: bool) -> DataFusionRuntimeProfile:
    return cast(
        "DataFusionRuntimeProfile",
        _RuntimeProfile(features=_FeatureGates(enable_udfs=enable_udfs)),
    )


def _execution_plan(
    *,
    view_nodes: tuple[_ViewNode, ...] = (),
    scan_units: tuple[_ScanUnit, ...] = (),
    task_plan_metrics: dict[str, _TaskPlanMetric] | None = None,
    plan_signals_by_task: dict[str, PlanSignals] | None = None,
) -> ExecutionPlan:
    return cast(
        "ExecutionPlan",
        _ExecutionPlan(
            view_nodes=view_nodes,
            scan_units=scan_units,
            task_plan_metrics=task_plan_metrics or {},
            plan_signals_by_task=plan_signals_by_task or {},
        ),
    )


def _semantic_manifest(*dataset_names: str) -> object:
    return _SemanticManifest(
        dataset_bindings=_DatasetBindings(
            locations={name: object() for name in dataset_names},
        ),
    )


def test_validate_policy_bundle_flags_strict_native_delta_incompatibility() -> None:
    """Strict native provider + incompatible Delta extension should be an error."""
    result = validate_policy_bundle(
        _execution_plan(),
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": True,
            "delta": {"compatible": False, "error": "ffi provider unavailable"},
            "execution_metrics": {},
        },
    )
    assert any(
        issue.code == "DELTA_EXTENSION_INCOMPATIBLE" and issue.severity == "error"
        for issue in result.issues
    )


def test_validate_policy_bundle_warns_when_execution_metrics_missing_or_errored() -> None:
    """Missing or errored execution metrics should emit warnings, not hard errors."""
    missing = validate_policy_bundle(
        _execution_plan(),
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": None,
        },
    )
    errored = validate_policy_bundle(
        _execution_plan(),
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {"error": "probe failed"},
        },
    )
    assert any(issue.code == "RUNTIME_EXECUTION_METRICS_UNAVAILABLE" for issue in missing.issues)
    assert any(issue.code == "RUNTIME_EXECUTION_METRICS_UNAVAILABLE" for issue in errored.issues)
    assert all(issue.severity == "warn" for issue in missing.issues)
    assert all(issue.severity == "warn" for issue in errored.issues)


def test_validate_policy_bundle_preserves_existing_checks() -> None:
    """Existing UDF, Delta protocol, and small-scan checks remain active."""
    plan = _execution_plan(
        view_nodes=(_ViewNode(name="view_x", required_udfs=("udf_missing",)),),
        task_plan_metrics={"view_x": _TaskPlanMetric(stats_row_count=10)},
        plan_signals_by_task={
            "view_x": PlanSignals(
                scan_compat=(
                    ScanUnitCompatSummary(
                        dataset_name="dataset_x",
                        compatible=False,
                        reason="protocol mismatch",
                    ),
                ),
            ),
        },
    )
    result = validate_policy_bundle(
        plan,
        runtime_profile=_runtime_profile(enable_udfs=False),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {},
        },
    )
    codes = {issue.code for issue in result.issues}
    assert "UDFS_DISABLED" in codes
    assert "UDF_MISSING" in codes
    assert "DELTA_PROTOCOL_INCOMPATIBLE" in codes
    assert "SMALL_INPUT_SCAN_POLICY" in codes


def test_validate_policy_bundle_uses_plan_signals_scan_compat() -> None:
    """Delta protocol compatibility errors should be sourced from plan signals."""
    plan = _execution_plan(
        plan_signals_by_task={
            "view_x": PlanSignals(
                scan_compat=(
                    ScanUnitCompatSummary(
                        dataset_name="dataset_x",
                        compatible=False,
                        reason="protocol mismatch",
                    ),
                ),
            ),
        },
    )
    result = validate_policy_bundle(
        plan,
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {},
        },
    )
    errors = [issue for issue in result.issues if issue.code == "DELTA_PROTOCOL_INCOMPATIBLE"]
    assert len(errors) == 1
    assert errors[0].task == "dataset_x"


def test_validate_policy_bundle_deduplicates_scan_compat_by_dataset_and_reason() -> None:
    """Duplicate signal entries for the same dataset/reason should emit one issue."""
    incompat = ScanUnitCompatSummary(
        dataset_name="dataset_x",
        compatible=False,
        reason="protocol mismatch",
    )
    plan = _execution_plan(
        plan_signals_by_task={
            "view_a": PlanSignals(scan_compat=(incompat,)),
            "view_b": PlanSignals(scan_compat=(incompat,)),
        },
    )
    result = validate_policy_bundle(
        plan,
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {},
        },
    )
    errors = [issue for issue in result.issues if issue.code == "DELTA_PROTOCOL_INCOMPATIBLE"]
    assert len(errors) == 1


def test_validate_policy_bundle_manifest_alignment_exact_match_no_warning() -> None:
    """Exact dataset-name matches should not emit manifest alignment warnings."""
    plan = _execution_plan(
        plan_signals_by_task={
            "view_x": PlanSignals(
                scan_compat=(ScanUnitCompatSummary(dataset_name="dataset_x", compatible=True),),
            ),
        },
    )
    result = validate_policy_bundle(
        plan,
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {},
        },
        semantic_manifest=cast("SemanticProgramManifest", _semantic_manifest("dataset_x")),
    )
    codes = {issue.code for issue in result.issues}
    assert "SCAN_DATASET_NOT_IN_MANIFEST" not in codes


def test_validate_policy_bundle_manifest_alignment_suffix_match_no_warning() -> None:
    """Terminal and two-segment suffix matches should be treated as aligned."""
    plan = _execution_plan(
        plan_signals_by_task={
            "view_a": PlanSignals(
                scan_compat=(
                    ScanUnitCompatSummary(dataset_name="catalog.dataset_a", compatible=True),
                ),
            ),
            "view_b": PlanSignals(
                scan_compat=(
                    ScanUnitCompatSummary(dataset_name="root.schema.dataset_b", compatible=True),
                ),
            ),
        },
    )
    result = validate_policy_bundle(
        plan,
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {},
        },
        semantic_manifest=cast(
            "SemanticProgramManifest",
            _semantic_manifest("dataset_a", "schema.dataset_b"),
        ),
    )
    codes = {issue.code for issue in result.issues}
    assert "SCAN_DATASET_NOT_IN_MANIFEST" not in codes


def test_validate_policy_bundle_manifest_alignment_warns_for_unresolved_dataset() -> None:
    """Unknown scan datasets should emit warn-level manifest alignment issues."""
    plan = _execution_plan(
        plan_signals_by_task={
            "view_x": PlanSignals(
                scan_compat=(
                    ScanUnitCompatSummary(dataset_name="missing_dataset", compatible=True),
                ),
            ),
        },
    )
    result = validate_policy_bundle(
        plan,
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot={
            "strict_native_provider_enabled": False,
            "delta": {"compatible": True},
            "execution_metrics": {},
        },
        semantic_manifest=cast("SemanticProgramManifest", _semantic_manifest("dataset_x")),
    )
    warnings = [issue for issue in result.issues if issue.code == "SCAN_DATASET_NOT_IN_MANIFEST"]
    assert len(warnings) == 1
    assert warnings[0].severity == "warn"
    assert warnings[0].task == "view_x"
