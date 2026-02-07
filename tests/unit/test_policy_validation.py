"""Unit tests for policy validation checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from relspec.policy_validation import validate_policy_bundle

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from relspec.execution_plan import ExecutionPlan


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
) -> ExecutionPlan:
    return cast(
        "ExecutionPlan",
        _ExecutionPlan(
            view_nodes=view_nodes,
            scan_units=scan_units,
            task_plan_metrics=task_plan_metrics or {},
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
        scan_units=(
            _ScanUnit(
                dataset_name="dataset_x",
                protocol_compatibility=_ProtocolCompatibility(
                    compatible=False,
                    reason="protocol mismatch",
                ),
            ),
        ),
        task_plan_metrics={"view_x": _TaskPlanMetric(stats_row_count=10)},
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
