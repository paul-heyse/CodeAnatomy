"""Unit tests for policy validation checks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from datafusion_engine.plan.signals import PlanSignals, ScanUnitCompatSummary
from relspec.compiled_policy import CompiledExecutionPolicy
from relspec.policy_validation import PolicyValidationIssue, validate_policy_bundle

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from relspec.execution_planning_runtime import ExecutionPlan
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
        """Return dataset names.

        Returns:
        -------
        tuple[str, ...]
            Dataset names.
        """
        return tuple(self.locations)


@dataclass(frozen=True)
class _SemanticIRView:
    name: str


@dataclass(frozen=True)
class _SemanticIR:
    views: tuple[_SemanticIRView, ...]


@dataclass(frozen=True)
class _SemanticManifest:
    dataset_bindings: _DatasetBindings
    semantic_ir: _SemanticIR | None = None


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


def _semantic_manifest(
    *dataset_names: str,
    view_names: tuple[str, ...] = (),
) -> object:
    """Build a stub manifest with dataset bindings and optional view names.

    Returns:
    -------
    object
        Stub manifest suitable for cast to ``SemanticProgramManifest``.
    """
    ir = (
        _SemanticIR(views=tuple(_SemanticIRView(name=n) for n in view_names))
        if view_names
        else None
    )
    return _SemanticManifest(
        dataset_bindings=_DatasetBindings(
            locations={name: object() for name in dataset_names},
        ),
        semantic_ir=ir,
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


# ---------------------------------------------------------------------------
# Compiled policy validation tests
# ---------------------------------------------------------------------------

_BASE_CAPABILITY: Mapping[str, object] = {
    "strict_native_provider_enabled": False,
    "delta": {"compatible": True},
    "execution_metrics": {},
}


def _base_validate(
    *,
    compiled_policy: CompiledExecutionPolicy | None = None,
    semantic_manifest: object | None = None,
    capability_snapshot: Mapping[str, object] | None = None,
) -> tuple[set[str], list[PolicyValidationIssue]]:
    """Run validate_policy_bundle with minimal boilerplate, return codes and issues.

    Returns:
    -------
    tuple[set[str], list[PolicyValidationIssue]]
        Issue code set and full issue list.
    """
    result = validate_policy_bundle(
        _execution_plan(),
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot=capability_snapshot or _BASE_CAPABILITY,
        semantic_manifest=cast("SemanticProgramManifest", semantic_manifest)
        if semantic_manifest
        else None,
        compiled_policy=compiled_policy,
    )
    return {i.code for i in result.issues}, list(result.issues)


def test_compiled_policy_none_produces_no_compiled_issues() -> None:
    """When compiled_policy is None, no compiled-policy issues should appear."""
    codes, _ = _base_validate(compiled_policy=None)
    compiled_codes = {c for c in codes if c.startswith(("compiled_policy", "stats_"))}
    assert not compiled_codes


def test_compiled_policy_extra_cache_views_warns() -> None:
    """Cache views not in manifest should produce a warning."""
    policy = CompiledExecutionPolicy(
        cache_policy_by_view={
            "view_a": "delta_staging",
            "view_b": "none",
            "view_extra": "delta_output",
        },
        policy_fingerprint="abc123",
    )
    manifest = _semantic_manifest("ds_a", view_names=("view_a", "view_b"))
    codes, issues = _base_validate(compiled_policy=policy, semantic_manifest=manifest)
    assert "compiled_policy_extra_cache_views" in codes
    extra_issues = [i for i in issues if i.code == "compiled_policy_extra_cache_views"]
    assert len(extra_issues) == 1
    assert extra_issues[0].task == "view_extra"
    assert extra_issues[0].severity == "warn"


def test_compiled_policy_cache_views_subset_no_warning() -> None:
    """Cache view keys that are a subset of manifest views should produce no issues."""
    policy = CompiledExecutionPolicy(
        cache_policy_by_view={"view_a": "delta_staging"},
        policy_fingerprint="abc123",
    )
    manifest = _semantic_manifest("ds_a", view_names=("view_a", "view_b"))
    codes, _ = _base_validate(compiled_policy=policy, semantic_manifest=manifest)
    assert "compiled_policy_extra_cache_views" not in codes


def test_compiled_policy_extra_scan_overrides_warns() -> None:
    """Scan overrides referencing unknown datasets should produce warnings."""
    policy = CompiledExecutionPolicy(
        scan_policy_overrides={
            "ds_a": {"policy": {}, "reasons": ("cap_and_stats",)},
            "ds_unknown": {"policy": {}, "reasons": ("heuristic",)},
        },
        policy_fingerprint="abc123",
    )
    manifest = _semantic_manifest("ds_a", view_names=("view_a",))
    codes, issues = _base_validate(compiled_policy=policy, semantic_manifest=manifest)
    assert "compiled_policy_extra_scan_overrides" in codes
    extra = [i for i in issues if i.code == "compiled_policy_extra_scan_overrides"]
    assert len(extra) == 1
    assert extra[0].task == "ds_unknown"
    assert extra[0].severity == "warn"


def test_compiled_policy_scan_overrides_subset_no_warning() -> None:
    """Scan override keys that match manifest datasets should produce no issues."""
    policy = CompiledExecutionPolicy(
        scan_policy_overrides={"ds_a": {"policy": {}, "reasons": ()}},
        policy_fingerprint="abc123",
    )
    manifest = _semantic_manifest("ds_a", "ds_b", view_names=("view_a",))
    codes, _ = _base_validate(compiled_policy=policy, semantic_manifest=manifest)
    assert "compiled_policy_extra_scan_overrides" not in codes


def test_compiled_policy_consistency_skipped_without_manifest() -> None:
    """Consistency checks should be skipped when no manifest is provided."""
    policy = CompiledExecutionPolicy(
        cache_policy_by_view={"orphan_view": "none"},
        scan_policy_overrides={"orphan_ds": {"policy": {}, "reasons": ()}},
        policy_fingerprint="abc123",
    )
    codes, _ = _base_validate(compiled_policy=policy, semantic_manifest=None)
    assert "compiled_policy_extra_cache_views" not in codes
    assert "compiled_policy_extra_scan_overrides" not in codes


def test_stats_dependent_override_warns_without_capabilities() -> None:
    """Stats-dependent scan override should warn when capabilities lack plan stats."""
    policy = CompiledExecutionPolicy(
        scan_policy_overrides={
            "ds_a": {"policy": {}, "reasons": ("cap_and_stats", "statistics_available")},
        },
        policy_fingerprint="abc123",
    )
    cap: Mapping[str, object] = {
        "strict_native_provider_enabled": False,
        "delta": {"compatible": True},
        "execution_metrics": {},
        "plan_capabilities": {"has_execution_plan_statistics": False},
    }
    codes, issues = _base_validate(compiled_policy=policy, capability_snapshot=cap)
    assert "stats_dependent_override_without_capabilities" in codes
    stat_issues = [i for i in issues if i.code == "stats_dependent_override_without_capabilities"]
    assert len(stat_issues) == 1
    assert stat_issues[0].severity == "warn"


def test_stats_dependent_override_no_warning_when_stats_available() -> None:
    """Stats-dependent scan override should be silent when stats are available."""
    policy = CompiledExecutionPolicy(
        scan_policy_overrides={
            "ds_a": {"policy": {}, "reasons": ("statistics_available",)},
        },
        policy_fingerprint="abc123",
    )
    cap: Mapping[str, object] = {
        "strict_native_provider_enabled": False,
        "delta": {"compatible": True},
        "execution_metrics": {},
        "plan_capabilities": {"has_execution_plan_statistics": True},
    }
    codes, _ = _base_validate(compiled_policy=policy, capability_snapshot=cap)
    assert "stats_dependent_override_without_capabilities" not in codes


def test_stats_no_warning_when_reasons_not_stats_related() -> None:
    """Non-statistics reasons should not trigger the stats-availability check."""
    policy = CompiledExecutionPolicy(
        scan_policy_overrides={
            "ds_a": {"policy": {}, "reasons": ("heuristic", "pushed_filters")},
        },
        policy_fingerprint="abc123",
    )
    cap: Mapping[str, object] = {
        "strict_native_provider_enabled": False,
        "delta": {"compatible": True},
        "execution_metrics": {},
        "plan_capabilities": {"has_execution_plan_statistics": False},
    }
    codes, _ = _base_validate(compiled_policy=policy, capability_snapshot=cap)
    assert "stats_dependent_override_without_capabilities" not in codes


def test_evidence_coherence_missing_fingerprint() -> None:
    """Compiled policy without a fingerprint should produce a warning."""
    policy = CompiledExecutionPolicy(policy_fingerprint=None)
    codes, issues = _base_validate(compiled_policy=policy)
    assert "compiled_policy_missing_fingerprint" in codes
    fp_issues = [i for i in issues if i.code == "compiled_policy_missing_fingerprint"]
    assert len(fp_issues) == 1
    assert fp_issues[0].severity == "warn"


def test_evidence_coherence_present_fingerprint_no_warning() -> None:
    """Compiled policy with a fingerprint should not warn about missing fingerprint."""
    policy = CompiledExecutionPolicy(policy_fingerprint="sha256:abc")
    codes, _ = _base_validate(compiled_policy=policy)
    assert "compiled_policy_missing_fingerprint" not in codes


def test_evidence_coherence_empty_cache_with_other_sections() -> None:
    """Empty cache_policy_by_view with populated other sections should warn."""
    policy = CompiledExecutionPolicy(
        cache_policy_by_view={},
        scan_policy_overrides={"ds_a": {"policy": {}, "reasons": ()}},
        policy_fingerprint="abc",
    )
    codes, issues = _base_validate(compiled_policy=policy)
    assert "compiled_policy_empty_cache_section" in codes
    cache_issues = [i for i in issues if i.code == "compiled_policy_empty_cache_section"]
    assert len(cache_issues) == 1
    assert cache_issues[0].severity == "warn"


def test_evidence_coherence_empty_policy_no_warning() -> None:
    """Fully empty compiled policy should not warn about empty cache section."""
    policy = CompiledExecutionPolicy(policy_fingerprint="abc")
    codes, _ = _base_validate(compiled_policy=policy)
    assert "compiled_policy_empty_cache_section" not in codes


def test_compiled_policy_backward_compatible_without_param() -> None:
    """Existing callers without compiled_policy param should still work."""
    result = validate_policy_bundle(
        _execution_plan(),
        runtime_profile=_runtime_profile(enable_udfs=True),
        udf_snapshot={},
        capability_snapshot=_BASE_CAPABILITY,
    )
    assert result.is_valid
