"""Structured policy validation for execution plan compilation.

Validate runtime policy bundles against plan requirements at the
execution-plan boundary. Run after ``compile_authority_plan()``,
NOT inside ``compile_semantic_program()`` which returns only
``SemanticProgramManifest`` and has no execution plan.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from datafusion_engine.extensions.runtime_capabilities import RuntimeCapabilitiesSnapshot
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.plan.signals import PlanSignals
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from relspec.compiled_policy import CompiledExecutionPolicy
    from relspec.execution_planning_runtime import ExecutionPlan
    from semantics.program_manifest import SemanticProgramManifest

_SMALL_SCAN_ROW_THRESHOLD: int = 1000
_DATASET_SUFFIX_SEGMENT_COUNT: int = 2
_STATS_REASON_KEYWORDS: frozenset[str] = frozenset({"stats", "statistics", "row_count", "num_rows"})

# Scan policy reasons that rely on row-count or similar statistics at the plan
# level.  Kept separate from ``_STATS_REASON_KEYWORDS`` which targets serialized
# compiled-policy reason strings.
_PLAN_STATS_DEPENDENT_REASONS: frozenset[str] = frozenset({"small_table"})


@dataclass(frozen=True)
class PolicyValidationIssue:
    """Single policy validation issue.

    Parameters
    ----------
    code
        Machine-readable error code.
    severity
        Issue severity level.
    task
        Task or view name associated with the issue.
    detail
        Human-readable detail about the issue.
    """

    code: str
    severity: Literal["error", "warn"]
    task: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class PolicyValidationResult:
    """Aggregated policy validation result.

    Parameters
    ----------
    issues
        All validation issues found.
    """

    issues: tuple[PolicyValidationIssue, ...]

    @property
    def is_valid(self) -> bool:
        """Return True when no error-severity issues exist."""
        return not any(i.severity == "error" for i in self.issues)

    @property
    def warnings(self) -> tuple[PolicyValidationIssue, ...]:
        """Return warning-severity issues only."""
        return tuple(i for i in self.issues if i.severity == "warn")

    @property
    def errors(self) -> tuple[PolicyValidationIssue, ...]:
        """Return error-severity issues only."""
        return tuple(i for i in self.issues if i.severity == "error")

    @classmethod
    def from_issues(cls, issues: list[PolicyValidationIssue]) -> PolicyValidationResult:
        """Create a result from a mutable issue list.

        Parameters
        ----------
        issues
            Mutable list of validation issues.

        Returns:
        -------
        PolicyValidationResult
            Immutable validation result.
        """
        return cls(issues=tuple(issues))

    @classmethod
    def empty(cls) -> PolicyValidationResult:
        """Return an empty (passing) validation result."""
        return cls(issues=())


@dataclass(frozen=True)
class PolicyValidationArtifact:
    """Machine-readable policy validation artifact.

    Parameters
    ----------
    validation_mode
        Active validation mode (``"error"``, ``"warn"``, or ``"off"``).
    issue_count
        Total number of issues found.
    error_codes
        Distinct error codes encountered.
    runtime_hash
        Runtime configuration hash for determinism verification.
    is_deterministic
        True when the result is deterministic under the same runtime hash.
    """

    validation_mode: str
    issue_count: int
    error_codes: tuple[str, ...]
    runtime_hash: str
    is_deterministic: bool


def _error(
    code: str,
    *,
    task: str | None = None,
    detail: str | None = None,
) -> PolicyValidationIssue:
    """Create an error-severity validation issue.

    Returns:
    -------
    PolicyValidationIssue
        Issue with ``"error"`` severity.
    """
    return PolicyValidationIssue(code=code, severity="error", task=task, detail=detail)


def _warn(
    code: str,
    *,
    task: str | None = None,
    detail: str | None = None,
) -> PolicyValidationIssue:
    """Create a warning-severity validation issue.

    Returns:
    -------
    PolicyValidationIssue
        Issue with ``"warn"`` severity.
    """
    return PolicyValidationIssue(code=code, severity="warn", task=task, detail=detail)


def validate_policy_bundle(
    execution_plan: ExecutionPlan,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    udf_snapshot: Mapping[str, object],
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None,
    semantic_manifest: SemanticProgramManifest | None = None,
    compiled_policy: CompiledExecutionPolicy | None = None,
) -> PolicyValidationResult:
    """Validate policy bundle against execution plan requirements.

    Run at the execution-plan boundary after ``compile_authority_plan()``,
    NOT inside ``compile_semantic_program()``.

    Parameters
    ----------
    execution_plan
        Compiled execution plan with view nodes, scan units, and metrics.
    runtime_profile
        Active runtime profile with feature gates.
    udf_snapshot
        Available UDF snapshot mapping (name -> UDF object).
    capability_snapshot
        Runtime capabilities payload used for strict provider and metrics checks.
    semantic_manifest
        Optional semantic manifest used to validate scan dataset alignment.
    compiled_policy
        Compile-time-resolved execution policy artifact.  When provided,
        additional consistency checks are run against the manifest and
        capability snapshot.

    Returns:
    -------
    PolicyValidationResult
        Validation result with all issues found.
    """
    issues: list[PolicyValidationIssue] = []
    issues.extend(_udf_feature_gate_issues(execution_plan, runtime_profile=runtime_profile))
    issues.extend(_udf_availability_issues(execution_plan, udf_snapshot=udf_snapshot))
    issues.extend(_delta_protocol_issues(execution_plan))
    issues.extend(
        _manifest_alignment_issues(
            execution_plan,
            semantic_manifest=semantic_manifest,
        )
    )
    issues.extend(_small_scan_policy_issues(execution_plan))
    issues.extend(_capability_issues(capability_snapshot=capability_snapshot))
    issues.extend(
        _scan_policy_compatibility_issues(
            execution_plan,
            capability_snapshot=capability_snapshot,
        )
    )
    if compiled_policy is not None:
        issues.extend(
            _compiled_policy_consistency_issues(
                compiled_policy,
                semantic_manifest=semantic_manifest,
            )
        )
        issues.extend(
            _statistics_availability_issues(
                compiled_policy,
                capability_snapshot=capability_snapshot,
            )
        )
        issues.extend(_evidence_coherence_issues(compiled_policy))
    return PolicyValidationResult.from_issues(issues)


def _udf_feature_gate_issues(
    execution_plan: ExecutionPlan,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    for node in execution_plan.view_nodes:
        required = tuple(node.required_udfs or ())
        if required and not runtime_profile.features.enable_udfs:
            issues.append(
                _error(
                    "UDFS_DISABLED",
                    task=node.name,
                    detail=f"UDFs disabled but required: {sorted(required)}",
                )
            )
    return issues


def _udf_availability_issues(
    execution_plan: ExecutionPlan,
    *,
    udf_snapshot: Mapping[str, object],
) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    for node in execution_plan.view_nodes:
        required = tuple(node.required_udfs or ())
        if not required:
            continue
        missing = sorted(name for name in required if name not in udf_snapshot)
        if missing:
            issues.append(
                _error(
                    "UDF_MISSING",
                    task=node.name,
                    detail=f"Missing UDFs: {missing}",
                )
            )
    return issues


def _delta_protocol_issues(execution_plan: ExecutionPlan) -> list[PolicyValidationIssue]:
    return _delta_protocol_issues_from_signals(_validation_signals_by_task(execution_plan))


def _delta_protocol_issues_from_signals(
    signals_by_task: Mapping[str, PlanSignals],
) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    seen: set[tuple[str, str | None]] = set()
    for task_name in sorted(signals_by_task):
        for compat in signals_by_task[task_name].scan_compat:
            if compat.compatible is not False:
                continue
            key = (compat.dataset_name, compat.reason)
            if key in seen:
                continue
            seen.add(key)
            issues.append(
                _error(
                    "DELTA_PROTOCOL_INCOMPATIBLE",
                    task=compat.dataset_name,
                    detail=compat.reason,
                )
            )
    return issues


def _validation_signals_by_task(execution_plan: ExecutionPlan) -> dict[str, PlanSignals]:
    existing = _existing_signals_by_task(execution_plan)
    if existing:
        return existing
    return _derive_signals_by_task(execution_plan)


def _manifest_alignment_issues(
    execution_plan: ExecutionPlan,
    *,
    semantic_manifest: SemanticProgramManifest | None,
) -> list[PolicyValidationIssue]:
    if semantic_manifest is None:
        return []
    manifest_names = _manifest_dataset_names(semantic_manifest)
    if not manifest_names:
        return []
    manifest_name_set = frozenset(manifest_names)
    terminal_name_set = frozenset(
        name.rsplit(".", 1)[-1] for name in manifest_name_set if isinstance(name, str) and name
    )
    suffix2_name_set = frozenset(
        ".".join(parts[-_DATASET_SUFFIX_SEGMENT_COUNT:])
        for name in manifest_name_set
        if (parts := name.split(".")) and len(parts) >= _DATASET_SUFFIX_SEGMENT_COUNT
    )
    issues: list[PolicyValidationIssue] = []
    seen: set[tuple[str, str]] = set()
    signals_by_task = _validation_signals_by_task(execution_plan)
    for task_name in sorted(signals_by_task):
        dataset_names = tuple(
            sorted(
                {
                    compat.dataset_name
                    for compat in signals_by_task[task_name].scan_compat
                    if isinstance(compat.dataset_name, str) and compat.dataset_name
                }
            )
        )
        for dataset_name in dataset_names:
            if _dataset_name_matches_manifest(
                dataset_name,
                manifest_name_set=manifest_name_set,
                terminal_name_set=terminal_name_set,
                suffix2_name_set=suffix2_name_set,
            ):
                continue
            key = (task_name, dataset_name)
            if key in seen:
                continue
            seen.add(key)
            issues.append(
                _warn(
                    "SCAN_DATASET_NOT_IN_MANIFEST",
                    task=task_name,
                    detail=(
                        f"Scan dataset {dataset_name!r} not found in semantic manifest "
                        "dataset bindings."
                    ),
                )
            )
    return issues


def _manifest_dataset_names(semantic_manifest: SemanticProgramManifest) -> tuple[str, ...]:
    bindings = getattr(semantic_manifest, "dataset_bindings", None)
    if bindings is None:
        return ()
    names: set[str] = set()
    names_fn = getattr(bindings, "names", None)
    if callable(names_fn):
        candidates = names_fn()
        if isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes)):
            names.update(name for name in candidates if isinstance(name, str) and name)
    locations = getattr(bindings, "locations", None)
    if isinstance(locations, Mapping):
        names.update(name for name in locations if isinstance(name, str) and name)
    return tuple(sorted(names))


def _dataset_name_matches_manifest(
    dataset_name: str,
    *,
    manifest_name_set: frozenset[str],
    terminal_name_set: frozenset[str],
    suffix2_name_set: frozenset[str],
) -> bool:
    if dataset_name in manifest_name_set:
        return True
    parts = dataset_name.split(".")
    if not parts:
        return False
    if parts[-1] in terminal_name_set:
        return True
    if len(parts) < _DATASET_SUFFIX_SEGMENT_COUNT:
        return False
    return ".".join(parts[-_DATASET_SUFFIX_SEGMENT_COUNT:]) in suffix2_name_set


def _existing_signals_by_task(execution_plan: ExecutionPlan) -> dict[str, PlanSignals]:
    payload = getattr(execution_plan, "plan_signals_by_task", None)
    if not isinstance(payload, Mapping):
        return {}
    from datafusion_engine.plan.signals import PlanSignals as _PlanSignals

    signals: dict[str, _PlanSignals] = {}
    for task_name in sorted(payload):
        candidate = payload[task_name]
        if not isinstance(candidate, _PlanSignals):
            continue
        signals[str(task_name)] = candidate
    return signals


def _derive_signals_by_task(execution_plan: ExecutionPlan) -> dict[str, PlanSignals]:
    from datafusion_engine.plan.signals import extract_plan_signals

    scan_units = _plan_scan_units(execution_plan)
    scan_by_key = {
        key: unit
        for unit in scan_units
        if isinstance((key := getattr(unit, "key", None)), str) and key
    }
    scan_keys_by_task = _plan_scan_keys_by_task(execution_plan)
    view_nodes = _plan_view_nodes(execution_plan)
    signals: dict[str, PlanSignals] = {}
    for node in sorted(view_nodes, key=lambda item: str(getattr(item, "name", ""))):
        name = getattr(node, "name", None)
        bundle = getattr(node, "plan_bundle", None)
        if not isinstance(name, str) or not name or bundle is None:
            continue
        keys = scan_keys_by_task.get(name, ())
        scoped_units = cast(
            "tuple[ScanUnit, ...]",
            tuple(scan_by_key[key] for key in keys if key in scan_by_key),
        )
        try:
            signals[name] = extract_plan_signals(bundle, scan_units=scoped_units)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            continue
    return signals


def _plan_view_nodes(execution_plan: ExecutionPlan) -> tuple[object, ...]:
    payload = getattr(execution_plan, "view_nodes", ())
    if isinstance(payload, Sequence):
        return tuple(payload)
    return ()


def _plan_scan_units(execution_plan: ExecutionPlan) -> tuple[object, ...]:
    payload = getattr(execution_plan, "scan_units", ())
    if isinstance(payload, Sequence):
        return tuple(payload)
    return ()


def _plan_scan_keys_by_task(
    execution_plan: ExecutionPlan,
) -> Mapping[str, tuple[str, ...]]:
    payload = getattr(execution_plan, "scan_keys_by_task", None)
    if isinstance(payload, Mapping):
        normalized: dict[str, tuple[str, ...]] = {}
        for task_name in sorted(payload):
            keys = payload[task_name]
            if (
                isinstance(task_name, str)
                and isinstance(keys, Sequence)
                and not isinstance(keys, (str, bytes))
            ):
                normalized[task_name] = tuple(key for key in keys if isinstance(key, str))
        return normalized
    return {}


def _small_scan_policy_issues(execution_plan: ExecutionPlan) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    for task_name, metric in execution_plan.task_plan_metrics.items():
        if (
            metric.stats_row_count is not None
            and metric.stats_row_count < _SMALL_SCAN_ROW_THRESHOLD
        ):
            issues.append(
                _warn(
                    "SMALL_INPUT_SCAN_POLICY",
                    task=task_name,
                    detail=f"Row count {metric.stats_row_count} < {_SMALL_SCAN_ROW_THRESHOLD}",
                )
            )
    return issues


def _capability_issues(
    *,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    capability_payload = _capability_payload(capability_snapshot)
    strict_native_provider = _bool_value(capability_payload, "strict_native_provider_enabled")
    delta_payload = _mapping_value(capability_payload, "delta") or {}
    delta_compatible = _bool_value(delta_payload, "compatible")
    delta_error = _string_value(delta_payload, "error")
    if strict_native_provider is True and delta_compatible is False:
        detail = (
            f"Strict native provider is enabled and delta extension is incompatible: "
            f"{delta_error or 'unknown reason'}"
        )
        issues.append(_error("DELTA_EXTENSION_INCOMPATIBLE", detail=detail))

    execution_metrics = _mapping_value(capability_payload, "execution_metrics")
    if execution_metrics is None:
        issues.append(
            _warn(
                "RUNTIME_EXECUTION_METRICS_UNAVAILABLE",
                detail="Runtime execution metrics snapshot is unavailable.",
            )
        )
        return issues

    metrics_error = _string_value(execution_metrics, "error")
    if metrics_error is not None:
        issues.append(
            _warn(
                "RUNTIME_EXECUTION_METRICS_UNAVAILABLE",
                detail=f"Runtime execution metrics probe failed: {metrics_error}",
            )
        )
    return issues


def _compiled_policy_consistency_issues(
    compiled_policy: CompiledExecutionPolicy,
    *,
    semantic_manifest: SemanticProgramManifest | None,
) -> list[PolicyValidationIssue]:
    if semantic_manifest is None:
        return []
    issues: list[PolicyValidationIssue] = []
    manifest_view_names = _manifest_view_names(semantic_manifest)
    if manifest_view_names is not None and compiled_policy.cache_policy_by_view:
        extra_views = sorted(
            set(compiled_policy.cache_policy_by_view) - manifest_view_names,
        )
        issues.extend(
            _warn(
                "compiled_policy_extra_cache_views",
                task=view_name,
                detail=(
                    f"Cache policy references view {view_name!r} not present in semantic manifest."
                ),
            )
            for view_name in extra_views
        )
    manifest_dataset_name_set = frozenset(_manifest_dataset_names(semantic_manifest))
    if manifest_dataset_name_set and compiled_policy.scan_policy_overrides:
        extra_datasets = sorted(
            set(compiled_policy.scan_policy_overrides) - manifest_dataset_name_set,
        )
        issues.extend(
            _warn(
                "compiled_policy_extra_scan_overrides",
                task=dataset_name,
                detail=(
                    f"Scan policy override references dataset {dataset_name!r} "
                    "not present in semantic manifest."
                ),
            )
            for dataset_name in extra_datasets
        )
    return issues


def _statistics_availability_issues(
    compiled_policy: CompiledExecutionPolicy,
    *,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    has_stats_dependent = _has_statistics_dependent_overrides(compiled_policy)
    if has_stats_dependent:
        has_plan_stats = _capability_has_plan_statistics(capability_snapshot)
        if has_plan_stats is False:
            issues.append(
                _warn(
                    "stats_dependent_override_without_capabilities",
                    detail=(
                        "Scan policy overrides reference statistics-derived reasons "
                        "but capability snapshot does not confirm "
                        "has_execution_plan_statistics."
                    ),
                )
            )
    return issues


def _evidence_coherence_issues(
    compiled_policy: CompiledExecutionPolicy,
) -> list[PolicyValidationIssue]:
    issues: list[PolicyValidationIssue] = []
    if compiled_policy.policy_fingerprint is None:
        issues.append(
            _warn(
                "compiled_policy_missing_fingerprint",
                detail="Compiled policy lacks a policy_fingerprint for determinism.",
            )
        )
    has_other_sections = bool(
        compiled_policy.scan_policy_overrides
        or compiled_policy.udf_requirements_by_view
        or compiled_policy.diagnostics_flags
        or compiled_policy.maintenance_policy_by_dataset
    )
    if not compiled_policy.cache_policy_by_view and has_other_sections:
        issues.append(
            _warn(
                "compiled_policy_empty_cache_section",
                detail=(
                    "Compiled policy has populated non-cache sections but "
                    "cache_policy_by_view is empty, suggesting incomplete compilation."
                ),
            )
        )
    return issues


def _scan_policy_compatibility_issues(
    execution_plan: ExecutionPlan,
    *,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> list[PolicyValidationIssue]:
    """Check scan policy overrides are compatible with runtime capabilities.

    Verify that stats-dependent overrides (e.g. ``small_table``) are backed
    by a capability snapshot that confirms ``has_execution_plan_statistics``.
    Also flags overrides that reference datasets not present in the plan's
    scan units.

    Parameters
    ----------
    execution_plan
        Compiled execution plan with plan signals and scan units.
    capability_snapshot
        Runtime capabilities payload.

    Returns:
    -------
    list[PolicyValidationIssue]
        Compatibility issues found.
    """
    issues: list[PolicyValidationIssue] = []
    signals_by_task = _validation_signals_by_task(execution_plan)
    if not signals_by_task:
        return issues

    has_plan_stats = _capability_has_plan_statistics(capability_snapshot)
    seen_stats_tasks: set[str] = set()

    for task_name in sorted(signals_by_task):
        if task_name in seen_stats_tasks:
            continue
        signals = signals_by_task[task_name]
        if signals.lineage is None:
            continue
        reasons = _infer_override_reasons_for_scan(signals)
        if not reasons:
            continue
        has_stats_reason = any(
            r in _PLAN_STATS_DEPENDENT_REASONS
            or any(kw in r.lower() for kw in _STATS_REASON_KEYWORDS)
            for r in reasons
        )
        if not has_stats_reason or has_plan_stats is not False:
            continue
        # Report against the first scan dataset for context.
        first_dataset = (
            signals.lineage.scans[0].dataset_name if signals.lineage.scans else task_name
        )
        seen_stats_tasks.add(task_name)
        issues.append(
            _warn(
                "scan_override_stats_without_capabilities",
                task=task_name,
                detail=(
                    f"Scan override for {first_dataset!r} uses "
                    "statistics-dependent reasons but capability "
                    "snapshot does not confirm "
                    "has_execution_plan_statistics."
                ),
            )
        )
    return issues


def _infer_override_reasons_for_scan(
    signals: PlanSignals,
) -> tuple[str, ...]:
    """Extract inferred override reasons from plan signals.

    Returns:
    -------
    tuple[str, ...]
        Reason strings that would appear in a ``ScanPolicyOverride``.
    """
    reasons: list[str] = []
    if signals.stats is not None and signals.stats.num_rows is not None:
        from relspec.table_size_tiers import TableSizeTier, classify_table_size

        if classify_table_size(int(signals.stats.num_rows)) is TableSizeTier.SMALL:
            reasons.append("small_table")
    if signals.lineage is not None:
        for scan in signals.lineage.scans:
            if scan.pushed_filters:
                reasons.append("has_pushed_filters")
                break
    return tuple(reasons)


def _manifest_view_names(
    semantic_manifest: SemanticProgramManifest,
) -> frozenset[str] | None:
    ir = getattr(semantic_manifest, "semantic_ir", None)
    if ir is None:
        return None
    views = getattr(ir, "views", None)
    if views is None:
        return None
    names: set[str] = set()
    for view in views:
        name = getattr(view, "name", None)
        if isinstance(name, str) and name:
            names.add(name)
    return frozenset(names) if names else None


def _has_statistics_dependent_overrides(
    compiled_policy: CompiledExecutionPolicy,
) -> bool:
    for override_value in compiled_policy.scan_policy_overrides.values():
        reasons = _extract_reasons_from_override(override_value)
        for reason in reasons:
            reason_lower = reason.lower()
            if any(kw in reason_lower for kw in _STATS_REASON_KEYWORDS):
                return True
    return False


def _extract_reasons_from_override(override_value: object) -> tuple[str, ...]:
    if isinstance(override_value, Mapping):
        raw = override_value.get("reasons", ())
        if isinstance(raw, (tuple, list)):
            return tuple(r for r in raw if isinstance(r, str))
        return ()
    reasons = getattr(override_value, "reasons", None)
    if isinstance(reasons, (tuple, list)):
        return tuple(r for r in reasons if isinstance(r, str))
    return ()


def _capability_has_plan_statistics(
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> bool | None:
    """Resolve ``has_execution_plan_statistics`` from the capability snapshot.

    Returns:
    -------
    bool | None
        True/False when resolvable, None when not available.
    """
    if capability_snapshot is None:
        return None
    # First try structured attribute access.
    plan_caps = getattr(capability_snapshot, "plan_capabilities", None)
    if plan_caps is not None:
        value = getattr(plan_caps, "has_execution_plan_statistics", None)
        if isinstance(value, bool):
            return value
    # Fall back to mapping-based access.
    if isinstance(capability_snapshot, Mapping):
        plan_caps_map = capability_snapshot.get("plan_capabilities")
        if isinstance(plan_caps_map, Mapping):
            value = plan_caps_map.get("has_execution_plan_statistics")
            if isinstance(value, bool):
                return value
    return None


def _capability_payload(
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None,
) -> Mapping[str, object]:
    if capability_snapshot is None:
        return {}
    if isinstance(capability_snapshot, Mapping):
        return capability_snapshot
    payload: dict[str, object] = {
        "strict_native_provider_enabled": capability_snapshot.strict_native_provider_enabled,
        "execution_metrics": capability_snapshot.execution_metrics,
    }
    delta_payload: dict[str, object] = {
        "compatible": capability_snapshot.delta.compatible,
        "error": capability_snapshot.delta.error,
    }
    payload["delta"] = delta_payload
    return payload


def _mapping_value(payload: Mapping[str, object], key: str) -> Mapping[str, object] | None:
    value = payload.get(key)
    if isinstance(value, Mapping):
        return value
    return None


def _bool_value(payload: Mapping[str, object], key: str) -> bool | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    return None


def _string_value(payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def build_policy_validation_artifact(
    result: PolicyValidationResult,
    *,
    validation_mode: str,
    runtime_hash: str,
) -> PolicyValidationArtifact:
    """Build a machine-readable artifact from a validation result.

    Parameters
    ----------
    result
        Aggregated validation result from ``validate_policy_bundle()``.
    validation_mode
        Active validation mode (``"error"``, ``"warn"``, or ``"off"``).
    runtime_hash
        Runtime configuration hash for determinism verification.

    Returns:
    -------
    PolicyValidationArtifact
        Serializable artifact summarizing the validation outcome.
    """
    error_codes = tuple(sorted({i.code for i in result.issues if i.severity == "error"}))
    return PolicyValidationArtifact(
        validation_mode=validation_mode,
        issue_count=len(result.issues),
        error_codes=error_codes,
        runtime_hash=runtime_hash,
        is_deterministic=True,
    )


__all__ = [
    "PolicyValidationArtifact",
    "PolicyValidationIssue",
    "PolicyValidationResult",
    "build_policy_validation_artifact",
    "validate_policy_bundle",
]
