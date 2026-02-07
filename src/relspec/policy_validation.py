"""Structured policy validation for execution plan compilation.

Validate runtime policy bundles against plan requirements at the
execution-plan boundary. Run after ``compile_execution_plan()``,
NOT inside ``compile_semantic_program()`` which returns only
``SemanticProgramManifest`` and has no execution plan.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datafusion_engine.extensions.runtime_capabilities import RuntimeCapabilitiesSnapshot
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from relspec.execution_plan import ExecutionPlan

_SMALL_SCAN_ROW_THRESHOLD: int = 1000


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
) -> PolicyValidationResult:
    """Validate policy bundle against execution plan requirements.

    Run at the execution-plan boundary after ``compile_execution_plan()``,
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

    Returns:
    -------
    PolicyValidationResult
        Validation result with all issues found.
    """
    issues: list[PolicyValidationIssue] = []
    issues.extend(_udf_feature_gate_issues(execution_plan, runtime_profile=runtime_profile))
    issues.extend(_udf_availability_issues(execution_plan, udf_snapshot=udf_snapshot))
    issues.extend(_delta_protocol_issues(execution_plan))
    issues.extend(_small_scan_policy_issues(execution_plan))
    issues.extend(_capability_issues(capability_snapshot=capability_snapshot))
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
    issues: list[PolicyValidationIssue] = []
    for unit in execution_plan.scan_units:
        compat = unit.protocol_compatibility
        if compat is not None and compat.compatible is False:
            reason = compat.reason if hasattr(compat, "reason") else None
            issues.append(
                _error(
                    "DELTA_PROTOCOL_INCOMPATIBLE",
                    task=unit.dataset_name,
                    detail=reason,
                )
            )
    return issues


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
