"""Orchestration authority context for execution-layer composition."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from datafusion_engine.extract.adapter_registry import adapter_executor_key
from relspec.errors import RelspecExecutionAuthorityError

_LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from datafusion_engine.extensions.runtime_capabilities import RuntimeCapabilitiesSnapshot
    from extract.coordination.evidence_plan import EvidencePlan
    from relspec.compiled_policy import CompiledExecutionPolicy
    from semantics.compile_context import SemanticExecutionContext
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


ExtractExecutor = Callable[[Any, Any, str], Mapping[str, object]]
ExecutionAuthorityEnforcement = Literal["warn", "error"]


@dataclass(frozen=True)
class ExecutionAuthorityValidationIssue:
    """Validation issue detected in an execution authority context."""

    code: str
    message: str


@dataclass(frozen=True)
class ExecutionAuthorityContext:
    """Orchestration authority context composing semantic + execution concerns.

    Waves 1-2 thread ``SemanticExecutionContext`` (semantic compile artifacts only).
    Waves 3+ build ``ExecutionAuthorityContext`` on top for orchestration fields.

    Parameters
    ----------
    semantic_context
        Compiled semantic execution context (manifest, resolver, runtime profile).
    evidence_plan
        Evidence plan describing required extract datasets.
    extract_executor_map
        Mapping of executor names to executor instances.
    capability_snapshot
        DataFusion runtime capability detection result.
    session_runtime_fingerprint
        Stable fingerprint of the session runtime configuration.
    enforcement_mode
        Validation enforcement mode (``warn`` or ``error``).
    compiled_policy
        Compile-time-resolved execution policy artifact.  Optional during
        rollout; will become required once all callers populate it.
    """

    semantic_context: SemanticExecutionContext
    evidence_plan: EvidencePlan | None = None
    extract_executor_map: Mapping[str, Any] | None = None
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None
    session_runtime_fingerprint: str | None = None
    enforcement_mode: ExecutionAuthorityEnforcement = "warn"
    compiled_policy: CompiledExecutionPolicy | None = None

    def __post_init__(self) -> None:
        """Validate and optionally enforce authority requirements.

        Raises:
            RelspecExecutionAuthorityError: If validation fails in ``error`` mode.
        """
        issues = self.validation_issues()
        if not issues:
            return
        summary = "; ".join(issue.message for issue in issues)
        if self.enforcement_mode == "error":
            raise RelspecExecutionAuthorityError(summary)
        _LOGGER.warning("Execution authority validation warnings: %s", summary)

    @property
    def manifest(self) -> SemanticProgramManifest:
        """Return the semantic program manifest from the semantic context."""
        return self.semantic_context.manifest

    @property
    def dataset_resolver(self) -> ManifestDatasetResolver:
        """Return the dataset resolver from the semantic context."""
        return self.semantic_context.dataset_resolver

    def required_adapter_keys(self) -> tuple[str, ...]:
        """Return required adapter keys derived from the evidence plan."""
        if self.evidence_plan is None:
            return ()
        return self.evidence_plan.required_adapter_keys()

    def missing_adapter_keys(self) -> tuple[str, ...]:
        """Return required adapter keys missing from the executor map."""
        required = set(self.required_adapter_keys())
        if not required:
            return ()
        available = set(self.extract_executor_map or {})
        return tuple(sorted(required - available))

    def resolve_dataset_location(self, dataset_name: str) -> object | None:
        """Resolve a dataset location through the semantic resolver.

        Parameters
        ----------
        dataset_name
            Dataset identifier in the manifest resolver.

        Returns:
        -------
        object | None
            Resolver-specific location object, or ``None`` when unresolved.
        """
        return self.dataset_resolver.location(dataset_name)

    def executor_for_adapter(self, adapter_name: str) -> ExtractExecutor:
        """Return the executor for an adapter template name.

        Parameters
        ----------
        adapter_name
            Adapter template name from the adapter registry.

        Returns:
        -------
        ExtractExecutor
            Registered executor callable.

        Raises:
            ValueError: If no executor is registered for the adapter.
        """
        key = adapter_executor_key(adapter_name)
        executor = (self.extract_executor_map or {}).get(key)
        if callable(executor):
            return cast("ExtractExecutor", executor)
        msg = f"Missing extract executor for adapter {adapter_name!r} (key={key!r})."
        raise ValueError(msg)

    def validation_issues(self) -> tuple[ExecutionAuthorityValidationIssue, ...]:
        """Return deterministic validation issues for the authority context."""
        issues: list[ExecutionAuthorityValidationIssue] = []
        if (
            not isinstance(self.session_runtime_fingerprint, str)
            or not self.session_runtime_fingerprint
        ):
            issues.append(
                ExecutionAuthorityValidationIssue(
                    code="missing_runtime_fingerprint",
                    message="session_runtime_fingerprint must be a non-empty string.",
                )
            )
        if self.evidence_plan is not None and self.extract_executor_map is None:
            issues.append(
                ExecutionAuthorityValidationIssue(
                    code="missing_executor_map",
                    message="extract_executor_map is required when evidence_plan is present.",
                )
            )
        if self.capability_snapshot is None:
            issues.append(
                ExecutionAuthorityValidationIssue(
                    code="missing_capability_snapshot",
                    message="capability_snapshot must be populated for runtime feature gating.",
                )
            )
        missing = self.missing_adapter_keys()
        if missing:
            issues.append(
                ExecutionAuthorityValidationIssue(
                    code="missing_required_adapters",
                    message=f"Missing required extract adapter keys: {', '.join(missing)}",
                )
            )
        return tuple(sorted(issues, key=lambda issue: issue.code))


__all__ = [
    "ExecutionAuthorityContext",
    "ExecutionAuthorityEnforcement",
    "ExecutionAuthorityValidationIssue",
]
