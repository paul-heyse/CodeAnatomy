"""Migration-safe API contract tests for execution context extensions.

Verify backward compatibility, validation behavior, property delegation,
immutability, and determinism of ExecutionAuthorityContext and its
composition with SemanticExecutionContext.

Plan reference: docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md, Section 8.5
"""

from __future__ import annotations

import logging
from dataclasses import FrozenInstanceError
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from relspec.errors import RelspecExecutionAuthorityError
from relspec.execution_authority import (
    ExecutionAuthorityContext,
    ExecutionAuthorityEnforcement,
    ExecutionAuthorityValidationIssue,
)
from semantics.compile_context import SemanticExecutionContext
from semantics.program_manifest import ManifestDatasetBindings, SemanticProgramManifest
from tests.test_helpers.immutability import assert_immutable_assignment

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datafusion_engine.extensions.runtime_capabilities import (
        RuntimeCapabilitiesSnapshot,
    )
    from extract.coordination.evidence_plan import EvidencePlan

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _stub_semantic_ir() -> Any:
    ir = MagicMock()
    ir.views = ()
    ir.join_groups = ()
    ir.model_hash = "stub_model_hash"
    ir.ir_hash = "stub_ir_hash"
    return ir


def _stub_manifest() -> SemanticProgramManifest:
    return SemanticProgramManifest(
        semantic_ir=_stub_semantic_ir(),
        requested_outputs=(),
        input_mapping={},
        validation_policy="schema_only",
        dataset_bindings=ManifestDatasetBindings(locations={}),
    )


def _stub_dataset_resolver() -> ManifestDatasetBindings:
    return ManifestDatasetBindings(locations={})


@pytest.fixture
def manifest() -> SemanticProgramManifest:
    """Provide a minimal semantic program manifest.

    Returns:
    -------
    SemanticProgramManifest
        Stub manifest with empty outputs and no validation.
    """
    return _stub_manifest()


@pytest.fixture
def dataset_resolver() -> ManifestDatasetBindings:
    """Provide a minimal dataset resolver.

    Returns:
    -------
    ManifestDatasetBindings
        Empty dataset bindings.
    """
    return _stub_dataset_resolver()


@pytest.fixture
def semantic_context(
    manifest: SemanticProgramManifest,
    dataset_resolver: ManifestDatasetBindings,
) -> SemanticExecutionContext:
    """Provide a SemanticExecutionContext with stub dependencies.

    Returns:
    -------
    SemanticExecutionContext
        Context backed by mock runtime profile and session context.
    """
    runtime_profile = MagicMock()
    ctx = MagicMock()
    return SemanticExecutionContext(
        manifest=manifest,
        dataset_resolver=dataset_resolver,
        runtime_profile=runtime_profile,
        ctx=ctx,
    )


@pytest.fixture
def evidence_plan_mock() -> EvidencePlan:
    """Provide a mock EvidencePlan with controllable adapter keys.

    Returns:
    -------
    EvidencePlan
        Mock plan returning ('ast', 'cst') as required adapter keys.
    """
    plan: Any = MagicMock()
    plan.required_adapter_keys.return_value = ("ast", "cst")
    return plan


# ---------------------------------------------------------------------------
# 1. Backward compatibility: all optional fields default correctly
# ---------------------------------------------------------------------------


class TestExecutionAuthorityBackwardCompatibility:
    """Verify ExecutionAuthorityContext backward-compatible construction."""

    def test_construct_with_only_semantic_context(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Construct with only the required field."""
        authority = ExecutionAuthorityContext(semantic_context=semantic_context)
        assert authority.semantic_context is semantic_context

    def test_evidence_plan_defaults_to_none(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify evidence_plan defaults to None."""
        authority = ExecutionAuthorityContext(semantic_context=semantic_context)
        assert authority.evidence_plan is None

    def test_extract_executor_map_defaults_to_none(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify extract_executor_map defaults to None."""
        authority = ExecutionAuthorityContext(semantic_context=semantic_context)
        assert authority.extract_executor_map is None

    def test_capability_snapshot_defaults_to_none(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify capability_snapshot defaults to None."""
        authority = ExecutionAuthorityContext(semantic_context=semantic_context)
        assert authority.capability_snapshot is None

    def test_session_runtime_fingerprint_defaults_to_none(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify session_runtime_fingerprint defaults to None."""
        authority = ExecutionAuthorityContext(semantic_context=semantic_context)
        assert authority.session_runtime_fingerprint is None

    def test_enforcement_mode_defaults_to_warn(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify enforcement_mode defaults to 'warn'."""
        authority = ExecutionAuthorityContext(semantic_context=semantic_context)
        assert authority.enforcement_mode == "warn"


# ---------------------------------------------------------------------------
# 2. Validation behavior
# ---------------------------------------------------------------------------


class TestExecutionAuthorityValidation:
    """Verify validation_issues() codes and enforcement modes."""

    def test_missing_executor_map_when_evidence_plan_set(
        self,
        semantic_context: SemanticExecutionContext,
        evidence_plan_mock: EvidencePlan,
    ) -> None:
        """Produce missing_executor_map when evidence_plan set without executor map."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            evidence_plan=evidence_plan_mock,
            extract_executor_map=None,
        )
        codes = {issue.code for issue in authority.validation_issues()}
        assert "missing_executor_map" in codes

    def test_missing_runtime_fingerprint_when_none(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Produce missing_runtime_fingerprint when fingerprint is None."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint=None,
        )
        codes = {issue.code for issue in authority.validation_issues()}
        assert "missing_runtime_fingerprint" in codes

    def test_missing_runtime_fingerprint_when_empty_string(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Produce missing_runtime_fingerprint when fingerprint is empty."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="",
        )
        codes = {issue.code for issue in authority.validation_issues()}
        assert "missing_runtime_fingerprint" in codes

    def test_no_missing_runtime_fingerprint_when_present(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Skip missing_runtime_fingerprint when fingerprint is non-empty."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="abc123",
        )
        codes = {issue.code for issue in authority.validation_issues()}
        assert "missing_runtime_fingerprint" not in codes

    def test_error_mode_raises_on_validation_issues(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Raise RelspecExecutionAuthorityError in error mode with issues."""
        with pytest.raises(RelspecExecutionAuthorityError):
            ExecutionAuthorityContext(
                semantic_context=semantic_context,
                session_runtime_fingerprint=None,
                enforcement_mode="error",
            )

    def test_warn_mode_logs_but_does_not_raise(
        self,
        semantic_context: SemanticExecutionContext,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Log warning in warn mode but allow construction to succeed."""
        with caplog.at_level(logging.WARNING, logger="relspec.execution_authority"):
            authority = ExecutionAuthorityContext(
                semantic_context=semantic_context,
                session_runtime_fingerprint=None,
                enforcement_mode="warn",
            )
        assert authority is not None
        assert any("validation warnings" in record.message.lower() for record in caplog.records)

    def test_no_issues_when_fully_specified(
        self,
        semantic_context: SemanticExecutionContext,
        evidence_plan_mock: EvidencePlan,
    ) -> None:
        """Produce no validation issues when all requirements are satisfied."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            evidence_plan=evidence_plan_mock,
            extract_executor_map={"ast": MagicMock(), "cst": MagicMock()},
            capability_snapshot={"delta_compatible": True},
            session_runtime_fingerprint="fp_abc123",
        )
        assert authority.validation_issues() == ()

    def test_missing_required_adapters_issue(
        self,
        semantic_context: SemanticExecutionContext,
        evidence_plan_mock: EvidencePlan,
    ) -> None:
        """Produce missing_required_adapters when executor map lacks keys."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            evidence_plan=evidence_plan_mock,
            extract_executor_map={"ast": MagicMock()},
            session_runtime_fingerprint="fp_abc123",
        )
        codes = {issue.code for issue in authority.validation_issues()}
        assert "missing_required_adapters" in codes


# ---------------------------------------------------------------------------
# 3. Property delegation and identity
# ---------------------------------------------------------------------------


class TestExecutionAuthorityPropertyDelegation:
    """Verify property delegation to semantic_context preserves identity."""

    def test_manifest_delegates_to_semantic_context(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify manifest property delegates to semantic_context.manifest."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="fp",
        )
        assert authority.manifest is semantic_context.manifest

    def test_dataset_resolver_delegates_to_semantic_context(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify dataset_resolver delegates to semantic_context.dataset_resolver."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="fp",
        )
        assert authority.dataset_resolver is semantic_context.dataset_resolver

    def test_manifest_identity_preserved(self, semantic_context: SemanticExecutionContext) -> None:
        """Verify manifest id() is the same object (no copy)."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="fp",
        )
        assert id(authority.manifest) == id(semantic_context.manifest)

    def test_dataset_resolver_identity_preserved(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Verify dataset_resolver id() is the same object (no copy)."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="fp",
        )
        assert id(authority.dataset_resolver) == id(semantic_context.dataset_resolver)


# ---------------------------------------------------------------------------
# 4. Frozen immutability
# ---------------------------------------------------------------------------


class TestExecutionAuthorityImmutability:
    """Verify frozen dataclass semantics for both context types."""

    def test_execution_authority_is_frozen(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Raise FrozenInstanceError when setting enforcement_mode."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="fp",
        )
        assert_immutable_assignment(
            factory=lambda: authority,
            attribute="enforcement_mode",
            attempted_value="error",
            expected_exception=FrozenInstanceError,
        )

    def test_execution_authority_semantic_context_not_reassignable(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Raise FrozenInstanceError when reassigning semantic_context."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint="fp",
        )
        assert_immutable_assignment(
            factory=lambda: authority,
            attribute="semantic_context",
            attempted_value=semantic_context,
            expected_exception=FrozenInstanceError,
        )

    def test_semantic_execution_context_is_frozen(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Raise FrozenInstanceError when setting manifest on SemanticExecutionContext."""
        assert_immutable_assignment(
            factory=lambda: semantic_context,
            attribute="manifest",
            attempted_value=_stub_manifest(),
            expected_exception=FrozenInstanceError,
        )


# ---------------------------------------------------------------------------
# 5. Validation determinism
# ---------------------------------------------------------------------------


class TestValidationDeterminism:
    """Verify that validation_issues() produces deterministic results."""

    def test_same_inputs_same_order(
        self,
        semantic_context: SemanticExecutionContext,
        evidence_plan_mock: EvidencePlan,
    ) -> None:
        """Produce identical issues in the same order for the same context."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            evidence_plan=evidence_plan_mock,
            extract_executor_map=None,
            session_runtime_fingerprint=None,
        )
        first = authority.validation_issues()
        second = authority.validation_issues()
        assert first == second

    def test_multiple_calls_return_identical_results(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Return stable results across repeated calls without mutation."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            session_runtime_fingerprint=None,
        )
        results = [authority.validation_issues() for _ in range(5)]
        assert all(r == results[0] for r in results)

    def test_issues_are_sorted_by_code(
        self,
        semantic_context: SemanticExecutionContext,
        evidence_plan_mock: EvidencePlan,
    ) -> None:
        """Sort multiple issues alphabetically by code."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            evidence_plan=evidence_plan_mock,
            extract_executor_map=None,
            session_runtime_fingerprint=None,
        )
        issues = authority.validation_issues()
        codes = [issue.code for issue in issues]
        assert codes == sorted(codes)


# ---------------------------------------------------------------------------
# 6. capability_snapshot accepts multiple types
# ---------------------------------------------------------------------------


class TestCapabilitySnapshotPolymorphism:
    """Verify capability_snapshot accepts RuntimeCapabilitiesSnapshot and Mapping."""

    def test_accepts_plain_mapping(self, semantic_context: SemanticExecutionContext) -> None:
        """Accept a plain dict without validation issues from snapshot type."""
        snapshot: Mapping[str, object] = {"delta_compatible": True, "version": "1.0"}
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            capability_snapshot=snapshot,
            session_runtime_fingerprint="fp_abc123",
        )
        assert authority.capability_snapshot == snapshot
        assert authority.validation_issues() == ()

    def test_accepts_runtime_capabilities_snapshot(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Accept a RuntimeCapabilitiesSnapshot mock."""
        snapshot: RuntimeCapabilitiesSnapshot = MagicMock()
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            capability_snapshot=snapshot,
            session_runtime_fingerprint="fp_abc123",
        )
        assert authority.capability_snapshot is snapshot
        assert authority.validation_issues() == ()

    def test_none_snapshot_produces_missing_issue(
        self, semantic_context: SemanticExecutionContext
    ) -> None:
        """Produce missing_capability_snapshot when capability_snapshot is None."""
        authority = ExecutionAuthorityContext(
            semantic_context=semantic_context,
            capability_snapshot=None,
            session_runtime_fingerprint="fp_abc123",
        )
        codes = {issue.code for issue in authority.validation_issues()}
        assert "missing_capability_snapshot" in codes


# ---------------------------------------------------------------------------
# 7. Enforcement mode type literal
# ---------------------------------------------------------------------------


class TestEnforcementModeLiteral:
    """Verify the enforcement mode literal type values."""

    @pytest.mark.parametrize("mode", ["warn", "error"])
    def test_valid_enforcement_modes(
        self, semantic_context: SemanticExecutionContext, mode: ExecutionAuthorityEnforcement
    ) -> None:
        """Accept both 'warn' and 'error' as valid enforcement modes."""
        if mode == "error":
            # Will raise due to missing fingerprint, but that is expected
            with pytest.raises(RelspecExecutionAuthorityError):
                ExecutionAuthorityContext(
                    semantic_context=semantic_context,
                    enforcement_mode=mode,
                )
        else:
            authority = ExecutionAuthorityContext(
                semantic_context=semantic_context,
                enforcement_mode=mode,
            )
            assert authority.enforcement_mode == mode


# ---------------------------------------------------------------------------
# 8. ValidationIssue immutability
# ---------------------------------------------------------------------------


class TestValidationIssueImmutability:
    """Verify ExecutionAuthorityValidationIssue is frozen."""

    def test_issue_is_frozen(self) -> None:
        """Raise FrozenInstanceError when setting code on a validation issue."""
        issue = ExecutionAuthorityValidationIssue(
            code="test_code",
            message="test message",
        )
        assert_immutable_assignment(
            factory=lambda: issue,
            attribute="code",
            attempted_value="other",
            expected_exception=FrozenInstanceError,
        )

    def test_issue_fields(self) -> None:
        """Verify code and message fields are preserved."""
        issue = ExecutionAuthorityValidationIssue(
            code="missing_runtime_fingerprint",
            message="session_runtime_fingerprint must be a non-empty string.",
        )
        assert issue.code == "missing_runtime_fingerprint"
        assert "non-empty string" in issue.message
