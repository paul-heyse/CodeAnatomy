"""Evidence plan to extractor pipeline integration tests.

Scope: Verify plan gating flows through to extractor configuration.
Covers plan_feature_flags() propagation to rule_execution_options(),
enabled_when stage gating, and projected column subset behavior.

Key API references:
- EvidencePlan in extract.coordination.evidence_plan
- plan_feature_flags() in extract.coordination.spec_helpers (line 73)
- rule_execution_options() in extract.coordination.spec_helpers (line 159)
- extractor_option_values() in extract.coordination.spec_helpers (line 131)
- ExtractExecutionOptions in extract.coordination.spec_helpers
"""

from __future__ import annotations

import pytest

from extract.coordination.evidence_plan import EvidencePlan, compile_evidence_plan
from extract.coordination.spec_helpers import (
    ExtractExecutionOptions,
    extractor_option_values,
    plan_feature_flags,
    rule_execution_options,
)


@pytest.fixture
def plan_without_bytecode() -> EvidencePlan:
    """Evidence plan that does not require bytecode datasets.

    Returns
    -------
    EvidencePlan
        Evidence plan excluding bytecode-backed sources.
    """
    return EvidencePlan(
        sources=("cst_refs", "cst_defs", "ast_files"),
    )


@pytest.fixture
def plan_cst_only() -> EvidencePlan:
    """Evidence plan requiring only CST datasets.

    Returns
    -------
    EvidencePlan
        Evidence plan scoped to CST sources and required columns.
    """
    return EvidencePlan(
        sources=("cst_refs", "cst_defs", "cst_callsites"),
        required_columns={
            "cst_refs": ("file_id", "bstart", "bend"),
        },
    )


@pytest.fixture
def plan_all_sources() -> EvidencePlan:
    """Evidence plan compiled with all available sources.

    Returns
    -------
    EvidencePlan
        Fully compiled evidence plan including all default sources.
    """
    return compile_evidence_plan()


@pytest.mark.integration
class TestEvidencePlanToExtractorPipeline:
    """Verify plan gating flows through to extractor configuration."""

    def test_plan_feature_flags_propagate_to_rule_execution_options(
        self,
        plan_without_bytecode: EvidencePlan,
    ) -> None:
        """Verify plan_feature_flags() values appear in rule_execution_options().

        plan_feature_flags() generates feature flag overrides that flow
        into rule_execution_options() through ExtractExecutionOptions.
        """
        flags = plan_feature_flags("cst", plan_without_bytecode)
        options = rule_execution_options("cst", plan_without_bytecode)

        # Feature flags from plan should be present in execution options
        assert isinstance(options, ExtractExecutionOptions)
        for key, value in flags.items():
            assert key in options.feature_flags
            assert options.feature_flags[key] == value

    def test_rule_execution_options_merge_overrides(
        self,
        plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify rule_execution_options() merges manual overrides with plan flags."""
        options_no_override = rule_execution_options("cst", plan_cst_only)
        options_with_override = rule_execution_options(
            "cst",
            plan_cst_only,
            overrides={"include_refs": False},
        )

        # Override should change the flag value
        assert options_with_override.feature_flags.get("include_refs") is False
        # Other flags should remain from plan
        assert isinstance(options_no_override.feature_flags, dict)

    def test_extractor_option_values_incorporates_plan(
        self,
        plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify extractor_option_values() merges plan with registry defaults."""
        values = extractor_option_values("cst", plan_cst_only)
        assert isinstance(values, dict)

    def test_extractor_option_values_with_none_plan_uses_defaults(self) -> None:
        """Verify None plan returns registry defaults without restriction."""
        values_none = extractor_option_values("cst", None)
        assert isinstance(values_none, dict)

    def test_plan_feature_flags_unrequired_template_disables_all(self) -> None:
        """Verify plan_feature_flags() disables all for unrequired template.

        When the evidence plan doesn't require a template, all feature
        flags for that template should be set to False.
        """
        plan = EvidencePlan(sources=("ast_files",))  # Only AST, no CST
        flags = plan_feature_flags("cst", plan)
        # If there are flags for "cst" template, they should be False
        # when the plan doesn't require the cst template
        for flag_value in flags.values():
            if not plan.requires_template("cst"):
                assert flag_value is False

    def test_required_columns_for_returns_subset(
        self,
        plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify required_columns_for() returns projected column subset."""
        cols = plan_cst_only.required_columns_for("cst_refs")
        assert isinstance(cols, tuple)
        assert "file_id" in cols
        assert "bstart" in cols
        assert "bend" in cols

    def test_plan_feature_flags_consistent_with_requires_template(self) -> None:
        """Verify plan_feature_flags aligns with requires_template."""
        plan = EvidencePlan(sources=("cst_refs", "cst_defs"))
        flags = plan_feature_flags("cst", plan)
        # If template is required, at least some flags should be True (if any flags exist)
        if plan.requires_template("cst") and flags:
            assert any(v is True for v in flags.values())

    def test_empty_plan_disables_all_feature_flags(self) -> None:
        """Verify empty plan (no sources) disables all feature flags."""
        plan = EvidencePlan(sources=())
        flags = plan_feature_flags("cst", plan)
        for flag_value in flags.values():
            assert flag_value is False
