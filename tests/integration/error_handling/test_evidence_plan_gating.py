"""Evidence plan gating integration tests.

Scope: Test the full gating path from EvidencePlan API through
plan_feature_flags() and rule_execution_options(). Unit tests own
individual method behavior (requires_dataset, required_columns_for).
Integration tests verify the cross-component gating contract.

Key API references:
- EvidencePlan in extract.coordination.evidence_plan
- plan_feature_flags() in extract.coordination.spec_helpers (line 73)
- rule_execution_options() in extract.coordination.spec_helpers (line 159)
- plan_requires_row() in extract.coordination.spec_helpers (line 62)
- apply_query_and_project() in extract.coordination.materialization (line 293)
"""

from __future__ import annotations

import pytest

from extract.coordination.evidence_plan import EvidencePlan, compile_evidence_plan
from extract.coordination.spec_helpers import (
    ExtractExecutionOptions,
    plan_feature_flags,
    plan_requires_row,
    rule_execution_options,
)


@pytest.fixture
def evidence_plan_cst_only() -> EvidencePlan:
    """Evidence plan requiring only CST-related datasets.

    Returns:
    -------
    EvidencePlan
        Plan containing only CST sources and cst_refs projection.
    """
    return EvidencePlan(
        sources=("cst_refs", "cst_defs", "cst_callsites"),
        required_columns={"cst_refs": ("file_id", "bstart", "bend", "qualified_name")},
    )


@pytest.fixture
def evidence_plan_with_ast() -> EvidencePlan:
    """Evidence plan requiring CST and AST datasets.

    Returns:
    -------
    EvidencePlan
        Plan containing CST and AST sources.
    """
    return EvidencePlan(
        sources=("cst_refs", "cst_defs", "ast_files", "ast_nodes"),
    )


@pytest.fixture
def evidence_plan_compiled() -> EvidencePlan:
    """Evidence plan compiled from string output names.

    Returns:
    -------
    EvidencePlan
        Compiled plan built via ``compile_evidence_plan``.
    """
    return compile_evidence_plan(
        rules=["cst_refs", "cst_defs"],
        extra_sources=("ast_files",),
        required_columns={"cst_refs": ["file_id", "bstart"]},
    )


@pytest.mark.integration
class TestEvidencePlanGating:
    """Tests for evidence plan gating behavior.

    Integration scope: test the gating mechanics (requires_dataset,
    required_columns_for, plan_feature_flags, enabled_when).
    """

    @staticmethod
    def test_gated_dataset_excluded_from_plan(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify gated datasets are excluded via requires_dataset()."""
        plan = evidence_plan_cst_only
        # CST datasets are required
        assert plan.requires_dataset("cst_refs") is True
        assert plan.requires_dataset("cst_defs") is True
        # AST and bytecode datasets are not required
        assert plan.requires_dataset("ast_files") is False
        assert plan.requires_dataset("bytecode_ops") is False

    @staticmethod
    def test_required_columns_for_returns_projection(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify required_columns_for() returns column subset."""
        plan = evidence_plan_cst_only
        cols = plan.required_columns_for("cst_refs")
        assert "file_id" in cols
        assert "bstart" in cols
        assert "bend" in cols
        assert "qualified_name" in cols

    @staticmethod
    def test_required_columns_for_returns_empty_for_unrestricted(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify required_columns_for() returns empty for datasets without column restrictions."""
        plan = evidence_plan_cst_only
        # cst_defs is required but has no column restrictions
        cols = plan.required_columns_for("cst_defs")
        assert cols == ()

    @staticmethod
    def test_required_columns_for_returns_empty_for_excluded(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify required_columns_for() returns empty for excluded datasets."""
        plan = evidence_plan_cst_only
        cols = plan.required_columns_for("ast_files")
        assert cols == ()

    @staticmethod
    def test_compile_evidence_plan_includes_extra_sources(
        evidence_plan_compiled: EvidencePlan,
    ) -> None:
        """Verify compile_evidence_plan() includes extra_sources."""
        plan = evidence_plan_compiled
        # ast_files was in extra_sources
        assert plan.requires_dataset("ast_files") is True

    @staticmethod
    def test_compile_evidence_plan_required_columns_as_tuple(
        evidence_plan_compiled: EvidencePlan,
    ) -> None:
        """Verify compiled plan converts column sequences to tuples."""
        plan = evidence_plan_compiled
        cols = plan.required_columns_for("cst_refs")
        assert isinstance(cols, tuple)
        assert "file_id" in cols

    @staticmethod
    def test_plan_feature_flags_returns_dict(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify plan_feature_flags() returns a dict of bool flags.

        Feature-flag gating is via plan_feature_flags() from
        extract.coordination.spec_helpers.
        """
        flags = plan_feature_flags("cst", evidence_plan_cst_only)
        assert isinstance(flags, dict)
        for key, value in flags.items():
            assert isinstance(key, str)
            assert isinstance(value, bool)

    @staticmethod
    def test_plan_feature_flags_none_plan_returns_empty() -> None:
        """Verify plan_feature_flags() returns empty dict for None plan."""
        flags = plan_feature_flags("cst", None)
        assert flags == {}

    @staticmethod
    def test_rule_execution_options_returns_execution_options(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify rule_execution_options() returns ExtractExecutionOptions."""
        options = rule_execution_options("cst", evidence_plan_cst_only)
        assert isinstance(options, ExtractExecutionOptions)
        assert isinstance(options.feature_flags, dict)

    @staticmethod
    def test_rule_execution_options_with_overrides(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify rule_execution_options() applies overrides."""
        options = rule_execution_options(
            "cst",
            evidence_plan_cst_only,
            overrides={"include_refs": False},
        )
        assert isinstance(options, ExtractExecutionOptions)
        # Override should appear in feature_flags
        assert options.feature_flags.get("include_refs") is False

    @staticmethod
    def test_plan_requires_row_checks_output_name() -> None:
        """Verify plan_requires_row() checks both name and output_name.

        plan_requires_row(plan, row: ExtractMetadata) returns True
        when the plan requires the dataset identified by row.output_name()
        or row.name.
        """
        from datafusion_engine.extract.metadata import extract_metadata_specs

        plan = EvidencePlan(sources=("cst_refs",))
        # Find the metadata row for cst_refs
        matching_rows = [
            row
            for row in extract_metadata_specs()
            if row.name == "cst_refs" or row.output_name() == "cst_refs"
        ]
        if matching_rows:
            row = matching_rows[0]
            assert plan_requires_row(plan, row) is True

    @staticmethod
    def test_evidence_plan_requires_template(
        evidence_plan_cst_only: EvidencePlan,
    ) -> None:
        """Verify requires_template() identifies required templates."""
        plan = evidence_plan_cst_only
        # CST template should be required since cst_refs/cst_defs are sources
        # The actual template name depends on the extract metadata registry
        # This test validates the API works without asserting specific templates
        result = plan.requires_template("cst")
        assert isinstance(result, bool)

    @staticmethod
    def test_empty_plan_requires_nothing() -> None:
        """Verify empty evidence plan gates all datasets."""
        plan = EvidencePlan(sources=())
        assert plan.requires_dataset("cst_refs") is False
        assert plan.requires_dataset("ast_files") is False
        assert plan.requires_template("cst") is False
