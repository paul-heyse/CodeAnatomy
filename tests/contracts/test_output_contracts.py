"""Output contract tests for the engine execution pipeline.

Verify that output contracts are consistent and complete.
"""

from __future__ import annotations

import pytest

from engine.output_contracts import (
    ENGINE_CPG_OUTPUTS,
    FULL_PIPELINE_OUTPUTS,
    ORCHESTRATOR_OUTPUTS,
    OUTPUT_SOURCE_MAP,
    PYTHON_AUXILIARY_OUTPUTS,
)


class TestOutputContractConstants:
    """Verify output contract constant integrity."""

    def test_full_pipeline_outputs_count(self) -> None:
        """FULL_PIPELINE_OUTPUTS has exactly 10 entries."""
        assert len(FULL_PIPELINE_OUTPUTS) == 10

    def test_full_pipeline_outputs_exact_keys(self) -> None:
        """FULL_PIPELINE_OUTPUTS matches the canonical output names."""
        expected = {
            "write_cpg_nodes_delta",
            "write_cpg_edges_delta",
            "write_cpg_props_delta",
            "write_cpg_props_map_delta",
            "write_cpg_edges_by_src_delta",
            "write_cpg_edges_by_dst_delta",
            "write_normalize_outputs_delta",
            "write_extract_error_artifacts_delta",
            "write_run_manifest_delta",
            "write_run_bundle_dir",
        }
        assert set(FULL_PIPELINE_OUTPUTS) == expected

    def test_engine_cpg_outputs_count(self) -> None:
        """ENGINE_CPG_OUTPUTS has exactly 6 Rust-producible entries."""
        assert len(ENGINE_CPG_OUTPUTS) == 6

    def test_python_auxiliary_outputs_count(self) -> None:
        """PYTHON_AUXILIARY_OUTPUTS has exactly 3 Python-side entries."""
        assert len(PYTHON_AUXILIARY_OUTPUTS) == 3

    def test_orchestrator_outputs_count(self) -> None:
        """ORCHESTRATOR_OUTPUTS has exactly 1 orchestrator entry."""
        assert len(ORCHESTRATOR_OUTPUTS) == 1

    def test_no_duplicate_outputs(self) -> None:
        """No output name appears in more than one source category."""
        all_outputs = ENGINE_CPG_OUTPUTS + PYTHON_AUXILIARY_OUTPUTS + ORCHESTRATOR_OUTPUTS
        assert len(all_outputs) == len(set(all_outputs))

    def test_full_equals_union(self) -> None:
        """FULL_PIPELINE_OUTPUTS is the union of all source categories."""
        union = set(ENGINE_CPG_OUTPUTS) | set(PYTHON_AUXILIARY_OUTPUTS) | set(ORCHESTRATOR_OUTPUTS)
        assert set(FULL_PIPELINE_OUTPUTS) == union

    def test_output_source_map_covers_all(self) -> None:
        """OUTPUT_SOURCE_MAP has an entry for every output."""
        assert set(OUTPUT_SOURCE_MAP.keys()) == set(FULL_PIPELINE_OUTPUTS)

    def test_output_source_map_values(self) -> None:
        """OUTPUT_SOURCE_MAP values are valid source names."""
        valid_sources = {"rust_engine", "python_auxiliary", "orchestrator"}
        assert set(OUTPUT_SOURCE_MAP.values()).issubset(valid_sources)


class TestRunResultMapping:
    """Test skeletons for RunResult to GraphProductBuildResult mapping."""

    @pytest.mark.skip(reason="Pending build orchestrator implementation")
    def test_run_result_to_build_result(self) -> None:
        """RunResult dict maps to GraphProductBuildResult fields."""

    @pytest.mark.skip(reason="Pending build orchestrator implementation")
    def test_run_result_contains_all_engine_outputs(self) -> None:
        """RunResult contains entries for all ENGINE_CPG_OUTPUTS."""


class TestCLIOutputContracts:
    """Test skeletons for CLI JSON output field contracts."""

    @pytest.mark.skip(reason="Pending CLI rewrite")
    def test_build_json_output_fields(self) -> None:
        """CLI build --json output contains required fields."""

    @pytest.mark.skip(reason="Pending CLI rewrite")
    def test_plan_json_output_fields(self) -> None:
        """CLI plan --json output contains required fields."""


class TestDiagnosticArtifactContracts:
    """Test skeletons for diagnostic artifact payload schemas."""

    @pytest.mark.skip(reason="Pending obs artifact implementation")
    def test_plan_summary_artifact_schema(self) -> None:
        """EnginePlanSummaryArtifact has required fields."""

    @pytest.mark.skip(reason="Pending obs artifact implementation")
    def test_execution_summary_artifact_schema(self) -> None:
        """EngineExecutionSummaryArtifact has required fields."""
