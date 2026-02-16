"""Parity tests: engine outputs match expected schemas and shapes."""

from __future__ import annotations

from planning_engine.output_contracts import (
    CANONICAL_CPG_OUTPUTS,
    ENGINE_CPG_OUTPUTS,
    FULL_PIPELINE_OUTPUTS,
    ORCHESTRATOR_OUTPUTS,
    PYTHON_AUXILIARY_OUTPUTS,
)

EXPECTED_ENGINE_OUTPUTS = 6
EXPECTED_AUX_OUTPUTS = 3
EXPECTED_TOTAL_OUTPUTS = 10


class TestOutputContractParity:
    """Engine output contract parity tests."""

    @staticmethod
    def test_engine_cpg_output_count() -> None:
        """Verify 6 CPG outputs from Rust engine."""
        assert len(ENGINE_CPG_OUTPUTS) == EXPECTED_ENGINE_OUTPUTS

    @staticmethod
    def test_python_auxiliary_output_count() -> None:
        """Verify 3 Python auxiliary outputs."""
        assert len(PYTHON_AUXILIARY_OUTPUTS) == EXPECTED_AUX_OUTPUTS

    @staticmethod
    def test_orchestrator_output_count() -> None:
        """Verify 1 orchestrator output."""
        assert len(ORCHESTRATOR_OUTPUTS) == 1

    @staticmethod
    def test_full_pipeline_output_count() -> None:
        """Verify total 10 pipeline outputs."""
        assert len(FULL_PIPELINE_OUTPUTS) == EXPECTED_TOTAL_OUTPUTS

    @staticmethod
    def test_all_cpg_outputs_use_cpg_prefix() -> None:
        """Verify canonical CPG outputs use cpg_* naming."""
        for name in ENGINE_CPG_OUTPUTS:
            assert name.startswith("cpg_"), f"{name} missing cpg_* prefix"

    @staticmethod
    def test_engine_outputs_are_canonical() -> None:
        """Verify ENGINE_CPG_OUTPUTS is the canonical CPG output set."""
        assert ENGINE_CPG_OUTPUTS == CANONICAL_CPG_OUTPUTS

    @staticmethod
    def test_no_duplicate_output_names() -> None:
        """Verify no duplicate output names across all categories."""
        all_names = list(FULL_PIPELINE_OUTPUTS)
        assert len(all_names) == len(set(all_names)), "Duplicate output names found"
