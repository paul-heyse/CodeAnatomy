"""Parity tests: engine outputs match expected schemas and shapes."""

from __future__ import annotations

from engine.output_contracts import (
    CANONICAL_CPG_OUTPUTS,
    ENGINE_CPG_OUTPUTS,
    FULL_PIPELINE_OUTPUTS,
    ORCHESTRATOR_OUTPUTS,
    PYTHON_AUXILIARY_OUTPUTS,
)


class TestOutputContractParity:
    """Engine output contract parity tests."""

    def test_engine_cpg_output_count(self) -> None:
        """Verify 6 CPG outputs from Rust engine."""
        assert len(ENGINE_CPG_OUTPUTS) == 6

    def test_python_auxiliary_output_count(self) -> None:
        """Verify 3 Python auxiliary outputs."""
        assert len(PYTHON_AUXILIARY_OUTPUTS) == 3

    def test_orchestrator_output_count(self) -> None:
        """Verify 1 orchestrator output."""
        assert len(ORCHESTRATOR_OUTPUTS) == 1

    def test_full_pipeline_output_count(self) -> None:
        """Verify total 10 pipeline outputs."""
        assert len(FULL_PIPELINE_OUTPUTS) == 10

    def test_all_cpg_outputs_use_cpg_prefix(self) -> None:
        """Verify canonical CPG outputs use cpg_* naming."""
        for name in ENGINE_CPG_OUTPUTS:
            assert name.startswith("cpg_"), f"{name} missing cpg_* prefix"

    def test_engine_outputs_are_canonical(self) -> None:
        """Verify ENGINE_CPG_OUTPUTS is the canonical CPG output set."""
        assert ENGINE_CPG_OUTPUTS == CANONICAL_CPG_OUTPUTS

    def test_no_duplicate_output_names(self) -> None:
        """Verify no duplicate output names across all categories."""
        all_names = list(FULL_PIPELINE_OUTPUTS)
        assert len(all_names) == len(set(all_names)), "Duplicate output names found"
