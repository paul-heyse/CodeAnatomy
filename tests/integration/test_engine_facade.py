"""Integration test: spec -> engine -> valid RunResult."""

from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Rust engine")
class TestEngineFacade:
    """Spec to engine to valid RunResult integration tests."""

    def test_spec_to_run_result(self) -> None:
        """Verify SemanticExecutionSpec produces valid RunResult from planning_engine."""

    def test_run_result_has_outputs(self) -> None:
        """Verify RunResult contains output entries."""

    def test_run_result_has_trace_metrics(self) -> None:
        """Verify RunResult contains trace_metrics_summary."""
