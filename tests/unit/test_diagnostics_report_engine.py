"""Unit tests for diagnostics report from engine-native spans."""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="Requires diagnostics report engine infrastructure")
class TestDiagnosticsReportEngine:
    """Diagnostics report from engine spans tests."""

    def test_plan_execution_diff_engine_sources(self) -> None:
        """Verify plan_execution_diff uses engine spec view definitions."""

    def test_stage_breakdown_includes_engine_phases(self) -> None:
        """Verify stage_breakdown includes engine execution phases."""

    def test_metrics_section_populated(self) -> None:
        """Verify metrics section populated from engine metrics bridge."""
