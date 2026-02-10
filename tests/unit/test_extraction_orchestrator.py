"""Unit tests for the extraction orchestrator."""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="Requires extraction infrastructure")
class TestExtractionOrchestrator:
    """Extraction orchestrator unit tests."""

    def test_staged_execution_ordering(self) -> None:
        """Verify stage 0 completes before stage 1."""

    def test_parallel_stage1_extractors(self) -> None:
        """Verify stage 1 extractors run in parallel via ThreadPoolExecutor."""

    def test_error_collection(self) -> None:
        """Verify extraction errors are collected into ExtractionResult.errors."""

    def test_delta_output_locations(self) -> None:
        """Verify Delta output locations are returned in ExtractionResult.delta_locations."""

    def test_timing_recorded(self) -> None:
        """Verify extraction timing is recorded in ExtractionResult.timing."""
