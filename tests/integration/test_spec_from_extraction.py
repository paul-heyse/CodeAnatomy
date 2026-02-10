"""Integration test: extraction outputs -> valid execution spec."""

from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.skip(reason="Requires full extraction + spec builder infrastructure")
class TestSpecFromExtraction:
    """Extraction outputs to valid spec integration tests."""

    def test_extraction_delta_to_spec(self) -> None:
        """Verify extraction Delta outputs produce a valid SemanticExecutionSpec."""

    def test_spec_view_definitions_populated(self) -> None:
        """Verify spec has non-empty view_definitions from semantic IR."""
