"""E2E test: CLI plan command through engine-native path."""

from __future__ import annotations

import pytest


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires Rust engine and SemanticPlanCompiler")
class TestCliPlanEngine:
    """Full CLI -> plan compilation -> display E2E tests."""

    def test_plan_text_output(self) -> None:
        """Verify plan command produces text output with spec hash."""

    def test_plan_json_output(self) -> None:
        """Verify plan command produces valid JSON output."""

    def test_plan_dot_output(self) -> None:
        """Verify plan command produces DOT graph output."""

    def test_plan_tier1_fields_present(self) -> None:
        """Verify Tier 1 fields present: plan_signature, view_count, output_targets."""
