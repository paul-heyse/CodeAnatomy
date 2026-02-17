"""E2E checks for incremental enrichment CLI controls."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult


@pytest.mark.e2e
def test_search_incremental_mode_flag(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Verify search command accepts and applies incremental mode flag."""
    result = run_cq_result(
        [
            "search",
            "AsyncService",
            "--in",
            "tests/e2e/cq/_golden_workspace/python_project",
            "--lang",
            "python",
            "--enrich",
            "--enrich-mode",
            "ts_sym_dis",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    assert result.summary.get("error") is None


@pytest.mark.e2e
def test_neighborhood_incremental_mode_flag(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Verify neighborhood command reports the configured incremental mode."""
    result = run_cq_result(
        [
            "neighborhood",
            "forwarding_adapter",
            "--lang",
            "python",
            "--top-k",
            "5",
            "--enrich-mode",
            "full",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    assert result.summary.get("incremental_enrichment_mode") == "full"
