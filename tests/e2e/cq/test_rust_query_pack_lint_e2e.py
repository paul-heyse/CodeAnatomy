"""E2E regression tests for Rust query-pack lint health."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult


@pytest.mark.e2e
def test_search_reports_query_pack_lint_ok_for_rust(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Rust search should not report query-pack lint failures."""
    result = run_cq_result(
        [
            "search",
            "register_udf",
            "--lang",
            "rust",
            "--in",
            "rust",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    query_pack_lint = result.summary.get("query_pack_lint")
    assert isinstance(query_pack_lint, dict)
    assert query_pack_lint.get("status") == "ok", query_pack_lint
    assert not any("Query pack lint failed" in finding.message for finding in result.key_findings)
