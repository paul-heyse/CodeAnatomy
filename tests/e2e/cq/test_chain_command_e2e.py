"""Command-level e2e tests for `cq chain` over hermetic workspaces."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec

EXPECTED_CHAIN_STEPS = 2


@pytest.mark.e2e
def test_chain_search_and_q_steps_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test chain search and q steps golden."""
    result = run_cq_result(
        [
            "chain",
            "search",
            "AsyncService",
            "--in",
            "tests/e2e/cq/_golden_workspace/python_project",
            "--with-neighborhood",
            "AND",
            "q",
            "entity=function name=resolve in=tests/e2e/cq/_golden_workspace/python_project/app",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/chain_search_q_python_spec.json")
    assert_result_matches_spec(result, spec)
    steps = result.summary.get("steps")
    assert isinstance(steps, list)
    assert len(steps) == EXPECTED_CHAIN_STEPS
    section_titles = [section.title for section in result.sections]
    assert any("Target Candidates" in title for title in section_titles)
    assert any("Neighborhood Preview" in title for title in section_titles)

    assert_json_snapshot_data(
        "chain_search_q_python.json",
        result_snapshot_projection(result),
        update=update_golden,
    )
