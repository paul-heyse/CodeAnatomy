# ruff: noqa: FBT001
"""Command-level e2e tests for `cq neighborhood`/`cq nb`."""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec


@pytest.mark.e2e
def test_neighborhood_symbol_target_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    """Test neighborhood symbol target golden."""
    result = run_cq_result(
        [
            "neighborhood",
            "forwarding_adapter",
            "--lang",
            "python",
            "--top-k",
            "5",
            "--no-semantic-enrichment",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/neighborhood_symbol_forwarding_adapter_spec.json")
    assert_result_matches_spec(result, spec)
    assert result.summary.get("target_resolution_kind") == "symbol_fallback"

    assert_json_snapshot_data(
        "neighborhood_symbol_forwarding_adapter.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_neighborhood_cli_and_run_step_parity(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Test neighborhood cli and run step parity."""
    target = "tests/e2e/cq/_golden_workspace/python_project/app/api.py:13"
    step_payload = {
        "type": "neighborhood",
        "target": target,
        "lang": "python",
        "top_k": 5,
        "no_semantic_enrichment": True,
    }

    direct = run_cq_result(
        [
            "neighborhood",
            target,
            "--lang",
            "python",
            "--top-k",
            "5",
            "--no-semantic-enrichment",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    run_step = run_cq_result(
        [
            "run",
            "--step",
            json.dumps(step_payload, separators=(",", ":")),
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    direct_titles = [section.title for section in direct.sections]
    run_titles = [
        title.split(": ", 1)[1] if ": " in title else title
        for title in (s.title for s in run_step.sections)
    ]

    assert direct.summary.get("target_resolution_kind") == "anchor"
    steps = run_step.summary.get("steps")
    assert isinstance(steps, list)
    assert len(steps) == 1
    assert direct_titles == run_titles
