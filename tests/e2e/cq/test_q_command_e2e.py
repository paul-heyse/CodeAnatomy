# ruff: noqa: FBT001, ISC004
"""Command-level e2e tests for `cq q` on hermetic golden workspaces."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec


@pytest.mark.e2e
def test_q_python_resolve_entities_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    result = run_cq_result(
        [
            "q",
            "entity=function name=resolve in=tests/e2e/cq/_golden_workspace/python_project/app",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/q_python_resolve_spec.json")
    assert_result_matches_spec(result, spec)
    assert_json_snapshot_data(
        "q_python_resolve.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_q_rust_compile_target_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    result = run_cq_result(
        [
            "q",
            "entity=function name=compile_target lang=rust "
            "in=tests/e2e/cq/_golden_workspace/rust_workspace",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/q_rust_compile_target_spec.json")
    assert_result_matches_spec(result, spec)
    assert_json_snapshot_data(
        "q_rust_compile_target.json",
        result_snapshot_projection(result),
        update=update_golden,
    )
