# ruff: noqa: FBT001
"""Command-level e2e tests for `cq search` over hermetic golden workspaces."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec


@pytest.mark.e2e
def test_search_python_workspace_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    result = run_cq_result(
        [
            "search",
            "AsyncService",
            "--in",
            "tests/e2e/cq/_golden_workspace/python_project",
            "--lang",
            "python",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/search_python_asyncservice_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    target = insight.get("target")
    assert isinstance(target, dict)
    assert target.get("kind") in {"function", "class", "type"}
    assert any(section.title == "Neighborhood Preview" for section in result.sections)
    assert_json_snapshot_data(
        "search_python_asyncservice.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_search_rust_workspace_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    result = run_cq_result(
        [
            "search",
            "compile_target",
            "--in",
            "tests/e2e/cq/_golden_workspace/rust_workspace",
            "--lang",
            "rust",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/search_rust_compile_target_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    rust_lsp_telemetry = result.summary.get("rust_lsp_telemetry")
    assert isinstance(rust_lsp_telemetry, dict)
    assert {"attempted", "applied", "failed", "timed_out"}.issubset(rust_lsp_telemetry.keys())
    neighborhood = insight.get("neighborhood")
    assert isinstance(neighborhood, dict)
    for key in ("callers", "callees", "references", "hierarchy_or_scope"):
        payload = neighborhood.get(key)
        assert isinstance(payload, dict)
        assert {
            "total",
            "preview",
            "availability",
            "source",
            "overflow_artifact_ref",
        }.issubset(payload.keys())
    assert_json_snapshot_data(
        "search_rust_compile_target.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_search_mixed_workspace_auto_lang_golden(
    run_cq_result: Callable[..., CqResult],
    update_golden: bool,
) -> None:
    result = run_cq_result(
        [
            "search",
            "mixed_symbol",
            "--in",
            "tests/e2e/cq/_golden_workspace/mixed_workspace",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/search_mixed_auto_spec.json")
    assert_result_matches_spec(result, spec)

    anchored_files = [
        finding.anchor.file
        for finding in (*result.key_findings, *result.evidence)
        if finding.anchor is not None
    ]
    assert any("python_pkg/pipeline.py" in path for path in anchored_files)
    assert any("rust/tooling/src/lib.rs" in path for path in anchored_files)

    assert_json_snapshot_data(
        "search_mixed_auto_symbol.json",
        result_snapshot_projection(result),
        update=update_golden,
    )
