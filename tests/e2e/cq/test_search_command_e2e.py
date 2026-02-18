"""Command-level e2e tests for `cq search` over hermetic golden workspaces."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult
from tools.cq.core.summary_types import SemanticTelemetryV1

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec
from tests.e2e.cq._support.projections import result_snapshot_projection
from tests.e2e.cq._support.specs import assert_result_matches_spec

_ALLOWED_INSIGHT_TARGET_KINDS = {
    "function",
    "class",
    "type",
    "module",
    "reference",
}


@pytest.mark.e2e
def test_search_python_workspace_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test search python workspace golden."""
    result = run_cq_result(
        [
            "search",
            "AsyncService",
            "--in",
            "tests/e2e/cq/_golden_workspace/python_project",
            "--lang",
            "python",
            "--with-neighborhood",
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
    assert target.get("kind") in _ALLOWED_INSIGHT_TARGET_KINDS
    assert any(section.title == "Neighborhood Preview" for section in result.sections)
    assert_json_snapshot_data(
        "search_python_asyncservice.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_search_rust_workspace_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test search rust workspace golden."""
    result = run_cq_result(
        [
            "search",
            "compile_target",
            "--in",
            "tests/e2e/cq/_golden_workspace/rust_workspace",
            "--lang",
            "rust",
            "--with-neighborhood",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/search_rust_compile_target_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    target = insight.get("target")
    assert isinstance(target, dict)
    assert target.get("kind") in _ALLOWED_INSIGHT_TARGET_KINDS
    rust_semantic_telemetry = result.summary.get("rust_semantic_telemetry")
    assert isinstance(rust_semantic_telemetry, SemanticTelemetryV1)
    assert rust_semantic_telemetry.attempted >= 0
    assert rust_semantic_telemetry.applied >= 0
    assert rust_semantic_telemetry.failed >= 0
    assert rust_semantic_telemetry.timed_out >= 0
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
    *,
    update_golden: bool,
) -> None:
    """Test search mixed workspace auto lang golden."""
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


@pytest.mark.e2e
def test_search_mixed_monorepo_rust_nested_workspace_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test search mixed monorepo rust nested workspace golden."""
    result = run_cq_result(
        [
            "search",
            "compile_target",
            "--in",
            "tests/e2e/cq/_golden_workspace/mixed_monorepo",
            "--lang",
            "rust",
            "--with-neighborhood",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/search_mixed_monorepo_rust_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    target = insight.get("target")
    assert isinstance(target, dict)
    assert target.get("kind") in _ALLOWED_INSIGHT_TARGET_KINDS
    rust_semantic_telemetry = result.summary.get("rust_semantic_telemetry")
    assert isinstance(rust_semantic_telemetry, SemanticTelemetryV1)
    assert_json_snapshot_data(
        "search_mixed_monorepo_rust_compile_target.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_search_mixed_monorepo_python_nested_workspace_golden(
    run_cq_result: Callable[..., CqResult],
    *,
    update_golden: bool,
) -> None:
    """Test search mixed monorepo python nested workspace golden."""
    result = run_cq_result(
        [
            "search",
            "dispatch_wrapper",
            "--in",
            "tests/e2e/cq/_golden_workspace/mixed_monorepo",
            "--lang",
            "python",
            "--with-neighborhood",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )

    spec = load_golden_spec("golden_specs/search_mixed_monorepo_python_spec.json")
    assert_result_matches_spec(result, spec)
    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    target = insight.get("target")
    assert isinstance(target, dict)
    assert target.get("kind") in _ALLOWED_INSIGHT_TARGET_KINDS
    python_semantic_telemetry = result.summary.get("python_semantic_telemetry")
    assert isinstance(python_semantic_telemetry, SemanticTelemetryV1)
    assert_json_snapshot_data(
        "search_mixed_monorepo_python_dispatch_wrapper.json",
        result_snapshot_projection(result),
        update=update_golden,
    )


@pytest.mark.e2e
def test_search_cli_incremental_mode_roundtrip(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Search CLI should propagate incremental enrichment flags to summary."""
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
    assert result.summary.get("incremental_enrichment_mode") == "ts_sym_dis"
