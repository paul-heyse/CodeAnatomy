"""Tests for compact summary rendering and diagnostics offload behavior."""

from __future__ import annotations

import msgspec
from tools.cq.core.front_door_contracts import (
    FrontDoorInsightV1,
    InsightArtifactRefsV1,
    InsightTargetV1,
)
from tools.cq.core.report import ARTIFACT_ONLY_KEYS, compact_summary_for_rendering, render_markdown
from tools.cq.core.schema import CqResult, Finding, RunMeta, Section
from tools.cq.core.summary_contract import summary_from_mapping


def _run_meta() -> RunMeta:
    return RunMeta(
        macro="search",
        argv=["cq", "search", "target"],
        root="/tmp/repo",
        started_ms=0.0,
        elapsed_ms=10.0,
        toolchain={},
    )


def _result_with_diagnostics() -> CqResult:
    insight = FrontDoorInsightV1(
        source="search",
        target=InsightTargetV1(symbol="target", kind="function"),
        artifact_refs=InsightArtifactRefsV1(
            diagnostics=".cq/artifacts/search_diagnostics.json",
            telemetry=".cq/artifacts/search_diagnostics.json",
        ),
    )
    summary: dict[str, object] = {
        "query": "target",
        "mode": "identifier",
        "front_door_insight": msgspec.to_builtins(insight),
        "enrichment_telemetry": {
            "python": {
                "ast_grep": {"applied": 2, "total": 3, "degraded": 1},
            }
        },
        "python_semantic_telemetry": {"attempted": 2, "applied": 1, "failed": 1},
        "rust_semantic_telemetry": {"attempted": 1, "applied": 1, "failed": 0},
        "semantic_planes": {"semantic_tokens_count": 4, "inlay_hints_count": 2},
        "python_semantic_diagnostics": [{"message": "diag"}],
        "language_capabilities": {"python": {}, "rust": {}, "shared": {}},
        "cross_language_diagnostics": [{"code": "ML001", "message": "info"}],
    }
    return CqResult(
        run=_run_meta(),
        summary=summary_from_mapping(summary),
        key_findings=[Finding(category="definition", message="function: target")],
        sections=[
            Section(
                title="Definitions",
                findings=[Finding(category="definition", message="function: target")],
            )
        ],
    )


def test_compact_summary_offloads_diagnostic_payloads() -> None:
    """Test compact summary offloads diagnostic payloads."""
    result = _result_with_diagnostics()
    compact, offloaded = compact_summary_for_rendering(result.summary)
    offloaded_keys = {key for key, _ in offloaded}
    for key in ARTIFACT_ONLY_KEYS:
        if key in result.summary:
            assert key in offloaded_keys
    assert isinstance(compact.get("python_semantic_diagnostics"), str)
    assert isinstance(compact.get("cross_language_diagnostics"), str)


def test_render_markdown_places_insight_card_first() -> None:
    """Test render markdown places insight card first."""
    output = render_markdown(_result_with_diagnostics())
    insight_idx = output.find("## Insight Card")
    overview_idx = output.find("## Code Overview")
    assert insight_idx >= 0
    assert overview_idx >= 0
    assert insight_idx < overview_idx


def test_render_markdown_hides_raw_diagnostic_payloads() -> None:
    """Test render markdown hides raw diagnostic payloads."""
    output = render_markdown(_result_with_diagnostics())
    assert "Diagnostic Details" not in output
    assert '"attempted": 2' not in output


def test_render_markdown_shows_artifact_refs_from_insight() -> None:
    """Test render markdown shows artifact refs from insight."""
    output = render_markdown(_result_with_diagnostics())
    assert ".cq/artifacts/search_diagnostics.json" in output
