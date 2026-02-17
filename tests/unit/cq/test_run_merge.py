"""Tests for run result merging."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

import msgspec
from tools.cq.core.contracts import MergeResultsRequest
from tools.cq.core.front_door_contracts import FrontDoorInsightV1, InsightSliceV1, InsightTargetV1
from tools.cq.core.merge import merge_step_results
from tools.cq.core.schema import CqResult, Finding, RunMeta, Section
from tools.cq.core.summary_contract import SemanticTelemetryV1, summary_from_mapping
from tools.cq.orchestration.multilang_orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)

PYTHON_ATTEMPTED_COUNT = 2


def test_merge_step_results_adds_provenance() -> None:
    """Merged findings include source_step and source_macro details."""
    run = RunMeta(
        macro="calls",
        argv=["cq", "calls", "foo"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    finding = Finding(category="test", message="example")
    step_result = CqResult(
        run=run,
        key_findings=(finding,),
        sections=(Section(title="Section", findings=[finding]),),
    )

    merged = CqResult(run=run)
    merged = merge_step_results(merged, "step_0", step_result)

    assert merged.key_findings[0].details.data["source_step"] == "step_0"
    assert merged.key_findings[0].details.data["source_macro"] == "calls"
    assert merged.sections[0].title == "step_0: Section"


def test_merge_language_cq_results_builds_multilang_contract() -> None:
    """Language merge helper should emit canonical multilang summary keys."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    py_result = CqResult(
        run=run,
        summary=summary_from_mapping({"matches": 1}),
        key_findings=(Finding(category="d", message="py"),),
    )
    rs_result = CqResult(
        run=run,
        summary=summary_from_mapping({"matches": 2}),
        key_findings=(Finding(category="d", message="rs"),),
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope="auto",
            results={"python": py_result, "rust": rs_result},
            run=run,
        )
    )
    assert merged.summary["lang_scope"] == "auto"
    assert merged.summary["language_order"] == ("python", "rust")
    assert isinstance(merged.summary["languages"], MappingProxyType)
    languages = merged.summary["languages"]
    assert isinstance(languages, MappingProxyType)
    assert "python" in languages
    assert "rust" in languages
    assert isinstance(merged.summary["cross_language_diagnostics"], tuple)
    assert isinstance(merged.summary["language_capabilities"], MappingProxyType)
    assert "python_semantic_overview" in merged.summary
    assert "python_semantic_telemetry" in merged.summary
    assert "rust_semantic_telemetry" in merged.summary
    assert "semantic_planes" in merged.summary
    assert "python_semantic_diagnostics" in merged.summary


def test_merge_language_cq_results_preserves_summary_common() -> None:
    """Language merge should preserve supplied query/mode metadata."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    py_result = CqResult(
        run=run,
        summary=summary_from_mapping({"matches": 1}),
        key_findings=(Finding(category="d", message="py"),),
    )
    rs_result = CqResult(
        run=run,
        summary=summary_from_mapping({"matches": 2}),
        key_findings=(Finding(category="d", message="rs"),),
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope="auto",
            results={"python": py_result, "rust": rs_result},
            run=run,
            summary_common={"query": "entity=function name=target", "mode": "entity"},
        )
    )
    assert merged.summary["query"] == "entity=function name=target"
    assert merged.summary["mode"] == "entity"


def test_runmeta_for_scope_merge_builds_runmeta() -> None:
    """Runmeta helper should preserve macro and root context."""
    run = runmeta_for_scope_merge(
        macro="q",
        root=Path(),
        argv=["cq", "q"],
        tc=None,
    )
    assert run.macro == "q"


def test_merge_language_results_preserves_front_door_insight() -> None:
    """Test merge language results preserves front door insight."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    py_insight = FrontDoorInsightV1(
        source="entity",
        target=InsightTargetV1(symbol="target", kind="function"),
        neighborhood=msgspec.structs.replace(
            FrontDoorInsightV1(
                source="entity", target=InsightTargetV1(symbol="target")
            ).neighborhood,
            callers=InsightSliceV1(total=2, availability="full", source="structural"),
        ),
    )
    py_result = CqResult(
        run=run,
        summary=summary_from_mapping(
            {"matches": 1, "front_door_insight": msgspec.to_builtins(py_insight)}
        ),
        key_findings=(Finding(category="definition", message="py"),),
    )
    rs_result = CqResult(
        run=run,
        summary=summary_from_mapping({"matches": 0}),
        key_findings=(),
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope="auto",
            results={"python": py_result, "rust": rs_result},
            run=run,
            summary_common={"query": "entity=function name=target", "mode": "entity"},
        )
    )
    raw = merged.summary.get("front_door_insight")
    assert isinstance(raw, Mapping)
    assert raw.get("source") == "entity"
    target = raw.get("target")
    assert isinstance(target, Mapping)
    assert target.get("symbol") == "target"


def test_merge_language_results_marks_partial_when_language_missing_insight() -> None:
    """Test merge language results marks partial when language missing insight."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    insight = FrontDoorInsightV1(
        source="entity",
        target=InsightTargetV1(symbol="target", kind="function"),
        neighborhood=msgspec.structs.replace(
            FrontDoorInsightV1(
                source="entity", target=InsightTargetV1(symbol="target")
            ).neighborhood,
            callers=InsightSliceV1(total=3, availability="full", source="structural"),
        ),
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope="auto",
            results={
                "python": CqResult(
                    run=run,
                    summary=summary_from_mapping(
                        {"matches": 1, "front_door_insight": msgspec.to_builtins(insight)}
                    ),
                ),
                "rust": CqResult(run=run, summary=summary_from_mapping({"matches": 0})),
            },
            run=run,
            summary_common={"query": "entity=function name=target", "mode": "entity"},
        )
    )
    raw = merged.summary.get("front_door_insight")
    assert isinstance(raw, Mapping)
    neighborhood = raw.get("neighborhood")
    assert isinstance(neighborhood, Mapping)
    callers = neighborhood.get("callers")
    assert isinstance(callers, Mapping)
    assert callers.get("availability") == "partial"
    degradation = raw.get("degradation")
    assert isinstance(degradation, MappingProxyType)
    assert degradation.get("scope_filter") == "partial"
    notes = degradation.get("notes")
    assert isinstance(notes, tuple)
    assert any("missing_languages=rust" in str(note) for note in notes)


def test_merge_language_results_aggregates_semantic_telemetry() -> None:
    """Test merge language results aggregates semantic telemetry."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    py_result = CqResult(
        run=run,
        summary=summary_from_mapping(
            {
                "matches": 2,
                "python_semantic_telemetry": {
                    "attempted": 2,
                    "applied": 1,
                    "failed": 1,
                    "skipped": 0,
                    "timed_out": 0,
                },
            }
        ),
        key_findings=(Finding(category="definition", message="py"),),
    )
    rust_result = CqResult(
        run=run,
        summary=summary_from_mapping(
            {
                "matches": 1,
                "rust_semantic_telemetry": {
                    "attempted": 1,
                    "applied": 1,
                    "failed": 0,
                    "skipped": 0,
                    "timed_out": 0,
                },
            }
        ),
        key_findings=(Finding(category="definition", message="rs"),),
    )

    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope="auto",
            results={"python": py_result, "rust": rust_result},
            run=run,
            summary_common={"query": "entity=function name=target", "mode": "entity"},
        )
    )

    python_semantic = merged.summary.get("python_semantic_telemetry")
    rust = merged.summary.get("rust_semantic_telemetry")
    assert isinstance(python_semantic, SemanticTelemetryV1)
    assert isinstance(rust, SemanticTelemetryV1)
    assert python_semantic.attempted == PYTHON_ATTEMPTED_COUNT
    assert python_semantic.applied == 1
    assert python_semantic.failed == 1
    assert rust.attempted == 1
    assert rust.applied == 1
    assert rust.failed == 0
