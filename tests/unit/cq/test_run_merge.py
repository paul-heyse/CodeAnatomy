"""Tests for run result merging."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.merge import merge_step_results
from tools.cq.core.multilang_orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.core.requests import MergeResultsRequest
from tools.cq.core.schema import CqResult, Finding, RunMeta, Section


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
        key_findings=[finding],
        sections=[Section(title="Section", findings=[finding])],
    )

    merged = CqResult(run=run)
    merge_step_results(merged, "step_0", step_result)

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
        run=run, summary={"matches": 1}, key_findings=[Finding(category="d", message="py")]
    )
    rs_result = CqResult(
        run=run, summary={"matches": 2}, key_findings=[Finding(category="d", message="rs")]
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope="auto",
            results={"python": py_result, "rust": rs_result},
            run=run,
        )
    )
    assert merged.summary["lang_scope"] == "auto"
    assert merged.summary["language_order"] == ["python", "rust"]
    assert isinstance(merged.summary["languages"], dict)
    assert "python" in merged.summary["languages"]
    assert "rust" in merged.summary["languages"]
    assert isinstance(merged.summary["cross_language_diagnostics"], list)
    assert isinstance(merged.summary["language_capabilities"], dict)


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
        summary={"matches": 1},
        key_findings=[Finding(category="d", message="py")],
    )
    rs_result = CqResult(
        run=run,
        summary={"matches": 2},
        key_findings=[Finding(category="d", message="rs")],
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
