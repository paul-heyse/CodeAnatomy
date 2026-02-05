"""Tests for run result merging."""

from __future__ import annotations

from tools.cq.core.merge import merge_step_results
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
