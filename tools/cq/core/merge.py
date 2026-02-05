"""Shared merge helpers for CQ multi-step results."""

from __future__ import annotations

from tools.cq.core.schema import CqResult, DetailPayload, Finding, Section


def _clone_with_provenance(
    finding: Finding,
    *,
    step_id: str,
    source_macro: str,
) -> Finding:
    data = dict(finding.details.data)
    data["source_step"] = step_id
    data["source_macro"] = source_macro
    details = DetailPayload(kind=finding.details.kind, score=finding.details.score, data=data)
    return Finding(
        category=finding.category,
        message=finding.message,
        anchor=finding.anchor,
        severity=finding.severity,
        details=details,
    )


def merge_step_results(merged: CqResult, step_id: str, step_result: CqResult) -> None:
    """Merge a step result into the aggregated run result."""
    source_macro = step_result.run.macro
    merged.summary.setdefault("steps", []).append(step_id)
    merged.summary.setdefault("step_summaries", {})[step_id] = step_result.summary

    merged.key_findings.extend(
        _clone_with_provenance(finding, step_id=step_id, source_macro=source_macro)
        for finding in step_result.key_findings
    )
    merged.evidence.extend(
        _clone_with_provenance(finding, step_id=step_id, source_macro=source_macro)
        for finding in step_result.evidence
    )
    merged.sections.extend(
        Section(
            title=f"{step_id}: {section.title}",
            findings=[
                _clone_with_provenance(finding, step_id=step_id, source_macro=source_macro)
                for finding in section.findings
            ],
            collapsed=section.collapsed,
        )
        for section in step_result.sections
    )
    merged.artifacts.extend(step_result.artifacts)


__all__ = [
    "merge_step_results",
]
