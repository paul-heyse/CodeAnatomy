"""Shared merge helpers for CQ multi-step results."""

from __future__ import annotations

import msgspec

from tools.cq.core.schema import (
    CqResult,
    DetailPayload,
    Finding,
    Section,
    extend_result_evidence,
    extend_result_key_findings,
    update_result_summary,
)


def _clone_with_provenance(
    finding: Finding,
    *,
    step_id: str,
    source_macro: str,
) -> Finding:
    data = dict(finding.details.data)
    data["source_step"] = step_id
    data["source_macro"] = source_macro
    details = DetailPayload(
        kind=finding.details.kind,
        score=finding.details.score,
        data_items=tuple(sorted(data.items())),
    )
    return Finding(
        category=finding.category,
        message=finding.message,
        anchor=finding.anchor,
        severity=finding.severity,
        details=details,
    )


def merge_step_results(merged: CqResult, step_id: str, step_result: CqResult) -> CqResult:
    """Merge a step result into the aggregated run result.

    Returns:
        CqResult: Updated merged run result.
    """
    source_macro = step_result.run.macro
    merged = update_result_summary(
        merged,
        {
            "steps": [*merged.summary.steps, step_id],
            "step_summaries": {
                **merged.summary.step_summaries,
                step_id: step_result.summary.to_dict(),
            },
        },
    )

    merged = extend_result_key_findings(
        merged,
        (
            _clone_with_provenance(finding, step_id=step_id, source_macro=source_macro)
            for finding in step_result.key_findings
        ),
    )
    merged = extend_result_evidence(
        merged,
        (
            _clone_with_provenance(finding, step_id=step_id, source_macro=source_macro)
            for finding in step_result.evidence
        ),
    )
    return msgspec.structs.replace(
        merged,
        sections=(
            *merged.sections,
            *tuple(
                Section(
                    title=f"{step_id}: {section.title}",
                    findings=[
                        _clone_with_provenance(
                            finding,
                            step_id=step_id,
                            source_macro=source_macro,
                        )
                        for finding in section.findings
                    ],
                    collapsed=section.collapsed,
                )
                for section in step_result.sections
            ),
        ),
        artifacts=(*merged.artifacts, *step_result.artifacts),
    )


__all__ = [
    "merge_step_results",
]
