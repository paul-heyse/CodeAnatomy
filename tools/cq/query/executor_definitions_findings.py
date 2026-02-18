"""Definition-finding builders extracted from executor_definitions."""

from __future__ import annotations

from collections.abc import Sequence

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.schema import Anchor, Finding
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)
from tools.cq.query.shared_utils import extract_def_name


def def_to_finding(
    def_record: SgRecord,
    calls_within: Sequence[SgRecord],
    *,
    caller_count: int = 0,
    callee_count: int | None = None,
    enclosing_scope: str | None = None,
) -> Finding:
    """Convert a definition record to a finding.

    Returns:
        Normalized definition finding payload.
    """
    def_name = extract_def_name(def_record) or "unknown"
    anchor = Anchor(
        file=def_record.file,
        line=def_record.start_line,
        col=def_record.start_col,
        end_line=def_record.end_line,
        end_col=def_record.end_col,
    )
    effective_callee_count = len(calls_within) if callee_count is None else callee_count
    scope_label = enclosing_scope or "<module>"
    impact_signals = ImpactSignals(
        sites=max(caller_count, effective_callee_count),
        files=1,
        depth=1,
    )
    conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")
    score = build_score_details(impact=impact_signals, confidence=conf_signals)
    return Finding(
        category="definition",
        message=f"{def_record.kind}: {def_name}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "kind": def_record.kind,
                "name": def_name,
                "calls_within": len(calls_within),
                "caller_count": caller_count,
                "callee_count": effective_callee_count,
                "enclosing_scope": scope_label,
            },
            score=score,
        ),
    )


__all__ = ["def_to_finding"]
