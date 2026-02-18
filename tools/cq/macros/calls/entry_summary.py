"""Typed summary-update construction for calls macro entry flow."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.core.summary_update_contracts import CallsSummaryUpdateV1


@dataclass(frozen=True, slots=True)
class CallsSummaryMetrics:
    """Typed counters used to produce calls summary updates."""

    total_sites: int
    files_with_calls: int
    total_py_files: int
    candidate_files: int
    scanned_files: int
    call_records: int
    rg_candidates: int
    used_fallback: bool


def build_calls_summary(
    *,
    function_name: str,
    signature: str,
    metrics: CallsSummaryMetrics,
) -> CallsSummaryUpdateV1:
    """Build typed calls summary update contract from computed scan counters.

    Returns:
        Typed calls summary payload for result summary updates.
    """
    return CallsSummaryUpdateV1(
        query=function_name,
        mode="macro:calls",
        function=function_name,
        signature=signature,
        total_sites=metrics.total_sites,
        files_with_calls=metrics.files_with_calls,
        total_py_files=metrics.total_py_files,
        candidate_files=metrics.candidate_files,
        scanned_files=metrics.scanned_files,
        call_records=metrics.call_records,
        rg_candidates=metrics.rg_candidates,
        scan_method="ast-grep" if not metrics.used_fallback else "rg",
    )


__all__ = ["CallsSummaryMetrics", "build_calls_summary"]
