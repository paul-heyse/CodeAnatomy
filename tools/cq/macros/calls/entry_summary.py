"""Typed summary-update construction for calls macro entry flow."""

from __future__ import annotations

from tools.cq.core.summary_update_contracts import CallsSummaryUpdateV1


def build_calls_summary(
    *,
    function_name: str,
    signature: str,
    total_sites: int,
    files_with_calls: int,
    total_py_files: int,
    candidate_files: int,
    scanned_files: int,
    call_records: int,
    rg_candidates: int,
    used_fallback: bool,
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
        total_sites=total_sites,
        files_with_calls=files_with_calls,
        total_py_files=total_py_files,
        candidate_files=candidate_files,
        scanned_files=scanned_files,
        call_records=call_records,
        rg_candidates=rg_candidates,
        scan_method="ast-grep" if not used_fallback else "rg",
    )


__all__ = ["build_calls_summary"]
