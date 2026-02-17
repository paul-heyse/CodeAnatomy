"""Typed summary-update contracts for query and macro paths."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class EntitySummaryUpdateV1(CqStruct, frozen=True, kw_only=True):
    """Entity query summary counters."""

    matches: int
    total_defs: int
    total_calls: int
    total_imports: int


class CallsSummaryUpdateV1(CqStruct, frozen=True, kw_only=True):
    """Calls macro summary payload."""

    query: str
    mode: str
    function: str
    signature: str
    total_sites: int
    files_with_calls: int
    total_py_files: int
    candidate_files: int
    scanned_files: int
    call_records: int
    rg_candidates: int
    scan_method: str


class ImpactSummaryUpdateV1(CqStruct, frozen=True, kw_only=True):
    """Impact macro summary payload."""

    query: str
    mode: str
    function: str
    parameter: str
    taint_sites: int
    max_depth: int
    functions_analyzed: int
    callers_found: int


__all__ = [
    "CallsSummaryUpdateV1",
    "EntitySummaryUpdateV1",
    "ImpactSummaryUpdateV1",
]
