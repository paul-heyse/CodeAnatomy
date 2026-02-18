"""Typed summary-update contracts for query and macro paths."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.summary_types import SummaryEnvelopeV1, apply_summary_mapping


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


type SummaryUpdateV1 = EntitySummaryUpdateV1 | CallsSummaryUpdateV1 | ImpactSummaryUpdateV1


def summary_update_mapping(update: SummaryUpdateV1) -> dict[str, object]:
    """Convert one typed summary-update contract to deterministic builtins mapping.

    Returns:
        Mapping payload for summary-merge application.
    """
    builtins = msgspec.to_builtins(update, order="deterministic", str_keys=True)
    if not isinstance(builtins, Mapping):
        return {}
    return {key: value for key, value in builtins.items() if isinstance(key, str)}


def apply_summary_update[SummaryEnvelopeT: SummaryEnvelopeV1](
    summary: SummaryEnvelopeT,
    update: SummaryUpdateV1,
) -> SummaryEnvelopeT:
    """Apply one typed summary-update payload onto a summary envelope.

    Returns:
        Updated summary envelope with ``update`` applied.
    """
    return apply_summary_mapping(summary, summary_update_mapping(update))


__all__ = [
    "CallsSummaryUpdateV1",
    "EntitySummaryUpdateV1",
    "ImpactSummaryUpdateV1",
    "SummaryUpdateV1",
    "apply_summary_update",
    "summary_update_mapping",
]
