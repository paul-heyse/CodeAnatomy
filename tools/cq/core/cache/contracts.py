"""Typed cache payload contracts for CQ runtime cache namespaces."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class SearchPartitionCacheV1(CqStruct, frozen=True):
    """Cached search partition payload."""

    pattern: str
    raw_matches: list[dict[str, object]]
    stats: dict[str, object]
    enriched_matches: list[dict[str, object]]


class QueryEntityScanCacheV1(CqStruct, frozen=True):
    """Cached entity-query scan payload."""

    records: list[dict[str, object]]


class CallsTargetCacheV1(CqStruct, frozen=True):
    """Cached calls-target metadata payload."""

    target_location: tuple[str, int] | None = None
    target_callees: dict[str, int] = msgspec.field(default_factory=dict)


__all__ = [
    "CallsTargetCacheV1",
    "QueryEntityScanCacheV1",
    "SearchPartitionCacheV1",
]
