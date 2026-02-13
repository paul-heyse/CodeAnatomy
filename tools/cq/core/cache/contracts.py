"""Typed cache payload contracts for CQ runtime cache namespaces."""

from __future__ import annotations

from typing import Annotated

import msgspec

from tools.cq.astgrep.sgpy_scanner import RecordType
from tools.cq.core.structs import CqCacheStruct

NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]


class SgRecordCacheV1(CqCacheStruct, frozen=True):
    """Cache-safe serialization contract for one ast-grep record."""

    record: RecordType = "def"
    kind: str = ""
    file: str = ""
    start_line: NonNegativeInt = 0
    start_col: NonNegativeInt = 0
    end_line: NonNegativeInt = 0
    end_col: NonNegativeInt = 0
    text: str = ""
    rule_id: str = ""


class SearchPartitionCacheV1(CqCacheStruct, frozen=True):
    """Cached search partition payload."""

    pattern: str
    raw_matches: list[dict[str, object]]
    stats: dict[str, object]
    enriched_matches: list[dict[str, object]]


class QueryEntityScanCacheV1(CqCacheStruct, frozen=True):
    """Cached entity-query scan payload."""

    records: list[SgRecordCacheV1]


class CallsTargetCacheV1(CqCacheStruct, frozen=True):
    """Cached calls-target metadata payload."""

    target_location: tuple[str, int] | None = None
    target_callees: dict[str, int] = msgspec.field(default_factory=dict)


__all__ = [
    "CallsTargetCacheV1",
    "QueryEntityScanCacheV1",
    "SearchPartitionCacheV1",
    "SgRecordCacheV1",
]
