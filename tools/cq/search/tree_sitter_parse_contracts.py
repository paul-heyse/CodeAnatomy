"""Contracts for request-scoped tree-sitter parse sessions."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.core.structs import CqStruct

PointV1 = tuple[int, int]


@dataclass(frozen=True, slots=True)
class TreeSitterInputEditV1:
    """One `Tree.edit` payload for incremental parsing."""

    start_byte: int
    old_end_byte: int
    new_end_byte: int
    start_point: PointV1
    old_end_point: PointV1
    new_end_point: PointV1


class ParseSessionStatsV1(CqStruct, frozen=True):
    """Parse-session counters for observability."""

    entries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    parse_count: int = 0
    reparse_count: int = 0
    edit_failures: int = 0


__all__ = [
    "ParseSessionStatsV1",
    "PointV1",
    "TreeSitterInputEditV1",
]
