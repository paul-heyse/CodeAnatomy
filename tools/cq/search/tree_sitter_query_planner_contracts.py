"""Contracts for tree-sitter query pattern planning and cost estimation."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class QueryPatternPlanV1(CqStruct, frozen=True):
    """Per-pattern planning row used for pack scheduling."""

    pattern_idx: int
    rooted: bool
    non_local: bool
    guaranteed_step0: bool
    start_byte: int
    end_byte: int
    assertions: dict[str, tuple[str, bool]]
    capture_quantifiers: tuple[str, ...]
    score: float


class QueryPackPlanV1(CqStruct, frozen=True):
    """Plan rows for all patterns in one query pack."""

    pack_name: str
    query_hash: str
    plans: tuple[QueryPatternPlanV1, ...]
    score: float = 0.0


__all__ = ["QueryPackPlanV1", "QueryPatternPlanV1"]
