"""Query planner helpers for tree-sitter query pattern prioritization."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_query_planner_contracts import (
    QueryPackPlanV1,
    QueryPatternPlanV1,
)

if TYPE_CHECKING:
    from tree_sitter import Query

_MIN_ASSERTION_TUPLE_SIZE = 2


def _capture_quantifier_name(query: Query, pattern_idx: int, capture_idx: int) -> str:
    try:
        quantifier = query.capture_quantifier(pattern_idx, capture_idx)
    except (RuntimeError, TypeError, ValueError, IndexError, SystemError):
        return "none"
    text = str(quantifier).strip()
    return text if text else "none"


def _pattern_score(
    *,
    rooted: bool,
    non_local: bool,
    guaranteed_step0: bool,
    has_assertions: bool,
    has_captures: bool,
) -> float:
    """Compute a weighted score for a query pattern.

    Returns:
        float: Deterministic value used for ordering plans.
    """
    score = 0.0
    if rooted:
        score += 2.0
    if guaranteed_step0:
        score += 1.2
    if has_assertions:
        score += 1.0
    if has_captures:
        score += 0.5
    if non_local:
        score -= 2.5
    return score


def build_pattern_plan(query: Query) -> tuple[QueryPatternPlanV1, ...]:
    """Build deterministic per-pattern planning rows from a compiled query.

    Returns:
        tuple[QueryPatternPlanV1, ...]: Sorted pattern plans by score.
    """
    pattern_count = int(getattr(query, "pattern_count", 0))
    capture_count = int(getattr(query, "capture_count", 0))
    plans: list[QueryPatternPlanV1] = []
    for pattern_idx in range(pattern_count):
        rooted = bool(query.is_pattern_rooted(pattern_idx))
        non_local = bool(query.is_pattern_non_local(pattern_idx))
        guaranteed_step = bool(query.is_pattern_guaranteed_at_step(pattern_idx))
        assertions_raw = query.pattern_assertions(pattern_idx)
        assertions: dict[str, tuple[str, bool]] = {}
        if isinstance(assertions_raw, dict):
            for key, value in assertions_raw.items():
                if not isinstance(key, str):
                    continue
                if isinstance(value, tuple) and len(value) >= _MIN_ASSERTION_TUPLE_SIZE:
                    assertions[str(key)] = (str(value[0]), bool(value[1]))
        quantifiers = tuple(
            _capture_quantifier_name(query, pattern_idx, capture_idx)
            for capture_idx in range(capture_count)
        )
        score = _pattern_score(
            rooted=rooted,
            non_local=non_local,
            guaranteed_step0=guaranteed_step,
            has_assertions=bool(assertions),
            has_captures=any(q not in {"", "none"} for q in quantifiers),
        )
        plans.append(
            QueryPatternPlanV1(
                pattern_idx=pattern_idx,
                rooted=rooted,
                non_local=non_local,
                guaranteed_step0=guaranteed_step,
                start_byte=int(query.start_byte_for_pattern(pattern_idx)),
                end_byte=int(query.end_byte_for_pattern(pattern_idx)),
                assertions=assertions,
                capture_quantifiers=quantifiers,
                score=score,
            )
        )
    return tuple(sorted(plans, key=lambda row: row.score, reverse=True))


def build_pack_plan(
    *,
    pack_name: str,
    query: Query,
    query_text: str,
) -> QueryPackPlanV1:
    """Build a plan summary for one pack for scheduling and cache keys.

    Returns:
        QueryPackPlanV1: Pack score and ordered pattern plans.
    """
    patterns = build_pattern_plan(query)
    pack_score = sum(pattern.score for pattern in patterns)
    digest = hashlib.sha256(query_text.encode("utf-8")).hexdigest()[:16]
    return QueryPackPlanV1(pack_name=pack_name, query_hash=digest, plans=patterns, score=pack_score)


def sort_pack_plans(
    pack_plans: Iterable[tuple[str, str, QueryPackPlanV1]],
) -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    """Sort query packs from highest to lowest planned score.

    Returns:
        tuple[tuple[str, str, QueryPackPlanV1], ...]: Sorted pack plans.

    Input tuple shape is ``(pack_name, source, plan)`` so callers keep pack source
    text aligned with the planned order.
    """
    return tuple(
        sorted(
            pack_plans,
            key=lambda item: (item[2].score, item[2].query_hash),
            reverse=True,
        )
    )


__all__ = ["build_pack_plan", "build_pattern_plan", "sort_pack_plans"]
