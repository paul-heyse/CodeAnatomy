"""Contracts for CQ custom tree-sitter predicate definitions."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class QueryPredicateRuleV1(CqStruct, frozen=True):
    """One supported predicate signature in CQ runtime."""

    predicate_name: str
    min_args: int = 0
    max_args: int | None = None


SUPPORTED_QUERY_PREDICATES: tuple[QueryPredicateRuleV1, ...] = (
    QueryPredicateRuleV1(predicate_name="cq-regex?", min_args=2, max_args=2),
    QueryPredicateRuleV1(predicate_name="cq-eq?", min_args=2),
    QueryPredicateRuleV1(predicate_name="cq-match?", min_args=2),
    QueryPredicateRuleV1(predicate_name="cq-any-of?", min_args=2),
)

SUPPORTED_QUERY_PREDICATE_NAMES: frozenset[str] = frozenset(
    row.predicate_name for row in SUPPORTED_QUERY_PREDICATES
)

ALIASED_QUERY_PREDICATE_NAMES: frozenset[str] = frozenset(
    {
        "eq?",
        "eq",
        "match?",
        "any-of?",
    }
)


__all__ = [
    "ALIASED_QUERY_PREDICATE_NAMES",
    "SUPPORTED_QUERY_PREDICATES",
    "SUPPORTED_QUERY_PREDICATE_NAMES",
    "QueryPredicateRuleV1",
]
