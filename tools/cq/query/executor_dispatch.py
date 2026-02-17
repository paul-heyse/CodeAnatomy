"""Dispatch facade for CQ query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity query for a prepared execution context.

    Returns:
        CqResult: Query result produced by the entity executor.
    """
    from tools.cq.query.executor_entity import execute_entity_query as _execute_entity_query

    return _execute_entity_query(ctx)


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern query for a prepared execution context.

    Returns:
        CqResult: Query result produced by the pattern executor.
    """
    from tools.cq.query.executor_pattern import execute_pattern_query as _execute_pattern_query

    return _execute_pattern_query(ctx)


__all__ = ["execute_entity_query", "execute_pattern_query"]
