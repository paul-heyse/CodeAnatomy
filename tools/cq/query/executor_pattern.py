"""Pattern-query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import PatternQueryRequest


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern query for a prepared execution context.

    Returns:
        CqResult: Query result payload.
    """
    from tools.cq.query.executor import _execute_pattern_query

    return _execute_pattern_query(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute pattern query using pre-tabulated files.

    Returns:
        CqResult: Query result payload.
    """
    from tools.cq.query.executor import execute_pattern_query_with_files

    return execute_pattern_query_with_files(request)


__all__ = ["execute_pattern_query", "execute_pattern_query_with_files"]
