"""Pattern-query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import PatternQueryRequest
from tools.cq.query.executor_runtime import PatternExecutionState
from tools.cq.query.executor_runtime import (
    execute_pattern_query as _execute_pattern_query_impl,
)
from tools.cq.query.executor_runtime import (
    execute_pattern_query_with_files as _execute_pattern_query_with_files_impl,
)

__all__ = [
    "PatternExecutionState",
    "execute_pattern_query",
    "execute_pattern_query_with_files",
]


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern query for a prepared execution context.

    Returns:
        CqResult: Pattern-query result payload.
    """
    return _execute_pattern_query_impl(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute pattern query with a pre-tabulated file list.

    Returns:
        CqResult: Pattern-query result payload using provided file scope.
    """
    return _execute_pattern_query_with_files_impl(request)
