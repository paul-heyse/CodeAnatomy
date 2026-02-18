"""Pattern-query execution entry points extracted from executor_runtime."""

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
    """Execute pattern query for a prepared execution context.

    Returns:
        Pattern query result payload.
    """
    return _execute_pattern_query_impl(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute pattern query with pre-tabulated files.

    Returns:
        Pattern query result payload.
    """
    return _execute_pattern_query_with_files_impl(request)
