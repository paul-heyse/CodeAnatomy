"""Pattern-query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import PatternQueryRequest
from tools.cq.query.executor_runtime import (
    execute_pattern_query as runtime_execute_pattern_query,
)
from tools.cq.query.executor_runtime import (
    execute_pattern_query_with_files as runtime_execute_pattern_query_with_files,
)


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern query for a prepared execution context.

    Returns:
        CqResult: Query result payload.
    """
    return runtime_execute_pattern_query(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute pattern query using pre-tabulated files.

    Returns:
        CqResult: Query result payload.
    """
    return runtime_execute_pattern_query_with_files(request)


__all__ = ["execute_pattern_query", "execute_pattern_query_with_files"]
