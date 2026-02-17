"""Entity-query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest
from tools.cq.query.executor_runtime import (
    execute_entity_query as runtime_execute_entity_query,
)
from tools.cq.query.executor_runtime import (
    execute_entity_query_from_records as runtime_execute_entity_query_from_records,
)


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity query for a prepared execution context.

    Returns:
        CqResult: Query result payload.
    """
    return runtime_execute_entity_query(ctx)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute an entity query using pre-scanned records.

    Returns:
        CqResult: Query result payload.
    """
    return runtime_execute_entity_query_from_records(request)


__all__ = ["execute_entity_query", "execute_entity_query_from_records"]
