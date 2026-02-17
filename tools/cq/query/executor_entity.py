"""Entity-query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity query for a prepared execution context.

    Returns:
        CqResult: Query result payload.
    """
    from tools.cq.query.executor import _execute_entity_query

    return _execute_entity_query(ctx)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute an entity query using pre-scanned records.

    Returns:
        CqResult: Query result payload.
    """
    from tools.cq.query.executor import execute_entity_query_from_records

    return execute_entity_query_from_records(request)


__all__ = ["execute_entity_query", "execute_entity_query_from_records"]
