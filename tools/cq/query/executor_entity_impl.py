"""Entity-query execution entry points."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest
from tools.cq.query.executor_runtime_impl import EntityExecutionState
from tools.cq.query.executor_runtime_impl import (
    execute_entity_query as _execute_entity_query_impl,
)
from tools.cq.query.executor_runtime_impl import (
    execute_entity_query_from_records as _execute_entity_query_from_records_impl,
)

__all__ = [
    "EntityExecutionState",
    "execute_entity_query",
    "execute_entity_query_from_records",
]


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity query for a prepared execution context.

    Returns:
        CqResult: Entity-query result payload.
    """
    return _execute_entity_query_impl(ctx)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute an entity query over pre-scanned records.

    Returns:
        CqResult: Entity-query result payload built from provided records.
    """
    return _execute_entity_query_from_records_impl(request)
