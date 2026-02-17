"""Thin orchestration facade for CQ query execution runtime."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.toolchain import Toolchain
from tools.cq.query import executor_runtime_impl
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest, PatternQueryRequest
from tools.cq.query.executor_ast_grep import (
    collect_match_spans as collect_match_spans_runtime,
)
from tools.cq.query.executor_ast_grep import (
    execute_ast_grep_rules as execute_ast_grep_rules_runtime,
)
from tools.cq.query.executor_ast_grep import (
    filter_records_by_spans as filter_records_by_spans_runtime,
)
from tools.cq.query.ir import Scope
from tools.cq.search._shared.types import SearchLimits

EntityExecutionState = executor_runtime_impl.EntityExecutionState
ExecutePlanRequestV1 = executor_runtime_impl.ExecutePlanRequestV1
PatternExecutionState = executor_runtime_impl.PatternExecutionState
_collect_match_spans = collect_match_spans_runtime
_execute_ast_grep_rules = execute_ast_grep_rules_runtime
_filter_records_by_spans = filter_records_by_spans_runtime


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute an entity query for a prepared execution context.

    Returns:
        CqResult: Entity-query result payload.
    """
    return executor_runtime_impl.execute_entity_query(ctx)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute an entity query using pre-scanned records.

    Returns:
        CqResult: Entity-query result payload.
    """
    return executor_runtime_impl.execute_entity_query_from_records(request)


def execute_pattern_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute a pattern query for a prepared execution context.

    Returns:
        CqResult: Pattern-query result payload.
    """
    return executor_runtime_impl.execute_pattern_query(ctx)


def execute_pattern_query_with_files(request: PatternQueryRequest) -> CqResult:
    """Execute a pattern query using a pre-tabulated file list.

    Returns:
        CqResult: Pattern-query result payload.
    """
    return executor_runtime_impl.execute_pattern_query_with_files(request)


def execute_plan(request: ExecutePlanRequestV1, *, tc: Toolchain) -> CqResult:
    """Execute a ToolPlan and return results.

    Returns:
        CqResult: Query execution result payload.
    """
    return executor_runtime_impl.execute_plan(request, tc=tc)


def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
    *,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Find files with matches using ripgrep path filtering rules.

    Returns:
        list[Path]: Matching file paths.
    """
    return executor_runtime_impl.rg_files_with_matches(root, pattern, scope, limits=limits)


__all__ = [
    "EntityExecutionState",
    "ExecutePlanRequestV1",
    "PatternExecutionState",
    "_collect_match_spans",
    "_execute_ast_grep_rules",
    "_filter_records_by_spans",
    "execute_entity_query",
    "execute_entity_query_from_records",
    "execute_pattern_query",
    "execute_pattern_query_with_files",
    "execute_plan",
    "rg_files_with_matches",
]
