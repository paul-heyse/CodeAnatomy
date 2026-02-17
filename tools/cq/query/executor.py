"""Thin query executor facade.

Entity/pattern execution internals live in ``executor_runtime.py`` while
this module provides the stable import surface used by CLI/runtime callers.
"""

from __future__ import annotations

from tools.cq.query.executor_runtime import (
    EntityExecutionState,
    ExecutePlanRequestV1,
    PatternExecutionState,
    _collect_match_spans,
    _execute_ast_grep_rules,
    _execute_entity_query,
    _execute_pattern_query,
    _filter_records_by_spans,
    execute_entity_query_from_records,
    execute_pattern_query_with_files,
    execute_plan,
    rg_files_with_matches,
)

__all__ = [
    "EntityExecutionState",
    "ExecutePlanRequestV1",
    "PatternExecutionState",
    "_collect_match_spans",
    "_execute_ast_grep_rules",
    "_execute_entity_query",
    "_execute_pattern_query",
    "_filter_records_by_spans",
    "execute_entity_query_from_records",
    "execute_pattern_query_with_files",
    "execute_plan",
    "rg_files_with_matches",
]
