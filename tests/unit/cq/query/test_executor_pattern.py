"""Tests for pattern-executor delegation helpers."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.core.schema import CqResult
from tools.cq.query import executor_pattern
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import PatternQueryRequest


def test_execute_pattern_query_delegates_to_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate pattern query execution to the shared executor helper."""
    sentinel_ctx = cast("QueryExecutionContext", object())
    sentinel_result = cast("CqResult", object())

    def _fake_execute(ctx: QueryExecutionContext) -> CqResult:
        assert ctx is sentinel_ctx
        return sentinel_result

    monkeypatch.setattr(
        "tools.cq.query.executor_pattern.runtime_execute_pattern_query",
        _fake_execute,
    )

    assert executor_pattern.execute_pattern_query(sentinel_ctx) is sentinel_result


def test_execute_pattern_query_with_files_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate file-scoped pattern query execution to shared executor."""
    sentinel_request = cast("PatternQueryRequest", object())
    sentinel_result = cast("CqResult", object())

    def _fake_execute(request: PatternQueryRequest) -> CqResult:
        assert request is sentinel_request
        return sentinel_result

    monkeypatch.setattr(
        "tools.cq.query.executor_pattern.runtime_execute_pattern_query_with_files",
        _fake_execute,
    )

    assert executor_pattern.execute_pattern_query_with_files(sentinel_request) is sentinel_result
