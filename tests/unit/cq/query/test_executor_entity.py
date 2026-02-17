"""Tests for entity-executor delegation helpers."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.core.schema import CqResult
from tools.cq.query import executor_entity
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest


def test_execute_entity_query_delegates_to_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate entity query execution to the shared executor helper."""
    sentinel_ctx = cast("QueryExecutionContext", object())
    sentinel_result = cast("CqResult", object())

    def _fake_execute(ctx: QueryExecutionContext) -> CqResult:
        assert ctx is sentinel_ctx
        return sentinel_result

    monkeypatch.setattr("tools.cq.query.executor_runtime.execute_entity_query", _fake_execute)

    assert executor_entity.execute_entity_query(sentinel_ctx) is sentinel_result


def test_execute_entity_query_from_records_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate record-based entity query execution to shared executor."""
    sentinel_request = cast("EntityQueryRequest", object())
    sentinel_result = cast("CqResult", object())

    def _fake_execute(request: EntityQueryRequest) -> CqResult:
        assert request is sentinel_request
        return sentinel_result

    monkeypatch.setattr(
        "tools.cq.query.executor_runtime.execute_entity_query_from_records",
        _fake_execute,
    )

    assert executor_entity.execute_entity_query_from_records(sentinel_request) is sentinel_result
