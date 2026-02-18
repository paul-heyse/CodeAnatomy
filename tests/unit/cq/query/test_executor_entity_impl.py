"""Tests for entity-query runtime wrapper delegates."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.query import executor_runtime_entity
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest


def test_execute_entity_query_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify entity-query wrapper delegates to runtime implementation."""
    sentinel = object()

    monkeypatch.setattr(
        "tools.cq.query.executor_runtime.execute_entity_query",
        lambda _ctx: sentinel,
    )

    assert (
        executor_runtime_entity.execute_entity_query(cast("QueryExecutionContext", object()))
        is sentinel
    )


def test_execute_entity_query_from_records_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify record-based entity wrapper delegates to runtime implementation."""
    sentinel = object()

    monkeypatch.setattr(
        "tools.cq.query.executor_runtime.execute_entity_query_from_records",
        lambda _request: sentinel,
    )

    assert (
        executor_runtime_entity.execute_entity_query_from_records(
            cast("EntityQueryRequest", object())
        )
        is sentinel
    )
