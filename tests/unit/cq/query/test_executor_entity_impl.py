"""Tests for entity-query execution wrapper delegates."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.query import executor_entity_impl
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.execution_requests import EntityQueryRequest


def test_execute_entity_query_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify entity-query wrapper delegates to runtime implementation."""
    sentinel = object()

    monkeypatch.setattr(
        "tools.cq.query.executor_entity_impl._execute_entity_query_impl",
        lambda _ctx: sentinel,
    )

    assert executor_entity_impl.execute_entity_query(cast("QueryExecutionContext", object())) is sentinel


def test_execute_entity_query_from_records_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify record-based entity wrapper delegates to runtime implementation."""
    sentinel = object()

    monkeypatch.setattr(
        "tools.cq.query.executor_entity_impl._execute_entity_query_from_records_impl",
        lambda _request: sentinel,
    )

    assert executor_entity_impl.execute_entity_query_from_records(
        cast("EntityQueryRequest", object())
    ) is sentinel
