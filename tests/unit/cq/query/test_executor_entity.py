"""Tests for entity-executor delegation helpers."""

from __future__ import annotations

from tools.cq.query import executor_entity


def test_execute_entity_query_delegates_to_executor(monkeypatch) -> None:
    sentinel_ctx = object()
    sentinel_result = object()

    def _fake_execute(ctx: object) -> object:
        assert ctx is sentinel_ctx
        return sentinel_result

    monkeypatch.setattr("tools.cq.query.executor._execute_entity_query", _fake_execute)

    assert executor_entity.execute_entity_query(sentinel_ctx) is sentinel_result


def test_execute_entity_query_from_records_delegates(monkeypatch) -> None:
    sentinel_request = object()
    sentinel_result = object()

    def _fake_execute(request: object) -> object:
        assert request is sentinel_request
        return sentinel_result

    monkeypatch.setattr(
        "tools.cq.query.executor.execute_entity_query_from_records",
        _fake_execute,
    )

    assert executor_entity.execute_entity_query_from_records(sentinel_request) is sentinel_result
