"""Tests for query executor dispatch facade."""

from __future__ import annotations

from typing import cast

import pytest
import tools.cq.query.executor as executor_module
from tools.cq.core.schema import CqResult, mk_runmeta
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.executor_dispatch import execute_entity_query, execute_pattern_query


def _result(macro: str) -> CqResult:
    return CqResult(
        run=mk_runmeta(
            macro=macro,
            argv=["cq", "q"],
            root=".",
            started_ms=0.0,
            toolchain={},
        )
    )


def test_execute_entity_query_delegates_to_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Entity dispatch should delegate to executor entity path."""
    expected = _result("entity")
    monkeypatch.setattr(
        executor_module,
        "_execute_entity_query",
        lambda *_args, **_kwargs: expected,
    )

    ctx = cast("QueryExecutionContext", object())
    assert execute_entity_query(ctx) is expected


def test_execute_pattern_query_delegates_to_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pattern dispatch should delegate to executor pattern path."""
    expected = _result("pattern")
    monkeypatch.setattr(
        executor_module,
        "_execute_pattern_query",
        lambda *_args, **_kwargs: expected,
    )

    ctx = cast("QueryExecutionContext", object())
    assert execute_pattern_query(ctx) is expected
