"""Tests for plan-dispatch wrapper delegates."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.query import executor_plan_dispatch
from tools.cq.query.executor_runtime_impl import ExecutePlanRequestV1


def test_execute_plan_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify plan-dispatch wrapper delegates to runtime implementation."""
    sentinel = object()

    def fake_execute_plan(request: object, *, tc: object) -> object:
        assert request == "request"
        assert tc == "toolchain"
        return sentinel

    monkeypatch.setattr(
        "tools.cq.query.executor_plan_dispatch.execute_plan_impl",
        fake_execute_plan,
    )

    assert (
        executor_plan_dispatch.execute_plan(
            cast("ExecutePlanRequestV1", "request"),
            tc=cast("Toolchain", "toolchain"),
        )
        is sentinel
    )
