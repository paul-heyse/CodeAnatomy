"""Tests for Ibis cache policy recording."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from ibis_engine.plan import IbisPlan
from ibis_engine.runner import (
    IbisCachePolicy,
    IbisPlanExecutionOptions,
    materialize_plan,
    stream_plan,
)


def test_cache_policy_records_events() -> None:
    """Record cache decisions for materialize and stream paths."""
    events: list[dict[str, object]] = []

    def _record(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    policy = IbisCachePolicy(enabled=False, reason="test", reporter=_record)
    execution = IbisPlanExecutionOptions(cache_policy=policy)
    plan = IbisPlan(expr=ibis.memtable({"a": [1, 2]}))

    materialize_plan(plan, execution=execution)
    assert events[0]["mode"] == "materialize"
    assert not events[0]["enabled"]

    events.clear()
    stream_plan(plan, execution=execution)
    assert events[0]["mode"] == "stream"
    assert not events[0]["enabled"]
