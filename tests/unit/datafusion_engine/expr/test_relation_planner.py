"""Unit tests for relation planner protocol surface."""

from __future__ import annotations

import pytest

from datafusion_engine.expr.relation_planner import (
    CodeAnatomyRelationPlanner,
    RelationPlannerPort,
)


def test_default_relation_planner_is_protocol_compatible() -> None:
    """Default planner implements the relation planner protocol."""
    planner = CodeAnatomyRelationPlanner()
    assert isinstance(planner, RelationPlannerPort)
    with pytest.raises(RuntimeError, match="does not expose Python-side relation planning"):
        planner.plan_relation(object(), object())
