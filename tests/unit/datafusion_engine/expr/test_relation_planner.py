"""Unit tests for relation planner protocol surface."""

from __future__ import annotations

from datafusion_engine.expr.relation_planner import (
    CodeAnatomyRelationPlanner,
    RelationPlannerPort,
)


def test_default_relation_planner_is_protocol_compatible() -> None:
    """Default planner implements the relation planner protocol."""
    planner = CodeAnatomyRelationPlanner()
    assert isinstance(planner, RelationPlannerPort)
    assert planner.plan_relation(object(), object()) is None
