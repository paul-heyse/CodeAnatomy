"""Relation planner protocol for DF52 FROM-clause extension points."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RelationPlannerPort(Protocol):
    """Protocol for relation planner implementations."""

    def plan_relation(self, relation: object, schema: object) -> object | None:
        """Return a planned relation object or ``None`` when unhandled."""
        ...


class CodeAnatomyRelationPlanner:
    """Default no-op relation planner for extension wiring."""

    def plan_relation(self, relation: object, schema: object) -> object | None:
        """Return ``None`` to signal that no custom relation was planned."""
        _ = self
        _ = relation
        _ = schema
        return None


__all__ = ["CodeAnatomyRelationPlanner", "RelationPlannerPort"]
