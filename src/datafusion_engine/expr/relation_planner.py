"""Relation planner install helpers for DF52 FROM-clause extension points."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RelationPlannerPort(Protocol):
    """Protocol for relation planner implementations."""

    def install_relation_planner(self, ctx: object) -> None:
        """Install relation planner runtime hooks."""
        ...

    def plan_relation(self, relation: object, schema: object) -> object | None:
        """Return a planned relation object or ``None`` when unhandled."""
        ...


class CodeAnatomyRelationPlanner:
    """Default relation planner hook backed by the native extension."""

    def install_relation_planner(self, ctx: object) -> None:
        """Install relation planner hooks on the provided context."""
        _ = self
        install_relation_planner(ctx)

    def plan_relation(self, relation: object, schema: object) -> object | None:
        """Return ``None`` to delegate relation planning to DataFusion defaults."""
        _ = self
        _ = relation
        _ = schema
        return None


def relation_planner_extension_available() -> bool:
    """Return whether the native extension exposes relation-planner hooks."""
    from datafusion_engine.extensions import datafusion_ext

    return callable(getattr(datafusion_ext, "install_relation_planner", None))


def install_relation_planner(ctx: object) -> None:
    """Install relation planner runtime hook via native extension.

    Raises:
        TypeError: If the native extension entrypoint is unavailable.
    """
    from datafusion_engine.extensions import datafusion_ext

    installer = getattr(datafusion_ext, "install_relation_planner", None)
    if not callable(installer):
        msg = "DataFusion extension relation planner entrypoint is unavailable."
        raise TypeError(msg)
    installer(ctx)


__all__ = [
    "CodeAnatomyRelationPlanner",
    "RelationPlannerPort",
    "install_relation_planner",
    "relation_planner_extension_available",
]
