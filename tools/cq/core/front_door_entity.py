"""Entity-focused front-door insight assembly."""

from __future__ import annotations

from tools.cq.core.front_door_dispatch import (
    augment_insight_with_semantic,
    build_entity_insight,
)

__all__ = ["augment_insight_with_semantic", "build_entity_insight"]
