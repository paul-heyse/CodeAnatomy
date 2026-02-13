"""Application service wrapper for entity front-door attachment."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct
from tools.cq.query.entity_front_door import attach_entity_front_door_insight

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50


class EntityFrontDoorRequest(CqStruct, frozen=True):
    """Typed request contract for entity front-door attachment."""

    result: CqResult
    relationship_detail_max_matches: int = _ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES


class EntityService:
    """Application-layer service for CQ entity flow."""

    @staticmethod
    def attach_front_door(request: EntityFrontDoorRequest) -> None:
        """Attach entity front-door insight to a CQ result."""
        attach_entity_front_door_insight(
            request.result,
            relationship_detail_max_matches=request.relationship_detail_max_matches,
        )


__all__ = ["EntityFrontDoorRequest", "EntityService"]
