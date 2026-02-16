"""Tests for front-door render module wrappers."""

from __future__ import annotations

from tools.cq.core.front_door_builders import FrontDoorInsightV1, InsightTargetV1
from tools.cq.core.front_door_render import (
    coerce_front_door_insight,
    render_insight_card,
    to_public_front_door_insight_dict,
)


def test_front_door_render_roundtrip_from_public_payload() -> None:
    """Render helpers should round-trip a public insight payload."""
    insight = FrontDoorInsightV1(
        source="entity",
        target=InsightTargetV1(symbol="pkg.target", kind="function"),
    )

    payload = to_public_front_door_insight_dict(insight)
    restored = coerce_front_door_insight(payload)
    card = render_insight_card(insight)

    assert restored is not None
    assert restored.target.symbol == "pkg.target"
    assert card
    assert card[0] == "## Insight Card"
