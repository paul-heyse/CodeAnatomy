"""Tests for deterministic front-door serialization."""

from __future__ import annotations

from tools.cq.core.front_door_contracts import FrontDoorInsightV1, InsightTargetV1
from tools.cq.core.front_door_serialization import to_public_front_door_insight_dict


def test_to_public_front_door_insight_dict_serializes_contract() -> None:
    """Serializer emits deterministic mapping for front-door contract."""
    insight = FrontDoorInsightV1(
        source="search",
        target=InsightTargetV1(symbol="build_graph", kind="function"),
    )
    payload = to_public_front_door_insight_dict(insight)

    assert payload["source"] == "search"
    target = payload["target"]
    assert isinstance(target, dict)
    assert target["symbol"] == "build_graph"
    assert target["kind"] == "function"
