"""Tests for semantic registry required-tag enforcement."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from hamilton import node as hamilton_node

from hamilton_pipeline.semantic_registry import compile_semantic_registry


def _node(name: str, *, tags: Mapping[str, object]) -> hamilton_node.Node:
    def _fn() -> object:
        return None

    return hamilton_node.Node(
        name=name,
        typ=object,
        callabl=_fn,
        tags=dict(tags),
    )


def test_semantic_registry_raises_when_required_tags_are_missing() -> None:
    """Semantic outputs must provide the full required semantic tag set."""
    bad_node = _node(
        "semantic_bad",
        tags={
            "layer": "semantic",
            "kind": "table",
        },
    )
    nodes = cast("Mapping[str, hamilton_node.Node]", {"semantic_bad": bad_node})
    with pytest.raises(ValueError, match="Semantic registry validation failed"):
        compile_semantic_registry(nodes, plan_signature="plan:semantic:test")


def test_semantic_registry_accepts_complete_semantic_tags() -> None:
    """Valid semantic tags compile into a deterministic registry payload."""
    good_node = _node(
        "semantic_good",
        tags={
            "layer": "semantic",
            "semantic_id": "semantic.good.v1",
            "kind": "table",
            "entity": "node",
            "grain": "per_node",
            "version": "1",
            "stability": "stable",
        },
    )
    nodes = cast("Mapping[str, hamilton_node.Node]", {"semantic_good": good_node})
    registry = compile_semantic_registry(nodes, plan_signature="plan:semantic:test")
    assert registry.plan_signature == "plan:semantic:test"
    assert not registry.errors
    assert len(registry.records) == 1
    record = next(iter(registry.records.snapshot().values()))
    assert record.semantic_id == "semantic.good.v1"
    assert record.node_name == "semantic_good"
