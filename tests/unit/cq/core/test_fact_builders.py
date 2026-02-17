"""Unit tests for fact-builder wrapper helpers."""

from __future__ import annotations

from tools.cq.core.fact_accessors import get_additional_language_payload, is_fact_value_present
from tools.cq.core.fact_builders import build_fact_clusters, build_primary_language_payload


def test_build_primary_language_payload_defaults_to_empty_mapping() -> None:
    """Primary payload builder should always return a mapping."""
    payload = build_primary_language_payload({})
    assert isinstance(payload, dict)


def test_build_fact_clusters_returns_tuple() -> None:
    """Fact-cluster builder should normalize to an immutable tuple."""
    clusters = build_fact_clusters({"language": "python", "node_kind": "function_definition"})
    assert isinstance(clusters, tuple)


def test_fact_accessors_helpers() -> None:
    """Accessor helpers should preserve expected defaults."""
    assert is_fact_value_present("x")
    assert get_additional_language_payload(None) == {}
