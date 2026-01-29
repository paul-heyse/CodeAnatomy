"""Tests for shared core type helpers."""

from __future__ import annotations

from core_types import DeterminismTier, parse_determinism_tier


def test_parse_determinism_tier_from_enum() -> None:
    """Ensure determinism tier parser accepts enum inputs."""
    assert parse_determinism_tier(DeterminismTier.CANONICAL) is DeterminismTier.CANONICAL


def test_parse_determinism_tier_from_string() -> None:
    """Ensure determinism tier parser normalizes strings."""
    assert parse_determinism_tier("tier2") is DeterminismTier.CANONICAL
    assert parse_determinism_tier("stable") is DeterminismTier.STABLE_SET
    assert parse_determinism_tier("best_effort") is DeterminismTier.BEST_EFFORT


def test_parse_determinism_tier_unknown() -> None:
    """Ensure determinism tier parser returns None for unknown values."""
    assert parse_determinism_tier("unknown") is None
    assert parse_determinism_tier(None) is None
