"""Tests for MEMORY cache policy mapping and validation."""

from __future__ import annotations

from semantics.ir import ir_cache_hint_to_execution_policy
from semantics.pipeline_cache import normalize_cache_policy


def test_ir_cache_hint_maps_eager_to_memory() -> None:
    """High fan-out eager hint should prefer in-memory caching."""
    assert ir_cache_hint_to_execution_policy("eager") == "memory"


def test_pipeline_cache_normalizes_memory_policy() -> None:
    """Pipeline cache helpers accept MEMORY cache policy."""
    assert normalize_cache_policy("memory") == "memory"
