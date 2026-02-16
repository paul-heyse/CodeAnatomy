# ruff: noqa: D103, INP001
"""Tests for shared core config spec contracts."""

from __future__ import annotations

from core.config_specs import CacheConfigSpec, DeltaConfigSpec, RootConfigSpec


def test_root_config_spec_roundtrip_fields() -> None:
    root = RootConfigSpec(
        cache=CacheConfigSpec(policy_profile="default"),
        delta=DeltaConfigSpec(),
    )
    assert root.cache is not None
    assert root.cache.policy_profile == "default"
