"""Tests for test_cache_snapshot_pinned_prefix."""

from __future__ import annotations

from datafusion_engine.session.runtime import (
    CACHE_PROFILES,
    DataFusionRuntimeProfile,
    PolicyBundleConfig,
    cache_prefix_for_delta_snapshot,
)


def test_cache_prefix_for_delta_snapshot() -> None:
    """Build cache prefix strings for delta snapshot-aware datasets."""
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(snapshot_pinned_mode="delta_version"),
    )
    prefix = cache_prefix_for_delta_snapshot(
        profile,
        dataset_name="events",
        snapshot={"version": 12},
    )
    assert prefix is not None
    assert prefix.endswith("::events::12")
    assert prefix.startswith(profile.context_cache_key())



def test_named_cache_profile_applies_settings_and_telemetry() -> None:
    """Validate profile settings and telemetry payload alignment for presets."""
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(cache_profile_name="snapshot_pinned"),
    )
    settings = profile.settings_payload()
    expected = CACHE_PROFILES["snapshot_pinned"]
    for key, value in expected.items():
        assert settings.get(key) == value

    telemetry = profile.telemetry_payload()
    assert telemetry.get("cache_profile_name") == "snapshot_pinned"
    assert telemetry.get("cache_profile_settings") == dict(expected)
