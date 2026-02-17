"""Tests for consolidated session constants."""

from __future__ import annotations

from datafusion_engine.session._session_constants import CACHE_PROFILES, parse_major_version

EXPECTED_MAJOR_VERSION = 51


def test_parse_major_version_handles_standard_inputs() -> None:
    """Version parser extracts major version and tolerates invalid strings."""
    assert parse_major_version("51.0.0") == EXPECTED_MAJOR_VERSION
    assert parse_major_version("not-a-version") is None


def test_cache_profiles_contains_expected_profiles() -> None:
    """Canonical cache profiles expose required named presets."""
    assert "snapshot_pinned" in CACHE_PROFILES
    assert "always_latest_ttl30s" in CACHE_PROFILES
