"""Runtime profile snapshot tests."""

from __future__ import annotations

from datafusion_engine.runtime import DataFusionJoinPolicy, DataFusionRuntimeProfile
from engine.runtime_profile import runtime_profile_snapshot

HASH_LENGTH: int = 64


def test_runtime_profile_snapshot_version() -> None:
    """Expose the runtime profile snapshot version."""
    profile = DataFusionRuntimeProfile()
    snapshot = runtime_profile_snapshot(profile, name="test")
    assert snapshot.version == 3


def test_runtime_profile_snapshot_includes_settings_hash() -> None:
    """Include the settings fingerprint in snapshots."""
    profile = DataFusionRuntimeProfile()
    snapshot = runtime_profile_snapshot(profile, name="test")
    assert len(snapshot.datafusion_settings_hash) == HASH_LENGTH


def test_runtime_profile_hash_changes_with_join_policy() -> None:
    """Update profile hash when join policy changes."""
    base = DataFusionRuntimeProfile()
    snapshot_base = runtime_profile_snapshot(base, name="test")
    modified = DataFusionRuntimeProfile(
        join_policy=DataFusionJoinPolicy(enable_hash_join=False),
    )
    snapshot_modified = runtime_profile_snapshot(modified, name="test")
    assert snapshot_base.profile_hash != snapshot_modified.profile_hash


def test_schema_evolution_adapter_enabled_by_default() -> None:
    """Enable schema evolution adapter in the default profile."""
    profile = DataFusionRuntimeProfile()
    assert profile.enable_schema_evolution_adapter is True
