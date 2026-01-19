"""Runtime profile snapshot tests."""

from __future__ import annotations

from arrowdsl.core.runtime_profiles import RuntimeProfile
from arrowdsl.core.runtime_profiles import ScanProfile
from datafusion_engine.runtime import DataFusionJoinPolicy, DataFusionRuntimeProfile
from engine.runtime_profile import runtime_profile_snapshot

HASH_LENGTH: int = 64


def test_runtime_profile_snapshot_version() -> None:
    """Expose the runtime profile snapshot version."""
    runtime = RuntimeProfile(name="test", scan=ScanProfile(name="default"))
    snapshot = runtime_profile_snapshot(runtime)
    assert snapshot.version == 1


def test_runtime_profile_snapshot_includes_registry_hash() -> None:
    """Include the function registry fingerprint in snapshots."""
    runtime = RuntimeProfile(name="test", scan=ScanProfile(name="default"))
    snapshot = runtime_profile_snapshot(runtime)
    assert len(snapshot.function_registry_hash) == HASH_LENGTH


def test_runtime_profile_hash_changes_with_join_policy() -> None:
    """Update profile hash when join policy changes."""
    base = RuntimeProfile(name="test", scan=ScanProfile(name="default"))
    snapshot_base = runtime_profile_snapshot(base)
    modified = RuntimeProfile(
        name="test",
        scan=ScanProfile(name="default"),
        datafusion=DataFusionRuntimeProfile(
            join_policy=DataFusionJoinPolicy(enable_hash_join=False)
        ),
    )
    snapshot_modified = runtime_profile_snapshot(modified)
    assert snapshot_base.profile_hash != snapshot_modified.profile_hash


def test_runtime_profile_hash_changes_with_ibis_options() -> None:
    """Update profile hash when Ibis options change."""
    base = RuntimeProfile(name="test", scan=ScanProfile(name="default"))
    snapshot_base = runtime_profile_snapshot(base)
    modified = RuntimeProfile(
        name="test",
        scan=ScanProfile(name="default"),
        ibis_fuse_selects=False,
        ibis_default_limit=100,
    )
    snapshot_modified = runtime_profile_snapshot(modified)
    assert snapshot_base.profile_hash != snapshot_modified.profile_hash
