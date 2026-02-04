"""Runtime profile snapshot tests."""

from __future__ import annotations

from typing import cast

from datafusion_engine.session.runtime import (
    DataFusionJoinPolicy,
    DataFusionRuntimeProfile,
    ExecutionConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from engine.runtime_profile import PROFILE_HASH_VERSION, runtime_profile_snapshot
from tests.test_helpers.datafusion_runtime import df_profile

HASH_LENGTH: int = 64
DELTA_MAX_SPILL_SIZE: int = 1024
DELTA_MAX_TEMP_DIRECTORY_SIZE: int = 4096


def test_runtime_profile_snapshot_version() -> None:
    """Expose the runtime profile snapshot version."""
    profile = df_profile()
    snapshot = runtime_profile_snapshot(profile, name="test")
    assert snapshot.version == PROFILE_HASH_VERSION


def test_runtime_profile_snapshot_includes_settings_hash() -> None:
    """Include the settings fingerprint in snapshots."""
    profile = df_profile()
    snapshot = runtime_profile_snapshot(profile, name="test")
    assert len(snapshot.datafusion_settings_hash) == HASH_LENGTH


def test_runtime_profile_hash_changes_with_join_policy() -> None:
    """Update profile hash when join policy changes."""
    base = df_profile()
    snapshot_base = runtime_profile_snapshot(base, name="test")
    modified = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(
            join_policy=DataFusionJoinPolicy(enable_hash_join=False),
        ),
    )
    snapshot_modified = runtime_profile_snapshot(modified, name="test")
    assert snapshot_base.profile_hash != snapshot_modified.profile_hash


def test_schema_evolution_adapter_enabled_by_default() -> None:
    """Enable schema evolution adapter in the default profile."""
    profile = df_profile()
    assert profile.enable_schema_evolution_adapter is True


def test_force_disable_ident_normalization_overrides() -> None:
    """Disable identifier normalization when forced."""
    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            enable_ident_normalization=True,
            force_disable_ident_normalization=True,
        ),
    )
    settings = profile.settings_payload()
    assert settings["datafusion.sql_parser.enable_ident_normalization"] == "false"


def test_delta_runtime_env_telemetry_payload() -> None:
    """Expose Delta runtime env options in telemetry payloads."""
    profile = DataFusionRuntimeProfile(
        execution=ExecutionConfig(
            delta_max_spill_size=DELTA_MAX_SPILL_SIZE,
            delta_max_temp_directory_size=DELTA_MAX_TEMP_DIRECTORY_SIZE,
        ),
    )
    payload = profile.telemetry_payload_v1()
    extensions = cast("dict[str, object]", payload["extensions"])
    delta_runtime = cast("dict[str, object]", extensions["delta_runtime_env"])
    assert delta_runtime["max_spill_size"] == DELTA_MAX_SPILL_SIZE
    assert delta_runtime["max_temp_directory_size"] == DELTA_MAX_TEMP_DIRECTORY_SIZE
