"""Runtime profile snapshot tests."""

from __future__ import annotations

from typing import cast

from datafusion_engine.plan.perf_policy import (
    CachePolicyTier,
    PerformancePolicy,
    StatisticsPolicy,
)
from datafusion_engine.session.contracts import IdentifierNormalizationMode
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_config_policies import DataFusionJoinPolicy
from datafusion_engine.session.runtime_profile_config import (
    DiagnosticsConfig,
    ExecutionConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
    ZeroRowBootstrapConfig,
)
from extraction.runtime_profile import PROFILE_HASH_VERSION, runtime_profile_snapshot
from obs.diagnostics import DiagnosticsCollector

LISTING_CACHE_TTL_SECONDS = 12
META_FETCH_CONCURRENCY = 5
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
    assert profile.features.enable_schema_evolution_adapter is True


def test_delta_session_defaults_disable_non_strict_ident_normalization() -> None:
    """Delta session defaults should disable non-strict identifier normalization."""
    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            identifier_normalization_mode=IdentifierNormalizationMode.SQL_SAFE,
            enable_delta_session_defaults=True,
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


def test_zero_row_bootstrap_config_is_in_telemetry_and_fingerprint() -> None:
    """Zero-row bootstrap settings should affect telemetry payload and profile hash."""
    baseline = DataFusionRuntimeProfile()
    configured = DataFusionRuntimeProfile(
        zero_row_bootstrap=ZeroRowBootstrapConfig(
            validation_mode="bootstrap",
            include_semantic_outputs=False,
            include_internal_tables=False,
            strict=False,
            allow_semantic_row_probe_fallback=True,
            bootstrap_mode="seeded_minimal_rows",
            seeded_datasets=("cst_refs",),
        )
    )
    assert baseline.fingerprint() != configured.fingerprint()
    payload = configured.telemetry_payload_v1()
    zero_row = cast("dict[str, object]", payload["zero_row_bootstrap"])
    assert zero_row == {
        "validation_mode": "bootstrap",
        "include_semantic_outputs": False,
        "include_internal_tables": False,
        "strict": False,
        "allow_semantic_row_probe_fallback": True,
        "bootstrap_mode": "seeded_minimal_rows",
        "seeded_datasets": ["cst_refs"],
    }


def test_settings_hash_changes_with_performance_policy() -> None:
    """Performance-policy changes should affect runtime settings hash."""
    baseline = DataFusionRuntimeProfile()
    modified = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(
            performance_policy=PerformancePolicy(
                cache=CachePolicyTier(
                    listing_cache_ttl_seconds=45,
                    listing_cache_max_entries=4096,
                    metadata_cache_max_entries=8192,
                ),
                statistics=StatisticsPolicy(
                    collect_statistics=False,
                    meta_fetch_concurrency=2,
                    fallback_when_unavailable="skip",
                ),
            )
        ),
    )
    assert baseline.settings_hash() != modified.settings_hash()


def test_settings_payload_includes_performance_policy_knobs() -> None:
    """Performance policy should map to DataFusion runtime settings keys."""
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(
            performance_policy=PerformancePolicy(
                cache=CachePolicyTier(
                    listing_cache_ttl_seconds=15,
                    listing_cache_max_entries=2048,
                    metadata_cache_max_entries=4096,
                ),
                statistics=StatisticsPolicy(
                    collect_statistics=False,
                    meta_fetch_concurrency=3,
                    fallback_when_unavailable="skip",
                ),
            )
        )
    )
    settings = profile.settings_payload()
    assert settings["datafusion.runtime.list_files_cache_ttl"] == "15s"
    assert settings["datafusion.runtime.list_files_cache_limit"] == "2048"
    assert settings["datafusion.runtime.metadata_cache_limit"] == "4096"
    assert settings["datafusion.execution.collect_statistics"] == "false"
    assert settings["datafusion.execution.meta_fetch_concurrency"] == "3"


def test_session_startup_records_performance_policy_artifact() -> None:
    """Runtime startup should emit performance-policy artifact with applied knobs."""
    diagnostics = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(
        diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
        policies=PolicyBundleConfig(
            performance_policy=PerformancePolicy(
                cache=CachePolicyTier(
                    listing_cache_ttl_seconds=12,
                    listing_cache_max_entries=1024,
                    metadata_cache_max_entries=2048,
                ),
                statistics=StatisticsPolicy(
                    collect_statistics=True,
                    meta_fetch_concurrency=5,
                    fallback_when_unavailable="skip",
                ),
            )
        ),
    )
    _ = profile.session_context()
    artifacts = diagnostics.artifacts_snapshot().get("performance_policy_v1", [])
    assert artifacts
    payload = cast("dict[str, object]", artifacts[-1])
    cache_payload = cast("dict[str, object]", payload["cache"])
    statistics_payload = cast("dict[str, object]", payload["statistics"])
    assert cache_payload["listing_cache_ttl_seconds"] == LISTING_CACHE_TTL_SECONDS
    assert statistics_payload["meta_fetch_concurrency"] == META_FETCH_CONCURRENCY
    applied_knobs = cast("dict[str, object]", payload["applied_knobs"])
    assert applied_knobs["datafusion.runtime.list_files_cache_ttl"] == "12s"
    assert applied_knobs["datafusion.execution.meta_fetch_concurrency"] == "5"
    assert "statistics_mode" in applied_knobs
