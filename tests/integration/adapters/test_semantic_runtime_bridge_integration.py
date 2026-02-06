"""Semantic runtime adapter integration tests.

Scope: Validate bidirectional adapter between DataFusionRuntimeProfile
and SemanticRuntimeConfig. Unit tests own basic round-trip behavior
(tests/unit/datafusion_engine/test_semantics_runtime_bridge.py). This
suite covers profile -> semantic config -> profile behavior inside
session construction.

Module Notes:
- SemanticRuntimeConfig lives in semantics.runtime (owned by semantics)
- Adapter functions live in datafusion_engine.semantics_runtime
- semantic_runtime_from_profile(profile) extracts config from profile
- apply_semantic_runtime_config(profile, config) applies config back
- Profile update uses msgspec.structs.replace() for structural sharing
"""

from __future__ import annotations

import pytest

from datafusion_engine.semantics_runtime import (
    apply_semantic_runtime_config,
    semantic_runtime_from_profile,
)
from semantics.runtime import SemanticRuntimeConfig
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion


def setup_module() -> None:
    """Ensure DataFusion is available for this test module."""
    require_datafusion()


@pytest.mark.integration
class TestSemanticRuntimeAdapter:
    """Tests for semantic runtime adapter round-trip."""

    def test_profile_to_semantic_config_returns_semantic_config(self) -> None:
        """Verify semantic_runtime_from_profile() returns SemanticRuntimeConfig."""
        profile = df_profile()
        config = semantic_runtime_from_profile(profile)
        assert isinstance(config, SemanticRuntimeConfig)

    def test_profile_to_semantic_config_extracts_cdf_enabled(self) -> None:
        """Verify CDF enabled flag extracted from profile."""
        profile = df_profile()
        config = semantic_runtime_from_profile(profile)
        # CDF enabled flag should match profile.features.enable_delta_cdf
        assert isinstance(config.cdf_enabled, bool)
        assert config.cdf_enabled == bool(profile.features.enable_delta_cdf)

    def test_profile_to_semantic_config_extracts_schema_evolution(self) -> None:
        """Verify schema evolution flag extracted from profile."""
        profile = df_profile()
        config = semantic_runtime_from_profile(profile)
        assert isinstance(config.schema_evolution_enabled, bool)
        assert config.schema_evolution_enabled == bool(
            profile.features.enable_schema_evolution_adapter
        )

    def test_profile_to_semantic_config_extracts_output_locations(self) -> None:
        """Verify output locations extracted from profile."""
        profile = df_profile()
        config = semantic_runtime_from_profile(profile)
        assert isinstance(config.output_locations, dict)

    def test_semantic_config_to_profile_returns_new_profile(self) -> None:
        """Verify apply_semantic_runtime_config() returns new profile.

        Uses msgspec.structs.replace() for structural sharing.
        """
        profile = df_profile()
        config = SemanticRuntimeConfig(
            output_locations={"test_view": "/tmp/test"},
            cdf_enabled=True,
            schema_evolution_enabled=False,
        )
        updated = apply_semantic_runtime_config(profile, config)
        # Should be a new object, not the same reference
        assert updated is not profile

    def test_immutable_update_original_unchanged(self) -> None:
        """Verify original profile is not mutated by apply."""
        profile = df_profile()
        original_cdf = profile.features.enable_delta_cdf
        config = SemanticRuntimeConfig(
            output_locations={},
            cdf_enabled=not original_cdf,
        )
        _ = apply_semantic_runtime_config(profile, config)
        # Original should be unchanged
        assert profile.features.enable_delta_cdf == original_cdf

    def test_apply_semantic_config_updates_cdf_flag(self) -> None:
        """Verify apply updates CDF enabled in profile features."""
        profile = df_profile()
        config = SemanticRuntimeConfig(
            output_locations={},
            cdf_enabled=True,
            schema_evolution_enabled=True,
        )
        updated = apply_semantic_runtime_config(profile, config)
        assert updated.features.enable_delta_cdf is True

    def test_apply_semantic_config_updates_schema_evolution(self) -> None:
        """Verify apply updates schema evolution in profile features."""
        profile = df_profile()
        config = SemanticRuntimeConfig(
            output_locations={},
            schema_evolution_enabled=False,
        )
        updated = apply_semantic_runtime_config(profile, config)
        assert updated.features.enable_schema_evolution_adapter is False

    def test_adapter_round_trip_preserves_cdf_enabled(self) -> None:
        """Verify profile -> config -> profile round-trip preserves CDF flag."""
        profile = df_profile()
        config = semantic_runtime_from_profile(profile)
        restored = apply_semantic_runtime_config(profile, config)
        assert restored.features.enable_delta_cdf == profile.features.enable_delta_cdf

    def test_adapter_round_trip_preserves_schema_evolution(self) -> None:
        """Verify round-trip preserves schema evolution flag."""
        profile = df_profile()
        config = semantic_runtime_from_profile(profile)
        restored = apply_semantic_runtime_config(profile, config)
        assert (
            restored.features.enable_schema_evolution_adapter
            == profile.features.enable_schema_evolution_adapter
        )

    def test_cache_policy_override_merging_semantic_wins(self) -> None:
        """Verify cache policy merging: semantic_config > profile.

        Profile cache overrides are at
        profile.data_sources.semantic_output.cache_overrides.
        """
        profile = df_profile()
        # Apply a config with specific cache overrides
        config = SemanticRuntimeConfig(
            output_locations={},
            cache_policy_overrides={"view_a": "delta_output"},
        )
        updated = apply_semantic_runtime_config(profile, config)
        # Semantic config cache policy should be present
        cache_overrides = updated.data_sources.semantic_output.cache_overrides
        assert cache_overrides.get("view_a") == "delta_output"

    def test_apply_propagates_cdf_cursor_store(self) -> None:
        """Verify CDF cursor store propagated through apply."""
        from pathlib import Path

        from semantics.incremental.cdf_cursors import CdfCursorStore

        profile = df_profile()
        store = CdfCursorStore(cursors_path=Path("/tmp/test_cursors"))
        config = SemanticRuntimeConfig(
            output_locations={},
            cdf_cursor_store=store,
        )
        updated = apply_semantic_runtime_config(profile, config)
        assert updated.data_sources.cdf_cursor_store is store
