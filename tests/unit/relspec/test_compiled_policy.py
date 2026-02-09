"""Tests for CompiledExecutionPolicy struct and fingerprint stability."""

from __future__ import annotations

import msgspec
import pytest

from relspec.compiled_policy import CompiledExecutionPolicy
from serde_msgspec import to_builtins
from tests.test_helpers.immutability import assert_immutable_assignment


class TestCompiledExecutionPolicyConstruction:
    """Test CompiledExecutionPolicy construction and defaults."""

    def test_default_construction(self) -> None:
        """Construct with all defaults and verify empty mappings."""
        policy = CompiledExecutionPolicy()
        assert policy.cache_policy_by_view == {}
        assert policy.scan_policy_overrides == {}
        assert policy.maintenance_policy_by_dataset == {}
        assert policy.udf_requirements_by_view == {}
        assert policy.join_strategy_by_view == {}
        assert policy.inference_confidence_by_view == {}
        assert policy.materialization_strategy is None
        assert policy.diagnostics_flags == {}
        assert policy.workload_class is None
        assert policy.validation_mode == "warn"
        assert policy.policy_fingerprint is None

    def test_construction_with_cache_policies(self) -> None:
        """Construct with populated cache policies."""
        cache = {"cpg_nodes": "delta_output", "rel_calls": "delta_staging"}
        policy = CompiledExecutionPolicy(cache_policy_by_view=cache)
        assert policy.cache_policy_by_view == cache
        assert policy.validation_mode == "warn"

    def test_construction_with_all_fields(self) -> None:
        """Construct with all fields populated."""
        policy = CompiledExecutionPolicy(
            cache_policy_by_view={"v1": "delta_output"},
            scan_policy_overrides={"ds1": {"policy": "full"}},
            maintenance_policy_by_dataset={"ds1": {"optimize": True}},
            udf_requirements_by_view={"v1": ("udf_a", "udf_b")},
            join_strategy_by_view={"v1": "foreign_key"},
            inference_confidence_by_view={
                "v1": {
                    "confidence_score": 0.85,
                    "decision_type": "join_strategy",
                    "decision_value": "foreign_key",
                    "evidence_sources": ("schema",),
                }
            },
            materialization_strategy="delta",
            diagnostics_flags={"capture_datafusion_metrics": True},
            workload_class="batch_ingest",
            validation_mode="error",
            policy_fingerprint="abc123",
        )
        assert policy.cache_policy_by_view == {"v1": "delta_output"}
        assert policy.join_strategy_by_view == {"v1": "foreign_key"}
        assert policy.materialization_strategy == "delta"
        assert policy.workload_class == "batch_ingest"
        assert policy.validation_mode == "error"
        assert policy.policy_fingerprint == "abc123"

    def test_frozen_immutability(self) -> None:
        """Verify the struct is frozen (immutable)."""
        policy = CompiledExecutionPolicy()
        assert_immutable_assignment(
            factory=lambda: policy,
            attribute="validation_mode",
            attempted_value="error",
            expected_exception=AttributeError,
        )

    def test_forbid_unknown_fields(self) -> None:
        """Verify the struct rejects unknown fields during deserialization."""
        with pytest.raises(msgspec.ValidationError):
            msgspec.json.decode(
                b'{"unknown_field": true}',
                type=CompiledExecutionPolicy,
            )


class TestCompiledExecutionPolicySerialization:
    """Test serialization round-trip behavior."""

    def test_to_builtins_empty(self) -> None:
        """Empty policy serializes to a minimal dict (omit_defaults)."""
        policy = CompiledExecutionPolicy()
        payload = to_builtins(policy)
        # omit_defaults: only non-default fields appear
        assert isinstance(payload, dict)

    def test_to_builtins_with_data(self) -> None:
        """Policy with data serializes to dict with expected keys."""
        policy = CompiledExecutionPolicy(
            cache_policy_by_view={"cpg_nodes": "delta_output"},
            validation_mode="error",
        )
        payload = to_builtins(policy)
        assert isinstance(payload, dict)
        assert payload["cache_policy_by_view"] == {"cpg_nodes": "delta_output"}
        assert payload["validation_mode"] == "error"

    def test_round_trip_msgspec(self) -> None:
        """Encode and decode round-trip preserves data."""
        original = CompiledExecutionPolicy(
            cache_policy_by_view={"v1": "delta_output", "v2": "none"},
            udf_requirements_by_view={"v1": ("udf_x",)},
            policy_fingerprint="deadbeef",
        )
        encoded = msgspec.json.encode(original)
        decoded = msgspec.json.decode(encoded, type=CompiledExecutionPolicy)
        assert decoded.cache_policy_by_view == original.cache_policy_by_view
        assert decoded.udf_requirements_by_view == original.udf_requirements_by_view
        assert decoded.policy_fingerprint == original.policy_fingerprint


class TestCompiledExecutionPolicyFingerprint:
    """Test fingerprint stability and determinism."""

    def test_fingerprint_deterministic(self) -> None:
        """Same inputs produce the same fingerprint."""
        from relspec.policy_compiler import _compute_policy_fingerprint

        policy = CompiledExecutionPolicy(
            cache_policy_by_view={"a": "delta_output", "b": "none"},
        )
        fp1 = _compute_policy_fingerprint(policy)
        fp2 = _compute_policy_fingerprint(policy)
        assert fp1 == fp2
        assert len(fp1) == 64  # SHA-256 hex digest

    def test_fingerprint_differs_for_different_policies(self) -> None:
        """Different policies produce different fingerprints."""
        from relspec.policy_compiler import _compute_policy_fingerprint

        policy_a = CompiledExecutionPolicy(
            cache_policy_by_view={"a": "delta_output"},
        )
        policy_b = CompiledExecutionPolicy(
            cache_policy_by_view={"a": "none"},
        )
        assert _compute_policy_fingerprint(policy_a) != _compute_policy_fingerprint(policy_b)

    def test_fingerprint_is_hex_string(self) -> None:
        """Fingerprint is a valid hex string."""
        from relspec.policy_compiler import _compute_policy_fingerprint

        policy = CompiledExecutionPolicy(
            cache_policy_by_view={"x": "delta_staging"},
        )
        fp = _compute_policy_fingerprint(policy)
        int(fp, 16)  # Should not raise
