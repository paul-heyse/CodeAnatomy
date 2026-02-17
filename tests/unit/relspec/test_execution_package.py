"""Tests for the reproducible execution package module."""

from __future__ import annotations

import time
from dataclasses import dataclass
from unittest.mock import MagicMock

import msgspec

from relspec.execution_package import (
    ExecutionPackageArtifact,
    build_execution_package,
)
from tests.test_helpers.immutability import assert_immutable_assignment

FIXED_CREATED_AT_UNIX_MS = 1_700_000_000_000
EXPECTED_PLAN_BUNDLE_COUNT = 3
SCHEMA_FINGERPRINT_LENGTH = 32


@dataclass(frozen=True)
class _SemanticIrStub:
    ir_hash: str


@dataclass(frozen=True)
class _ManifestStub:
    model_hash: str | None = None
    semantic_ir: _SemanticIrStub | None = None


@dataclass(frozen=True)
class _PolicyStub:
    policy_fingerprint: str | None


@dataclass(frozen=True)
class _CapabilityStub:
    settings_hash: str


class TestExecutionPackageArtifactConstruction:
    """Test that ExecutionPackageArtifact can be constructed with all fields."""

    @staticmethod
    def test_construct_with_all_fields() -> None:
        """Construct an artifact with all required fields."""
        pkg = ExecutionPackageArtifact(
            package_fingerprint="abc123",
            manifest_hash="mh_001",
            policy_artifact_hash="ph_001",
            capability_snapshot_hash="ch_001",
            plan_bundle_fingerprints={"view_a": "fp_a", "view_b": "fp_b"},
            session_config_hash="sh_001",
            created_at_unix_ms=FIXED_CREATED_AT_UNIX_MS,
        )
        assert pkg.package_fingerprint == "abc123"
        assert pkg.manifest_hash == "mh_001"
        assert pkg.policy_artifact_hash == "ph_001"
        assert pkg.capability_snapshot_hash == "ch_001"
        assert pkg.plan_bundle_fingerprints == {"view_a": "fp_a", "view_b": "fp_b"}
        assert pkg.session_config_hash == "sh_001"
        assert pkg.created_at_unix_ms == FIXED_CREATED_AT_UNIX_MS

    @staticmethod
    def test_frozen_immutability() -> None:
        """Verify the artifact is frozen and immutable."""
        pkg = ExecutionPackageArtifact(
            package_fingerprint="abc",
            manifest_hash="m",
            policy_artifact_hash="p",
            capability_snapshot_hash="c",
            plan_bundle_fingerprints={},
            session_config_hash="s",
            created_at_unix_ms=0,
        )
        assert_immutable_assignment(
            factory=lambda: pkg,
            attribute="package_fingerprint",
            attempted_value="new",
            expected_exception=AttributeError,
        )


class TestBuildExecutionPackageDeterminism:
    """Test that build_execution_package produces deterministic fingerprints."""

    @staticmethod
    def test_same_inputs_same_fingerprint() -> None:
        """Identical inputs produce identical package fingerprints."""
        manifest = _ManifestStub(model_hash="hash_abc")
        policy = _PolicyStub(policy_fingerprint="policy_xyz")
        capability = _CapabilityStub(settings_hash="cap_123")
        bundles = {"view_a": "fp_a", "view_b": "fp_b"}

        pkg1 = build_execution_package(
            manifest=manifest,
            compiled_policy=policy,
            capability_snapshot=capability,
            plan_bundle_fingerprints=bundles,
            session_config="config_hash_1",
        )
        pkg2 = build_execution_package(
            manifest=manifest,
            compiled_policy=policy,
            capability_snapshot=capability,
            plan_bundle_fingerprints=bundles,
            session_config="config_hash_1",
        )
        assert pkg1.package_fingerprint == pkg2.package_fingerprint
        assert pkg1.manifest_hash == pkg2.manifest_hash
        assert pkg1.policy_artifact_hash == pkg2.policy_artifact_hash
        assert pkg1.capability_snapshot_hash == pkg2.capability_snapshot_hash

    @staticmethod
    def test_different_manifest_different_fingerprint() -> None:
        """Different manifest produces a different package fingerprint."""
        manifest_a = _ManifestStub(model_hash="hash_a")
        manifest_b = _ManifestStub(model_hash="hash_b")

        pkg_a = build_execution_package(manifest=manifest_a)
        pkg_b = build_execution_package(manifest=manifest_b)
        assert pkg_a.package_fingerprint != pkg_b.package_fingerprint
        assert pkg_a.manifest_hash != pkg_b.manifest_hash

    @staticmethod
    def test_different_policy_different_fingerprint() -> None:
        """Different compiled policy produces a different fingerprint."""
        policy_a = _PolicyStub(policy_fingerprint="fp_a")
        policy_b = _PolicyStub(policy_fingerprint="fp_b")

        pkg_a = build_execution_package(compiled_policy=policy_a)
        pkg_b = build_execution_package(compiled_policy=policy_b)
        assert pkg_a.package_fingerprint != pkg_b.package_fingerprint

    @staticmethod
    def test_different_capability_different_fingerprint() -> None:
        """Different capability snapshot produces a different fingerprint."""
        cap_a = _CapabilityStub(settings_hash="cap_a")
        cap_b = _CapabilityStub(settings_hash="cap_b")

        pkg_a = build_execution_package(capability_snapshot=cap_a)
        pkg_b = build_execution_package(capability_snapshot=cap_b)
        assert pkg_a.package_fingerprint != pkg_b.package_fingerprint

    @staticmethod
    def test_different_plan_bundles_different_fingerprint() -> None:
        """Different plan bundle fingerprints produce a different fingerprint."""
        pkg_a = build_execution_package(
            plan_bundle_fingerprints={"view_a": "fp_1"},
        )
        pkg_b = build_execution_package(
            plan_bundle_fingerprints={"view_a": "fp_2"},
        )
        assert pkg_a.package_fingerprint != pkg_b.package_fingerprint

    @staticmethod
    def test_different_session_config_different_fingerprint() -> None:
        """Different session config produces a different fingerprint."""
        pkg_a = build_execution_package(session_config="config_a")
        pkg_b = build_execution_package(session_config="config_b")
        assert pkg_a.package_fingerprint != pkg_b.package_fingerprint


class TestBuildExecutionPackageTimestamp:
    """Test that created_at_unix_ms is populated correctly."""

    @staticmethod
    def test_timestamp_is_populated() -> None:
        """Verify the timestamp is a reasonable value."""
        before_ms = int(time.time() * 1000)
        pkg = build_execution_package()
        after_ms = int(time.time() * 1000)

        assert before_ms <= pkg.created_at_unix_ms <= after_ms

    @staticmethod
    def test_timestamp_is_positive() -> None:
        """Verify the timestamp is a positive integer."""
        pkg = build_execution_package()
        assert pkg.created_at_unix_ms > 0


class TestBuildExecutionPackagePlanBundleFingerprints:
    """Test plan_bundle_fingerprints handling."""

    @staticmethod
    def test_none_plan_bundles_produce_empty_dict() -> None:
        """None plan bundles normalize to an empty dict."""
        pkg = build_execution_package(plan_bundle_fingerprints=None)
        assert pkg.plan_bundle_fingerprints == {}

    @staticmethod
    def test_plan_bundles_are_sorted() -> None:
        """Plan bundle fingerprints are sorted by key."""
        bundles = {"z_view": "fp_z", "a_view": "fp_a", "m_view": "fp_m"}
        pkg = build_execution_package(plan_bundle_fingerprints=bundles)
        keys = list(pkg.plan_bundle_fingerprints.keys())
        assert keys == sorted(keys)

    @staticmethod
    def test_plan_bundles_preserved_correctly() -> None:
        """All plan bundle entries are preserved in the output."""
        bundles = {"view_1": "fp_1", "view_2": "fp_2", "view_3": "fp_3"}
        pkg = build_execution_package(plan_bundle_fingerprints=bundles)
        assert len(pkg.plan_bundle_fingerprints) == EXPECTED_PLAN_BUNDLE_COUNT
        for key, value in bundles.items():
            assert pkg.plan_bundle_fingerprints[key] == value


class TestBuildExecutionPackageGracefulDegradation:
    """Test graceful handling of missing or None inputs."""

    @staticmethod
    def test_all_none_inputs() -> None:
        """All None inputs produce a valid package with empty hash strings."""
        pkg = build_execution_package()
        assert not pkg.manifest_hash
        assert not pkg.policy_artifact_hash
        assert not pkg.capability_snapshot_hash
        assert not pkg.session_config_hash
        assert pkg.plan_bundle_fingerprints == {}
        assert isinstance(pkg.package_fingerprint, str)
        assert len(pkg.package_fingerprint) > 0

    @staticmethod
    def test_manifest_fallback_to_ir_hash() -> None:
        """Manifest without model_hash falls back to ir_hash."""
        ir = _SemanticIrStub(ir_hash="ir_hash_value")
        manifest = _ManifestStub(model_hash=None, semantic_ir=ir)
        pkg = build_execution_package(manifest=manifest)
        assert pkg.manifest_hash == "ir_hash_value"

    @staticmethod
    def test_session_config_callable_settings_hash() -> None:
        """Session config with callable settings_hash is handled correctly."""
        mock_profile = MagicMock()
        mock_profile.settings_hash.return_value = "callable_hash_result"
        pkg = build_execution_package(session_config=mock_profile)
        assert pkg.session_config_hash == "callable_hash_result"

    @staticmethod
    def test_session_config_string_passthrough() -> None:
        """String session config is used directly as the hash."""
        pkg = build_execution_package(session_config="direct_hash_string")
        assert pkg.session_config_hash == "direct_hash_string"


class TestExecutionPackageMsgspecRoundTrip:
    """Test msgspec serialization round-trip for ExecutionPackageArtifact."""

    @staticmethod
    def test_json_roundtrip() -> None:
        """JSON encode and decode produce an equivalent artifact."""
        original = ExecutionPackageArtifact(
            package_fingerprint="fp_test",
            manifest_hash="mh",
            policy_artifact_hash="ph",
            capability_snapshot_hash="ch",
            plan_bundle_fingerprints={"v1": "f1", "v2": "f2"},
            session_config_hash="sh",
            created_at_unix_ms=FIXED_CREATED_AT_UNIX_MS,
        )
        encoded = msgspec.json.encode(original)
        decoded = msgspec.json.decode(encoded, type=ExecutionPackageArtifact)
        assert decoded.package_fingerprint == original.package_fingerprint
        assert decoded.manifest_hash == original.manifest_hash
        assert decoded.policy_artifact_hash == original.policy_artifact_hash
        assert decoded.capability_snapshot_hash == original.capability_snapshot_hash
        assert decoded.plan_bundle_fingerprints == original.plan_bundle_fingerprints
        assert decoded.session_config_hash == original.session_config_hash
        assert decoded.created_at_unix_ms == original.created_at_unix_ms

    @staticmethod
    def test_msgpack_roundtrip() -> None:
        """Msgpack encode and decode produce an equivalent artifact."""
        original = ExecutionPackageArtifact(
            package_fingerprint="fp_msgpack",
            manifest_hash="mh_mp",
            policy_artifact_hash="ph_mp",
            capability_snapshot_hash="ch_mp",
            plan_bundle_fingerprints={"view_x": "fp_x"},
            session_config_hash="sh_mp",
            created_at_unix_ms=FIXED_CREATED_AT_UNIX_MS,
        )
        encoded = msgspec.msgpack.encode(original)
        decoded = msgspec.msgpack.decode(encoded, type=ExecutionPackageArtifact)
        assert decoded.package_fingerprint == original.package_fingerprint
        assert decoded.plan_bundle_fingerprints == original.plan_bundle_fingerprints

    @staticmethod
    def test_empty_plan_bundles_roundtrip() -> None:
        """Empty plan_bundle_fingerprints survives serialization."""
        original = ExecutionPackageArtifact(
            package_fingerprint="fp_empty",
            manifest_hash="",
            policy_artifact_hash="",
            capability_snapshot_hash="",
            plan_bundle_fingerprints={},
            session_config_hash="",
            created_at_unix_ms=0,
        )
        encoded = msgspec.json.encode(original)
        decoded = msgspec.json.decode(encoded, type=ExecutionPackageArtifact)
        assert decoded.plan_bundle_fingerprints == {}


class TestExecutionPackageSpecRegistration:
    """Test that the artifact spec is properly registered."""

    @staticmethod
    def test_spec_is_registered() -> None:
        """EXECUTION_PACKAGE_SPEC is available in the global registry."""
        from serde_artifact_specs import EXECUTION_PACKAGE_SPEC

        assert EXECUTION_PACKAGE_SPEC.canonical_name == "execution_package_v1"

    @staticmethod
    def test_spec_has_payload_type() -> None:
        """EXECUTION_PACKAGE_SPEC has the correct payload type."""
        from serde_artifact_specs import EXECUTION_PACKAGE_SPEC

        assert EXECUTION_PACKAGE_SPEC.payload_type is ExecutionPackageArtifact

    @staticmethod
    def test_spec_has_fingerprint() -> None:
        """EXECUTION_PACKAGE_SPEC has a 32-char schema fingerprint."""
        from serde_artifact_specs import EXECUTION_PACKAGE_SPEC

        assert len(EXECUTION_PACKAGE_SPEC.schema_fingerprint) == SCHEMA_FINGERPRINT_LENGTH

    @staticmethod
    def test_spec_in_global_registry() -> None:
        """Verify the spec appears in the global artifact spec registry."""
        from serde_schema_registry import artifact_spec_registry

        registry = artifact_spec_registry()
        assert "execution_package_v1" in registry
