"""Tests for ArtifactSpec and ArtifactSpecRegistry governance infrastructure."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import msgspec
import pytest

from serde_schema_registry import (
    ArtifactSpec,
    ArtifactSpecRegistry,
    artifact_spec_registry,
    get_artifact_spec,
    register_artifact_spec,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _SamplePayload(msgspec.Struct, frozen=True):
    """Sample payload for testing spec validation."""

    name: str
    value: int
    optional_field: str | None = None


class _OtherPayload(msgspec.Struct, frozen=True):
    """Another sample payload to test fingerprint differences."""

    key: str


# ---------------------------------------------------------------------------
# ArtifactSpec creation and properties
# ---------------------------------------------------------------------------


class TestArtifactSpecCreation:
    """Test ArtifactSpec construction and schema fingerprinting."""

    def test_create_spec_with_payload_type(self) -> None:
        """Create spec with a payload type and verify fingerprint is computed."""
        spec = ArtifactSpec(
            canonical_name="test_artifact_v1",
            description="A test artifact.",
            payload_type=_SamplePayload,
        )
        assert spec.canonical_name == "test_artifact_v1"
        assert spec.description == "A test artifact."
        assert spec.payload_type is _SamplePayload
        assert spec.version == 1
        assert len(spec.schema_fingerprint) == 32

    def test_create_spec_without_payload_type(self) -> None:
        """Create spec without a payload type (untyped artifact)."""
        spec = ArtifactSpec(
            canonical_name="untyped_v1",
            description="Untyped artifact.",
        )
        assert spec.payload_type is None
        assert spec.schema_fingerprint == ""
        assert spec.version == 1

    def test_fingerprint_deterministic(self) -> None:
        """Schema fingerprint is deterministic across instances."""
        spec_a = ArtifactSpec(
            canonical_name="a_v1",
            description="First.",
            payload_type=_SamplePayload,
        )
        spec_b = ArtifactSpec(
            canonical_name="b_v1",
            description="Second.",
            payload_type=_SamplePayload,
        )
        assert spec_a.schema_fingerprint == spec_b.schema_fingerprint

    def test_fingerprint_differs_for_different_types(self) -> None:
        """Different payload types produce different fingerprints."""
        spec_a = ArtifactSpec(
            canonical_name="a_v1",
            description="First.",
            payload_type=_SamplePayload,
        )
        spec_b = ArtifactSpec(
            canonical_name="b_v1",
            description="Second.",
            payload_type=_OtherPayload,
        )
        assert spec_a.schema_fingerprint != spec_b.schema_fingerprint

    def test_custom_version(self) -> None:
        """Spec version defaults to 1 but can be overridden."""
        spec = ArtifactSpec(
            canonical_name="versioned_v2",
            description="Version 2.",
            version=2,
        )
        assert spec.version == 2

    def test_fingerprint_is_hex_string(self) -> None:
        """Schema fingerprint is a valid hex string."""
        spec = ArtifactSpec(
            canonical_name="hex_v1",
            description="Hex test.",
            payload_type=_SamplePayload,
        )
        int(spec.schema_fingerprint, 16)  # Should not raise

    def test_fingerprint_length_is_32(self) -> None:
        """Schema fingerprint is always 32 characters (128-bit truncation)."""
        spec = ArtifactSpec(
            canonical_name="len_v1",
            description="Length test.",
            payload_type=_OtherPayload,
        )
        assert len(spec.schema_fingerprint) == 32


# ---------------------------------------------------------------------------
# ArtifactSpec.validate
# ---------------------------------------------------------------------------


class TestArtifactSpecValidation:
    """Test ArtifactSpec payload validation."""

    def test_validate_valid_payload(self) -> None:
        """Valid payload passes validation without error."""
        spec = ArtifactSpec(
            canonical_name="test_v1",
            description="Test.",
            payload_type=_SamplePayload,
        )
        spec.validate({"name": "hello", "value": 42})

    def test_validate_valid_payload_with_optional(self) -> None:
        """Valid payload with optional fields passes validation."""
        spec = ArtifactSpec(
            canonical_name="test_v1",
            description="Test.",
            payload_type=_SamplePayload,
        )
        spec.validate({"name": "hello", "value": 42, "optional_field": "extra"})

    def test_validate_invalid_payload_raises(self) -> None:
        """Invalid payload raises ValidationError."""
        spec = ArtifactSpec(
            canonical_name="test_v1",
            description="Test.",
            payload_type=_SamplePayload,
        )
        with pytest.raises(msgspec.ValidationError):
            spec.validate({"name": "hello", "value": "not_an_int"})

    def test_validate_missing_required_field_raises(self) -> None:
        """Payload missing a required field raises ValidationError."""
        spec = ArtifactSpec(
            canonical_name="test_v1",
            description="Test.",
            payload_type=_SamplePayload,
        )
        with pytest.raises(msgspec.ValidationError):
            spec.validate({"name": "hello"})

    def test_validate_untyped_is_noop(self) -> None:
        """Untyped spec validation is a no-op (accepts anything)."""
        spec = ArtifactSpec(
            canonical_name="untyped_v1",
            description="Untyped.",
        )
        spec.validate({"anything": "goes"})
        spec.validate({})

    def test_validate_untyped_accepts_empty_mapping(self) -> None:
        """Untyped spec accepts an empty mapping without error."""
        spec = ArtifactSpec(
            canonical_name="untyped_empty_v1",
            description="Untyped accepts empty.",
        )
        spec.validate({})

    def test_validate_untyped_accepts_arbitrary_keys(self) -> None:
        """Untyped spec silently accepts any key structure."""
        spec = ArtifactSpec(
            canonical_name="untyped_keys_v1",
            description="Untyped keys.",
        )
        spec.validate({"foo": 1, "bar": [1, 2, 3], "nested": {"a": True}})


# ---------------------------------------------------------------------------
# ArtifactSpecRegistry
# ---------------------------------------------------------------------------


class TestArtifactSpecRegistry:
    """Test ArtifactSpecRegistry operations."""

    def test_register_and_get(self) -> None:
        """Register and retrieve an artifact spec."""
        registry = ArtifactSpecRegistry()
        spec = ArtifactSpec(
            canonical_name="my_artifact_v1",
            description="Test artifact.",
        )
        registry.register("my_artifact_v1", spec)
        assert registry.get("my_artifact_v1") is spec

    def test_get_missing_returns_none(self) -> None:
        """Get on missing key returns None."""
        registry = ArtifactSpecRegistry()
        assert registry.get("nonexistent") is None

    def test_contains(self) -> None:
        """Check __contains__ works."""
        registry = ArtifactSpecRegistry()
        spec = ArtifactSpec(canonical_name="x_v1", description="X.")
        registry.register("x_v1", spec)
        assert "x_v1" in registry
        assert "y_v1" not in registry

    def test_len(self) -> None:
        """Check __len__ reflects registered count."""
        registry = ArtifactSpecRegistry()
        assert len(registry) == 0
        registry.register("a_v1", ArtifactSpec(canonical_name="a_v1", description="A."))
        assert len(registry) == 1
        registry.register("b_v1", ArtifactSpec(canonical_name="b_v1", description="B."))
        assert len(registry) == 2

    def test_iter(self) -> None:
        """Check __iter__ yields registered keys."""
        registry = ArtifactSpecRegistry()
        registry.register("a_v1", ArtifactSpec(canonical_name="a_v1", description="A."))
        registry.register("b_v1", ArtifactSpec(canonical_name="b_v1", description="B."))
        assert sorted(registry) == ["a_v1", "b_v1"]

    def test_snapshot(self) -> None:
        """Snapshot returns a copy of registry state."""
        registry = ArtifactSpecRegistry()
        spec = ArtifactSpec(canonical_name="s_v1", description="S.")
        registry.register("s_v1", spec)
        snap = registry.snapshot()
        assert isinstance(snap, Mapping)
        assert "s_v1" in snap
        assert snap["s_v1"] is spec

    def test_restore(self) -> None:
        """Restore replaces registry state from a snapshot."""
        registry = ArtifactSpecRegistry()
        spec = ArtifactSpec(canonical_name="r_v1", description="R.")
        registry.register("r_v1", spec)
        snap = registry.snapshot()
        registry.register("extra_v1", ArtifactSpec(canonical_name="extra_v1", description="E."))
        assert len(registry) == 2
        registry.restore(snap)
        assert len(registry) == 1
        assert "r_v1" in registry
        assert "extra_v1" not in registry


# ---------------------------------------------------------------------------
# Global registry and register helper
# ---------------------------------------------------------------------------


class TestGlobalRegistry:
    """Test the global artifact spec registry and helper."""

    def test_global_registry_is_populated(self) -> None:
        """Global registry should have specs after importing serde_artifact_specs."""
        import serde_artifact_specs

        registry = artifact_spec_registry()
        assert len(registry) > 0
        assert len(serde_artifact_specs.__all__) > 0

    def test_known_specs_registered(self) -> None:
        """Verify known spec canonical names are registered."""
        import serde_artifact_specs

        registry = artifact_spec_registry()
        assert serde_artifact_specs.VIEW_CACHE_ARTIFACT_SPEC is not None
        expected_names = [
            "view_cache_artifact_v1",
            "delta_protocol_compatibility_v1",
            "delta_stats_decision_v1",
            "semantic_validation_v1",
            "plan_schedule_v1",
            "plan_validation_v1",
            "run_manifest_v1",
            "normalize_outputs_v1",
            "extract_errors_v1",
            "runtime_profile_snapshot_v1",
            "incremental_metadata_v1",
            "compiled_execution_policy_v1",
            "execution_package_v1",
            "workload_classification_v1",
            "pruning_metrics_v1",
            "decision_provenance_graph_v1",
            "policy_counterfactual_replay_v1",
            "fallback_quarantine_v1",
            "policy_calibration_result_v1",
        ]
        for name in expected_names:
            assert name in registry, f"Expected {name!r} in artifact spec registry"

    def test_register_artifact_spec_returns_spec(self) -> None:
        """register_artifact_spec returns the registered spec."""
        spec = ArtifactSpec(
            canonical_name="test_global_v1",
            description="Test global registration.",
        )
        result = register_artifact_spec(spec)
        assert result is spec
        assert artifact_spec_registry().get("test_global_v1") is spec

    def test_get_artifact_spec_helper(self) -> None:
        """get_artifact_spec returns spec by canonical name."""
        spec = get_artifact_spec("view_cache_artifact_v1")
        assert spec is not None
        assert spec.canonical_name == "view_cache_artifact_v1"

    def test_get_artifact_spec_missing(self) -> None:
        """get_artifact_spec returns None for unknown names."""
        assert get_artifact_spec("nonexistent_artifact_v99") is None


# ---------------------------------------------------------------------------
# Spec constants from serde_artifacts
# ---------------------------------------------------------------------------


class TestSerdeArtifactSpecs:
    """Test that the spec constants from serde_artifacts are well-formed."""

    def test_view_cache_artifact_spec(self) -> None:
        """VIEW_CACHE_ARTIFACT_SPEC has expected properties."""
        from serde_artifact_specs import VIEW_CACHE_ARTIFACT_SPEC
        from serde_artifacts import ViewCacheArtifact

        assert VIEW_CACHE_ARTIFACT_SPEC.canonical_name == "view_cache_artifact_v1"
        assert VIEW_CACHE_ARTIFACT_SPEC.payload_type is ViewCacheArtifact
        assert len(VIEW_CACHE_ARTIFACT_SPEC.schema_fingerprint) == 32

    def test_run_manifest_spec(self) -> None:
        """RUN_MANIFEST_SPEC has expected properties."""
        from serde_artifact_specs import RUN_MANIFEST_SPEC
        from serde_artifacts import RunManifest

        assert RUN_MANIFEST_SPEC.canonical_name == "run_manifest_v1"
        assert RUN_MANIFEST_SPEC.payload_type is RunManifest
        assert len(RUN_MANIFEST_SPEC.schema_fingerprint) == 32

    def test_plan_schedule_spec(self) -> None:
        """PLAN_SCHEDULE_SPEC has expected properties."""
        from serde_artifact_specs import PLAN_SCHEDULE_SPEC
        from serde_artifacts import PlanScheduleArtifact

        assert PLAN_SCHEDULE_SPEC.canonical_name == "plan_schedule_v1"
        assert PLAN_SCHEDULE_SPEC.payload_type is PlanScheduleArtifact

    def test_write_artifact_spec(self) -> None:
        """WRITE_ARTIFACT_SPEC links to WriteArtifactRow."""
        from serde_artifact_specs import WRITE_ARTIFACT_SPEC
        from serde_artifacts import WriteArtifactRow

        assert WRITE_ARTIFACT_SPEC.canonical_name == "write_artifact_v2"
        assert WRITE_ARTIFACT_SPEC.payload_type is WriteArtifactRow
        assert len(WRITE_ARTIFACT_SPEC.schema_fingerprint) == 32

    def test_datafusion_view_artifacts_spec(self) -> None:
        """DATAFUSION_VIEW_ARTIFACTS_SPEC links to ViewArtifactPayload."""
        from serde_artifact_specs import DATAFUSION_VIEW_ARTIFACTS_SPEC
        from serde_artifacts import ViewArtifactPayload

        assert DATAFUSION_VIEW_ARTIFACTS_SPEC.canonical_name == "datafusion_view_artifacts_v4"
        assert DATAFUSION_VIEW_ARTIFACTS_SPEC.payload_type is ViewArtifactPayload
        assert len(DATAFUSION_VIEW_ARTIFACTS_SPEC.schema_fingerprint) == 32


# ---------------------------------------------------------------------------
# New spec constants (wave 5 additions)
# ---------------------------------------------------------------------------


class TestNewArtifactSpecs:
    """Test the new artifact spec constants added in the wave-5 expansion."""

    def test_all_new_specs_are_registered(self) -> None:
        """Every spec exported from serde_artifact_specs is in the global registry."""
        import serde_artifact_specs

        registry = artifact_spec_registry()
        for attr_name in serde_artifact_specs.__all__:
            spec = getattr(serde_artifact_specs, attr_name)
            assert isinstance(spec, ArtifactSpec), f"{attr_name} is not an ArtifactSpec"
            assert spec.canonical_name in registry, (
                f"{attr_name} ({spec.canonical_name!r}) not in registry"
            )
            assert registry.get(spec.canonical_name) is spec

    def test_all_new_specs_have_descriptions(self) -> None:
        """Every spec has a non-empty description."""
        import serde_artifact_specs

        for attr_name in serde_artifact_specs.__all__:
            spec = getattr(serde_artifact_specs, attr_name)
            assert isinstance(spec, ArtifactSpec)
            assert spec.description, f"{attr_name} has empty description"

    def test_all_new_specs_have_canonical_names(self) -> None:
        """Every spec has a non-empty canonical_name."""
        import serde_artifact_specs

        for attr_name in serde_artifact_specs.__all__:
            spec = getattr(serde_artifact_specs, attr_name)
            assert isinstance(spec, ArtifactSpec)
            assert spec.canonical_name, f"{attr_name} has empty canonical_name"

    def test_typed_specs_have_fingerprints(self) -> None:
        """Specs with payload_type have 32-char fingerprints."""
        import serde_artifact_specs

        for attr_name in serde_artifact_specs.__all__:
            spec = getattr(serde_artifact_specs, attr_name)
            if not isinstance(spec, ArtifactSpec):
                continue
            if spec.payload_type is not None:
                assert len(spec.schema_fingerprint) == 32, (
                    f"{attr_name} has fingerprint length "
                    f"{len(spec.schema_fingerprint)}, expected 32"
                )

    def test_untyped_specs_have_no_fingerprint(self) -> None:
        """Specs without payload_type have an empty fingerprint."""
        import serde_artifact_specs

        for attr_name in serde_artifact_specs.__all__:
            spec = getattr(serde_artifact_specs, attr_name)
            if not isinstance(spec, ArtifactSpec):
                continue
            if spec.payload_type is None:
                assert spec.schema_fingerprint == "", (
                    f"{attr_name} has non-empty fingerprint but no payload_type"
                )

    def test_untyped_spec_validates_anything(self) -> None:
        """Untyped specs accept any payload without raising."""
        from serde_artifact_specs import DELTA_MAINTENANCE_SPEC

        assert DELTA_MAINTENANCE_SPEC.payload_type is None
        DELTA_MAINTENANCE_SPEC.validate({"table_uri": "/tmp/test", "operation": "optimize"})
        DELTA_MAINTENANCE_SPEC.validate({})

    def test_new_canonical_names_registered(self) -> None:
        """Verify the 14 new canonical names are registered."""
        registry = artifact_spec_registry()
        new_names = [
            "write_artifact_v2",
            "datafusion_view_artifacts_v4",
            "delta_maintenance_v1",
            "plan_execute_v1",
            "schema_contract_violations_v1",
            "view_contract_violations_v1",
            "view_udf_parity_v1",
            "view_fingerprints_v1",
            "pipeline_cache_lineage_v2",
            "dataset_readiness_v1",
            "semantic_quality_artifact_v1",
            "datafusion_delta_commit_v1",
            "datafusion_run_started_v1",
            "datafusion_run_finished_v1",
        ]
        for name in new_names:
            assert name in registry, f"Expected {name!r} in artifact spec registry"

    def test_no_duplicate_canonical_names(self) -> None:
        """All spec constants use unique canonical names."""
        import serde_artifact_specs

        seen: dict[str, str] = {}
        for attr_name in serde_artifact_specs.__all__:
            spec = getattr(serde_artifact_specs, attr_name)
            if not isinstance(spec, ArtifactSpec):
                continue
            if spec.canonical_name in seen:
                pytest.fail(
                    f"Duplicate canonical_name {spec.canonical_name!r}: "
                    f"{seen[spec.canonical_name]} and {attr_name}"
                )
            seen[spec.canonical_name] = attr_name

    def test_total_spec_count(self) -> None:
        """Verify the total number of registered specs is at least 145."""
        registry = artifact_spec_registry()
        assert len(registry) >= 145, f"Expected at least 145 specs, found {len(registry)}"


# ---------------------------------------------------------------------------
# Typed record_artifact contract
# ---------------------------------------------------------------------------


class TestRecordArtifactTypedContract:
    """Test that record_artifact requires typed ArtifactSpec inputs."""

    def test_record_artifact_with_spec(self) -> None:
        """Recording with an ArtifactSpec resolves canonical name via recorder."""
        from datafusion_engine.lineage.diagnostics import (
            DiagnosticsContext,
            DiagnosticsRecorder,
            InMemoryDiagnosticsSink,
        )

        sink = InMemoryDiagnosticsSink()
        context = DiagnosticsContext(session_id="test", operation_id="op")
        recorder = DiagnosticsRecorder(sink, context)
        spec = ArtifactSpec(
            canonical_name="typed_test_v1",
            description="Typed test artifact.",
        )
        recorder.record_artifact(spec, {"key": "value"})
        artifacts = sink.get_artifacts("typed_test_v1")
        assert len(artifacts) == 1
        assert artifacts[0]["key"] == "value"

    def test_record_artifact_spec_in_snapshot(self) -> None:
        """Artifacts recorded via spec appear in snapshot by canonical name."""
        from datafusion_engine.lineage.diagnostics import (
            DiagnosticsContext,
            DiagnosticsRecorder,
            InMemoryDiagnosticsSink,
        )

        sink = InMemoryDiagnosticsSink()
        context = DiagnosticsContext(session_id="test", operation_id="op")
        recorder = DiagnosticsRecorder(sink, context)
        spec = ArtifactSpec(
            canonical_name="snap_test_v1",
            description="Snapshot test.",
        )
        recorder.record_artifact(spec, {"a": 1})
        snapshot = sink.artifacts_snapshot()
        assert "snap_test_v1" in snapshot

    def test_diagnostics_recorder_with_spec(self) -> None:
        """DiagnosticsRecorder accepts ArtifactSpec for record_artifact."""
        from datafusion_engine.lineage.diagnostics import (
            DiagnosticsContext,
            DiagnosticsRecorder,
            InMemoryDiagnosticsSink,
        )

        sink = InMemoryDiagnosticsSink()
        context = DiagnosticsContext(session_id="test", operation_id="op")
        recorder = DiagnosticsRecorder(sink, context)
        spec = ArtifactSpec(
            canonical_name="recorder_test_v1",
            description="Recorder test.",
        )
        recorder.record_artifact(spec, {"data": True})
        artifacts = sink.get_artifacts("recorder_test_v1")
        assert len(artifacts) == 1

    def test_diagnostics_recorder_adapter_with_spec(self) -> None:
        """DiagnosticsRecorderAdapter works with typed ArtifactSpec values."""
        from datafusion_engine.lineage.diagnostics import (
            DiagnosticsRecorderAdapter,
            InMemoryDiagnosticsSink,
        )

        sink = InMemoryDiagnosticsSink()
        adapter = DiagnosticsRecorderAdapter(
            sink=sink,
            session_id="test-session",
        )
        spec = ArtifactSpec(
            canonical_name="adapter_test_v1",
            description="Adapter test.",
        )
        adapter.record_artifact(spec, {"value": 42})
        artifacts = sink.get_artifacts("adapter_test_v1")
        assert len(artifacts) == 1

    def test_runtime_record_artifact_rejects_non_artifact_spec(self) -> None:
        """Runtime record_artifact rejects non-ArtifactSpec name values."""
        from datafusion_engine.session.runtime import record_artifact

        with pytest.raises(TypeError, match="requires an ArtifactSpec instance"):
            record_artifact(
                cast("Any", object()),
                cast("Any", "not_a_spec"),
                {"x": 1},
            )

    def test_lineage_record_artifact_rejects_non_artifact_spec(self) -> None:
        """Lineage record_artifact rejects non-ArtifactSpec name values."""
        from datafusion_engine.lineage.diagnostics import record_artifact

        with pytest.raises(TypeError, match="requires an ArtifactSpec instance"):
            record_artifact(
                None,
                cast("Any", "not_a_spec"),
                {"x": 1},
            )
