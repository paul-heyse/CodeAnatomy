"""Tests for Delta protocol helper functions added by scope item 8.9."""

from __future__ import annotations

import msgspec

from datafusion_engine.delta.protocol import (
    DeltaProtocolCompatibility,
    combined_table_features,
    delta_protocol_artifact_payload,
)
from serde_artifacts import DeltaProtocolArtifact

SCHEMA_FINGERPRINT_LENGTH = 32

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_compatibility(
    *,
    required_reader_features: tuple[str, ...] = (),
    required_writer_features: tuple[str, ...] = (),
    compatible: bool | None = True,
    reason: str | None = None,
) -> DeltaProtocolCompatibility:
    """Build a minimal ``DeltaProtocolCompatibility`` for testing.

    Returns:
    -------
    DeltaProtocolCompatibility
        Compatibility result with the given features.
    """
    return DeltaProtocolCompatibility(
        compatible=compatible,
        reason=reason,
        required_reader_features=required_reader_features,
        required_writer_features=required_writer_features,
    )


# ---------------------------------------------------------------------------
# combined_table_features
# ---------------------------------------------------------------------------


class TestCombinedTableFeatures:
    """Test the ``combined_table_features`` helper."""

    @staticmethod
    def test_empty_features() -> None:
        """Empty reader and writer features produce an empty tuple."""
        compat = _make_compatibility()
        assert combined_table_features(compat) == ()

    @staticmethod
    def test_reader_only() -> None:
        """Only reader features returns those features sorted."""
        compat = _make_compatibility(required_reader_features=("b", "a"))
        assert combined_table_features(compat) == ("a", "b")

    @staticmethod
    def test_writer_only() -> None:
        """Only writer features returns those features sorted."""
        compat = _make_compatibility(required_writer_features=("z", "m"))
        assert combined_table_features(compat) == ("m", "z")

    @staticmethod
    def test_union_deduplicated() -> None:
        """Overlapping features are deduplicated in the union."""
        compat = _make_compatibility(
            required_reader_features=("a", "b", "c"),
            required_writer_features=("b", "c", "d"),
        )
        assert combined_table_features(compat) == ("a", "b", "c", "d")

    @staticmethod
    def test_disjoint_features() -> None:
        """Disjoint feature sets are merged correctly."""
        compat = _make_compatibility(
            required_reader_features=("x",),
            required_writer_features=("y",),
        )
        assert combined_table_features(compat) == ("x", "y")


# ---------------------------------------------------------------------------
# delta_protocol_artifact_payload
# ---------------------------------------------------------------------------


class TestDeltaProtocolArtifactPayload:
    """Test the ``delta_protocol_artifact_payload`` helper."""

    @staticmethod
    def test_basic_payload_structure() -> None:
        """Payload contains all DeltaProtocolCompatibility fields."""
        compat = _make_compatibility(
            compatible=True,
            reason="protocol_unavailable",
            required_reader_features=("columnMapping",),
            required_writer_features=("appendOnly",),
        )
        payload = delta_protocol_artifact_payload(compat)
        assert payload["compatible"] is True
        assert payload["reason"] == "protocol_unavailable"
        assert payload["required_reader_features"] == ("columnMapping",)
        assert payload["required_writer_features"] == ("appendOnly",)

    @staticmethod
    def test_table_uri_included() -> None:
        """Table URI is included when provided."""
        compat = _make_compatibility()
        payload = delta_protocol_artifact_payload(compat, table_uri="/tmp/delta/table")
        assert payload["table_uri"] == "/tmp/delta/table"

    @staticmethod
    def test_dataset_name_included() -> None:
        """Dataset name is included when provided."""
        compat = _make_compatibility()
        payload = delta_protocol_artifact_payload(compat, dataset_name="my_dataset")
        assert payload["dataset_name"] == "my_dataset"

    @staticmethod
    def test_table_uri_omitted_when_none() -> None:
        """Table URI key is absent when not provided."""
        compat = _make_compatibility()
        payload = delta_protocol_artifact_payload(compat)
        assert "table_uri" not in payload

    @staticmethod
    def test_dataset_name_omitted_when_none() -> None:
        """Dataset name key is absent when not provided."""
        compat = _make_compatibility()
        payload = delta_protocol_artifact_payload(compat)
        assert "dataset_name" not in payload

    @staticmethod
    def test_payload_validates_against_artifact_schema() -> None:
        """Payload conforms to the ``DeltaProtocolArtifact`` msgspec schema."""
        compat = DeltaProtocolCompatibility(
            compatible=False,
            reason="incompatible_writer_version",
            required_reader_version=3,
            required_writer_version=7,
            supported_reader_version=3,
            supported_writer_version=5,
            required_reader_features=("columnMapping",),
            required_writer_features=("appendOnly", "timestampNtz"),
            supported_reader_features=("columnMapping",),
            supported_writer_features=("appendOnly",),
            missing_reader_features=(),
            missing_writer_features=("timestampNtz",),
            reader_version_ok=True,
            writer_version_ok=False,
            feature_support_ok=False,
        )
        payload = delta_protocol_artifact_payload(
            compat,
            table_uri="s3://bucket/delta/table",
            dataset_name="test_ds",
        )
        # Validate the payload can be decoded as DeltaProtocolArtifact
        artifact = msgspec.convert(payload, type=DeltaProtocolArtifact, strict=False)
        assert artifact.compatible is False
        assert artifact.table_uri == "s3://bucket/delta/table"
        assert artifact.dataset_name == "test_ds"
        assert artifact.missing_writer_features == ("timestampNtz",)


# ---------------------------------------------------------------------------
# DeltaProtocolArtifact spec registration
# ---------------------------------------------------------------------------


class TestDeltaProtocolArtifactSpec:
    """Test the artifact spec registration for delta_protocol_compatibility_v1."""

    @staticmethod
    def test_spec_registered() -> None:
        """delta_protocol_compatibility_v1 is registered in the global registry."""
        import serde_artifact_specs  # triggers registration

        assert serde_artifact_specs.__all__
        from serde_schema_registry import artifact_spec_registry

        registry = artifact_spec_registry()
        assert "delta_protocol_compatibility_v1" in registry

    @staticmethod
    def test_spec_has_payload_type() -> None:
        """Spec payload type is DeltaProtocolArtifact."""
        from serde_artifact_specs import DELTA_PROTOCOL_ARTIFACT_SPEC

        assert DELTA_PROTOCOL_ARTIFACT_SPEC.payload_type is DeltaProtocolArtifact
        assert DELTA_PROTOCOL_ARTIFACT_SPEC.canonical_name == "delta_protocol_compatibility_v1"
        assert len(DELTA_PROTOCOL_ARTIFACT_SPEC.schema_fingerprint) == SCHEMA_FINGERPRINT_LENGTH
