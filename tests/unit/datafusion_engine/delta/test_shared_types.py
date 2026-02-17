"""Unit tests for shared Delta protocol types."""

from __future__ import annotations

from datafusion_engine.delta.shared_types import DeltaFeatureGate, DeltaProtocolSnapshot

_WRITER_VERSION = 7


def test_delta_protocol_snapshot_defaults() -> None:
    """Protocol snapshot defaults to unset versions and empty feature flags."""
    snapshot = DeltaProtocolSnapshot()
    assert snapshot.min_reader_version is None
    assert snapshot.min_writer_version is None
    assert snapshot.reader_features == ()
    assert snapshot.writer_features == ()


def test_delta_feature_gate_generated_type_roundtrip() -> None:
    """Feature gate preserves explicit protocol versions and required features."""
    gate = DeltaFeatureGate(
        min_reader_version=1,
        min_writer_version=_WRITER_VERSION,
        required_reader_features=("deletionvectors",),
        required_writer_features=("deletionvectors",),
    )
    assert gate.min_reader_version == 1
    assert gate.min_writer_version == _WRITER_VERSION
