"""Test DataFusion-native view artifact creation."""

from __future__ import annotations

from unittest.mock import Mock

import pyarrow as pa
import pytest

from datafusion_engine.plan_bundle import DataFusionPlanBundle
from datafusion_engine.view_artifacts import (
    DataFusionViewArtifact,
    build_view_artifact_from_bundle,
)


def test_datafusion_view_artifact_creation() -> None:
    """Test DataFusionViewArtifact can be created and used."""
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

    artifact = DataFusionViewArtifact(
        name="test_view",
        plan_fingerprint="test_fingerprint_123",
        schema=schema,
        required_udfs=("udf_a", "udf_b"),
        referenced_tables=("table1", "table2"),
    )

    assert artifact.name == "test_view"
    assert artifact.plan_fingerprint == "test_fingerprint_123"
    assert artifact.schema == schema
    assert artifact.required_udfs == ("udf_a", "udf_b")
    assert artifact.referenced_tables == ("table1", "table2")


def test_datafusion_view_artifact_payload() -> None:
    """Test DataFusionViewArtifact payload generation."""
    schema = pa.schema([pa.field("id", pa.int64())])

    artifact = DataFusionViewArtifact(
        name="test_view",
        plan_fingerprint="test_fp",
        schema=schema,
        required_udfs=("udf_x",),
        referenced_tables=("table_y",),
    )

    payload = artifact.payload()

    assert payload["name"] == "test_view"
    assert payload["plan_fingerprint"] == "test_fp"
    assert "schema" in payload
    assert payload["required_udfs"] == ["udf_x"]
    assert payload["referenced_tables"] == ["table_y"]


def test_datafusion_view_artifact_diagnostics_payload() -> None:
    """Test DataFusionViewArtifact diagnostics payload generation."""
    schema = pa.schema([pa.field("id", pa.int64())])

    artifact = DataFusionViewArtifact(
        name="test_view",
        plan_fingerprint="test_fp",
        schema=schema,
        required_udfs=(),
        referenced_tables=(),
    )

    event_time = 1234567890000
    payload = artifact.diagnostics_payload(event_time_unix_ms=event_time)

    assert payload["event_time_unix_ms"] == event_time
    assert payload["name"] == "test_view"
    assert payload["plan_fingerprint"] == "test_fp"
    assert "schema_fingerprint" in payload
    assert "schema_msgpack" in payload
    assert payload["required_udfs"] == []
    assert payload["referenced_tables"] == []


def test_build_view_artifact_from_bundle(mock_plan_bundle: DataFusionPlanBundle) -> None:
    """Test building DataFusionViewArtifact from a plan bundle.

    Parameters
    ----------
    mock_plan_bundle
        Mock DataFusionPlanBundle fixture for testing.
    """
    schema = pa.schema([pa.field("id", pa.int64())])

    artifact = build_view_artifact_from_bundle(
        mock_plan_bundle,
        name="test_view",
        schema=schema,
        required_udfs=("udf_a",),
        referenced_tables=("table1",),
    )

    assert isinstance(artifact, DataFusionViewArtifact)
    assert artifact.name == "test_view"
    assert artifact.plan_fingerprint == mock_plan_bundle.plan_fingerprint
    assert artifact.schema == schema
    assert artifact.required_udfs == ("udf_a",)
    assert artifact.referenced_tables == ("table1",)


@pytest.fixture
def mock_plan_bundle() -> DataFusionPlanBundle:
    """Create a mock DataFusionPlanBundle for testing.

    Returns
    -------
    DataFusionPlanBundle
        Mock plan bundle with test fingerprint.
    """
    mock_df = Mock()
    mock_logical_plan = Mock()
    mock_optimized_plan = Mock()

    return DataFusionPlanBundle(
        df=mock_df,
        logical_plan=mock_logical_plan,
        optimized_logical_plan=mock_optimized_plan,
        execution_plan=None,
        substrait_bytes=None,
        plan_fingerprint="mock_fp_12345",
        plan_details={},
    )
