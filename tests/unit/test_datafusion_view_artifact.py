"""Test DataFusion-native view artifact creation."""

from __future__ import annotations

from unittest.mock import Mock

import pyarrow as pa
import pytest

from datafusion_engine.plan_bundle import DataFusionPlanBundle, PlanArtifacts
from datafusion_engine.view_artifacts import (
    DataFusionViewArtifact,
    ViewArtifactLineage,
    ViewArtifactRequest,
    build_view_artifact_from_bundle,
)


def test_datafusion_view_artifact_creation() -> None:
    """Test DataFusionViewArtifact can be created and used."""
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

    artifact = DataFusionViewArtifact(
        name="test_view",
        plan_fingerprint="test_fingerprint_123",
        plan_task_signature="sig:test_fingerprint_123",
        schema=schema,
        required_udfs=("udf_a", "udf_b"),
        referenced_tables=("table1", "table2"),
    )

    assert artifact.name == "test_view"
    assert artifact.plan_fingerprint == "test_fingerprint_123"
    assert artifact.plan_task_signature == "sig:test_fingerprint_123"
    assert artifact.schema == schema
    assert artifact.required_udfs == ("udf_a", "udf_b")
    assert artifact.referenced_tables == ("table1", "table2")


def test_datafusion_view_artifact_payload() -> None:
    """Test DataFusionViewArtifact payload generation."""
    schema = pa.schema([pa.field("id", pa.int64())])

    artifact = DataFusionViewArtifact(
        name="test_view",
        plan_fingerprint="test_fp",
        plan_task_signature="sig:test_fp",
        schema=schema,
        required_udfs=("udf_x",),
        referenced_tables=("table_y",),
    )

    payload = artifact.payload()

    assert payload["name"] == "test_view"
    assert payload["plan_fingerprint"] == "test_fp"
    assert payload["plan_task_signature"] == "sig:test_fp"
    assert "schema" in payload
    assert payload["schema_describe"] == []
    assert payload["schema_provenance"] == {}
    assert payload["required_udfs"] == ["udf_x"]
    assert payload["referenced_tables"] == ["table_y"]


def test_datafusion_view_artifact_diagnostics_payload() -> None:
    """Test DataFusionViewArtifact diagnostics payload generation."""
    schema = pa.schema([pa.field("id", pa.int64())])

    artifact = DataFusionViewArtifact(
        name="test_view",
        plan_fingerprint="test_fp",
        plan_task_signature="sig:test_fp",
        schema=schema,
        required_udfs=(),
        referenced_tables=(),
    )

    event_time = 1234567890000
    payload = artifact.diagnostics_payload(event_time_unix_ms=event_time)

    assert payload["event_time_unix_ms"] == event_time
    assert payload["name"] == "test_view"
    assert payload["plan_fingerprint"] == "test_fp"
    assert payload["plan_task_signature"] == "sig:test_fp"
    assert "schema_fingerprint" in payload
    assert "schema_msgpack" in payload
    assert "schema_describe_json" in payload
    assert "schema_provenance_json" in payload
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
        request=ViewArtifactRequest(
            name="test_view",
            schema=schema,
            lineage=ViewArtifactLineage(
                required_udfs=("udf_a",),
                referenced_tables=("table1",),
            ),
        ),
    )

    assert isinstance(artifact, DataFusionViewArtifact)
    assert artifact.name == "test_view"
    assert artifact.plan_fingerprint == mock_plan_bundle.plan_fingerprint
    assert isinstance(artifact.plan_task_signature, str)
    assert artifact.plan_task_signature
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
    artifacts = PlanArtifacts(
        explain_tree_rows=None,
        explain_verbose_rows=None,
        explain_analyze_duration_ms=None,
        explain_analyze_output_rows=None,
        df_settings={},
        planning_env_snapshot={},
        planning_env_hash="mock_env_hash",
        rulepack_snapshot=None,
        rulepack_hash=None,
        information_schema_snapshot={},
        information_schema_hash="mock_info_schema_hash",
        substrait_validation=None,
        logical_plan_proto=None,
        optimized_plan_proto=None,
        execution_plan_proto=None,
        udf_snapshot_hash="mock_udf_hash",
        function_registry_hash="mock_registry_hash",
        function_registry_snapshot={},
        rewrite_tags=(),
        domain_planner_names=(),
        udf_snapshot={},
        udf_planner_snapshot=None,
    )

    return DataFusionPlanBundle(
        df=mock_df,
        logical_plan=mock_logical_plan,
        optimized_logical_plan=mock_optimized_plan,
        execution_plan=None,
        substrait_bytes=None,
        plan_fingerprint="mock_fp_12345",
        artifacts=artifacts,
        plan_details={},
    )
