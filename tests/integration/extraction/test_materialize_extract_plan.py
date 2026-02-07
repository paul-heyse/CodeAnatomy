"""Integration tests for materialize_extract_plan boundary.

NOTE: Full roundtrip materialization tests require complex pipeline wiring
(UDFs, normalization context, etc.). These tests focus on API contracts and
options handling that can be validated without full pipeline infrastructure.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa
import pytest

from core_types import DeterminismTier
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, DiagnosticsConfig
from extract.coordination.evidence_plan import compile_evidence_plan
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_reader,
    materialize_extract_plan,
    record_batch_reader_from_rows,
)
from extract.session import ExtractSession, build_extract_session
from obs.diagnostics import DiagnosticsCollector


@pytest.fixture
def extract_session() -> ExtractSession:
    """Provide an ExtractSession for tests.

    Returns:
    -------
    ExtractSession
        Extract session configured for tests.
    """
    from engine.runtime_profile import RuntimeProfileSpec

    runtime_spec = RuntimeProfileSpec(
        name="test_extract",
        datafusion=DataFusionRuntimeProfile(),
    )
    return build_extract_session(runtime_spec=runtime_spec)


@pytest.fixture
def runtime_profile_with_diagnostics() -> tuple[DataFusionRuntimeProfile, DiagnosticsCollector]:
    """Provide a runtime profile with diagnostics collector.

    Returns:
    -------
    tuple[DataFusionRuntimeProfile, DiagnosticsCollector]
        Runtime profile and diagnostics collector tuple.
    """
    collector = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(
        diagnostics=DiagnosticsConfig(diagnostics_sink=collector),
    )
    return profile, collector


def _sample_repo_files_rows() -> list[Mapping[str, Any]]:
    """Return sample rows for repo_files_v1 dataset.

    Returns:
    -------
    list[Mapping[str, Any]]
        Sample repo_files_v1 rows with required schema fields.
    """
    return [
        {
            "file_id": "test_file1.py",
            "path": "src/test_file1.py",
            "file_sha256": "abc123",
            "size_bytes": 100,
        },
        {
            "file_id": "test_file2.py",
            "path": "src/test_file2.py",
            "file_sha256": "def456",
            "size_bytes": 200,
        },
    ]


@pytest.mark.integration
def test_materialize_basic_roundtrip(extract_session: ExtractSession) -> None:
    """Test basic roundtrip materialization of an extract plan.

    Verifies that materialize_extract_plan produces valid Arrow output
    with expected schema and row count.

    NOTE: This test requires full extraction pipeline infrastructure including:
    - Registered UDFs for normalization
    - Proper normalization context with repo metadata
    - Complete session setup with all required extractors
    """
    rows = _sample_repo_files_rows()
    reader = record_batch_reader_from_rows("repo_files_v1", rows)
    plan = extract_plan_from_reader("repo_files_v1", reader, session=extract_session)

    runtime_profile = extract_session.engine_session.datafusion_profile
    result = materialize_extract_plan(
        "repo_files_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=DeterminismTier.STABLE,
    )

    # Result can be either Table or RecordBatchReader
    table: pa.Table = (
        result.read_all() if isinstance(result, pa.RecordBatchReader) else result  # type: ignore[assignment]
    )

    # Verify schema contains expected columns
    schema_names = {field.name for field in table.schema}
    assert "file_id" in schema_names
    assert "path" in schema_names

    # Verify row count
    assert table.num_rows > 0


@pytest.mark.integration
def test_evidence_plan_compilation() -> None:
    """Test evidence plan compilation behavior.

    Verifies that compile_evidence_plan correctly builds an EvidencePlan
    with expected sources and column requirements.

    NOTE: compile_evidence_plan defaults to ALL extract outputs when rules=None.
    To create a restricted plan, pass explicit rules.
    """
    # Default plan includes all extract outputs
    default_plan = compile_evidence_plan()
    assert default_plan.requires_dataset("repo_files_v1")
    assert default_plan.requires_dataset("ast_files_v1")

    # Plan with extra sources adds to the default set
    extended_plan = compile_evidence_plan(extra_sources=["repo_files_v1"])
    assert extended_plan.requires_dataset("repo_files_v1")

    # Plan with explicit rules restricts to those rules only
    restricted_plan = compile_evidence_plan(rules=["ast_files_v1"])
    assert restricted_plan.requires_dataset("ast_files_v1")
    # Note: repo_files_v1 may still be included if it's a dependency

    # Plan with column requirements
    columnar_plan = compile_evidence_plan(
        rules=["repo_files_v1"],
        required_columns={"repo_files_v1": ["file_id", "path"]},
    )
    required = columnar_plan.required_columns_for("repo_files_v1")
    assert "file_id" in required
    assert "path" in required


@pytest.mark.integration
def test_extract_plan_options_construction() -> None:
    """Test ExtractPlanOptions construction and API.

    Verifies that ExtractPlanOptions correctly handles normalize options,
    evidence plans, and repo_id resolution.
    """
    # Create evidence plan with restricted columns
    evidence_plan = compile_evidence_plan(
        rules=["repo_files_v1"],
        required_columns={"repo_files_v1": ["file_id", "path"]},
    )

    # Verify column requirements on the evidence plan
    required = evidence_plan.required_columns_for("repo_files_v1")
    assert "file_id" in required
    assert "path" in required

    # Create ExtractPlanOptions with evidence plan
    options = ExtractPlanOptions(evidence_plan=evidence_plan)
    assert options.evidence_plan is evidence_plan

    # Test repo_id resolution
    assert options.resolved_repo_id() is None

    options_with_repo = ExtractPlanOptions(repo_id="test_repo")
    assert options_with_repo.resolved_repo_id() == "test_repo"


@pytest.mark.integration
def test_artifact_emissions(
    runtime_profile_with_diagnostics: tuple[DataFusionRuntimeProfile, DiagnosticsCollector],
) -> None:
    """Test that diagnostics artifacts are emitted during materialization.

    Verifies that extract_plan_compile_v1 and extract_plan_execute_v1
    artifacts are recorded with expected fields.
    """
    runtime_profile, collector = runtime_profile_with_diagnostics

    rows = _sample_repo_files_rows()
    reader = record_batch_reader_from_rows("repo_files_v1", rows)

    # Build extract session with diagnostics-enabled runtime profile
    from engine.runtime_profile import RuntimeProfileSpec

    runtime_spec = RuntimeProfileSpec(name="test_extract_diag", datafusion=runtime_profile)
    diag_session = build_extract_session(runtime_spec=runtime_spec)

    plan = extract_plan_from_reader("repo_files_v1", reader, session=diag_session)

    _ = materialize_extract_plan(
        "repo_files_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=DeterminismTier.STABLE,
    )

    # Verify diagnostics artifacts were recorded
    artifacts_snapshot = collector.artifacts_snapshot()

    # Check for compile event
    assert "extract_plan_compile_v1" in artifacts_snapshot
    compile_artifacts = artifacts_snapshot["extract_plan_compile_v1"]
    assert len(compile_artifacts) > 0
    compile_payload = compile_artifacts[0]
    assert "dataset" in compile_payload
    assert "plan_fingerprint" in compile_payload

    # Check for execute event
    assert "extract_plan_execute_v1" in artifacts_snapshot
    execute_artifacts = artifacts_snapshot["extract_plan_execute_v1"]
    assert len(execute_artifacts) > 0
    execute_payload = execute_artifacts[0]
    assert "dataset" in execute_payload
    assert "rows" in execute_payload


@pytest.mark.integration
def test_extract_materialize_options_construction() -> None:
    """Test ExtractMaterializeOptions construction and API.

    Verifies that ExtractMaterializeOptions correctly handles prefer_reader
    and apply_post_kernels options.
    """
    # Default options
    default_opts = ExtractMaterializeOptions()
    assert default_opts.prefer_reader is False
    assert default_opts.apply_post_kernels is False
    assert default_opts.normalize is None

    # Options with prefer_reader
    reader_opts = ExtractMaterializeOptions(prefer_reader=True)
    assert reader_opts.prefer_reader is True

    # Options with post kernels
    kernel_opts = ExtractMaterializeOptions(apply_post_kernels=True)
    assert kernel_opts.apply_post_kernels is True

    # Combined options
    combined = ExtractMaterializeOptions(
        prefer_reader=True,
        apply_post_kernels=True,
    )
    assert combined.prefer_reader is True
    assert combined.apply_post_kernels is True


@pytest.mark.integration
def test_record_batch_reader_construction() -> None:
    """Test record_batch_reader_from_rows helper function.

    Verifies that record_batch_reader_from_rows produces a valid
    RecordBatchReader with expected schema aligned to dataset schema.
    """
    rows = _sample_repo_files_rows()
    reader = record_batch_reader_from_rows("repo_files_v1", rows)

    # Reader should have a schema
    assert reader.schema is not None

    # Schema should contain expected columns
    schema_names = {field.name for field in reader.schema}
    assert "file_id" in schema_names
    assert "path" in schema_names
    assert "file_sha256" in schema_names

    # Read data and verify row count matches input
    table = reader.read_all()
    assert table.num_rows == len(rows)

    # Verify values are correctly typed
    file_id_col = table["file_id"]
    path_col = table["path"]

    for i in range(table.num_rows):
        file_id_val = file_id_col[i].as_py()
        path_val = path_col[i].as_py()
        # Both are required non-nullable columns
        assert file_id_val is not None
        assert path_val is not None
        assert isinstance(file_id_val, str)
        assert isinstance(path_val, str)
